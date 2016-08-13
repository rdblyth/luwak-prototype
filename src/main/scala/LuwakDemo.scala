import java.io.{File, FileInputStream}
import java.util.zip.GZIPInputStream

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.slf4j.LoggerFactory
import uk.co.flax.luwak.matchers.SimpleMatcher
import uk.co.flax.luwak._
import uk.co.flax.luwak.presearcher.{MatchAllPresearcher, TermFilteredPresearcher}
import uk.co.flax.luwak.queryparsers.LuceneQueryParser

import scala.collection.JavaConversions._
import scala.io.{Codec, Source}

object LuwakDemo extends App {
  val queriesDir = "src/main/resources/queries"
  val documentsDir = "src/main/resources/docs"
  val FIELD = "text"

  val logger = LoggerFactory.getLogger("LuwakDemo")
  val analyzer = new StandardAnalyzer

  val monitor = new Monitor(new LuceneQueryParser(FIELD, analyzer), new TermFilteredPresearcher)
  //val monitor = new Monitor(new LuceneQueryParser(FIELD, analyzer), new MatchAllPresearcher)
  monitor.update(getQueries(queriesDir).toList)
  logger.info(s"Added ${monitor.getQueryCount} queries to monitor")

  val docMatches = matchDocuments(documentsDir, monitor)
  val totalTime = docMatches.map(_._2.getSearchTime()).sum
  logger.info(s"Matched ${docMatches.size} documents in $totalTime ms")
  logger.info(s"Average time to process a document: ${totalTime / docMatches.size} ms")

  monitor.close()

  def getQueries(directory: String) = {
    logger.info(s"Loading queries from $queriesDir")
    for (file <- new File(directory).listFiles()) yield {
      val query = Source.fromFile(file)(Codec.ISO8859).getLines.mkString("\n")
      new MonitorQuery(file.getName, query)
    }
  }

  def matchDocuments(documentDir: String, monitor: Monitor) = {
    val documentFiles = new File(documentsDir).listFiles().filter(_.getName.endsWith(".gz"))
    for(file <- documentFiles) yield {
      val matches = monitor.`match`(buildDocument(file), SimpleMatcher.FACTORY)
      logDocumentMatches(file.getName, matches)
      (file.getName, matches)
    }
  }

  def buildDocument(file: File) = {
    val content = Source.fromInputStream(new GZIPInputStream(new FileInputStream(file))).getLines().mkString("\n")
    InputDocument.builder(file.getName).addField(FIELD, content, new StandardAnalyzer).build()
  }

  def logDocumentMatches(docName: String, matches: Matches[QueryMatch]) = {
    logger.info(s"Documents $docName was matched in ${matches.getSearchTime()} ms with ${matches.getQueriesRun()} queries run")
  }
}
