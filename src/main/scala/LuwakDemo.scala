import java.io.File

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.slf4j.LoggerFactory
import uk.co.flax.luwak.matchers.SimpleMatcher
import uk.co.flax.luwak._
import uk.co.flax.luwak.presearcher.TermFilteredPresearcher
import uk.co.flax.luwak.queryparsers.LuceneQueryParser

import scala.collection.JavaConversions._
import scala.io.{Codec, Source}

object LuwakDemo extends App {
  val queriesFile = "src/main/resources/queries.txt"
  val documentsDir = "src/main/resources/gutenberg"
  val FIELD = "text"

  val logger = LoggerFactory.getLogger("LuwakDemo")
  val analyzer = new StandardAnalyzer

  val monitor = new Monitor(new LuceneQueryParser(FIELD, analyzer), new TermFilteredPresearcher())
  monitor.update(getQueries(queriesFile))
  logger.info(s"Added ${monitor.getQueryCount} queries to monitor")

  val documents = buildDocs(documentsDir).toList
  logger.info(s"Loaded ${documents.size} documents")

  val docMatches = documents.map(doc => (doc, monitor.`match`(doc, SimpleMatcher.FACTORY)))
  docMatches.foreach{ case(doc, matches) => logDocumentMatches(doc, matches) }

  val totalTime = docMatches.map(_._2.getSearchTime()).sum
  logger.info(s"Total time to match all documents: $totalTime ms")

  def getQueries(queriesFile: String) = {
    logger.info(s"Loading queries from $queriesFile")
    val lines = Source.fromFile(queriesFile).getLines
    lines.zipWithIndex.map { case (query, count) => new MonitorQuery(s"$count-$query", query) }.toList
  }

  def buildDocs(directory: String) = {
    for (file <- new File(directory).listFiles()) yield {
      val content = Source.fromFile(file)(Codec.ISO8859).getLines.mkString("\n")
      InputDocument.builder(file.getName).addField(FIELD, content, new StandardAnalyzer).build()
    }
  }

  def logDocumentMatches(doc: InputDocument, matches: Matches[QueryMatch]) = {
    logger.info(s"Documents ${doc.getId} was matched in ${matches.getSearchTime()} ms with ${matches.getQueriesRun()} queries run")
  }
}
