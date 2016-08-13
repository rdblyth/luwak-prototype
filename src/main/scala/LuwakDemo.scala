import java.io.File

import org.apache.lucene.analysis.standard.StandardAnalyzer
import org.slf4j.LoggerFactory
import uk.co.flax.luwak.matchers.{HighlightingMatcher, HighlightsMatch}
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
  val queries = getQueries(queriesFile)
  monitor.update(queries)

  logger.info(s"Added ${queries.size} queries to monitor")
  val batch = DocumentBatch.of(buildDocs(documentsDir).toList)

  logger.info(s"Loaded ${batch.getBatchSize} documents")
  val matches = monitor.`match`(batch, HighlightingMatcher.FACTORY)
  outputMatches(matches)

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

  def outputMatches(matches : Matches[HighlightsMatch]) {
    logger.info(s"Matched batch of ${matches.getBatchSize()} documents in ${matches.getSearchTime()} milliseconds with ${matches.getQueriesRun()} queries run")

    for(docMatches <- matches; hightLightsmatch <- docMatches) {
      logger.info(s"\tQuery: ${hightLightsmatch.getQueryId()} (${hightLightsmatch.getHitCount()} hits)")
    }
  }
}
