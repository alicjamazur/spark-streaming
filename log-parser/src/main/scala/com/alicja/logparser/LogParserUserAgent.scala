package com.alicja.logparser

import java.util.regex.Matcher

import com.alicja.logparser.Utilities.{apacheLogPattern, setupLogging}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

/** Maintains top URL's visited over a 5 minute window, from a stream
 * of Apache access logs on port 9999.
 */
object LogParserUserAgent {

  def main(args: Array[String]) {

    // Create the context with a 1 second batch size
    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    setupLogging()

    // Construct a regular expression (regex) to extract fields from raw Apache log lines
    val pattern = apacheLogPattern()

    // Create a socket stream to read log data published via netcat on port 9999 locally
    val lines = ssc.socketTextStream("127.0.0.1", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    // Extract the request field from each log line
    val agents = lines.map(x => {
      val matcher: Matcher = pattern.matcher(x);
      if (matcher.matches()) matcher.group(9)
    })


    // Reduce by URL over a 5-minute window sliding every second
    val urlCounts = agents.map(x => (x, 1)).reduceByKeyAndWindow(_ + _, _ - _, Seconds(300), Seconds(1))

    // Sort and print the results
    val sortedResults = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))
    sortedResults.print()

    // Kick it off
    ssc.checkpoint("/Users/alicjamazur/Desktop/all-projects/spark-streaming-course/log-parsing/checkpoint2")
    ssc.start()
    ssc.awaitTermination()
  }
}
