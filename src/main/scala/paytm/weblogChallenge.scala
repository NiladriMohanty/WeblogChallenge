package paytm

import org.apache.spark.sql.{SparkSession, DataFrame, SQLContext}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


object weblogChallenge {

  val schema = StructType(
    Array(
      StructField("timestamp", TimestampType),
      StructField("elb", StringType),
      StructField("client_port", StringType),
      StructField("backend_port", StringType),
      StructField("request_processing_time", DoubleType),
      StructField("backend_processing_time", DoubleType),
      StructField("response_processing_time", DoubleType),
      StructField("elb_status_code", IntegerType),
      StructField("backend_status_code", IntegerType),
      StructField("received_bytes", LongType),
      StructField("send_bytes", LongType),
      StructField("request", StringType),
      StructField("user_agent", StringType),
      StructField("ssl_cipher", StringType),
      StructField("ssl_protocol", StringType)
    )
  )

  def main(args: Array[String]): Unit = {

    // Initiating Spark session
    val spark = SparkSession.builder.appName("paytm_weblogChallenge").getOrCreate()
    import spark.implicits._

    // Input file path
    val inpFilePath = args(0)

    // Loading weblog data
    val weblogDf = spark.read.format("csv").option("sep", " ").schema(schema).load(inpFilePath)

    // Dropping duplicates from the weblog data
    val df = weblogDf.dropDuplicates()

    // During exploratory analysis, I observed that few "backend_port" & "user_agent" values are not present.
    // They have "-" as values. This means that a session is not complete.
    // Also, sometimes column has null values  so not selecting rows where important information is missing
    // which is needed for sessioning the data.
    // Therefore, cleaning the data where it has "-" in "backend_port" & "user_agent" and selecting not null from
    // "timestamp", "client_port", "request" & "user_agent".
    val dfSelect = (df
      .filter(($"backend_port" =!= "-") && ($"user_agent" =!= "-") && ($"timestamp".isNotNull) &&
        ($"client_port".isNotNull) && ($"request".isNotNull) && ($"user_agent".isNotNull))
      .select($"timestamp", $"client_port", $"request", $"user_agent")
      )

    // Further data cleaning by extracting client ip & url from "client_port" & "request" respectfully.
    // Converting timestamp to epoch seconds and extracting date which will help to establish sessions later.
    val dfClean = (dfSelect
      .withColumn("client_ip", split($"client_port", ":").getItem(0))
      .withColumn("url", split($"request", " ").getItem(1))
      .withColumn("epoch",unix_timestamp(to_timestamp(trim($"timestamp"))))
      .withColumn("date", split($"timestamp", " ").getItem(0))
      .select($"date", $"epoch", $"client_ip", $"url", $"user_agent")
      )


    println("Processing & Analytical goals solution started")


    // Solution to Q1. Sessionize the web log is as follows

    // Converting threshold inactivity in seconds
    // To complete this project, I chose 30 minutes as the threshold inactivity.
    // The reason is explained below.
    val inactivity_threshold_secs = args(1).toLong * 60

    // "client_ip" & "user_agent" are used to partition the data in this window function
    // "client_ip" & "user_agent" are unique to an user. This will help us in grouping them together with
    // the ordered time of the request.
    val winFunc = Window.partitionBy($"client_ip",$"user_agent").orderBy("epoch")

    // Establishing sessions using the window made above.
    // I used time-based expiration to sessionize. I find it difficult to sessionize using time based window.
    // Because time window for a session can vary from users to users.
    // Rather it is a convention that 30 minutes of inactivity can be considered as a new session.

    // Sessions (using time-based expiration) can be broadly determined using following 2 criteria:
    //    1. More than 30 minutes of inactivity
    //    2. At midnight (date change)
    // Using above declared window function and with the two time-based expiration criteria, I have calculated sessions.
    // These sessions have been numbered using the same above window.
    // Then an unique session id is assigned to them using a hash of "client_ip", "user_agent" & "session_num".
    val dfSessionize = (dfClean
      .withColumn("prev_request_epoch", lag($"epoch", 1, 0).over(winFunc))
      .withColumn("prev_request_date", lag($"date", 1, 0).over(winFunc))
      .withColumn("request_interval_epoch", $"epoch" - $"prev_request_epoch")
      .withColumn("request_interval_day", $"date" - $"prev_request_date")
      .withColumn("is_new_session", when($"request_interval_epoch" > inactivity_threshold_secs, 1)
        .when($"request_interval_day" > 0, 1)
        .otherwise(0))
      .withColumn("session_num", sum($"is_new_session").over(winFunc))
      .withColumn("session_id",hash($"client_ip", $"user_agent", $"session_num"))
      ).cache()

    println("Solution to Q1. Sessionize the web log is done")
    println()

    // Weblog data after sessioning written as a csv file in the output directory
    val outFilePath = args(2)

    println("Weblog data after sessioning written as a csv file in the output directory")
    (dfSessionize.coalesce(1).write.mode("overwrite").option("header", "true").option("compression","gzip")
      .csv(s"${outFilePath}/weblog_data_sessionized"))

    // Solution to Q2. Determine the average session time

    // Grouping using client_ip, user_agent & session_id (established above) and calculate session duration for
    // each session.
    // Also, removing sessions which is zero in duration. These sessions has only 1 activity.
    val dfSessionTime = (dfSessionize
      .groupBy($"client_ip",$"user_agent",$"session_id")
      .agg((max($"epoch") - min($"epoch")).as("session_duration"))
      .where($"session_duration" =!= 0)
      ).cache()

    // Calculating average session duration by taking average of all sessions' duration
    println("Solution to Q2. Determine the average session time is done")
    println()

    println("Average session duration in seconds is as follows")
    (dfSessionTime.select(avg($"session_duration").as("avg_session_duration"))).show(false)


    // Solution to Q3. Determine unique URL visits per session. To clarify, count a hit to a unique URL only
    // once per session.

    // Grouping using client_ip, user_agent & session_id (established above) and count distinct urls for each session.
    println("Solution to Q3. Determine unique URL visits per session is done")
    println()

    val dfSessionUniqueUrl = (dfSessionize
      .groupBy($"client_ip",$"user_agent",$"session_id")
      .agg(countDistinct($"url").as("unique_url_per_session"))
      .select("client_ip", "unique_url_per_session"))

    println("Unique URL visits per session is as follows")
    dfSessionUniqueUrl.show(false)

    // Weblog data with their unique urls for each session written as a csv file in the output directory
    println("Weblog data with their unique urls for each session written as a csv file in the output directory")
    (dfSessionUniqueUrl.coalesce(1).write.mode("overwrite").option("header", "true").option("compression","gzip")
      .csv(s"${outFilePath}/weblog_data_unique_urls"))


    // Solution to Q4. Find the most engaged users, ie the IPs with the longest session times

    // Calculating difference between session duration across sessions using reduce
    val longestSessionTime = dfSessionTime.reduce((a, b) => if (a.getLong(3) > b.getLong(3)) a else b)

    println("Solution to Q4. Find the most engaged users is done")
    println()

    println("The most engaged user is as follows")
    println("client_ip = " + longestSessionTime(0))
    println("user_agent = " + longestSessionTime(1))
    println("session_duration = " + longestSessionTime(3))

    spark.close()


  }
}