package sample
import com.sun.net.httpserver.HttpServer
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.config.ConfigFactory

import scala.io.Source
import java.net.InetSocketAddress
import org.apache.spark.sql.functions.{col, levenshtein, lit, log, struct, to_json}
import sample.util.AppConfig

import java.util.stream.Collectors
//--add-exports java.base/sun.nio.ch=ALL-UNNAMED

object Main {
    private def sendResponse(he: HttpExchange, status: Int, response: String): Unit = {
      he.getResponseHeaders.set("Content-Type", "application/json");
      he.getResponseHeaders.set("Access-Control-Allow-Methods", "GET, OPTIONS, POST");
      he.getResponseHeaders.set("Access-Control-Allow-Headers", "Content-Type,Authorization");
      he.getResponseHeaders.set("Access-Control-Allow-Origin", "*");
      val bs = response.getBytes("UTF-8")
      he.sendResponseHeaders(status, bs.length)
      val os = he.getResponseBody()
      os.write(bs)
      os.close()
    }
  private def startServer(df: DataFrame): Unit = {
    val server = HttpServer.create(new InetSocketAddress(8002), 0)
    server.setExecutor(null)
    server.createContext("/", new HttpHandler {
      override def handle(httpExchange: HttpExchange): Unit = {
        val payload = Source.fromInputStream(httpExchange.getRequestBody).mkString
        val result = df.withColumn("searchFor", lit(payload))
          .withColumn("difference", levenshtein(col("sourceusername"), col("searchFor")))
          .sort("difference").limit(10).select(to_json(struct(s"*")))
          .toDF("user")
        var jsonString = "["
        for (row <- result.collect) {
          jsonString = s"$jsonString${row.mkString},"
        }
        jsonString = jsonString.substring(0, jsonString.length-1)
        jsonString = s"$jsonString]"
        print(jsonString)

        val timestamp = new java.sql.Timestamp(System.currentTimeMillis())

        try {
          val response = s"""{ "success": true, "timestamp": "$timestamp", "users": $jsonString }"""
          sendResponse(httpExchange, status = 200, response)
        } catch {
          case e: Throwable => print(e)
        }
      }
    })

    println(s"Server online at http://localhost:", 8002, "/")
    println("Done")
    server.start()
  }
  def main(args: Array[String]) = {
    val spark = SparkSession.builder().config("spark.master", "local").getOrCreate()
    print("STARTING APPLICATION")
    AppConfig.parse(args.toList)

    val df = spark.read.format("org.neo4j.spark.DataSource")
      .option("url", "bolt://neo.luckylive.ir")
      .option("authentication.basic.username", "neo4j")
      .option("authentication.basic.password", AppConfig.neoPassword)
      .option("relationship", "FOLLOWS")
      .option("relationship.source.labels", "User")
      .option("relationship.target.labels", "User")
//      .option("labels", "User:Post")
      //      .option("query", "MATCH (u:User) WHERE apoc.text.distance(u.full_name, 'aref') < 5 RETURN u.full_name, u.pk")
      .load()
    var newDf = df
    for (col <- df.columns) {
      val r1 = col.replaceAll("[\\W]|_", "")
      newDf = newDf.withColumnRenamed(col, r1)
    }
    newDf = newDf.selectExpr("sourceusername", "sourcefullname", "targetusername")
    newDf.show()
    newDf.printSchema()

    startServer(newDf)

    //    MATCH(a: Document)
    //    WHERE apoc
    //    .text.distance(a.title, "A title") < 10
    //    df.where("full_name = 'aref'").show()
    //    val resultDf = spark.sql(
    //  """MATCH(a: Document)
    //    |WHERE apoc
    //    |.text.distance(a.title, "A title") < 10
    //    |""".stripMargin
    //)
  }
}

