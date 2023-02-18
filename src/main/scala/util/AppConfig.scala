package sample.util

import com.typesafe.config.ConfigFactory

object AppConfig {

  val conf = ConfigFactory.load
  val neoPasswordDef = conf.getString("neo.password")

  var neoPassword = neoPasswordDef

  def main(args: Array[String]): Unit = {
    parse("-m localhost1 --akkaHttpPort 8080".split(" ").toList)
  }

  val usage =
    s"""
    Apart from Spark, this application uses akka-http from browser integration.
    So, it needs config params like AkkaWebPort to bind to, SparkMaster
    and SparkAppName.

    Usage: spark-submit graph-knowledge-browser.jar [options]
      Options:
      -h, --help
      -p, --neoPassword <password>              Neo4J password. Default: $neoPasswordDef
  """

  def parse(list: List[String]): this.type = {

    list match {
      case Nil => this
      case ("--neoPassword" | "-p") :: value :: tail => {
        neoPassword = value.toString
        parse(tail)
      }
      case ("--help" | "-h") :: tail => {
        printUsage(0)
      }
      case _ => {
        printUsage(1)
      }
    }
  }

  def printUsage(exitNumber: Int) = {
    println(usage)
    sys.exit(status = exitNumber)
  }
}
