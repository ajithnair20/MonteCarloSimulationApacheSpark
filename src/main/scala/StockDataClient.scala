import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import scala.io.Source._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import scala.io.Source

class StockDataClient {
  //Url of API to fetch finance data with API Key X5TH7YQYI9JWFX5D
  val config = ConfigFactory.load("Application.conf")

  val sparkConf = new SparkConf().setAppName("DataFrame From CSV").setMaster("local")
  val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  //Method to fetch Data from API and return Data Frame
  def fetchData(company : String) : DataFrame = {
    import ss.implicits._
    logger.info("Fetching Data from API for Company: " + company)
    val apiURL = config.getString("ApiURL").replace("COMPANYNAME", company)

    try {
      val res = Source.fromURL(apiURL).mkString.stripMargin.lines.toList
      val csvData: Dataset[String] = ss.sparkContext.parallelize(res).toDS()
      val frame: DataFrame = ss.read.option("header", true).option("inferSchema", true).csv(csvData)
      return frame
    }catch { case x: Exception => logger.error(x.getMessage)}
    null
  }
}
