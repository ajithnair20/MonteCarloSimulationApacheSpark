import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext, SparkSession}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.mutable.ListBuffer
//spark-submit --class Main --master yarn --deploy-mode client ./ajithjayaraman_nair_hw3_2.12-1.0.jar

object MonteCarloSim {

  //Declaring and initializing Spark configuration variables
  val config = ConfigFactory.load("Application.conf")
  val logger: Logger = LoggerFactory.getLogger(this.getClass)
  val sparkConf = new SparkConf().setAppName("DataFrame From CSV").setMaster("local")
  val sc = new SparkContext(sparkConf)
  val sqlContext = new SQLContext(sc)
  val ss: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
  val client = new StockDataClient()

  //Setting up range for the number of days of samples and the number of simulations for sampling Monte Carlo
  val days : Int = config.getString("Days").toInt
  val samples : Int = config.getString("Samples").toInt

  //Output and Input Paths
  var OutputPath : String = ""
  var DataPath : String = ""

  def main(args: Array[String]): Unit = {

    logger.info("Starting simulation")

    //setting output path
    OutputPath = args(0).toString
    //DataPath = args(1).toString

    //Generating Stock Data for Initial Companies
    val initCompanies : String = config.getString("InCompanies")
    val inInv : Double = config.getDouble("InvAmt")

    //Generate the records of the companies in which money is invested and the maximum profit that can be genrated during the period
    val profitList = initCompanies.split(",").map(t => {
      val stockstats =  getStockStats(t)
      val maxProfit = getMaxProfit(sc.parallelize(stockstats._2))
      val profitGenerated = (inInv / stockstats._1) * maxProfit
      (t, profitGenerated)
    })
    val outputDF = ss.createDataFrame(profitList).toDF("Company","Profit")
    outputDF.write.format("com.databricks.spark.csv").save(OutputPath + "/output")
    outputDF.show()
  }

  //Method to load DataFrame from a CSV file
  /*def loadDataFrameFromCSV(filePath:String) :DataFrame ={
    logger.info("Loading Dataframe from CSV file")

      //Create window and lagging columns to compute change, change percent and log of change
      val window = Window.orderBy("timestamp")
      val laggingCol = lag(col("close"), 1).over(window)
      val closeChange = log(col("close") / col("lastclose"))


      //Convert CSV file to Data Frame and add change, percent change and log columns
      //remove columns that are not required
      val df = sqlContext.read.format("com.databricks.spark.csv")
        .option("delimiter", ",")
        .option("header", true)
        .load(filePath)
        .drop("open").drop("high").drop("low").drop("volume")
        .withColumn("lastclose", laggingCol)
        .withColumn("change", closeChange)
      return df
  }*/
  def loadDataFrameFromCSV(filePath:String) :DataFrame ={
    logger.info("Loading Dataframe from CSV file")
    //Fetch Data from API using Client
    val df = client.fetchData(filePath)

    //Create window and lagging columns to compute change, change percent and log of change
    val window = Window.orderBy("timestamp")
    val laggingCol = lag(col("close"), 1).over(window)
    val closeChange = log(col("close") / col("lastclose"))


    //Convert CSV file to Data Frame and add change, percent change and log columns
    //remove columns that are not required
    val modDF = df.drop("open").drop("high").drop("low").drop("volume")
      .withColumn("lastclose", laggingCol)
      .withColumn("change", closeChange)
    modDF.show()
    modDF
  }



  //Method to generate simulations from given change set and DataFrame
  def generateSimulations(StockDF : DataFrame, sparkSession: SparkSession, stockName:String) : List[List[Double]] = {
    import sparkSession.implicits._
    logger.info("Generating simulations with " + days + " and " + samples + " samples." )

    //Extract the list of change value to randomize and compute simulated value
    val changeSetList = StockDF.select("change").rdd.collect().toList.drop(1)
    //First day price which is then regressed.
    val firstDayPrice : Row = StockDF.select("close").rdd.collect().toList.head
    var resList = new ListBuffer[List[Double]]()

    try {
      //Generating values for each sample
      (1 to samples).foreach(i => {
        var simmArr = new ListBuffer[Double]()
        //simmArr += firstDayPrice.getString(0).toDouble
        simmArr += firstDayPrice.getDouble(0)//.toDouble

        //shuffle the change set to get random change value for each day in a simulation
        val shuffledList = scala.util.Random.shuffle(changeSetList).map(row => row.getDouble(0))

        //For every shuffled change value compute the expected price value
        shuffledList.foreach(x => {
          simmArr += simmArr(simmArr.length - 1).toString().toDouble * Math.exp(x)
        });
        resList += simmArr.toList
      })
    }catch {case x: Exception => logger.error(x.getMessage())}

    //Save output of simulations
    sc.parallelize(resList.toList.transpose).saveAsTextFile(OutputPath + "/simulations" + stockName)

    //Return list output of simulations
    resList.toList.transpose//.toDF()
  }

  //Method to fetch stats for a particular Stock over a range of days
  def getStockStats(stockType :String): (Double, List[Double]) ={

    logger.info("Extracting stock stats for :" + stockType)
    //Fetch Data Frame based on the name of the company using the CSV file
    //val df = loadDataFrameFromCSV(DataPath + "/" + stockType + ".csv")
    //val df = loadDataFrameFromCSV(config.getString("DataPath") + stockType + ".csv")
    val df = loadDataFrameFromCSV(stockType)

    //Generate simulations for monte carlo method and fetch mean value of the price of stock for N simulations
    val sims : List[Double] = generateSimulations(df,ss,stockType).map(row => row.sum/row.size)

    //Return original value of the stock at the beginning of the simulations to compute profit
    val initialPrice = df.select("close").first().getDouble(0)//.toDouble
    //val initialPrice = df.select("close").first().getString(0).toDouble
    (initialPrice,sims)
  }

  //Method to calculate maximum profit from a RDD of mean values of prices of every day
  def getMaxProfit(prices : RDD[Double]) : Double = {
    val pricesList = prices.collect().toList
    pricesList.zip(pricesList.scan(Double.MaxValue)(math.min).tail).map(p => p._1 - p._2).foldLeft(0.0)(math.max)
  }
}