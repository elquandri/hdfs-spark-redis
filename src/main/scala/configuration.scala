import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

trait configuration {


  val config: Config = ConfigFactory.load("Redis.conf")
  val redisHostC = config.getString("redisHostC")
  val appName = config.getString("appName")
  val master = config.getString("master")
  val redisHost = config.getString("redisHost")
  val redisPortC = config.getString("redisPortC")
  val redisPort = config.getString("redisPort")
  val filePath = config.getString("filePath")
  val pathResult =config.getString("pathResult")
  val resultPrefix = config.getString("resultPrefix")
  val pathHdfs = config.getString("pathHdfs")
  val csvFile = config.getString("csvFile")


  val spark = SparkSession
    .builder()
    .appName(appName)
    .master(master)
    .config(redisHostC,redisHost) // LabV1
    .config(redisPortC,redisPort) // LabV1
    .getOrCreate()

  val sc = spark.sparkContext

}
