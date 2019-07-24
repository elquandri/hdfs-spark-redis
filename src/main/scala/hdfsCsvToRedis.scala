import java.io.BufferedOutputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import resultcheck.generate

object hdfsCsvToRedis extends App with configuration {

  val ds =readCsv(csvFile)
  initifile()
  writeRedis(ds)
  checkresult(ds)

  def readCsv(path:String) : DataFrame =
  {
    val df = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
    return df.dropDuplicates("Code_commune_INSEE")
  }



  def writeRedis(df: DataFrame){
    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "csv")
      .option("key.column", "Code_commune_INSEE")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def checkresult(df : DataFrame): Unit = {

    var c=""

    val dfRead = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "csv")
      .option("key.column", "Code_commune_INSEE")
      .load()

    val diff = dfRead.except(df)

    if (diff.count() ==0)
    {
      c = generate("passed").toString
    }
    else if(diff.count() !=0) {
      c = generate("failed").toString
    }
    else {
      c = generate("bloqued").toString
    }

    saveResultFile(c)

  }

  def initifile(): Unit =
  {
    val c =generate("failed").toString
    saveResultFile(c)
  }

  def saveResultFile(content: String) = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", pathHdfs)

    val date = java.time.LocalDate.now.toString
    val filePath = pathResult + resultPrefix + "_" + date + ".json"

    val fs = FileSystem.get(conf)
    val output = fs.create(new Path(filePath), true)

    val writer = new BufferedOutputStream(output)

    try {
      writer.write(content.getBytes("UTF-8"))
    }
    finally {
      writer.close()
    }
  }
  spark.stop()

}
