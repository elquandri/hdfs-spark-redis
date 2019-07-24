import java.io.BufferedOutputStream

import com.redislabs.provider.redis._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import resultcheck.generate

object hdfsTxtToRedis extends App with configuration {

  val ds =readFile(filePath)
          initifile()
  val df =traitement(ds)
           writeRedis(df)
          checkresult(df)

  def readFile(path : String): Dataset[String] ={

    val data = spark.read.textFile(path).cache()
    return data

  }
  def traitement(ds : Dataset[String]): DataFrame={

    val numAs = ds.filter(line => line.contains("a")).count()
    val numBs = ds.filter(line => line.contains("b")).count()
    val numCs = ds.filter(line => line.contains("c")).count()
    val numDs = ds.filter(line => line.contains("d")).count()
    val numEs = ds.filter(line => line.contains("e")).count()
    val numFs = ds.filter(line => line.contains("f")).count()
    val df = spark.createDataFrame(List(("Lines with a",numAs),
             ("Lines with b",numBs),("Lines with c",numCs),
              ("Lines with d",numDs),("Lines with e",numEs),("Lines with f",numFs)))
      .withColumnRenamed("_1","Ligne")
      .withColumnRenamed("_2","count")

  return df

  }

  def writeRedis(df: DataFrame){
    df.write
      .format("org.apache.spark.sql.redis")
      .option("table", "words")
      .option("key.column", "Ligne")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def checkresult(df : DataFrame): Unit = {

    var c=""

    val dfRead = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "words")
      .option("key.column", "Ligne")
      .load()
    df.show()
    dfRead.show()

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
