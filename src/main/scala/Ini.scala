import org.apache.spark
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

import scala.io.Source

object Ini extends LazyLogging{

  def main(args: Array[String]): Unit = {

    var exitCode = 0

    try {
      implicit val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("Spark SQL basic example")
        .config("spark.some.config.option", "some-value")
        .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      File.processJson

    inicio
    }
    finally {
      logger.info(s"System Exit Code: $exitCode")
      val recursos = Source.fromResource("recursos.json")
      System.exit(0)
    }
  }


  def procesar(first: Row) = ???

  def inicio(implicit sparkSession: SparkSession) ={
    import sparkSession.implicits._
    //val json = "src/main/resources/recursos.json"
    val data = "src/main/resources/data.csv"
    println("hola")
    //val df = sparkSession.read.option("multiline","true").json(json)
    val dataDf = sparkSession.read.option("header","true").csv(data)
    //val process = df.collectAsList()//crearWiths(_))//.collect()
    //pro
    //val arrayerrores: Array[Row] = process.toArray
    //arrayerrores.map(crearWiths(_))
    //val first = process.get(1)
    //procesar(first)
    //process.forEach(crearWiths(_))
    //process.foreach(println)
    //process.show()
    //df.withColumn("error",lit("error"))
    //val rddDF = df.map(_)
    //
    // val ventana = Window.partitionBy("column")
   // df.select(col("name"), col("error").over(ventana)).show()
    //dataDf.show()
    //dataDf.withColumn("error",createError).show(
    dataDf.withColumn("error1",Errores.getErrors).show
dataDf.withColumn("error", validationNulls(col("name")) or (validationNulls(col("age")))).show
   // process.toList.foreach(println)

  }

  private def createError = {
    when(col("gender").isNull, lit(1))
  }
  def reducir(row: Row, row1: Row): Row = {
println(row.length)
println(row1.length)

    //column

    row
  }
  def crearWiths(row: Row)={
    val colName = row.getAs[String]("column")
    println(s"colName:$colName")
    val tipeValidation = row.getAs[String]("error")
    println(s"tipeValidation:$tipeValidation")
    val fatalError = row.getAs[Boolean]("fatal")
    println(s"fatalError:$fatalError")
val lista = Seq("1","2").toList
    tipeValidation match {
      case "isNull" => validationNulls(col(colName))
      case "gender" => validationOptions(col(colName),lista)
      case "noNumber" => noNumber(col(colName))
    }

  }

  def validationNulls (name:Column)= {
    name.isNull
  }
  def validationOptions (name:Column, list: List[String]) = {
    name.isin(list)
  }
  def noNumber(name:Column) = {
    name.isNaN
  }


}
