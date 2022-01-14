import Errores.json
import org.apache.spark.sql.SparkSession
import play.api.libs.json.Json

import scala.io.Source
import org.apache.spark.sql.functions._

object File {
  val json = "src/main/resources/recursos2.json"

  def processJson(implicit sparkSession: SparkSession) = {
    import sparkSession.implicits._

    val df = sparkSession.read.option("multiline","true").json(json)
    df.show()
    println("### explode")
    df.select($"column",explode($"errores")).dropDuplicates().show()
    println("### explode_outer")
    df.select($"column",explode_outer($"errores")).dropDuplicates().show()
    println("### posexplode")

    df.select($"column",posexplode($"errores")).dropDuplicates().show()
    println("### posexplode_outer")

    df.select($"column",posexplode_outer($"errores")).dropDuplicates().show()

    val listError = df.collect().toList
 //   val firstRow = listError.head.asInstanceOf[Errors]
  //  println(s"first:${firstRow.column}:${firstRow.validations}")
    println(listError)
   val jsonFile = Json.parse(Source.fromResource("recursos2.json").getLines().mkString)
   // println(jsonFile)
  }
}
case class Errors (
                           column: String,
                           validations: Seq[Validations]
                         )

case class Validations (
                         fatal: Boolean,
                         error: String
                       )


