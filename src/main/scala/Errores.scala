import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions.{col,when}

object Errores {
  val json = "src/main/resources/recursos.json"
  val listaGenero = Seq("male","female")

  def getErrors(implicit sparkSession: SparkSession) ={
    val df = sparkSession.read.option("multiline","true").json(json)
    val creates = df.collect().map(crearWiths(_))
    creates.reduce(_ or _)
  }
  def crearWiths(row: Row)={
    val colName = row.getAs[String]("column")
    println(s"colName:$colName")
    val tipeValidation = row.getAs[String]("error")
    //val fatalError = row.getAs[Boolean]("fatal")

   // val passValidate :Boolean = ???

    getPassValidation(colName, tipeValidation)
  }

  private def getPassValidation(colName: String, tipeValidation: String) = {
    tipeValidation match {
      case "isNull" => validationNulls(col(colName))
      case "gender" => validationOptions(col(colName),listaGenero)
      case "noNumber" => noNumber(col(colName))
    }
  }

  def validationNulls (name:Column)= {
    name.isNull
  }
  def validationOptions (name:Column, list: Seq[String]) = {
    name.isin(list)
  }
  def noNumber(name:Column) = {
    name.isNaN
  }
  implicit class ColumnMethods(col: Column) {
    def isTrue: Column = {
     // if (col.isNotNull and true) true else false
      when(col.isNull, false).otherwise(col === true)
    }
  }

}
