import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

object SparkSQLApplication {
  // Spark sessionConf. I run on my local machine.
  val sparkSession = SparkSession.builder()
    .appName("SparkSQL test assignments")
    .config("spark.master", "local")
    .getOrCreate()

  /**
    * Check value from CSV for emptiness
    * @param value
    * @return
    */
  private def nonEmpty(value: String): Boolean = {
    value.replaceAll("\\s+", "").nonEmpty
  }

  /**
    * Convert column from one type to another. In this case i have some trouble with converting string->date, because
    * data from your .csv doesn't converts. If i change format in csv to like this yyyy:mm:dd. So, i found some
    * solution, but for it seems not efficient.
    *
     * @param parameters
    * @param dataFrame
    * @return
    */
  private def convertDataFrameColumn(parameters: ConvertParameters, dataFrame: DataFrame): DataFrame = {
    parameters.newDataType match {
      case "date" => convertDateColumn(parameters, dataFrame)
      case _ => convertPrimitiveColumn(parameters, dataFrame)
    }
  }

  /**
    * Whit all primitive typs it works very well. What about if user in future provide new type, we only implement our
    * own type and thats all, don't need to change logic.
    * @param parameters
    * @param dataFrame
    * @return
    */
  private def convertPrimitiveColumn(parameters: ConvertParameters, dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(parameters.newColName, dataFrame(parameters.existingColName)
      .cast(parameters.newDataType))
      .drop(parameters.existingColName)
  }

  import org.apache.spark.sql.functions._

  private def convertDateColumn(parameters: ConvertParameters, dataFrame: DataFrame): DataFrame = {
    dataFrame.withColumn(parameters.newColName, col(parameters.existingColName)
      /*date_format(col(parameters.existingColName), "MM/dd/yyyy")*/ .cast(parameters.newDataType)).drop(parameters.existingColName)
  }
}

class SparkSQLApplication(schema: StructType) {

  import SparkSQLApplication._

  /**
    * ---------------------STEP 1--------------------
    * @param path
    * @return
    */
  def readCSV(path: String): DataFrame = {
    sparkSession.read.option("header", value = true).schema(schema).csv(path)
  }

  /**
    * ---------------------STEP 2--------------------
    * @param dataFrame
    * @return
    */
  def filterEmptyData(dataFrame: DataFrame): DataFrame = {
    dataFrame.filter { row =>
      row.toSeq.count(value => value == null ||
        (value.isInstanceOf[String] && nonEmpty(value.asInstanceOf[String]))
      ) == row.length
    }
  }

  /**
    * ---------------------STEP 3--------------------
    * @param dataFrame
    * @param userInputs
    * @return
    */
  def convertData(dataFrame: DataFrame, userInputs: String): DataFrame = {
    implicit val formats = DefaultFormats
    val json = parse(userInputs)
    val parameters = json.camelizeKeys.extract[List[ConvertParameters]]
    var mutableDataFrame = dataFrame.drop(dataFrame.columns.diff(parameters.map(_.existingColName)): _*)
    parameters.foreach { param =>
      mutableDataFrame = convertDataFrameColumn(param, mutableDataFrame)
    }
    mutableDataFrame
  }

  /**
    * May be not enough efficient and transparently, but i don't have enough experience with json.
    * ---------------------STEP 4 --------------------
    * @param dataFrame
    * @return
    */
  def profiling(dataFrame: DataFrame): String = {
    import org.apache.spark.sql.functions.approx_count_distinct

    val json = dataFrame.columns.toList.map { column =>
      ("Column" -> column) ~
        ("Unique-values" -> dataFrame.agg(approx_count_distinct(column)).head().get(0).toString) ~
        ("Values" ->
          dataFrame.select(column).rdd.filter(_.get(0) != null)
            .map(row => (row.get(0), 1))
            .reduceByKey(_ + _)
            .collect().toList
            .map { key =>
              s"${key._1.toString}" -> key._2
            })
    }

    compact(render(json))
  }
}

/**
  * Converts users inpur from Steps 3 to more readble and efficient way, because json4s library provide
  * great solution converting json to case object.
  *
  * @param existingColName
  * @param newColName
  * @param newDataType
  * @param dateExpression
  */
case class ConvertParameters(existingColName: String, newColName: String, newDataType: String, dateExpression: Option[String])
