import org.apache.spark.sql.types._

//If your want to see intermediate result just uncommented block of code below.
object TestApplication {
  //Data from user for STEP 3
  val schema = StructType(
    StructField("name", StringType, nullable = true) ::
      StructField("age", StringType, nullable = true) ::
      StructField("birthday", StringType, nullable = true) ::
      StructField("gender", StringType, nullable = true) :: Nil
  )

  def main(args: Array[String]): Unit = {
    //Create instance of my API
    val api = new SparkSQLApplication(schema)

    val resultStep1 = api.readCSV("src/main/resources/data/data.csv")
    //  println("Results after -----STEP1-----")
    //  resultStep1.printSchema()
    //  resultStep1.foreach(row => println())

    val resultStep2 = api.filterEmptyData(resultStep1)
    //  println("Results after -----STEP2-----")
    //  resultStep2.printSchema()
    //  resultStep2.foreach(row => println())

    val json =
      """[
      {"existing_col_name" : "name", "new_col_name" : "first_name", "new_data_type" : "string"},
      {"existing_col_name" : "age", "new_col_name" : "total_years", "new_data_type" : "integer"},
      {"existing_col_name" : "birthday", "new_col_name" : "d_o_b", "new_data_type" : "date", "date_expression" : "dd-MM-yyyy"}
      ]
      """

    val resultStep3 = api.convertData(resultStep2, json)
    //  println("Results after -----STEP3-----")
    //  resultStep3.printSchema()
    //  resultStep3.foreach(row => println())

    val resultStep4 = api.profiling(resultStep3)
    //  println("Results after -----STEP4-----")

    println(resultStep4)
  }
}
