import org.scalatest._

class SparkSQLApplicationSuite extends FlatSpec {

  var sparkApi = new SparkSQLApplication(TestApplication.schema)

  "A size of dataframe " should "be 6 after loading from CSV file STEP1" in {
    assert(sparkApi.readCSV("src/main/resources/data/data.csv").count() === 6)
  }


  "A size of dataframe " should "be 3 after filtering empty data STEP2" in {
    val resultStep1 = sparkApi.readCSV("src/main/resources/data/data.csv")

    assert(sparkApi.filterEmptyData(resultStep1).count() === 3)
  }

  "A size  of columns " should "be 3 after changing columns names and omit some " in {
    val resultStep1 = sparkApi.readCSV("src/main/resources/data/data.csv")
    val resultStep2 = sparkApi.filterEmptyData(resultStep1)
    val json =
      """[
      {"existing_col_name" : "name", "new_col_name" : "first_name", "new_data_type" : "string"},
      {"existing_col_name" : "age", "new_col_name" : "total_years", "new_data_type" : "integer"},
      {"existing_col_name" : "birthday", "new_col_name" : "d_o_b", "new_data_type" : "date", "date_expression" : "dd-MM-yyyy"}
      ]
      """

    val resultStep3 = sparkApi.convertData(resultStep2, json)

    assert(resultStep3.columns.length === 3)
  }


  "A size of distinct values" should "equivalent to results" in {
    val resultStep1 = sparkApi.readCSV("src/main/resources/data/data.csv")
    val resultStep2 = sparkApi.filterEmptyData(resultStep1)
    val json =
      """[
      {"existing_col_name" : "name", "new_col_name" : "first_name", "new_data_type" : "string"},
      {"existing_col_name" : "age", "new_col_name" : "total_years", "new_data_type" : "integer"},
      {"existing_col_name" : "birthday", "new_col_name" : "d_o_b", "new_data_type" : "date", "date_expression" : "dd-MM-yyyy"}
      ]
      """

    val resultStep3 = sparkApi.convertData(resultStep2, json)
    import org.apache.spark.sql.functions.approx_count_distinct

    assert(resultStep3.agg(approx_count_distinct("first_name")).head().get(0).toString === "2")
    assert(resultStep3.agg(approx_count_distinct("total_years")).head().get(0).toString === "1")
    assert(resultStep3.agg(approx_count_distinct("d_o_b")).head().get(0).toString === "1")


  }

}