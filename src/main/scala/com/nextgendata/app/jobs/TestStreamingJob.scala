package com.nextgendata.app.jobs

//import com.nextgendata.app.source.biz.CustomerRow
//import com.nextgendata.app.source.cif.XrefRow
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

/**
  * Created by Craig on 2016-06-30.
  */
object TestStreamingJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("streaming_app")
      //The spark.eventLog.enabled is used if you want Spark to record run history that you can view later in the Spark Web UI by
      //starting up the history server
      //.config("spark.eventLog.enabled", "true")
      //May need to change the line below if you are not on Windows
      .config("spark.sql.warehouse.dir", "file:///${system:user.dir}/spark-warehouse")
      .getOrCreate()

    import spark.implicits._

    val bizCustStream = spark
      .readStream
      .format("csv")
      .option("header", "false")
      .option("sep", "\t")
      .schema(new StructType()
        .add("firstName", "string")
        .add("lastName", "string")
        .add("companyName", "string")
        .add("address", "string")
        .add("city", "string")
        .add("province", "string")
        .add("postal", "string")
        .add("phone1", "string")
        .add("phone2", "string")
        .add("email", "string")
        .add("web", "string"))
      .load("resources/source_data/biz")
//      .as[CustomerRow]

//    val cifXrefStream = spark
//      .readStream
//      .format("csv")
//      .option("header", "false")
//      .option("sep", "\t")
//      .schema(new StructType()
//        .add("XrefSystem", "string")
//        .add("XrefId", "string")
//        .add("CIFId", "int"))
//      .load("examples/spark_streaming/source_data/cif/xref")
//      .as[XrefRow]
//      .filter(_.XrefSystem == "biz")

    val query = bizCustStream
//      .join(
//        cifXrefStream,
//        bizCustStream("email") === cifXrefStream("XrefId"),
//        "left_outer")
      .writeStream
      .outputMode("append")
      .format("console")
      .start()

    query.awaitTermination()
  }
}
