package com.gett.s2udfs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * Created by liorbaber on 03/07/2017.
  */
object App {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().master("local[1]").getOrCreate()

    import spark.implicits._

    val df = Seq((1,32.118109, 34.818770),(2,32.113774, 34.830679)).toDF("id","lat","long")

    df.select($"id",explode(S2Udfs.latLongToS2CellIDs($"lat",$"long",lit(10),lit(10))).alias("cell_token")).withColumn("cell_token_from_radius",explode(S2Udfs.s2CellCoverageByRadius(col("cell_token"),lit(10.0)))).show()




  }

}
