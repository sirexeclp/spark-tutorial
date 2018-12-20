package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.spark.sql.functions.explode

object Sindy{

   def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {

     val data = inputs.map(
       spark.read
         .option("inferSchema", "false")
         .option("header", "true")
         .option("delimiter", ";")
         .csv(_)
     ) // as data sets

    val results = ListBuffer[(String,String)]()
     import spark.implicits._
     for (dataset <- data){
       val column_names = dataset.columns
        for (row:Row <- dataset.collect()) {
          for (j <- row.toSeq.indices) {
            results += ((row.getString(j), column_names(j)))
          }
        }
     }
     import org.apache.spark.sql.functions.udf
     import org.apache.spark.sql.functions._
     val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten)
     val resDF = spark.createDataFrame(results)
       resDF.groupBy($"_1").agg(collect_set($"_2")).groupBy($"collect_set(_2)").agg(collect_set($"_1")).filter(_.getSeq(0).length > 1).show(100)

     //val test = data.map(dataset => (dataset, dataset.columns))

     //print(data.count())
      //data.show(100)

  }
}
