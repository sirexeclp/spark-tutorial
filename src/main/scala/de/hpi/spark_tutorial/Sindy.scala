package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

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

     for (dataset <- data){
       val column_names = dataset.columns
        for (row:Row <- dataset.collect()) {
          for (j <- row.toSeq.indices) {
            results += ((row.getString(j), column_names(j)))
          }
        }
     }

     //val test = data.map(dataset => (dataset, dataset.columns))

     //print(data.count())
      //data.show(100)

  }
}
