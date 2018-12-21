package de.hpi.spark_tutorial

import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.udf

object Sindy {

	def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
		import spark.implicits._
		val data = inputs.map(
			spark.read
					.option("inferSchema", "false")
					.option("header", "true")
					.option("delimiter", ";")
					.csv(_)
		) // as data sets
		println("data read")
		val results = ListBuffer[(String, String)]()

	//	for (dataset <- data) {
	//		val column_names = dataset.columns
	//		for (row: Row <- dataset.collect()) {
	//			for (j <- row.toSeq.indices) {
	//				results += ((row.getString(j), column_names(j)))
	//			}
	//		}
		//}
		//data(0).columns.toSeq.foreach(println)
	//	val headers= data(0).columns.toSeq
	//	println(headers)
	//	val test = data(0).flatMap(r => r.toSeq.asInstanceOf[Seq[String]].zipWithIndex)
	//		test.show(100)
	//	test.map(x=> (x._1, headers)).show(100)
	//	data(0).flatMap( row => row.toSeq.asInstanceOf[Seq[String]].zip(data(0).columns.toSeq)).show(100)
	//	data(0).flatMap( (row:Row) => row.toSeq.asInstanceOf[Seq[String]].zip(data(0).columns.toSeq)).show(100)

		val tupels = data.map(df => {
			val headers = df.columns.toSeq
			df.flatMap(row => row.toSeq.asInstanceOf[Seq[String]].zip(headers).map( x=>(x._1,x._2) ) )
		})
		.reduce(_.union(_))//.show(100)
	//		frame=>frame.map(row => row. )
		//)//
		//.foreach(_.show(100))



//
		//println("1")
		//val resDF = results.toDF.repartition(32)
		//println("2")
		val cells = tupels
				.groupBy($"_1")
				.agg(collect_set($"_2").alias("tupels"))
		//cells.show(100)
		println("3")
		val inclList =cells
			.select(explode($"tupels"), $"tupels")
			//.groupBy($"col")
			//.agg(collect_set($"tupels"))
			.as[(String,Seq[String])]
			.map( x => (x._1,x._2.filter(_!=x._1)))
			.groupBy($"_1")
			.agg(collect_set($"_2"))
			.as[(String,Seq[Seq[String]])]
			.map(x => (x._1,x._2.reduce(_.intersect(_))))
			.filter(x=> x._2.length > 0)
			.foreach(r => println(r._1+" < "+r._2.reduce(_+", "+_)) )
			//.groupBy($"col")
			//.agg(collect_set($"collect_set(collect_set(_2))"))
			//.filter(_.getSeq(1).length > 1)

	//	inclList.show(100, false)


		//val flatten = udf((xs: Seq[Seq[String]]) => xs.flatten)
		//val resDF = spark.createDataFrame(results)
		// resDF.groupBy($"_1").agg(collect_set($"_2"))
		//.select(explode($"collect_set(_2)"), $"collect_set(_2)").groupBy($"col").agg(collect_set($"collect_set(_2)"))//.map(r => r.getSeq(1).filter(_!=r.getString(0)) )
		// .show(100,false)
		//.groupBy($"collect_set(_2)").agg(collect_set($"_1")).filter(_.getSeq(0).length > 1)

		//val test = data.map(dataset => (dataset, dataset.columns))

		//print(data.count())
		//data.show(100)

	}
}
