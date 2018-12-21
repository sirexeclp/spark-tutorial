package de.hpi.spark_tutorial

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions._

object Sindy {

	def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
		import spark.implicits._
		val data = inputs.map(
			spark.read
					.option("inferSchema", "false")
					.option("header", "true")
					.option("delimiter", ";")
					.csv(_)
		) // as data frames

		val tupels = data.map(df => {
			val headers = df.columns.toSeq
			df.flatMap(row => row.toSeq.asInstanceOf[Seq[String]].zip(headers).map( x=>(x._1,x._2) ) )
		})
		.reduce(_.union(_))
		//.repartition(32)

		val cells = tupels
				.groupBy($"_1")
				.agg(collect_set($"_2").alias("tupels"))

		val inclList =cells
			.select(explode($"tupels"), $"tupels")
			.as[(String,Seq[String])]
			.map( x => (x._1,x._2.filter(_!=x._1)))
			.groupBy($"_1")
			.agg(collect_set($"_2"))
			.as[(String,Seq[Seq[String]])]
			.map(x => (x._1,x._2.reduce(_.intersect(_))))
			.filter(x=> x._2.length > 0)
			.foreach(r => println(r._1+" < "+r._2.reduce(_+", "+_)) )
	}
}
