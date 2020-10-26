package com.liao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

/**
 * @author liao
 * @since 2020/10/22 17:15
 */
object WordCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("wordCount")
    val context = new SparkContext(conf)
    val lines: RDD[String] = context.textFile("datas")

    //    val value: RDD[(String, Int)] = lines.flatMap((w: String) => w.split(" "))
    //      .groupBy((x: String) => x)
    //      .map((x: (String, Iterable[String])) => (x._1, x._2.toList.length))
    //    value.foreach(println)
    //    lines.flatMap((w: String) => w.split(" "))
    //      .map((s: String) => (s, 1))
    //      .groupBy((x: (String, Int)) => x._1)
    //      .map((e: (String, Iterable[(String, Int)])) => (e, e._2.toList))
    //      .mapValues((q: immutable.Seq[(String, Int)]) => q.length)
    //      .foreach(println)

    lines.flatMap((w: String) => w.split(" "))
      .map((s: String) => (s, 1))
      .groupBy((x: (String, Int)) => x._1)
      .map((x: (String, Iterable[(String, Int)])) => {
        x._2.reduce((t1, t2) => {
          (t1._1, t1._2 + t2._2)
        })

      }).foreach(println)


    lines.flatMap((w: String) => w.split(" "))
      .map((s: String) => (s, 1))
      .reduceByKey((_: Int) + (_: Int)).foreach(println)

    val array: Array[(String, Int)] = lines.flatMap((x: String) => x.split(" "))
      .map(((_: String), 1)).reduceByKey((_: Int) + (_: Int)).collect()
    lines.flatMap((x: String) => x.split(" "))
      .map(((_: String), 1))
    println(array.toBuffer)






    //    val words: RDD[String] = lines.flatMap((_: String).split(" "))
    //    val wordGroupBy: RDD[(String, Iterable[String])] = words.groupBy((word: String) => word)

  }
}