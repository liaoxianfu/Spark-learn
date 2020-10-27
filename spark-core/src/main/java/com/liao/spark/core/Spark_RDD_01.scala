package com.liao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author liao
 * @since 2020/10/24 16:22
 */
object Spark_RDD_01 {

  /**
   * 创建RDD的方式
   */

  def createRDD(): Unit = {


    // 创建sparkConf
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")

    // 创建sc
    val sc = new SparkContext(conf)


    // 内存中创建
    // 使用 parallelize 创建
    val rdd1: RDD[Int] = sc.parallelize(Seq(1, 2, 3, 4, 5))
    rdd1.foreach(println)
    // 获取分区大小 如果不显式指定分区的大小默认是cpu的核心数作为分区的大小
    val partSize: Int = rdd1.partitions.length
    println("分区大小" + partSize)

    // 2 使用makeRDD创建 底层其实就是调用parallelize() 方法
    val rdd2: RDD[Int] = sc.makeRDD(Seq(1, 2, 3, 4))
    rdd2.foreach(println)


    // 外部文件创建
    // 从外部存储系统的数据集创建
    // 本地文件系统
    val lines: RDD[String] = sc.textFile("datas")
    lines.foreach((line: String) => println(line))

    // 从hdfs文件系统读取 当然如果在使用集群中使用yarn模式 无需指定为hdfs
    val lines2: RDD[String] = sc.textFile("hdfs://hadoop201:9000/data")
    lines2.foreach((line: String) => println(line))


    // 带文件路径的创建
    val lines3: RDD[(String, String)] = sc.wholeTextFiles("hdfs://hadoop201:9000/data")
    lines3.foreach((fileWithPath: (String, String)) => println(fileWithPath))
    // 关闭sc
    sc.stop()

  }


  /**
   * 分区测试
   */
  def partition(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("partition")
    val sc = new SparkContext(conf)


    // 指定分区数后 不管文件的内容是否足够分成该区的大小都会产生这么多的分区数、
    val rdd1: RDD[String] = sc.textFile("datas", minPartitions = 5)
    rdd1.saveAsTextFile("output")

    // 默认分区 当文件的数量>=当前cpu的核心数时 按照核心的数量进行分区 当<当前cpu核心数时 取 核心数和2的最小值
    val rdd2: RDD[String] = sc.textFile("datas", minPartitions = 5)
    rdd2.saveAsTextFile("output")


    // 关闭SparkContext
    sc.stop()


  }


  /**
   * RDD整体分为Value类型，双Value类型和Key-Value类型
   */
  def transformationDemo(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transformationDemo")
    val sc = new SparkContext(conf)

    // map 算子 与scala中的map功能基本相同
    // 函数原型   def map[U: ClassTag](f: T => U): RDD[U] = withScope
    // 参数f是一个函数 接收每一个参数 当某个RDD执行map方法时会遍历RDD中的每个数据并应用map中的f函数
    // 并产生新的rdd
    val arrNew: Array[Int] = sc.makeRDD(Array(1, 2, 3, 4, 5)).map((x: Int) => x + 1).collect()
    arrNew.foreach((x: Int) => print(x + " "))


    // MapPartitions算子
    // Map算子一次处理一个元素 而mapPartition算子一个处理一个分区的数据
    // 将数组1,2,3,4分成2个分区 每个分区使用mapPartitions进行处理
    val str: String = sc.makeRDD(Array(1, 2, 3, 4), numSlices = 2).mapPartitions((x: Iterator[Int]) => x.map((_: Int) * 2)).collect().mkString(",")
    println(str)


    // mapPartitionsWithIndex
    // 类似mapPartition但是多了一个整数参数表示分区号'
    // 例如 要求将每个分区中的元素与所在的分区编号组成一个元组
    val tuples: Array[(Int, Int)] = sc.makeRDD(Array(1, 2, 3, 4), numSlices = 2).mapPartitionsWithIndex(
      (index: Int, items: Iterator[Int]) => items.map((index, _: Int))
    ).collect()
    println(tuples.toBuffer)

    // 要求 将第二个分区中的元素*2 其他的不变
    val array: Array[Int] = sc.makeRDD(Array(1, 2, 3, 4), numSlices = 2).mapPartitionsWithIndex((index: Int, items: Iterator[Int]) => {
      println(index)
      if (index == 1) {
        items.map((x: Int) => x * 2)
      } else {
        items
      }
    }).collect()
    println(array.toBuffer)


    // flatMap()
    // 与scala中的基本一致
    val listRDD: RDD[List[Int]] = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6), List(7)), 2)
    listRDD.flatMap((list: Seq[Int]) => list).collect().foreach(println)

    //    glom 算子
    //    该操作将RDD中的每个分区变换为一个分区并放置在新的RDD中，数组中的元素类型与原分区中的元素保持一致。
    //    例： 创建两个RDD分区 并将每个分区的数据放到一个数组 求出每个分区的最大值

    listRDD.flatMap((list: Seq[Int]) => list).glom().map((arr: Array[Int]) => arr.max).collect().foreach(println)

    // groupBy 算子
    // 与scala基本一致
    val arrRdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), numSlices = 2)
    val array1: Array[(Int, Iterable[Int])] = arrRdd.groupBy((_: Int) % 2).collect()
    println(array1.toBuffer)
    val rdd1: RDD[String] = sc.makeRDD(List("hello", "hive", "hadoop", "spark", "scala"))
    rdd1.groupBy((s: String) => s.substring(0, 1)).collect().foreach(println)

    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("Hello Scala", 2), ("Hello Spark", 3), ("Hello World", 2)))

    //    rdd.map((line: (String, Int)) => {
    //      val newArr = new ListBuffer[String]()
    //      for (_ <- 0 to line._2) {
    //        newArr ++= line._1.split(" ")
    //      }
    //      newArr.map((w: String) => (w, 1)).toList
    //    }).flatMap((w: Seq[(String, Int)]) => w).reduceByKey((_: Int) + (_: Int)).collect().foreach(println)

    rdd.flatMap((kv: (String, Int)) => {
      val arrs = new ListBuffer[String]()
      for (_ <- 0 to kv._2) {
        arrs ++= kv._1.split(" ")
      }
      arrs
    })
      .map((w: String) => (w, 1))
      .reduceByKey((_: Int) + (_: Int))
      .collect()
      .foreach(println)

    // filter 与scala 基本一致

    // 统计rdd中去除hello的数量
    rdd.flatMap((kv) => {
      val arrs = new ListBuffer[String]()
      for (_ <- 0 to kv._2) {
        arrs ++= kv._1.split(" ")
      }
      arrs
    })
      .filter((w: String) => !w.equals("Hello"))
      .map((w: String) => (w, 1))
      .reduceByKey((_: Int) + (_: Int)).collect()
      .foreach(println)

    // distinct去重
    val distinctRdd: RDD[Int] = sc.makeRDD(List(1, 2, 1, 5, 2, 9, 6, 1))

    // 3.2 打印去重后生成的新RDD
    distinctRdd.distinct().collect().foreach(println)

    // 3.3 对RDD采用多个Task去重，提高并发度
    distinctRdd.distinct(2).collect().foreach(println)

    // sample函数
    // 从大量数据中进行采样

    val sampleRDD: RDD[Int] = sc.makeRDD(1 to 10, 2)
    sampleRDD
      .sample(withReplacement = false, 0.3, 3)
      .collect()
      .foreach(println)




    // 关闭sc
    sc.stop()
  }


  /**
   *
   *
   **/

  def transformationDemo2(): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("transformationDemo2")
    val sc = new SparkContext(config = sparkConf)
    val disRDD: RDD[Int] = sc.makeRDD(List(1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 3, 3, 3, 3, 3), 4)
    disRDD.distinct(4)
      .coalesce(1) // 缩减分区
      .collect()
      .foreach(println)
  }


  def doubleValue(): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("doubleValue")
    val sc = new SparkContext(config = conf)
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
    val rdd2: RDD[Int] = sc.makeRDD(List(2, 3, 4, 5, 6))
    // 求并集
    print("并集：")
    rdd1.union(rdd2).collect().foreach(show)
    println()


    // 求交集
    print("交集:")
    rdd1.intersection(rdd2).collect().foreach(show)
    println()
    //    差集
    print("差集：")
    rdd1.subtract(rdd2).collect().foreach(show)
    println()


    /**
     * zip函数 与python中的zip类似
     * 想要两个rdd进行zip必须保证两个rdd的元素个数和分区数保持相同 缺一不可
     **/

    val rdd3: RDD[String] = sc.makeRDD(List("A", "B", "C"))
    val rdd4: RDD[Int] = sc.makeRDD(List(1, 2, 3))
    rdd3.zip(rdd4).collect().foreach(println)


    sc.stop()
  }


  def show(data: AnyVal): Unit = {
    print(data + " ")
  }


  def keyValueDemo(): Unit = {
    //    partitionBY 通过key进行进行重新分区

    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("keyValueDemo")
    val sc = new SparkContext(conf)
    val partRdd: RDD[(Int, String)] = sc.makeRDD(List((1, "aaa"), (2, "bbb"), (3, "ccc")))

    val size: Int = partRdd.partitionBy(new MyPartition(2)).partitions.length
    println(size)

    sc.stop()
  }


  def adDemo(): Unit = {
    import scala.collection.mutable.ArrayBuffer
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("ad")
    val sc = new SparkContext(conf)
    //    读取文件
    val lines: RDD[String] = sc.textFile("ad")

    val value: RDD[((String, String), Int)] =
      lines.map((line: String) => line.split(" ")) // 将每一行通过空格进行分割
        .map((arr: Array[String]) => ((arr(1), arr(4)), 1)) // 获取省和广告名称作为key value=1
        .reduceByKey((_: Int) + (_: Int)) // 通过key进行累加 获取相同省的相同广告的点击量
    val rdd3: RDD[(String, Iterable[(String, Int)])] =
      value.map((w: ((String, String), Int)) => (w._1._1, (w._1._2, w._2))) // 将key变为省 value=(广告名称,点击量)
        .groupByKey() // 分组聚合
        .map((w: (String, Iterable[(String, Int)])) => (w._1, w._2.slice(0, 3))) // 获取广告点击前3
    rdd3.collect().foreach(println)
  }


  def main(args: Array[String]): Unit = {
    adDemo()
  }


}


class MyPartition(val num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    key match {
      case v: Int =>
        if (v % 2 == 1) {
          return 1
        }
      case _ => return 0
    }
    return 0
  }
}