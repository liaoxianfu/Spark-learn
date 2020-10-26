package com.liao.spark.core

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.parallel.immutable.ParSeq

/**
 * @author liao
 * @since 2020/10/22 19:26
 */
object Scala02 {
  def main(args: Array[String]): Unit = {
    /**
     * Scala 的集合有三大类：序列 Seq、集 Set、映射 Map，
     * 所有的集合都扩展自 Iterable 特质
     * 在 Scala 中集合有可变（mutable）和不可变（immutable）两种类型，
     * immutable 类型的集合 初始化后就不能改变了（注意与 val 修饰的变量进行区别。
     */
    // 定长数组
    val arr = new Array[Int](8)
    // 直接打印是hashcode值
    println(arr)
    // 转换为数组缓冲可以打印
    println(arr.toBuffer)

    // 初始化并指定数据
    val arr2: Array[String] = Array[String]("hello", "world", "你好")
    // 通过索引获取数值
    println(arr2(1))

    // 创建可变长数组
    val arrs: ArrayBuffer[Int] = ArrayBuffer[Int]()
    // 末尾追加
    arrs += 1
    // 追加多个元素
    arrs += (1, 2, 3)
    // 追加数组
    arrs ++= Array[Int](4, 5, 6)
    // z追加一个可变长数组
    arrs ++= ArrayBuffer[Int](7, 8)
    // 打印
    arrs.foreach((a: Int) => print(a + " "))
    println()

    // 移除指定的元素
    arrs.remove(0, 1)
    println(arrs)


    // map 将数组中所有元素进行某种映操作


    // 第一种写法
    val arrayBuffer: ArrayBuffer[AnyVal] = arrs map ((x: Int) => {
      if (x % 2 == 1) x
      else -1
    })

    println(arrayBuffer)

    // 第二种写法
    val mapArrs: ArrayBuffer[Int] = arrs.map((x: Int) => x * 2)
    mapArrs.foreach((a: Int) => print(a + " "))

    // 第三种
    // 亦或者，_表示入参，代表数组中每个元素
    arrs.map((_: Int) * 2)


    val words: Array[String] = Array("tom jack oliver jack", "hello tom oliver tom ")
    //    此时数组中的每个元素进行split操作之后变成了Array,
    val wordArr: Array[Array[String]] = words.map((word: String) => word.split(" "))

    // 进行扁平化操作
    val flatten: Array[String] = wordArr.flatten
    println()
    flatten.foreach((word: String) => print(word + " "))

    // 上面的步骤可以合二为一
    val array: Array[String] = words.flatMap((_: String).split(" "))
    println()
    array.foreach((word: String) => print(word + " "))


    // Seq序列
    // 创建一个不可变集合
    val list: List[Int] = List[Int](1, 2, 3)

    // 创建一个可变序列
    val mutalist = ListBuffer(1, 2, 3)
    // 将0插入到list前面生成一个新的list
    val list1: List[Int] = list.:+(0)
    val list2: List[Int] = 0 +: list
    val list3: List[Int] = 0 :: list
    val list4: List[Int] = list.::(0)

    // 将一个元素添加到list后面，形成新的list5
    val list5: List[Int] = list :+ (4)


    val list6: List[Int] = List[Int](4, 5, 6)
    // 将一个新的列表添加到该列表中
    val list7: List[Int] = list ++ list6

    // 将list6添加到list前面
    val list8: List[Int] = list.:::(list6)
    list8.foreach((x: Int) => print(x + " "))


    // 创建可变的hashset
    val set = new mutable.HashSet[Int]()

    set += 1
    set.add(2) // 相当于+=
    // 添加一个set中的所有元素
    set ++= mutable.Set(3, 4)
    // 移出元素
    set -= 2
    set.remove(2)
    println()
    set.foreach((s: Int) => println(s))

    /**
     * 集合中常用方法
     * map
     * flatten
     * flatMap
     * filter
     * sorted
     * sortBy
     * sortWith
     * grouped
     * fold(折叠)
     * foldLeft
     * foldRight
     * reduce
     * reduceLeft
     * aggregate
     * union
     * intersect(交集)
     * diff(差集)
     * head
     * tail
     * zip
     * mkString
     * foreach
     * length
     * slice
     * sum
     */
    function1()

  }


  def function1(): Unit = {
    val list = List(1, 2, 3, 4, 5)
    // map
    list.map((x: Int) => x * 2)

    // 从小到大排序
    list.sortBy((x: Int) => x)

    // 从大到小
    list.sortBy((x: Int) => -x)
    val words: List[(String, Int)] = List(("a", 3), ("b", 5), ("c", 2))
    // 按第二个元素排列
    words.sortBy((x: (String, Int)) => x._2)

    // 按照第一个元素
    words.sortBy((x: (String, Int)) => x._1)

    // 分组

    val list2 = List(1, 2, 3, 4, 5)
    val toList: List[List[Int]] = list2.grouped(2).toList


    /**
     * 1、fold函数开始对传进的两个参数进行计算，在本例中，仅仅是做加法计算，然后返回计算的值；
     * 　　2、Fold函数然后将上一步返回的值作为输入函数的第一个参数，并且把list中的下一个item作为第二个参数传进继续计算，同样返回计算的值；
     * 　　3、第2步将重复计算，直到list中的所有元素都被遍历之后，返回最后的计算值，整个过程结束；
     */


    // fold 折叠操作
    val list1 = List(1, 2, 3, 4, 5)
    val i: Int = list1.fold(10)((_: Int) * (_: Int)) // 相当于给定一个初始值 然后让初始值与list中的值进行连乘 10*1*2*3*4*5
    // 或者
    val i1: Int = list1.fold(0)((x: Int, y: Int) => {
      x + y
    }) // 累加操作 代码开始运行的时候，初始值0作为第一个参数传进到fold函数中，list中的第一个item作为第二个参数传进fold函数中。
    //    1、fold函数开始对传进的两个参数进行计算，在本例中，仅仅是做加法计算，然后返回计算的值；
    val i2: Int = list1.foldRight(2)((_: Int) - (_: Int)) // 等价于  1-(2-(3-(4-(5-2))))
    val rdd1 = List(1, 2, 3, 4, 5)

    val i3: Int = rdd1.par.aggregate(0)((acc: Int, number: Int) => {
      val res1 = acc + number
      println("par    " + acc + " + " + number + " = " + res1)
      res1
    }, (par1: Int, par2: Int) => {
      val res2: Int = par1 + par2
      println("com    " + par1 + " + " + par2 + " = " + res2)
      res2
    })
    println(i3)

    // 交集与并集
    val list3 = List(1, 2, 3, 4, 5)
    val list4 = List(1, 3, 5, 7, 8)
    val list5: List[Int] = list3.intersect(list4) //交集
    val list6: List[Int] = list3.union(list4) // 并集
    val list7: List[Int] = list3.diff(list4) // list3中存在而list4中不存在的数据
    val head: Int = list3.head //  头 1
    val tail: List[Int] = list3.tail //  尾 2,3,4，5
    val tuples: List[(Int, Int)] = list3.zip(list4) //  List[(Int, Int)] = List((1,1), (2,3), (3,5), (4,7), (5,8))
    val str: String = list3.mkString(",") // String = 1,2,3,4,5
    list3.foreach(print)

    val length: Int = list3.length

    val list8: List[Int] = list3.slice(1, 3) // List[Int] = List(2, 3)


    /**
     * 调用集合的 par 方法, 会将集合转换成并行化集合
     */

    val par: ParSeq[Int] = list3.par

    println()
    val i4: Int = par.reduce((x, y) => {
      println("x=" + x + "    y=" + y)
      x + y
    })

    println(i4)

    /**
     * x=1    y=2
     * x=4    y=5
     * x=3    y=9
     * x=3    y=12
     * 15
     */


    /**
     * Map 和 Option
     * 在Scala中Option类型样例类用来表示可能存在或也可能不存在的值(Option的子类有Some 和 None)。Some 包装了某个值，None 表示没有值。
     */

    //    val map: Map[String, Int] = Map("a" -> 1, "b" -> 2, "c" -> 3)
    //    // 会抛出异常
    //    val i5: Int = map("d")
    //    // map的get方法返回值Option，意味着maybeInt可能取到值也可能没有取到值
    //    val maybeInt: Option[Int] = map.get("d")
    //    // 如果maybeInt=None时会抛出异常
    //    val v = maybeInt.get // v=None
    //    // 第一个参数为要获取的key
    //    // 第二个参数为如果没有这个key返回一个默认值
    //    val ele: Int = map.getOrElse("d", -1)


    /**
     *
     */

    val wordList: Array[String] = Array("hello tom jack oliver", "tom jack jim hello")
    val arrayBuffer: ArrayBuffer[String] = ArrayBuffer[String]()
    wordList.foreach(word => {
      val ss: Array[String] = word.split(" ")
      arrayBuffer ++= ss
    })
    val tuples1: ArrayBuffer[(String, Int)] = arrayBuffer.map(s => (s, 1))
    val stringToTuples: Map[String, ArrayBuffer[(String, Int)]] = tuples1.groupBy(_._1)
    val stringToInt: Map[String, Int] = stringToTuples.mapValues(_.length)
    stringToInt.foreach(println)

    val strings: Array[String] = wordList.flatMap((word: String) => word.split(" "))
    val tuples2: Array[(String, Int)] = strings.map((word: String) => (word, 1))
    val stringToTuples1: Map[String, Array[(String, Int)]] = tuples2.groupBy(w => w._1)
    val stringToInt1: Map[String, Int] = stringToTuples1.mapValues((_: Array[(String, Int)]).length)
    stringToInt1.foreach(println)


    val stringToInt2: Map[String, Int] = wordList.flatMap((word: String) => word.split(" ")).map((w: String) => (w, 1)).groupBy((_: (String, Int))._1)
      .mapValues((arrs: Array[(String, Int)]) => arrs.length)
    println(stringToInt2)

    wordList.flatMap((word: String) => word.split(" "))
      .groupBy((x: String) => x)
      .map((t: (String, Array[String])) => (t._1, t._2.length))
      .toList.foreach(println)


  }


}
