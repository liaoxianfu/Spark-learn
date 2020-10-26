package com.liao.spark.core

/**
 * @author liao
 * @since 2020/10/22 17:31
 */
object Scala01 {
  def main(args: Array[String]): Unit = {
    // 变量定义
    // 定义常量
    val name = "tom" // 不可更改 相当于final
    // 定义变量
    var age = 18 // 变量可更改
    var age02: Int = 1 // 可以显式指定变量类型
    // 字符串格式化输出
    println("name=" + name)
    // f 插值器 所有变量引用都应该是printf样式格式说明符，如％d ,％i ,％f等
    println(f"age=$age%d")
    // s 插值器 可以直接将String类型的数据插入 无需显示指定类型
    println(s"name=$name")

    // 条件表达式
    var res = if (age > 14) "老了" else "年轻"
    println(res)

    // 多级别表达
    if (age < 10) {
      println("年幼")
    } else if (age >= 10 && age < 20) {
      println("青年")
    } else {
      println("老年")
    }

    // 循环
    val arr: Array[Int] = Array(1, 2, 3, 4, 5, 6)
    for (ele <- arr) {
      println(ele)
    }

    // 范围集合 相当于python range 左闭右开
    for (i <- 0 to 5) {
      println(i)
    }

    // for循环中可以增加守卫，下面这段语句是打印arr数组中的偶数
    for (i <- arr if i % 2 == 0) {
      println(i)
    }
    println("-----------------------------------------")
    //    for (i <- 0 to 5; j <- 0 to 5; k <- 0 to 5 if i != j && j != k) {
    //      println("i=" + i, "j=" + j, "k=" + k)
    //      println(f"i=$i%d, j=$j%d")
    //    }

    for (i <- 0 to 5) {
      for (j <- 0 to 5) {
        if (i != j) {
          println(f"i=$i%d,j=$j%d")
        }
      }
    }
    val arr2: Array[Int] = for (e <- arr if e % 2 == 0)
      yield e

    for (j <- arr2) {
      println(j)
    }


    // 定义方法
    def sum(a: Int, b: Int): Int = {
      if (a + b > 0) a + b
      else -1
    }

    val num: Int = sum(1, 2)
    println(num)
    // 定义函数
    val function0: (Int, Int) => Int = (x: Int, y: Int) => {
      x * y
    }
    // 不能指定返回值的类型
    val function1 = (x: Int, y: Int) => {
      x * 10
    }
    println(function0(1, 2))
    println(function1(3, 4))


    // 高阶函数
    def currentTime(): Long = {
      System.nanoTime()
    }

    // 调用无参数
    def showTime(f: () => Long): Unit = {
      println(f())
    }

    // 等价于
    def showTime2(time: Long): Unit = {
      println(time)
    }

    showTime(currentTime)

    // 带参数
    def showSum(f: (Int, Int) => Int, a: Int, b: Int): Unit = {
      println(f(a, b))
    }

    showSum(sum, 2, 3)


    // 可变参数
    def methodManyParams(params: Int*): Unit = {
      for (elem <- params) {
        println(elem)
      }
    }

    methodManyParams(1, 2, 3, 4, 5)

    // 默认参数

    def defaultParams(a: Int = 2, b: Int = 3): Unit = {
      println(a + b)
    }
    // 传递的值
    defaultParams(1, 20)
    // 默认值
    defaultParams()

    // 参数应用函数
    /**
     * 如果函数传递所有预期的参数，
     * 则表示已完全应用它。 如果只传递几个参数并不是全部参数，
     * 那么将返回部分应用的函数。这样就可以方便地绑定一些参数，
     * 其余的参数可稍后填写 补上。
     */
    // 调用sum函数的时候，传入了一个参数a=10，但是b为待定参数。
    // sumWithTen形成了一个新的函数，参数个数为一个，类型为int型，
    def sumWithTen: Int => Int = sum(10, _: Int)

    println(sumWithTen(2)) // sum(10,2)

    // 柯里化
    def add(a: Int): Int => Int = {
      (b: Int) => a + b
    }

    // 偏函数
    /*
    被包在花括号内没有 match 的一组 case 语句是一个偏函数，
    它是 PartialFunction[A,B]的一个 实例，A 代表参数类型，
    B 代表返回类型，常用作输入模式匹配。
     */

    def fun1: PartialFunction[String, Int] = {
      case "one" => 1
      case "two" => 2
      case _ => -1
    }

    println(fun1("one"))














  }
}
