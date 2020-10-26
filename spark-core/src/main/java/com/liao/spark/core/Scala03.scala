package com.liao.spark.core

/**
 * @author liao
 * @since 2020/10/23 13:22
 */
object Scala03 {

  def main(args: Array[String]): Unit = {
    ScalaSingleton.sayHello()

    val liaoStu = new Stu(name = "liao", age = 10)
    liaoStu.gender = "man"
    // 使用辅助构造器进行构造
    val wang = new Stu("wang", 10, "man")
    //    val student = new Student2("de", 12)

    val student: Student2 = Student2.makeInstance()
    val student1: Student2 = Student2.makeInstance()
    println(student == student1)
  }

  /**
   * 在 Scala 中，是没有 static 这个东西的，但是它也为我们提供了单例模式的实现方法，那 就是使用关键字 object,object 对象不能带参数。
   */

  object ScalaSingleton {
    def sayHello(): Unit = {
      println("hello world")
    }
  }


}


/*
 * scala中，类并不用声明为public
 * 如果你没有定义构造器，类会有一个默认的空参构造器
 *
 * var 修饰的变量，对外提供setter、getter方法
 * val 修饰的变量，对外只提供getter方法，不提供setter方法
*/

class Student {
  // var 能够使用'_'占位符 表示数据不确定 val不能使用 必须明确的初始化数据
  var stuName: String = _

  val stuAge: Int = 10


}


/**
 *
 * 定义在类后面的为类主构造器, 一个类可以有多个辅助构造器 如果需要构造辅助构造器就必须构造主构造器
 * 定义在类名称后面的构造器为主构造器
 * 类的主构造器中的属性会定义成类的成员变量
 * 类的主构造器中属性没有被var|val修饰的话，该属性不能访问，相当于对外没有提供get方法
 * 如果属性使用var修饰，相当于对外提供set和get方法
 *
 * @param name 姓名
 * @param age  年龄
 */
class Stu(var name: String, var age: Int) {
  var gender: String = _

  def this(name: String, age: Int, gender: String) {
    this(name, age)
    this.gender = gender
  }
}


/**
 * private 加在主构造器之前，这说明该类的主构造器是私有的，外部对象或者外部类不能访问
 * 也适用与辅助构造器
 */

class Student2 private(var name: String, val age: Int) {
  var gender: String = _

  private def this(name: String, age: Int, gender: String) {
    this(name, age)
    this.gender = gender
  }
}

object Student2 {

  private var stu: Student2 = _

  def makeInstance(): Student2 = {
    if (stu == null) {
      stu = new Student2(name = "10", age = 10)
    }
    stu
  }


  /*
   * private val age
   * age 在本类中有setter和getter方法
   * 但是加上private 也就意味着age只能在这个类的内部及其伴生类中可以修改
   */
  class Student3 private() {
    private var name: String = _
    // 伴生类可以访问
    private var age: Int = _
    // private [this]关键字标识给属性只能在类内部访问，伴生类不能访问
    private[this] var gender: String = "man"
  }

  // 伴生类
  object Student3 {
    private val student = new Student3()
    student.name = "liao"
    student.age = 10
    //    student.gender // 访问不到


  }

  //  类包的访问权限

  /*
 * private [this] class放在类声明最前面，是修饰类的访问权限，也就是说类在某些包下可见或不能访问
 * private [sheep] class代表该类在sheep包及其子包下可见，同级包不能访问
 */
  private[this] class Student4(val name: String, private var age: Int) {
    var gender: String = _
  }

  /**
   * Scala Trait(特质) 相当于 Java 的接口，实际上它比接口还功能强大。
   * 与接口不同的是，它还可以定义属性和方法的实现。
   * 一般情况下 Scala 的类只能够继承单一父类，但是如果是 Trait(特质) 的话就可以继承多个，
   * 实现了多重继承。使用的关键字是 trait。
   */
  trait ScalaTrait {
    // 定义一个属性
    var pro: Int = 15

    // 定义一个接口
    def sayHello()


    // 定义一个实现的方法
    def sayHi(): Unit = {
      println("hi")
    }

  }

  trait ScalaSum {
    def sum(p: Int*): Int
  }

  /**
   * 使用with 实现多继承
   */
  class ScalaDemo extends ScalaTrait with ScalaSum {
    override def sayHello(): Unit = {
      println("hello")
    }

    override def sum(p: Int*): Int = {
      var sum = 0
      p.foreach((a: Int) => {
        sum += a
      })
      sum
    }
  }


}




