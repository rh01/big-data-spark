package main.scala.basic

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  /**
    * 远程调试spark集群应用，实现一个wordcount的应用，统计单词及对应的出现次数
    * @param args
    */
  def main(args: Array[String]) {
    val inputFile =  "hdfs://hw:8020/test/*"
    /** 查找spark master的URL，并且设置在submit时的jar包位置，即包含源码的jar包*/
    val conf = new SparkConf().setAppName("WordCount").setMaster("spark://192.168.59.150:7077")
      .setJars(List("F:\\06_workshop\\0402\\WordCount\\out\\artifacts\\WordCount_jar\\WordCount.jar"))
    /** 定义上下文 */
    val sc = new SparkContext(conf)
    /** 读取文件系统的内容，这里文件系统可以时HDFS也可以是本地文件系统*/
    val textFile = sc.textFile(inputFile)
    /** 这里是transform操作*/
    val wordCount = textFile.flatMap(_.split(" ")).map((_, 1)).reduceByKey( _ + _)
    //wordCount.foreach(println)
    /** 这里是actor的操作*/
    val result = wordCount.collect()
    /** 遍历输出 */
    for (res <- result){
      println(res)
    }
    /** 输出多少个不重复的记录数目*/
    println(wordCount.count())
  }
}
