package com.pingan.lcs.report.study.scala.dataset
/**
 * Author: 	Wang Jialin
 * Contact Information:
 * 	WeChat:	18610086859
 *  QQ:		1740415547
 * 	Email: 18610086859@126.com
 *  Tel:	18610086859
 */
object List_Fold_Sort {

  def main(args: Array[String]){
    println((1 to 100).foldLeft(0)(_+_) )
    println((0 /: (1 to 100))(_+_))
    
    println((1 to 5).foldRight(100)(_-_))
    println(((1 to 5):\100)(_-_))
    
    
    println(List(1, -3, 4, 2, 6) sortWith (_ < _))
    println(List(1, -3, 4, 2, 6) sortWith (_ > _))
    
  }

}