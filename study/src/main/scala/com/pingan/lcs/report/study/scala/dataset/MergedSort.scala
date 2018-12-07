package com.pingan.lcs.report.study.scala.dataset
/**
 * Author: 	Wang Jialin
 * Contact Information:
 * 	WeChat:	18610086859
 *  QQ:		1740415547
 * 	Email: 18610086859@126.com
 *  Tel:	18610086859
 */
object MergedSort {

  def main(args: Array[String]){
     
    def mergedsort[T] (less: (T, T) => Boolean) (input: List[T]): List[T] = {
    /**
     * @param xList Ҫ�ϲ��������б�
     * @param yList Ҫ�ϲ��������б�
     * @return �ϲ�����б�
     */
    def merge(xList: List[T], yList: List[T]): List[T] =
      (xList, yList) match {
      case (Nil, _) => yList
      case (_, Nil) => xList
      case (x :: xtail, y :: ytail) =>
        if (less(x, y)) x :: merge(xtail, yList)
        else y :: merge(xList, ytail)
    }
    val n = input.length / 2
    if (n == 0) input
    else {
      val (x, y) = input splitAt n //��Ҫ������б�inputƽ���ֳ������б�
      merge(mergedsort(less)(x), mergedsort(less)(y))  //�ȶԷֺ�������б�鲢�����ٶ��źõ��������й鲢
    }
  }
    println(mergedsort((x: Int, y: Int) => x < y) (List (3, 7, 9, 5)))
    val reversed_mergedsort=mergedsort((x: Int, y: Int) => x > y) _
    println(reversed_mergedsort(List(3, 7, 9, 5)))
    
    
    
    
    
}
}