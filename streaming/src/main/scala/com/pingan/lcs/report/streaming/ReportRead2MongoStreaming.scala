package org.apache.spark.examples.streaming

import com.alibaba.fastjson.JSON
import com.mongodb.spark._
import com.mongodb.spark.sql._
import com.pingan.lcs.report.streaming.StreamingExamples
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka010._

/**
  * Consumes messages from one or more topics in Kafka and does wordcount.
  * Usage: DirectKafkaWordCount <brokers> <topics>
  * <brokers> is a list of one or more Kafka brokers
  * <groupId> is a consumer group name to consume from topics
  * <topics> is a list of one or more kafka topics to consume from
  *
  * Example:
  * $ bin/run-example streaming.DirectKafkaWordCount broker1-host:port,broker2-host:port \
  * consumer-group topic1,topic2
  */
object ReportRead2MongoStreaming {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(
        s"""
           |Usage: DirectKafkaWordCount <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <groupId> is a consumer group name to consume from topics
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    StreamingExamples.setStreamingLogLevels()

    val Array(brokers, groupId, topics) = args


    //    val sc = new SparkConf().setAppName("DirectKafkaWordCount")
    //      .setMaster("local")
    //      .set("spark.", "org.apache.spark.serializer.KryoSerializer")
    //      .set("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/report.staff_info")
    //      .set("spark.mongodb.keep_alive_ms", "10000000")
    val session: SparkSession = SparkSession.builder().appName("DirectKafkaWordCount")
      .master("local")
      .config("spark.", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/report.staff_info")
      .config("spark.mongodb.keep_alive_ms", "10000000").getOrCreate()
    val ssc = new StreamingContext(session.sparkContext, Seconds(1))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    // Get the lines, split them into words, count the words and print

    messages.foreachRDD(messageRdd => {
      import session.implicits._
      val staffInfoDF: DataFrame = messageRdd.map(message => {
        val dataStr: String = JSON.parseObject(message.value).getString("data")
        val info: StaffInfo = JSON.parseObject(dataStr.substring(1, dataStr.length - 1), classOf[StaffInfo])

        info
      }).toDF()
      staffInfoDF.write.mode("append").mongo()
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

// scalastyle:on println
case class StaffInfo(
                      deptno: String,
                      empno: String,
                      emp_name: String,
                      sex: String,
                      birth_date: String,
                      id_type: String,
                      emp_idno: String,
                      hire_date: String,
                      reg_date: String,
                      leave_date: String,
                      fcd: String,
                      fcu: String,
                      pk_serial: String
                    )
