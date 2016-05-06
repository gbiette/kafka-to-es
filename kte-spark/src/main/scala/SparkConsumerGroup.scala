import _root_.util.CustomUtils._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.elasticsearch.spark.rdd.EsSpark

import scala.xml.XML


/**
  * Created by gregoire on 26/04/16.
  */
object SparkConsumerGroup {

  val zkQuorum = "localhost:2181"
  val group = "spark"
  val topicMap = Map[String, Int]("test-multi" -> 1)

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val (estype, mapping) = loadMapping("/mapping_piwik.json")

    initiateIndex("local_greg", "piwik_kafka_stream", estype, mapping)


    val conf = new SparkConf().setAppName("piwik-kafka")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("checkpoint")

    val sRDD = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)

    sRDD.foreachRDD(_.foreach(println))

    ssc.start()

    ssc.awaitTermination()

  }


}
