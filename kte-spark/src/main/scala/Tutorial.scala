import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark


/**
 * Created by gregoire on 26/04/16.
 */
object Tutorial {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val conf = new SparkConf().setAppName("tutorial-kafka")
    conf.setMaster("local[1]")
    val ssc = new StreamingContext(conf, Seconds(20))

    ssc.checkpoint("checkpoint")

    val sRDD = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map[String, String]("metadata.broker.list" -> "localhost:9092"), Set[String]("connect-test"))

    val payloadRDD = sRDD.map(_._2).map(_.split("payload\":\"")(1).dropRight(2))

    case class SoloString(val input: String) {}

    payloadRDD.map(s => SoloString(s)).foreachRDD(rdd => EsSpark.saveToEs(rdd, "test_streaming/type_streaming"))
    payloadRDD.foreachRDD(_.foreach(println))

    ssc.start()

    ssc.awaitTermination()

  }

  def run(): Unit = {

  }


}
