import _root_.util.CustomUtils._
import kafka.serializer.StringDecoder
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._


/**
  * Created by gregoire on 26/04/16.
  */
object SparkConsumerDirect {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val (estype, mapping) = loadMapping("/mapping_piwik.json")

    initiateIndex("local_greg", "piwik_kafka_stream", estype, mapping)


    val conf = new SparkConf().setAppName("piwik-kafka")
    conf.setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("checkpoint")

    var offsetRanges = Array[OffsetRange]()

    val sRDD = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map[String, String]("metadata.broker.list" -> "localhost:9092"), Set[String]("test-multi"))

    sRDD.transform { rdd =>
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }.foreachRDD { rdd =>
      for (o <- offsetRanges) {
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    sRDD.foreachRDD(_.foreach(println))

    ssc.start()

    ssc.awaitTermination()

  }


}
