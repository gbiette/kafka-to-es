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
object SparkConsumer {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    val (estype, mapping) = loadMapping("/mapping_piwik.json")

    initiateIndex("local_greg", "piwik_kafka_stream", estype, mapping)


    val conf = new SparkConf().setAppName("piwik-kafka")
    conf.setMaster("local[3]")
    val ssc = new StreamingContext(conf, Seconds(5))

    ssc.checkpoint("checkpoint")

    val sRDD = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, Map[String, String]("metadata.broker.list" -> "localhost:9092"), Set[String]("test-piwik"))

    case class PiwikSessionGB(visitId: String, serverDate: String, serverHour: Int, localHour: Int, countryCode: String, device: String, duration: Int, nbUrl: Int, urls: Array[String])

    val sessions = sRDD.map(_._2).map { fileContent =>

      val xmlContent = XML.loadString(fileContent)
      val visitID = try {
        (xmlContent \ "idVisit")(0).text
      } catch {
        case e: Exception => ""
      }
      val serverDate = try {
        (xmlContent \ "serverDate")(0).text
      } catch {
        case e: Exception => ""
      }
      val serverHour = try {
        (xmlContent \ "visitServerHour")(0).text.toInt
      } catch {
        case e: Exception => -1
      }
      val localHour = try {
        (xmlContent \ "visitLocalHour")(0).text.toInt
      } catch {
        case e: Exception => -1
      }
      val countryCode = try {
        (xmlContent \ "countryCode")(0).text.toUpperCase
      } catch {
        case e: Exception => ""
      }
      val device = try {
        (xmlContent \ "deviceType")(0).text
      } catch {
        case e: Exception => ""
      }
      val duration = try {
        (xmlContent \ "visitDuration")(0).text.toInt
      } catch {
        case e: Exception => -1
      }
      val nbUrl =
        (xmlContent \ "actionDetails" \ "row").size

      val urls = (xmlContent \ "actionDetails" \ "row").map(s => (s \ "url").text.split( """\?""", -1)(0)).toArray

      PiwikSessionGB(visitID, serverDate, serverHour, localHour, countryCode, device, duration, nbUrl, urls)

    }
    sessions.foreachRDD(rdd => EsSpark.saveToEs(rdd, "piwik_kafka_stream/" + estype))

    ssc.start()

    ssc.awaitTermination()

  }


}
