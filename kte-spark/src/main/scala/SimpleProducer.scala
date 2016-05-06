import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

import scala.collection.mutable.ListBuffer

/**
 * Created by gregoire on 28/04/16.
 */
object SimpleProducer {

  def main(args: Array[String]): Unit = {

    //    val topic= args(0)
    val topic = "test-piwik"
    //    val brokers = args(1)
    val brokers = "localhost:9092"

    val properties = new Properties()
    properties.put("metadata.broker.list", brokers)
    properties.put("serializer.class", "kafka.serializer.StringEncoder")
    properties.put("producer.type", "async")

    val prodConfig = new ProducerConfig(properties)
    val producer = new Producer[String, String](prodConfig)

    val file = "/media/gregoire/disk1/kafka_test/kafka-to-es/data/2/VisitLog-2015-01-30.xml"

    val lines = scala.io.Source.fromFile(file).getLines()

    val packetBuffer = ListBuffer[String]()
    for (line <- lines) {
      //Throw away firsts and last line
      if (!(line.startsWith("<?xml") || line.startsWith("<result") || line.startsWith("</result>"))) {
        if (line.startsWith("\t<row>") && packetBuffer.size > 0) {
          sendPacket(packetBuffer, producer, topic)
          packetBuffer.clear()
          Thread.sleep(1000)

        }
        packetBuffer.append(line.drop(1))
      }
    }
    sendPacket(packetBuffer, producer, topic)
    producer.close()


  }

  def sendPacket(packetBuffer: ListBuffer[String], producer: Producer[String, String], topic: String): Unit = {
    val message = packetBuffer.mkString("\n")
    val id = packetBuffer(3).drop(10).dropRight(10)
    val data = new KeyedMessage[String, String](topic, id, message)
    producer.send(data)
    println(s"Sent 1 message with id $id")
  }


}
