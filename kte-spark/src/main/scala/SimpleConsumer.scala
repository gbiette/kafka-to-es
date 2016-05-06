import java.util.Properties
import java.util.concurrent.Executors

import kafka.consumer.{KafkaStream, ConsumerConfig, Consumer}
import kafka.utils.Logging

/**
 * Created by gregoire on 28/04/16.
 */
object SimpleConsumer {

  def main(args: Array[String]) = {

    val zookeeper = "localhost:2181"
    val groupID = "group1"
    val topic = "test-piwik"
    val numThreads = 3

    val properties = new Properties()
    properties.put("zookeeper.connect", zookeeper)
    properties.put("group.id", groupID)
    properties.put("auto.offset.reset", "largest")
    properties.put("zookeeper.session.timeout.ms", "400")
    properties.put("zookeeper.sync.time.ms", "200")
    properties.put("auto.commit.interval.ms", "1000")
    val consumerConfig = new ConsumerConfig(properties)
    val consumer = Consumer.create(consumerConfig)

    val topicCountMap = Map(topic -> numThreads)
    val consumerMap = consumer.createMessageStreams(topicCountMap)
    val streams = consumerMap.get(topic).get

    val executors = Executors.newFixedThreadPool(numThreads)

    var threadNumber = 0

    for (stream <- streams) {
      executors.submit(new ConsumerThread(stream, threadNumber))
      threadNumber += 1
    }


  }

  class ConsumerThread(val stream: KafkaStream[Array[Byte], Array[Byte]], val threadId: Int) extends Logging with Runnable {

    def run(): Unit = {
      val iterator = stream.iterator()
      println(s"Start of thread $threadId")

      while (iterator.hasNext()) {
        val msg = new String(iterator.next.message())
        println(s"\tThread $threadId : $msg")
      }

      println(s"End of thread $threadId")
    }

  }


}
