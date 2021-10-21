package consumer

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._

class Consumer(brokers: String, topic: String, groupId: String) {

  val consumer = new KafkaConsumer[String, String](configuration)
  consumer.subscribe(List(topic).asJava)

  private def configuration: Properties = {
    val props = new Properties()

    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props
  }

  def receiveMessages(): Unit = {
    while (true) {
      val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofSeconds(1))
      for (record <- records.asScala) {
        println(record.value())
      }
    }
  }
}

object Service extends App {
  val consumer = new Consumer(brokers = "localhost:9092", topic = "MyTopic1", groupId = "test-group")
  consumer.receiveMessages()
}
