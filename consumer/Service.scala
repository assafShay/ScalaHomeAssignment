package consumer

import java.time.Duration
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.jdk.CollectionConverters.IterableHasAsScala

object Service extends App {

  consumeFromKafka("listing_topic")
  val jdbcHostname = "LEDZEPPELIN"
  val jdbcDatabase = "test"
  val jdbcUrl = s"jdbc:sqlserver://${jdbcHostname};database=${jdbcDatabase}"

  def consumeFromKafka(topic: String) = {
    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9094")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "consumer-group")

    val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
    try {
      consumer.subscribe(util.Arrays.asList(topic))

      while (true) {
        val records = consumer.poll(Duration.ofSeconds(1))
        for (record <- records.asScala) {
          sendToDb(record.value())

          println("Topic: " + record.topic() +
            ",Key: " + record.key() +
            ",Value: " + record.value() +
            ",Offset: " + record.offset() +
            ",Partition: " + record.partition())
        }
      }
    }
    catch {
      case e:Exception => e.printStackTrace()
    }
    finally {
      consumer.close()
    }
  }

  def sendToDb(record: String): Unit = {
    // TO DO
  }
}
