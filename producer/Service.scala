package producer

import org.json4s._
import org.json4s.jackson.JsonMethods._

import java.util.Properties
import org.apache.kafka.clients.producer._

object Service extends App {

  val filePath: String = "C:\\ScalaP\\udemy-scala-for-beginners\\src\\main\\scala\\producer\\data.json"
  var doc = parseJson(filePath)
  doc = transformJson(doc)
  //println(pretty(doc))
  produceToKafka(doc)

  def parseJson(filePath: String): JValue = {
    val rawJson = os.read(os.pwd / filePath)
    val doc = parse(rawJson)
    doc
  }

  def transformJson(doc: JValue): JValue = {
    val transformedDoc = doc transformField {
      case JField("ListingStatus", JString("Active Under Contract")) => ("ListingStatus", JString("Pending"))
      case JField("ListingStatus", JString("Closed")) => ("ListingStatus", JString("Sold"))
      case JField("ListingStatus", v) if v != JString("Active") && v != JString("Active Under Contract") && v != JString("Closed") => ("ListingStatus", JString("Unknown"))
      case JField("HouseType", v) if v == JString("Condominium") || v == JString("Stock Cooperative") || v == JString("Tenancy in Common") => ("HouseType", JString("Condo"))
      case JField("HouseType", v) if v == JString("2 Houses on Lot") ||
        v == JString("3+ Houses on Lot") ||
        v == JString("Halfplex") ||
        v == JString("Manufactured Home") ||
        v == JString("Mobile Home") ||
        v == JString("Modular") ||
        v == JString("Single Family") ||
        v == JString("Residence") => ("HouseType", JString("House"))
      case JField("HouseType", v) if v != JString("House") && v != JString("Condo") && v != JString("Townhouse") => ("HouseType", JString("Unknown"))
      case JField("ProviderName", v) if v == JString("CRMLS") => ("ProviderId", JInt(1))
      case JField("ProviderName", v) if v == JString("MLS") => ("ProviderId", JInt(2))
      case JField("ProviderName", v) if v == JString("Santa Barbara") => ("ProviderId", JInt(3))
    }

    transformedDoc
  }

  def produceToKafka(doc: JValue): Unit = {
    val props = new Properties()

    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "listing_topic"

    try {
      for {
        JArray(objList) <- doc
        JObject(obj) <- objList
      } {
        val kvList = for ((key, JString(value)) <- obj) yield (key, value)
        val record = new ProducerRecord[String, String](topic, kvList.toString())
        //println(record.value())
        val metadata = producer.send(record)

        printf(s"sent record(key=%s value=%s) " +
          "meta(partition=%d, offset=%d)\n",
          record.key(), record.value(),
          metadata.get().partition(),
          metadata.get().offset())
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      producer.close()
    }
  }
}