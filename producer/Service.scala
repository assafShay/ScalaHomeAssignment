package producer

import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.Properties
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

class Producer(topic: String, brokers: String) {

  val producer = new KafkaProducer[String, String](configuration)

  private def configuration: Properties = {
    val props = new Properties()

    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getCanonicalName)
    props
  }

  def sendMessages(doc: JValue): Unit = {
    try {
      for {
        JArray(objList) <- doc
        JObject(obj) <- objList
      } {
        val kvList = for ((key, JString(value)) <- obj) yield (key, value)
        val record = new ProducerRecord[String, String](topic, kvList.toMap.toString())
        //println(record)
        producer.send(record)
      }
    }
    catch {
      case e: Exception => e.printStackTrace()
    }
    finally {
      println("closing...")
      producer.close()
    }
  }
}

class Mapper {
  def map(doc: JValue): JValue = {
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
}

object Service extends App {
  // parse document
  val FILE_PATH: String = "C:\\ScalaP\\ScalaHomeAssignment\\src\\main\\scala\\producer\\data.json"
  val doc = parse(os.read(os.pwd / FILE_PATH))

  // transform data
  val mapper = new Mapper
  val transformedDoc = mapper.map(doc)

  // produce to kafka
  val producer = new Producer(brokers = "localhost:9092", topic = "listing-topic")
  producer.sendMessages(transformedDoc)
}
