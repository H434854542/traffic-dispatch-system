import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.util.{Collections, Properties}
import scala.collection.JavaConverters._

class KafkaReceiver(topic: String, brokers: String, groupId: String)
  extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {

  override def onStart(): Unit = {
    new Thread("Kafka Receiver") {
      override def run(): Unit = receive()
    }.start()
  }

  def receive(): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singleton(topic))

    while (!isStopped()) {
      val records = consumer.poll(java.time.Duration.ofMillis(100))
      for (record <- records.asScala) {
        store(record.value())
      }
    }
    consumer.close()
  }

  override def onStop(): Unit = {}
}