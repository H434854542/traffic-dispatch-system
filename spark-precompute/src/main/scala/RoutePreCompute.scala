import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis
import com.alibaba.fastjson.JSON

case class ScheduleChangeMsg(
  action: String,
  fromCity: String,
  toCity: String,
  routeId: String
)

object RoutePreCompute {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("RoutePreCompute")
      .setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val kafkaStream = ssc.receiverStream(new KafkaReceiver(
      topic = "schedule-change",
      brokers = "kafka-service.schedule-system:9092",
      groupId = "route-precompute-group"
    ))

    val redisHost = "redis-0.redis-headless"
    val redisPort = 6379
    val redisPassword = "Redis@123456"

    kafkaStream.foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val jedis = new Jedis(redisHost, redisPort)
        jedis.auth(redisPassword)
        partition.foreach(msg => {
          try {
            val changeMsg = JSON.parseObject(msg, classOf[ScheduleChangeMsg])
            println(s"[预计算] 收到班次变动：${changeMsg}")

            val cacheKeys = jedis.keys(s"route:${changeMsg.fromCity}:${changeMsg.toCity}:*")
            if (cacheKeys != null && !cacheKeys.isEmpty) {
              jedis.del(cacheKeys.toArray(new Array[String](0)): _*)
              println(s"[预计算] 已失效旧缓存，共${cacheKeys.size}条")
            }

            val cityPair = s"${changeMsg.fromCity}-${changeMsg.toCity}"
            val strategies = Array(1, 2, 3)
            strategies.foreach(strategy => {
              val cacheKey = s"route:${changeMsg.fromCity}:${changeMsg.toCity}:${strategy}"
              val preComputeResult = s"""{"from_city":"${changeMsg.fromCity}","to_city":"${changeMsg.toCity}","strategy":${strategy},"precompute_time":"${System.currentTimeMillis()}"}"""
              jedis.setex(cacheKey, 12L * 3600, preComputeResult)
            })
            println(s"[预计算] 线路${cityPair}预计算完成")
          } catch {
            case e: Exception =>
              println(s"[预计算] 处理消息失败：${e.getMessage}")
          }
        })
        jedis.close()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}