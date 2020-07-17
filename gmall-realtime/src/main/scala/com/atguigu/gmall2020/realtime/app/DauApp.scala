package com.atguigu.gmall2020.realtime.app

object DauApp {

    def main(args: Array[String]): Unit = {
      val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("dau_app")
      val ssc = new StreamingContext(sparkConf, Seconds(5))
      val groupId = "GMALL_DAU_CONSUMER"
      val topic = "GMALL_START"
      val startupInputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(topic, ssc)


      val startLogInfoDStream: DStream[JSONObject] = startupInputDstream.map { record =>
        val startupJson: String = record.value()
        val startupJSONObj: JSONObject = JSON.parseObject(startupJson)
        val ts: lang.Long = startupJSONObj.getLong("ts")
        startupJSONObj
      }
      startLogInfoDStream.print(100)

      ssc.start()
      ssc.awaitTermination()
    }
  }

