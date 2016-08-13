package com.xpandit.spark

import java.sql.{Date, Timestamp}

import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.streaming._
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.collection.mutable.Set

import scala.tools.nsc.io.File


object SparkStatefulStreaming {

  val logger = Logger.getLogger(SparkStatefulStreaming.getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkStatefulStreaming")
    conf.set("spark.streaming.kafka.maxRatePerPartition", "250000")

    val ssc = new StreamingContext(conf, Seconds(1))
    ssc.checkpoint("/tmp/spark/checkpoint") // set checkpoint directory


    //Retrieving from Kafka
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092", "auto.offset.reset" -> "smallest")
    val topicsSet = scala.collection.immutable.Set("events")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val nonFilteredEvents = kafkaStream.map( (tuple) => createEvent(tuple._2) )

    val events = nonFilteredEvents.filter((pair) => {
      pair._2.highTemperature() && pair._2.isTimeRelevant()
    })


    //mapWithState function
    val updateState = (batchTime: Time, key: Int, value: Option[TemperatureStateEvent], state: State[(Option[Long], Set[TemperatureStateEvent])]) => {

      if (!state.exists) state.update((None, Set.empty))

      var updatedSet = Set[TemperatureStateEvent](value.get)

      //exclude non-relevant events
      state.get()._2.foreach((tempEvent) => {
        if (tempEvent.isTimeRelevant()) updatedSet.add(tempEvent)
      })

      var lastAlertTime = state.get()._1

      //launch alert if no alerts launched yet or if last launched alert was more than X seconds ago
      if (updatedSet.size >= 2 && (lastAlertTime.isEmpty || !timeNoMoreThanXseconds(lastAlertTime.get, 120))) {

        lastAlertTime = Some(System.currentTimeMillis())

        var sb = new StringBuilder()
        sb.append("\n---------- ALERT SITUATION AT " +  new Timestamp(lastAlertTime.get) + "-----------\n")
        updatedSet.foreach(sb.append(_))
        sb.append("----------------------------------------------------------------\n\n")


        File("src/main/resources/output/output.txt").appendAll(sb.toString())


      }

      state.update((lastAlertTime, updatedSet))

      Some((key, updatedSet)) // mapped value

    }

    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = events.mapWithState(spec)

    mappedStatefulStream.print()

    ssc.start()
    ssc.awaitTermination()
  }


  @SerialVersionUID(100L)
  class TemperatureStateEvent(val rackId: Int, val time: Long, val temp: Double) extends Serializable {

    val HighTemperature = 40.0
    val RelevantTime = 120        //time window in seconds in which events will be considered from

    def highTemperature() = temp > HighTemperature

    //is event from no more than 2min ago
    def isTimeRelevant() = timeNoMoreThanXseconds(time, RelevantTime)

    override def toString(): String = "[ " + rackId + " , " + new Timestamp(time) + " , " + temp + " ]\n"

  }

  def createEvent(strEvent: String): (Int, TemperatureStateEvent) = {

    val eventData = strEvent.split('|')

    val time = eventData(0).toLong
    val rackId = eventData(1).toInt
    val temp = eventData(2).toDouble

    (rackId, new TemperatureStateEvent(rackId, time, temp))
  }

  def timeNoMoreThanXseconds(timestamp: Long, maxTimeDiffSeconds: Int): Boolean = {
    val diff = (System.currentTimeMillis() - timestamp)
    diff <= maxTimeDiffSeconds * 1000
  }

}