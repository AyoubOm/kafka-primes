package com.github.kafka.primes.runtime

import com.github.kafka.primes.NumbersEmitter
import com.github.kafka.primes.config.Config.primeNumbersTopic
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import java.lang
import java.lang.Math.sqrt
import java.util.{Properties, UUID}
import scala.math.round

object ResultVerifier extends App {

  val props = new Properties()

  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongDeserializer")
  props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString)
  props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString)
  props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")


  val consumer = new KafkaConsumer[lang.Long, lang.Long](props)

  consumer.subscribe(java.util.Arrays.asList(primeNumbersTopic))

  var count = 0

  var records = consumer.poll(5000)
  while (! records.isEmpty) {
    count += records.count()
    records.forEach(record => println(record.key()))
    records = consumer.poll(5000)
  }

  println(s"Number of primes = $count")
  println(s"Expected count = ${numberOfPrimesBelow(NumbersEmitter.generateUntil)}")

  private def numberOfPrimesBelow(n: Long): Int = {
    def isPrime(n: Long) = {
      (2L until round(sqrt(n))).forall(num => n % num != 0)
    }

    (2L to n).count(isPrime)
  }
}
