package com.github.kafka.primes.runtime


import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import java.lang
import java.util.Properties

object SeedSender extends App {

  val props = new Properties()

  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.LongSerializer")
  props.put(ProducerConfig.ACKS_CONFIG, "all")

  val producer = new KafkaProducer[lang.Long, lang.Long](props)

  producer
    .send(new ProducerRecord[lang.Long, lang.Long]("prime-numbers", 2L, 2L))
    .get()


  println("Sent seed !")

}

