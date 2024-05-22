package com.github.kafka.primes.common

import org.apache.kafka.streams.StreamsConfig

import java.util.Properties

object Common {

  def props: Properties = {
    val props: Properties = new Properties
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-primes")
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-permanent-app")
    props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3)
    props
  }
}
