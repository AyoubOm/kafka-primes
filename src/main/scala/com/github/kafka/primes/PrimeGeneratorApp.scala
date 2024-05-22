package com.github.kafka.primes

import com.github.kafka.primes.common.Common
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, Topology}

object PrimeGeneratorApp extends App {

  private val streams: KafkaStreams = new KafkaStreams(topology, Common.props)

  streams.start()

  Runtime.getRuntime.addShutdownHook(new Thread(() => streams.close()))


  private def topology: Topology = {
    val streams = new StreamsBuilder()

    val pipeline = (NumbersEmitter.generateNumbersToTest _).andThen(PrimeTester.testPrimes)

    pipeline(streams).build()
  }
}
/*

kafka-topics --bootstrap-server localhost:9092  --delete --topic prime-numbers; kafka-topics --bootstrap-server localhost:9092 --delete --topic numbers-to-test; \
kafka-topics --bootstrap-server localhost:9092  --create --topic prime-numbers --partitions 1; kafka-topics --bootstrap-server localhost:9092 --create --topic numbers-to-test --partitions 3;


 */