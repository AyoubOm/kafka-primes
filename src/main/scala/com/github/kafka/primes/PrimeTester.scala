package com.github.kafka.primes

import com.github.kafka.primes.config.Config
import org.apache.kafka.common.serialization.Serdes.LongSerde
import org.apache.kafka.streams.kstream.{Consumed, KeyValueMapper, Produced}
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}

import java.lang.Math.sqrt
import scala.jdk.CollectionConverters.IterableHasAsJava
import scala.math.round

import java.lang

object PrimeTester {

  def testPrimes(streams: StreamsBuilder): StreamsBuilder = {
    streams
      .stream(Config.numbersToTestTopic, Consumed.`with`(new LongSerde, new LongSerde))
      .peek((k, _) => println(s"Testing $k"))
      .flatMap(forwardIfPrime)
      .to(Config.primeNumbersTopic, Produced.`with`(new LongSerde, new LongSerde))

    streams
  }

  private def forwardIfPrime: KeyValueMapper[lang.Long, lang.Long, java.lang.Iterable[KeyValue[lang.Long, lang.Long]]] = {
    (key, _) => {
      Option
        .when(
          (2L until round(sqrt(key.doubleValue()))).forall(num => key % num != 0)
        )(KeyValue.pair(key, key)).toSeq.asJava
    }
  }
}
