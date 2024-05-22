package com.github.kafka.primes

import com.github.kafka.primes.config.Config
import com.github.kafka.primes.serdes.Serdes.consumed
import org.apache.kafka.common.serialization.Serdes.LongSerde
import org.apache.kafka.streams.kstream.{KeyValueMapper, Produced}
import org.apache.kafka.streams.{KeyValue, StreamsBuilder}

import java.lang
import scala.jdk.CollectionConverters.IterableHasAsJava

object NumbersEmitter {

  var lastNumberEmitted: Long = 1
  val generateUntil: Long = 5000000L

  def generateNumbersToTest(streams: StreamsBuilder): StreamsBuilder = {

    streams
      .stream(Config.primeNumbersTopic, consumed)
      .peek((key, _) => println(s"received prime number: $key"))
      .flatMap(nextNumbers)
      .to(Config.numbersToTestTopic, Produced.`with`(new LongSerde, new LongSerde)) // TODO: use produced // TODO: solve lang.Long

    streams
  }

  private def nextNumbers: KeyValueMapper[lang.Long, lang.Long, java.lang.Iterable[KeyValue[lang.Long, lang.Long]]] = {
    (key, _) => {
      val n = key.longValue()
      val square = Math.min(Math.pow(n.longValue(), 2).asInstanceOf[Long], generateUntil)
      val seed = 2 + Math.max(
        lastNumberEmitted, // lastNumberEmitted is always odd
        if (n % 2 != 0) n else n - 1 // n-1 to handle case of n=2
      )// We want the list to contain only odd numbers // TODO: not clean

      val numbers = seed until square by 2
      numbers.lastOption.foreach(lastNumberEmitted = _)
      numbers.map(n => KeyValue.pair(n.asInstanceOf[lang.Long], n.asInstanceOf[lang.Long])).toList.asJava
    }
  } // TODO: to improve (more concise ?) + Long vs lang.Long

}
