package com.github.kafka.primes.serdes

import org.apache.kafka.common.serialization.Serdes.LongSerde
import org.apache.kafka.streams.kstream.{Consumed, Produced}

import java.lang

object Serdes {

    val consumed: Consumed[lang.Long, lang.Long] = Consumed.`with`(new LongSerde, new LongSerde)
    val produced: Produced[lang.Long, lang.Long] = Produced.`with`(new LongSerde, new LongSerde)

}
