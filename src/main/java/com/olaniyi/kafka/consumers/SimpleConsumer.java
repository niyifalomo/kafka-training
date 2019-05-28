package com.olaniyi.kafka.consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer
{
	private static String TOPIC="";
	private static String BOOTSTRAP_SERVERS = "";

	private static Consumer<Long,String> createConsumer(String topic, String bootstrapServers){
		TOPIC = topic;
		BOOTSTRAP_SERVERS = bootstrapServers;

		//define consumer properties
		Properties props =  new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
		// consumer group
		props.put(ConsumerConfig.GROUP_ID_CONFIG,"kafkaexampleconsumer");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

		//create consumer
		final Consumer<Long,String> consumer = new KafkaConsumer<>(props);
		//subscribe to topic
		consumer.subscribe(Collections.singletonList(TOPIC));

		return consumer;
	}

	public static void runConsumer(String topic,String bootstrapServers){
		final Consumer<Long,String> consumer = createConsumer(topic, bootstrapServers);

		final int giveUp  =90;
		int noRecordsCount = 0;

		Duration duration = Duration.ofMillis(1000);

		while(true){

			final ConsumerRecords<Long,String> consumerRecords = consumer.poll(duration);

			// if no record is received
			if (consumerRecords.count()==0){
				noRecordsCount++;
				if(noRecordsCount>giveUp) break;
				continue;
			}

			consumerRecords.forEach(record -> {
				System.out.printf("Consumer Record: (%d, %s, %d, %d)\n",record.key(),record.value(),record.partition(),record.offset());
			});

			//commit offsets returned on consumer.poll(..)
			consumer.commitAsync();
		}

		consumer.close();
		System.out.println("COMPLETED");

	}




}
