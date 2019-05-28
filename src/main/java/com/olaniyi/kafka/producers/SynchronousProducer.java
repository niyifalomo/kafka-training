package com.olaniyi.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SynchronousProducer
{
	private static String TOPIC = "";
	private static String BOOTSTRAP_SERVERS = "";

	/**
	 * Creates producer
	 *
	 * @param topic
	 * @param bootstrapServers
	 * @return
	 */
	private static Producer<Long, String> createProducer(String topic, String bootstrapServers)
	{
		// set topic to push records to
		setTOPIC(topic);
		setBootstrapServers(bootstrapServers);

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "SynchronousProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return new KafkaProducer<>(props);
	}

	/**
	 * Send records synchronously wih Kafka producer
	 *
	 * @param topic
	 * @param bootstrapServers
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void runProducer(String topic, String bootstrapServers) throws ExecutionException, InterruptedException
	{
		Producer<Long, String> producer = createProducer(topic, bootstrapServers);

		try
		{
			for (long i = 0; i < 5; i++)
			{
				ProducerRecord<Long, String> record = new ProducerRecord<>(getTOPIC(), i, "Message" + i);
				RecordMetadata recordMetadata = producer.send(record).get();
				System.out.printf("sent record : (key=%s value=%s)  recordMetadata : (partition=%d, offset=%d)\n", record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset());
			}
		} finally
		{
			producer.flush();
			producer.close();
		}

	}

	public static String getTOPIC()
	{
		return TOPIC;
	}

	public static void setTOPIC(String TOPIC)
	{
		SynchronousProducer.TOPIC = TOPIC;
	}

	public static String getBootstrapServers()
	{
		return BOOTSTRAP_SERVERS;
	}

	public static void setBootstrapServers(String bootstrapServers)
	{
		BOOTSTRAP_SERVERS = bootstrapServers;
	}
}
