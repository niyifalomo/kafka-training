package com.olaniyi.kafka.producers;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SimpleProducer
{
	private static String TOPIC = "";
	private static String BOOTSTRAP_SERVERS = "";

	private static Producer<Long, String> createProducer(String topic, String bootstrapServers)
	{
		setTOPIC(topic);
		setBootstrapServers(bootstrapServers);

		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return new KafkaProducer<>(props);
	}

	/**
	 * Send records synchronously wih Kafka producer
	 *
	 *
	 * @throws ExecutionException
	 * @throws InterruptedException
	 */
	public static void runSyncProducer(String topic, String bootstrapServers) throws ExecutionException, InterruptedException
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

	/**
	 * Send records asynchronously with Kafka Producer
	 *
	 * @throws InterruptedException
	 */
	public static void runAsyncProducer(String topic, String bootstrapServers) throws ExecutionException, InterruptedException
	{
		Producer<Long, String> producer = createProducer(topic, bootstrapServers);

		final CountDownLatch countDownLatch = new CountDownLatch(5);

		try
		{
			for (long i = 0; i < 5; i++)
			{
				ProducerRecord<Long, String> record = new ProducerRecord<>(getTOPIC(), i, "Message" + i);
				producer.send(record, (recordMetadata, e) ->
				{
					if (recordMetadata != null)
					{
						System.out.printf("sent record : (key=%s value=%s)  metadata : (partition=%d, offset=%d)\n", record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset());
					} else
					{
						e.printStackTrace();
					}
					countDownLatch.countDown();

				});

			}
			countDownLatch.await(25, TimeUnit.SECONDS);
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
		SimpleProducer.TOPIC = TOPIC;
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
