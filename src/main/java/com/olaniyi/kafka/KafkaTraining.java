package com.olaniyi.kafka;

import com.olaniyi.kafka.consumers.SimpleConsumer;
import com.olaniyi.kafka.producers.SimpleProducer;

import java.util.concurrent.ExecutionException;

public class KafkaTraining
{
	private String topic;
	private String bootstrapServers;

	public KafkaTraining(String topic, String bootstrapServers){
		this.topic = topic;
		this.bootstrapServers = bootstrapServers;
	}

	public static void main(String[] args)
	{

		String topic = "test";
		String bootstrapServers = "localhost:9092";

		KafkaTraining proc = new KafkaTraining(topic,bootstrapServers);

		proc.produce();
		proc.consume();


	}


	/**
	 * Start the consumer in the main thread
	 */
	public void consume(){
		SimpleConsumer.runConsumer(this.topic,this.bootstrapServers);
	}

	/**
	 * Start producer in a new thread
	 */
	public void produce(){
		Thread thread = new Thread(() ->
		{
			try
			{
				//sends records asynchronously
				SimpleProducer.runAsyncProducer(this.topic,this.bootstrapServers);

				//sends record synchronously
				//SimpleProducer.runSyncProducer(this.topic,this.bootstrapServers);
			} catch (ExecutionException e)
			{
				e.printStackTrace();
			} catch (InterruptedException e)
			{
				e.printStackTrace();
			}

		});
		thread.start();
	}
}
