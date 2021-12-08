package service;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import org.apache.kafka.clients.producer.ProducerConfig;

public class Consumer<K, V> {
		
	private KafkaConsumer<K, V> consumer;
	private Properties properties;
	public Consumer(  String server 
					, boolean autoOffset
					, String keyDeserializer
					, String valueDeserializer
					, String group
					, String subscribe) {
		
		properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
		properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
		properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		if(autoOffset)
			properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		this.consumer = new KafkaConsumer<K, V>(properties);
		consumer.subscribe(Arrays.asList(subscribe));
	}
	
	public ConsumerRecords<K, V> consumers(int time) {
		return consumer.poll(Duration.ofSeconds(time));
	}
}
