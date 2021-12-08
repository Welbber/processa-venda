package service;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class Producer<K, V> {

	KafkaProducer<K, V> producer;

	public Producer(String server, String keySerializer, String valueSerializer) {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
		this.producer = new KafkaProducer<K, V>(properties);
	}

	public void send(String topic, V message) {
		ProducerRecord<K, V> record = new ProducerRecord<K, V>(topic, message);
		producer.send(record);
	}

	public void close() {
		producer.close();
	}
}