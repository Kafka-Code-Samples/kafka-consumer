package com.consumer.kafkaconsumer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;

@SpringBootApplication
@EnableScheduling
public class KafkaConsumerApplication {

	@Scheduled(initialDelay = 1000, fixedDelay = 500)
	public void consumerExicution() {
		System.out.println("consumerExicution");
		String groupId = "RG";
		String topicName = "RandomProducerTopic";
		Consumer<String, String> kafkaConsumer;
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", groupId);
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


		// Figure out where to start processing messages from
		kafkaConsumer = new KafkaConsumer<String, String>(props);
		kafkaConsumer.subscribe(Arrays.asList(topicName));
		// Start processing messages
		try {
			while (true) {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
				for (ConsumerRecord<String, String> record : records)
					System.out.println(record.value());
			}
		} catch (Exception ex) {
			System.out.println("Exception caught " + ex.getMessage());
		} finally {
			kafkaConsumer.close();
			System.out.println("After closing KafkaConsumer");
		}
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaConsumerApplication.class, args);
		System.out.println("Application has configured....");
	}
}
