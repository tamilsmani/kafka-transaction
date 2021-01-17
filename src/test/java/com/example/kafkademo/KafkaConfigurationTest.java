package com.example.kafkademo;

import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = KafkaConfiguration.class)
@DirtiesContext
@EmbeddedKafka(partitions = 1, controlledShutdown = false,topics = { "${kafka.topic}" })
@TestPropertySource(properties = { "kafka.servers=${spring.embedded.kafka.brokers}",
		"spring.kafka.consumer.auto-offset-reset=earliest", "offsets.topic.replication.factor=1"})
public class KafkaConfigurationTest {
    
    @Value("${kafka.topic}")
    private String topic;
    
    @Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

    @Before
    public void setUp() {
      //  this.producer = buildKafkaTemplate();
       // this.producer.setDefaultTopic(topic);
    }
    private KafkaTemplate<Integer, String> buildKafkaTemplate() {
      //<String, Object> senderProps = KafkaTestUtils.producerProps(kafkaConfiguration);
     //   ProducerFactory<Integer, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
      //  return new KafkaTemplate<>(pf);
    	return null;
    }
    @org.junit.jupiter.api.Test
    @Transactional
    public void listenerShouldConsumeMessages() throws InterruptedException {
        // Given
    	kafkaTemplate.send(topic, "Hello world");
    	System.out.println("Published....");
        // Then
        Thread.sleep(100000);
    }
}