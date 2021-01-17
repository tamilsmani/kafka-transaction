package com.example.kafkademo;

import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Producer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = { "${kafka.topic}" })
public class KafkaDemoApplicationTests {

	@Autowired
	private Producer producer;

	@Test
	public void testReceive() throws Exception {
		//producer.send("My First Kakfa test!");
		TimeUnit.SECONDS.sleep(5000);
	}

}
