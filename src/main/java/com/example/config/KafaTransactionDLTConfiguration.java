package com.example.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultAfterRollbackProcessor;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.transaction.ChainedKafkaTransactionManager;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.util.backoff.FixedBackOff;

import com.example.entity.TitleRepository;
import com.example.event.CustomEventPublisher;

@Component
@EnableKafka
public class KafaTransactionDLTConfiguration {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafaTransactionDLTConfiguration.class);

	@Value("${kafka.server}")
	private String kafaServer;

	@Value("${kafka.topic}")
	private String topic;

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	CustomEventPublisher customEventPublisher;

	@Autowired
	private TitleRepository titleRepository;

	@KafkaListener(id = "${kafka.topic}", topics = "${kafka.topic}", groupId = "${kafka.topic}-group", 
			autoStartup = "${listen.auto.start:true}", concurrency = "${listen.concurrency:1}", 
			containerFactory = "kafkaListenerContainerFactory", errorHandler = "errorHandler")
	@Transactional(transactionManager = "chainedKafkaTransactionManager")
	public void listen(@Payload String payload, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.CONSUMER) KafkaConsumer<String, String> consumer,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts, @Header(KafkaHeaders.OFFSET) long offset)
			throws Exception {

		LOGGER.info(">>>>>> Received transactional message(in-transaction) from topic[{}] and payload[{}]", topic,
				payload);
		customEventPublisher.publish(payload + "=> Consumed & DB insert is success");
		// Throwing exception programmatically to test DLT by committing KafkaTransaction on 'my-replicated-demo-topic' topic
		titleRepository.save(null);
		System.out.println(kafkaTemplate.inTransaction());

	}

	@KafkaListener(groupId = "${kafka.topic}.DLT-group", topics = "${kafka.topic}.DLT", 
			autoStartup = "${listen.auto.start:true}", containerFactory = "kafkaListenerContainerFactory")
	public void deadLetterTopic(@Payload String payload, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition,
			@Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
			@Header(KafkaHeaders.CONSUMER) KafkaConsumer<String, String> consumer,
			@Header(KafkaHeaders.DLT_EXCEPTION_MESSAGE) byte[] exception,
			@Header(KafkaHeaders.RECEIVED_TIMESTAMP) long ts, @Header(KafkaHeaders.OFFSET) long offset) {
		LOGGER.info("Error - {} ", new String(exception));
		LOGGER.info("Message from DLT - {}", payload);

	}

	@Bean
	public KafkaListenerErrorHandler errorHandler() {
		return new KafkaListenerErrorHandler() {
			@Override
			public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
				LOGGER.info("In Error handler {}", exception);
				return message;
			}
		};
	}

	// --------------- DLT Config
	@Bean
	public DefaultAfterRollbackProcessor<String, String> rollbackProcessor(
			KafkaTemplate<String, String> kafkaTemplate) {
		return new DefaultAfterRollbackProcessor<String, String>(new DeadLetterPublishingRecoverer(kafkaTemplate),
				new FixedBackOff(1000L, 1), kafkaTemplate, true);
	}

	// --------------------- Consumer config
	@Bean
	public ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(
			ConcurrentKafkaListenerContainerFactoryConfigurer configurer,
			ConsumerFactory<String, String> kafkaConsumerFactory,
			ChainedKafkaTransactionManager<Object, Object> chainedTM,
			DefaultAfterRollbackProcessor<String, String> rollbackProcessor) {

		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.getContainerProperties().setTransactionManager((PlatformTransactionManager) chainedTM);

		factory.setConsumerFactory(kafkaConsumerFactory);
		factory.setConcurrency(3);

		factory.setAfterRollbackProcessor(rollbackProcessor);
		return factory;
	}

	@Bean(name = "chainedKafkaTransactionManager")
	public ChainedKafkaTransactionManager<Object, Object> chainedTm(KafkaTransactionManager<String, String> ktm,
			@Qualifier("transactionManager") JpaTransactionManager dstm) {
		return new ChainedKafkaTransactionManager<>(ktm, dstm);
	}

	@Bean
	public KafkaTransactionManager<String, String> kafkaTransactionManager(
			ProducerFactory<String, String> producerFactory) {
		KafkaTransactionManager<String, String> ktm = new KafkaTransactionManager<String, String>(producerFactory);
		ktm.setNestedTransactionAllowed(true);
		ktm.setTransactionSynchronization(AbstractPlatformTransactionManager.SYNCHRONIZATION_ON_ACTUAL_TRANSACTION);
		return ktm;
	}

//	@Bean
//    public DataSourceTransactionManager dstm(DataSource dataSource) {
//        return new DataSourceTransactionManager(dataSource);
//    }

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(config());
	}

	// --------------------- Producer Config
	@Bean
	public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> producerFactory) {
		return new KafkaTemplate<String, String>(producerFactory, true);
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(config());
		producerFactory.setTransactionIdPrefix("tamil-tx");
		return producerFactory;
	}

	@Bean
	public Map<String, Object> config() {
		Map<String, Object> configMap = new HashMap<String, Object>();
		configMap.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafaServer);
		configMap.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

		configMap.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
		configMap.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configMap.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configMap.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		configMap.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		// configMap.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
		// configMap.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

		// introduce a delay on the send to allow more messages to accumulate
		configMap.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		// records
		configMap.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		// automatically reset the offset to the earliest offset
		configMap.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return configMap;

	}

}