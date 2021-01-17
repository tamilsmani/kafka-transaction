package com.example;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ImportResource;

import com.example.config.KafaTransactionDLTConfiguration;
import com.example.event.CustomEventListener;
import com.example.event.CustomEventPublisher;

@SpringBootApplication
@ImportResource({
	"classpath:jpa-persistence-context.xml"
})
@ComponentScan(basePackageClasses = { KafaTransactionDLTConfiguration.class ,CustomEventListener.class, CustomEventPublisher.class}
					   )
public class KafkaTranactionDemoDLTApplication {

	private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTranactionDemoDLTApplication.class);
	
	@Autowired
	KafaTransactionDLTConfiguration kafaTransactionDLTConfiguration;
	
	public static void main(String[] args) throws Exception {
		ConfigurableApplicationContext context = new SpringApplicationBuilder(KafkaTranactionDemoDLTApplication.class).run(args);
        context.getBean(KafkaTranactionDemoDLTApplication.class).run(context);
        context.close();
	}

	private void run(ConfigurableApplicationContext context) throws Exception {
		
		LOGGER.info("Transaction with DLT example ........ ");
		TimeUnit.SECONDS.sleep(10);
		//kafaTransactionConfiguration.send(">>>>Sending Transactional message >>>>>>>>>");
		LOGGER.info("Sleeping for 5 seconds ........ ");
		TimeUnit.SECONDS.sleep(50000);
        
    }
}
