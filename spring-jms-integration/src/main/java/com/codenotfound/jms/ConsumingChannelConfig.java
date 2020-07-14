package com.codenotfound.jms;

import com.microsoft.azure.spring.autoconfigure.jms.AzureServiceBusJMSProperties;
import com.microsoft.azure.spring.autoconfigure.jms.ConnectionStringResolver;
import com.microsoft.azure.spring.autoconfigure.jms.ServiceBusKey;
import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.apache.qpid.jms.policy.JmsPrefetchPolicy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.handler.LoggingHandler;
import org.springframework.integration.jms.ChannelPublishingJmsMessageListener;
import org.springframework.integration.jms.JmsMessageDrivenEndpoint;
import org.springframework.integration.jms.dsl.Jms;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.listener.SimpleMessageListenerContainer;

import javax.jms.ConnectionFactory;

@Configuration
public class ConsumingChannelConfig {

  @Value("${destination.integration}")
  private String integrationDestination;

  @Bean
  public DirectChannel consumingChannel() {
    return new DirectChannel();
  }

  @Bean
  @ConditionalOnMissingBean
  public ConnectionFactory jmsConnectionFactory(AzureServiceBusJMSProperties serviceBusJMSProperties) {
    final String connectionString = serviceBusJMSProperties.getConnectionString();
    final String clientId = serviceBusJMSProperties.getTopicClientId();
    final int idleTimeout = serviceBusJMSProperties.getIdleTimeout();

    final ServiceBusKey serviceBusKey = ConnectionStringResolver.getServiceBusKey(connectionString);
    final String host = serviceBusKey.getHost();
    final String sasKeyName = serviceBusKey.getSharedAccessKeyName();
    final String sasKey = serviceBusKey.getSharedAccessKey();

    final String remoteUri = String.format("amqps://%s?amqp.idleTimeout=%d", host, idleTimeout);
    final JmsConnectionFactory jmsConnectionFactory = new JmsConnectionFactory();
    jmsConnectionFactory.setRemoteURI(remoteUri);
    jmsConnectionFactory.setClientID(clientId);
    jmsConnectionFactory.setUsername(sasKeyName);
    jmsConnectionFactory.setPassword(sasKey);
    final var jmsDefaultPrefetchPolicy = new JmsDefaultPrefetchPolicy();
    jmsDefaultPrefetchPolicy.setQueuePrefetch(1);
    jmsDefaultPrefetchPolicy.setQueueBrowserPrefetch(1);
    jmsConnectionFactory.setPrefetchPolicy(jmsDefaultPrefetchPolicy);
    return new CachingConnectionFactory(jmsConnectionFactory);
  }

  @Bean
  public JmsMessageDrivenEndpoint jmsMessageDrivenEndpoint(
      ConnectionFactory connectionFactory) {
    JmsMessageDrivenEndpoint endpoint = new JmsMessageDrivenEndpoint(
        simpleMessageListenerContainer(connectionFactory),
        channelPublishingJmsMessageListener());
    endpoint.setOutputChannel(consumingChannel());

    return endpoint;
  }

  @Bean
  public SimpleMessageListenerContainer simpleMessageListenerContainer(
      ConnectionFactory connectionFactory) {
    SimpleMessageListenerContainer container =
        new SimpleMessageListenerContainer();
    container.setConnectionFactory(connectionFactory);
    container.setDestinationName(integrationDestination);
    return container;
  }

  @Bean
  public ChannelPublishingJmsMessageListener channelPublishingJmsMessageListener() {
    return new ChannelPublishingJmsMessageListener();
  }

  @Bean
  @ServiceActivator(inputChannel = "consumingChannel")
  public CountDownLatchHandler countDownLatchHandler() {
    return new CountDownLatchHandler();
  }
}
