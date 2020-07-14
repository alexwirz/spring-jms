package com.codenotfound.jms;

import org.apache.qpid.jms.JmsConnectionFactory;
import org.apache.qpid.jms.policy.JmsDefaultPrefetchPolicy;
import org.springframework.jms.connection.CachingConnectionFactory;

import javax.jms.ConnectionFactory;

public final class SequentialPrefetchPolicy {

    private final int prefetchCount;

    public SequentialPrefetchPolicy() {
        this(1);
    }

    public SequentialPrefetchPolicy(int prefetchCount) {
        this.prefetchCount = prefetchCount;
    }

    public ConnectionFactory applyTo(ConnectionFactory connectionFactory) {
        if(connectionFactory instanceof CachingConnectionFactory) {
            final var cachingConnectionFactory = (CachingConnectionFactory) connectionFactory;
            final var targetConnectionFactory = (JmsConnectionFactory) cachingConnectionFactory.getTargetConnectionFactory();
            return targetConnectionFactory == null ? connectionFactory : setupPrefetchForSlowConsumers(targetConnectionFactory);
        }

        return connectionFactory;
    }

    private ConnectionFactory setupPrefetchForSlowConsumers(JmsConnectionFactory jmsConnectionFactory) {
        final var sequentialPrefetchPolicy = new JmsDefaultPrefetchPolicy();
        sequentialPrefetchPolicy.setQueuePrefetch(prefetchCount);
        sequentialPrefetchPolicy.setQueueBrowserPrefetch(prefetchCount);
        sequentialPrefetchPolicy.setTopicPrefetch(prefetchCount);
        sequentialPrefetchPolicy.setDurableTopicPrefetch(prefetchCount);
        jmsConnectionFactory.setPrefetchPolicy(sequentialPrefetchPolicy);
        return jmsConnectionFactory;
    }
}
