package com.codenotfound.jms;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

import javax.jms.ConnectionFactory;

@Component
public final class PrefetchPolicyPostProcessor implements BeanPostProcessor {
    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if(bean instanceof ConnectionFactory) {
            new SequentialPrefetchPolicy().applyTo((ConnectionFactory) bean);
        }

        return bean;
    }
}
