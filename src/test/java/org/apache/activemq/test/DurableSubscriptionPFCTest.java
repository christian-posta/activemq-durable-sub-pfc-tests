/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.*;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.plugin.DiscardingDLQBrokerPlugin;
import org.apache.activemq.plugin.ForcePersistencyModeBrokerPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class DurableSubscriptionPFCTest {

    private BrokerService brokerService;
    private ActiveMQConnectionFactory factory;
    private Connection connection;

    @Before
    public void setUp() throws Exception{
        createBroker();
        initConn();
    }

    @Test
    public void testProducerFlowControl() throws Exception {
        ActiveMQTopic topic = new ActiveMQTopic("wel.messages.WelData");
        Session producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = producerSession.createProducer(topic);

        Session consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer durableSubscriber = consumerSession.createDurableSubscriber(topic, "foobar");

        connection.start();
        int i = 0;
        while(true){

            producer.send(producerSession.createTextMessage(createText(256 * 1024)));
            System.out.println("Sent message: " + i++);
        }

//        assertAtLeastOneConnection();



    }

    private String createText(int length) {
        StringBuilder builder = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            builder.append("*");
        }
        return builder.toString();
    }

    private void assertAtLeastOneConnection() {
        TransportConnector connector = brokerService.getTransportConnectorByScheme("vm");
        assertNotNull(connector);
        assertEquals("We were expecting at least one connection...", 1, connector.getConnections().size());
    }

    @After
    public void cleanup() throws Exception{
        stopBroker(brokerService);

        if (this.connection != null) {
            connection.close();
        }

    }

    private void stopBroker(BrokerService brokerService) throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    private void initConn() throws JMSException {
        if (factory == null) {
            factory = new ActiveMQConnectionFactory("vm://localhost");
        }

        connection = factory.createConnection();
        connection.setClientID("test-client-id");
    }

    private void createBroker() {
        try {
            brokerService = BrokerFactory.createBroker("broker:(vm://localhost)/localhost?persistent=true");
            assertNotNull(brokerService);
            configureBroker(brokerService);
            startBroker(brokerService);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void configureBroker(BrokerService broker) throws IOException {
        broker.setUseJmx(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        PolicyMap policyMap = new PolicyMap();
        ActiveMQTopic topic = new ActiveMQTopic("wel.messages.WelData");

        // wel data
        PolicyEntry policy = new PolicyEntry();
        policy.setDestination(topic);
        policy.setProducerFlowControl(true);
        policy.setMemoryLimit(1 * 1024 * 1024);
        policy.setTopicPrefetch(1);
        policy.setDurableTopicPrefetch(5);
//        policy.setCursorMemoryHighWaterMark(100);

        // * what is current prefetch?
        // * what is the size of messages being passed through
        // * what are the settings for storage?
        // * to do: you'll need to calculate what kind of backlog you want, and where you want memory PFC to
        // kick in
        policy.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());
        policy.setPendingDurableSubscriberPolicy(new VMPendingDurableSubscriberMessageStoragePolicy());

        policyMap.put(topic, policy);

        broker.setDestinationPolicy(policyMap);

        // set  broker resources
        broker.getSystemUsage().getMemoryUsage().setLimit(10 * 1024 * 1024);
//        broker.getSystemUsage().getStoreUsage().setLimit(4 * 1024 * 1024);
//        broker.getSystemUsage().getTempUsage().setLimit(4 * 1024 * 1024);

        // persistence adapter
//        ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).setJournalMaxFileLength(4 * 1024 * 1024);
//        ((KahaDBPersistenceAdapter)broker.getPersistenceAdapter()).setCleanupInterval(1 * 1000);
//        ((PListStoreImpl)broker.getTempDataStore()).setJournalMaxFileLength(4 * 1024 * 1024);

        // plugins
        DiscardingDLQBrokerPlugin discardingDLQBrokerPlugin = new DiscardingDLQBrokerPlugin();
        discardingDLQBrokerPlugin.setDropAll(true);
        discardingDLQBrokerPlugin.setDropTemporaryQueues(true);
        discardingDLQBrokerPlugin.setDropTemporaryTopics(true);

        ForcePersistencyModeBrokerPlugin forcePersistencyModeBrokerPlugin = new ForcePersistencyModeBrokerPlugin();
        forcePersistencyModeBrokerPlugin.setPersistenceFlag(false);
//        broker.setPlugins(new BrokerPlugin[]{discardingDLQBrokerPlugin, forcePersistencyModeBrokerPlugin});
        broker.setPlugins(new BrokerPlugin[]{discardingDLQBrokerPlugin});
    }

    private void startBroker(BrokerService brokerService) throws Exception {
        brokerService.start();
        brokerService.waitUntilStarted();
    }


}
