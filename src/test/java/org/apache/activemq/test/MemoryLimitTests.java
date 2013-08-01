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
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;
import org.apache.activemq.broker.region.cursors.StoreDurableSubscriberCursor;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.StorePendingDurableSubscriberMessageStoragePolicy;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.File;
import java.util.Random;
import java.util.UUID;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class MemoryLimitTests {
    Logger LOG = LoggerFactory.getLogger(MemoryLimitTests.class);

    final int msgSizeBytes = (int) (1024 * 1024 * 0.4);// 0.4MB
    final int prefetchLimitForAll = 5;

    String brokerUrl;
    Random random = new Random();

    @Test
    public void test() throws Exception {
        File dir = new File("activemq-data");
        if (dir.exists()) {
            FileUtils.deleteDirectory(dir);
        }

        // start embedded broker
        BrokerService brokerService = new BrokerService();
        brokerUrl = brokerService.addConnector("tcp://localhost:0").getPublishableConnectString();

        brokerService.setPersistent(true);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        brokerService.setDeleteAllMessagesOnStartup(true);
//        brokerService.setKeepDurableSubsActive(false);

        brokerService.getSystemUsage().getMemoryUsage().setLimit(10 * 1024 * 1024); // 10MB broker main memory limit

        PolicyEntry policyEntry = new PolicyEntry();
        policyEntry.setMemoryLimit(1 * 1024 * 1024); // 1MB per-destination memory limit
        policyEntry.setOptimizedDispatch(true);
        policyEntry.setExpireMessagesPeriod(0L);
        policyEntry.setPendingDurableSubscriberPolicy(new StorePendingDurableSubscriberMessageStoragePolicy() {
            @Override
            public PendingMessageCursor
            getSubscriberPendingMessageCursor(Broker broker, String clientId, String
                    name, int maxBatchSize, DurableTopicSubscription sub) {

                broker.getBrokerService().setPersistent(false);
                StoreDurableSubscriberCursor cursor =
                        (StoreDurableSubscriberCursor) super
                                .getSubscriberPendingMessageCursor(broker, clientId, name, maxBatchSize, sub);
                broker.getBrokerService().setPersistent(true);
                return cursor;
            }
        });

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(policyEntry);
        brokerService.setDestinationPolicy(policyMap);

        brokerService.setPersistenceAdapter(new KahaDBPersistenceAdapter());

        brokerService.start();

        try {
//            testPersistentQueue();
            testPersistentTopic();
        } finally {
            brokerService.stop();
        }
    }

    private void testPersistentQueue() throws Exception {
        ActiveMQConnectionFactory factory = createActiveMQConnectionFactory();

        Connection cnxn = null;
        try {
            cnxn = factory.createConnection();
            cnxn.start();
            Session session = cnxn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue queue = session.createQueue("queue-1");
            MessageConsumer consumer = session.createConsumer(queue);
            MessageProducer producer = session.createProducer(queue);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
            for (int i = 0; i < 100; ++i) {
                BytesMessage msg = session.createBytesMessage();
                byte[] bytes = new byte[msgSizeBytes];
                random.nextBytes(bytes);
                msg.writeBytes(bytes);
                producer.send(msg);
                LOG.info("persistent msg [{}] sent", i);
            }
        } finally {
            if (cnxn != null) {
                cnxn.close();
            }
        }
    }

    private void testPersistentTopic() throws Exception {
        ActiveMQConnectionFactory factory = createActiveMQConnectionFactory();

        final String clientId = "cid-" + UUID.randomUUID();
        final String subName = "sub-" + UUID.randomUUID();

        Connection cnxn = null;
        try {
            cnxn = factory.createConnection();
            cnxn.setClientID(clientId);
            cnxn.start();

            Session session = cnxn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Topic topic = session.createTopic("topic-1");

//            TopicSubscriber durableSubscriber = session.createDurableSubscriber(topic, subName);
            session.createConsumer(topic);
//            durableSubscriber.close();
            MessageProducer publisher = session.createProducer(topic);
            publisher.setDeliveryMode(DeliveryMode.PERSISTENT);
            for (int i = 0; i < 100; ++i) {
                BytesMessage msg = session.createBytesMessage();
                byte[] bytes = new byte[msgSizeBytes];
                random.nextBytes(bytes);
                msg.writeBytes(bytes);
                publisher.send(msg);
                LOG.info("persistent msg [{}] sent", i);
            }
        } finally {
            if (cnxn != null) {
                cnxn.close();
            }
        }
    }

    private ActiveMQConnectionFactory createActiveMQConnectionFactory() {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerUrl);
        factory.setWatchTopicAdvisories(false);
        factory.setSendAcksAsync(false);
        factory.getPrefetchPolicy().setAll(prefetchLimitForAll);
        return factory;
    }
}
