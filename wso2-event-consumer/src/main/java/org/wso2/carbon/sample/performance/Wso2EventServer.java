/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.carbon.sample.performance;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.commons.Credentials;
import org.wso2.carbon.databridge.commons.Event;
import org.wso2.carbon.databridge.commons.StreamDefinition;
import org.wso2.carbon.databridge.commons.thrift.data.ThriftEventBundle;
import org.wso2.carbon.databridge.core.AgentCallback;
import org.wso2.carbon.databridge.core.DataBridge;
import org.wso2.carbon.databridge.core.RawDataAgentCallback;
import org.wso2.carbon.databridge.core.Utils.AgentSession;
import org.wso2.carbon.databridge.core.Utils.EventComposite;
import org.wso2.carbon.databridge.core.definitionstore.AbstractStreamDefinitionStore;
import org.wso2.carbon.databridge.core.definitionstore.InMemoryStreamDefinitionStore;
import org.wso2.carbon.databridge.core.exception.DataBridgeException;
import org.wso2.carbon.databridge.core.exception.StreamDefinitionStoreException;
import org.wso2.carbon.databridge.core.internal.authentication.AuthenticationHandler;
import org.wso2.carbon.databridge.receiver.binary.conf.BinaryDataReceiverConfiguration;
import org.wso2.carbon.databridge.receiver.binary.internal.BinaryDataReceiver;
import org.wso2.carbon.databridge.receiver.thrift.ThriftDataReceiver;
import org.wso2.carbon.user.api.UserStoreException;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DecimalFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Wso2EventServer {
    private static Log log = LogFactory.getLog(Wso2EventServer.class);
    private ThriftDataReceiver thriftDataReceiver;
    private BinaryDataReceiver binaryDataReceiver;
    private AtomicLong counter = new AtomicLong(0);
    private AbstractStreamDefinitionStore streamDefinitionStore = new InMemoryStreamDefinitionStore();
    private static final Wso2EventServer testServer = new Wso2EventServer();


    public static void main(String[] args) throws DataBridgeException, StreamDefinitionStoreException, URISyntaxException {

        String cepHome = "/home/manorama/Products/CEP/wso2cep-4.0.0";
        int noOfFeatures = 100;
        int elapsedCount = 10000;
        String calcType = "throughput";// throughput/latency
        String protocol = "thrift";
        String host = "localhost";
        int port = 7661;

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    log.info("Final event count: " + testServer.counter.get());
                } catch (Throwable t) {
                    log.error("Unexpected error when running shutdown hook:" + t.getMessage(), t);
                }
            }
        });
        log.info("Shutdown hook added.");

        WSO2EventServerUtil.setCepHome(cepHome);
        testServer.start(host, port, protocol, elapsedCount, calcType, noOfFeatures);
        synchronized (testServer) {
            try {
                testServer.wait();
            } catch (InterruptedException ignored) {
                //ignore
            }
        }
    }


    public void start(String host, int receiverPort, String protocol, final int elapsedCount, String calcType, int noOfFeatures)
            throws DataBridgeException, StreamDefinitionStoreException, URISyntaxException {
        WSO2EventServerUtil.setKeyStoreParams();

        DataBridge databridge = new DataBridge(new AuthenticationHandler() {
            @Override
            public boolean authenticate(String userName,
                                        String password) {
                return true;
            }

            @Override
            public String getTenantDomain(String userName) {
                return "carbon.super";
            }

            @Override
            public int getTenantId(String s) throws UserStoreException {
                return -1234;
            }

            @Override
            public void initContext(AgentSession agentSession) {

            }

            @Override
            public void destroyContext(AgentSession agentSession) {

            }

        }, streamDefinitionStore, WSO2EventServerUtil.getDataBridgeConfigPath());

        streamDefinitionStore.saveStreamDefinitionToStore(WSO2EventServerUtil.loadStream(noOfFeatures), -1234);

        if ("latency".equalsIgnoreCase(calcType)) {
            databridge.subscribe(new LatencyAgentCallback(elapsedCount));
        } else if ("raw-latency".equalsIgnoreCase(calcType)) {
            databridge.subscribe(new RawDataLatencyAgentCallback(elapsedCount));
        } else {
            databridge.subscribe(new ThroughputAgentCallback(elapsedCount));
        }


        if (protocol.equalsIgnoreCase("binary")) {
            binaryDataReceiver = new BinaryDataReceiver(
                    new BinaryDataReceiverConfiguration(receiverPort + 100, receiverPort), databridge);
            try {
                binaryDataReceiver.start();
            } catch (IOException e) {
                log.error("Error starting binary data receiver: " + e.getMessage(), e);
            }
        } else {
            thriftDataReceiver = new ThriftDataReceiver(receiverPort, databridge);
            thriftDataReceiver.start(host);
        }
        log.info("WSO2 Event Server Started");
    }

    class ThroughputAgentCallback implements AgentCallback {
        private AtomicLong totalDelay = new AtomicLong(0);
        private AtomicLong lastIndex = new AtomicLong(0);
        private AtomicLong lastCounter = new AtomicLong(0);
        private AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
        private AtomicBoolean calcInProgress = new AtomicBoolean(false);
        private DecimalFormat decimalFormat = new DecimalFormat("#.##");
        private int elapsedCount = 0;

        public ThroughputAgentCallback(int elapsedCount) {
            this.elapsedCount = elapsedCount;
        }

        public void definedStream(StreamDefinition streamDefinition,
                                  int tenantID) {
        }

        @Override
        public void removeStream(StreamDefinition streamDefinition, int tenantID) {
            //To change body of implemented methods use File | Settings | File Templates.
            log.info("Test");
        }

        @Override
        public void receive(List<Event> eventList, Credentials credentials) {
            long currentTime = System.currentTimeMillis();
            long currentBatchTotalDelay = 0;
            for (Event event : eventList) {
                currentBatchTotalDelay = currentBatchTotalDelay + (currentTime - event.getTimeStamp());
            }
            /** Following section should ideally be atomic **/
            long localTotalDelay = totalDelay.addAndGet(currentBatchTotalDelay);
            long localCounter = counter.addAndGet(eventList.size());
            /** End of wish for atomic section **/

            long index = localCounter / elapsedCount;

            if (lastIndex.get() != index) {
                if (calcInProgress.compareAndSet(false, true)) {
                    //TODO Can be made thread safe further
                    lastIndex.set(index);
                    long currentWindowEventsReceived = localCounter - lastCounter.getAndSet(localCounter);
                    //log.info("Current time: " + System.currentTimeMillis() + ", Event received time: " + currentTime + ", Last calculation time: " + lastTime.get());
                    long elapsedTime = currentTime - lastTime.getAndSet(currentTime);
                    double throughputPerSecond = (((double) currentWindowEventsReceived) / elapsedTime) * 1000;

                    log.info("[" + Thread.currentThread().getName() + "] Received " + currentWindowEventsReceived + " sensor events in " + elapsedTime
                            + " milliseconds with total throughput of " + decimalFormat.format(throughputPerSecond)
                            + " events per second. Average delay is " + decimalFormat.format(localTotalDelay / (double) currentWindowEventsReceived));
                    totalDelay.addAndGet(-localTotalDelay);
                    calcInProgress.set(false);
                }
            }
        }
    }

    class LatencyAgentCallback implements AgentCallback {
        private AtomicLong totalDelay = new AtomicLong(0);
        private AtomicLong lastIndex = new AtomicLong(0);
        private AtomicLong lastCounter = new AtomicLong(0);
        private AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
        private AtomicLong maxLatency = new AtomicLong(0);
        private AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
        private AtomicBoolean calcInProgress = new AtomicBoolean(false);
        private DecimalFormat decimalFormat = new DecimalFormat("#.##");
        private int elapsedCount = 0;

        public LatencyAgentCallback(int elapsedCount) {
            this.elapsedCount = elapsedCount;
        }

        public void definedStream(StreamDefinition streamDefinition,
                                  int tenantID) {
        }

        @Override
        public void removeStream(StreamDefinition streamDefinition, int tenantID) {
            //To change body of implemented methods use File | Settings | File Templates.
            log.info("Test");
        }

        @Override
        public void receive(List<Event> eventList, Credentials credentials) {
            long currentTime = System.currentTimeMillis();
            long currentBatchTotalDelay = 0;
            for (Event event : eventList) {
                currentTime = System.currentTimeMillis();
                long currentEventLatency = currentTime - event.getTimeStamp();

                long currentMaxLatency = maxLatency.get();
                if (currentEventLatency > currentMaxLatency) {
                    maxLatency.compareAndSet(currentMaxLatency, currentEventLatency);
                }
                long currentMinLatency = minLatency.get();
                if (currentEventLatency < currentMinLatency) {
                    minLatency.compareAndSet(currentMinLatency, currentEventLatency);
                }
                currentBatchTotalDelay = currentBatchTotalDelay + currentEventLatency;
            }
            long localCounter = counter.addAndGet(eventList.size());
            long localTotalDelay = totalDelay.addAndGet(currentBatchTotalDelay);

            long index = localCounter / elapsedCount;

            if (lastIndex.get() != index) {
                if (calcInProgress.compareAndSet(false, true)) {
                    lastIndex.set(index);
                    long currentWindowEventsReceived = localCounter - lastCounter.getAndSet(localCounter);
                    long elapsedTime = currentTime - lastTime.getAndSet(currentTime);
                    log.info("Received " + currentWindowEventsReceived + " events in " + elapsedTime + " ms; Latency - Avg: "
                            + decimalFormat.format(localTotalDelay / (double) currentWindowEventsReceived)
                            + ", Min: " + minLatency.get() + ", Max: " + maxLatency.get());
                    maxLatency.set(0);
                    minLatency.set(Long.MAX_VALUE);
                    totalDelay.addAndGet(-localTotalDelay);
                    calcInProgress.set(false);
                }
            }

        }
    }

    class RawDataLatencyAgentCallback implements RawDataAgentCallback {
        private AtomicLong totalDelay = new AtomicLong(0);
        private AtomicLong lastIndex = new AtomicLong(0);
        private AtomicLong lastCounter = new AtomicLong(0);
        private AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
        private AtomicLong maxLatency = new AtomicLong(0);
        private AtomicLong minLatency = new AtomicLong(Long.MAX_VALUE);
        private AtomicBoolean calcInProgress = new AtomicBoolean(false);
        private DecimalFormat decimalFormat = new DecimalFormat("#.##");
        private int elapsedCount = 0;

        public RawDataLatencyAgentCallback(int elapsedCount) {
            this.elapsedCount = elapsedCount;
        }

        public void definedStream(StreamDefinition streamDefinition,
                                  int tenantID) {
        }

        @Override
        public void removeStream(StreamDefinition streamDefinition, int tenantID) {
            //To change body of implemented methods use File | Settings | File Templates.
            log.info("Test");
        }

        @Override
        public void receive(Object receivedObject) {
            long currentTime = System.currentTimeMillis();
            if (receivedObject instanceof EventComposite) {
                EventComposite eventComposite = (EventComposite) receivedObject;
                ThriftEventBundle thriftEventBundle = (ThriftEventBundle) eventComposite.getEventBundle();
                long currentBatchTotalDelay = 0;
                long eventTimestamp = thriftEventBundle.getLongAttributeList().get(0);
                long currentEventLatency = currentTime - eventTimestamp;
                long currentMaxLatency = maxLatency.get();
                if (currentEventLatency > currentMaxLatency) {
                    maxLatency.compareAndSet(currentMaxLatency, currentEventLatency);
                }
                long currentMinLatency = minLatency.get();
                if (currentEventLatency < currentMinLatency) {
                    minLatency.compareAndSet(currentMinLatency, currentEventLatency);
                }
                currentBatchTotalDelay = currentBatchTotalDelay + currentEventLatency;
                long localCounter = counter.addAndGet(thriftEventBundle.getEventNum());
                long localTotalDelay = totalDelay.addAndGet(currentBatchTotalDelay);

                long index = localCounter / elapsedCount;

                if (lastIndex.get() != index) {
                    if (calcInProgress.compareAndSet(false, true)) {
                        lastIndex.set(index);
                        long currentWindowEventsReceived = localCounter - lastCounter.getAndSet(localCounter);
                        long elapsedTime = currentTime - lastTime.getAndSet(currentTime);
                        log.info("Received " + currentWindowEventsReceived + " events in " + elapsedTime + " ms; Latency - Avg: "
                                + decimalFormat.format(localTotalDelay / (double) currentWindowEventsReceived)
                                + ", Min: " + minLatency.get() + ", Max: " + maxLatency.get());
                        maxLatency.set(0);
                        minLatency.set(Long.MAX_VALUE);
                        totalDelay.addAndGet(-localTotalDelay);
                        calcInProgress.set(false);
                    }
                }
            }
        }
    }

    public void stop() {
        if (thriftDataReceiver != null) {
            thriftDataReceiver.stop();
        }
        if (binaryDataReceiver != null) {
            binaryDataReceiver.stop();
        }
        log.info("Test Server Stopped");
    }
}