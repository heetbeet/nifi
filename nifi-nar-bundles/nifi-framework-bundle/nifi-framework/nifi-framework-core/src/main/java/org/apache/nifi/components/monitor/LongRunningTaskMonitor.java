/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.components.monitor;

import org.apache.nifi.controller.ActiveThreadInfo;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.management.thread.ThreadDump;
import org.apache.nifi.management.thread.ThreadDumpProvider;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * Long Running Task Monitor evaluates Processor Active Threads using a configurable threshold to publish warnings
 */
public class LongRunningTaskMonitor implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LongRunningTaskMonitor.class);

    private final FlowManager flowManager;
    private final EventReporter eventReporter;
    private final ThreadDumpProvider threadDumpProvider;
    private final long thresholdMillis;

    public LongRunningTaskMonitor(final FlowManager flowManager,
                                  final EventReporter eventReporter,
                                  final ThreadDumpProvider threadDumpProvider,
                                  final long thresholdMillis) {
        this.flowManager = Objects.requireNonNull(flowManager, "Flow Manager required");
        this.eventReporter = Objects.requireNonNull(eventReporter, "Event Reporter required");
        this.threadDumpProvider = Objects.requireNonNull(threadDumpProvider, "Thread Dump Provider required");
        this.thresholdMillis = thresholdMillis;
    }

    @Override
    public void run() {
        final long start = System.nanoTime();

        int activeThreadCount = 0;
        int longRunningThreadCount = 0;

        final ThreadDump threadDump = threadDumpProvider.getThreadDump();

        for (final ProcessorNode processorNode : flowManager.getRootGroup().findAllProcessors()) {
            final List<ActiveThreadInfo> activeThreads = processorNode.getActiveThreads(threadDump);
            activeThreadCount += activeThreads.size();

            for (final ActiveThreadInfo activeThread : activeThreads) {
                if (activeThread.getActiveMillis() > thresholdMillis) {
                    longRunningThreadCount++;
                    reportActiveThread(processorNode, activeThread);
                }
            }
        }

        final long duration = System.nanoTime() - start;
        LOGGER.info("Threads Active [{}] Long Running [{}] Task Duration [{} nanos]", activeThreadCount, longRunningThreadCount, duration);
    }

    private void reportActiveThread(final ProcessorNode processorNode, final ActiveThreadInfo activeThreadInfo) {
        final String id = processorNode.getIdentifier();
        final String name = processorNode.getName();
        final String type = processorNode.getComponentType();
        final String threadName = activeThreadInfo.getThreadName();
        final long activeMillis = activeThreadInfo.getActiveMillis();
        final String stackTrace = activeThreadInfo.getStackTrace();

        processorNode.getLogger().warn("Long Running Task Found: Thread Name [{}] Duration [{} ms]", new Object[]{ threadName, activeMillis });
        LOGGER.warn("Long Running Task Found: Processor ID [{}] Name [{}] Duration [{} ms]\n{}", id, name, activeMillis, stackTrace);

        final String message = String.format("Processor ID [%s] Name [%s] Type [%s] Thread Name [%s] Duration [%d ms]", id, name, type, threadName, activeMillis);
        eventReporter.reportEvent(Severity.WARNING, "Long Running Task", message);
    }
}
