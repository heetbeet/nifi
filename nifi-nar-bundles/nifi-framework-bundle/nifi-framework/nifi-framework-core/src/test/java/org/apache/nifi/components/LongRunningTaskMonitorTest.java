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
package org.apache.nifi.components;

import org.apache.nifi.components.monitor.LongRunningTaskMonitor;
import org.apache.nifi.controller.ActiveThreadInfo;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.management.thread.ThreadDump;
import org.apache.nifi.management.thread.ThreadDumpProvider;
import org.apache.nifi.management.thread.ThreadSummary;
import org.apache.nifi.reporting.Severity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class LongRunningTaskMonitorTest {

    private static final String STACKTRACE = "line1\nline2";

    private static final long FIRST_THREAD_ID = 1024;

    private static final String FIRST_THREAD_NAME = "FirstThread";

    private static final long LONG_RUNNING_THREAD_ID = 2048;

    private static final String LONG_RUNNING_THREAD_NAME = "LongRunningThread";

    private static final String RUNNABLE = "RUNNABLE";

    private static final long THRESHOLD = 1000;

    private static final long THRESHOLD_EXCEEDED = 2000;

    private static final String FIRST_PROCESSOR_ID = "PROCESSOR-1";

    private static final String FIRST_PROCESSOR_NAME = "FirstProcessor";

    private static final String PROCESSOR_TYPE = "SimpleProcessor";

    private static final ActiveThreadInfo ACTIVE_THREAD = new ActiveThreadInfo(FIRST_THREAD_NAME, STACKTRACE, THRESHOLD, false);

    private static final ActiveThreadInfo LONG_RUNNING_ACTIVE_THREAD = new ActiveThreadInfo(LONG_RUNNING_THREAD_NAME, STACKTRACE, THRESHOLD_EXCEEDED, false);

    private static final String EVENT_CATEGORY = "Long Running Task";

    @Mock
    private EventReporter eventReporter;

    @Mock
    private FlowManager flowManager;

    @Mock
    private ProcessGroup processGroup;

    @Mock
    private ThreadDumpProvider threadDumpProvider;

    @Mock
    private TerminationAwareLogger firstProcessorLogger;

    @Captor
    private ArgumentCaptor<String> eventMessageCaptor;

    private LongRunningTaskMonitor monitor;

    private ThreadDump threadDump;

    @BeforeEach
    public void setFlowManager() {
        when(flowManager.getRootGroup()).thenReturn(processGroup);

        final ThreadSummary firstThreadSummary = new ThreadSummary(FIRST_THREAD_ID, FIRST_THREAD_NAME, RUNNABLE, STACKTRACE);
        final ThreadSummary longRunningThreadSummary = new ThreadSummary(LONG_RUNNING_THREAD_ID, LONG_RUNNING_THREAD_NAME, RUNNABLE, STACKTRACE);

        final Map<Long, ThreadSummary> summaries = new HashMap<>();
        summaries.put(firstThreadSummary.getId(), firstThreadSummary);
        summaries.put(longRunningThreadSummary.getId(), longRunningThreadSummary);
        threadDump = new ThreadDump(summaries);
        when(threadDumpProvider.getThreadDump()).thenReturn(threadDump);

        monitor = new LongRunningTaskMonitor(flowManager, eventReporter, threadDumpProvider, THRESHOLD);
    }

    @Test
    public void testRunReportEvent() {
        final ProcessorNode longRunningProcessor = mock(ProcessorNode.class);
        when(longRunningProcessor.getActiveThreads(threadDump)).thenReturn(Arrays.asList(LONG_RUNNING_ACTIVE_THREAD, ACTIVE_THREAD));
        when(longRunningProcessor.getIdentifier()).thenReturn(FIRST_PROCESSOR_ID);
        when(longRunningProcessor.getName()).thenReturn(FIRST_PROCESSOR_NAME);
        when(longRunningProcessor.getComponentType()).thenReturn(PROCESSOR_TYPE);
        when(longRunningProcessor.getLogger()).thenReturn(firstProcessorLogger);

        final ProcessorNode processor = mock(ProcessorNode.class);
        when(processor.getActiveThreads(threadDump)).thenReturn(Collections.singletonList(ACTIVE_THREAD));

        when(processGroup.findAllProcessors()).thenReturn(Arrays.asList(longRunningProcessor, processor));
        when(flowManager.getRootGroup()).thenReturn(processGroup);

        monitor.run();

        verify(eventReporter).reportEvent(eq(Severity.WARNING), eq(EVENT_CATEGORY), eventMessageCaptor.capture());

        final String eventMessage = eventMessageCaptor.getValue();
        assertTrue(eventMessage.contains(FIRST_PROCESSOR_ID));
        assertTrue(eventMessage.contains(PROCESSOR_TYPE));
        assertTrue(eventMessage.contains(FIRST_PROCESSOR_NAME));
        assertTrue(eventMessage.contains(LONG_RUNNING_THREAD_NAME));
    }
}
