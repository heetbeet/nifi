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
import org.apache.nifi.controller.ThreadDetails;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.reporting.Severity;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.verification.VerificationMode;
import org.slf4j.Logger;

import java.util.Collections;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;

public class LongRunningTaskMonitorTest {
    private static final long ACTIVE_MILLIS = 1000;

    private static final long WARNING_THRESHOLD = ACTIVE_MILLIS / 2;

    private static final long NO_WARNING_THRESHOLD = ACTIVE_MILLIS * 2;

    @Test
    public void testRunReportWarnings() {
        final ThreadDetails threadDetails = mock(ThreadDetails.class);
        final TerminationAwareLogger processorLogger = mock(TerminationAwareLogger.class);
        final FlowManager flowManager = mockFlowManager(threadDetails, processorLogger);
        final EventReporter eventReporter = mock(EventReporter.class);
        final Logger monitorLogger = mock(Logger.class);

        final LongRunningTaskMonitor longRunningTaskMonitor = new LongRunningTaskMonitor(flowManager, eventReporter, WARNING_THRESHOLD) {
            @Override
            protected Logger getLogger() {
                return monitorLogger;
            }

            @Override
            protected ThreadDetails captureThreadDetails() {
                return threadDetails;
            }
        };

        longRunningTaskMonitor.run();

        verifyMonitorLogger(monitorLogger, atLeastOnce());
        verifyProcessorLogger(processorLogger, atLeastOnce());
        verifyEventReporter(eventReporter, atLeastOnce());
    }

    @Test
    public void testRunNoWarnings() {
        final ThreadDetails threadDetails = mock(ThreadDetails.class);
        final TerminationAwareLogger processorLogger = mock(TerminationAwareLogger.class);
        final FlowManager flowManager = mockFlowManager(threadDetails, processorLogger);
        final EventReporter eventReporter = mock(EventReporter.class);
        final Logger monitorLogger = mock(Logger.class);

        final LongRunningTaskMonitor longRunningTaskMonitor = new LongRunningTaskMonitor(flowManager, eventReporter, NO_WARNING_THRESHOLD) {
            @Override
            protected Logger getLogger() {
                return monitorLogger;
            }

            @Override
            protected ThreadDetails captureThreadDetails() {
                return threadDetails;
            }
        };

        longRunningTaskMonitor.run();

        verifyMonitorLogger(monitorLogger, never());
        verifyProcessorLogger(processorLogger, never());
        verifyEventReporter(eventReporter, never());
    }

    private void verifyMonitorLogger(final Logger logger, final VerificationMode verificationMode) {
        verify(logger, verificationMode).warn(anyString(), ArgumentMatchers.<Object[]>any());
    }

    private void verifyProcessorLogger(final TerminationAwareLogger logger, final VerificationMode verificationMode) {
        verify(logger, verificationMode).warn(anyString(), ArgumentMatchers.<Object[]>any());
    }

    private void verifyEventReporter(final EventReporter eventReporter, final VerificationMode verificationMode) {
        verify(eventReporter, verificationMode).reportEvent(eq(Severity.WARNING), anyString(), anyString());
    }

    private FlowManager mockFlowManager(final ThreadDetails threadDetails, final TerminationAwareLogger processorLogger) {
        final ActiveThreadInfo activeThreadInfo = mock(ActiveThreadInfo.class);
        when(activeThreadInfo.getThreadName()).thenReturn("Thread-1");
        when(activeThreadInfo.getActiveMillis()).thenReturn(ACTIVE_MILLIS);

        final ProcessorNode processorNode = mock(ProcessorNode.class);

        when(processorNode.getIdentifier()).thenReturn(UUID.randomUUID().toString());
        when(processorNode.getName()).thenReturn("Processor-Name");
        when(processorNode.getComponentType()).thenReturn("Processor-Type");
        when(processorNode.getLogger()).thenReturn(processorLogger);
        when(processorNode.getActiveThreads(threadDetails)).thenReturn(Collections.singletonList(activeThreadInfo));

        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.findAllProcessors()).thenReturn(Collections.singletonList(processorNode));
        final FlowManager flowManager = mock(FlowManager.class);
        when(flowManager.getRootGroup()).thenReturn(processGroup);
        return flowManager;
    }
}
