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
package org.apache.nifi.management.thread;

import org.apache.nifi.util.ThreadUtils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Standard Thread Dump Provider implementation using ThreadMXBean
 */
public class StandardThreadDumpProvider implements ThreadDumpProvider {
    /**
     * Get Thread Summaries using ThreadMXBean.dumpAllThreads()
     *
     * @return Map of Thread Identifier to Thread Summary
     */
    @Override
    public ThreadDump getThreadDump() {
        final ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        final ThreadInfo[] threads = bean.dumpAllThreads(bean.isObjectMonitorUsageSupported(), bean.isSynchronizerUsageSupported());
        final long[] deadlockedThreads = bean.findDeadlockedThreads();
        final long[] monitorDeadlockedThreads = bean.findMonitorDeadlockedThreads();

        final Map<Long, ThreadSummary> summaries = Arrays.stream(threads).collect(
                Collectors.toMap(
                        ThreadInfo::getThreadId, threadInfo -> getThreadSummary(threadInfo, deadlockedThreads, monitorDeadlockedThreads)
                )
        );
        return new ThreadDump(summaries);
    }

    private ThreadSummary getThreadSummary(final ThreadInfo threadInfo, final long[] deadlockedThreads, final long[] monitorDeadlockedThreads) {
        final String stackTrace = ThreadUtils.createStackTrace(threadInfo, deadlockedThreads, monitorDeadlockedThreads);
        return new ThreadSummary(threadInfo.getThreadId(), threadInfo.getThreadName(), threadInfo.getThreadState().name(), stackTrace);
    }
}
