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
package org.apache.nifi.web.api.metrics;

import io.prometheus.client.Collector;

import java.util.Enumeration;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Enumeration wrapping Prometheus Collector Samples with filtering based on multiple patterns
 */
public class FilteringMetricFamilySamplesEnumeration implements Enumeration<Collector.MetricFamilySamples> {
    private final Enumeration<Collector.MetricFamilySamples> metricFamilySamples;

    private final Pattern sampleNamePattern;

    private final Pattern sampleLabelValuePattern;

    private Collector.MetricFamilySamples nextElement;

    /**
     * Filtering Metric Family Samples Enumeration with required properties
     *
     * @param metricFamilySamples Metric Family Samples to be filtered
     * @param sampleNamePattern Pattern used to match against Sample.name field
     * @param sampleLabelValuePattern Pattern used to matching against Sample.labelValues field
     */
    public FilteringMetricFamilySamplesEnumeration(
            final Enumeration<Collector.MetricFamilySamples> metricFamilySamples,
            final Pattern sampleNamePattern,
            final Pattern sampleLabelValuePattern
    ) {
        this.metricFamilySamples = Objects.requireNonNull(metricFamilySamples);
        this.sampleNamePattern = Objects.requireNonNull(sampleNamePattern, "Sample Name Pattern required");
        this.sampleLabelValuePattern = Objects.requireNonNull(sampleLabelValuePattern, "Sample Label Values Pattern required");
        setNextElement();
    }

    /**
     * Has More Elements based on whether the next element is set from a previous operation
     *
     * @return More Elements status
     */
    @Override
    public boolean hasMoreElements() {
        return nextElement != null;
    }

    /**
     * Get Next Element and set next available element before returning
     *
     * @return Next Element based on applied filters
     */
    @Override
    public Collector.MetricFamilySamples nextElement() {
        if (nextElement == null) {
            throw new NoSuchElementException();
        }
        final Collector.MetricFamilySamples currentElement = nextElement;
        setNextElement();
        return currentElement;
    }

    /**
     * Set Next Element based on Sample having matching properties
     */
    private void setNextElement() {
        nextElement = null;
        while (metricFamilySamples.hasMoreElements()) {
            final Collector.MetricFamilySamples possibleNextElement = metricFamilySamples.nextElement();
            filterSamples(possibleNextElement);
            if (possibleNextElement.samples.size() == 0) {
                continue;
            }
            nextElement = possibleNextElement;
            break;
        }
    }

    private void filterSamples(final Collector.MetricFamilySamples element) {
        final Iterator<Collector.MetricFamilySamples.Sample> iterator = element.samples.iterator();
        samples: while (iterator.hasNext()) {
            final Collector.MetricFamilySamples.Sample sample = iterator.next();

            final Matcher sampleNameMatcher = sampleNamePattern.matcher(sample.name);
            if (!sampleNameMatcher.matches()) {
                iterator.remove();
                continue;
            }

            for (final String labelValue : sample.labelValues) {
                final Matcher sampleLabelValueMatcher = sampleLabelValuePattern.matcher(labelValue);
                if (sampleLabelValueMatcher.matches()) {
                    continue samples;
                }
            }
            iterator.remove();
        }
    }
}
