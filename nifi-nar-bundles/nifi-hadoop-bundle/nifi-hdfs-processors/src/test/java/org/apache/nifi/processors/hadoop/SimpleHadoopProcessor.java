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
package org.apache.nifi.processors.hadoop;

import org.apache.nifi.hadoop.KerberosProperties;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;

import java.io.File;

public class SimpleHadoopProcessor extends AbstractHadoopProcessor {

    private KerberosProperties testKerberosProperties;
    private boolean allowExplicitKeytab;
    private boolean localFileSystemAccessDenied;

    public SimpleHadoopProcessor(KerberosProperties kerberosProperties) {
        this(kerberosProperties, true, true);
    }

    public SimpleHadoopProcessor(KerberosProperties kerberosProperties, boolean allowExplicitKeytab) {
        this.testKerberosProperties = kerberosProperties;
        this.allowExplicitKeytab = allowExplicitKeytab;
    }

    public SimpleHadoopProcessor(KerberosProperties kerberosProperties, boolean allowExplicitKeytab, boolean localFileSystemAccessDenied) {
        this.testKerberosProperties = kerberosProperties;
        this.allowExplicitKeytab = allowExplicitKeytab;
        this.localFileSystemAccessDenied = localFileSystemAccessDenied;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    }

    @Override
    protected KerberosProperties getKerberosProperties(File kerberosConfigFile) {
        return testKerberosProperties;
    }

    @Override
    boolean isAllowExplicitKeytab() {
        return allowExplicitKeytab;
    }

    @Override
    boolean isLocalFileSystemAccessDenied() {
        return localFileSystemAccessDenied;
    }
}
