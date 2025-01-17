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
package org.apache.nifi.processors.azure.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageUri;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.PrimaryNodeOnly;
import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.configuration.DefaultSchedule;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processor.util.list.AbstractListProcessor;
import org.apache.nifi.processor.util.list.ListedEntityTracker;
import org.apache.nifi.processors.azure.storage.utils.AzureStorageUtils;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo;
import org.apache.nifi.processors.azure.storage.utils.BlobInfo.Builder;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Optional;

@PrimaryNodeOnly
@TriggerSerially
@Tags({ "azure", "microsoft", "cloud", "storage", "blob" })
@SeeAlso({ FetchAzureBlobStorage.class, PutAzureBlobStorage.class, DeleteAzureBlobStorage.class })
@CapabilityDescription("Lists blobs in an Azure Storage container. Listing details are attached to an empty FlowFile for use with FetchAzureBlobStorage.  " +
        "This Processor is designed to run on Primary Node only in a cluster. If the primary node changes, the new Primary Node will pick up where the " +
        "previous node left off without duplicating all of the data.")
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@WritesAttributes({ @WritesAttribute(attribute = "azure.container", description = "The name of the Azure container"),
        @WritesAttribute(attribute = "azure.blobname", description = "The name of the Azure blob"),
        @WritesAttribute(attribute = "azure.primaryUri", description = "Primary location for blob content"),
        @WritesAttribute(attribute = "azure.secondaryUri", description = "Secondary location for blob content"),
        @WritesAttribute(attribute = "azure.etag", description = "Etag for the Azure blob"),
        @WritesAttribute(attribute = "azure.length", description = "Length of the blob"),
        @WritesAttribute(attribute = "azure.timestamp", description = "The timestamp in Azure for the blob"),
        @WritesAttribute(attribute = "mime.type", description = "MimeType of the content"),
        @WritesAttribute(attribute = "lang", description = "Language code for the content"),
        @WritesAttribute(attribute = "azure.blobtype", description = "This is the type of blob and can be either page or block type") })
@Stateful(scopes = { Scope.CLUSTER }, description = "After performing a listing of blobs, the timestamp of the newest blob is stored. " +
        "This allows the Processor to list only blobs that have been added or modified after this date the next time that the Processor is run.  State is " +
        "stored across the cluster so that this Processor can be run on Primary Node only and if a new Primary Node is selected, the new node can pick up " +
        "where the previous node left off, without duplicating the data.")
@DefaultSchedule(strategy = SchedulingStrategy.TIMER_DRIVEN, period = "1 min")
public class ListAzureBlobStorage extends AbstractListAzureProcessor<BlobInfo> {

    private static final PropertyDescriptor PROP_PREFIX = new PropertyDescriptor.Builder()
            .name("prefix")
            .displayName("Prefix")
            .description("Search prefix for listing")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .required(false)
            .build();

    private static final List<PropertyDescriptor> PROPERTIES = Collections.unmodifiableList(Arrays.asList(
            LISTING_STRATEGY,
            AbstractListProcessor.RECORD_WRITER,
            AzureStorageUtils.CONTAINER,
            AzureStorageUtils.STORAGE_CREDENTIALS_SERVICE,
            AzureStorageUtils.ACCOUNT_NAME,
            AzureStorageUtils.ACCOUNT_KEY,
            AzureStorageUtils.PROP_SAS_TOKEN,
            AzureStorageUtils.ENDPOINT_SUFFIX,
            PROP_PREFIX,
            AzureStorageUtils.PROXY_CONFIGURATION_SERVICE,
            ListedEntityTracker.TRACKING_STATE_CACHE,
            ListedEntityTracker.TRACKING_TIME_WINDOW,
            ListedEntityTracker.INITIAL_LISTING_TARGET,
            MIN_AGE,
            MAX_AGE,
            MIN_SIZE,
            MAX_SIZE
    ));

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTIES;
    }

    @Override
    protected void customValidate(ValidationContext validationContext, Collection<ValidationResult> results) {
        results.addAll(AzureStorageUtils.validateCredentialProperties(validationContext));
        AzureStorageUtils.validateProxySpec(validationContext, results);
    }

    @Override
    protected Map<String, String> createAttributes(BlobInfo entity, ProcessContext context) {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("azure.container", entity.getContainerName());
        attributes.put("azure.etag", entity.getEtag());
        attributes.put("azure.primaryUri", entity.getPrimaryUri());
        attributes.put("azure.secondaryUri", entity.getSecondaryUri());
        attributes.put("azure.blobname", entity.getBlobName());
        attributes.put("filename", entity.getName());
        attributes.put("azure.blobtype", entity.getBlobType());
        attributes.put("azure.length", String.valueOf(entity.getLength()));
        attributes.put("azure.timestamp", String.valueOf(entity.getTimestamp()));
        attributes.put("mime.type", entity.getContentType());
        attributes.put("lang", entity.getContentLanguage());

        return attributes;
    }

    @Override
    protected String getListingContainerName(final ProcessContext context) {
        return String.format("Azure Blob Storage Container [%s]", getPath(context));
    }

    @Override
    protected String getPath(final ProcessContext context) {
        return context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions().getValue();
    }

    @Override
    protected boolean isListingResetNecessary(final PropertyDescriptor property) {
        // re-list if configuration changed, but not when security keys are rolled (not included in the condition)
        return PROP_PREFIX.equals(property)
                || AzureStorageUtils.ACCOUNT_NAME.equals(property)
                || AzureStorageUtils.CONTAINER.equals(property)
                || AzureStorageUtils.PROP_SAS_TOKEN.equals(property);
    }

    @Override
    protected Scope getStateScope(final PropertyContext context) {
        return Scope.CLUSTER;
    }

    @Override
    protected RecordSchema getRecordSchema() {
        return BlobInfo.getRecordSchema();
    }

    @Override
    protected String getDefaultTimePrecision() {
        // User does not have to choose one.
        // AUTO_DETECT can handle most cases, but it may incur longer latency
        // when all listed files do not have SECOND part in their timestamps although Azure Blob Storage does support seconds.
        return PRECISION_SECONDS.getValue();
    }

    @Override
    protected List<BlobInfo> performListing(final ProcessContext context, final Long minTimestamp, final ListingMode listingMode) throws IOException {
        final String containerName = context.getProperty(AzureStorageUtils.CONTAINER).evaluateAttributeExpressions().getValue();
        final String prefix = Optional.ofNullable(context.getProperty(PROP_PREFIX).evaluateAttributeExpressions().getValue()).orElse("");
        final List<BlobInfo> listing = new ArrayList<>();
        final long minimumTimestamp = minTimestamp == null ? 0 : minTimestamp;

        try {
            final CloudBlobClient blobClient = AzureStorageUtils.createCloudBlobClient(context, getLogger(), null);
            final CloudBlobContainer container = blobClient.getContainerReference(containerName);

            final OperationContext operationContext = new OperationContext();
            AzureStorageUtils.setProxy(operationContext, context);

            ResultContinuation continuationToken = null;

            do {
                final ResultSegment<ListBlobItem> result = container.listBlobsSegmented(prefix, true, EnumSet.of(BlobListingDetails.METADATA), null, continuationToken, null, operationContext);
                continuationToken = result.getContinuationToken();

                for (final ListBlobItem blob : result.getResults()) {
                    if (blob instanceof CloudBlob) {
                        final CloudBlob cloudBlob = (CloudBlob) blob;
                        final BlobProperties properties = cloudBlob.getProperties();

                        if (isFileInfoMatchesWithAgeAndSize(context, minimumTimestamp, properties.getLastModified().getTime(), properties.getLength())) {
                            final StorageUri uri = cloudBlob.getSnapshotQualifiedStorageUri();

                            final Builder builder = new BlobInfo.Builder()
                                    .primaryUri(uri.getPrimaryUri().toString())
                                    .blobName(cloudBlob.getName())
                                    .containerName(containerName)
                                    .contentType(properties.getContentType())
                                    .contentLanguage(properties.getContentLanguage())
                                    .etag(properties.getEtag())
                                    .lastModifiedTime(properties.getLastModified().getTime())
                                    .length(properties.getLength());

                            if (uri.getSecondaryUri() != null) {
                                builder.secondaryUri(uri.getSecondaryUri().toString());
                            }

                            if (blob instanceof CloudBlockBlob) {
                                builder.blobType(AzureStorageUtils.BLOCK);
                            } else {
                                builder.blobType(AzureStorageUtils.PAGE);
                            }
                            listing.add(builder.build());
                        }
                    }
                }
            } while (continuationToken != null);
        } catch (final Throwable t) {
            throw new IOException(ExceptionUtils.getRootCause(t));
        }
        return listing;
    }

    // Unfiltered listing is not supported - must provide a prefix
    @Override
    protected Integer countUnfilteredListing(final ProcessContext context) {
        return null;
    }
}
