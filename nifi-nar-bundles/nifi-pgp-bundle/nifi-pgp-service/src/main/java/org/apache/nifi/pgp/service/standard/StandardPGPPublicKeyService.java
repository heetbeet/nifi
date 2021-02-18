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
package org.apache.nifi.pgp.service.standard;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.pgp.service.api.PGPPublicKeyService;
import org.apache.nifi.pgp.service.standard.exception.PGPConfigurationException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.StringUtils;

import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;
import org.bouncycastle.openpgp.operator.KeyFingerPrintCalculator;
import org.bouncycastle.openpgp.operator.jcajce.JcaKeyFingerprintCalculator;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Standard Pretty Good Privacy Public Key Service reads Public Keys from configured Keyring files
 */
@Tags({"PGP", "GPG", "OpenPGP", "Encryption", "Private", "Key", "RFC 4880"})
@CapabilityDescription("PGP Public Key Service providing Public Keys loaded from files")
public class StandardPGPPublicKeyService extends AbstractControllerService implements PGPPublicKeyService {
    public static final PropertyDescriptor KEYRING_FILE = new PropertyDescriptor.Builder()
            .name("keyring-file")
            .displayName("Keyring File")
            .description("File path for PGP Keyring or Public Key encoded in binary or ASCII Armor")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor KEYRING = new PropertyDescriptor.Builder()
            .name("keyring")
            .displayName("Keyring")
            .description("PGP Keyring or Public Key encoded in ASCII Armor")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final Charset KEY_CHARSET = StandardCharsets.US_ASCII;

    private static final boolean PARALLEL_DISABLED = false;

    private static final List<PropertyDescriptor> DESCRIPTORS = Arrays.asList(
            KEYRING_FILE,
            KEYRING
    );

    private volatile Map<Long, PGPPublicKey> publicKeys = Collections.emptyMap();

    /**
     * On Enabled reads Keyring using configured properties
     *
     * @param context Configuration Context with properties
     * @throws InitializationException Thrown when unable to load Keyring
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        try {
            final List<PGPPublicKey> extractedPublicKeys = new ArrayList<>(readKeyringFile(context));
            extractedPublicKeys.addAll(readKeyring(context));

            publicKeys = extractedPublicKeys.stream().collect(
                    Collectors.toMap(
                            publicKey -> publicKey.getKeyID(),
                            publicKey -> publicKey
                    )
            );
        } catch (final RuntimeException e) {
            throw new InitializationException("Reading Public Keys Failed", e);
        }
    }

    /**
     * On Disabled clears Public Keys
     */
    @OnDisabled
    public void onDisabled() {
        publicKeys = Collections.emptyMap();
    }

    /**
     * Find Public Key matching Key Identifier
     *
     * @param keyIdentifier Public Key Identifier
     * @return Optional container for PGP Public Key empty when no matching Key found
     */
    @Override
    public Optional<PGPPublicKey> findPublicKey(final long keyIdentifier) {
        getLogger().debug("Find Public Key [{}]", Long.toHexString(keyIdentifier).toUpperCase());
        return Optional.ofNullable(publicKeys.get(keyIdentifier));
    }

    /**
     * Get Supported Property Descriptors
     *
     * @return Supported Property Descriptors
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    /**
     * Custom Validate reading keyring using provided password
     *
     * @param context Validation Context
     * @return Validation Results
     */
    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
        final Collection<ValidationResult> results = new ArrayList<>();

        final List<PGPPublicKey> extractedPublicKeys = new ArrayList<>();

        try {
            extractedPublicKeys.addAll(readKeyringFile(context));
        } catch (final RuntimeException e) {
            final ValidationResult result = new ValidationResult.Builder()
                    .valid(false)
                    .subject(KEYRING_FILE.getDisplayName())
                    .explanation(String.format("Reading Public Keyring File Failed: %s", e.getMessage()))
                    .build();
            results.add(result);
        }

        try {
            extractedPublicKeys.addAll(readKeyring(context));
        } catch (final RuntimeException e) {
            final ValidationResult result = new ValidationResult.Builder()
                    .valid(false)
                    .subject(KEYRING.getDisplayName())
                    .explanation(String.format("Reading Public Keyring Failed: %s", e.getMessage()))
                    .build();
            results.add(result);
        }

        if (extractedPublicKeys.isEmpty()) {
            final String explanation = String.format("No Public Keys Read from [%s] or [%s]", KEYRING_FILE.getDisplayName(), KEYRING.getDisplayName());
            final ValidationResult result = new ValidationResult.Builder()
                    .valid(false)
                    .subject(getClass().getSimpleName())
                    .explanation(explanation)
                    .build();
            results.add(result);
        }

        return results;
    }

    private List<PGPPublicKey> readKeyringFile(final PropertyContext context) {
        final List<PGPPublicKey> extractedPublicKeys = new ArrayList<>();
        final String keyringFile = context.getProperty(KEYRING_FILE).evaluateAttributeExpressions().getValue();
        if (StringUtils.isNotBlank(keyringFile)) {
            try (final InputStream inputStream = new FileInputStream(keyringFile)) {
                extractedPublicKeys.addAll(extractPublicKeys(inputStream));
            } catch (final IOException | RuntimeException e) {
                final String message = String.format("Reading Public Keyring File [%s] Failed", keyringFile);
                throw new PGPConfigurationException(message, e);
            }
        }
        return extractedPublicKeys;
    }

    private List<PGPPublicKey> readKeyring(final PropertyContext context) {
        final List<PGPPublicKey> extractedPublicKeys = new ArrayList<>();
        final String keyring = context.getProperty(KEYRING).getValue();
        if (StringUtils.isNotBlank(keyring)) {
            final byte[] keyringBytes = keyring.getBytes(KEY_CHARSET);
            try (final InputStream inputStream = new ByteArrayInputStream(keyringBytes)) {
                extractedPublicKeys.addAll(extractPublicKeys(inputStream));
            } catch (final IOException | RuntimeException e) {
                throw new PGPConfigurationException("Reading Public Keyring Failed", e);
            }
        }
        return extractedPublicKeys;
    }

    private List<PGPPublicKey> extractPublicKeys(final InputStream inputStream) throws IOException {
        final KeyFingerPrintCalculator calculator = new JcaKeyFingerprintCalculator();
        try (final InputStream decoderStream = PGPUtil.getDecoderStream(inputStream)) {
            final PGPPublicKeyRingCollection keyRings = new PGPPublicKeyRingCollection(decoderStream, calculator);
            return StreamSupport.stream(keyRings.spliterator(), PARALLEL_DISABLED)
                    .flatMap(keyRing -> StreamSupport.stream(keyRing.spliterator(), PARALLEL_DISABLED))
                    .collect(Collectors.toList());
        } catch (final PGPException e) {
            throw new PGPConfigurationException("Reading Public Keyring Collection Failed", e);
        }
    }
}
