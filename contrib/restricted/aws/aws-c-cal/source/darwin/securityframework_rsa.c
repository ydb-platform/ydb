/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/cal/private/rsa.h>

#include <aws/cal/cal.h>
#include <aws/common/encoding.h>

#include <Security/SecKey.h>
#include <Security/Security.h>

struct sec_rsa_key_pair {
    struct aws_rsa_key_pair base;
    CFAllocatorRef cf_allocator;
    SecKeyRef priv_key_ref;
    SecKeyRef pub_key_ref;
};

static void s_rsa_destroy_key(void *key_pair) {
    if (key_pair == NULL) {
        return;
    }

    struct aws_rsa_key_pair *base = key_pair;
    struct sec_rsa_key_pair *impl = base->impl;

    if (impl->pub_key_ref) {
        CFRelease(impl->pub_key_ref);
    }

    if (impl->priv_key_ref) {
        CFRelease(impl->priv_key_ref);
    }

    if (impl->cf_allocator) {
        aws_wrapped_cf_allocator_destroy(impl->cf_allocator);
    }

    aws_rsa_key_pair_base_clean_up(base);

    aws_mem_release(base->allocator, impl);
}

/*
 * Transforms security error code into crt error code and raises it as necessary.
 * Docs on what security apis can throw are fairly sparse and so far in testing
 * it only threw generic -50 error. So just log for now and we can add additional
 * error translation later.
 */
static int s_reinterpret_sec_error_as_crt(CFErrorRef error, const char *function_name) {
    if (error == NULL) {
        return AWS_OP_SUCCESS;
    }

    CFIndex error_code = CFErrorGetCode(error);
    CFStringRef error_message = CFErrorCopyDescription(error); /* This function never returns NULL */

    /*
     * Note: CFStringGetCStringPtr returns NULL quite often.
     * Refer to writeup at the start of CFString.h as to why.
     * To reliably get an error message we need to use the following function
     * that will copy error string into our buffer.
     */
    const char *error_cstr = NULL;
    char buffer[128];
    if (CFStringGetCString(error_message, buffer, 128, kCFStringEncodingUTF8)) {
        error_cstr = buffer;
    }

    int crt_error = AWS_ERROR_CAL_CRYPTO_OPERATION_FAILED;

    /*
     * Mac seems throws errSecVerifyFailed for any signature verification
     * failures (based on testing and not review of their code).
     * Which makes it impossible to distinguish between signature validation
     * failure and api call failure.
     * So let errSecVerifyFailed as signature validation failure, rather than a
     * more generic Crypto Failure as it seems more intuitive to caller that
     * signature cannot be verified, rather than something wrong with crypto (and
     * in most cases crypto is working correctly, but returning non-specific error).
     */
    if (error_code == errSecVerifyFailed) {
        crt_error = AWS_ERROR_CAL_SIGNATURE_VALIDATION_FAILED;
    }

    AWS_LOGF_ERROR(
        AWS_LS_CAL_RSA,
        "%s() failed. CFError:%ld(%s) aws_error:%s",
        function_name,
        error_code,
        error_cstr ? error_cstr : "",
        aws_error_name(crt_error));

    CFRelease(error_message);

    return aws_raise_error(crt_error);
}

/*
 * Maps crt encryption algo enum to Security Framework equivalent.
 * Fails with AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM if mapping cannot be done for
 * some reason.
 * Mapped value is passed back through out variable.
 */
static int s_map_rsa_encryption_algo_to_sec(enum aws_rsa_encryption_algorithm algorithm, SecKeyAlgorithm *out) {

    switch (algorithm) {
        case AWS_CAL_RSA_ENCRYPTION_PKCS1_5:
            *out = kSecKeyAlgorithmRSAEncryptionPKCS1;
            return AWS_OP_SUCCESS;
        case AWS_CAL_RSA_ENCRYPTION_OAEP_SHA256:
            *out = kSecKeyAlgorithmRSAEncryptionOAEPSHA256;
            return AWS_OP_SUCCESS;
        case AWS_CAL_RSA_ENCRYPTION_OAEP_SHA512:
            *out = kSecKeyAlgorithmRSAEncryptionOAEPSHA512;
            return AWS_OP_SUCCESS;
    }

    return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
}

/*
 * Maps crt encryption algo enum to Security Framework equivalent.
 * Fails with AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM if mapping cannot be done for
 * some reason.
 * Mapped value is passed back through out variable.
 */
static int s_map_rsa_signing_algo_to_sec(enum aws_rsa_signature_algorithm algorithm, SecKeyAlgorithm *out) {

    switch (algorithm) {
        case AWS_CAL_RSA_SIGNATURE_PKCS1_5_SHA256:
            *out = kSecKeyAlgorithmRSASignatureDigestPKCS1v15SHA256;
            return AWS_OP_SUCCESS;
        case AWS_CAL_RSA_SIGNATURE_PSS_SHA256:
#if (defined(__MAC_OS_X_VERSION_MAX_ALLOWED) && (__MAC_OS_X_VERSION_MAX_ALLOWED >= 101300 /* macOS 10.13 */)) ||       \
    (defined(__IPHONE_OS_VERSION_MAX_ALLOWED) && (__IPHONE_OS_VERSION_MAX_ALLOWED >= 110000 /* iOS v11 */)) ||         \
    (defined(__TV_OS_VERSION_MAX_ALLOWED) && (__TV_OS_VERSION_MAX_ALLOWED >= 110000 /* tvos v11 */)) ||                \
    (defined(__WATCH_OS_VERSION_MAX_ALLOWED) && (__WATCH_OS_VERSION_MAX_ALLOWED >= 40000 /* watchos v4 */))
            if (__builtin_available(macos 10.13, ios 11.0, tvos 11.0, watchos 4.0, *)) {
                *out = kSecKeyAlgorithmRSASignatureDigestPSSSHA256;
                return AWS_OP_SUCCESS;
            } else {
                return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
            }
#else
            return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
#endif
    }

    return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
}

static int s_rsa_encrypt(
    const struct aws_rsa_key_pair *key_pair,
    enum aws_rsa_encryption_algorithm algorithm,
    struct aws_byte_cursor plaintext,
    struct aws_byte_buf *out) {
    struct sec_rsa_key_pair *key_pair_impl = key_pair->impl;

    if (key_pair_impl->pub_key_ref == NULL) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "RSA Key Pair is missing Public Key required for encrypt operation.");
        return aws_raise_error(AWS_ERROR_CAL_MISSING_REQUIRED_KEY_COMPONENT);
    }

    SecKeyAlgorithm alg;
    if (s_map_rsa_encryption_algo_to_sec(algorithm, &alg)) {
        return AWS_OP_ERR;
    }

    if (!SecKeyIsAlgorithmSupported(key_pair_impl->pub_key_ref, kSecKeyOperationTypeEncrypt, alg)) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "Algo is not supported for this operation");
        return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
    }

    CFDataRef plaintext_ref =
        CFDataCreateWithBytesNoCopy(key_pair_impl->cf_allocator, plaintext.ptr, plaintext.len, kCFAllocatorNull);
    AWS_FATAL_ASSERT(plaintext_ref);

    CFErrorRef error = NULL;
    CFDataRef ciphertext_ref = SecKeyCreateEncryptedData(key_pair_impl->pub_key_ref, alg, plaintext_ref, &error);
    if (s_reinterpret_sec_error_as_crt(error, "SecKeyCreateEncryptedData")) {
        CFRelease(error);
        goto on_error;
    }

    struct aws_byte_cursor ciphertext_cur =
        aws_byte_cursor_from_array(CFDataGetBytePtr(ciphertext_ref), CFDataGetLength(ciphertext_ref));

    if (aws_byte_buf_append(out, &ciphertext_cur)) {
        aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        goto on_error;
    }

    CFRelease(plaintext_ref);
    CFRelease(ciphertext_ref);
    return AWS_OP_SUCCESS;

on_error:
    if (plaintext_ref != NULL) {
        CFRelease(plaintext_ref);
    }

    if (ciphertext_ref != NULL) {
        CFRelease(ciphertext_ref);
    }

    return AWS_OP_ERR;
}

static int s_rsa_decrypt(
    const struct aws_rsa_key_pair *key_pair,
    enum aws_rsa_encryption_algorithm algorithm,
    struct aws_byte_cursor ciphertext,
    struct aws_byte_buf *out) {
    struct sec_rsa_key_pair *key_pair_impl = key_pair->impl;

    if (key_pair_impl->priv_key_ref == NULL) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "RSA Key Pair is missing Private Key required for encrypt operation.");
        return aws_raise_error(AWS_ERROR_CAL_MISSING_REQUIRED_KEY_COMPONENT);
    }

    SecKeyAlgorithm alg;
    if (s_map_rsa_encryption_algo_to_sec(algorithm, &alg)) {
        return AWS_OP_ERR;
    }

    if (!SecKeyIsAlgorithmSupported(key_pair_impl->priv_key_ref, kSecKeyOperationTypeDecrypt, alg)) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "Algo is not supported for this operation");
        return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
    }

    CFDataRef ciphertext_ref =
        CFDataCreateWithBytesNoCopy(key_pair_impl->cf_allocator, ciphertext.ptr, ciphertext.len, kCFAllocatorNull);
    AWS_FATAL_ASSERT(ciphertext_ref);

    CFErrorRef error = NULL;
    CFDataRef plaintext_ref = SecKeyCreateDecryptedData(key_pair_impl->priv_key_ref, alg, ciphertext_ref, &error);
    if (s_reinterpret_sec_error_as_crt(error, "SecKeyCreateDecryptedData")) {
        CFRelease(error);
        goto on_error;
    }

    struct aws_byte_cursor plaintext_cur =
        aws_byte_cursor_from_array(CFDataGetBytePtr(plaintext_ref), CFDataGetLength(plaintext_ref));

    if (aws_byte_buf_append(out, &plaintext_cur)) {
        aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        goto on_error;
    }

    CFRelease(plaintext_ref);
    CFRelease(ciphertext_ref);
    return AWS_OP_SUCCESS;

on_error:
    if (plaintext_ref != NULL) {
        CFRelease(plaintext_ref);
    }

    if (ciphertext_ref != NULL) {
        CFRelease(ciphertext_ref);
    }

    return AWS_OP_ERR;
}

static int s_rsa_sign(
    const struct aws_rsa_key_pair *key_pair,
    enum aws_rsa_signature_algorithm algorithm,
    struct aws_byte_cursor digest,
    struct aws_byte_buf *out) {
    struct sec_rsa_key_pair *key_pair_impl = key_pair->impl;

    if (key_pair_impl->priv_key_ref == NULL) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "RSA Key Pair is missing Private Key required for sign operation.");
        return aws_raise_error(AWS_ERROR_CAL_MISSING_REQUIRED_KEY_COMPONENT);
    }

    SecKeyAlgorithm alg;
    if (s_map_rsa_signing_algo_to_sec(algorithm, &alg)) {
        return AWS_OP_ERR;
    }

    if (!SecKeyIsAlgorithmSupported(key_pair_impl->priv_key_ref, kSecKeyOperationTypeSign, alg)) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "Algo is not supported for this operation");
        return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
    }

    CFDataRef digest_ref =
        CFDataCreateWithBytesNoCopy(key_pair_impl->cf_allocator, digest.ptr, digest.len, kCFAllocatorNull);
    AWS_FATAL_ASSERT(digest_ref);

    CFErrorRef error = NULL;
    CFDataRef signature_ref = SecKeyCreateSignature(key_pair_impl->priv_key_ref, alg, digest_ref, &error);
    if (s_reinterpret_sec_error_as_crt(error, "SecKeyCreateSignature")) {
        CFRelease(error);
        goto on_error;
    }

    struct aws_byte_cursor signature_cur =
        aws_byte_cursor_from_array(CFDataGetBytePtr(signature_ref), CFDataGetLength(signature_ref));

    if (aws_byte_buf_append(out, &signature_cur)) {
        aws_raise_error(AWS_ERROR_SHORT_BUFFER);
        goto on_error;
    }

    CFRelease(digest_ref);
    CFRelease(signature_ref);

    return AWS_OP_SUCCESS;

on_error:
    CFRelease(digest_ref);

    if (signature_ref != NULL) {
        CFRelease(signature_ref);
    }

    return AWS_OP_ERR;
}

static int s_rsa_verify(
    const struct aws_rsa_key_pair *key_pair,
    enum aws_rsa_signature_algorithm algorithm,
    struct aws_byte_cursor digest,
    struct aws_byte_cursor signature) {
    struct sec_rsa_key_pair *key_pair_impl = key_pair->impl;

    if (key_pair_impl->pub_key_ref == NULL) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "RSA Key Pair is missing Public Key required for verify operation.");
        return aws_raise_error(AWS_ERROR_CAL_MISSING_REQUIRED_KEY_COMPONENT);
    }

    SecKeyAlgorithm alg;
    if (s_map_rsa_signing_algo_to_sec(algorithm, &alg)) {
        return AWS_OP_ERR;
    }

    if (!SecKeyIsAlgorithmSupported(key_pair_impl->pub_key_ref, kSecKeyOperationTypeVerify, alg)) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "Algo is not supported for this operation");
        return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
    }

    CFDataRef digest_ref =
        CFDataCreateWithBytesNoCopy(key_pair_impl->cf_allocator, digest.ptr, digest.len, kCFAllocatorNull);
    CFDataRef signature_ref =
        CFDataCreateWithBytesNoCopy(key_pair_impl->cf_allocator, signature.ptr, signature.len, kCFAllocatorNull);
    AWS_FATAL_ASSERT(digest_ref && signature_ref);

    CFErrorRef error = NULL;
    Boolean result = SecKeyVerifySignature(key_pair_impl->pub_key_ref, alg, digest_ref, signature_ref, &error);

    CFRelease(digest_ref);
    CFRelease(signature_ref);
    if (s_reinterpret_sec_error_as_crt(error, "SecKeyVerifySignature")) {
        CFRelease(error);
        return AWS_OP_ERR;
    }

    return result ? AWS_OP_SUCCESS : aws_raise_error(AWS_ERROR_CAL_SIGNATURE_VALIDATION_FAILED);
}

static struct aws_rsa_key_vtable s_rsa_key_pair_vtable = {
    .encrypt = s_rsa_encrypt,
    .decrypt = s_rsa_decrypt,
    .sign = s_rsa_sign,
    .verify = s_rsa_verify,
};

struct aws_rsa_key_pair *aws_rsa_key_pair_new_from_private_key_pkcs1_impl(
    struct aws_allocator *allocator,
    struct aws_byte_cursor key) {
    struct sec_rsa_key_pair *key_pair_impl = aws_mem_calloc(allocator, 1, sizeof(struct sec_rsa_key_pair));

    CFMutableDictionaryRef key_attributes = NULL;
    CFDataRef private_key_data = NULL;

    aws_ref_count_init(&key_pair_impl->base.ref_count, &key_pair_impl->base, s_rsa_destroy_key);
    key_pair_impl->base.impl = key_pair_impl;
    key_pair_impl->base.allocator = allocator;
    key_pair_impl->cf_allocator = aws_wrapped_cf_allocator_new(allocator);
    aws_byte_buf_init_copy_from_cursor(&key_pair_impl->base.priv, allocator, key);

    private_key_data = CFDataCreate(key_pair_impl->cf_allocator, key.ptr, key.len);
    AWS_FATAL_ASSERT(private_key_data);

    key_attributes = CFDictionaryCreateMutable(key_pair_impl->cf_allocator, 0, NULL, NULL);
    AWS_FATAL_ASSERT(key_attributes);

    CFDictionaryAddValue(key_attributes, kSecClass, kSecClassKey);
    CFDictionaryAddValue(key_attributes, kSecAttrKeyType, kSecAttrKeyTypeRSA);
    CFDictionaryAddValue(key_attributes, kSecAttrKeyClass, kSecAttrKeyClassPrivate);

    CFErrorRef error = NULL;
    key_pair_impl->priv_key_ref = SecKeyCreateWithData(private_key_data, key_attributes, &error);
    if (s_reinterpret_sec_error_as_crt(error, "SecKeyCreateWithData")) {
        CFRelease(error);
        goto on_error;
    }

    key_pair_impl->pub_key_ref = SecKeyCopyPublicKey(key_pair_impl->priv_key_ref);
    AWS_FATAL_ASSERT(key_pair_impl->pub_key_ref);

    key_pair_impl->base.vtable = &s_rsa_key_pair_vtable;
    size_t block_size = SecKeyGetBlockSize(key_pair_impl->priv_key_ref);

    if (block_size < (AWS_CAL_RSA_MIN_SUPPORTED_KEY_SIZE_IN_BITS / 8) ||
        block_size > (AWS_CAL_RSA_MAX_SUPPORTED_KEY_SIZE_IN_BITS / 8)) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "Unsupported key size: %zu", block_size);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto on_error;
    }

    key_pair_impl->base.key_size_in_bits = block_size * 8;

    CFRelease(key_attributes);
    CFRelease(private_key_data);

    return &key_pair_impl->base;

on_error:
    if (private_key_data) {
        CFRelease(private_key_data);
    }

    if (key_attributes) {
        CFRelease(key_attributes);
    }
    s_rsa_destroy_key(&key_pair_impl->base);
    return NULL;
}

struct aws_rsa_key_pair *aws_rsa_key_pair_new_from_public_key_pkcs1_impl(
    struct aws_allocator *allocator,
    struct aws_byte_cursor key) {
    struct sec_rsa_key_pair *key_pair_impl = aws_mem_calloc(allocator, 1, sizeof(struct sec_rsa_key_pair));

    CFMutableDictionaryRef key_attributes = NULL;
    CFDataRef public_key_data = NULL;

    aws_ref_count_init(&key_pair_impl->base.ref_count, &key_pair_impl->base, s_rsa_destroy_key);
    key_pair_impl->base.impl = key_pair_impl;
    key_pair_impl->base.allocator = allocator;
    key_pair_impl->cf_allocator = aws_wrapped_cf_allocator_new(allocator);
    aws_byte_buf_init_copy_from_cursor(&key_pair_impl->base.pub, allocator, key);

    public_key_data = CFDataCreate(key_pair_impl->cf_allocator, key.ptr, key.len);
    AWS_FATAL_ASSERT(public_key_data);

    key_attributes = CFDictionaryCreateMutable(key_pair_impl->cf_allocator, 0, NULL, NULL);
    AWS_FATAL_ASSERT(key_attributes);

    CFDictionaryAddValue(key_attributes, kSecClass, kSecClassKey);
    CFDictionaryAddValue(key_attributes, kSecAttrKeyType, kSecAttrKeyTypeRSA);
    CFDictionaryAddValue(key_attributes, kSecAttrKeyClass, kSecAttrKeyClassPublic);

    CFErrorRef error = NULL;
    key_pair_impl->pub_key_ref = SecKeyCreateWithData(public_key_data, key_attributes, &error);
    if (s_reinterpret_sec_error_as_crt(error, "SecKeyCreateWithData")) {
        CFRelease(error);
        goto on_error;
    }

    key_pair_impl->base.vtable = &s_rsa_key_pair_vtable;
    size_t block_size = SecKeyGetBlockSize(key_pair_impl->pub_key_ref);
    if (block_size < (AWS_CAL_RSA_MIN_SUPPORTED_KEY_SIZE_IN_BITS / 8) ||
        block_size > (AWS_CAL_RSA_MAX_SUPPORTED_KEY_SIZE_IN_BITS / 8)) {
        AWS_LOGF_ERROR(AWS_LS_CAL_RSA, "Unsupported key size: %zu", block_size);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto on_error;
    }
    key_pair_impl->base.key_size_in_bits = block_size * 8;

    CFRelease(key_attributes);
    CFRelease(public_key_data);

    return &key_pair_impl->base;

on_error:
    if (public_key_data) {
        CFRelease(public_key_data);
    }

    if (key_attributes) {
        CFRelease(key_attributes);
    }
    s_rsa_destroy_key(&key_pair_impl->base);
    return NULL;
}
