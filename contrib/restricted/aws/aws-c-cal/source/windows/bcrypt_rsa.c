/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/cal/private/rsa.h>

#include <aws/cal/cal.h>
#include <aws/cal/private/der.h>
#include <aws/common/encoding.h>

#define WIN32_NO_STATUS
#include <windows.h>
#undef WIN32_NO_STATUS

#include <bcrypt.h>
#include <ntstatus.h>

static BCRYPT_ALG_HANDLE s_rsa_alg = NULL;

static aws_thread_once s_rsa_thread_once = AWS_THREAD_ONCE_STATIC_INIT;

static void s_load_alg_handle(void *user_data) {
    (void)user_data;
    /* this function is incredibly slow, LET IT LEAK*/
    NTSTATUS status = BCryptOpenAlgorithmProvider(&s_rsa_alg, BCRYPT_RSA_ALGORITHM, MS_PRIMITIVE_PROVIDER, 0);
    AWS_FATAL_ASSERT(s_rsa_alg && "BCryptOpenAlgorithmProvider() failed");
    AWS_FATAL_ASSERT(BCRYPT_SUCCESS(status));
}

struct bcrypt_rsa_key_pair {
    struct aws_rsa_key_pair base;
    BCRYPT_KEY_HANDLE key_handle;
    struct aws_byte_buf key_buf;
};

static void s_rsa_destroy_key(void *key_pair) {
    if (key_pair == NULL) {
        return;
    }

    struct aws_rsa_key_pair *base = key_pair;
    struct bcrypt_rsa_key_pair *impl = base->impl;

    if (impl->key_handle) {
        BCryptDestroyKey(impl->key_handle);
    }
    aws_byte_buf_clean_up_secure(&impl->key_buf);

    aws_rsa_key_pair_base_clean_up(base);

    aws_mem_release(base->allocator, impl);
}

/*
 * Transforms bcrypt error code into crt error code and raises it as necessary.
 */
static int s_reinterpret_bc_error_as_crt(NTSTATUS error, const char *function_name) {
    if (BCRYPT_SUCCESS(error)) {
        return AWS_OP_SUCCESS;
    }

    int crt_error = AWS_OP_ERR;
    switch (error) {
        case STATUS_BUFFER_TOO_SMALL: {
            crt_error = AWS_ERROR_SHORT_BUFFER;
            goto on_error;
        }
        case STATUS_NOT_SUPPORTED: {
            crt_error = AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM;
            goto on_error;
        }
    }

    crt_error = AWS_ERROR_CAL_CRYPTO_OPERATION_FAILED;

on_error:
    AWS_LOGF_ERROR(
        AWS_LS_CAL_RSA, "%s() failed. returned: %X aws_error:%s", function_name, error, aws_error_name(crt_error));

    return aws_raise_error(crt_error);
}

static int s_check_encryption_algorithm(enum aws_rsa_encryption_algorithm algorithm) {
    if (algorithm != AWS_CAL_RSA_ENCRYPTION_PKCS1_5 && algorithm != AWS_CAL_RSA_ENCRYPTION_OAEP_SHA256 &&
        algorithm != AWS_CAL_RSA_ENCRYPTION_OAEP_SHA512) {
        return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
    }
    return AWS_OP_SUCCESS;
}

static int s_rsa_encrypt(
    const struct aws_rsa_key_pair *key_pair,
    enum aws_rsa_encryption_algorithm algorithm,
    struct aws_byte_cursor plaintext,
    struct aws_byte_buf *out) {
    struct bcrypt_rsa_key_pair *key_pair_impl = key_pair->impl;

    if (s_check_encryption_algorithm(algorithm)) {
        return AWS_OP_ERR;
    }

    BCRYPT_OAEP_PADDING_INFO padding_info_oaep = {
        .pszAlgId = algorithm == AWS_CAL_RSA_ENCRYPTION_OAEP_SHA256 ? BCRYPT_SHA256_ALGORITHM : BCRYPT_SHA512_ALGORITHM,
        .pbLabel = NULL,
        .cbLabel = 0};

    ULONG length_written = 0;
    NTSTATUS status = BCryptEncrypt(
        key_pair_impl->key_handle,
        plaintext.ptr,
        (ULONG)plaintext.len,
        algorithm == AWS_CAL_RSA_ENCRYPTION_PKCS1_5 ? NULL : &padding_info_oaep,
        NULL,
        0,
        out->buffer + out->len,
        (ULONG)(out->capacity - out->len),
        &length_written,
        algorithm == AWS_CAL_RSA_ENCRYPTION_PKCS1_5 ? BCRYPT_PAD_PKCS1 : BCRYPT_PAD_OAEP);

    if (s_reinterpret_bc_error_as_crt(status, "BCryptEncrypt")) {
        return AWS_OP_ERR;
    }

    out->len += length_written;
    return AWS_OP_SUCCESS;
}

static int s_rsa_decrypt(
    const struct aws_rsa_key_pair *key_pair,
    enum aws_rsa_encryption_algorithm algorithm,
    struct aws_byte_cursor ciphertext,
    struct aws_byte_buf *out) {
    struct bcrypt_rsa_key_pair *key_pair_impl = key_pair->impl;

    /* There is a bug in old versions of BCryptDecrypt, where it does not return
     * error status if out buffer is too short. So manually check that buffer is
     * large enough.
     */
    if ((out->capacity - out->len) < aws_rsa_key_pair_block_length(key_pair)) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    if (s_check_encryption_algorithm(algorithm)) {
        return AWS_OP_ERR;
    }

    BCRYPT_OAEP_PADDING_INFO padding_info_oaep = {
        .pszAlgId = algorithm == AWS_CAL_RSA_ENCRYPTION_OAEP_SHA256 ? BCRYPT_SHA256_ALGORITHM : BCRYPT_SHA512_ALGORITHM,
        .pbLabel = NULL,
        .cbLabel = 0};

    ULONG length_written = 0;
    NTSTATUS status = BCryptDecrypt(
        key_pair_impl->key_handle,
        ciphertext.ptr,
        (ULONG)ciphertext.len,
        algorithm == AWS_CAL_RSA_ENCRYPTION_PKCS1_5 ? NULL : &padding_info_oaep,
        NULL,
        0,
        out->buffer + out->len,
        (ULONG)(out->capacity - out->len),
        &length_written,
        algorithm == AWS_CAL_RSA_ENCRYPTION_PKCS1_5 ? BCRYPT_PAD_PKCS1 : BCRYPT_PAD_OAEP);

    if (s_reinterpret_bc_error_as_crt(status, "BCryptDecrypt")) {
        return AWS_OP_ERR;
    }

    out->len += length_written;
    return AWS_OP_SUCCESS;
}

union sign_padding_info {
    BCRYPT_PKCS1_PADDING_INFO pkcs1;
    BCRYPT_PSS_PADDING_INFO pss;
};

static int s_sign_padding_info_init(union sign_padding_info *info, enum aws_rsa_signature_algorithm algorithm) {
    memset(info, 0, sizeof(union sign_padding_info));

    if (algorithm == AWS_CAL_RSA_SIGNATURE_PKCS1_5_SHA256) {
        info->pkcs1.pszAlgId = BCRYPT_SHA256_ALGORITHM;
        return AWS_OP_SUCCESS;
    } else if (algorithm == AWS_CAL_RSA_SIGNATURE_PSS_SHA256) {
        info->pss.pszAlgId = BCRYPT_SHA256_ALGORITHM;
        info->pss.cbSalt = 32;
        return AWS_OP_SUCCESS;
    }

    return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
}

static int s_rsa_sign(
    const struct aws_rsa_key_pair *key_pair,
    enum aws_rsa_signature_algorithm algorithm,
    struct aws_byte_cursor digest,
    struct aws_byte_buf *out) {
    struct bcrypt_rsa_key_pair *key_pair_impl = key_pair->impl;

    union sign_padding_info padding_info;
    if (s_sign_padding_info_init(&padding_info, algorithm)) {
        return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
    }

    ULONG length_written = 0;
    NTSTATUS status = BCryptSignHash(
        key_pair_impl->key_handle,
        &padding_info,
        digest.ptr,
        (ULONG)digest.len,
        out->buffer + out->len,
        (ULONG)(out->capacity - out->len),
        (ULONG *)&length_written,
        algorithm == AWS_CAL_RSA_SIGNATURE_PKCS1_5_SHA256 ? BCRYPT_PAD_PKCS1 : BCRYPT_PAD_PSS);

    if (s_reinterpret_bc_error_as_crt(status, "BCryptSignHash")) {
        goto on_error;
    }

    out->len += length_written;

    return AWS_OP_SUCCESS;

on_error:
    return AWS_OP_ERR;
}

static int s_rsa_verify(
    const struct aws_rsa_key_pair *key_pair,
    enum aws_rsa_signature_algorithm algorithm,
    struct aws_byte_cursor digest,
    struct aws_byte_cursor signature) {
    struct bcrypt_rsa_key_pair *key_pair_impl = key_pair->impl;

    /* BCrypt raises invalid argument if signature does not have correct size.
     * Verify size here and raise appropriate error and treat all other errors
     * from BCrypt (including invalid arg) in reinterp. */
    if (signature.len != aws_rsa_key_pair_signature_length(key_pair)) {
        return aws_raise_error(AWS_ERROR_CAL_SIGNATURE_VALIDATION_FAILED);
    }

    union sign_padding_info padding_info;
    if (s_sign_padding_info_init(&padding_info, algorithm)) {
        return aws_raise_error(AWS_ERROR_CAL_UNSUPPORTED_ALGORITHM);
    }
    /* okay, now we've got a windows compatible signature, let's verify it. */
    NTSTATUS status = BCryptVerifySignature(
        key_pair_impl->key_handle,
        &padding_info,
        digest.ptr,
        (ULONG)digest.len,
        signature.ptr,
        (ULONG)signature.len,
        algorithm == AWS_CAL_RSA_SIGNATURE_PKCS1_5_SHA256 ? BCRYPT_PAD_PKCS1 : BCRYPT_PAD_PSS);

    if (status == STATUS_INVALID_SIGNATURE) {
        return aws_raise_error(AWS_ERROR_CAL_SIGNATURE_VALIDATION_FAILED);
    }

    if (s_reinterpret_bc_error_as_crt(status, "BCryptVerifySignature")) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
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

    aws_thread_call_once(&s_rsa_thread_once, s_load_alg_handle, NULL);
    struct bcrypt_rsa_key_pair *key_pair_impl = aws_mem_calloc(allocator, 1, sizeof(struct bcrypt_rsa_key_pair));

    aws_ref_count_init(&key_pair_impl->base.ref_count, &key_pair_impl->base, s_rsa_destroy_key);
    key_pair_impl->base.impl = key_pair_impl;
    key_pair_impl->base.allocator = allocator;
    aws_byte_buf_init_copy_from_cursor(&key_pair_impl->base.priv, allocator, key);

    struct aws_der_decoder *decoder = aws_der_decoder_new(allocator, key);

    if (!decoder) {
        goto on_error;
    }

    struct aws_rsa_private_key_pkcs1 private_key_data;
    AWS_ZERO_STRUCT(private_key_data);
    if (aws_der_decoder_load_private_rsa_pkcs1(decoder, &private_key_data)) {
        goto on_error;
    }

    /* Hard to predict final blob size, so use pkcs1 key size as upper bound. */
    size_t total_buffer_size = key.len + sizeof(BCRYPT_RSAKEY_BLOB);

    aws_byte_buf_init(&key_pair_impl->key_buf, allocator, total_buffer_size);

    BCRYPT_RSAKEY_BLOB key_blob;
    AWS_ZERO_STRUCT(key_blob);
    key_blob.Magic = BCRYPT_RSAFULLPRIVATE_MAGIC;
    key_blob.BitLength = (ULONG)private_key_data.modulus.len * 8;
    key_blob.cbPublicExp = (ULONG)private_key_data.publicExponent.len;
    key_blob.cbModulus = (ULONG)private_key_data.modulus.len;
    key_blob.cbPrime1 = (ULONG)private_key_data.prime1.len;
    key_blob.cbPrime2 = (ULONG)private_key_data.prime2.len;

    struct aws_byte_cursor header = aws_byte_cursor_from_array(&key_blob, sizeof(key_blob));
    aws_byte_buf_append(&key_pair_impl->key_buf, &header);

    LPCWSTR blob_type = BCRYPT_RSAFULLPRIVATE_BLOB;
    ULONG flags = 0;

    aws_byte_buf_append(&key_pair_impl->key_buf, &private_key_data.publicExponent);
    aws_byte_buf_append(&key_pair_impl->key_buf, &private_key_data.modulus);
    aws_byte_buf_append(&key_pair_impl->key_buf, &private_key_data.prime1);
    aws_byte_buf_append(&key_pair_impl->key_buf, &private_key_data.prime2);
    aws_byte_buf_append(&key_pair_impl->key_buf, &private_key_data.exponent1);
    aws_byte_buf_append(&key_pair_impl->key_buf, &private_key_data.exponent2);
    aws_byte_buf_append(&key_pair_impl->key_buf, &private_key_data.coefficient);
    aws_byte_buf_append(&key_pair_impl->key_buf, &private_key_data.privateExponent);

    NTSTATUS status = BCryptImportKeyPair(
        s_rsa_alg,
        NULL,
        blob_type,
        &key_pair_impl->key_handle,
        key_pair_impl->key_buf.buffer,
        (ULONG)key_pair_impl->key_buf.len,
        flags);

    if (s_reinterpret_bc_error_as_crt(status, "BCryptImportKeyPair")) {
        goto on_error;
    }

    key_pair_impl->base.vtable = &s_rsa_key_pair_vtable;
    key_pair_impl->base.key_size_in_bits = private_key_data.modulus.len * 8;

    aws_der_decoder_destroy(decoder);

    return &key_pair_impl->base;

on_error:
    aws_der_decoder_destroy(decoder);
    s_rsa_destroy_key(&key_pair_impl->base);
    return NULL;
}

struct aws_rsa_key_pair *aws_rsa_key_pair_new_from_public_key_pkcs1_impl(
    struct aws_allocator *allocator,
    struct aws_byte_cursor key) {

    aws_thread_call_once(&s_rsa_thread_once, s_load_alg_handle, NULL);
    struct bcrypt_rsa_key_pair *key_pair_impl = aws_mem_calloc(allocator, 1, sizeof(struct bcrypt_rsa_key_pair));

    aws_ref_count_init(&key_pair_impl->base.ref_count, &key_pair_impl->base, s_rsa_destroy_key);
    key_pair_impl->base.impl = key_pair_impl;
    key_pair_impl->base.allocator = allocator;
    aws_byte_buf_init_copy_from_cursor(&key_pair_impl->base.pub, allocator, key);

    struct aws_der_decoder *decoder = aws_der_decoder_new(allocator, key);

    if (!decoder) {
        goto on_error;
    }

    struct aws_rsa_public_key_pkcs1 public_key_data;
    AWS_ZERO_STRUCT(public_key_data);
    if (aws_der_decoder_load_public_rsa_pkcs1(decoder, &public_key_data)) {
        goto on_error;
    }

    /* Hard to predict final blob size, so use pkcs1 key size as upper bound. */
    size_t total_buffer_size = key.len + sizeof(BCRYPT_RSAKEY_BLOB);

    aws_byte_buf_init(&key_pair_impl->key_buf, allocator, total_buffer_size);

    BCRYPT_RSAKEY_BLOB key_blob;
    AWS_ZERO_STRUCT(key_blob);
    key_blob.Magic = BCRYPT_RSAPUBLIC_MAGIC;
    key_blob.BitLength = (ULONG)public_key_data.modulus.len * 8;
    key_blob.cbPublicExp = (ULONG)public_key_data.publicExponent.len;
    key_blob.cbModulus = (ULONG)public_key_data.modulus.len;

    struct aws_byte_cursor header = aws_byte_cursor_from_array(&key_blob, sizeof(key_blob));
    aws_byte_buf_append(&key_pair_impl->key_buf, &header);

    LPCWSTR blob_type = BCRYPT_PUBLIC_KEY_BLOB;
    ULONG flags = 0;

    aws_byte_buf_append(&key_pair_impl->key_buf, &public_key_data.publicExponent);
    aws_byte_buf_append(&key_pair_impl->key_buf, &public_key_data.modulus);

    NTSTATUS status = BCryptImportKeyPair(
        s_rsa_alg,
        NULL,
        blob_type,
        &key_pair_impl->key_handle,
        key_pair_impl->key_buf.buffer,
        (ULONG)key_pair_impl->key_buf.len,
        flags);

    if (s_reinterpret_bc_error_as_crt(status, "BCryptImportKeyPair")) {
        goto on_error;
    }

    key_pair_impl->base.vtable = &s_rsa_key_pair_vtable;
    key_pair_impl->base.key_size_in_bits = public_key_data.modulus.len * 8;

    aws_der_decoder_destroy(decoder);

    return &key_pair_impl->base;

on_error:
    aws_der_decoder_destroy(decoder);
    s_rsa_destroy_key(&key_pair_impl->base);
    return NULL;
}
