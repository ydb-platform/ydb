/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/cal/hmac.h>
#include <aws/common/thread.h>

#include <windows.h>

#include <bcrypt.h>
#include <winerror.h>

static BCRYPT_ALG_HANDLE s_sha256_hmac_alg = NULL;
static size_t s_sha256_hmac_obj_len = 0;

static aws_thread_once s_sha256_hmac_once = AWS_THREAD_ONCE_STATIC_INIT;

static void s_destroy(struct aws_hmac *hash);
static int s_update(struct aws_hmac *hash, const struct aws_byte_cursor *to_hash);
static int s_finalize(struct aws_hmac *hash, struct aws_byte_buf *output);

static struct aws_hmac_vtable s_sha256_hmac_vtable = {
    .destroy = s_destroy,
    .update = s_update,
    .finalize = s_finalize,
    .alg_name = "SHA256 HMAC",
    .provider = "Windows CNG",
};

struct bcrypt_hmac_handle {
    struct aws_hmac hmac;
    BCRYPT_HASH_HANDLE hash_handle;
    uint8_t *hash_obj;
};

static void s_load_alg_handle(void *user_data) {
    (void)user_data;
    /* this function is incredibly slow, LET IT LEAK*/
    BCryptOpenAlgorithmProvider(
        &s_sha256_hmac_alg, BCRYPT_SHA256_ALGORITHM, MS_PRIMITIVE_PROVIDER, BCRYPT_ALG_HANDLE_HMAC_FLAG);
    AWS_ASSERT(s_sha256_hmac_alg);
    DWORD result_length = 0;
    BCryptGetProperty(
        s_sha256_hmac_alg,
        BCRYPT_OBJECT_LENGTH,
        (PBYTE)&s_sha256_hmac_obj_len,
        sizeof(s_sha256_hmac_obj_len),
        &result_length,
        0);
}

struct aws_hmac *aws_sha256_hmac_default_new(struct aws_allocator *allocator, const struct aws_byte_cursor *secret) {
    aws_thread_call_once(&s_sha256_hmac_once, s_load_alg_handle, NULL);

    struct bcrypt_hmac_handle *bcrypt_hmac;
    uint8_t *hash_obj;
    aws_mem_acquire_many(
        allocator, 2, &bcrypt_hmac, sizeof(struct bcrypt_hmac_handle), &hash_obj, s_sha256_hmac_obj_len);

    if (!bcrypt_hmac) {
        return NULL;
    }

    AWS_ZERO_STRUCT(*bcrypt_hmac);
    bcrypt_hmac->hmac.allocator = allocator;
    bcrypt_hmac->hmac.vtable = &s_sha256_hmac_vtable;
    bcrypt_hmac->hmac.impl = bcrypt_hmac;
    bcrypt_hmac->hmac.digest_size = AWS_SHA256_HMAC_LEN;
    bcrypt_hmac->hmac.good = true;
    bcrypt_hmac->hash_obj = hash_obj;
    NTSTATUS status = BCryptCreateHash(
        s_sha256_hmac_alg,
        &bcrypt_hmac->hash_handle,
        bcrypt_hmac->hash_obj,
        (ULONG)s_sha256_hmac_obj_len,
        secret->ptr,
        (ULONG)secret->len,
        0);

    if (((NTSTATUS)status) < 0) {
        aws_mem_release(allocator, bcrypt_hmac);
        return NULL;
    }

    return &bcrypt_hmac->hmac;
}

static void s_destroy(struct aws_hmac *hmac) {
    struct bcrypt_hmac_handle *ctx = hmac->impl;
    BCryptDestroyHash(ctx->hash_handle);
    aws_mem_release(hmac->allocator, ctx);
}

static int s_update(struct aws_hmac *hmac, const struct aws_byte_cursor *to_hash) {
    if (!hmac->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    struct bcrypt_hmac_handle *ctx = hmac->impl;
    NTSTATUS status = BCryptHashData(ctx->hash_handle, to_hash->ptr, (ULONG)to_hash->len, 0);

    if (((NTSTATUS)status) < 0) {
        hmac->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    return AWS_OP_SUCCESS;
}

static int s_finalize(struct aws_hmac *hmac, struct aws_byte_buf *output) {
    if (!hmac->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    struct bcrypt_hmac_handle *ctx = hmac->impl;

    size_t buffer_len = output->capacity - output->len;

    if (buffer_len < hmac->digest_size) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    NTSTATUS status = BCryptFinishHash(ctx->hash_handle, output->buffer + output->len, (ULONG)hmac->digest_size, 0);

    hmac->good = false;
    if (((NTSTATUS)status) < 0) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    output->len += hmac->digest_size;
    return AWS_OP_SUCCESS;
}
