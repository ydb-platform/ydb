/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/cal/hash.h>
#include <aws/common/thread.h>

#include <windows.h>

#include <bcrypt.h>
#include <winerror.h>

static BCRYPT_ALG_HANDLE s_sha256_alg = NULL;
static size_t s_sha256_obj_len = 0;
static aws_thread_once s_sha256_once = AWS_THREAD_ONCE_STATIC_INIT;

static BCRYPT_ALG_HANDLE s_sha1_alg = NULL;
static size_t s_sha1_obj_len = 0;
static aws_thread_once s_sha1_once = AWS_THREAD_ONCE_STATIC_INIT;

static BCRYPT_ALG_HANDLE s_md5_alg = NULL;
static size_t s_md5_obj_len = 0;
static aws_thread_once s_md5_once = AWS_THREAD_ONCE_STATIC_INIT;

static void s_destroy(struct aws_hash *hash);
static int s_update(struct aws_hash *hash, const struct aws_byte_cursor *to_hash);
static int s_finalize(struct aws_hash *hash, struct aws_byte_buf *output);

static struct aws_hash_vtable s_sha256_vtable = {
    .destroy = s_destroy,
    .update = s_update,
    .finalize = s_finalize,
    .alg_name = "SHA256",
    .provider = "Windows CNG",
};

static struct aws_hash_vtable s_sha1_vtable = {
    .destroy = s_destroy,
    .update = s_update,
    .finalize = s_finalize,
    .alg_name = "SHA1",
    .provider = "Windows CNG",
};

static struct aws_hash_vtable s_md5_vtable = {
    .destroy = s_destroy,
    .update = s_update,
    .finalize = s_finalize,
    .alg_name = "MD5",
    .provider = "Windows CNG",
};

struct bcrypt_hash_handle {
    struct aws_hash hash;
    BCRYPT_HASH_HANDLE hash_handle;
    uint8_t *hash_obj;
};

static void s_load_sha256_alg_handle(void *user_data) {
    (void)user_data;
    /* this function is incredibly slow, LET IT LEAK*/
    (void)BCryptOpenAlgorithmProvider(&s_sha256_alg, BCRYPT_SHA256_ALGORITHM, MS_PRIMITIVE_PROVIDER, 0);
    AWS_ASSERT(s_sha256_alg);
    DWORD result_length = 0;
    (void)BCryptGetProperty(
        s_sha256_alg, BCRYPT_OBJECT_LENGTH, (PBYTE)&s_sha256_obj_len, sizeof(s_sha256_obj_len), &result_length, 0);
}

static void s_load_sha1_alg_handle(void *user_data) {
    (void)user_data;
    /* this function is incredibly slow, LET IT LEAK*/
    (void)BCryptOpenAlgorithmProvider(&s_sha1_alg, BCRYPT_SHA1_ALGORITHM, MS_PRIMITIVE_PROVIDER, 0);
    AWS_ASSERT(s_sha1_alg);
    DWORD result_length = 0;
    (void)BCryptGetProperty(
        s_sha1_alg, BCRYPT_OBJECT_LENGTH, (PBYTE)&s_sha1_obj_len, sizeof(s_sha1_obj_len), &result_length, 0);
}

static void s_load_md5_alg_handle(void *user_data) {
    (void)user_data;
    /* this function is incredibly slow, LET IT LEAK*/
    (void)BCryptOpenAlgorithmProvider(&s_md5_alg, BCRYPT_MD5_ALGORITHM, MS_PRIMITIVE_PROVIDER, 0);
    AWS_ASSERT(s_md5_alg);
    DWORD result_length = 0;
    (void)BCryptGetProperty(
        s_md5_alg, BCRYPT_OBJECT_LENGTH, (PBYTE)&s_md5_obj_len, sizeof(s_md5_obj_len), &result_length, 0);
}

struct aws_hash *aws_sha256_default_new(struct aws_allocator *allocator) {
    aws_thread_call_once(&s_sha256_once, s_load_sha256_alg_handle, NULL);

    struct bcrypt_hash_handle *bcrypt_hash = NULL;
    uint8_t *hash_obj = NULL;
    aws_mem_acquire_many(allocator, 2, &bcrypt_hash, sizeof(struct bcrypt_hash_handle), &hash_obj, s_sha256_obj_len);

    if (!bcrypt_hash) {
        return NULL;
    }

    AWS_ZERO_STRUCT(*bcrypt_hash);
    bcrypt_hash->hash.allocator = allocator;
    bcrypt_hash->hash.vtable = &s_sha256_vtable;
    bcrypt_hash->hash.impl = bcrypt_hash;
    bcrypt_hash->hash.digest_size = AWS_SHA256_LEN;
    bcrypt_hash->hash.good = true;
    bcrypt_hash->hash_obj = hash_obj;
    NTSTATUS status = BCryptCreateHash(
        s_sha256_alg, &bcrypt_hash->hash_handle, bcrypt_hash->hash_obj, (ULONG)s_sha256_obj_len, NULL, 0, 0);

    if (((NTSTATUS)status) < 0) {
        aws_mem_release(allocator, bcrypt_hash);
        return NULL;
    }

    return &bcrypt_hash->hash;
}

struct aws_hash *aws_sha1_default_new(struct aws_allocator *allocator) {
    aws_thread_call_once(&s_sha1_once, s_load_sha1_alg_handle, NULL);

    struct bcrypt_hash_handle *bcrypt_hash = NULL;
    uint8_t *hash_obj = NULL;
    aws_mem_acquire_many(allocator, 2, &bcrypt_hash, sizeof(struct bcrypt_hash_handle), &hash_obj, s_sha1_obj_len);

    if (!bcrypt_hash) {
        return NULL;
    }

    AWS_ZERO_STRUCT(*bcrypt_hash);
    bcrypt_hash->hash.allocator = allocator;
    bcrypt_hash->hash.vtable = &s_sha1_vtable;
    bcrypt_hash->hash.impl = bcrypt_hash;
    bcrypt_hash->hash.digest_size = AWS_SHA1_LEN;
    bcrypt_hash->hash.good = true;
    bcrypt_hash->hash_obj = hash_obj;
    NTSTATUS status = BCryptCreateHash(
        s_sha1_alg, &bcrypt_hash->hash_handle, bcrypt_hash->hash_obj, (ULONG)s_sha1_obj_len, NULL, 0, 0);

    if (((NTSTATUS)status) < 0) {
        aws_mem_release(allocator, bcrypt_hash);
        return NULL;
    }

    return &bcrypt_hash->hash;
}

struct aws_hash *aws_md5_default_new(struct aws_allocator *allocator) {
    aws_thread_call_once(&s_md5_once, s_load_md5_alg_handle, NULL);

    struct bcrypt_hash_handle *bcrypt_hash = NULL;
    uint8_t *hash_obj = NULL;
    aws_mem_acquire_many(allocator, 2, &bcrypt_hash, sizeof(struct bcrypt_hash_handle), &hash_obj, s_md5_obj_len);

    if (!bcrypt_hash) {
        return NULL;
    }

    AWS_ZERO_STRUCT(*bcrypt_hash);
    bcrypt_hash->hash.allocator = allocator;
    bcrypt_hash->hash.vtable = &s_md5_vtable;
    bcrypt_hash->hash.impl = bcrypt_hash;
    bcrypt_hash->hash.digest_size = AWS_MD5_LEN;
    bcrypt_hash->hash.good = true;
    bcrypt_hash->hash_obj = hash_obj;
    NTSTATUS status =
        BCryptCreateHash(s_md5_alg, &bcrypt_hash->hash_handle, bcrypt_hash->hash_obj, (ULONG)s_md5_obj_len, NULL, 0, 0);

    if (((NTSTATUS)status) < 0) {
        aws_mem_release(allocator, bcrypt_hash);
        return NULL;
    }

    return &bcrypt_hash->hash;
}

static void s_destroy(struct aws_hash *hash) {
    struct bcrypt_hash_handle *ctx = hash->impl;
    BCryptDestroyHash(ctx->hash_handle);
    aws_mem_release(hash->allocator, ctx);
}

static int s_update(struct aws_hash *hash, const struct aws_byte_cursor *to_hash) {
    if (!hash->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    struct bcrypt_hash_handle *ctx = hash->impl;
    NTSTATUS status = BCryptHashData(ctx->hash_handle, to_hash->ptr, (ULONG)to_hash->len, 0);

    if (((NTSTATUS)status) < 0) {
        hash->good = false;
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    return AWS_OP_SUCCESS;
}

static int s_finalize(struct aws_hash *hash, struct aws_byte_buf *output) {
    if (!hash->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    struct bcrypt_hash_handle *ctx = hash->impl;

    size_t buffer_len = output->capacity - output->len;

    if (buffer_len < hash->digest_size) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    NTSTATUS status = BCryptFinishHash(ctx->hash_handle, output->buffer + output->len, (ULONG)hash->digest_size, 0);

    hash->good = false;
    if (((NTSTATUS)status) < 0) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    output->len += hash->digest_size;
    return AWS_OP_SUCCESS;
}
