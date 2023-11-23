/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/cal/hash.h>

#include <CommonCrypto/CommonDigest.h>

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"

static void s_destroy(struct aws_hash *hash);
static int s_update(struct aws_hash *hash, const struct aws_byte_cursor *to_hash);
static int s_finalize(struct aws_hash *hash, struct aws_byte_buf *output);

static struct aws_hash_vtable s_vtable = {
    .destroy = s_destroy,
    .update = s_update,
    .finalize = s_finalize,
    .alg_name = "MD5",
    .provider = "CommonCrypto",
};

struct cc_md5_hash {
    struct aws_hash hash;
    CC_MD5_CTX cc_hash;
};
struct aws_hash *aws_md5_default_new(struct aws_allocator *allocator) {
    struct cc_md5_hash *cc_md5_hash = aws_mem_acquire(allocator, sizeof(struct cc_md5_hash));

    if (!cc_md5_hash) {
        return NULL;
    }

    cc_md5_hash->hash.allocator = allocator;
    cc_md5_hash->hash.vtable = &s_vtable;
    cc_md5_hash->hash.digest_size = AWS_MD5_LEN;
    cc_md5_hash->hash.impl = cc_md5_hash;
    cc_md5_hash->hash.good = true;

    CC_MD5_Init(&cc_md5_hash->cc_hash);
    return &cc_md5_hash->hash;
}

static void s_destroy(struct aws_hash *hash) {
    struct cc_md5_hash *ctx = hash->impl;
    aws_mem_release(hash->allocator, ctx);
}

static int s_update(struct aws_hash *hash, const struct aws_byte_cursor *to_hash) {
    if (!hash->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    struct cc_md5_hash *ctx = hash->impl;

    CC_MD5_Update(&ctx->cc_hash, to_hash->ptr, (CC_LONG)to_hash->len);
    return AWS_OP_SUCCESS;
}

static int s_finalize(struct aws_hash *hash, struct aws_byte_buf *output) {
    if (!hash->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    struct cc_md5_hash *ctx = hash->impl;

    size_t buffer_len = output->capacity - output->len;

    if (buffer_len < hash->digest_size) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    CC_MD5_Final(output->buffer + output->len, &ctx->cc_hash);
    hash->good = false;
    output->len += hash->digest_size;
    return AWS_OP_SUCCESS;
}

#pragma clang diagnostic pop
