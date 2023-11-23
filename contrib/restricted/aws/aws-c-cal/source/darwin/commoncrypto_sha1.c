/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/cal/hash.h>

#include <CommonCrypto/CommonDigest.h>

static void s_destroy(struct aws_hash *hash);
static int s_update(struct aws_hash *hash, const struct aws_byte_cursor *to_hash);
static int s_finalize(struct aws_hash *hash, struct aws_byte_buf *output);

static struct aws_hash_vtable s_vtable = {
    .destroy = s_destroy,
    .update = s_update,
    .finalize = s_finalize,
    .alg_name = "SHA1",
    .provider = "CommonCrypto",
};

struct cc_sha1_hash {
    struct aws_hash hash;
    CC_SHA1_CTX cc_hash;
};

struct aws_hash *aws_sha1_default_new(struct aws_allocator *allocator) {
    struct cc_sha1_hash *sha1_hash = aws_mem_acquire(allocator, sizeof(struct cc_sha1_hash));

    if (!sha1_hash) {
        return NULL;
    }

    sha1_hash->hash.allocator = allocator;
    sha1_hash->hash.vtable = &s_vtable;
    sha1_hash->hash.impl = sha1_hash;
    sha1_hash->hash.digest_size = AWS_SHA1_LEN;
    sha1_hash->hash.good = true;

    CC_SHA1_Init(&sha1_hash->cc_hash);
    return &sha1_hash->hash;
}

static void s_destroy(struct aws_hash *hash) {
    struct cc_sha1_hash *ctx = hash->impl;
    aws_mem_release(hash->allocator, ctx);
}

static int s_update(struct aws_hash *hash, const struct aws_byte_cursor *to_hash) {
    if (!hash->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    struct cc_sha1_hash *ctx = hash->impl;

    CC_SHA1_Update(&ctx->cc_hash, to_hash->ptr, (CC_LONG)to_hash->len);
    return AWS_OP_SUCCESS;
}

static int s_finalize(struct aws_hash *hash, struct aws_byte_buf *output) {
    if (!hash->good) {
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    struct cc_sha1_hash *ctx = hash->impl;

    size_t buffer_len = output->capacity - output->len;

    if (buffer_len < hash->digest_size) {
        return aws_raise_error(AWS_ERROR_SHORT_BUFFER);
    }

    CC_SHA1_Final(output->buffer + output->len, &ctx->cc_hash);
    hash->good = false;
    output->len += hash->digest_size;
    return AWS_OP_SUCCESS;
}
