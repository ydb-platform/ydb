#ifndef AWS_CHECKSUMS_XXHASH_H
#define AWS_CHECKSUMS_XXHASH_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/checksums/exports.h>
#include <aws/common/byte_buf.h>

AWS_PUSH_SANE_WARNING_LEVEL

struct aws_xxhash_impl;

enum aws_xxhash_type { XXHASH64 = 0, XXHASH3_64 = 1, XXHASH3_128 = 2 };

struct aws_xxhash {
    struct aws_allocator *allocator;
    enum aws_xxhash_type type;
    struct aws_xxhash_impl *impl;
};

AWS_EXTERN_C_BEGIN

/**
 * Streaming.
 * General flow is create a new hash, update it as data arrives and then finalize when you need the checksum.
 * Note: finalizing does not make hash unusable, it just returns checksum of the data so far,
 * and more data can be pushed through the hash after.
 **/

/**
 * Allocates and initializes a XXHASH64 hash instance.
 */
AWS_CHECKSUMS_API struct aws_xxhash *aws_xxhash64_new(struct aws_allocator *allocator, uint64_t seed);

/**
 * Allocates and initializes a XXHASH3 64bit hash instance.
 */
AWS_CHECKSUMS_API struct aws_xxhash *aws_xxhash3_64_new(struct aws_allocator *allocator, uint64_t seed);

/**
 * Allocates and initializes a XXHASH3 128bit hash instance.
 */
AWS_CHECKSUMS_API struct aws_xxhash *aws_xxhash3_128_new(struct aws_allocator *allocator, uint64_t seed);

/**
 * Update hash state from the data.
 * Can return error. Hash is unusable after error;
 */
AWS_CHECKSUMS_API int aws_xxhash_update(struct aws_xxhash *hash, struct aws_byte_cursor data);

/**
 * Write out bytes of the hash to out.
 * Out buffer should be allocated by caller and should have enough capacity.
 * Hash is written out in big-endian, i.e. network order.
 */
AWS_CHECKSUMS_API int aws_xxhash_finalize(struct aws_xxhash *hash, struct aws_byte_buf *out);

/**
 * Destroy allocated hash.
 */
AWS_CHECKSUMS_API void aws_xxhash_destroy(struct aws_xxhash *hash);

/**
 * One-shot.
 */

/**
 * Compute XXH64 hash.
 */
AWS_CHECKSUMS_API int aws_xxhash64_compute(uint64_t seed, struct aws_byte_cursor data, struct aws_byte_buf *out);

/**
 * Compute XXH3_64 hash.
 */
AWS_CHECKSUMS_API int aws_xxhash3_64_compute(uint64_t seed, struct aws_byte_cursor data, struct aws_byte_buf *out);

/**
 * Compute XXH3_128 hash.
 */
AWS_CHECKSUMS_API int aws_xxhash3_128_compute(uint64_t seed, struct aws_byte_cursor data, struct aws_byte_buf *out);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_CHECKSUMS_XXHASH_H */
