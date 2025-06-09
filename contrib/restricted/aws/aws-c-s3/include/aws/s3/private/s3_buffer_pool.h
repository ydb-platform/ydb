#ifndef AWS_S3_BUFFER_ALLOCATOR_H
#define AWS_S3_BUFFER_ALLOCATOR_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

/*
 * S3 buffer pool.
 * Buffer pool used for pooling part sized buffers for Put/Get operations.
 * Provides additional functionally for limiting overall memory used.
 * High-level buffer pool usage flow:
 * - Create buffer with overall memory limit and common buffer size, aka chunk
 *   size (typically part size configured on client)
 * - For each request:
 *   -- call reserve to acquire ticket for future buffer acquisition. this will
 *   mark memory reserved, but would not allocate it. if reserve call hits
 *   memory limit, it fails and reservation hold is put on the whole buffer
 *   pool. (aws_s3_buffer_pool_remove_reservation_hold can be used to remove
 *   reservation hold).
 *   -- once request needs memory, it can exchange ticket for a buffer using
 *   aws_s3_buffer_pool_acquire_buffer. this operation never fails, even if it
 *   ends up going over memory limit.
 *   -- buffer lifetime is tied to the ticket. so once request is done with the
 *   buffer, ticket is released and buffer returns back to the pool.
 */

AWS_EXTERN_C_BEGIN

struct aws_s3_buffer_pool;
struct aws_s3_buffer_pool_ticket;

struct aws_s3_buffer_pool_usage_stats {
    /* Effective Max memory limit. Memory limit value provided during construction minus
     * buffer reserved for overhead of the pool */
    size_t mem_limit;

    /* Max size of buffer to be allocated from primary. */
    size_t primary_cutoff;

    /* How much mem is used in primary storage. includes memory used by blocks
     * that are waiting on all allocs to release before being put back in circulation. */
    size_t primary_used;
    /* Overall memory allocated for blocks. */
    size_t primary_allocated;
    /* Reserved memory. Does not account for how that memory will map into
     * blocks and in practice can be lower than used memory. */
    size_t primary_reserved;
    /* Number of blocks allocated in primary. */
    size_t primary_num_blocks;

    /* Secondary mem used. Accurate, maps directly to base allocator. */
    size_t secondary_used;
    /* Secondary mem reserved. Accurate, maps directly to base allocator. */
    size_t secondary_reserved;
};

/*
 * Create new buffer pool.
 * chunk_size - specifies the size of memory that will most commonly be acquired
 * from the pool (typically part size).
 * mem_limit - limit on how much mem buffer pool can use. once limit is hit,
 * buffers can no longer be reserved from (reservation hold is placed on the pool).
 * Returns buffer pool pointer on success and NULL on failure.
 */
AWS_S3_API struct aws_s3_buffer_pool *aws_s3_buffer_pool_new(
    struct aws_allocator *allocator,
    size_t chunk_size,
    size_t mem_limit);

/*
 * Destroys buffer pool.
 * Does nothing if buffer_pool is NULL.
 */
AWS_S3_API void aws_s3_buffer_pool_destroy(struct aws_s3_buffer_pool *buffer_pool);

/*
 * Reserves memory from the pool for later use.
 * Best effort and can potentially reserve memory slightly over the limit.
 * Reservation takes some memory out of the available pool, but does not
 * allocate it right away.
 * On success ticket will be returned.
 * On failure NULL is returned, error is raised and reservation hold is placed
 * on the buffer. Any further reservations while hold is active will fail.
 * Remove reservation hold to unblock reservations.
 */
AWS_S3_API struct aws_s3_buffer_pool_ticket *aws_s3_buffer_pool_reserve(
    struct aws_s3_buffer_pool *buffer_pool,
    size_t size);

/*
 * Whether pool has a reservation hold.
 */
AWS_S3_API bool aws_s3_buffer_pool_has_reservation_hold(struct aws_s3_buffer_pool *buffer_pool);

/*
 * Remove reservation hold on pool.
 */
AWS_S3_API void aws_s3_buffer_pool_remove_reservation_hold(struct aws_s3_buffer_pool *buffer_pool);

/*
 * Trades in the ticket for a buffer.
 * Cannot fail and can over allocate above mem limit if reservation was not accurate.
 * Using the same ticket twice will return the same buffer.
 * Buffer is only valid until the ticket is released.
 */
AWS_S3_API struct aws_byte_buf aws_s3_buffer_pool_acquire_buffer(
    struct aws_s3_buffer_pool *buffer_pool,
    struct aws_s3_buffer_pool_ticket *ticket);

/*
 * Releases the ticket.
 * Any buffers associated with the ticket are invalidated.
 */
AWS_S3_API void aws_s3_buffer_pool_release_ticket(
    struct aws_s3_buffer_pool *buffer_pool,
    struct aws_s3_buffer_pool_ticket *ticket);

/*
 * Get pool memory usage stats.
 */
AWS_S3_API struct aws_s3_buffer_pool_usage_stats aws_s3_buffer_pool_get_usage(struct aws_s3_buffer_pool *buffer_pool);

/*
 * Trims all unused mem from the pool.
 * Warning: fairly slow operation, do not use in critical path.
 * TODO: partial trimming? ex. only trim down to 50% of max?
 */
AWS_S3_API void aws_s3_buffer_pool_trim(struct aws_s3_buffer_pool *buffer_pool);

AWS_EXTERN_C_END

#endif /* AWS_S3_BUFFER_ALLOCATOR_H */
