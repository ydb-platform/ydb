/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/** \file
 * Bit pool data structure
 */

#ifndef SPDK_BIT_POOL_H
#define SPDK_BIT_POOL_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

struct spdk_bit_pool;
struct spdk_bit_array;

/**
 * Return the number of bits that a bit pool is currently sized to hold.
 *
 * \param pool Bit pool to query.
 *
 * \return the number of bits.
 */
uint32_t spdk_bit_pool_capacity(const struct spdk_bit_pool *pool);

/**
 * Create a bit pool.
 *
 * All bits in the pool will be available for allocation.
 *
 * \param num_bits Number of bits that the bit pool is sized to hold.
 *
 * \return a pointer to the new bit pool.
 */
struct spdk_bit_pool *spdk_bit_pool_create(uint32_t num_bits);

/**
 * Create a bit pool from an existing spdk_bit_array.
 *
 * The starting state of the bit pool will be specified by the state
 * of the specified spdk_bit_array.
 *
 * The new spdk_bit_pool will consume the spdk_bit_array and assumes
 * responsibility for freeing it.  The caller should not use the
 * spdk_bit_array after this function returns.
 *
 * \param array spdk_bit_array representing the starting state of the new bit pool.
 *
 * \return a pointer to the new bit pool, NULL if one could not be created (in which
 *         case the caller maintains responsibility for the spdk_bit_array)
 */
struct spdk_bit_pool *spdk_bit_pool_create_from_array(struct spdk_bit_array *array);

/**
 * Free a bit pool and set the pointer to NULL.
 *
 * \param pool Bit pool to free.
 */
void spdk_bit_pool_free(struct spdk_bit_pool **pool);

/**
 * Create or resize a bit pool.
 *
 * To create a new bit pool, pass a pointer to a spdk_bit_pool pointer that is
 * NULL.
 *
 * The bit pool will be sized to hold at least num_bits.
 *
 * If num_bits is larger than the previous size of the bit pool,
 * the new bits will all be available for future allocations.
 *
 * \param pool Bit pool to create/resize.
 * \param num_bits Number of bits that the bit pool is sized to hold.
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_bit_pool_resize(struct spdk_bit_pool **pool, uint32_t num_bits);

/**
 * Return whether the specified bit has been allocated from the bit pool.
 *
 * If bit_index is beyond the end of the current size of the bit pool, this
 * function will return false (i.e. bits beyond the end of the pool cannot be allocated).
 *
 * \param pool Bit pool to query.
 * \param bit_index The index of a bit to query.
 *
 * \return true if the bit has been allocated, false otherwise
 */
bool spdk_bit_pool_is_allocated(const struct spdk_bit_pool *pool, uint32_t bit_index);

/**
 * Allocate a bit from the bit pool.
 *
 * \param pool Bit pool to allocate a bit from
 *
 * \return index of the allocated bit, UINT32_MAX if no free bits exist
 */
uint32_t spdk_bit_pool_allocate_bit(struct spdk_bit_pool *pool);

/**
 * Free a bit back to the bit pool.
 *
 * Callers must not try to free a bit that has not been allocated, otherwise the
 * pool may become corrupted without notification.  Freeing a bit that has not
 * been allocated will result in an assert in debug builds.
 *
 * \param pool Bit pool to place the freed bit
 * \param bit_index The index of a bit to free.
 */
void spdk_bit_pool_free_bit(struct spdk_bit_pool *pool, uint32_t bit_index);

/**
 * Count the number of bits allocated from the pool.
 *
 * \param pool The bit pool to count.
 *
 * \return the number of bits allocated from the pool.
 */
uint32_t spdk_bit_pool_count_allocated(const struct spdk_bit_pool *pool);

/**
 * Count the number of free bits in the pool.
 *
 * \param pool The bit pool to count.
 *
 * \return the number of free bits in the pool.
 */
uint32_t spdk_bit_pool_count_free(const struct spdk_bit_pool *pool);

/**
 * Store bitmask from bit pool.
 *
 * \param pool Bit pool.
 * \param mask Destination mask. Mask and bit array pool must be equal.
 */
void spdk_bit_pool_store_mask(const struct spdk_bit_pool *pool, void *mask);

/**
 * Load bitmask to bit pool.
 *
 * \param pool Bit pool.
 * \param mask Source mask. Mask and bit array pool must be equal.
 */
void spdk_bit_pool_load_mask(struct spdk_bit_pool *pool, const void *mask);

/**
 * Free all bits back into the bit pool.
 *
 * \param pool Bit pool.
 */
void spdk_bit_pool_free_all_bits(struct spdk_bit_pool *pool);

#ifdef __cplusplus
}
#endif

#endif
