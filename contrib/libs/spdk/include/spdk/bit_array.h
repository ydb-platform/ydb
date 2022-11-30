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
 * Bit array data structure
 */

#ifndef SPDK_BIT_ARRAY_H
#define SPDK_BIT_ARRAY_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Variable-length bit array.
 */
struct spdk_bit_array;

/**
 * Return the number of bits that a bit array is currently sized to hold.
 *
 * \param ba Bit array to query.
 *
 * \return the number of bits.
 */
uint32_t spdk_bit_array_capacity(const struct spdk_bit_array *ba);

/**
 * Create a bit array.
 *
 * \param num_bits Number of bits that the bit array is sized to hold.
 *
 * All bits in the array will be cleared.
 *
 * \return a pointer to the new bit array.
 */
struct spdk_bit_array *spdk_bit_array_create(uint32_t num_bits);

/**
 * Free a bit array and set the pointer to NULL.
 *
 * \param bap Bit array to free.
 */
void spdk_bit_array_free(struct spdk_bit_array **bap);

/**
 * Create or resize a bit array.
 *
 * To create a new bit array, pass a pointer to a spdk_bit_array pointer that is
 * NULL for bap.
 *
 * The bit array will be sized to hold at least num_bits.
 *
 * If num_bits is smaller than the previous size of the bit array,
 * any data beyond the new num_bits size will be cleared.
 *
 * If num_bits is larger than the previous size of the bit array,
 * any data beyond the old num_bits size will be cleared.
 *
 * \param bap Bit array to create/resize.
 * \param num_bits Number of bits that the bit array is sized to hold.
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_bit_array_resize(struct spdk_bit_array **bap, uint32_t num_bits);

/**
 * Get the value of a bit from the bit array.
 *
 * If bit_index is beyond the end of the current size of the bit array, this
 * function will return false (i.e. bits beyond the end of the array are implicitly 0).
 *
 * \param ba Bit array to query.
 * \param bit_index The index of a bit to query.
 *
 * \return the value of a bit from the bit array on success, or false on failure.
 */
bool spdk_bit_array_get(const struct spdk_bit_array *ba, uint32_t bit_index);

/**
 * Set (to 1) a bit in the bit array.
 *
 * If bit_index is beyond the end of the bit array, this function will return -EINVAL.
 *
 * \param ba Bit array to set a bit.
 * \param bit_index The index of a bit to set.
 *
 * \return 0 on success, negative errno on failure.
 */
int spdk_bit_array_set(struct spdk_bit_array *ba, uint32_t bit_index);

/**
 * Clear (to 0) a bit in the bit array.
 *
 * If bit_index is beyond the end of the bit array, no action is taken. Bits
 * beyond the end of the bit array are implicitly 0.
 *
 * \param ba Bit array to clear a bit.
 * \param bit_index The index of a bit to clear.
 */
void spdk_bit_array_clear(struct spdk_bit_array *ba, uint32_t bit_index);

/**
 * Find the index of the first set bit in the array.
 *
 * \param ba The bit array to search.
 * \param start_bit_index The bit index from which to start searching (0 to start
 * from the beginning of the array).
 *
 * \return the index of the first set bit. If no bits are set, returns UINT32_MAX.
 */
uint32_t spdk_bit_array_find_first_set(const struct spdk_bit_array *ba, uint32_t start_bit_index);

/**
 * Find the index of the first cleared bit in the array.
 *
 * \param ba The bit array to search.
 * \param start_bit_index The bit index from which to start searching (0 to start
 * from the beginning of the array).
 *
 * \return the index of the first cleared bit. If no bits are cleared, returns UINT32_MAX.
 */
uint32_t spdk_bit_array_find_first_clear(const struct spdk_bit_array *ba, uint32_t start_bit_index);

/**
 * Count the number of set bits in the array.
 *
 * \param ba The bit array to search.
 *
 * \return the number of bits set in the array.
 */
uint32_t spdk_bit_array_count_set(const struct spdk_bit_array *ba);

/**
 * Count the number of cleared bits in the array.
 *
 * \param ba The bit array to search.
 *
 * \return the number of bits cleared in the array.
 */
uint32_t spdk_bit_array_count_clear(const struct spdk_bit_array *ba);

/**
 * Store bitmask from bit array.
 *
 * \param ba Bit array.
 * \param mask Destination mask. Mask and bit array capacity must be equal.
 */
void spdk_bit_array_store_mask(const struct spdk_bit_array *ba, void *mask);

/**
 * Load bitmask to bit array.
 *
 * \param ba Bit array.
 * \param mask Source mask. Mask and bit array capacity must be equal.
 */
void spdk_bit_array_load_mask(struct spdk_bit_array *ba, const void *mask);

/**
 * Clear (to 0) bit array bitmask.
 *
 * \param ba Bit array.
 */
void spdk_bit_array_clear_mask(struct spdk_bit_array *ba);

#ifdef __cplusplus
}
#endif

#endif
