#include <contrib/libs/spdk/ndebug.h>
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

#include "spdk/stdinc.h"

#include "spdk/bit_array.h"
#include "spdk/bit_pool.h"
#include "spdk/env.h"

#include "spdk/likely.h"
#include "spdk/util.h"

typedef uint64_t spdk_bit_array_word;
#define SPDK_BIT_ARRAY_WORD_TZCNT(x)	(__builtin_ctzll(x))
#define SPDK_BIT_ARRAY_WORD_POPCNT(x)	(__builtin_popcountll(x))
#define SPDK_BIT_ARRAY_WORD_C(x)	((spdk_bit_array_word)(x))
#define SPDK_BIT_ARRAY_WORD_BYTES	sizeof(spdk_bit_array_word)
#define SPDK_BIT_ARRAY_WORD_BITS	(SPDK_BIT_ARRAY_WORD_BYTES * 8)
#define SPDK_BIT_ARRAY_WORD_INDEX_SHIFT	spdk_u32log2(SPDK_BIT_ARRAY_WORD_BITS)
#define SPDK_BIT_ARRAY_WORD_INDEX_MASK	((1u << SPDK_BIT_ARRAY_WORD_INDEX_SHIFT) - 1)

struct spdk_bit_array {
	uint32_t bit_count;
	spdk_bit_array_word words[];
};

struct spdk_bit_array *
spdk_bit_array_create(uint32_t num_bits)
{
	struct spdk_bit_array *ba = NULL;

	spdk_bit_array_resize(&ba, num_bits);

	return ba;
}

void
spdk_bit_array_free(struct spdk_bit_array **bap)
{
	struct spdk_bit_array *ba;

	if (!bap) {
		return;
	}

	ba = *bap;
	*bap = NULL;
	spdk_free(ba);
}

static inline uint32_t
bit_array_word_count(uint32_t num_bits)
{
	return (num_bits + SPDK_BIT_ARRAY_WORD_BITS - 1) >> SPDK_BIT_ARRAY_WORD_INDEX_SHIFT;
}

static inline spdk_bit_array_word
bit_array_word_mask(uint32_t num_bits)
{
	assert(num_bits < SPDK_BIT_ARRAY_WORD_BITS);
	return (SPDK_BIT_ARRAY_WORD_C(1) << num_bits) - 1;
}

int
spdk_bit_array_resize(struct spdk_bit_array **bap, uint32_t num_bits)
{
	struct spdk_bit_array *new_ba;
	uint32_t old_word_count, new_word_count;
	size_t new_size;

	/*
	 * Max number of bits allowed is UINT32_MAX - 1, because we use UINT32_MAX to denote
	 * when a set or cleared bit cannot be found.
	 */
	if (!bap || num_bits == UINT32_MAX) {
		return -EINVAL;
	}

	new_word_count = bit_array_word_count(num_bits);
	new_size = offsetof(struct spdk_bit_array, words) + new_word_count * SPDK_BIT_ARRAY_WORD_BYTES;

	/*
	 * Always keep one extra word with a 0 and a 1 past the actual required size so that the
	 * find_first functions can just keep going until they match.
	 */
	new_size += SPDK_BIT_ARRAY_WORD_BYTES;

	new_ba = (struct spdk_bit_array *)spdk_realloc(*bap, new_size, 64);
	if (!new_ba) {
		return -ENOMEM;
	}

	/*
	 * Set up special extra word (see above comment about find_first_clear).
	 *
	 * This is set to 0b10 so that find_first_clear will find a 0 at the very first
	 * bit past the end of the buffer, and find_first_set will find a 1 at the next bit
	 * past that.
	 */
	new_ba->words[new_word_count] = 0x2;

	if (*bap == NULL) {
		old_word_count = 0;
		new_ba->bit_count = 0;
	} else {
		old_word_count = bit_array_word_count(new_ba->bit_count);
	}

	if (new_word_count > old_word_count) {
		/* Zero out new entries */
		memset(&new_ba->words[old_word_count], 0,
		       (new_word_count - old_word_count) * SPDK_BIT_ARRAY_WORD_BYTES);
	} else if (new_word_count == old_word_count && num_bits < new_ba->bit_count) {
		/* Make sure any existing partial last word is cleared beyond the new num_bits. */
		uint32_t last_word_bits;
		spdk_bit_array_word mask;

		last_word_bits = num_bits & SPDK_BIT_ARRAY_WORD_INDEX_MASK;
		mask = bit_array_word_mask(last_word_bits);
		new_ba->words[old_word_count - 1] &= mask;
	}

	new_ba->bit_count = num_bits;
	*bap = new_ba;
	return 0;
}

uint32_t
spdk_bit_array_capacity(const struct spdk_bit_array *ba)
{
	return ba->bit_count;
}

static inline int
bit_array_get_word(const struct spdk_bit_array *ba, uint32_t bit_index,
		   uint32_t *word_index, uint32_t *word_bit_index)
{
	if (spdk_unlikely(bit_index >= ba->bit_count)) {
		return -EINVAL;
	}

	*word_index = bit_index >> SPDK_BIT_ARRAY_WORD_INDEX_SHIFT;
	*word_bit_index = bit_index & SPDK_BIT_ARRAY_WORD_INDEX_MASK;

	return 0;
}

bool
spdk_bit_array_get(const struct spdk_bit_array *ba, uint32_t bit_index)
{
	uint32_t word_index, word_bit_index;

	if (bit_array_get_word(ba, bit_index, &word_index, &word_bit_index)) {
		return false;
	}

	return (ba->words[word_index] >> word_bit_index) & 1U;
}

int
spdk_bit_array_set(struct spdk_bit_array *ba, uint32_t bit_index)
{
	uint32_t word_index, word_bit_index;

	if (bit_array_get_word(ba, bit_index, &word_index, &word_bit_index)) {
		return -EINVAL;
	}

	ba->words[word_index] |= (SPDK_BIT_ARRAY_WORD_C(1) << word_bit_index);
	return 0;
}

void
spdk_bit_array_clear(struct spdk_bit_array *ba, uint32_t bit_index)
{
	uint32_t word_index, word_bit_index;

	if (bit_array_get_word(ba, bit_index, &word_index, &word_bit_index)) {
		/*
		 * Clearing past the end of the bit array is a no-op, since bit past the end
		 * are implicitly 0.
		 */
		return;
	}

	ba->words[word_index] &= ~(SPDK_BIT_ARRAY_WORD_C(1) << word_bit_index);
}

static inline uint32_t
bit_array_find_first(const struct spdk_bit_array *ba, uint32_t start_bit_index,
		     spdk_bit_array_word xor_mask)
{
	uint32_t word_index, first_word_bit_index;
	spdk_bit_array_word word, first_word_mask;
	const spdk_bit_array_word *words, *cur_word;

	if (spdk_unlikely(start_bit_index >= ba->bit_count)) {
		return ba->bit_count;
	}

	word_index = start_bit_index >> SPDK_BIT_ARRAY_WORD_INDEX_SHIFT;
	words = ba->words;
	cur_word = &words[word_index];

	/*
	 * Special case for first word: skip start_bit_index % SPDK_BIT_ARRAY_WORD_BITS bits
	 * within the first word.
	 */
	first_word_bit_index = start_bit_index & SPDK_BIT_ARRAY_WORD_INDEX_MASK;
	first_word_mask = bit_array_word_mask(first_word_bit_index);

	word = (*cur_word ^ xor_mask) & ~first_word_mask;

	/*
	 * spdk_bit_array_resize() guarantees that an extra word with a 1 and a 0 will always be
	 * at the end of the words[] array, so just keep going until a word matches.
	 */
	while (word == 0) {
		word = *++cur_word ^ xor_mask;
	}

	return ((uintptr_t)cur_word - (uintptr_t)words) * 8 + SPDK_BIT_ARRAY_WORD_TZCNT(word);
}


uint32_t
spdk_bit_array_find_first_set(const struct spdk_bit_array *ba, uint32_t start_bit_index)
{
	uint32_t bit_index;

	bit_index = bit_array_find_first(ba, start_bit_index, 0);

	/*
	 * If we ran off the end of the array and found the 1 bit in the extra word,
	 * return UINT32_MAX to indicate no actual 1 bits were found.
	 */
	if (bit_index >= ba->bit_count) {
		bit_index = UINT32_MAX;
	}

	return bit_index;
}

uint32_t
spdk_bit_array_find_first_clear(const struct spdk_bit_array *ba, uint32_t start_bit_index)
{
	uint32_t bit_index;

	bit_index = bit_array_find_first(ba, start_bit_index, SPDK_BIT_ARRAY_WORD_C(-1));

	/*
	 * If we ran off the end of the array and found the 0 bit in the extra word,
	 * return UINT32_MAX to indicate no actual 0 bits were found.
	 */
	if (bit_index >= ba->bit_count) {
		bit_index = UINT32_MAX;
	}

	return bit_index;
}

uint32_t
spdk_bit_array_count_set(const struct spdk_bit_array *ba)
{
	const spdk_bit_array_word *cur_word = ba->words;
	uint32_t word_count = bit_array_word_count(ba->bit_count);
	uint32_t set_count = 0;

	while (word_count--) {
		/*
		 * No special treatment is needed for the last (potentially partial) word, since
		 * spdk_bit_array_resize() makes sure the bits past bit_count are cleared.
		 */
		set_count += SPDK_BIT_ARRAY_WORD_POPCNT(*cur_word++);
	}

	return set_count;
}

uint32_t
spdk_bit_array_count_clear(const struct spdk_bit_array *ba)
{
	return ba->bit_count - spdk_bit_array_count_set(ba);
}

void
spdk_bit_array_store_mask(const struct spdk_bit_array *ba, void *mask)
{
	uint32_t size, i;
	uint32_t num_bits = spdk_bit_array_capacity(ba);

	size = num_bits / CHAR_BIT;
	memcpy(mask, ba->words, size);

	for (i = 0; i < num_bits % CHAR_BIT; i++) {
		if (spdk_bit_array_get(ba, i + size * CHAR_BIT)) {
			((uint8_t *)mask)[size] |= (1U << i);
		} else {
			((uint8_t *)mask)[size] &= ~(1U << i);
		}
	}
}

void
spdk_bit_array_load_mask(struct spdk_bit_array *ba, const void *mask)
{
	uint32_t size, i;
	uint32_t num_bits = spdk_bit_array_capacity(ba);

	size = num_bits / CHAR_BIT;
	memcpy(ba->words, mask, size);

	for (i = 0; i < num_bits % CHAR_BIT; i++) {
		if (((uint8_t *)mask)[size] & (1U << i)) {
			spdk_bit_array_set(ba, i + size * CHAR_BIT);
		} else {
			spdk_bit_array_clear(ba, i + size * CHAR_BIT);
		}
	}
}

void
spdk_bit_array_clear_mask(struct spdk_bit_array *ba)
{
	uint32_t size, i;
	uint32_t num_bits = spdk_bit_array_capacity(ba);

	size = num_bits / CHAR_BIT;
	memset(ba->words, 0, size);

	for (i = 0; i < num_bits % CHAR_BIT; i++) {
		spdk_bit_array_clear(ba, i + size * CHAR_BIT);
	}
}

struct spdk_bit_pool {
	struct spdk_bit_array	*array;
	uint32_t		lowest_free_bit;
	uint32_t		free_count;
};

struct spdk_bit_pool *
spdk_bit_pool_create(uint32_t num_bits)
{
	struct spdk_bit_pool *pool = NULL;
	struct spdk_bit_array *array;

	array = spdk_bit_array_create(num_bits);
	if (array == NULL) {
		return NULL;
	}

	pool = calloc(1, sizeof(*pool));
	if (pool == NULL) {
		spdk_bit_array_free(&array);
		return NULL;
	}

	pool->array = array;
	pool->lowest_free_bit = 0;
	pool->free_count = num_bits;

	return pool;
}

struct spdk_bit_pool *
spdk_bit_pool_create_from_array(struct spdk_bit_array *array)
{
	struct spdk_bit_pool *pool = NULL;

	pool = calloc(1, sizeof(*pool));
	if (pool == NULL) {
		return NULL;
	}

	pool->array = array;
	pool->lowest_free_bit = spdk_bit_array_find_first_clear(array, 0);
	pool->free_count = spdk_bit_array_count_clear(array);

	return pool;
}

void
spdk_bit_pool_free(struct spdk_bit_pool **ppool)
{
	struct spdk_bit_pool *pool;

	if (!ppool) {
		return;
	}

	pool = *ppool;
	*ppool = NULL;
	if (pool != NULL) {
		spdk_bit_array_free(&pool->array);
		free(pool);
	}
}

int
spdk_bit_pool_resize(struct spdk_bit_pool **ppool, uint32_t num_bits)
{
	struct spdk_bit_pool *pool;
	int rc;

	assert(ppool != NULL);

	pool = *ppool;
	rc = spdk_bit_array_resize(&pool->array, num_bits);
	if (rc) {
		return rc;
	}

	pool->lowest_free_bit = spdk_bit_array_find_first_clear(pool->array, 0);
	pool->free_count = spdk_bit_array_count_clear(pool->array);

	return 0;
}

uint32_t
spdk_bit_pool_capacity(const struct spdk_bit_pool *pool)
{
	return spdk_bit_array_capacity(pool->array);
}

bool
spdk_bit_pool_is_allocated(const struct spdk_bit_pool *pool, uint32_t bit_index)
{
	return spdk_bit_array_get(pool->array, bit_index);
}

uint32_t
spdk_bit_pool_allocate_bit(struct spdk_bit_pool *pool)
{
	uint32_t bit_index = pool->lowest_free_bit;

	if (bit_index == UINT32_MAX) {
		return UINT32_MAX;
	}

	spdk_bit_array_set(pool->array, bit_index);
	pool->lowest_free_bit = spdk_bit_array_find_first_clear(pool->array, bit_index);
	pool->free_count--;
	return bit_index;
}

void
spdk_bit_pool_free_bit(struct spdk_bit_pool *pool, uint32_t bit_index)
{
	assert(spdk_bit_array_get(pool->array, bit_index) == true);

	spdk_bit_array_clear(pool->array, bit_index);
	if (pool->lowest_free_bit > bit_index) {
		pool->lowest_free_bit = bit_index;
	}
	pool->free_count++;
}

uint32_t
spdk_bit_pool_count_allocated(const struct spdk_bit_pool *pool)
{
	return spdk_bit_array_capacity(pool->array) - pool->free_count;
}

uint32_t
spdk_bit_pool_count_free(const struct spdk_bit_pool *pool)
{
	return pool->free_count;
}

void
spdk_bit_pool_store_mask(const struct spdk_bit_pool *pool, void *mask)
{
	spdk_bit_array_store_mask(pool->array, mask);
}

void
spdk_bit_pool_load_mask(struct spdk_bit_pool *pool, const void *mask)
{
	spdk_bit_array_load_mask(pool->array, mask);
	pool->lowest_free_bit = spdk_bit_array_find_first_clear(pool->array, 0);
	pool->free_count = spdk_bit_array_count_clear(pool->array);
}

void
spdk_bit_pool_free_all_bits(struct spdk_bit_pool *pool)
{
	spdk_bit_array_clear_mask(pool->array);
	pool->lowest_free_bit = 0;
	pool->free_count = spdk_bit_array_capacity(pool->array);
}
