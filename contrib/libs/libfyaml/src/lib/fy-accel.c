/*
 * fy-accel.c - YAML accelerated access methods
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <limits.h>
#include <string.h>

#include <libfyaml.h>

#include "fy-parse.h"
#include "fy-doc.h"

#include "fy-accel.h"

#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

/* powers of two and the closest primes before
 *
 * pow2:  1 2 4 8 16 32 64 128 256 512 1024 2048 4096 8192 16384 32768 65536
 * prime: 1 2 3 7 13 31 61 127 251 509 1021 2039 4093 8191 16381 32749 65521
 *
 * pow2:  131072 262144 524288
 * prime: 130657 262051 524201
 */

/* 64K bucket should be enough for everybody */
static const uint32_t prime_lt_pow2[] = {
	1, 2, 3, 7, 13, 31, 61, 127, 251, 509, 1021,
	2039, 4093, 8191, 16381, 32749, 65521,
	130657, 262051, 524201
};

static inline unsigned int
fy_accel_hash_to_pos(struct fy_accel *xl, const void *hash, unsigned int nbuckets)
{
	uint64_t pos;

	switch (xl->hd->size) {
	case 1:
		pos = *(const uint8_t *)hash;
		break;
	case 2:
		assert(!((uintptr_t)hash & 1));
		pos = *(const uint16_t *)hash;
		break;
	case 4:
		assert(!((uintptr_t)hash & 3));
		pos = *(const uint32_t *)hash;
		break;
	case 8:
		assert(!((uintptr_t)hash & 7));
		pos = *(const uint64_t *)hash;
		break;
	default:
		/* sigh, what ever */
		pos = XXH32(hash, xl->hd->size, 0);
		break;
	}

	return (unsigned int)(pos % nbuckets);
}

static inline bool
fy_accel_hash_eq(struct fy_accel *xl, const void *hash1, const void *hash2)
{
	switch (xl->hd->size) {
	case 1:
		return *(const uint8_t *)hash1 == *(const uint8_t *)hash2;
	case 2:
		assert(!((uintptr_t)hash1 & 1));
		assert(!((uintptr_t)hash2 & 1));
		return *(const uint16_t *)hash1 == *(const uint16_t *)hash2;
	case 4:
		assert(!((uintptr_t)hash1 & 3));
		assert(!((uintptr_t)hash2 & 3));
		return *(const uint32_t *)hash1 == *(const uint32_t *)hash2;
	case 8:
		assert(!((uintptr_t)hash1 & 7));
		assert(!((uintptr_t)hash2 & 7));
		return *(const uint64_t *)hash1 == *(const uint64_t *)hash2;
	default:
		break;
	}

	return !memcmp(hash1, hash2, xl->hd->size);
}

int fy_accel_resize(struct fy_accel *xl, unsigned int min_buckets)
{
	unsigned int next_pow2, exp, i, nbuckets, pos;
	struct fy_accel_entry_list *xlel;
	struct fy_accel_entry *xle;
	struct fy_accel_entry_list *buckets_new;

	/* get the next power of two larger or equal */
	next_pow2 = 1;
	exp = 0;
	while (next_pow2 < min_buckets &&
	       exp < sizeof(prime_lt_pow2)/sizeof(prime_lt_pow2[0])) {
		next_pow2 <<= 1;
		exp++;
	}

	nbuckets = prime_lt_pow2[exp];
	if (nbuckets == xl->nbuckets)
		return 0;

	buckets_new = malloc(sizeof(*buckets_new) * nbuckets);
	if (!buckets_new)
		return -1;

	for (i = 0, xlel = buckets_new; i < nbuckets; i++, xlel++)
		fy_accel_entry_list_init(xlel);

	if (xl->buckets) {
		for (i = 0, xlel = xl->buckets; i < xl->nbuckets; i++, xlel++) {
			while ((xle = fy_accel_entry_list_pop(xlel)) != NULL) {
				pos = fy_accel_hash_to_pos(xl, xle->hash, nbuckets);
				fy_accel_entry_list_add_tail(&buckets_new[pos], xle);
			}
		}
		free(xl->buckets);
	}
	xl->buckets = buckets_new;
	xl->nbuckets = nbuckets;
	xl->next_exp2 = exp;

	return 0;
}

int fy_accel_grow(struct fy_accel *xl)
{
	if (!xl)
		return -1;

	/* should not grow indefinetely */
	if (xl->next_exp2 >= sizeof(prime_lt_pow2)/sizeof(prime_lt_pow2[0]))
		return -1;

	return fy_accel_resize(xl, prime_lt_pow2[xl->next_exp2 + 1]);
}

int fy_accel_shrink(struct fy_accel *xl)
{
	if (!xl)
		return -1;

	/* should not shrink indefinetely */
	if (xl->next_exp2 <= 0)
		return -1;

	return fy_accel_resize(xl, prime_lt_pow2[xl->next_exp2 - 1]);
}

int
fy_accel_setup(struct fy_accel *xl,
	       const struct fy_hash_desc *hd,
	       void *userdata,
	       unsigned int min_buckets)
{
	if (!xl || !hd || !hd->size || !hd->hash)
		return -1;

	memset(xl, 0, sizeof(*xl));
	xl->hd = hd;
	xl->userdata = userdata;
	xl->count = 0;

	return fy_accel_resize(xl, min_buckets);
}

void fy_accel_cleanup(struct fy_accel *xl)
{
	unsigned int i;
	struct fy_accel_entry_list *xlel;
	struct fy_accel_entry *xle;

	if (!xl)
		return;

	for (i = 0, xlel = xl->buckets; i < xl->nbuckets; i++, xlel++) {
		while ((xle = fy_accel_entry_list_pop(xlel)) != NULL) {
			free(xle);
			assert(xl->count > 0);
			xl->count--;
		}
	}

	free(xl->buckets);
}

struct fy_accel_entry *
fy_accel_entry_insert(struct fy_accel *xl, const void *key, const void *value)
{
	struct fy_accel_entry *xle, *xlet;
	struct fy_accel_entry_list *xlel;
	unsigned int pos, bucket_size;
	int rc;

	if (!xl)
		return NULL;

	xle = malloc(sizeof(*xle) + xl->hd->size);
	if (!xle)
		goto err_out;

	rc = xl->hd->hash(xl, key, xl->userdata, xle->hash);
	if (rc)
		goto err_out;
	xle->key = key;
	xle->value = value;

	pos = fy_accel_hash_to_pos(xl, xle->hash, xl->nbuckets);
	xlel = &xl->buckets[pos];

	fy_accel_entry_list_add_tail(xlel, xle);

	assert(xl->count < UINT_MAX);
	xl->count++;

	/* if we don't auto-resize, return */
	if (xl->hd->max_bucket_grow_limit) {
		bucket_size = 0;
		for (xlet = fy_accel_entry_list_first(xlel); xlet; xlet = fy_accel_entry_next(xlel, xlet)) {
			bucket_size++;
			if (bucket_size >= xl->hd->max_bucket_grow_limit)
				break;
		}

		/* we don't really care whether the grow up succeeds or not */
		if (bucket_size >= xl->hd->max_bucket_grow_limit)
			(void)fy_accel_grow(xl);
	}

	return xle;
err_out:
	if (xle)
		free(xle);
	return NULL;
}

struct fy_accel_entry *
fy_accel_entry_lookup(struct fy_accel *xl, const void *key)
{
	struct fy_accel_entry_iter xli;
	struct fy_accel_entry *xle;

	xle = fy_accel_entry_iter_start(&xli, xl, key);
	fy_accel_entry_iter_finish(&xli);

	return xle;
}

struct fy_accel_entry *
fy_accel_entry_lookup_key_value(struct fy_accel *xl, const void *key, const void *value)
{
	struct fy_accel_entry_iter xli;
	struct fy_accel_entry *xle;

	for (xle = fy_accel_entry_iter_start(&xli, xl, key); xle;
	     xle = fy_accel_entry_iter_next(&xli)) {

		if (xle->value == value)
			break;
	}
	fy_accel_entry_iter_finish(&xli);

	return xle;
}

void
fy_accel_entry_remove(struct fy_accel *xl, struct fy_accel_entry *xle)
{
	unsigned int pos;

	if (!xl || !xle)
		return;

	pos = fy_accel_hash_to_pos(xl, xle->hash, xl->nbuckets);

	fy_accel_entry_list_del(&xl->buckets[pos], xle);

	assert(xl->count > 0);
	xl->count--;

	free(xle);
}

int
fy_accel_insert(struct fy_accel *xl, const void *key, const void *value)
{
	struct fy_accel_entry *xle;

	xle = fy_accel_entry_lookup(xl, key);
	if (xle)
		return -1;	/* exists */

	xle = fy_accel_entry_insert(xl, key, value);
	if (!xle)
		return -1;	/* failure to insert */

	return 0;
}

const void *
fy_accel_lookup(struct fy_accel *xl, const void *key)
{
	struct fy_accel_entry *xle;

	xle = fy_accel_entry_lookup(xl, key);
	return xle ? xle->value : NULL;
}

int
fy_accel_remove(struct fy_accel *xl, const void *data)
{
	struct fy_accel_entry *xle;

	xle = fy_accel_entry_lookup(xl, data);
	if (!xle)
		return -1;

	fy_accel_entry_remove(xl, xle);

	return 0;
}

struct fy_accel_entry *
fy_accel_entry_iter_next_internal(struct fy_accel_entry_iter *xli)
{
	struct fy_accel *xl;
	struct fy_accel_entry *xle;
	struct fy_accel_entry_list *xlel;
	const void *key;
	void *hash;

	if (!xli)
		return NULL;

	xl = xli->xl;
	hash = xli->hash;
	xlel = xli->xlel;
	if (!xl || !hash || !xlel)
		return NULL;
	key = xli->key;

	xle = !xli->xle ? fy_accel_entry_list_first(xlel) :
			  fy_accel_entry_next(xlel, xli->xle);
	for (; xle; xle = fy_accel_entry_next(xlel, xle)) {
		if (fy_accel_hash_eq(xl, hash, xle->hash) &&
		    xl->hd->eq(xl, hash, xle->key, key, xl->userdata))
			break;
	}
	return xli->xle = xle;
}

struct fy_accel_entry *
fy_accel_entry_iter_start(struct fy_accel_entry_iter *xli, struct fy_accel *xl, const void *key)
{
	unsigned int pos;
	int rc;

	if (!xli || !xl)
		return NULL;
	xli->xl = xl;
	xli->key = key;
	if (xl->hd->size <= sizeof(xli->hash_inline))
		xli->hash = xli->hash_inline;
	else
		xli->hash = malloc(xl->hd->size);
	xli->xlel = NULL;

	if (!xli->hash)
		goto err_out;

	rc = xl->hd->hash(xl, key, xl->userdata, xli->hash);
	if (rc)
		goto err_out;

	pos = fy_accel_hash_to_pos(xl, xli->hash, xl->nbuckets);
	xli->xlel = &xl->buckets[pos];

	xli->xle = NULL;

	return fy_accel_entry_iter_next_internal(xli);

err_out:
	fy_accel_entry_iter_finish(xli);
	return NULL;
}

void fy_accel_entry_iter_finish(struct fy_accel_entry_iter *xli)
{
	if (!xli)
		return;

	if (xli->hash && xli->hash != xli->hash_inline)
		free(xli->hash);
}

struct fy_accel_entry *
fy_accel_entry_iter_next(struct fy_accel_entry_iter *xli)
{
	if (!xli || !xli->xle)
		return NULL;

	return fy_accel_entry_iter_next_internal(xli);
}
