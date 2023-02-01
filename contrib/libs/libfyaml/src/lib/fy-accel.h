/*
 * fy-accel.h - YAML accelerated access methods
 *
 * Copyright (c) 2020 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_ACCEL_H
#define FY_ACCEL_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdbool.h>

#include <libfyaml.h>

#include "fy-list.h"
#include "fy-typelist.h"

struct fy_accel_entry {
	struct fy_list_head node;
	const void *key;
	const void *value;
	uint8_t hash[0];
};
FY_TYPE_FWD_DECL_LIST(accel_entry);
FY_TYPE_DECL_LIST(accel_entry);

struct fy_accel;

struct fy_hash_desc {
	unsigned int size;
	unsigned int max_bucket_grow_limit;
	bool unique;
	int (*hash)(struct fy_accel *xl, const void *key, void *userdata, void *hash);
	bool (*eq)(struct fy_accel *xl, const void *hash, const void *key1, const void *key2, void *userdata);
};

struct fy_accel {
	const struct fy_hash_desc *hd;
	void *userdata;
	unsigned int count;
	unsigned int nbuckets;
	unsigned int next_exp2;
	struct fy_accel_entry_list *buckets;
};

int
fy_accel_setup(struct fy_accel *xl,
	       const struct fy_hash_desc *hd,
	       void *userdata,
	       unsigned int min_buckets);

void fy_accel_cleanup(struct fy_accel *xl);

int fy_accel_resize(struct fy_accel *xl, unsigned int min_buckets);
int fy_accel_grow(struct fy_accel *xl);
int fy_accel_shrink(struct fy_accel *xl);

int fy_accel_insert(struct fy_accel *xl, const void *key, const void *value);
const void *fy_accel_lookup(struct fy_accel *xl, const void *key);
int fy_accel_remove(struct fy_accel *xl, const void *key);

struct fy_accel_entry_iter {
	struct fy_accel *xl;
	const void *key;
	void *hash;
	struct fy_accel_entry_list *xlel;
	struct fy_accel_entry *xle;
	uint64_t hash_inline[4];	/* to avoid allocation */
};

struct fy_accel_entry *
fy_accel_entry_insert(struct fy_accel *xl, const void *key, const void *value);

struct fy_accel_entry *
fy_accel_entry_lookup(struct fy_accel *xl, const void *key);
struct fy_accel_entry *
fy_accel_entry_lookup_key_value(struct fy_accel *xl, const void *key, const void *value);

void fy_accel_entry_remove(struct fy_accel *xl, struct fy_accel_entry *xle);

struct fy_accel_entry *
fy_accel_entry_iter_start(struct fy_accel_entry_iter *xli,
			  struct fy_accel *xl, const void *key);
void fy_accel_entry_iter_finish(struct fy_accel_entry_iter *xli);
struct fy_accel_entry *
fy_accel_entry_iter_next(struct fy_accel_entry_iter *xli);

#endif
