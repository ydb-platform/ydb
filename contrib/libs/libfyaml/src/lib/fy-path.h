/*
 * fy-path.h - YAML parser private path definitions
 *
 * Copyright (c) 2021 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_PATH_H
#define FY_PATH_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdbool.h>

#include <libfyaml.h>

#include "fy-list.h"
#include "fy-typelist.h"

#include "fy-emit-accum.h"

FY_TYPE_FWD_DECL_LIST(path_component);

enum fy_path_component_type {
	FYPCT_NONE,	/* not yet instantiated */
	FYPCT_MAP,	/* it's a mapping */
	FYPCT_SEQ,	/* it's a sequence */
};

/* fwd declaration */
struct fy_document;
struct fy_document_builder;

#define FY_PATH_MAPPING_SHORT_KEY	32

struct fy_path_mapping_state {
	bool root : 1;		/* no keys, values yet */
	bool await_key : 1;
	bool accumulating_complex_key : 1;
	bool has_key : 1;			/* has a key */
	bool is_complex_key : 1;
	bool complex_key_complete : 1;
	union {
		struct {
			struct fy_token *tag;
			struct fy_token *key;
		} scalar;
		struct fy_document *complex_key;
	};
	void *key_user_data;
};

struct fy_path_sequence_state {
	int idx;
};

struct fy_path_component {
	struct fy_list_head node;
	enum fy_path_component_type type;
	union {
		struct fy_path_mapping_state map;
		struct fy_path_sequence_state seq;
	};
	void *user_data;
};
FY_TYPE_DECL_LIST(path_component);

static inline bool
fy_path_component_is_collection_root(struct fy_path_component *fypc)
{
	if (!fypc)
		return false;

	switch (fypc->type) {
	case FYPCT_NONE:
		break;
	case FYPCT_SEQ:
		return fypc->seq.idx < 0;
	case FYPCT_MAP:
		return fypc->map.root;
	}

	return false;
}

FY_TYPE_FWD_DECL_LIST(path);
struct fy_path {
	struct fy_list_head node;
	struct fy_path_component_list recycled_component;
	struct fy_path_component_list components;
	struct fy_document_builder *fydb;	/* for complex keys */
	struct fy_path *parent;			/* when we have a parent */
	void *user_data;
};
FY_TYPE_DECL_LIST(path);

struct fy_path *fy_path_create(void);
void fy_path_destroy(struct fy_path *fypp);

void fy_path_reset(struct fy_path *fypp);

struct fy_path_component *fy_path_component_alloc(struct fy_path *fypp);
void fy_path_component_cleanup(struct fy_path_component *fypc);
void fy_path_component_free(struct fy_path_component *fypc);
void fy_path_component_destroy(struct fy_path_component *fypc);
void fy_path_component_recycle(struct fy_path *fypp, struct fy_path_component *fypc);
void fy_path_component_clear_state(struct fy_path_component *fypc);

struct fy_path_component *fy_path_component_create_mapping(struct fy_path *fypp);
struct fy_path_component *fy_path_component_create_sequence(struct fy_path *fypp);

#endif
