/*
 * fy-doc.h - YAML document internal header file
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_DOC_H
#define FY_DOC_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-ctype.h"
#include "fy-utf8.h"
#include "fy-list.h"
#include "fy-typelist.h"
#include "fy-types.h"
#include "fy-diag.h"
#include "fy-dump.h"
#include "fy-docstate.h"
#include "fy-accel.h"
#include "fy-walk.h"
#include "fy-path.h"

struct fy_eventp;

/* TODO vary according to platfom */
static inline int fy_depth_limit(void)
{
	return FYPCF_GUARANTEED_MINIMUM_DEPTH_LIMIT;
}

FY_TYPE_FWD_DECL_LIST(document);

struct fy_node;

struct fy_node_pair {
	struct fy_list_head node;
	struct fy_node *key;
	struct fy_node *value;
	struct fy_document *fyd;
	struct fy_node *parent;
};
FY_TYPE_FWD_DECL_LIST(node_pair);
FY_TYPE_DECL_LIST(node_pair);

FY_TYPE_FWD_DECL_LIST(node);
struct fy_node {
	struct fy_list_head node;
	struct fy_token *tag;
	enum fy_node_style style;
	struct fy_node *parent;
	struct fy_document *fyd;
	unsigned int marks;
#if !defined(_MSC_VER)
	enum fy_node_type type : 2;	/* 2 bits are enough for 3 types */
#else
	enum fy_node_type type;	/* it converted incorrectly and can't be used as supposed to be */
#endif
	bool has_meta : 1;
	bool attached : 1;		/* when it's attached somewhere */
	bool synthetic : 1;		/* node has been modified programmaticaly */
	bool key_root : 1;		/* node is the root of key fy_node_get_parent() will return NULL */
	void *meta;
	struct fy_accel *xl;		/* mapping access accelerator */
	struct fy_path_expr_node_data *pxnd;
	union {
		struct fy_token *scalar;
		struct fy_node_list sequence;
		struct fy_node_pair_list mapping;
	};
	union {
		struct fy_token *sequence_start;
		struct fy_token *mapping_start;
	};
	union {
		struct fy_token *sequence_end;
		struct fy_token *mapping_end;
	};
};
FY_TYPE_DECL_LIST(node);

struct fy_node *fy_node_alloc(struct fy_document *fyd, enum fy_node_type type);
struct fy_node_pair *fy_node_pair_alloc(struct fy_document *fyd);
int fy_node_pair_free(struct fy_node_pair *fynp);

void fy_node_detach_and_free(struct fy_node *fyn);
void fy_node_pair_detach_and_free(struct fy_node_pair *fynp);

struct fy_anchor {
	struct fy_list_head node;
	struct fy_node *fyn;
	struct fy_token *anchor;
	bool multiple : 1;
};
FY_TYPE_FWD_DECL_LIST(anchor);
FY_TYPE_DECL_LIST(anchor);

struct fy_document {
	struct fy_list_head node;
	struct fy_anchor_list anchors;
	struct fy_accel *axl;		/* name -> anchor access accelerator */
	struct fy_accel *naxl;		/* node -> anchor access accelerator */
	struct fy_document_state *fyds;
	struct fy_diag *diag;
	struct fy_parse_cfg parse_cfg;
	struct fy_node *root;
	bool parse_error : 1;

	struct fy_document *parent;
	struct fy_document_list children;

	fy_node_meta_clear_fn meta_clear_fn;
	void *meta_user;

	fy_document_on_destroy_fn on_destroy_fn;
	void *userdata;

	struct fy_path_expr_document_data *pxdd;
};
/* only the list declaration/methods */
FY_TYPE_DECL_LIST(document);

struct fy_document *fy_parse_document_create(struct fy_parser *fyp, struct fy_eventp *fyep);

struct fy_node_mapping_sort_ctx {
	fy_node_mapping_sort_fn key_cmp;
	void *arg;
	struct fy_node_pair **fynpp;
	int count;
};

void fy_node_mapping_perform_sort(struct fy_node *fyn_map,
		fy_node_mapping_sort_fn key_cmp, void *arg,
		struct fy_node_pair **fynpp, int count);

void fy_node_mapping_fill_array(struct fy_node *fyn_map,
		struct fy_node_pair **fynpp, int count);

struct fy_node_pair **fy_node_mapping_sort_array(struct fy_node *fyn_map,
		fy_node_mapping_sort_fn key_cmp,
		void *arg, int *countp);

void fy_node_mapping_release_array(struct fy_node *fyn_map, struct fy_node_pair **fynpp);

struct fy_node_walk_ctx {
	unsigned int max_depth;
	unsigned int next_slot;
	unsigned int mark;
	struct fy_node *marked[0];
};

bool fy_node_is_empty(struct fy_node *fyn);

bool fy_check_ref_loop(struct fy_document *fyd, struct fy_node *fyn,
		       enum fy_node_walk_flags flags,
		       struct fy_node_walk_ctx *ctx);

#define FYNWF_VISIT_MARKER	(FYNWF_MAX_USER_MARKER + 1)
#define FYNWF_REF_MARKER	(FYNWF_MAX_USER_MARKER + 2)

#define FYNWF_SYSTEM_MARKS	(FY_BIT(FYNWF_VISIT_MARKER) | \
				 FY_BIT(FYNWF_REF_MARKER))

bool fy_node_uses_single_input_only(struct fy_node *fyn, struct fy_input *fyi);
struct fy_input *fy_node_get_first_input(struct fy_node *fyn);
bool fy_node_is_synthetic(struct fy_node *fyn);
void fy_node_mark_synthetic(struct fy_node *fyn);
struct fy_input *fy_node_get_input(struct fy_node *fyn);
int fy_document_register_anchor(struct fy_document *fyd,
				struct fy_node *fyn, struct fy_token *anchor);
bool fy_node_mapping_key_is_duplicate(struct fy_node *fyn, struct fy_node *fyn_key);

struct fy_token *fy_node_non_synthesized_token(struct fy_node *fyn);
struct fy_token *fy_node_token(struct fy_node *fyn);

FILE *fy_document_get_error_fp(struct fy_document *fyd);
enum fy_parse_cfg_flags fy_document_get_cfg_flags(const struct fy_document *fyd);
bool fy_document_is_accelerated(struct fy_document *fyd);
bool fy_document_can_be_accelerated(struct fy_document *fyd);

/* TODO move to main include */
struct fy_node *fy_node_collection_iterate(struct fy_node *fyn, void **prevp);

/* indirect node */
FY_TYPE_FWD_DECL_LIST(ptr_node);
struct fy_ptr_node {
	struct fy_list_head node;
	struct fy_node *fyn;
};
FY_TYPE_DECL_LIST(ptr_node);

struct fy_ptr_node *fy_ptr_node_create(struct fy_node *fyn);
void fy_ptr_node_destroy(struct fy_ptr_node *fypn);
void fy_ptr_node_list_free_all(struct fy_ptr_node_list *fypnl);
bool fy_ptr_node_list_contains(struct fy_ptr_node_list *fypnl, struct fy_node *fyn);
int fy_node_linearize_recursive(struct fy_ptr_node_list *fypnl, struct fy_node *fyn);
int fy_node_linearize(struct fy_ptr_node_list *fypnl, struct fy_node *fyn);
void fy_node_iterator_check(struct fy_node *fyn);


enum fy_document_iterator_state {
	FYDIS_WAITING_STREAM_START,
	FYDIS_WAITING_DOCUMENT_START,
	FYDIS_WAITING_BODY_START_OR_DOCUMENT_END,
	FYDIS_BODY,
	FYDIS_WAITING_DOCUMENT_END,
	FYDIS_WAITING_STREAM_END_OR_DOCUMENT_START,
	FYDIS_ERROR,
};

struct fy_document_iterator_body_state {
	struct fy_node *fyn;	/* the collection node */
	bool processed_key : 1;	/* for mapping only */
	union {
		struct fy_node *fyni;		/* for sequence */
		struct fy_node_pair *fynp;	/* for mapping */
	};
};

struct fy_document_iterator {
	enum fy_document_iterator_state state;
	struct fy_document *fyd;
	struct fy_node *iterate_root;
	bool suppress_recycling_force : 1;
	bool suppress_recycling : 1;

	struct fy_eventp_list recycled_eventp;
	struct fy_token_list recycled_token;

	struct fy_eventp_list *recycled_eventp_list;	/* NULL when suppressing */
	struct fy_token_list *recycled_token_list;	/* NULL when suppressing */

	unsigned int stack_top;
	unsigned int stack_alloc;
	struct fy_document_iterator_body_state *stack;
	struct fy_document_iterator_body_state in_place[FYPCF_GUARANTEED_MINIMUM_DEPTH_LIMIT];
};

void fy_document_iterator_setup(struct fy_document_iterator *fydi);
void fy_document_iterator_cleanup(struct fy_document_iterator *fydi);
struct fy_document_iterator *fy_document_iterator_create(void);
void fy_document_iterator_destroy(struct fy_document_iterator *fydi);
void fy_document_iterator_start(struct fy_document_iterator *fydi, struct fy_document *fyd);
void fy_document_iterator_end(struct fy_document_iterator *fydi);

struct fy_document_iterator_body_result {
	struct fy_node *fyn;
	bool end;
};

bool
fy_document_iterator_body_next_internal(struct fy_document_iterator *fydi,
					struct fy_document_iterator_body_result *res);

#endif
