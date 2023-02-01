/*
 * fy-path.c - Internal ypath support
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-parse.h"
#include "fy-doc.h"

#include "fy-utils.h"

#undef DBG
// #define DBG fyp_notice
#define DBG fyp_scan_debug

static int fy_path_setup(struct fy_path *fypp)
{
	memset(fypp, 0, sizeof(*fypp));

	fy_path_component_list_init(&fypp->recycled_component);
	fy_path_component_list_init(&fypp->components);

	return 0;
}

static void fy_path_cleanup(struct fy_path *fypp)
{
	struct fy_path_component *fypc;

	if (!fypp)
		return;

	if (fypp->fydb) {
		fy_document_builder_destroy(fypp->fydb);
		fypp->fydb = NULL;
	}

	while ((fypc = fy_path_component_list_pop(&fypp->components)) != NULL)
		fy_path_component_free(fypc);

	while ((fypc = fy_path_component_list_pop(&fypp->recycled_component)) != NULL)
		fy_path_component_free(fypc);
}

struct fy_path *fy_path_create(void)
{
	struct fy_path *fypp;
	int rc;

	fypp = malloc(sizeof(*fypp));
	if (!fypp)
		return NULL;

	rc = fy_path_setup(fypp);
	if (rc)
		return NULL;

	return fypp;
}

void fy_path_destroy(struct fy_path *fypp)
{
	if (!fypp)
		return;

	fy_path_cleanup(fypp);

	free(fypp);
}

void fy_path_reset(struct fy_path *fypp)
{
	struct fy_path_component *fypc;

	if (!fypp)
		return;

	while ((fypc = fy_path_component_list_pop(&fypp->components)) != NULL)
		fy_path_component_free(fypc);
}

struct fy_path_component *fy_path_component_alloc(struct fy_path *fypp)
{
	struct fy_path_component *fypc;

	if (!fypp)
		return NULL;

	fypc = fy_path_component_list_pop(&fypp->recycled_component);
	if (!fypc) {
		fypc = malloc(sizeof(*fypc));
		if (!fypc)
			return NULL;
		memset(fypc, 0, sizeof(*fypc));
	}

	/* not yet instantiated */
	fypc->type = FYPCT_NONE;

	return fypc;
}

void fy_path_component_clear_state(struct fy_path_component *fypc)
{
	if (!fypc)
		return;

	switch (fypc->type) {
	case FYPCT_NONE:
		/* nothing */
		break;

	case FYPCT_MAP:
		if (fypc->map.has_key) {
			if (fypc->map.is_complex_key) {
				if (fypc->map.complex_key_complete)
					fy_document_destroy(fypc->map.complex_key);
				fypc->map.complex_key = NULL;
			} else {
				fy_token_unref(fypc->map.scalar.tag);
				fy_token_unref(fypc->map.scalar.key);
				fypc->map.scalar.tag = NULL;
				fypc->map.scalar.key = NULL;
			}
		}
		fypc->map.root = true;
		fypc->map.has_key = false;
		fypc->map.await_key = true;
		fypc->map.is_complex_key = false;
		fypc->map.accumulating_complex_key = false;
		fypc->map.complex_key_complete = false;
		break;

	case FYPCT_SEQ:
		fypc->seq.idx = -1;
		break;
	}
}

void fy_path_component_cleanup(struct fy_path_component *fypc)
{
	if (!fypc)
		return;

	fy_path_component_clear_state(fypc);
	fypc->type = FYPCT_NONE;
}

void fy_path_component_free(struct fy_path_component *fypc)
{
	if (!fypc)
		return;

	fy_path_component_cleanup(fypc);
	free(fypc);
}

void fy_path_component_destroy(struct fy_path_component *fypc)
{
	if (!fypc)
		return;

	fy_path_component_cleanup(fypc);
	fy_path_component_free(fypc);
}

void fy_path_component_recycle(struct fy_path *fypp, struct fy_path_component *fypc)
{
	if (!fypc)
		return;

	fy_path_component_cleanup(fypc);

	if (!fypp)
		fy_path_component_free(fypc);
	else
		fy_path_component_list_push(&fypp->recycled_component, fypc);
}

struct fy_path_component *fy_path_component_create_mapping(struct fy_path *fypp)
{
	struct fy_path_component *fypc;

	if (!fypp)
		return NULL;

	fypc = fy_path_component_alloc(fypp);
	if (!fypc)
		return NULL;

	fypc->type = FYPCT_MAP;

	fypc->map.root = true;
	fypc->map.await_key = true;
	fypc->map.is_complex_key = false;
	fypc->map.accumulating_complex_key = false;
	fypc->map.complex_key_complete = false;

	return fypc;
}

struct fy_path_component *fy_path_component_create_sequence(struct fy_path *fypp)
{
	struct fy_path_component *fypc;

	if (!fypp)
		return NULL;

	fypc = fy_path_component_alloc(fypp);
	if (!fypc)
		return NULL;

	fypc->type = FYPCT_SEQ;

	fypc->seq.idx = -1;

	return fypc;
}

bool fy_path_component_is_mapping(struct fy_path_component *fypc)
{
	return fypc && fypc->type == FYPCT_MAP;
}

int fy_path_component_sequence_get_index(struct fy_path_component *fypc)
{
	return fypc && fypc->type == FYPCT_SEQ ? fypc->seq.idx : -1;
}

struct fy_token *fy_path_component_mapping_get_scalar_key(struct fy_path_component *fypc)
{
	return fypc && fypc->type == FYPCT_MAP &&
	       fypc->map.has_key && !fypc->map.is_complex_key ? fypc->map.scalar.key : NULL;
}

struct fy_token *fy_path_component_mapping_get_scalar_key_tag(struct fy_path_component *fypc)
{
	return fypc && fypc->type == FYPCT_MAP &&
	       fypc->map.has_key && !fypc->map.is_complex_key ? fypc->map.scalar.tag : NULL;
}

struct fy_document *fy_path_component_mapping_get_complex_key(struct fy_path_component *fypc)
{
	return fypc && fypc->type == FYPCT_MAP &&
	       fypc->map.has_key && fypc->map.is_complex_key ? fypc->map.complex_key : NULL;
}

bool fy_path_component_is_sequence(struct fy_path_component *fypc)
{
	return fypc && fypc->type == FYPCT_SEQ;
}

static int fy_path_component_get_text_internal(struct fy_emit_accum *ea, struct fy_path_component *fypc)
{
	char *doctxt;
	const char *text;
	size_t len;

	switch (fypc->type) {
	case FYPCT_NONE:
		abort();

	case FYPCT_MAP:

		/* we don't handle transitionals */
		if (!fypc->map.has_key || fypc->map.await_key || fypc->map.root)
			return -1;

		if (!fypc->map.is_complex_key && fypc->map.scalar.key) {
			text = fy_token_get_text(fypc->map.scalar.key, &len);
			if (!text)
				return -1;

			if (fypc->map.scalar.key->type == FYTT_ALIAS)
				fy_emit_accum_utf8_put_raw(ea, '*');
			fy_emit_accum_utf8_write_raw(ea, text, len);

		} else if (fypc->map.complex_key) {
			/* complex key */
			doctxt = fy_emit_document_to_string(fypc->map.complex_key,
				FYECF_WIDTH_INF | FYECF_INDENT_DEFAULT |
				FYECF_MODE_FLOW_ONELINE | FYECF_NO_ENDING_NEWLINE);
			fy_emit_accum_utf8_write_raw(ea, doctxt, strlen(doctxt));
			free(doctxt);
		}
		break;

	case FYPCT_SEQ:

		/* not started filling yet */
		if (fypc->seq.idx < 0)
			return -1;

		fy_emit_accum_utf8_printf_raw(ea, "%d", fypc->seq.idx);
		break;
	}

	return 0;
}

static int fy_path_get_text_internal(struct fy_emit_accum *ea, struct fy_path *fypp)
{
	struct fy_path_component *fypc;
	struct fy_document *fyd;
	char *doctxt;
	const char *text;
	size_t len;
	bool local_key = false;
	int rc, count;

	if (fypp->parent) {
		rc = fy_path_get_text_internal(ea, fypp->parent);
		assert(!rc);
		if (rc)
			return -1;
	}

	/* OK, we have to iterate and rebuild the paths */
	for (fypc = fy_path_component_list_head(&fypp->components), count = 0; fypc;
			fypc = fy_path_component_next(&fypp->components, fypc), count++) {

		fy_emit_accum_utf8_put_raw(ea, '/');

		switch (fypc->type) {
		case FYPCT_NONE:
			abort();

		case FYPCT_MAP:

			if (!fypc->map.has_key || fypc->map.root)
				break;

			/* key reference ? wrap in .key(X)*/
			local_key = false;
			if (fypc->map.await_key)
				local_key = true;

			if (local_key)
				fy_emit_accum_utf8_write_raw(ea, ".key(", 5);

			if (!fypc->map.is_complex_key) {

				if (fypc->map.scalar.key) {
					text = fy_token_get_text(fypc->map.scalar.key, &len);
					assert(text);
					if (!text)
						return -1;
					if (fypc->map.scalar.key->type == FYTT_ALIAS)
						fy_emit_accum_utf8_put_raw(ea, '*');
					fy_emit_accum_utf8_write_raw(ea, text, len);
				} else {
					fy_emit_accum_utf8_write_raw(ea, ".null()", 7);
				}
			} else {
				if (fypc->map.complex_key)
					fyd = fypc->map.complex_key;
				else
					fyd = fy_document_builder_peek_document(fypp->fydb);

				/* complex key */
				if (fyd) {
					doctxt = fy_emit_document_to_string(fyd,
						FYECF_WIDTH_INF | FYECF_INDENT_DEFAULT |
						FYECF_MODE_FLOW_ONELINE | FYECF_NO_ENDING_NEWLINE);
				} else
					doctxt = NULL;

				if (doctxt) {
					fy_emit_accum_utf8_write_raw(ea, doctxt, strlen(doctxt));
					free(doctxt);
				} else {
					fy_emit_accum_utf8_write_raw(ea, "<X>", 3);
				}
			}

			if (local_key)
				fy_emit_accum_utf8_put_raw(ea, ')');

			break;

		case FYPCT_SEQ:

			/* not started filling yet */
			if (fypc->seq.idx < 0)
				break;

			fy_emit_accum_utf8_printf_raw(ea, "%d", fypc->seq.idx);
			break;
		}
	}

	return 0;
}

char *fy_path_get_text(struct fy_path *fypp)
{
	struct fy_emit_accum ea;	/* use an emit accumulator */
	char *path = NULL;
	size_t len;
	int rc;

	/* no inplace buffer; we will need the malloc'ed contents anyway */
	fy_emit_accum_init(&ea, NULL, 0, 0, fylb_cr_nl);

	fy_emit_accum_start(&ea, 0, fylb_cr_nl);

	rc = fy_path_get_text_internal(&ea, fypp);
	if (rc)
		goto err_out;

	if (fy_emit_accum_empty(&ea))
		fy_emit_accum_utf8_printf_raw(&ea, "/");

	fy_emit_accum_make_0_terminated(&ea);

	path = fy_emit_accum_steal(&ea, &len);

err_out:
	fy_emit_accum_cleanup(&ea);

	return path;
}

char *fy_path_component_get_text(struct fy_path_component *fypc)
{
	struct fy_emit_accum ea;	/* use an emit accumulator */
	char *text = NULL;
	size_t len;
	int rc;

	/* no inplace buffer; we will need the malloc'ed contents anyway */
	fy_emit_accum_init(&ea, NULL, 0, 0, fylb_cr_nl);

	fy_emit_accum_start(&ea, 0, fylb_cr_nl);

	rc = fy_path_component_get_text_internal(&ea, fypc);
	if (rc)
		goto err_out;

	fy_emit_accum_make_0_terminated(&ea);

	text = fy_emit_accum_steal(&ea, &len);

err_out:
	fy_emit_accum_cleanup(&ea);

	return text;
}

int fy_path_depth(struct fy_path *fypp)
{
	struct fy_path_component *fypc;
	int depth;

	if (!fypp)
		return 0;

	depth = fy_path_depth(fypp->parent);
	for (fypc = fy_path_component_list_head(&fypp->components); fypc;
			fypc = fy_path_component_next(&fypp->components, fypc)) {

		depth++;
	}

	return depth;
}

struct fy_path *fy_path_parent(struct fy_path *fypp)
{
	if (!fypp)
		return NULL;
	return fypp->parent;
}

struct fy_path_component *
fy_path_last_component(struct fy_path *fypp)
{
	return fy_path_component_list_tail(&fypp->components);
}

struct fy_path_component *
fy_path_last_not_collection_root_component(struct fy_path *fypp)
{
	struct fy_path_component *fypc_last;

	fypc_last = fy_path_component_list_tail(&fypp->components);
	if (!fypc_last)
		return NULL;

	if (!fy_path_component_is_collection_root(fypc_last))
		return fypc_last;

	fypc_last = fy_path_component_prev(&fypp->components, fypc_last);
	if (fypc_last)
		return fypc_last;

	if (fypp->parent)
		return fy_path_component_list_tail(&fypp->parent->components);

	return NULL;
}

bool fy_path_in_root(struct fy_path *fypp)
{
	struct fy_path_component *fypc_last;

	if (!fypp)
		return true;

	fypc_last = fy_path_last_not_collection_root_component(fypp);
	return fypc_last == NULL;
}


bool fy_path_in_mapping(struct fy_path *fypp)
{
	struct fy_path_component *fypc_last;

	if (!fypp)
		return false;

	fypc_last = fy_path_last_not_collection_root_component(fypp);
	if (!fypc_last)
		return false;

	return fypc_last->type == FYPCT_MAP;
}

bool fy_path_in_sequence(struct fy_path *fypp)
{
	struct fy_path_component *fypc_last;

	if (!fypp)
		return false;

	fypc_last = fy_path_last_not_collection_root_component(fypp);
	if (!fypc_last)
		return false;

	return fypc_last->type == FYPCT_SEQ;
}

bool fy_path_in_mapping_key(struct fy_path *fypp)
{
	struct fy_path_component *fypc_last;

	if (!fypp)
		return false;

	fypc_last = fy_path_last_not_collection_root_component(fypp);
	if (!fypc_last)
		return false;

	return fypc_last->type == FYPCT_MAP && fypc_last->map.await_key;
}

bool fy_path_in_mapping_value(struct fy_path *fypp)
{
	struct fy_path_component *fypc_last;

	if (!fypp)
		return false;

	fypc_last = fy_path_last_not_collection_root_component(fypp);
	if (!fypc_last)
		return false;

	return fypc_last->type == FYPCT_MAP && !fypc_last->map.await_key;
}

bool fy_path_in_collection_root(struct fy_path *fypp)
{
	struct fy_path_component *fypc_last;

	if (!fypp)
		return false;

	fypc_last = fy_path_component_list_tail(&fypp->components);
	if (!fypc_last)
		return false;

	return fy_path_component_is_collection_root(fypc_last);
}

void *fy_path_get_root_user_data(struct fy_path *fypp)
{
	if (!fypp)
		return NULL;

	if (!fypp->parent)
		return fypp->user_data;

	return fy_path_get_root_user_data(fypp->parent);
}

void fy_path_set_root_user_data(struct fy_path *fypp, void *data)
{
	if (!fypp)
		return;

	if (!fypp->parent) {
		fypp->user_data = data;
		return;
	}

	fy_path_set_root_user_data(fypp->parent, data);
}

void *fy_path_component_get_mapping_user_data(struct fy_path_component *fypc)
{
	return fypc && fypc->type == FYPCT_MAP ? fypc->user_data : NULL;
}

void *fy_path_component_get_mapping_key_user_data(struct fy_path_component *fypc)
{
	return fypc && fypc->type == FYPCT_MAP ? fypc->map.key_user_data : NULL;
}

void *fy_path_component_get_sequence_user_data(struct fy_path_component *fypc)
{
	return fypc && fypc->type == FYPCT_SEQ ? fypc->user_data : NULL;
}

void fy_path_component_set_mapping_user_data(struct fy_path_component *fypc, void *data)
{
	if (!fypc || fypc->type != FYPCT_MAP)
		return;

	fypc->user_data = data;
}

void fy_path_component_set_mapping_key_user_data(struct fy_path_component *fypc, void *data)
{
	if (!fypc || fypc->type != FYPCT_MAP)
		return;

	fypc->map.key_user_data = data;
}

void fy_path_component_set_sequence_user_data(struct fy_path_component *fypc, void *data)
{
	if (!fypc || fypc->type != FYPCT_SEQ)
		return;

	fypc->user_data = data;
}
