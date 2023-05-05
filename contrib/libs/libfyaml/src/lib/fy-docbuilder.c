/*
 * fy-docbuilder.c - YAML document builder methods
 *
 * Copyright (c) 2022 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
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
#include <ctype.h>
#include <errno.h>
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#endif

#include <libfyaml.h>

#include "fy-utils.h"

#include "fy-docbuilder.h"

#include "fy-parse.h"
#include "fy-doc.h"

const char *fy_document_builder_state_txt[] = {
	[FYDBS_NODE]	= "node",
	[FYDBS_MAP_KEY]	= "map-key",
	[FYDBS_MAP_VAL]	= "map-val",
	[FYDBS_SEQ]	= "seq",
};

void
fy_document_builder_reset(struct fy_document_builder *fydb)
{
	struct fy_document_builder_ctx *c;
	unsigned int i;

	if (!fydb)
		return;

	for (i = 0, c = fydb->stack; i < fydb->next; i++, c++) {
		fy_node_free(c->fyn);
		c->fyn = NULL;
		fy_node_pair_free(c->fynp);
		c->fynp = NULL;
	}
	fydb->next = 0;

	if (fydb->fyd) {
		fy_document_destroy(fydb->fyd);
		fydb->fyd = NULL;
	}
	fydb->in_stream = false;
	fydb->doc_done = false;
}

static const struct fy_document_builder_cfg docbuilder_default_cfg = {
	.parse_cfg = {
		.flags = FYPCF_DEFAULT_DOC,
	}
};

struct fy_document_builder *
fy_document_builder_create(const struct fy_document_builder_cfg *cfg)
{
	struct fy_document_builder *fydb = NULL;

	if (!cfg)
		cfg = &docbuilder_default_cfg;

	fydb = malloc(sizeof(*fydb));
	if (!fydb)
		goto err_out;

	memset(fydb, 0, sizeof(*fydb));
	fydb->cfg = *cfg;
	fydb->next = 0;
	fydb->in_stream = false;
	fydb->doc_done = false;
	fydb->alloc = fy_depth_limit();	/* always start with this */
	fydb->max_depth = (cfg->parse_cfg.flags & FYPCF_DISABLE_DEPTH_LIMIT) ? 0 : fy_depth_limit();

	fydb->stack = malloc(fydb->alloc * sizeof(*fydb->stack));
	if (!fydb->stack)
		goto err_out;

	return fydb;

err_out:
	if (fydb) {
		if (fydb->stack)
			free(fydb->stack);
		free(fydb);
	}

	return NULL;
}

void
fy_document_builder_destroy(struct fy_document_builder *fydb)
{
	if (!fydb)
		return;

	fy_document_builder_reset(fydb);

	fy_diag_unref(fydb->cfg.diag);
	if (fydb->stack)
		free(fydb->stack);
	free(fydb);
}

struct fy_document *
fy_document_builder_get_document(struct fy_document_builder *fydb)
{
	return fydb ? fydb->fyd : NULL;
}

bool
fy_document_builder_is_in_stream(struct fy_document_builder *fydb)
{
	return fydb && fydb->in_stream;
}

bool
fy_document_builder_is_in_document(struct fy_document_builder *fydb)
{
	return fydb && fydb->fyd != NULL && !fydb->doc_done;
}

bool
fy_document_builder_is_document_complete(struct fy_document_builder *fydb)
{
	return fydb && fydb->fyd != NULL && fydb->doc_done;
}

struct fy_document *
fy_document_builder_take_document(struct fy_document_builder *fydb)
{
	struct fy_document *fyd;

	if (!fy_document_builder_is_document_complete(fydb))
		return NULL;
	fyd = fydb->fyd;
	fydb->fyd = NULL;
	fydb->doc_done = false;
	return fyd;
}

struct fy_document *
fy_document_builder_peek_document(struct fy_document_builder *fydb)
{
	struct fy_document *fyd;
	struct fy_document_builder_ctx *c;

	if (!fydb)
		return NULL;

	/* just peek; may be incomplete */
	fyd = fydb->fyd;

	assert(fydb->next > 0);
	c = &fydb->stack[0];

	/* wire the root */
	if (!fyd->root)
		fyd->root = c->fyn;

	return fyd;
}


void
fy_document_builder_set_in_stream(struct fy_document_builder *fydb)
{
	if (!fydb)
		return;
	/* reset */
	fy_document_builder_reset(fydb);

	fydb->in_stream = true;
}

int
fy_document_builder_set_in_document(struct fy_document_builder *fydb, struct fy_document_state *fyds, bool single)
{
	struct fy_document_builder_ctx *c;
	int rc;

	if (!fydb)
		return -1;

	/* reset */
	fy_document_builder_reset(fydb);

	fydb->in_stream = true;

	fydb->fyd = fy_document_create(&fydb->cfg.parse_cfg);
	if (!fydb->fyd)
		return -1;

	if (fyds) {
		rc = fy_document_set_document_state(fydb->fyd, fyds);
		if (rc)
			return rc;
	}

	fydb->doc_done = false;
	fydb->single_mode = single;

	/* be paranoid */
	assert(fydb->next < fydb->alloc);

	c = &fydb->stack[++fydb->next - 1];
	memset(c, 0, sizeof(*c));
	c->s = FYDBS_NODE;

	return 0;
}

int
fy_document_builder_process_event(struct fy_document_builder *fydb, struct fy_eventp *fyep)
{
	struct fy_event *fye;
	enum fy_event_type etype;
	struct fy_document *fyd;
	struct fy_document_builder_ctx *c, *cp;
	struct fy_node *fyn, *fyn_parent;
	struct fy_node_pair *fynp;
	struct fy_document_builder_ctx *newc;
	struct fy_token *fyt;
	int rc;

	fye = fyep ? &fyep->e : NULL;
	etype = fye ? fye->type : FYET_NONE;
	fyt = fye ? fy_event_get_token(fye) : NULL;

	/* not in document */
	if (!fydb->next) {
		switch (etype) {
		case FYET_STREAM_START:
			FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC,
					!fydb->in_stream, err_out,
					"STREAM_START while in stream error");
			fydb->in_stream = true;
			break;

		case FYET_STREAM_END:
			FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC,
					fydb->in_stream, err_out,
					"STREAM_END while not in stream error");
			fydb->in_stream = false;
			return 1;

		case FYET_DOCUMENT_START:
			FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC,
					fydb->in_stream, err_out,
					"DOCUMENT_START while not in stream error");

			/* no-one cares, destroy the document */
			if (!fydb->fyd)
				fy_document_destroy(fydb->fyd);

			fydb->fyd = fy_document_create(&fydb->cfg.parse_cfg);
			fydb_error_check(fydb, fydb->fyd, err_out,
					"fy_document_create() failed");

			rc = fy_document_set_document_state(fydb->fyd, fyep->e.document_start.document_state);
			fydb_error_check(fydb, !rc, err_out,
					"fy_document_set_document_state() failed");

			fydb->doc_done = false;
			goto push;

		case FYET_DOCUMENT_END:
			FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC,
					fydb->in_stream, err_out,
					"DOCUMENT_END while not in stream error");

			FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC,
					fydb->fyd, err_out,
					"DOCUMENT_END without a document");

			fydb->doc_done = true;
			break;

		default:
			/* unexpected event */
			FYDB_TOKEN_ERROR(fydb, fyt, FYEM_DOC,
					"Unexpected event %s in non-build mode\n",
						fy_event_type_txt[etype]);
			goto err_out;
		}

		return 0;
	}

	fyd = fydb->fyd;

	c = &fydb->stack[fydb->next - 1];
	fyn = NULL;

	/* verify that we have a document */
	assert(fydb->fyd);
	/* the top state must always be NODE for processing the event */
	assert(c->s == FYDBS_NODE);

	switch (etype) {
	case FYET_SCALAR:
	case FYET_ALIAS:
		fyn = fy_node_alloc(fyd, FYNT_SCALAR);
		fydb_error_check(fydb, fyn, err_out,
				"fy_node_alloc() SCALAR failed");

		if (etype == FYET_SCALAR) {
			if (fye->scalar.value)
				fyn->style = fy_node_style_from_scalar_style(fye->scalar.value->scalar.style);
			else
				fyn->style = FYNS_PLAIN;
			fyn->tag = fy_token_ref(fye->scalar.tag);
			if (fye->scalar.anchor) {
				rc = fy_document_register_anchor(fyd, fyn, fy_token_ref(fye->scalar.anchor));
				fydb_error_check(fydb, !rc, err_out,
						"fy_document_register_anchor() failed");
			}
			fyn->scalar = fy_token_ref(fye->scalar.value);
		} else {
			fyn->style = FYNS_ALIAS;
			fyn->scalar = fy_token_ref(fye->alias.anchor);
		}
		goto complete;

	case FYET_MAPPING_START:
		c->s = FYDBS_MAP_KEY;

		fyn = fy_node_alloc(fyd, FYNT_MAPPING);
		fydb_error_check(fydb, fyn, err_out,
				"fy_node_alloc() MAPPING failed");

		c->fyn = fyn;
		fyn->style = fye->mapping_start.mapping_start->type == FYTT_FLOW_MAPPING_START ? FYNS_FLOW : FYNS_BLOCK;
		fyn->tag = fy_token_ref(fye->mapping_start.tag);
		if (fye->mapping_start.anchor) {
			rc = fy_document_register_anchor(fyd, fyn, fy_token_ref(fye->mapping_start.anchor));
			fydb_error_check(fydb, !rc, err_out,
					"fy_document_register_anchor() failed");
		}
		fyn->mapping_start = fy_token_ref(fye->mapping_start.mapping_start);
		break;

	case FYET_MAPPING_END:

		FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC, fydb->next > 1, err_out,
				"Unexpected MAPPING_END (unexpected end of mapping)");

		cp = &fydb->stack[fydb->next - 2];

		FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC, cp->s == FYDBS_MAP_KEY, err_out,
				"Unexpected MAPPING_END (not in mapping)");

		fyn = cp->fyn;
		fyn->mapping_end = fy_token_ref(fye->mapping_end.mapping_end);
		fydb->next--;
		goto complete;

	case FYET_SEQUENCE_START:
		c->s = FYDBS_SEQ;
		fyn = fy_node_alloc(fyd, FYNT_SEQUENCE);
		fydb_error_check(fydb, fyn, err_out,
				"fy_node_alloc() SEQUENCE failed");

		c->fyn = fyn;
		fyn->style = fye->sequence_start.sequence_start->type == FYTT_FLOW_SEQUENCE_START ? FYNS_FLOW : FYNS_BLOCK;
		fyn->tag = fy_token_ref(fye->sequence_start.tag);
		if (fye->sequence_start.anchor) {
			rc = fy_document_register_anchor(fyd, fyn, fy_token_ref(fye->sequence_start.anchor));
			fydb_error_check(fydb, !rc, err_out,
					"fy_document_register_anchor() failed");
		}
		fyn->sequence_start = fy_token_ref(fye->sequence_start.sequence_start);
		break;

	case FYET_SEQUENCE_END:

		FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC, fydb->next > 1, err_out,
				"Unexpected SEQUENCE_END (unexpected end of sequence)");

		cp = &fydb->stack[fydb->next - 2];

		FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC, cp->s == FYDBS_SEQ, err_out,
				"Unexpected MAPPING_SEQUENCE (not in sequence)");

		fyn = cp->fyn;
		fyn->sequence_end = fy_token_ref(fye->sequence_end.sequence_end);
		fydb->next--;
		goto complete;

	default:
		/* unexpected event */
		FYDB_TOKEN_ERROR(fydb, fyt, FYEM_DOC,
				"Unexpected event %s in build mode\n",
					fy_event_type_txt[etype]);
		goto err_out;
	}

push:
	FYDB_TOKEN_ERROR_CHECK(fydb, fyt, FYEM_DOC,
			!fydb->max_depth || fydb->next < fydb->max_depth, err_out,
			"Max depth (%d) exceeded\n", fydb->next);

	/* grow the stack? */
	if (fydb->next >= fydb->alloc) {
		newc = realloc(fydb->stack, fydb->alloc * 2 * sizeof(*fydb->stack));
		fydb_error_check(fydb, newc, err_out,
				"Unable to grow the context stack");
		fydb->alloc *= 2;
		fydb->stack = newc;
	}
	assert(fydb->next < fydb->alloc);

	c = &fydb->stack[++fydb->next - 1];
	memset(c, 0, sizeof(*c));
	c->s = FYDBS_NODE;
	return 0;

err_out:
	return -1;

complete:
	assert(fydb->next > 0);
	c = &fydb->stack[fydb->next - 1];
	c->fyn = fyn;
	assert(fydb->next > 0);
	fydb->next--;

	/* root */
	if (fydb->next == 0) {
		fyd->root = fyn;
		/* if we're in single mode, don't wait for doc end */
		if (fydb->single_mode)
			fydb->doc_done = true;
		return 1;
	}

	c = &fydb->stack[fydb->next - 1];

	fyn_parent = c->fyn;

	switch (c->s) {

	case FYDBS_MAP_KEY:
		fynp = fy_node_pair_alloc(fyd);
		assert(fynp);
		fynp->key = fyn;
		c->fynp = fynp;

		/* if we don't allow duplicate keys */
		if (!(fyd->parse_cfg.flags & FYPCF_ALLOW_DUPLICATE_KEYS)) {

			/* make sure we don't add an already existing key */
			if (fy_node_mapping_key_is_duplicate(fyn_parent, fyn)) {
				FYDB_NODE_ERROR(fydb, fyn, FYEM_DOC, "duplicate key");
				goto err_out;
			}
		}

		c->s = FYDBS_MAP_VAL;
		goto push;

	case FYDBS_MAP_VAL:
		fynp = c->fynp;
		assert(fynp);
		fynp->value = fyn;

		/* set the parent of the node pair and value */
		fynp->parent = fyn_parent;
		if (fynp->key) {
			fynp->key->parent = fyn_parent;
			fynp->key->key_root = true;
		}
		if (fynp->value)
			fynp->value->parent = fyn_parent;

		fy_node_pair_list_add_tail(&c->fyn->mapping, fynp);
		if (fyn->xl) {
			rc = fy_accel_insert(fyn->xl, fynp->key, fynp);
			assert(!rc);
		}
		if (fynp->key)
			fynp->key->attached = true;
		if (fynp->value)
			fynp->value->attached = true;

		c->fynp = NULL;
		c->s = FYDBS_MAP_KEY;
		goto push;

	case FYDBS_SEQ:
		/* append sequence */
		fyn->parent = fyn_parent;
		fy_node_list_add_tail(&c->fyn->sequence, fyn);
		fyn->attached = true;
		goto push;

	case FYDBS_NODE:
		/* complete is a scalar */
		fyn->parent = fyn_parent;
		return 0;
	}

	return 0;
}

struct fy_document *
fy_document_builder_load_document(struct fy_document_builder *fydb,
				  struct fy_parser *fyp)
{
	struct fy_eventp *fyep = NULL;
	int rc;

	if (fyp->state == FYPS_END)
		return NULL;

	while (!fy_document_builder_is_document_complete(fydb) &&
		(fyep = fy_parse_private(fyp)) != NULL) {
		rc = fy_document_builder_process_event(fydb, fyep);
		fy_parse_eventp_recycle(fyp, fyep);
		if (rc < 0) {
			fyp->stream_error = true;
			return NULL;
		}
	}

	/* get ownership of the document */
	return fy_document_builder_take_document(fydb);
}
