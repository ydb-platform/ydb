/*
 * fy-doc.c - YAML document methods
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
#include <ctype.h>
#include <errno.h>
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#endif

#include <libfyaml.h>

#include "fy-parse.h"
#include "fy-doc.h"

#include "fy-utils.h"

#define XXH_STATIC_LINKING_ONLY
#include "xxhash.h"

static const struct fy_hash_desc hd_anchor;
static const struct fy_hash_desc hd_nanchor;
static const struct fy_hash_desc hd_mapping;

int fy_node_hash_uint(struct fy_node *fyn, unsigned int *hashp);

static struct fy_node *
fy_node_by_path_internal(struct fy_node *fyn,
			 const char *path, size_t pathlen,
		         enum fy_node_walk_flags flags);

#define FY_NODE_PATH_WALK_DEPTH_DEFAULT	16

static inline unsigned int
fy_node_walk_max_depth_from_flags(enum fy_node_walk_flags flags)
{
	unsigned int max_depth;

	max_depth = ((unsigned int)flags >> FYNWF_MAXDEPTH_SHIFT) & FYNWF_MAXDEPTH_MASK;
	if (max_depth == 0)
		max_depth = FY_NODE_PATH_WALK_DEPTH_DEFAULT;

	return max_depth;
}

static inline unsigned int
fy_node_walk_marker_from_flags(enum fy_node_walk_flags flags)
{
	return ((unsigned int)flags >> FYNWF_MARKER_SHIFT) & FYNWF_MARKER_MASK;
}

/* internal simple key to optimize string lookups */
static inline bool is_simple_key(const char *str, size_t len)
{
	const char *s, *e;
	char c;

	if (!str)
		return false;

	if (len == (size_t)-1)
		len = strlen(str);

	for (s = str, e = s + len; s < e; s++) {

		c = *s;

		/* note no isalpha() it's locale specific */
		if (!((c >= 'A' && c <= 'Z') ||
		      (c >= 'a' && c <= 'z') ||
		      (c >= '0' && c <= '9') ||
		      (c == '_')))
			break;
	}

	return s == e;
}

static void fy_resolve_parent_node(struct fy_document *fyd, struct fy_node *fyn, struct fy_node *fyn_parent);

void fy_anchor_destroy(struct fy_anchor *fya)
{
	if (!fya)
		return;
	fy_token_unref(fya->anchor);
	free(fya);
}

struct fy_anchor *fy_anchor_create(struct fy_document *fyd,
		struct fy_node *fyn, struct fy_token *anchor)
{
	struct fy_anchor *fya = NULL;

	fya = malloc(sizeof(*fya));
	if (!fya)
		return NULL;

	fya->fyn = fyn;
	fya->anchor = anchor;
	fya->multiple = false;

	return fya;
}

struct fy_anchor *fy_document_anchor_iterate(struct fy_document *fyd, void **prevp)
{
	struct fy_anchor_list *fyal;

	if (!fyd || !prevp)
		return NULL;

	fyal = &fyd->anchors;

	return *prevp = *prevp ? fy_anchor_next(fyal, *prevp) : fy_anchor_list_head(fyal);
}

#define FYDSAF_COPY		FY_BIT(0)
#define FYDSAF_MALLOCED		FY_BIT(1)

static int fy_document_set_anchor_internal(struct fy_document *fyd, struct fy_node *fyn, const char *text, size_t len,
					   unsigned int flags)
{
	const bool copy = !!(flags & FYDSAF_COPY);
	const bool malloced = !!(flags & FYDSAF_MALLOCED);
	struct fy_anchor *fya = NULL, *fyam = NULL;
	struct fy_input *fyi = NULL;
	struct fy_token *fyt = NULL;
	struct fy_accel_entry *xle;
	struct fy_atom handle;
	char *data_copy = NULL;
	const char *origtext;
	size_t origlen;
	int rc;

	if (!fyd || !fyn || fyn->fyd != fyd)
		return -1;

	if (text && len == (size_t)-1)
		len = strlen(text);

	fya = fy_document_lookup_anchor_by_node(fyd, fyn);

	if (!text) {
		/* no anchor, and trying to delete? OK */
		if (fya)
			return 0;
		/* remove the anchor */
		fy_anchor_list_del(&fyd->anchors, fya);

		if (fy_document_is_accelerated(fyd)) {
			xle = fy_accel_entry_lookup_key_value(fyd->axl, fya->anchor, fya);
			fy_accel_entry_remove(fyd->axl, xle);

			xle = fy_accel_entry_lookup_key_value(fyd->naxl, fya->fyn, fya);
			fy_accel_entry_remove(fyd->naxl, xle);
		}

		fy_anchor_destroy(fya);
		return 0;
	}

	/* trying to add duplicate anchor */
	if (fya) {
		origtext = fy_token_get_text(fya->anchor, &origlen);
		fyd_error_check(fyd, origtext, err_out,
				"fy_token_get_text() failed");

		FYD_NODE_ERROR(fyd, fyn, FYEM_DOC,
				"cannot set anchor %.*s (anchor %.*s already exists)",
				(int)len, text, (int)origlen, origtext);
		if (malloced && text)
			free((void *)text);
		fya = NULL;
		goto err_out;
	}

	if (copy) {
		data_copy = malloc(len);
		fyd_error_check(fyd, data_copy, err_out,
				"malloc() failed");
		memcpy(data_copy, text, len);
		fyi = fy_input_from_malloc_data(data_copy, len, &handle, true);
	} else if (malloced)
		data_copy = (char *)text;
	else
		data_copy = NULL;

	if (data_copy)
		fyi = fy_input_from_malloc_data((void *)text, len, &handle, true);
	else
		fyi = fy_input_from_data(text, len, &handle, true);
	fyd_error_check(fyd, fyi, err_out,
			"fy_input_from_data() failed");
	data_copy = NULL;

	/* it must not be something funky */
	if (!handle.valid_anchor)
		goto err_out;

	fyt = fy_token_create(FYTT_ANCHOR, &handle);
	if (!fyt)
		goto err_out;

	fya = fy_anchor_create(fyd, fyn, fyt);
	if (!fya)
		goto err_out;

	fy_anchor_list_add(&fyd->anchors, fya);
	if (fy_document_is_accelerated(fyd)) {
		xle = fy_accel_entry_lookup(fyd->axl, fya->anchor);
		if (xle) {
			fyam = (void *)xle->value;
			/* multiple */
			if (!fyam->multiple)
				fyam->multiple = true;
			fya->multiple = true;

			fyd_notice(fyd, "register anchor %.*s is multiple", (int)len, text);
		}

		xle = fy_accel_entry_insert(fyd->axl, fya->anchor, fya);
		fyd_error_check(fyd, xle, err_out,
				"fy_accel_entry_insert() fyd->axl failed");
	}

	if (fy_document_is_accelerated(fyd)) {
		rc = fy_accel_insert(fyd->naxl, fyn, fya);
		fyd_error_check(fyd, !rc, err_out_rc,
				"fy_accel_insert() fyd->naxl failed");
	}

	/* take away the input reference */
	fy_input_unref(fyi);

	return 0;
err_out:
	rc = -1;
err_out_rc:
	if (data_copy)
		free(data_copy);
	fy_anchor_destroy(fya);
	fy_token_unref(fyt);
	fy_input_unref(fyi);
	fyd->diag->on_error = false;
	return rc;
}

int fy_document_set_anchor(struct fy_document *fyd, struct fy_node *fyn, const char *text, size_t len)
{
	return fy_document_set_anchor_internal(fyd, fyn, text, len, 0);
}

int fy_node_set_anchor(struct fy_node *fyn, const char *text, size_t len)
{
	if (!fyn)
		return -1;
	return fy_document_set_anchor_internal(fyn->fyd, fyn, text, len, 0);
}

int fy_node_set_anchor_copy(struct fy_node *fyn, const char *text, size_t len)
{
	if (!fyn)
		return -1;
	return fy_document_set_anchor_internal(fyn->fyd, fyn, text, len, FYDSAF_COPY);
}

int fy_node_set_vanchorf(struct fy_node *fyn, const char *fmt, va_list ap)
{
    char *str;

	if (!fyn || !fmt)
		return -1;

    alloca_vsprintf(&str, fmt, ap);
	return fy_document_set_anchor_internal(fyn->fyd, fyn, str, FY_NT, FYDSAF_COPY);
}

int fy_node_set_anchorf(struct fy_node *fyn, const char *fmt, ...)
{
	va_list ap;
	int ret;

	va_start(ap, fmt);
	ret = fy_node_set_vanchorf(fyn, fmt, ap);
	va_end(ap);

	return ret;
}

int fy_node_remove_anchor(struct fy_node *fyn)
{
	return fy_node_set_anchor(fyn, NULL, 0);
}

struct fy_anchor *fy_node_get_anchor(struct fy_node *fyn)
{
	if (!fyn)
		return NULL;

	return fy_document_lookup_anchor_by_node(fyn->fyd, fyn);
}

struct fy_anchor *fy_node_get_nearest_anchor(struct fy_node *fyn)
{
	struct fy_anchor *fya;
	struct fy_node *fynt;

	while ((fya = fy_node_get_anchor(fyn)) == NULL && (fynt = fy_node_get_parent(fyn)))
		fyn = fynt;

	return fya;
}

struct fy_node *fy_node_get_nearest_child_of(struct fy_node *fyn_base,
					     struct fy_node *fyn)
{
        struct fy_node *fynp;

        if (!fyn)
                return NULL;
        if (!fyn_base)
                fyn_base = fy_document_root(fy_node_document(fyn));
        if (!fyn_base)
                return NULL;

        /* move up until we hit a node that's a child of fyn_base */
        fynp = fyn;
        while (fyn && (fynp = fy_node_get_parent(fyn)) != NULL && fyn_base != fynp)
                fyn = fynp;

        return fyn;
}

void fy_parse_document_destroy(struct fy_parser *fyp, struct fy_document *fyd)
{
	struct fy_node *fyn;
	struct fy_anchor *fya;
	struct fy_anchor *fyan;
	struct fy_accel_entry *xle;

	if (!fyd)
		return;

	fy_document_cleanup_path_expr_data(fyd);

	fyn = fyd->root;
	fyd->root = NULL;
	fy_node_detach_and_free(fyn);

	/* remove all anchors */
	for (fya = fy_anchor_list_head(&fyd->anchors); fya; fya = fyan) {
		fyan = fy_anchor_next(&fyd->anchors, fya);
		fy_anchor_list_del(&fyd->anchors, fya);

		if (fy_document_is_accelerated(fyd)) {
			xle = fy_accel_entry_lookup_key_value(fyd->axl, fya->anchor, fya);
			fy_accel_entry_remove(fyd->axl, xle);

			xle = fy_accel_entry_lookup_key_value(fyd->naxl, fya->fyn, fya);
			fy_accel_entry_remove(fyd->naxl, xle);
		}

		fy_anchor_destroy(fya);
	}

	if (fy_document_is_accelerated(fyd)) {
		fy_accel_cleanup(fyd->axl);
		free(fyd->axl);

		fy_accel_cleanup(fyd->naxl);
		free(fyd->naxl);
	}

	fy_document_state_unref(fyd->fyds);

	fy_diag_unref(fyd->diag);

	if (fyd->on_destroy_fn)
		fyd->on_destroy_fn(fyd, fyd->userdata);

	free(fyd);
}

struct fy_document *fy_parse_document_create(struct fy_parser *fyp, struct fy_eventp *fyep)
{
	struct fy_document *fyd = NULL;
	struct fy_document_state *fyds;
	struct fy_event *fye = NULL;
	int rc;

	if (!fyp || !fyep)
		return NULL;

	fye = &fyep->e;

	FYP_TOKEN_ERROR_CHECK(fyp, fy_event_get_token(fye), FYEM_DOC,
			fye->type == FYET_DOCUMENT_START, err_out,
			"invalid start of event stream");

	fyd = malloc(sizeof(*fyd));
	fyp_error_check(fyp, fyd, err_out,
		"malloc() failed");

	memset(fyd, 0, sizeof(*fyd));

	fyd->diag = fy_diag_ref(fyp->diag);
	fyd->parse_cfg = fyp->cfg;

	fy_anchor_list_init(&fyd->anchors);
	if (fy_document_can_be_accelerated(fyd)) {
		fyd->axl = malloc(sizeof(*fyd->axl));
		fyp_error_check(fyp, fyd->axl, err_out,
				"malloc() failed");

		/* start with a very small bucket list */
		rc = fy_accel_setup(fyd->axl, &hd_anchor, fyd, 8);
		fyp_error_check(fyp, !rc, err_out,
				"fy_accel_setup() failed");

		fyd->naxl = malloc(sizeof(*fyd->naxl));
		fyp_error_check(fyp, fyd->axl, err_out,
				"malloc() failed");

		/* start with a very small bucket list */
		rc = fy_accel_setup(fyd->naxl, &hd_nanchor, fyd, 8);
		fyp_error_check(fyp, !rc, err_out,
				"fy_accel_setup() failed");
	}

	fyd->root = NULL;

	fyds = fye->document_start.document_state;
	fye->document_start.document_state = NULL;

	/* and we're done with this event */
	fy_parse_eventp_recycle(fyp, fyep);

	/* drop the old reference */
	fy_document_state_unref(fyd->fyds);

	/* note that we keep the reference */
	fyd->fyds = fyds;

	fy_document_list_init(&fyd->children);

	return fyd;

err_out:
	fy_parse_document_destroy(fyp, fyd);
	fy_parse_eventp_recycle(fyp, fyep);
	fyd->diag->on_error = false;
	return NULL;
}

const struct fy_parse_cfg *fy_document_get_cfg(struct fy_document *fyd)
{
	if (!fyd)
		return NULL;
	return &fyd->parse_cfg;
}

struct fy_diag *fy_document_get_diag(struct fy_document *fyd)
{
	if (!fyd || !fyd->diag)
		return NULL;
	return fy_diag_ref(fyd->diag);
}

int fy_document_set_diag(struct fy_document *fyd, struct fy_diag *diag)
{
	struct fy_diag_cfg dcfg;

	if (!fyd)
		return -1;

	/* default? */
	if (!diag) {
		fy_diag_cfg_default(&dcfg);
		diag = fy_diag_create(&dcfg);
		if (!diag)
			return -1;
	}

	fy_diag_unref(fyd->diag);
	fyd->diag = fy_diag_ref(diag);

	return 0;
}

struct fy_document *fy_node_document(struct fy_node *fyn)
{
	return fyn ? fyn->fyd : NULL;
}

static inline struct fy_anchor *
fy_document_accel_lookup_anchor_by_token(struct fy_document *fyd, struct fy_token *fyt)
{
	assert(fyd);
	assert(fyd->axl);
	return (void *)fy_accel_lookup(fyd->axl, fyt);
}

static inline struct fy_anchor *
fy_document_accel_lookup_anchor_by_node(struct fy_document *fyd, struct fy_node *fyn)
{
	assert(fyd);
	assert(fyd->naxl);
	return (void *)fy_accel_lookup(fyd->naxl, fyn);
}

static inline struct fy_node_pair *
fy_node_accel_lookup_by_node(struct fy_node *fyn, struct fy_node *fyn_key)
{
	assert(fyn);
	assert(fyn->xl);

	return (void *)fy_accel_lookup(fyn->xl, (const void *)fyn_key);
}

struct fy_anchor *
fy_document_lookup_anchor(struct fy_document *fyd, const char *anchor, size_t len)
{
	struct fy_anchor *fya;
	struct fy_anchor_list *fyal;
	struct fy_input *fyi;
	struct fy_atom handle;
	struct fy_token *fyt;
	const char *text;
	size_t text_len;

	if (!fyd || !anchor)
		return NULL;

	if (len == (size_t)-1)
		len = strlen(anchor);

	if (fy_document_is_accelerated(fyd)) {
		fyi = fy_input_from_data(anchor, len, &handle, true);
		if (!fyi)
			return NULL;

		fyt = fy_token_create(FYTT_ANCHOR, &handle);

		if (!fyt) {
			fy_input_unref(fyi);
			return NULL;
		}

		fya = fy_document_accel_lookup_anchor_by_token(fyd, fyt);

		fy_input_unref(fyi);
		fy_token_unref(fyt);

		if (!fya)
			return NULL;

		/* single anchor? return it */
		if (!fya->multiple)
			return fya;

		/* multiple anchors, fall-through */
	}

	/* note that we're performing the lookup in reverse creation order
	 * so that we pick the most recent
	 */
	fyal = &fyd->anchors;
	for (fya = fy_anchor_list_tail(fyal); fya; fya = fy_anchor_prev(fyal, fya)) {
		text = fy_anchor_get_text(fya, &text_len);
		if (!text)
			return NULL;

		if (len == text_len && !memcmp(anchor, text, len))
			return fya;
	}

	return NULL;
}

struct fy_anchor *
fy_document_lookup_anchor_by_token(struct fy_document *fyd,
				   struct fy_token *anchor)
{
	struct fy_anchor *fya, *fya_found, *fya_found2;
	struct fy_anchor_list *fyal;
	const char *anchor_text, *text;
	size_t anchor_len, text_len;
	int count;

	if (!fyd || !anchor)
		return NULL;

	/* first try direct match (it's faster and the common case) */
	if (fy_document_is_accelerated(fyd)) {
		fya = fy_document_accel_lookup_anchor_by_token(fyd, anchor);
		if (!fya)
			return NULL;

		/* single anchor? return it */
		if (!fya->multiple)
			return fya;

		/* multiple anchors, fall-through */
	}

	anchor_text = fy_token_get_text(anchor, &anchor_len);
	if (!anchor_text)
		return NULL;

	fyal = &fyd->anchors;

	/* first pass, try with a single match */
	count = 0;
	fya_found = NULL;
	for (fya = fy_anchor_list_head(fyal); fya; fya = fy_anchor_next(fyal, fya)) {
		text = fy_anchor_get_text(fya, &text_len);
		if (!text)
			return NULL;

		if (anchor_len == text_len && !memcmp(anchor_text, text, anchor_len)) {
			count++;
			fya_found = fya;
		}
	}

	/* not found */
	if (!count)
		return NULL;

	/* single one? fine */
	if (count == 1)
		return fya_found;

	/* multiple ones, must pick the one that's the last one before
	 * the requesting token */
	/* fyd_notice(fyd, "multiple anchors for %.*s", (int)anchor_len, anchor_text); */

	/* only try the ones on the same input
	 * we don't try to cover the case where the label is referenced
	 * by other constructed documents
	 */
	fya_found2 = NULL;
	for (fya = fy_anchor_list_head(fyal); fya; fya = fy_anchor_next(fyal, fya)) {

		/* only on the same input */
		if (fy_token_get_input(fya->anchor) != fy_token_get_input(anchor))
			continue;

		text = fy_anchor_get_text(fya, &text_len);
		if (!text)
			return NULL;

		if (anchor_len == text_len && !memcmp(anchor_text, text, anchor_len) &&
		    fy_token_start_pos(fya->anchor) < fy_token_start_pos(anchor)) {
			fya_found2 = fya;
		}
	}

	/* just return the one find earlier */
	if (!fya_found2)
		return fya_found;

	/* return the one that was the latest */
	return fya_found2;
}

struct fy_anchor *fy_document_lookup_anchor_by_node(struct fy_document *fyd, struct fy_node *fyn)
{
	struct fy_anchor *fya;
	struct fy_anchor_list *fyal;

	if (!fyd || !fyn)
		return NULL;

	if (fy_document_is_accelerated(fyd)) {
		fya = fy_document_accel_lookup_anchor_by_node(fyd, fyn);
	} else {
		fyal = &fyd->anchors;
		for (fya = fy_anchor_list_head(fyal); fya; fya = fy_anchor_next(fyal, fya)) {
			if (fya->fyn == fyn)
				break;
		}
	}

	return fya;
}

const char *fy_anchor_get_text(struct fy_anchor *fya, size_t *lenp)
{
	if (!fya || !lenp)
		return NULL;
	return fy_token_get_text(fya->anchor, lenp);
}

struct fy_node *fy_anchor_node(struct fy_anchor *fya)
{
	if (!fya)
		return NULL;
	return fya->fyn;
}

int fy_node_pair_free(struct fy_node_pair *fynp)
{
	int rc, rc_ret = 0;

	if (!fynp)
		return 0;

	rc = fy_node_free(fynp->key);
	if (rc)
		rc_ret = -1;
	rc = fy_node_free(fynp->value);
	if (rc)
		rc_ret = -1;

	free(fynp);

	return rc_ret;
}

void fy_node_pair_detach_and_free(struct fy_node_pair *fynp)
{
	if (!fynp)
		return;

	fy_node_detach_and_free(fynp->key);
	fy_node_detach_and_free(fynp->value);
	free(fynp);
}

struct fy_node_pair *fy_node_pair_alloc(struct fy_document *fyd)
{
	struct fy_node_pair *fynp = NULL;

	fynp = malloc(sizeof(*fynp));
	if (!fynp)
		return NULL;

	fynp->key = NULL;
	fynp->value = NULL;
	fynp->fyd = fyd;
	fynp->parent = NULL;
	return fynp;
}

int fy_node_free(struct fy_node *fyn)
{
	struct fy_document *fyd;
	struct fy_node *fyni;
	struct fy_node_pair *fynp;
	struct fy_anchor *fya, *fyan;
		struct fy_accel_entry_iter xli;
	struct fy_accel_entry *xle, *xlen;

	if (!fyn)
		return 0;

	/* a document must exist */
	fyd = fyn->fyd;
	if (!fyd)
		return -1;

	if (fyn->attached)
		return -1;

	if (fy_document_is_accelerated(fyd)) {
		for (xle = fy_accel_entry_iter_start(&xli, fyd->naxl, fyn);
		     xle; xle = xlen) {
			xlen = fy_accel_entry_iter_next(&xli);

			fya = (void *)xle->value;

			fy_anchor_list_del(&fyd->anchors, fya);

			xle = fy_accel_entry_lookup_key_value(fyd->axl, fya->anchor, fya);
			fy_accel_entry_remove(fyd->axl, xle);

			xle = fy_accel_entry_lookup_key_value(fyd->naxl, fya->fyn, fya);
			fy_accel_entry_remove(fyd->naxl, xle);

			fy_anchor_destroy(fya);
		}
		fy_accel_entry_iter_finish(&xli);
	} else {
		/* remove anchors that are located on this node */
		for (fya = fy_anchor_list_head(&fyd->anchors); fya; fya = fyan) {
			fyan = fy_anchor_next(&fyd->anchors, fya);
			if (fya->fyn == fyn) {
				fy_anchor_list_del(&fyd->anchors, fya);
				fy_anchor_destroy(fya);
			}
		}
	}

	/* clear the meta data of this node */
	fy_node_clear_meta(fyn);

	fy_token_unref(fyn->tag);
	fyn->tag = NULL;
	switch (fyn->type) {
	case FYNT_SCALAR:
		fy_token_unref(fyn->scalar);
		fyn->scalar = NULL;
		break;
	case FYNT_SEQUENCE:
		while ((fyni = fy_node_list_pop(&fyn->sequence)) != NULL)
			fy_node_detach_and_free(fyni);
		fy_token_unref(fyn->sequence_start);
		fy_token_unref(fyn->sequence_end);
		fyn->sequence_start = NULL;
		fyn->sequence_end = NULL;
		break;
	case FYNT_MAPPING:
		while ((fynp = fy_node_pair_list_pop(&fyn->mapping)) != NULL) {
			if (fyn->xl)
				fy_accel_remove(fyn->xl, fynp->key);
			fy_node_pair_detach_and_free(fynp);
		}
		fy_token_unref(fyn->mapping_start);
		fy_token_unref(fyn->mapping_end);
		fyn->mapping_start = NULL;
		fyn->mapping_end = NULL;
		break;
	}

	if (fyn->xl) {
		fy_accel_cleanup(fyn->xl);
		free(fyn->xl);
	}

	fy_node_cleanup_path_expr_data(fyn);

	free(fyn);

	return 0;
}

void fy_node_detach_and_free(struct fy_node *fyn)
{
	int rc __FY_DEBUG_UNUSED__;

	if (!fyn || !fyn->fyd)
		return;

	fyn->attached = false;

	/* it must always succeed */
	rc = fy_node_free(fyn);
	assert(!rc);
}

struct fy_node *fy_node_alloc(struct fy_document *fyd, enum fy_node_type type)
{
	struct fy_node *fyn = NULL;
	int rc;

	fyn = malloc(sizeof(*fyn));
	if (!fyn)
		return NULL;

	memset(fyn, 0, sizeof(*fyn));

	fyn->style = FYNS_ANY;
	fyn->fyd = fyd;
	fyn->type = type;

	switch (fyn->type) {
	case FYNT_SCALAR:
		break;

	case FYNT_SEQUENCE:
		fy_node_list_init(&fyn->sequence);
		break;
	case FYNT_MAPPING:
		fy_node_pair_list_init(&fyn->mapping);

		if (fy_document_is_accelerated(fyd)) {
			fyn->xl = malloc(sizeof(*fyn->xl));
			fyd_error_check(fyd, fyn->xl, err_out,
					"malloc() failed");

			/* start with a very small bucket list */
			rc = fy_accel_setup(fyn->xl, &hd_mapping, fyd, 8);
			fyd_error_check(fyd, !rc, err_out,
					"fy_accel_setup() failed");
		}
		break;
	}
	return fyn;

err_out:
	if (fyn) {
		if (fyn->xl) {
			fy_accel_cleanup(fyn->xl);
			free(fyn->xl);
		}
		free(fyn);
	}
	return NULL;
}

struct fy_token *fy_node_non_synthesized_token(struct fy_node *fyn)
{
	struct fy_token *fyt_start = NULL, *fyt_end = NULL;
	struct fy_token *fyt;
	struct fy_input *fyi;
	struct fy_atom handle;
	unsigned int aflags;
	const char *s, *e;
	size_t size;

	if (!fyn)
		return NULL;

	fyi = fy_node_get_input(fyn);
	if (!fyi)
		return NULL;

	switch (fyn->type) {
	case FYNT_SCALAR:
		return fy_token_ref(fyn->scalar);

	case FYNT_SEQUENCE:
		fyt_start = fyn->sequence_start;
		fyt_end = fyn->sequence_end;
		break;

	case FYNT_MAPPING:
		fyt_start = fyn->mapping_start;
		fyt_end = fyn->mapping_end;
		break;

	}

	if (!fyt_start || !fyt_end)
		return NULL;

	s = (char *)fy_input_start(fyi) + fyt_start->handle.start_mark.input_pos;
	e = (char *)fy_input_start(fyi) + fyt_end->handle.end_mark.input_pos;
	size = (size_t)(e - s);

	if (size > 0)
		aflags = fy_analyze_scalar_content(s, size,
				fy_token_atom_json_mode(fyt_start),
				fy_token_atom_lb_mode(fyt_start),
				fy_token_atom_flow_ws_mode(fyt_start));
	else
		aflags = FYACF_EMPTY | FYACF_FLOW_PLAIN | FYACF_BLOCK_PLAIN;

	memset(&handle, 0, sizeof(handle));
	handle.start_mark = fyt_start->handle.start_mark;
	handle.end_mark = fyt_end->handle.end_mark;

	/* if it's plain, all is good */
	if (aflags & FYACF_FLOW_PLAIN) {
		handle.storage_hint = size;	/* maximum */
		handle.storage_hint_valid = false;
		handle.direct_output = !!(aflags & FYACF_JSON_ESCAPE);	/* direct only when no json escape */
		handle.style = FYAS_PLAIN;
	} else {
		handle.storage_hint = 0;	/* just calculate */
		handle.storage_hint_valid = false;
		handle.direct_output = false;
		handle.style = FYAS_DOUBLE_QUOTED_MANUAL;
	}
	handle.empty = !!(aflags & FYACF_EMPTY);
	handle.has_lb = !!(aflags & FYACF_LB);
	handle.has_ws = !!(aflags & FYACF_WS);
	handle.starts_with_ws = !!(aflags & FYACF_STARTS_WITH_WS);
	handle.starts_with_lb = !!(aflags & FYACF_STARTS_WITH_LB);
	handle.ends_with_ws = !!(aflags & FYACF_ENDS_WITH_WS);
	handle.ends_with_lb = !!(aflags & FYACF_ENDS_WITH_LB);
	handle.trailing_lb = !!(aflags & FYACF_TRAILING_LB);
	handle.size0 = !!(aflags & FYACF_SIZE0);
	handle.valid_anchor = !!(aflags & FYACF_VALID_ANCHOR);
	handle.json_mode = false;		/* always false */
	handle.lb_mode = fylb_cr_nl;		/* always \r\n */
	handle.fws_mode = fyfws_space_tab;	/* always space + tab */

	handle.chomp = FYAC_STRIP;
	handle.increment = 0;
	handle.fyi = fyi;
	handle.tabsize = 0;

	fyt = fy_token_create(FYTT_INPUT_MARKER, &handle);
	if (!fyt)
		return NULL;

	return fyt;
}

struct fy_token *fy_node_token(struct fy_node *fyn)
{
	struct fy_atom atom;
	struct fy_input *fyi = NULL;
	struct fy_token *fyt = NULL;
	char *buf = NULL;

	if (!fyn)
		return NULL;

	/* if it's non synthetic we can use the node extends */
	if (!fy_node_is_synthetic(fyn))
		return fy_node_non_synthesized_token(fyn);

	/* emit to a string and create the token there */
	buf = fy_emit_node_to_string(fyn, FYECF_MODE_FLOW_ONELINE | FYECF_WIDTH_INF);
	if (!buf)
		goto err_out;

	fyi = fy_input_from_malloc_data(buf, FY_NT, &atom, true);
	if (!fyi)
		goto err_out;

	fyt = fy_token_create(FYTT_INPUT_MARKER, &atom);
	if (!fyt)
		goto err_out;

	/* take away the input reference */
	fy_input_unref(fyi);

	return fyt;

err_out:
	fy_input_unref(fyi);
	if (buf)
		free(buf);
	return NULL;
}

bool fy_node_uses_single_input_only(struct fy_node *fyn, struct fy_input *fyi)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp;

	if (!fyn || !fyi)
		return false;

	switch (fyn->type) {
	case FYNT_SCALAR:
		return fy_token_get_input(fyn->scalar) == fyi;

	case FYNT_SEQUENCE:
		if (fy_token_get_input(fyn->sequence_start) != fyi)
			return false;

		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
		     fyni = fy_node_next(&fyn->sequence, fyni)) {

			if (!fy_node_uses_single_input_only(fyni, fyi))
				return false;
		}

		if (fy_token_get_input(fyn->sequence_end) != fyi)
			return false;
		break;

	case FYNT_MAPPING:
		if (fy_token_get_input(fyn->mapping_start) != fyi)
			return false;

		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp;
		     fynp = fy_node_pair_next(&fyn->mapping, fynp)) {

			if (fynp->key && !fy_node_uses_single_input_only(fynp->key, fyi))
				return false;

			if (fynp->value && !fy_node_uses_single_input_only(fynp->value, fyi))
				return false;
		}

		if (fy_token_get_input(fyn->mapping_end) != fyi)
			return false;

		break;
	}

	return true;
}

struct fy_input *fy_node_get_first_input(struct fy_node *fyn)
{
	if (!fyn)
		return NULL;

	switch (fyn->type) {
	case FYNT_SCALAR:
		return fy_token_get_input(fyn->scalar);

	case FYNT_SEQUENCE:
		return fy_token_get_input(fyn->sequence_start);

	case FYNT_MAPPING:
		return fy_token_get_input(fyn->mapping_start);
	}

	/* should never happen */
	return NULL;
}

/* a node is synthetic if any of it's tokens reside in
 * different inputs, or any sequence/mapping has been
 * created via the manual sequence/mapping creation methods
 */
bool fy_node_is_synthetic(struct fy_node *fyn)
{
	return fyn && fyn->synthetic;
}

/* map this node and all of it's parents synthetic */
void fy_node_mark_synthetic(struct fy_node *fyn)
{
	if (!fyn)
		return;
	fyn->synthetic = true;
	while ((fyn = fy_node_get_document_parent(fyn)) != NULL)
		fyn->synthetic = true;
}

struct fy_input *fy_node_get_input(struct fy_node *fyn)
{
	struct fy_input *fyi = NULL;

	fyi = fy_node_get_first_input(fyn);
	if (!fyi)
		return NULL;

	return fy_node_uses_single_input_only(fyn, fyi) ? fyi : NULL;
}

int fy_document_register_anchor(struct fy_document *fyd,
				struct fy_node *fyn, struct fy_token *anchor)
{
	struct fy_anchor *fya, *fyam;
	struct fy_accel_entry *xle;
	const char *text;
	size_t text_len;
	int rc;

	fya = fy_anchor_create(fyd, fyn, anchor);
	fyd_error_check(fyd, fya, err_out,
			"fy_anchor_create() failed");

	fy_anchor_list_add_tail(&fyd->anchors, fya);
	if (fy_document_is_accelerated(fyd)) {
		xle = fy_accel_entry_lookup(fyd->axl, fya->anchor);
		if (xle) {
			fyam = (void *)xle->value;
			/* multiple */
			if (!fyam->multiple)
				fyam->multiple = true;
			fya->multiple = true;

			text = fy_anchor_get_text(fya, &text_len);
			fyd_notice(fyd, "register anchor %.*s is multiple", (int)text_len, text);
		}

		xle = fy_accel_entry_insert(fyd->axl, fya->anchor, fya);
		fyd_error_check(fyd, xle, err_out,
				"fy_accel_entry_insert() fyd->axl failed");
	}

	if (fy_document_is_accelerated(fyd)) {
		rc = fy_accel_insert(fyd->naxl, fyn, fya);
		fyd_error_check(fyd, !rc, err_out_rc,
				"fy_accel_insert() fyd->naxl failed");
	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	fyd->diag->on_error = false;
	return rc;
}

struct fy_node_cmp_arg {
	fy_node_scalar_compare_fn cmp_fn;
	void *arg;
};

static int fy_node_scalar_cmp_default(struct fy_node *fyn_a,
				      struct fy_node *fyn_b,
				      void *arg);

static int fy_node_mapping_sort_cmp_default(const struct fy_node_pair *fynp_a,
					    const struct fy_node_pair *fynp_b,
					    void *arg);

bool fy_node_compare_user(struct fy_node *fyn1, struct fy_node *fyn2,
			 fy_node_mapping_sort_fn sort_fn, void *sort_fn_arg,
			 fy_node_scalar_compare_fn cmp_fn, void *cmp_fn_arg)
{
	struct fy_node *fyni1, *fyni2;
	struct fy_node_pair *fynp1, *fynp2;
	bool ret, null1, null2;
	struct fy_node_pair **fynpp1, **fynpp2;
	int i, count1, count2;
	bool alias1, alias2;
	struct fy_node_cmp_arg def_arg;

	if (!cmp_fn) {
		cmp_fn = fy_node_scalar_cmp_default;
		cmp_fn_arg = NULL;
	}
	if (!sort_fn) {
		sort_fn = fy_node_mapping_sort_cmp_default;
		def_arg.cmp_fn = cmp_fn;
		def_arg.arg = cmp_fn_arg;
		sort_fn_arg = &def_arg;
	} else {
		def_arg.cmp_fn = NULL;
		def_arg.arg = NULL;
	}
	/* equal pointers? */
	if (fyn1 == fyn2)
		return true;

	null1 = !fyn1 || (fyn1->type == FYNT_SCALAR && fy_token_get_text_length(fyn1->scalar) == 0);
	null2 = !fyn2 || (fyn2->type == FYNT_SCALAR && fy_token_get_text_length(fyn2->scalar) == 0);

	/* both null */
	if (null1 && null2)
		return true;

	/* either is NULL, no match */
	if (null1 || null2)
		return false;

	/* types must match */
	if (fyn1->type != fyn2->type)
		return false;

	ret = true;

	switch (fyn1->type) {
	case FYNT_SEQUENCE:
		fyni1 = fy_node_list_head(&fyn1->sequence);
		fyni2 = fy_node_list_head(&fyn2->sequence);
		while (fyni1 && fyni2) {

			ret = fy_node_compare(fyni1, fyni2);
			if (!ret)
				break;

			fyni1 = fy_node_next(&fyn1->sequence, fyni1);
			fyni2 = fy_node_next(&fyn2->sequence, fyni2);
		}
		if (ret && fyni1 != fyni2 && (!fyni1 || !fyni2))
			ret = false;

		break;

	case FYNT_MAPPING:
		count1 = fy_node_mapping_item_count(fyn1);
		count2 = fy_node_mapping_item_count(fyn2);

		/* mapping counts must match */
		if (count1 != count2) {
			ret = false;
			break;
		}

		fynpp1 = FY_ALLOCA(sizeof(*fynpp1) * (count1 + 1));
		fy_node_mapping_fill_array(fyn1, fynpp1, count1);
		fy_node_mapping_perform_sort(fyn1, sort_fn, sort_fn_arg, fynpp1, count1);

		fynpp2 = FY_ALLOCA(sizeof(*fynpp2) * (count2 + 1));
		fy_node_mapping_fill_array(fyn2, fynpp2, count2);
		fy_node_mapping_perform_sort(fyn2, sort_fn, sort_fn_arg, fynpp2, count2);

		for (i = 0; i < count1; i++) {
			fynp1 = fynpp1[i];
			fynp2 = fynpp2[i];

			ret = fy_node_compare(fynp1->key, fynp2->key);
			if (!ret)
				break;

			ret = fy_node_compare(fynp1->value, fynp2->value);
			if (!ret)
				break;
		}
		if (i >= count1)
			ret = true;

		break;

	case FYNT_SCALAR:
		alias1 = fy_node_is_alias(fyn1);
		alias2 = fy_node_is_alias(fyn2);

		/* either both must be aliases or both not */
		if (alias1 != alias2)
			return false;

		ret = !cmp_fn(fyn1, fyn2, cmp_fn_arg);
		break;
	}

	return ret;
}

bool fy_node_compare(struct fy_node *fyn1, struct fy_node *fyn2)
{
	return fy_node_compare_user(fyn1, fyn2, NULL, NULL, NULL, NULL);
}

bool fy_node_compare_string(struct fy_node *fyn, const char *str, size_t len)
{
	struct fy_document *fyd = NULL;
	bool ret;

	fyd = fy_document_build_from_string(NULL, str, len);
	if (!fyd)
		return false;

	ret = fy_node_compare(fyn, fy_document_root(fyd));

	fy_document_destroy(fyd);

	return ret;
}

bool fy_node_compare_token(struct fy_node *fyn, struct fy_token *fyt)
{
	/* check if there's NULL */
	if (!fyn || !fyt)
		return false;

	/* only valid for scalars */
	if (!fy_node_is_scalar(fyn) || fyt->type != FYTT_SCALAR)
		return false;

	return fy_token_cmp(fyn->scalar, fyt) == 0;
}

bool fy_node_compare_text(struct fy_node *fyn, const char *text, size_t len)
{
	const char *textn;
	size_t lenn;

	if (!fyn || !text)
		return false;

	textn = fy_node_get_scalar(fyn, &lenn);
	if (!textn)
		return false;

	if (len == FY_NT)
		len = strlen(text);

	if (len != lenn)
		return false;

	return memcmp(text, textn, len) == 0;
}

struct fy_node_pair *fy_node_mapping_lookup_pair(struct fy_node *fyn, struct fy_node *fyn_key)
{
	struct fy_node_pair *fynpi, *fynp;

	/* sanity check */
	if (!fy_node_is_mapping(fyn))
		return NULL;

	fynp = NULL;


	if (fyn->xl) {
		fynp = fy_node_accel_lookup_by_node(fyn, fyn_key);
	} else {
		for (fynpi = fy_node_pair_list_head(&fyn->mapping); fynpi;
			fynpi = fy_node_pair_next(&fyn->mapping, fynpi)) {
			if (fy_node_compare(fynpi->key, fyn_key)) {
				fynp = fynpi;
				break;
			}
		}
	}

	return fynp;
}

int fy_node_mapping_get_pair_index(struct fy_node *fyn, const struct fy_node_pair *fynp)
{
	struct fy_node_pair *fynpi;
	int i;

	for (i = 0, fynpi = fy_node_pair_list_head(&fyn->mapping); fynpi;
		fynpi = fy_node_pair_next(&fyn->mapping, fynpi), i++) {

		if (fynpi == fynp)
			return i;
	}

	return -1;
}

bool fy_node_mapping_key_is_duplicate(struct fy_node *fyn, struct fy_node *fyn_key)
{
	return fy_node_mapping_lookup_pair(fyn, fyn_key) != NULL;
}

static int
fy_parse_document_load_node(struct fy_parser *fyp, struct fy_document *fyd,
			    struct fy_eventp *fyep, struct fy_node **fynp,
			    int *depthp);

int fy_parse_document_load_alias(struct fy_parser *fyp, struct fy_document *fyd, struct fy_eventp *fyep, struct fy_node **fynp)
{
	*fynp = NULL;

	fyp_doc_debug(fyp, "in %s", __func__);

	/* TODO verify aliases etc */
	fy_parse_eventp_recycle(fyp, fyep);
	return 0;
}

static int
fy_parse_document_load_scalar(struct fy_parser *fyp, struct fy_document *fyd,
			      struct fy_eventp *fyep, struct fy_node **fynp,
			      int *depthp)
{
	struct fy_node *fyn = NULL;
	struct fy_event *fye;
	int rc;

	if (!fyd)
		return -1;

	fyp_error_check(fyp, fyep || !fyp->stream_error, err_out,
			"no event to process");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 0, FYEM_DOC,
			fyep, err_out,
			"premature end of event stream");

	fyp_doc_debug(fyp, "in %s [%s]", __func__, fy_event_type_txt[fyep->e.type]);

	*fynp = NULL;

	fye = &fyep->e;

	/* we don't free nodes that often, so no need for recycling */
	fyn = fy_node_alloc(fyd, FYNT_SCALAR);
	fyp_error_check(fyp, fyn, err_out,
			"fy_node_alloc() failed");

	if (fye->type == FYET_SCALAR) {

		/* move the tags and value to the node */
		if (fye->scalar.value)
			fyn->style = fy_node_style_from_scalar_style(fye->scalar.value->scalar.style);
		else
			fyn->style = FYNS_PLAIN;
		fyn->tag = fye->scalar.tag;
		fye->scalar.tag = NULL;

		fyn->scalar = fye->scalar.value;
		fye->scalar.value = NULL;

		if (fye->scalar.anchor) {
			rc = fy_document_register_anchor(fyd, fyn, fye->scalar.anchor);
			fyp_error_check(fyp, !rc, err_out_rc,
					"fy_document_register_anchor() failed");
			fye->scalar.anchor = NULL;
		}

	} else if (fye->type == FYET_ALIAS) {
		fyn->style = FYNS_ALIAS;
		fyn->scalar = fye->alias.anchor;
		fye->alias.anchor = NULL;
	} else
		assert(0);

	*fynp = fyn;
	fyn = NULL;

	/* everything OK */
	fy_parse_eventp_recycle(fyp, fyep);
	return 0;

err_out:
	rc = -1;
err_out_rc:
	fy_parse_eventp_recycle(fyp, fyep);
	fyd->diag->on_error = false;
	return rc;
}

static int
fy_parse_document_load_sequence(struct fy_parser *fyp, struct fy_document *fyd,
				struct fy_eventp *fyep, struct fy_node **fynp,
				int *depthp)
{
	struct fy_node *fyn = NULL, *fyn_item = NULL;
	struct fy_event *fye = NULL;
	struct fy_token *fyt_ss = NULL;
	int rc;

	fyp_error_check(fyp, fyep || !fyp->stream_error, err_out,
			"no event to process");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 0, FYEM_DOC,
			fyep, err_out,
			"premature end of event stream");

	fyp_doc_debug(fyp, "in %s [%s]", __func__, fy_event_type_txt[fyep->e.type]);

	*fynp = NULL;

	fye = &fyep->e;

	fyt_ss = fye->sequence_start.sequence_start;

	/* we don't free nodes that often, so no need for recycling */
	fyn = fy_node_alloc(fyd, FYNT_SEQUENCE);
	fyp_error_check(fyp, fyn, err_out,
			"fy_node_alloc() failed");

	fyn->style = fyt_ss && fyt_ss->type == FYTT_FLOW_SEQUENCE_START ? FYNS_FLOW : FYNS_BLOCK;

	fyn->tag = fye->sequence_start.tag;
	fye->sequence_start.tag = NULL;

	if (fye->sequence_start.anchor) {
		rc = fy_document_register_anchor(fyd, fyn, fye->sequence_start.anchor);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_document_register_anchor() failed");
		fye->sequence_start.anchor = NULL;
	}

	if (fye->sequence_start.sequence_start) {
		fyn->sequence_start = fye->sequence_start.sequence_start;
		fye->sequence_start.sequence_start = NULL;
	} else
		fyn->sequence_start = NULL;

	assert(fyn->sequence_start);

	/* done with this */
	fy_parse_eventp_recycle(fyp, fyep);
	fyep = NULL;

	while ((fyep = fy_parse_private(fyp)) != NULL) {
		fye = &fyep->e;
		if (fye->type == FYET_SEQUENCE_END)
			break;

		rc = fy_parse_document_load_node(fyp, fyd, fyep, &fyn_item, depthp);
		fyep = NULL;
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_parse_document_load_node() failed");

		fy_node_list_add_tail(&fyn->sequence, fyn_item);
		fyn_item->attached = true;
		fyn_item = NULL;
	}

	if (!fyep)
		goto err_out;

	if (fye->sequence_end.sequence_end) {
		fyn->sequence_end = fye->sequence_end.sequence_end;
		fye->sequence_end.sequence_end = NULL;
	} else
		fyn->sequence_end = NULL;

	assert(fyn->sequence_end);

	*fynp = fyn;
	fyn = NULL;

	fy_parse_eventp_recycle(fyp, fyep);
	return 0;

	/* fallthrough */
err_out:
	rc = -1;
err_out_rc:
	fy_parse_eventp_recycle(fyp, fyep);
	fy_node_detach_and_free(fyn_item);
	fy_node_detach_and_free(fyn);
	return rc;
}

static int
fy_parse_document_load_mapping(struct fy_parser *fyp, struct fy_document *fyd,
			       struct fy_eventp *fyep, struct fy_node **fynp,
			       int *depthp)
{
	struct fy_node *fyn = NULL, *fyn_key = NULL, *fyn_value = NULL;
	struct fy_node_pair *fynp_item = NULL;
	struct fy_event *fye = NULL;
	struct fy_token *fyt_ms = NULL;
	bool duplicate;
	int rc;

	fyp_error_check(fyp, fyep || !fyp->stream_error, err_out,
			"no event to process");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 0, FYEM_DOC,
			fyep, err_out,
			"premature end of event stream");

	fyp_doc_debug(fyp, "in %s [%s]", __func__, fy_event_type_txt[fyep->e.type]);

	*fynp = NULL;

	fye = &fyep->e;

	fyt_ms = fye->mapping_start.mapping_start;

	/* we don't free nodes that often, so no need for recycling */
	fyn = fy_node_alloc(fyd, FYNT_MAPPING);
	fyp_error_check(fyp, fyn, err_out,
			"fy_node_alloc() failed");

	fyn->style = fyt_ms && fyt_ms->type == FYTT_FLOW_MAPPING_START ? FYNS_FLOW : FYNS_BLOCK;

	fyn->tag = fye->mapping_start.tag;
	fye->mapping_start.tag = NULL;

	if (fye->mapping_start.anchor) {
		rc = fy_document_register_anchor(fyd, fyn, fye->mapping_start.anchor);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_document_register_anchor() failed");
		fye->mapping_start.anchor = NULL;
	}

	if (fye->mapping_start.mapping_start) {
		fyn->mapping_start = fye->mapping_start.mapping_start;
		fye->mapping_start.mapping_start = NULL;
	}

	assert(fyn->mapping_start);

	/* done with this */
	fy_parse_eventp_recycle(fyp, fyep);
	fyep = NULL;

	while ((fyep = fy_parse_private(fyp)) != NULL) {
		fye = &fyep->e;
		if (fye->type == FYET_MAPPING_END)
			break;

		fynp_item = fy_node_pair_alloc(fyd);
		fyp_error_check(fyp, fynp_item, err_out,
				"fy_node_pair_alloc() failed");

		fyn_key = NULL;
		fyn_value = NULL;

		rc = fy_parse_document_load_node(fyp, fyd,
						 fyep, &fyn_key, depthp);
		fyep = NULL;

		assert(fyn_key);

		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_parse_document_load_node() failed");

		/* if we don't allow duplicate keys */
		if (!(fyd->parse_cfg.flags & FYPCF_ALLOW_DUPLICATE_KEYS)) {

			/* make sure we don't add an already existing key */
			duplicate = fy_node_mapping_key_is_duplicate(fyn, fyn_key);

			FYP_NODE_ERROR_CHECK(fyp, fyn_key, FYEM_DOC,
					!duplicate, err_out,
					"duplicate key");
		}

		fyep = fy_parse_private(fyp);

		fyp_error_check(fyp, fyep || !fyp->stream_error, err_out,
				"fy_parse_private() failed");

		FYP_PARSE_ERROR_CHECK(fyp, 0, 0, FYEM_DOC,
				fyep, err_out,
				"missing mapping value");

		fye = &fyep->e;

		rc = fy_parse_document_load_node(fyp, fyd,
						 fyep, &fyn_value, depthp);
		fyep = NULL;
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_parse_document_load_node() failed");

		assert(fyn_value);

		fynp_item->key = fyn_key;
		fynp_item->value = fyn_value;
		fy_node_pair_list_add_tail(&fyn->mapping, fynp_item);
		if (fyn->xl) {
			rc = fy_accel_insert(fyn->xl, fynp_item->key, fynp_item);
			fyp_error_check(fyp, !rc, err_out_rc,
					"fy_accel_insert() failed");
		}

		if (fynp_item->key)
			fynp_item->key->attached = true;
		if (fynp_item->value)
			fynp_item->value->attached = true;
		fynp_item = NULL;
		fyn_key = NULL;
		fyn_value = NULL;
	}

	if (!fyep)
		goto err_out;

	if (fye->mapping_end.mapping_end) {
		fyn->mapping_end = fye->mapping_end.mapping_end;
		fye->mapping_end.mapping_end = NULL;
	}

	assert(fyn->mapping_end);

	*fynp = fyn;
	fyn = NULL;

	fy_parse_eventp_recycle(fyp, fyep);

	return 0;

err_out:
	rc = -1;
err_out_rc:
	fy_parse_eventp_recycle(fyp, fyep);
	fy_node_pair_free(fynp_item);
	fy_node_detach_and_free(fyn_key);
	fy_node_detach_and_free(fyn_value);
	fy_node_detach_and_free(fyn);
	return rc;
}

static int
fy_parse_document_load_node(struct fy_parser *fyp, struct fy_document *fyd,
			    struct fy_eventp *fyep, struct fy_node **fynp,
			    int *depthp)
{
	struct fy_event *fye;
	enum fy_event_type type;
	int ret;

	*fynp = NULL;

	fyp_error_check(fyp, fyep || !fyp->stream_error, err_out,
			"no event to process");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 0, FYEM_DOC,
			fyep, err_out,
			"premature end of event stream");

	fyp_doc_debug(fyp, "in %s [%s]", __func__, fy_event_type_txt[fyep->e.type]);

	fye = &fyep->e;

	type = fye->type;

	FYP_TOKEN_ERROR_CHECK(fyp, fy_event_get_token(fye), FYEM_DOC,
			type == FYET_ALIAS || type == FYET_SCALAR ||
			type == FYET_SEQUENCE_START || type == FYET_MAPPING_START, err_out,
			"bad event");

	(*depthp)++;

	FYP_TOKEN_ERROR_CHECK(fyp, fy_event_get_token(fye), FYEM_DOC,
			((fyp->cfg.flags & FYPCF_DISABLE_DEPTH_LIMIT) ||
				*depthp <= fy_depth_limit()), err_out,
			"depth limit exceeded");

	switch (type) {

	case FYET_ALIAS:
	case FYET_SCALAR:
		ret = fy_parse_document_load_scalar(fyp, fyd,
						     fyep, fynp, depthp);
		break;

	case FYET_SEQUENCE_START:
		ret = fy_parse_document_load_sequence(fyp, fyd,
						       fyep, fynp, depthp);
		break;

	case FYET_MAPPING_START:
		ret = fy_parse_document_load_mapping(fyp, fyd,
						      fyep, fynp, depthp);
		break;

	default:
		ret = 0;
		break;
	}

	--(*depthp);
	return ret;

err_out:
	fy_parse_eventp_recycle(fyp, fyep);
	return -1;
}

int fy_parse_document_load_end(struct fy_parser *fyp, struct fy_document *fyd, struct fy_eventp *fyep)
{
	struct fy_event *fye;
	int rc;

	fyp_error_check(fyp, fyep || !fyp->stream_error, err_out,
			"no event to process");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 0, FYEM_DOC,
			fyep, err_out,
			"premature end of event stream");

	fyp_doc_debug(fyp, "in %s [%s]", __func__, fy_event_type_txt[fyep->e.type]);

	fye = &fyep->e;

	FYP_TOKEN_ERROR_CHECK(fyp, fy_event_get_token(fye), FYEM_DOC,
			fye->type == FYET_DOCUMENT_END, err_out,
			"bad event");

	/* recycle the document end event */
	fy_parse_eventp_recycle(fyp, fyep);

	return 0;
err_out:
	rc = -1;
	fy_parse_eventp_recycle(fyp, fyep);
	return rc;
}

struct fy_document *fy_parse_load_document_recursive(struct fy_parser *fyp)
{
	struct fy_document *fyd = NULL;
	struct fy_eventp *fyep = NULL;
	struct fy_event *fye = NULL;
	int rc, depth;
	bool was_stream_start;

again:
	was_stream_start = false;
	do {
		/* get next event */
		fyep = fy_parse_private(fyp);

		/* no more */
		if (!fyep)
			return NULL;

		was_stream_start = fyep->e.type == FYET_STREAM_START;

		if (was_stream_start) {
			fy_parse_eventp_recycle(fyp, fyep);
			fyep = NULL;
		}

	} while (was_stream_start);

	fye = &fyep->e;

	/* STREAM_END */
	if (fye->type == FYET_STREAM_END) {
		fy_parse_eventp_recycle(fyp, fyep);

		/* final STREAM_END? */
		if (fyp->state == FYPS_END)
			return NULL;

		/* multi-stream */
		goto again;
	}

	FYP_TOKEN_ERROR_CHECK(fyp, fy_event_get_token(fye), FYEM_DOC,
			fye->type == FYET_DOCUMENT_START, err_out,
			"bad event");

	fyd = fy_parse_document_create(fyp, fyep);
	fyep = NULL;

	fyp_error_check(fyp, fyd, err_out,
			"fy_parse_document_create() failed");

	fyp_doc_debug(fyp, "calling load_node() for root");
	depth = 0;
	rc = fy_parse_document_load_node(fyp, fyd, fy_parse_private(fyp),
					 &fyd->root, &depth);
	fyp_error_check(fyp, !rc, err_out,
			"fy_parse_document_load_node() failed");

	rc = fy_parse_document_load_end(fyp, fyd, fy_parse_private(fyp));
	fyp_error_check(fyp, !rc, err_out,
			"fy_parse_document_load_node() failed");

	/* always resolve parents */
	fy_resolve_parent_node(fyd, fyd->root, NULL);

	if (fyp->cfg.flags & FYPCF_RESOLVE_DOCUMENT) {
		rc = fy_document_resolve(fyd);
		fyp_error_check(fyp, !rc, err_out,
				"fy_document_resolve() failed");
	}

	return fyd;

err_out:
	fy_parse_eventp_recycle(fyp, fyep);
	fy_parse_document_destroy(fyp, fyd);
	return NULL;
}

struct fy_document *fy_parse_load_document_with_builder(struct fy_parser *fyp)
{
	struct fy_document_builder_cfg cfg;
	struct fy_document *fyd;
	int rc;

	if (!fyp)
		return NULL;

	if (!fyp->fydb) {
		memset(&cfg, 0, sizeof(cfg));
		cfg.parse_cfg = fyp->cfg;
		cfg.userdata = fyp;
		cfg.diag = fy_diag_ref(fyp->diag);

		fyp->fydb = fy_document_builder_create(&cfg);
		if (!fyp->fydb)
			return NULL;
	}

	fyd = fy_document_builder_load_document(fyp->fydb, fyp);
	if (!fyd)
		return NULL;

	if (fyp->cfg.flags & FYPCF_RESOLVE_DOCUMENT) {
		rc = fy_document_resolve(fyd);
		if (rc) {
			fy_document_destroy(fyd);
			fyp->stream_error = true;
			return NULL;
		}
	}

	return fyd;
}

struct fy_document *fy_parse_load_document(struct fy_parser *fyp)
{
	if (!fyp)
		return NULL;

	return !(fyp->cfg.flags & FYPCF_PREFER_RECURSIVE) ?
		fy_parse_load_document_with_builder(fyp) :
		fy_parse_load_document_recursive(fyp);
}

struct fy_node *fy_node_copy_internal(struct fy_document *fyd, struct fy_node *fyn_from,
				      struct fy_node *fyn_parent)
{
	struct fy_document *fyd_from;
	struct fy_node *fyn, *fyni, *fynit;
	struct fy_node_pair *fynp, *fynpt;
	struct fy_anchor *fya, *fya_from;
	const char *anchor;
	size_t anchor_len;
	int rc;

	if (!fyd || !fyn_from || !fyn_from->fyd)
		return NULL;

	fyd_from = fyn_from->fyd;

	fyn = fy_node_alloc(fyd, fyn_from->type);
	fyd_error_check(fyd, fyn, err_out,
			"fy_node_alloc() failed");

	fyn->tag = fy_token_ref(fyn_from->tag);
	fyn->style = fyn_from->style;
	fyn->parent = fyn_parent;

	switch (fyn->type) {
	case FYNT_SCALAR:
		fyn->scalar = fy_token_ref(fyn_from->scalar);
		break;

	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn_from->sequence); fyni;
				fyni = fy_node_next(&fyn_from->sequence, fyni)) {

			fynit = fy_node_copy_internal(fyd, fyni, fyn);
			fyd_error_check(fyd, fynit, err_out,
					"fy_node_copy_internal() failed");

			fy_node_list_add_tail(&fyn->sequence, fynit);
			fynit->attached = true;
		}

		break;
	case FYNT_MAPPING:
		for (fynp = fy_node_pair_list_head(&fyn_from->mapping); fynp;
				fynp = fy_node_pair_next(&fyn_from->mapping, fynp)) {

			fynpt = fy_node_pair_alloc(fyd);
			fyd_error_check(fyd, fynpt, err_out,
					"fy_node_pair_alloc() failed");

			fynpt->key = fy_node_copy_internal(fyd, fynp->key, fyn);
			fynpt->value = fy_node_copy_internal(fyd, fynp->value, fyn);
			fynp->parent = fyn;

			fy_node_pair_list_add_tail(&fyn->mapping, fynpt);
			if (fyn->xl) {
				rc = fy_accel_insert(fyn->xl, fynpt->key, fynpt);
				fyd_error_check(fyd, !rc, err_out,
						"fy_accel_insert() failed");
			}
			if (fynpt->key) {
				fynpt->key->attached = true;
				fynpt->key->key_root = true;
			}
			if (fynpt->value)
				fynpt->value->attached = true;
		}
		break;
	}

	/* drop an anchor to the copy */
	for (fya_from = fy_anchor_list_head(&fyd_from->anchors); fya_from;
			fya_from = fy_anchor_next(&fyd_from->anchors, fya_from)) {
		if (fyn_from == fya_from->fyn)
			break;
	}

	/* source node has an anchor */
	if (fya_from) {
		fya = fy_document_lookup_anchor_by_token(fyd, fya_from->anchor);
		if (!fya) {
			fyd_doc_debug(fyd, "new anchor");
			/* update the new anchor position */
			rc = fy_document_register_anchor(fyd, fyn, fya_from->anchor);
			fyd_error_check(fyd, !rc, err_out,
					"fy_document_register_anchor() failed");

			fy_token_ref(fya_from->anchor);
		} else {
			anchor = fy_anchor_get_text(fya, &anchor_len);
			fyd_error_check(fyd, anchor, err_out,
					"fy_anchor_get_text() failed");
			fyd_doc_debug(fyd, "not overwritting anchor %.*s", (int)anchor_len, anchor);
		}
	}

	return fyn;

err_out:
	return NULL;
}

struct fy_node *fy_node_copy(struct fy_document *fyd, struct fy_node *fyn_from)
{
	struct fy_node *fyn;

	if (!fyd)
		return NULL;

	fyn = fy_node_copy_internal(fyd, fyn_from, NULL);
	if (!fyn) {
		fyd->diag->on_error = false;
		return NULL;
	}

	return fyn;
}

struct fy_document *fy_document_clone(struct fy_document *fydsrc)
{
	struct fy_document *fyd = NULL;

	if (!fydsrc)
		return NULL;

	fyd = fy_document_create(&fydsrc->parse_cfg);
	if (!fyd)
		return NULL;

	/* drop the default document state */
	fy_document_state_unref(fyd->fyds);
	/* and use the source document state (and ref it) */
	fyd->fyds = fy_document_state_ref(fydsrc->fyds);
	assert(fyd->fyds);

	if (fydsrc->root) {
		fyd->root = fy_node_copy(fyd, fydsrc->root);
		if (!fyd->root)
			goto err_out;
	}

	return fyd;
err_out:
	fy_document_destroy(fyd);
	return NULL;
}

int fy_node_copy_to_scalar(struct fy_document *fyd, struct fy_node *fyn_to, struct fy_node *fyn_from)
{
	struct fy_node *fyn, *fyni;
	struct fy_node_pair *fynp;

	fyn = fy_node_copy(fyd, fyn_from);
	if (!fyn)
		return -1;

	/* the node is guaranteed to be a scalar */
	fy_token_unref(fyn_to->tag);
	fyn_to->tag = NULL;
	fy_token_unref(fyn_to->scalar);
	fyn_to->scalar = NULL;

	fyn_to->type = fyn->type;
	fyn_to->tag = fy_token_ref(fyn->tag);
	fyn_to->style = fyn->style;

	switch (fyn->type) {
	case FYNT_SCALAR:
		fyn_to->scalar = fyn->scalar;
		fyn->scalar = NULL;
		break;
	case FYNT_SEQUENCE:
		fy_node_list_init(&fyn_to->sequence);
		while ((fyni = fy_node_list_pop(&fyn->sequence)) != NULL)
			fy_node_list_add_tail(&fyn_to->sequence, fyni);
		break;
	case FYNT_MAPPING:
		fy_node_pair_list_init(&fyn_to->mapping);
		while ((fynp = fy_node_pair_list_pop(&fyn->mapping)) != NULL) {
			if (fyn->xl)
				fy_accel_remove(fyn->xl, fynp->key);
			fy_node_pair_list_add_tail(&fyn_to->mapping, fynp);
			if (fyn_to->xl)
				fy_accel_insert(fyn_to->xl, fynp->key, fynp);
		}
		break;
	}

	/* and free */
	fy_node_free(fyn);

	return 0;
}

static int fy_document_node_update_tags(struct fy_document *fyd, struct fy_node *fyn)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp, *fynpi;
	struct fy_token *fyt_td;
	const char *handle;
	size_t handle_size;
	int rc;

	if (!fyd || !fyn)
		return 0;

	/* replace tag reference with the one that the document contains */
	if (fyn->tag) {
		fyd_error_check(fyd, fyn->tag->type == FYTT_TAG, err_out,
				"bad node tag");

		handle = fy_tag_directive_token_handle(fyn->tag->tag.fyt_td, &handle_size);
		fyd_error_check(fyd, handle, err_out,
				"bad tag directive token");

		fyt_td = fy_document_state_lookup_tag_directive(fyd->fyds, handle, handle_size);
		fyd_error_check(fyd, fyt_td, err_out,
				"Missing tag directive with handle=%.*s", (int)handle_size, handle);

		/* need to replace this */
		if (fyt_td != fyn->tag->tag.fyt_td) {
			fy_token_unref(fyn->tag->tag.fyt_td);
			fyn->tag->tag.fyt_td = fy_token_ref(fyt_td);

		}
	}


	switch (fyn->type) {
	case FYNT_SCALAR:
		break;

	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni)) {

			rc = fy_document_node_update_tags(fyd, fyni);
			if (rc)
				goto err_out_rc;
		}
		break;

	case FYNT_MAPPING:
		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp; fynp = fynpi) {

			fynpi = fy_node_pair_next(&fyn->mapping, fynp);

			/* the parent of the key is always NULL */
			rc = fy_document_node_update_tags(fyd, fynp->key);
			if (rc)
				goto err_out_rc;

			rc = fy_document_node_update_tags(fyd, fynp->value);
			if (rc)
				goto err_out_rc;
		}
		break;
	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_node_insert(struct fy_node *fyn_to, struct fy_node *fyn_from)
{
	struct fy_document *fyd;
	struct fy_node *fyn_parent, *fyn_cpy, *fyni, *fyn_prev;
	struct fy_node_pair *fynp, *fynpi, *fynpj;
	int rc;

	if (!fyn_to || !fyn_to->fyd)
		return -1;

	fyd = fyn_to->fyd;
	assert(fyd);

	fyn_parent = fy_node_get_document_parent(fyn_to);
	fynp = NULL;
	if (fyn_parent) {
		fyd_error_check(fyd, fyn_parent->type != FYNT_SCALAR, err_out,
				    "Illegal scalar parent node type");

		fyd_error_check(fyd, fyn_from, err_out,
				"Illegal NULL source node");

		if (fyn_parent->type == FYNT_MAPPING) {
			/* find mapping pair that contains the `to` node */
			for (fynp = fy_node_pair_list_head(&fyn_parent->mapping); fynp;
					fynp = fy_node_pair_next(&fyn_parent->mapping, fynp)) {
				if (fynp->value == fyn_to)
					break;
			}
		}
	}

	/* verify no funkiness on root */
	assert(fyn_parent || fyn_to == fyd->root);

	/* deleting target */
	if (!fyn_from) {
		fyn_to->parent = NULL;

		if (!fyn_parent) {
			fyd_doc_debug(fyd, "Deleting root node");
			fy_node_detach_and_free(fyn_to);
			fyd->root = NULL;
		} else if (fyn_parent->type == FYNT_SEQUENCE) {
			fyd_doc_debug(fyd, "Deleting sequence node");
			fy_node_list_del(&fyn_parent->sequence, fyn_to);
			fy_node_detach_and_free(fyn_to);
		} else {
			fyd_doc_debug(fyd, "Deleting mapping node");
			/* should never happen, it's checked right above, but play safe */
			assert(fyn_parent->type == FYNT_MAPPING);

			fyd_error_check(fyd, fynp, err_out,
					"Illegal mapping node found");

			fy_node_pair_list_del(&fyn_parent->mapping, fynp);
			if (fyn_parent->xl)
				fy_accel_remove(fyn_parent->xl, fynp->key);
			/* this will also delete fyn_to */
			fy_node_pair_detach_and_free(fynp);
		}
		return 0;
	}

	/*
	 * from: scalar
	 *
	 * to: another-scalar -> scalar
	 * to: { key: value } -> scalar
	 * to: [ seq0, seq1 ] -> scalar
	 *
	 * from: [ seq2 ]
	 * to: scalar -> [ seq2 ]
	 * to: { key: value } -> [ seq2 ]
	 * to: [ seq0, seq1 ] -> [ seq0, seq1, sec2 ]
	 *
	 * from: { another-key: another-value }
	 * to: scalar -> { another-key: another-value }
	 * to: { key: value } -> { key: value, another-key: another-value }
	 * to: [ seq0, seq1 ] -> { another-key: another-value }
	 *
	 * from: { key: another-value }
	 * to: scalar -> { key: another-value }
	 * to: { key: value } -> { key: another-value }
	 * to: [ seq0, seq1 ] -> { key: another-value }
	 *
	 */

	/* if types of `from` and `to` differ (or it's a scalar), it's a replace */
	if (fyn_from->type != fyn_to->type || fyn_from->type == FYNT_SCALAR) {

		fyn_cpy = fy_node_copy(fyd, fyn_from);
		fyd_error_check(fyd, fyn_cpy, err_out,
				"fy_node_copy() failed");

		if (!fyn_parent) {
			fyd_doc_debug(fyd, "Replacing root node");
			fy_node_detach_and_free(fyd->root);
			fyd->root = fyn_cpy;
		} else if (fyn_parent->type == FYNT_SEQUENCE) {
			fyd_doc_debug(fyd, "Replacing sequence node");

			/* get previous */
			fyn_prev = fy_node_prev(&fyn_parent->sequence, fyn_to);

			/* delete */
			fy_node_list_del(&fyn_parent->sequence, fyn_to);
			fy_node_detach_and_free(fyn_to);

			/* if there's no previous insert to head */
			if (!fyn_prev)
				fy_node_list_add(&fyn_parent->sequence, fyn_cpy);
			else
				fy_node_list_insert_after(&fyn_parent->sequence, fyn_prev, fyn_cpy);
		} else {
			fyd_doc_debug(fyd, "Replacing mapping node value");
			/* should never happen, it's checked right above, but play safe */
			assert(fyn_parent->type == FYNT_MAPPING);
			fyd_error_check(fyd, fynp, err_out,
					"Illegal mapping node found");

			fy_node_detach_and_free(fynp->value);
			fynp->value = fyn_cpy;
		}

		return 0;
	}

	/* types match, if it's a sequence append */
	if (fyn_to->type == FYNT_SEQUENCE) {

		fyd_doc_debug(fyd, "Appending to sequence node");

		for (fyni = fy_node_list_head(&fyn_from->sequence); fyni;
				fyni = fy_node_next(&fyn_from->sequence, fyni)) {

			fyn_cpy = fy_node_copy(fyd, fyni);
			fyd_error_check(fyd, fyn_cpy, err_out,
					"fy_node_copy() failed");

			fy_node_list_add_tail(&fyn_to->sequence, fyn_cpy);
			fyn_cpy->attached = true;
		}
	} else {
		/* only mapping is possible here */

		/* iterate over all the keys in the `from` */
		for (fynpi = fy_node_pair_list_head(&fyn_from->mapping); fynpi;
			fynpi = fy_node_pair_next(&fyn_from->mapping, fynpi)) {

			if (fyn_to->xl) {
				fynpj = fy_node_accel_lookup_by_node(fyn_to, fynpi->key);
			} else {
				/* find whether the key already exists */
				for (fynpj = fy_node_pair_list_head(&fyn_to->mapping); fynpj;
					fynpj = fy_node_pair_next(&fyn_to->mapping, fynpj)) {

					if (fy_node_compare(fynpi->key, fynpj->key))
						break;
				}
			}

			if (!fynpj) {
				fyd_doc_debug(fyd, "Appending to mapping node");

				/* not found? append it */
				fynpj = fy_node_pair_alloc(fyd);
				fyd_error_check(fyd, fynpj, err_out,
						"fy_node_pair_alloc() failed");

				fynpj->key = fy_node_copy(fyd, fynpi->key);
				fyd_error_check(fyd, !fynpi->key || fynpj->key, err_out,
						"fy_node_copy() failed");
				fynpj->value = fy_node_copy(fyd, fynpi->value);
				fyd_error_check(fyd, !fynpi->value || fynpj->value, err_out,
						"fy_node_copy() failed");

				fy_node_pair_list_add_tail(&fyn_to->mapping, fynpj);
				if (fyn_to->xl)
					fy_accel_insert(fyn_to->xl, fynpj->key, fynpj);

				if (fynpj->key)
					fynpj->key->attached = true;
				if (fynpj->value)
					fynpj->value->attached = true;

			} else {
				fyd_doc_debug(fyd, "Updating mapping node value (deep merge)");

				rc = fy_node_insert(fynpj->value, fynpi->value);
				fyd_error_check(fyd, !rc, err_out_rc,
						"fy_node_insert() failed");
			}
		}
	}

	/* adjust parents */
	switch (fyn_to->type) {
	case FYNT_SCALAR:
		break;

	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn_to->sequence); fyni;
				fyni = fy_node_next(&fyn_to->sequence, fyni)) {

			fyni->parent = fyn_to;
		}
		break;

	case FYNT_MAPPING:
		for (fynp = fy_node_pair_list_head(&fyn_to->mapping); fynp; fynp = fynpi) {

			fynpi = fy_node_pair_next(&fyn_to->mapping, fynp);

			if (fynp->key) {
				fynp->key->parent = fyn_to;
				fynp->key->key_root = true;
			}
			if (fynp->value)
				fynp->value->parent = fyn_to;
			fynp->parent = fyn_to;
		}
		break;
	}

	/* if the documents differ, merge their states */
	if (fyn_to->fyd != fyn_from->fyd) {
		rc = fy_document_state_merge(fyn_to->fyd->fyds, fyn_from->fyd->fyds);
		fyd_error_check(fyd, !rc, err_out_rc,
				"fy_document_state_merge() failed");

		rc = fy_document_node_update_tags(fyd, fy_document_root(fyd));
		fyd_error_check(fyd, !rc, err_out_rc,
				"fy_document_node_update_tags() failed");
	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_document_insert_at(struct fy_document *fyd,
			  const char *path, size_t pathlen,
			  struct fy_node *fyn)
{
	int rc;
	struct fy_node *fyn2;

	fyn2 = fy_node_by_path(fy_document_root(fyd), path, pathlen, FYNWF_DONT_FOLLOW);
	rc = fy_node_insert(fyn2, fyn);

	fy_node_free(fyn);

	return rc;
}

struct fy_token *fy_document_tag_directive_iterate(struct fy_document *fyd, void **prevp)
{
	struct fy_token_list *fytl;

	if (!fyd || !fyd->fyds || !prevp)
		return NULL;

	fytl = &fyd->fyds->fyt_td;

	return *prevp = *prevp ? fy_token_next(fytl, *prevp) : fy_token_list_head(fytl);
}

struct fy_token *fy_document_tag_directive_lookup(struct fy_document *fyd, const char *handle)
{
	struct fy_token *fyt;
	void *iter;
	const char *h;
	size_t h_size, len;

	if (!fyd || !handle)
		return NULL;
	len = strlen(handle);

	iter = NULL;
	while ((fyt = fy_document_tag_directive_iterate(fyd, &iter)) != NULL) {
		h = fy_tag_directive_token_handle(fyt, &h_size);
		if (!h)
			continue;
		if (h_size == len && !memcmp(h, handle, len))
			return fyt;
	}
	return NULL;
}

int fy_document_tag_directive_add(struct fy_document *fyd, const char *handle, const char *prefix)
{
	struct fy_token *fyt;

	if (!fyd || !fyd->fyds || !handle || !prefix)
		return -1;

	/* it must not exist */
	fyt = fy_document_tag_directive_lookup(fyd, handle);
	if (fyt)
		return -1;

	return fy_document_state_append_tag(fyd->fyds, handle, prefix, false);
}

int fy_document_tag_directive_remove(struct fy_document *fyd, const char *handle)
{
	struct fy_token *fyt;

	if (!fyd || !fyd->fyds || !handle)
		return -1;

	/* it must not exist */
	fyt = fy_document_tag_directive_lookup(fyd, handle);
	if (!fyt || fyt->refs != 1)
		return -1;

	fy_token_list_del(&fyd->fyds->fyt_td, fyt);
	fy_token_unref(fyt);

	return 0;
}

static int fy_resolve_alias(struct fy_document *fyd, struct fy_node *fyn)
{
	struct fy_node *fyn_copy = NULL;
	int rc;

	fyn_copy = fy_node_resolve_alias(fyn);
	FYD_NODE_ERROR_CHECK(fyd, fyn, FYEM_DOC,
			fyn_copy, err_out,
			"invalid alias");

	rc = fy_node_copy_to_scalar(fyd, fyn, fyn_copy);
	fyd_error_check(fyd, !rc, err_out,
			"fy_node_copy_to_scalar() failed");

	return 0;

err_out:
	fyd->diag->on_error = false;
	return -1;
}

static struct fy_node *
fy_node_follow_alias(struct fy_node *fyn, enum fy_node_walk_flags flags)
{
	enum fy_node_walk_flags ptr_flags;
	struct fy_anchor *fya;
	const char *anchor_text, *s, *e, *p, *path;
	size_t anchor_len, path_len;
	struct fy_node *fyn_path_root;
	unsigned int marker;

	if (!fyn || !fy_node_is_alias(fyn))
		return NULL;

	ptr_flags = flags & FYNWF_PTR(FYNWF_PTR_MASK);
	if (ptr_flags == FYNWF_PTR_YPATH)
		return fy_node_alias_resolve_by_ypath(fyn);

	/* try regular label target */
	fya = fy_document_lookup_anchor_by_token(fyn->fyd, fyn->scalar);
	if (fya)
		return fya->fyn;

	anchor_text = fy_token_get_text(fyn->scalar, &anchor_len);
	if (!anchor_text)
		return NULL;

	s = anchor_text;
	e = s + anchor_len;

	fyn_path_root = NULL;

	if (ptr_flags == FYNWF_PTR_YAML && (p = memchr(s, '/', e - s)) != NULL) {
		/* fyd_notice(fyn->fyd, "%s: alias contains a path component %.*s",
				__func__, (int)(e - p - 1), p + 1); */

		if (p > s) {

			fya = fy_document_lookup_anchor(fyn->fyd, s, p - s);
			if (!fya) {
				/* fyd_notice(fyn->fyd, "%s: unable to resolve alias %.*s @%s",
						__func__, (int)(p - s), s, fy_node_get_path(fya->fyn)); */
				return NULL;
			}

			/* fyd_notice(fyn->fyd, "%s: alias base %.*s @%s",
					__func__, (int)(p - s), s, fy_node_get_path(fya->fyn)); */
			path = ++p;
			path_len = e - p;

			fyn_path_root = fya->fyn;

		} else {
			/* fyd_notice(fyn->fyd, "%s: absolute %.*s @%s",
					__func__, (int)(p - s), s, fy_node_get_path(fya->fyn)); */
			path = s;
			path_len = e - s;

			fyn_path_root = fyn->fyd->root;
		}
	}

	if (!fyn_path_root)
		return NULL;

	marker = fy_node_walk_marker_from_flags(flags);
	if (marker >= FYNWF_MAX_USER_MARKER)
		return NULL;

	/* use the next marker */
	flags &= ~FYNWF_MARKER(FYNWF_MARKER_MASK);
	flags |= FYNWF_MARKER(marker + 1);

	return fy_node_by_path_internal(fyn_path_root, path, path_len, flags);
}

static bool fy_node_pair_is_merge_key(struct fy_node_pair *fynp)
{
	struct fy_node *fyn = fynp->key;

	return fyn && fyn->type == FYNT_SCALAR && fyn->style == FYNS_PLAIN &&
	       fy_plain_atom_streq(fy_token_atom(fyn->scalar), "<<");
}

static struct fy_node *fy_alias_get_merge_mapping(struct fy_document *fyd, struct fy_node *fyn)
{
	struct fy_anchor *fya;

	/* must be an alias */
	if (!fy_node_is_alias(fyn))
		return NULL;

	/* anchor must exist */
	fya = fy_document_lookup_anchor_by_token(fyd, fyn->scalar);
	if (!fya)
		return NULL;

	/* and it must be a mapping */
	if (fya->fyn->type != FYNT_MAPPING)
		return NULL;

	return fya->fyn;
}

static bool fy_node_pair_is_valid_merge_key(struct fy_document *fyd, struct fy_node_pair *fynp)
{
	struct fy_node *fyn, *fyni, *fynm;

	fyn = fynp->value;

	/* value must exist */
	if (!fyn)
		return false;

	/* scalar alias */
	fynm = fy_alias_get_merge_mapping(fyd, fyn);
	if (fynm)
		return true;

	/* it must be a sequence then */
	if (fyn->type != FYNT_SEQUENCE)
		return false;

	/* the sequence must only contain valid aliases for mapping */
	for (fyni = fy_node_list_head(&fyn->sequence); fyni;
			fyni = fy_node_next(&fyn->sequence, fyni)) {

		/* sequence of aliases only! */
		fynm = fy_alias_get_merge_mapping(fyd, fyni);
		if (!fynm)
			return false;

	}

	return true;
}

static int fy_resolve_merge_key_populate(struct fy_document *fyd, struct fy_node *fyn,
					  struct fy_node_pair *fynp, struct fy_node *fynm)
{
	struct fy_node_pair *fynpi, *fynpn;

	if (!fyd)
		return -1;

	fyd_error_check(fyd,
			fyn && fynp && fynm && fyn->type == FYNT_MAPPING && fynm->type == FYNT_MAPPING,
			err_out, "bad inputs to %s", __func__);

	for (fynpi = fy_node_pair_list_head(&fynm->mapping); fynpi;
		fynpi = fy_node_pair_next(&fynm->mapping, fynpi)) {

		/* if we don't allow duplicate keys */
		if (!(fyd->parse_cfg.flags & FYPCF_ALLOW_DUPLICATE_KEYS)) {

			/* make sure we don't override an already existing key */
			if (fy_node_mapping_key_is_duplicate(fyn, fynpi->key))
				continue;
		}

		fynpn = fy_node_pair_alloc(fyd);
		fyd_error_check(fyd, fynpn, err_out,
				"fy_node_pair_alloc() failed");

		fynpn->key = fy_node_copy(fyd, fynpi->key);
		fynpn->value = fy_node_copy(fyd, fynpi->value);

		fy_node_pair_list_insert_after(&fyn->mapping, fynp, fynpn);
		if (fyn->xl)
			fy_accel_insert(fyn->xl, fynpn->key, fynpn);
	}

	return 0;

err_out:
	return -1;
}

static int fy_resolve_merge_key(struct fy_document *fyd, struct fy_node *fyn, struct fy_node_pair *fynp)
{
	struct fy_node *fynv, *fyni, *fynm;
	int rc;

	/* it must be a valid merge key value */
	FYD_NODE_ERROR_CHECK(fyd, fynp->value, FYEM_DOC,
			fy_node_pair_is_valid_merge_key(fyd, fynp), err_out,
			"invalid merge key value");

	fynv = fynp->value;
	fynm = fy_alias_get_merge_mapping(fyd, fynv);
	if (fynm) {
		rc = fy_resolve_merge_key_populate(fyd, fyn, fynp, fynm);
		fyd_error_check(fyd, !rc, err_out_rc,
				"fy_resolve_merge_key_populate() failed");

		return 0;
	}

	/* it must be a sequence then */
	fyd_error_check(fyd, fynv->type == FYNT_SEQUENCE, err_out,
			"invalid node type to use for merge key");

	/* the sequence must only contain valid aliases for mapping */
	for (fyni = fy_node_list_head(&fynv->sequence); fyni;
			fyni = fy_node_next(&fynv->sequence, fyni)) {

		fynm = fy_alias_get_merge_mapping(fyd, fyni);
		fyd_error_check(fyd, fynm, err_out,
				"invalid merge key sequence item (not an alias)");

		rc = fy_resolve_merge_key_populate(fyd, fyn, fynp, fynm);
		fyd_error_check(fyd, !rc, err_out_rc,
				"fy_resolve_merge_key_populate() failed");
	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

/* the anchors are scalars that have the FYNS_ALIAS style */
static int fy_resolve_anchor_node(struct fy_document *fyd, struct fy_node *fyn)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp, *fynpi, *fynpit;
	int rc, ret_rc = 0;
	struct fy_token *fyt;

	if (!fyn)
		return 0;

	if (fy_node_is_alias(fyn))
		return fy_resolve_alias(fyd, fyn);

	if (fyn->type == FYNT_SEQUENCE) {

		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni)) {

			rc = fy_resolve_anchor_node(fyd, fyni);
			if (rc && !ret_rc)
				ret_rc = rc;
		}

	} else if (fyn->type == FYNT_MAPPING) {

		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp; fynp = fynpi) {

			fynpi = fy_node_pair_next(&fyn->mapping, fynp);

			if (fy_node_pair_is_merge_key(fynp)) {
				rc = fy_resolve_merge_key(fyd, fyn, fynp);
				if (rc && !ret_rc)
					ret_rc = rc;

				/* remove this node pair */
				if (!rc) {
					fy_node_pair_list_del(&fyn->mapping, fynp);
					if (fyn->xl)
						fy_accel_remove(fyn->xl, fynp->key);
					fy_node_pair_detach_and_free(fynp);
				}

			} else {

				rc = fy_resolve_anchor_node(fyd, fynp->key);

				if (!rc) {

					/* check whether the keys are duplicate */
					for (fynpit = fy_node_pair_list_head(&fyn->mapping); fynpit;
						fynpit = fy_node_pair_next(&fyn->mapping, fynpit)) {

						/* skip this node pair */
						if (fynpit == fynp)
							continue;

						if (!fy_node_compare(fynpit->key, fynp->key))
							continue;

						/* whoops, duplicate key after resolution */
						fyt = NULL;
						switch (fyn->type) {
						case FYNT_SCALAR:
							fyt = fyn->scalar;
							break;
						case FYNT_SEQUENCE:
							fyt = fyn->sequence_start;
							break;
						case FYNT_MAPPING:
							fyt = fyn->mapping_start;
							break;
						}

						FYD_TOKEN_ERROR_CHECK(fyd, fyt, FYEM_DOC,
								false, err_out,
								"duplicate key after resolving");

					}

				}

				if (rc && !ret_rc)
					ret_rc = rc;

				rc = fy_resolve_anchor_node(fyd, fynp->value);
				if (rc && !ret_rc)
					ret_rc = rc;

			}
		}
	}

	return ret_rc;

err_out:
	return -1;
}

static void fy_resolve_parent_node(struct fy_document *fyd, struct fy_node *fyn, struct fy_node *fyn_parent)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp, *fynpi;

	if (!fyn)
		return;

	fyn->parent = fyn_parent;

	switch (fyn->type) {
	case FYNT_SCALAR:
		break;

	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni)) {

			fy_resolve_parent_node(fyd, fyni, fyn);
		}
		break;

	case FYNT_MAPPING:
		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp; fynp = fynpi) {

			fynpi = fy_node_pair_next(&fyn->mapping, fynp);

			fy_resolve_parent_node(fyd, fynp->key, fyn);
			fy_resolve_parent_node(fyd, fynp->value, fyn);
			fynp->parent = fyn;
		}
		break;
	}
}

typedef void (*fy_node_applyf)(struct fy_node *fyn);

void fy_node_apply(struct fy_node *fyn, fy_node_applyf func)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp;

	if (!fyn || !func)
		return;

	(*func)(fyn);

	switch (fyn->type) {
	case FYNT_SCALAR:
		break;

	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni))
			fy_node_apply(fyni, func);
		break;

	case FYNT_MAPPING:
		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp;
				fynp = fy_node_pair_next(&fyn->mapping, fynp)) {

			fy_node_apply(fynp->key, func);
			fy_node_apply(fynp->value, func);
		}
		break;
	}
}

static void clear_system_marks(struct fy_node *fyn)
{
	fyn->marks &= ~FYNWF_SYSTEM_MARKS;
}

/* clear all the system markers */
void fy_node_clear_system_marks(struct fy_node *fyn)
{
	fy_node_apply(fyn, clear_system_marks);
}

int fy_document_resolve(struct fy_document *fyd)
{
	int rc;
	bool ret;

	if (!fyd)
		return 0;

	fy_node_clear_system_marks(fyd->root);

	/* for resolution to work, no reference loops should exist */
	ret = fy_check_ref_loop(fyd, fyd->root,
			FYNWF_MAXDEPTH_DEFAULT | FYNWF_FOLLOW, NULL);

	fy_node_clear_system_marks(fyd->root);

	if (ret)
		goto err_out;


	/* now resolve any anchor nodes */
	rc = fy_resolve_anchor_node(fyd, fyd->root);
	if (rc)
		goto err_out_rc;

	/* redo parent resolution */
	fy_resolve_parent_node(fyd, fyd->root, NULL);

	return 0;

err_out:
	rc = -1;
err_out_rc:
	fyd->diag->on_error = false;
	return rc;
}

void fy_document_free_nodes(struct fy_document *fyd)
{
	struct fy_document *fyd_child;

	for (fyd_child = fy_document_list_first(&fyd->children); fyd_child;
	     fyd_child = fy_document_next(&fyd->children, fyd_child))
		fy_document_free_nodes(fyd_child);

	fy_node_detach_and_free(fyd->root);
	fyd->root = NULL;
}

void fy_document_destroy(struct fy_document *fyd)
{
	struct fy_document *fyd_child;

	/* both the document and the parser object must exist */
	if (!fyd)
		return;

	/* we have to free the nodes first */
	fy_document_free_nodes(fyd);

	/* recursively delete children */
	while ((fyd_child = fy_document_list_pop(&fyd->children)) != NULL) {
		fyd_child->parent = NULL;
		fy_document_destroy(fyd_child);
	}

	fy_parse_document_destroy(NULL, fyd);
}

int fy_document_set_parent(struct fy_document *fyd, struct fy_document *fyd_child)
{

	if (!fyd || !fyd_child || fyd_child->parent)
		return -1;

	fyd_child->parent = fyd;
	fy_document_list_add_tail(&fyd->children, fyd_child);

	return 0;
}

static const struct fy_parse_cfg doc_parse_default_cfg = {
	.flags = FYPCF_DEFAULT_DOC,
};

struct fy_document *fy_document_create(const struct fy_parse_cfg *cfg)
{
	struct fy_document *fyd = NULL;
	struct fy_diag *diag;
	int rc;

	if (!cfg)
		cfg = &doc_parse_default_cfg;

	fyd = malloc(sizeof(*fyd));
	if (!fyd)
		goto err_out;

	memset(fyd, 0, sizeof(*fyd));
	fyd->parse_cfg = *cfg;

	diag = cfg->diag;
	if (!diag) {
		diag = fy_diag_create(NULL);
		if (!diag)
			goto err_out;
	} else
		fy_diag_ref(diag);

	fyd->diag = diag;

	fy_anchor_list_init(&fyd->anchors);
	if (fy_document_is_accelerated(fyd)) {
		fyd->axl = malloc(sizeof(*fyd->axl));
		fyd_error_check(fyd, fyd->axl, err_out,
				"malloc() failed");

		/* start with a very small bucket list */
		rc = fy_accel_setup(fyd->axl, &hd_anchor, fyd, 8);
		fyd_error_check(fyd, !rc, err_out,
				"fy_accel_setup() failed");

		fyd->naxl = malloc(sizeof(*fyd->naxl));
		fyd_error_check(fyd, fyd->axl, err_out,
				"malloc() failed");

		/* start with a very small bucket list */
		rc = fy_accel_setup(fyd->naxl, &hd_nanchor, fyd, 8);
		fyd_error_check(fyd, !rc, err_out,
				"fy_accel_setup() failed");
	}
	fyd->root = NULL;

	/* we don't do document create version setting,
	 * perhaps we should in the future
	 */
	fyd->fyds = fy_document_state_default(NULL, NULL);
	fyd_error_check(fyd, fyd->fyds, err_out,
			"fy_document_state_default() failed");

	/* turn on JSON mode if it's forced */
	fyd->fyds->json_mode = (cfg->flags &
			(FYPCF_JSON_MASK << FYPCF_JSON_SHIFT)) == FYPCF_JSON_FORCE;

	fy_document_list_init(&fyd->children);

	return fyd;

err_out:
	fy_parse_document_destroy(NULL, fyd);
	return NULL;
}

struct fy_document_build_string_ctx {
	const char *str;
	size_t len;
};

static int parser_setup_from_string(struct fy_parser *fyp, void *user)
{
	struct fy_document_build_string_ctx *ctx = user;

	return fy_parser_set_string(fyp, ctx->str, ctx->len);
}

struct fy_document_build_malloc_string_ctx {
	char *str;
	size_t len;
};

static int parser_setup_from_malloc_string(struct fy_parser *fyp, void *user)
{
	struct fy_document_build_malloc_string_ctx *ctx = user;

	return fy_parser_set_malloc_string(fyp, ctx->str, ctx->len);
}

struct fy_document_build_file_ctx {
	const char *file;
};

static int parser_setup_from_file(struct fy_parser *fyp, void *user)
{
	struct fy_document_build_file_ctx *ctx = user;

	return fy_parser_set_input_file(fyp, ctx->file);
}

struct fy_document_build_fp_ctx {
	const char *name;
	FILE *fp;
};

static int parser_setup_from_fp(struct fy_parser *fyp, void *user)
{
	struct fy_document_build_fp_ctx *ctx = user;

	return fy_parser_set_input_fp(fyp, ctx->name, ctx->fp);
}

struct fy_document_vbuildf_ctx {
	const char *fmt;
	va_list ap;
};

static int parser_setup_from_fmt_ap(struct fy_parser *fyp, void *user)
{
	struct fy_document_vbuildf_ctx *vctx = user;
	va_list ap, ap_orig;
	int size, sizew;
	char *buf;

	/* first try without allocating */
	va_copy(ap_orig, vctx->ap);
	size = vsnprintf(NULL, 0, vctx->fmt, ap_orig);
	va_end(ap_orig);

	fyp_error_check(fyp, size >= 0, err_out,
			"vsnprintf() failed");

	buf = malloc(size + 1);
	fyp_error_check(fyp, buf, err_out,
			"malloc() failed");

	va_copy(ap, vctx->ap);
	sizew = vsnprintf(buf, size + 1, vctx->fmt, ap);
	fyp_error_check(fyp, sizew == size, err_out,
			"vsnprintf() failed");
	va_end(ap);

	buf[size] = '\0';

	return fy_parser_set_malloc_string(fyp, buf, size);

err_out:
	return -1;
}

static struct fy_document *fy_document_build_internal(const struct fy_parse_cfg *cfg,
		int (*parser_setup)(struct fy_parser *fyp, void *user),
		void *user)
{
	struct fy_parser fyp_data, *fyp = &fyp_data;
	struct fy_document *fyd = NULL;
	struct fy_eventp *fyep;
	bool got_stream_end;
	int rc;

	if (!parser_setup)
		return NULL;

	if (!cfg)
		cfg = &doc_parse_default_cfg;

	rc = fy_parse_setup(fyp, cfg);
	if (rc)
		return NULL;

	rc = (*parser_setup)(fyp, user);
	fyp_error_check(fyp, !rc, err_out,
			"parser_setup() failed");

	fyd = fy_parse_load_document(fyp);

	/* we're going to handle stream errors from now */
	if (!fyd)
		fyp->stream_error = false;

	/* if we collect diagnostics, we can continue */
	fyp_error_check(fyp, fyd || (fyp->cfg.flags & FYPCF_COLLECT_DIAG), err_out,
			"fy_parse_load_document() failed");

	/* no document, but we're collecting diagnostics */
	if (!fyd) {

		fyp_error(fyp, "fy_parse_load_document() failed");

		fyp->stream_error = false;
		fyd = fy_parse_document_create(fyp, NULL);
		fyp_error_check(fyp, fyd, err_out,
				"fy_parse_document_create() failed");
		fyd->parse_error = true;

		/* XXX */
		goto out;
	}

	got_stream_end = false;
	while (!got_stream_end && (fyep = fy_parse_private(fyp)) != NULL) {
		if (fyep->e.type == FYET_STREAM_END)
			got_stream_end = true;
		fy_parse_eventp_recycle(fyp, fyep);
	}

	if (got_stream_end) {
		fyep = fy_parse_private(fyp);
		fyp_error_check(fyp, !fyep, err_out,
				"more events after stream end");
		fy_parse_eventp_recycle(fyp, fyep);
	}

out:
	fy_parse_cleanup(fyp);
	return fyd;

err_out:
	fy_document_destroy(fyd);
	fy_parse_cleanup(fyp);
	return NULL;
}

struct fy_document *fy_document_build_from_string(const struct fy_parse_cfg *cfg,
						  const char *str, size_t len)
{
	struct fy_document_build_string_ctx ctx = {
		.str = str,
		.len = len,
	};

	return fy_document_build_internal(cfg, parser_setup_from_string, &ctx);
}

struct fy_document *fy_document_build_from_malloc_string(const struct fy_parse_cfg *cfg,
							 char *str, size_t len)
{
	struct fy_document_build_malloc_string_ctx ctx = {
		.str = str,
		.len = len,
	};

	return fy_document_build_internal(cfg, parser_setup_from_malloc_string, &ctx);
}

struct fy_document *fy_document_build_from_file(const struct fy_parse_cfg *cfg,
						const char *file)
{
	struct fy_document_build_file_ctx ctx = {
		.file = file,
	};

	return fy_document_build_internal(cfg, parser_setup_from_file, &ctx);
}

struct fy_document *fy_document_build_from_fp(const struct fy_parse_cfg *cfg,
					      FILE *fp)
{
	struct fy_document_build_fp_ctx ctx = {
		.name = NULL,
		.fp = fp,
	};

	return fy_document_build_internal(cfg, parser_setup_from_fp, &ctx);
}

enum fy_node_type fy_node_get_type(struct fy_node *fyn)
{
	/* a NULL is a plain scalar node */
	return fyn ? fyn->type : FYNT_SCALAR;
}

enum fy_node_style fy_node_get_style(struct fy_node *fyn)
{
	/* a NULL is a plain scalar node */
	return fyn ? fyn->style : FYNS_PLAIN;
}

void fy_node_set_style(struct fy_node *fyn, enum fy_node_style style)
{
	if (!fyn)
		return;

	/* ignore alias nodes to save document structure */
	if (fyn->style == FYNS_ALIAS)
		return;

	fyn->style = style;
}

bool fy_node_is_null(struct fy_node *fyn)
{
	if (!fyn)
		return true;

	if (fyn->type != FYNT_SCALAR)
		return false;

	return fyn->scalar == NULL || fyn->scalar->scalar.is_null;
}

bool fy_node_is_attached(struct fy_node *fyn)
{
	return fyn ? fyn->attached : false;
}

struct fy_node *fy_node_get_parent(struct fy_node *fyn)
{
	return fyn && !fyn->key_root ? fyn->parent : NULL;
}

struct fy_node *fy_node_get_document_parent(struct fy_node *fyn)
{
	return fyn ? fyn->parent : NULL;
}

struct fy_token *fy_node_get_tag_token(struct fy_node *fyn)
{
	return fyn ? fyn->tag : NULL;
}

struct fy_token *fy_node_get_scalar_token(struct fy_node *fyn)
{
	return fyn && fyn->type == FYNT_SCALAR ? fyn->scalar : NULL;
}

struct fy_node *fy_node_pair_key(struct fy_node_pair *fynp)
{
	return fynp ? fynp->key : NULL;
}

struct fy_node *fy_node_pair_value(struct fy_node_pair *fynp)
{
	return fynp ? fynp->value : NULL;
}

int fy_node_pair_set_key(struct fy_node_pair *fynp, struct fy_node *fyn)
{
	struct fy_node *fyn_map;
	struct fy_node_pair *fynpi;

	if (!fynp)
		return -1;

	/* the node must not be attached */
	if (fyn && fyn->attached)
		return -1;

	/* sanity check and duplication check */
	fyn_map = fynp->parent;
	if (fyn_map) {

		/* (in)sanity check */
		if (!fy_node_is_mapping(fyn_map))
			return -1;

		if (fyn_map->xl) {
			/* either we're already on the parent list (and it's OK) */
			/* or we're not and we have a duplicate key */
			fynpi = fy_node_accel_lookup_by_node(fyn_map, fyn);
			if (fynpi && fynpi != fynp)
				return -1;
			/* remove that key */
			fy_accel_remove(fyn_map->xl, fynp->key);
		} else {
			/* check whether the key is a duplicate
			* skipping ourselves since our key gets replaced
			*/
			for (fynpi = fy_node_pair_list_head(&fyn_map->mapping); fynpi;
				fynpi = fy_node_pair_next(&fyn_map->mapping, fynpi)) {

				if (fynpi != fynp && fy_node_compare(fynpi->key, fyn))
					return -1;
			}
		}

		fy_node_mark_synthetic(fyn_map);
	}

	fy_node_detach_and_free(fynp->key);
	fynp->key = fyn;

	if (fyn_map && fyn_map->xl)
		fy_accel_insert(fyn_map->xl, fynp->key, fynp);

	fyn->attached = true;

	return 0;
}

int fy_node_pair_set_value(struct fy_node_pair *fynp, struct fy_node *fyn)
{
	if (!fynp)
		return -1;
	/* the node must not be attached */
	if (fyn && fyn->attached)
		return -1;
	fy_node_detach_and_free(fynp->value);
	fynp->value = fyn;
	fyn->attached = true;

	if (fynp->parent)
		fy_node_mark_synthetic(fynp->parent);

	return 0;
}

struct fy_node *fy_document_root(struct fy_document *fyd)
{
	return fyd->root;
}

const char *fy_node_get_tag(struct fy_node *fyn, size_t *lenp)
{
	size_t tmplen;

	if (!lenp)
		lenp = &tmplen;

	if (!fyn || !fyn->tag) {
		*lenp = 0;
		return NULL;
	}

	return fy_token_get_text(fyn->tag, lenp);
}

const char *fy_node_get_scalar(struct fy_node *fyn, size_t *lenp)
{
	size_t tmplen;

	if (!lenp)
		lenp = &tmplen;

	if (!fyn || fyn->type != FYNT_SCALAR) {
		*lenp = 0;
		return NULL;
	}

	return fy_token_get_text(fyn->scalar, lenp);
}

const char *fy_node_get_scalar0(struct fy_node *fyn)
{
	if (!fyn || fyn->type != FYNT_SCALAR)
		return NULL;

	return fy_token_get_text0(fyn->scalar);
}

size_t fy_node_get_scalar_length(struct fy_node *fyn)
{

	if (!fyn || fyn->type != FYNT_SCALAR)
		return 0;

	return fy_token_get_text_length(fyn->scalar);
}

size_t fy_node_get_scalar_utf8_length(struct fy_node *fyn)
{

	if (!fyn || fyn->type != FYNT_SCALAR)
		return 0;

	return fy_token_format_utf8_length(fyn->scalar);
}

struct fy_node *fy_node_sequence_iterate(struct fy_node *fyn, void **prevp)
{
	if (!fyn || fyn->type != FYNT_SEQUENCE || !prevp)
		return NULL;

	return *prevp = *prevp ? fy_node_next(&fyn->sequence, *prevp) : fy_node_list_head(&fyn->sequence);
}

struct fy_node *fy_node_sequence_reverse_iterate(struct fy_node *fyn, void **prevp)
{
	if (!fyn || fyn->type != FYNT_SEQUENCE || !prevp)
		return NULL;

	return *prevp = *prevp ? fy_node_prev(&fyn->sequence, *prevp) : fy_node_list_tail(&fyn->sequence);
}

int fy_node_sequence_item_count(struct fy_node *fyn)
{
	struct fy_node *fyni;
	int count;

	if (!fyn || fyn->type != FYNT_SEQUENCE)
		return -1;

	count = 0;
	for (fyni = fy_node_list_head(&fyn->sequence); fyni; fyni = fy_node_next(&fyn->sequence, fyni))
		count++;
	return count;
}

bool fy_node_sequence_is_empty(struct fy_node *fyn)
{
	return !fyn || fyn->type != FYNT_SEQUENCE || fy_node_list_empty(&fyn->sequence);
}

struct fy_node *fy_node_sequence_get_by_index(struct fy_node *fyn, int index)
{
	struct fy_node *fyni;
	void *iterp = NULL;

	if (!fyn || fyn->type != FYNT_SEQUENCE)
		return NULL;

	if (index >= 0) {
		do {
			fyni = fy_node_sequence_iterate(fyn, &iterp);
		} while (fyni && --index >= 0);
	} else {
		do {
			fyni = fy_node_sequence_reverse_iterate(fyn, &iterp);
		} while (fyni && ++index < 0);
	}

	return fyni;
}

struct fy_node_pair *fy_node_mapping_iterate(struct fy_node *fyn, void **prevp)
{
	if (!fyn || fyn->type != FYNT_MAPPING || !prevp)
		return NULL;

	return *prevp = *prevp ? fy_node_pair_next(&fyn->mapping, *prevp) : fy_node_pair_list_head(&fyn->mapping);
}

struct fy_node_pair *fy_node_mapping_reverse_iterate(struct fy_node *fyn, void **prevp)
{
	if (!fyn || fyn->type != FYNT_MAPPING || !prevp)
		return NULL;

	return *prevp = *prevp ? fy_node_pair_prev(&fyn->mapping, *prevp) : fy_node_pair_list_tail(&fyn->mapping);
}

struct fy_node *fy_node_collection_iterate(struct fy_node *fyn, void **prevp)
{
	struct fy_node_pair *fynp;

	if (!fyn || !prevp)
		return NULL;

	switch (fyn->type) {
	case FYNT_SEQUENCE:
		return fy_node_sequence_iterate(fyn, prevp);

	case FYNT_MAPPING:
		fynp = fy_node_mapping_iterate(fyn, prevp);
		if (!fynp)
			return NULL;
		return fynp->value;

	case FYNT_SCALAR:
		fyn = !*prevp ? fyn : NULL;
		*prevp = fyn;
		return fyn;
	}

	return NULL;
}


int fy_node_mapping_item_count(struct fy_node *fyn)
{
	struct fy_node_pair *fynpi;
	int count;

	if (!fyn || fyn->type != FYNT_MAPPING)
		return -1;

	count = 0;
	for (fynpi = fy_node_pair_list_head(&fyn->mapping); fynpi; fynpi = fy_node_pair_next(&fyn->mapping, fynpi))
		count++;
	return count;
}

bool fy_node_mapping_is_empty(struct fy_node *fyn)
{
	return !fyn || fyn->type != FYNT_MAPPING || fy_node_pair_list_empty(&fyn->mapping);
}

struct fy_node_pair *fy_node_mapping_get_by_index(struct fy_node *fyn, int index)
{
	struct fy_node_pair *fynpi;
	void *iterp = NULL;

	if (!fyn || fyn->type != FYNT_MAPPING)
		return NULL;

	if (index >= 0) {
		do {
			fynpi = fy_node_mapping_iterate(fyn, &iterp);
		} while (fynpi && --index >= 0);
	} else {
		do {
			fynpi = fy_node_mapping_reverse_iterate(fyn, &iterp);
		} while (fynpi && ++index < 0);
	}

	return fynpi;
}

struct fy_node_pair *
fy_node_mapping_lookup_pair_by_simple_key(struct fy_node *fyn,
					  const char *key, size_t len)
{
	struct fy_node_pair *fynpi;
	struct fy_node *fyn_scalar;

	if (!fyn || fyn->type != FYNT_MAPPING || !key)
		return NULL;

	if (len == (size_t)-1)
		len = strlen(key);

	if (fyn->xl) {
		fyn_scalar = fy_node_create_scalar(fyn->fyd, key, len);
		if (!fyn_scalar)
			return NULL;

		fynpi = fy_node_accel_lookup_by_node(fyn, fyn_scalar);

		fy_node_free(fyn_scalar);

		if (fynpi)
			return fynpi;
	} else {
		for (fynpi = fy_node_pair_list_head(&fyn->mapping); fynpi;
			fynpi = fy_node_pair_next(&fyn->mapping, fynpi)) {

			if (!fy_node_is_scalar(fynpi->key) || fy_node_is_alias(fynpi->key))
				continue;

			if (!fynpi->key && len == 0)
				return fynpi;

			if (fynpi->key && !fy_token_memcmp(fynpi->key->scalar, key, len))
				return fynpi;
		}
	}

	return NULL;
}

struct fy_node *
fy_node_mapping_lookup_value_by_simple_key(struct fy_node *fyn,
					   const char *key, size_t len)
{
	struct fy_node_pair *fynp;

	fynp = fy_node_mapping_lookup_pair_by_simple_key(fyn, key, len);
	return fynp ? fy_node_pair_value(fynp) : NULL;
}

struct fy_node_pair *
fy_node_mapping_lookup_pair_by_null_key(struct fy_node *fyn)
{
	struct fy_node_pair *fynpi;

	if (!fyn || fyn->type != FYNT_MAPPING)
		return NULL;

	/* no acceleration for NULLs */
	for (fynpi = fy_node_pair_list_head(&fyn->mapping); fynpi;
		fynpi = fy_node_pair_next(&fyn->mapping, fynpi)) {

		if (fy_node_is_null(fynpi->key))
			return fynpi;
	}

	return NULL;
}

struct fy_node *
fy_node_mapping_lookup_value_by_null_key(struct fy_node *fyn)
{
	struct fy_node_pair *fynp;

	fynp = fy_node_mapping_lookup_pair_by_null_key(fyn);
	return fynp ? fy_node_pair_value(fynp) : NULL;
}

const char *
fy_node_mapping_lookup_scalar_by_simple_key(struct fy_node *fyn, size_t *lenp,
					    const char *key, size_t keylen)
{
	struct fy_node *fyn_value;

	fyn_value = fy_node_mapping_lookup_value_by_simple_key(fyn, key, keylen);
	if (!fyn_value || !fy_node_is_scalar(fyn_value))
		return NULL;
	return fy_node_get_scalar(fyn_value, lenp);
}

const char *
fy_node_mapping_lookup_scalar0_by_simple_key(struct fy_node *fyn,
					     const char *key, size_t keylen)
{
	struct fy_node *fyn_value;

	fyn_value = fy_node_mapping_lookup_value_by_simple_key(fyn, key, keylen);
	if (!fyn_value || !fy_node_is_scalar(fyn_value))
		return NULL;
	return fy_node_get_scalar0(fyn_value);
}

struct fy_node *fy_node_mapping_lookup_value_by_key(struct fy_node *fyn, struct fy_node *fyn_key)
{
	struct fy_node_pair *fynp;

	fynp = fy_node_mapping_lookup_pair(fyn, fyn_key);
	return fynp ? fynp->value : NULL;
}

struct fy_node *fy_node_mapping_lookup_key_by_key(struct fy_node *fyn, struct fy_node *fyn_key)
{
	struct fy_node_pair *fynp;

	fynp = fy_node_mapping_lookup_pair(fyn, fyn_key);
	return fynp ? fynp->key : NULL;
}

struct fy_node_pair *
fy_node_mapping_lookup_pair_by_string(struct fy_node *fyn, const char *key, size_t len)
{
	struct fy_document *fyd;
	struct fy_node_pair *fynp;

	/* try quick and dirty simple scan */
	if (is_simple_key(key, len))
		return fy_node_mapping_lookup_pair_by_simple_key(fyn, key, len);

	fyd = fy_document_build_from_string(NULL, key, len);
	if (!fyd)
		return NULL;

	fynp = fy_node_mapping_lookup_pair(fyn, fy_document_root(fyd));

	fy_document_destroy(fyd);

	return fynp;
}

struct fy_node *
fy_node_mapping_lookup_by_string(struct fy_node *fyn,
				 const char *key, size_t len)
{
	struct fy_node_pair *fynp;

	fynp = fy_node_mapping_lookup_pair_by_string(fyn, key, len);
	return fynp ? fynp->value : NULL;
}

struct fy_node *
fy_node_mapping_lookup_value_by_string(struct fy_node *fyn,
				       const char *key, size_t len)
{
	return fy_node_mapping_lookup_by_string(fyn, key, len);
}

struct fy_node *
fy_node_mapping_lookup_key_by_string(struct fy_node *fyn,
				     const char *key, size_t len)
{
	struct fy_node_pair *fynp;

	fynp = fy_node_mapping_lookup_pair_by_string(fyn, key, len);
	return fynp ? fynp->key : NULL;
}

bool fy_node_is_empty(struct fy_node *fyn)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp;
	struct fy_atom *atom;

	/* skip if no value node or token */
	if (!fyn)
		return true;

	switch (fyn->type) {
	case FYNT_SCALAR:
		atom = fy_token_atom(fyn->scalar);
		if (atom && !atom->size0 && !atom->empty)
			return false;
		break;
	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni)) {
			if (!fy_node_is_empty(fyni))
				return false;
		}
		break;
	case FYNT_MAPPING:
		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp;
				fynp = fy_node_pair_next(&fyn->mapping, fynp)) {
			if (!fy_node_is_empty(fynp->value))
				return false;
		}
		break;
	}
	return true;
}

#define fy_node_walk_ctx_create_a(_max_depth, _mark, _res) \
	do { \
		unsigned int __max_depth = (_max_depth); \
		struct fy_node_walk_ctx *_ctx; \
		\
		_ctx = FY_ALLOCA(sizeof(*_ctx) + sizeof(struct fy_node *) * __max_depth); \
		_ctx->max_depth = _max_depth; \
		_ctx->next_slot = 0; \
		_ctx->mark = (_mark); \
		*(_res) = _ctx; \
	} while(false)

static inline void fy_node_walk_mark_start(struct fy_node_walk_ctx *ctx)
{
	ctx->next_slot = 0;
}

static inline void fy_node_walk_mark_end(struct fy_node_walk_ctx *ctx)
{
	struct fy_node *fyn;

	while (ctx->next_slot > 0) {
		fyn = ctx->marked[--ctx->next_slot];
		fyn->marks &= ~ctx->mark;
	}
}

/* fyn is guaranteed to be non NULL and an alias */
static inline bool fy_node_walk_mark(struct fy_node_walk_ctx *ctx, struct fy_node *fyn)
{
	struct fy_document *fyd = fyn->fyd;
	struct fy_token *fyt = NULL;

	switch (fyn->type) {
	case FYNT_SCALAR:
		fyt = fyn->scalar;
		break;
	case FYNT_SEQUENCE:
		fyt = fyn->sequence_start;
		break;
	case FYNT_MAPPING:
		fyt = fyn->mapping_start;
		break;
	}

	/* depth error */
	FYD_TOKEN_ERROR_CHECK(fyd, fyt, FYEM_DOC,
		ctx->next_slot < ctx->max_depth, err_out,
		"max recursion depth exceeded (%u)", ctx->max_depth);

	/* mark found, loop */
	FYD_TOKEN_ERROR_CHECK(fyd, fyt, FYEM_DOC,
		!(fyn->marks & ctx->mark), err_out,
		"cyclic reference detected");

	fyn->marks |= ctx->mark;
	ctx->marked[ctx->next_slot++] = fyn;

	return true;

err_out:
	return false;
}

static struct fy_node *
fy_node_follow_aliases(struct fy_node *fyn, enum fy_node_walk_flags flags, bool single)
{
	enum fy_node_walk_flags ptr_flags;
	struct fy_ptr_node_list nl;
	struct fy_ptr_node *fypn;

	if (!fyn || !fy_node_is_alias(fyn) || !(flags & FYNWF_FOLLOW))
		return fyn;

	ptr_flags = flags & FYNWF_PTR(FYNWF_PTR_MASK);
	if (ptr_flags != FYNWF_PTR_YAML && ptr_flags != FYNWF_PTR_YPATH)
		return fyn;

	fy_ptr_node_list_init(&nl);

	while (fyn && fy_node_is_alias(fyn)) {

		// fprintf(stderr, "%s: %s\n", __func__, fy_node_get_path_FY_ALLOCA(fyn));

		/* check for loops */
		if (fy_ptr_node_list_contains(&nl, fyn)) {
			fyn = NULL;
			break;
		}

		/* out of memory? */
		fypn = fy_ptr_node_create(fyn);
		if (!fypn) {
			fyn = NULL;
			break;
		}
		fy_ptr_node_list_add_tail(&nl, fypn);

		fyn = fy_node_follow_alias(fyn, flags);

		if (single)
			break;
	}

	/* release */
	while ((fypn = fy_ptr_node_list_pop(&nl)) != NULL)
		fy_ptr_node_destroy(fypn);

	return fyn;
}

struct fy_node *fy_node_resolve_alias(struct fy_node *fyn)
{
	enum fy_node_walk_flags flags;

	if (!fyn)
		return NULL;

	flags = FYNWF_FOLLOW | FYNWF_MAXDEPTH_DEFAULT | FYNWF_MARKER_DEFAULT;
	if (fyn->fyd->parse_cfg.flags & FYPCF_YPATH_ALIASES)
		flags |= FYNWF_PTR_YPATH;
	else
		flags |= FYNWF_PTR_YAML;
	return fy_node_follow_aliases(fyn, flags, false);
}

struct fy_node *fy_node_dereference(struct fy_node *fyn)
{
	enum fy_node_walk_flags flags;

	if (!fyn || !fy_node_is_alias(fyn))
		return NULL;

	flags = FYNWF_FOLLOW | FYNWF_MAXDEPTH_DEFAULT | FYNWF_MARKER_DEFAULT;
	if (fyn->fyd->parse_cfg.flags & FYPCF_YPATH_ALIASES)
		flags |= FYNWF_PTR_YPATH;
	else
		flags |= FYNWF_PTR_YAML;
	return fy_node_follow_aliases(fyn, flags, true);
}

static struct fy_node *
fy_node_by_path_internal(struct fy_node *fyn,
			 const char *path, size_t pathlen,
		         enum fy_node_walk_flags flags)
{
	enum fy_node_walk_flags ptr_flags;
	struct fy_node *fynt, *fyni;
	const char *s, *e, *ss, *ee;
	char *end_idx, *json_key, *t, *p, *uri_path;
	char c;
	int idx, rlen;
	size_t len, json_key_len, uri_path_len;
	bool has_json_key_esc;
	uint8_t code[4];
	int code_length;
	bool trailing_slash;

	if (!fyn || !path)
		return NULL;

	ptr_flags = flags & FYNWF_PTR(FYNWF_PTR_MASK);
	if (ptr_flags == FYNWF_PTR_YPATH)
		return fy_node_by_ypath(fyn, path, pathlen);

	s = path;
	if (pathlen == (size_t)-1)
		pathlen = strlen(path);
	e = s + pathlen;

	/* a trailing slash works just like unix and symbolic links
	 * if it does not exist no symbolic link lookups are performed
	 * at the end of the operation.
	 * if it exists they are followed upon resolution
	 */
	trailing_slash = pathlen > 0 && path[pathlen - 1] == '/';

	/* and continue on path lookup with the rest */

	/* skip all prefixed / */
	switch (ptr_flags) {
	default:
	case FYNWF_PTR_YAML:
		while (s < e && *s == '/')
			s++;
		/* for a last component / always match this one */
		if (s >= e)
			goto out;
		break;

	case FYNWF_PTR_JSON:
		/* "" -> everything here */
		if (s == e)
			return fyn;
		/* it must have a separator here */
		if (*s != '/')
			return NULL;
		s++;
		break;

	case FYNWF_PTR_RELJSON:
		break;
	}

	/* fyd_notice(fyn->fyd, "%s:%d following alias @%s \"%.*s\"",
			__func__, __LINE__, fy_node_get_path(fyn), (int)(e - s), s); */
	fyn = fy_node_follow_aliases(fyn, flags, true);

	/* scalar can be only last element in the path (it has no key) */
	if (fy_node_is_scalar(fyn)) {
		if (*s)
			fyn = NULL;	/* not end of the path - fail */
		goto out;
	}

	/* for a sequence the only allowed key is [n] where n is the index to follow */
	if (fy_node_is_sequence(fyn)) {

		c = -1;
		switch (ptr_flags) {
		default:
		case FYNWF_PTR_YAML:
			while (s < e && isspace(*s))
				s++;

			c = *s;
			if (c == '[')
				s++;
			else if (!isdigit(c) && c != '-')
				return NULL;

			idx = (int)strtol(s, &end_idx, 10);

			/* no digits found at all */
			if (idx == 0 && end_idx == s)
				return NULL;

			s = end_idx;

			while (s < e && isspace(*s))
				s++;

			if (c == '[' && *s++ != ']')
				return NULL;

			while (s < e && isspace(*s))
				s++;

			break;

		case FYNWF_PTR_JSON:
		case FYNWF_PTR_RELJSON:

			/* special array end - always fails */
			if (*s == '-')
				return NULL;

			idx = (int)strtol(s, &end_idx, 10);

			/* no digits found at all */
			if (idx == 0 && end_idx == s)
				return NULL;

			/* no negatives */
			if (idx < 0)
				return NULL;

			s = end_idx;

			if (s < e && *s == '/')
				s++;

			break;
		}

		len = e - s;

		fyn = fy_node_sequence_get_by_index(fyn, idx);
		if (trailing_slash)
			fyn = fy_node_follow_aliases(fyn, flags, false);
		fyn = fy_node_by_path_internal(fyn, s, len, flags);
		goto out;
	}

	/* be a little bit paranoid */
	assert(fy_node_is_mapping(fyn));

	path = s;
	pathlen = (size_t)(e - s);

	switch (ptr_flags) {
	default:
	case FYNWF_PTR_YAML:

		/* scan ahead for the end of the path component
		* note that we don't do UTF8 here, because all the
		* escapes are regular ascii characters, i.e.
		* '/', '*', '&', '.', '{', '}', '[', ']' and '\\'
		*/

		while (s < e) {
			c = *s;
			/* end of path component? */
			if (c == '/')
				break;
			s++;

			if (c == '\\') {
				/* it must be a valid escape */
				if (s >= e || !strchr("/*&.{}[]\\", *s))
					return NULL;
				s++;
			} else if (c == '"') {
				while (s < e && *s != '"') {
					c = *s++;
					if (c == '\\' && (s < e && *s == '"'))
						s++;
				}
				/* not a normal double quote end */
				if (s >= e || *s != '"')
					return NULL;
				s++;
			} else if (c == '\'') {
				while (s < e && *s != '\'') {
					c = *s++;
					if (c == '\'' && (s < e && *s == '\''))
						s++;
				}
				/* not a normal single quote end */
				if (s >= e || *s != '\'')
					return NULL;
				s++;
			}
		}
		len = s - path;

		fynt = fyn;
		fyn = fy_node_mapping_lookup_by_string(fyn, path, len);

		/* failed! last ditch attempt, is there a merge key? */
		if (!fyn && fynt && (flags & FYNWF_FOLLOW) && ptr_flags == FYNWF_PTR_YAML) {
			fyn = fy_node_mapping_lookup_by_string(fynt, "<<", 2);
			if (!fyn)
				goto out;

			if (fy_node_is_alias(fyn)) {

				/* single alias '<<: *foo' */
				fyn = fy_node_mapping_lookup_by_string(
						fy_node_follow_aliases(fyn, flags, false), path, len);

			} else if (fy_node_is_sequence(fyn)) {

				/* multi aliases '<<: [ *foo, *bar ]' */
				fynt = fyn;
				for (fyni = fy_node_list_head(&fynt->sequence); fyni;
						fyni = fy_node_next(&fynt->sequence, fyni)) {
					if (!fy_node_is_alias(fyni))
						continue;
					fyn = fy_node_mapping_lookup_by_string(
							fy_node_follow_aliases(fyni, flags, false),
							path, len);
					if (fyn)
						break;
				}
			} else
				fyn = NULL;
		}
		break;

	case FYNWF_PTR_JSON:
	case FYNWF_PTR_RELJSON:

		has_json_key_esc = false;
		while (s < e) {
			c = *s;
			/* end of path component? */
			if (c == '/')
				break;
			s++;
			if (c == '~')
				has_json_key_esc = true;
		}
		len = s - path;

		if (has_json_key_esc) {
			/* note that the escapes reduce the length, so allocating the
			 * same size is guaranteed safe */
			json_key = FY_ALLOCA(len + 1);

			ss = path;
			ee = s;
			t = json_key;
			while (ss < ee) {
				if (*ss != '~') {
					*t++ = *ss++;
					continue;
				}
				/* unterminated ~ escape, or neither ~0, ~1 */
				if (ss + 1 >= ee || (ss[1] < '0' && ss[1] > '1'))
					return NULL;
				*t++ = ss[1] == '0' ? '~' : '/';
				ss += 2;
			}
			json_key_len = t - json_key;

			path = json_key;
			len = json_key_len;
		}

		/* URI encoded escaped */
		if ((flags & FYNWF_URI_ENCODED) && memchr(path, '%', len)) {
			/* escapes shrink, so safe to allocate as much */
			uri_path = FY_ALLOCA(len + 1);

			ss = path;
			ee = path + len;
			t = uri_path;
			while (ss < ee) {
				/* copy run until '%' or end */
				p = memchr(ss, '%', ee - ss);
				rlen = (p ? p : ee) - ss;
				memcpy(t, ss, rlen);
				ss += rlen;
				t += rlen;

				/* if end, break */
				if (!p)
					break;

				/* collect a utf8 character sequence */
				code_length = sizeof(code);
				ss = fy_uri_esc(ss, ee - ss, code, &code_length);
				if (!ss) {
					/* bad % escape sequence */
					return NULL;
				}
				memcpy(t, code, code_length);
				t += code_length;
			}
			uri_path_len = t - uri_path;

			path = uri_path;
			len = uri_path_len;
		}

		fynt = fyn;
		fyn = fy_node_mapping_lookup_value_by_simple_key(fyn, path, len);
		break;
	}

	len = e - s;

	if (len > 0 && trailing_slash) {
		/* fyd_notice(fyn->fyd, "%s:%d following alias @%s \"%.*s\"",
				__func__, __LINE__, fy_node_get_path(fyn), (int)(e - s), s); */
		fyn = fy_node_follow_aliases(fyn, flags, true);
	}

	fyn = fy_node_by_path_internal(fyn, s, len, flags);

out:
	len = e - s;

	if (len > 0 && trailing_slash) {
		/* fyd_notice(fyn->fyd, "%s:%d following alias @%s \"%.*s\"",
				__func__, __LINE__, fy_node_get_path(fyn), (int)(e - s), s); */
		fyn = fy_node_follow_aliases(fyn, flags, true);
	}

	return fyn;
}

struct fy_node *fy_node_by_path(struct fy_node *fyn,
				const char *path, size_t len,
				enum fy_node_walk_flags flags)
{
	struct fy_document *fyd;
	struct fy_anchor *fya;
	const char *s, *e, *t, *anchor;
	size_t alen;
	char c;
	int idx, w;
	char *end_idx;

	if (!fyn || !path)
		return NULL;

	if (len == (size_t)-1)
		len = strlen(path);

	/* verify that the path string is well formed UTF8 */
	s = path;
	e = s + len;

	fyd = fyn->fyd;
	while (s < e) {
		c = fy_utf8_get(s, e - s, &w);
		if (c < 0) {
			fyd_error(fyd, "fy_node_by_path() malformed path string\n");
			return NULL;
		}
		s += w;
	}

	/* rewind */
	s = path;

	/* if it's a YPATH, just punt to that method */
	if ((flags & FYNWF_PTR(FYNWF_PTR_MASK)) == FYNWF_PTR_YPATH)
		return fy_node_by_ypath(fyn, path, len);

	/* fyd_notice(fyn->fyd, "%s: %.*s", __func__, (int)(len), s); */

	/* first path component may be an alias */
	if ((flags & FYNWF_FOLLOW) && fyn && path) {
		while (s < e && isspace(*s))
			s++;

		if (s >= e || *s != '*')
			goto regular_path_lookup;

		s++;

		c = -1;
		for (t = s; t < e; t++) {
			c = *t;
			/* it ends on anything non alias */
			if (c == '[' || c == ']' ||
				c == '{' || c == '}' ||
				c == ',' || c == ' ' || c == '\t' ||
				c == '/')
				break;
		}

		/* bad alias form for path */
		if (c == '[' || c == ']' || c == '{' || c == '}' || c == ',')
			return NULL;

		anchor = s;
		alen = t - s;

		if (alen) {
			/* we must be terminated by '/' or space followed by '/' */
			/* strip until spaces and '/' end */
			while (t < e && (*t == ' ' || *t == '\t'))
				t++;

			while (t < e && *t == '/')
				t++;

			/* update path */
			path = t;
			len = e - t;

			/* fyd_notice(fyn->fyd, "%s: looking up anchor=%.*s", __func__, (int)(alen), anchor); */

			/* lookup anchor */
			fya = fy_document_lookup_anchor(fyn->fyd, anchor, alen);
			if (!fya) {
				/* fyd_notice(fyn->fyd, "%s: failed to lookup anchor=%.*s", __func__, (int)(alen), anchor); */
				return NULL;
			}

			/* fyd_notice(fyn->fyd, "%s: found anchor=%.*s at %s",
					__func__, (int)(alen), anchor, fy_node_get_path(fya->fyn)); */

			/* nothing more? we're done */
			if (*path == '\0')
				return fya->fyn;

			/* anchor found... all good */

			fyn = fya->fyn;
		} else {
			/* no anchor it must be of the form *\/ */

			path = s;
			len = e - s;
		}

		/* fyd_notice(fyn->fyd, "%s: continuing looking for %.*s",
				__func__, (int)(len), path); */

	}

regular_path_lookup:

	/* if it's a relative json pointer... */
	if ((flags & FYNWF_PTR(FYNWF_PTR_MASK)) == FYNWF_PTR_RELJSON) {

		/* it must at least be one digit */
		if (len == 0)
			return NULL;

		idx = (int)strtol(path, &end_idx, 10);

		/* at least one digit must exist */
		if (idx == 0 && path == end_idx)
			return NULL;

		e = path + len;
		len = e - end_idx;
		path = end_idx;

		/* we don't do the trailing # here */
		if (len == 1 && *path == '#')
			return NULL;

		while (idx-- > 0)
			fyn = fy_node_get_parent(fyn);

		/* convert to regular json pointer from now on */
		flags &= ~FYNWF_PTR(FYNWF_PTR_MASK);
		flags |= FYNWF_PTR_JSON;
	}

	return fy_node_by_path_internal(fyn, path, len, flags);
}

static char *
fy_node_get_reference_internal(struct fy_node *fyn_base, struct fy_node *fyn, bool near)
{
	struct fy_anchor *fya;
	const char *path;
	char *path2, *path3;
	const char *text;
	size_t len;

	if (!fyn)
		return NULL;

	path2 = NULL;

	/* if the node has an anchor use it (ie return *foo) */
	if (!fyn_base && (fya = fy_node_get_anchor(fyn)) != NULL) {
		text = fy_anchor_get_text(fya, &len);
		if (!text)
			return NULL;
		path2 = FY_ALLOCA(1 + len + 1);
		path2[0] = '*';
		memcpy(path2 + 1, text, len);
		path2[len + 1] = '\0';

	} else {

		fya = fyn_base ? fy_node_get_anchor(fyn_base) : NULL;
		if (!fya && near)
			fya = fy_node_get_nearest_anchor(fyn);
		if (!fya) {
			/* no anchor, direct reference (ie return *\/foo\/bar */
			fy_node_get_path_alloca(fyn, &path);
			if (!*path)
				return NULL;
			path2 = FY_ALLOCA(1 + strlen(path) + 1);
			path2[0] = '*';
			strcpy(path2 + 1, path);
		} else {
			text = fy_anchor_get_text(fya, &len);
			if (!text)
				return NULL;
			if (fy_anchor_node(fya) != fyn) {
				fy_node_get_path_relative_to_alloca(fy_anchor_node(fya), fyn, &path);
				if (*path) {
					/* we have a relative path */
					path2 = FY_ALLOCA(1 + len + 1 + strlen(path) + 1);
					path2[0] = '*';
					memcpy(path2 + 1, text, len);
					path2[len + 1] = '/';
					memcpy(1 + path2 + len + 1, path, strlen(path) + 1);
				} else {
					/* absolute path */
					fy_node_get_path_alloca(fyn, &path);
					if (!*path)
						return NULL;
					path2 = FY_ALLOCA(1 + strlen(path) + 1);
					path2[0] = '*';
					strcpy(path2 + 1, path);
				}
			} else {
				path2 = FY_ALLOCA(1 + len + 1);
				path2[0] = '*';
				memcpy(path2 + 1, text, len);
				path2[len + 1] = '\0';
			}
		}
	}

	if (!path2)
		return NULL;

	path3 = strdup(path2);
	if (!path3)
		return NULL;

	return path3;
}

char *fy_node_get_reference(struct fy_node *fyn)
{
	return fy_node_get_reference_internal(NULL, fyn, false);
}

struct fy_node *fy_node_create_reference(struct fy_node *fyn)
{
	struct fy_node *fyn_ref;
	char *path, *alias;

	path = fy_node_get_reference(fyn);
	if (!path)
		return NULL;

	alias = path;
	if (*alias == '*')
		alias++;

	fyn_ref = fy_node_create_alias_copy(fy_node_document(fyn), alias, FY_NT);

	free(path);

	return fyn_ref;
}

char *fy_node_get_relative_reference(struct fy_node *fyn_base, struct fy_node *fyn)
{
	return fy_node_get_reference_internal(fyn_base, fyn, false);
}

struct fy_node *fy_node_create_relative_reference(struct fy_node *fyn_base, struct fy_node *fyn)
{
	struct fy_node *fyn_ref;
	char *path, *alias;

	path = fy_node_get_relative_reference(fyn_base, fyn);
	if (!path)
		return NULL;

	alias = path;
	if (*alias == '*')
		alias++;

	fyn_ref = fy_node_create_alias_copy(fy_node_document(fyn), alias, FY_NT);

	free(path);

	return fyn_ref;
}

bool fy_check_ref_loop(struct fy_document *fyd, struct fy_node *fyn,
		       enum fy_node_walk_flags flags,
		       struct fy_node_walk_ctx *ctx)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp, *fynpi;
	struct fy_node_walk_ctx *ctxn;
	bool ret;

	if (!fyn)
		return false;

	/* visited? no need to check */
	if (fyn->marks & FY_BIT(FYNWF_VISIT_MARKER))
		return false;

	/* marked node, it's a loop */
	if (ctx && !fy_node_walk_mark(ctx, fyn))
		return true;

	ret = false;

	switch (fyn->type) {
	case FYNT_SCALAR:

		/* if it's not an alias, we're done */
		if (!fy_node_is_alias(fyn))
			break;

		ctxn = ctx;
		if (!ctxn) 
			fy_node_walk_ctx_create_a(
				fy_node_walk_max_depth_from_flags(flags), FYNWF_REF_MARKER, &ctxn);


		if (!ctx) {
			fy_node_walk_mark_start(ctxn);

			/* mark this node */
			fy_node_walk_mark(ctxn, fyn);
		}

		fyni = fy_node_follow_alias(fyn, flags);

		ret = fy_check_ref_loop(fyd, fyni, flags, ctxn);

		if (!ctx)
			fy_node_walk_mark_end(ctxn);

		if (ret)
			break;

		break;

	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni)) {

			ret = fy_check_ref_loop(fyd, fyni, flags, ctx);
			if (ret)
				break;
		}
		break;

	case FYNT_MAPPING:
		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp; fynp = fynpi) {

			fynpi = fy_node_pair_next(&fyn->mapping, fynp);

			ret = fy_check_ref_loop(fyd, fynp->key, flags, ctx);
			if (ret)
				break;

			ret = fy_check_ref_loop(fyd, fynp->value, flags, ctx);
			if (ret)
				break;
		}
		break;
	}

	/* mark as visited */
	fyn->marks |= FY_BIT(FYNWF_VISIT_MARKER);

	return ret;
}

char *fy_node_get_parent_address(struct fy_node *fyn)
{
	struct fy_node *parent, *fyni;
	struct fy_node_pair *fynp;
	struct fy_node *fyna;
	char *path = NULL;
	const char *str;
	size_t len;
	int idx;
	bool is_key_root;
	int ret;
	const char *fmt;
	char *new_path, *old_path;

	if (!fyn)
		return NULL;

	parent = fy_node_get_document_parent(fyn);
	if (!parent)
		return NULL;

	if (fy_node_is_sequence(parent)) {

		/* for a sequence, find the index */
		idx = 0;
		for (fyni = fy_node_list_head(&parent->sequence); fyni;
				fyni = fy_node_next(&parent->sequence, fyni)) {
			if (fyni == fyn)
				break;
			idx++;
		}

		if (!fyni)
			return NULL;

		ret = asprintf(&path, "%d", idx);
		if (ret == -1)
			return NULL;
	}

	if (fy_node_is_mapping(parent)) {

		is_key_root = fyn->key_root;

		idx = 0;
		fyna = NULL;
		for (fynp = fy_node_pair_list_head(&parent->mapping); fynp;
				fynp = fy_node_pair_next(&parent->mapping, fynp)) {

			if ((!is_key_root && fynp->value == fyn) || (is_key_root && fynp->key == fyn))
				break;
			idx++;
		}

		if (!fynp)
			return NULL;

		fyna = fynp->key;
		if (!fyna)
			return NULL;

		/* if key is a plain scalar try to not use a complex style (even for quoted) */
		if (fyna && fy_node_is_scalar(fyna) && !fy_node_is_alias(fyna) &&
				(str = fy_token_get_scalar_path_key(fyna->scalar, &len)) != NULL) {

			fmt = !is_key_root ? "%.*s" : ".key(%.*s)";
			ret = asprintf(&path, fmt, (int)len, str);
			if (ret == -1)
				return NULL;

		} else {

			/* something complex, emit it */
			path = fy_emit_node_to_string(fyna,
				FYECF_MODE_FLOW_ONELINE | FYECF_WIDTH_INF |
				FYECF_STRIP_LABELS	| FYECF_STRIP_TAGS |
				FYECF_NO_ENDING_NEWLINE);
			if (!path)
				return NULL;

			if (is_key_root) {
				old_path = path;
				ret = asprintf(&new_path, ".key(%s)", path);
				if (ret == -1) {
					free(path);
					return NULL;
				}
				free(old_path);
				path = new_path;
			}
		}
	}

	return path;
}

char *fy_node_get_path(struct fy_node *fyn)
{
	struct path_track {
		struct path_track *prev;
		char *path;
	};
	struct path_track *track, *newtrack;
	char *path, *s, *path_mem;
	size_t len;
	struct fy_node *parent;

	if (!fyn)
		return NULL;

	/* easy on the root */
	parent = fy_node_get_document_parent(fyn);
	if (!parent) {
		path_mem = strdup("/");
		return path_mem;
	}

	track = NULL;
	len = 0;
	while ((path = fy_node_get_parent_address(fyn))) {
		newtrack = FY_ALLOCA(sizeof(*newtrack));
		newtrack->prev = track;
		newtrack->path = path;

		track = newtrack;

		len += strlen(path) + 1;

		fyn = fy_node_get_document_parent(fyn);
	}
	len += 2;

	path_mem = malloc(len);

	s = path_mem;

	while (track) {
		len = strlen(track->path);
		if (s) {
			*s++ = '/';
			memcpy(s, track->path, len);
			s += len;
		}
		free(track->path);
		track = track->prev;
	}

	if (s)
		*s = '\0';

	return path_mem;
}

char *fy_node_get_path_relative_to(struct fy_node *fyn_parent, struct fy_node *fyn)
{
	char *path, *ppath, *path2, *path_ret;
	size_t pathlen, ppathlen;
	struct fy_node *ni, *nj;

	if (!fyn)
		return NULL;

	/* must be on the same document */
	if (fyn_parent && (fyn_parent->fyd != fyn->fyd))
		return NULL;

	if (!fyn_parent)
		fyn_parent = fyn->fyd->root;

	/* verify that it's a parent */
	ni = fyn;
	while ((nj = fy_node_get_parent(ni)) != NULL && nj != fyn_parent)
		ni = nj;

	/* not a parent, illegal */
	if (!nj)
		return NULL;

	/* here we go... */
	path = "";
	pathlen = 0;

	ni = fyn;
	while ((nj = fy_node_get_parent(ni)) != NULL) {
		ppath = fy_node_get_parent_address(ni);
		if (!ppath)
			return NULL;

		ppathlen = strlen(ppath);

		if (pathlen > 0) {
			path2 = FY_ALLOCA(pathlen + 1 + ppathlen + 1);
			memcpy(path2, ppath, ppathlen);
			path2[ppathlen] = '/';
			memcpy(path2 + ppathlen + 1, path, pathlen);
			path2[ppathlen + 1 + pathlen] = '\0';
		} else {
			path2 = FY_ALLOCA(ppathlen + 1);
			memcpy(path2, ppath, ppathlen);
			path2[ppathlen] = '\0';
		}

		path = path2;
		pathlen = strlen(path);

		free(ppath);
		ni = nj;

		if (ni == fyn_parent)
			break;
	}

	path_ret = strdup(path);
	return path_ret;
}

char *fy_node_get_short_path(struct fy_node *fyn)
{
	struct fy_node *fyn_anchor;
	struct fy_anchor *fya;
	const char *text;
	size_t len;
	const char *str;
	char *path;

	if (!fyn)
		return NULL;

	/* get the nearest anchor traversing upwards */
	fya = fy_node_get_nearest_anchor(fyn);
	if (!fya)
		return fy_node_get_path(fyn);

	fyn_anchor = fy_anchor_node(fya);

	text = fy_anchor_get_text(fya, &len);
	if (!text)
		return NULL;

	if (fyn_anchor == fyn) {
		alloca_sprintf(&str, "*%.*s", (int)len, text);
    } else {
        fy_node_get_path_relative_to_alloca(fyn_anchor, fyn, &path);
		alloca_sprintf(&str, "*%.*s/%s", (int)len, text, path);
	}

	path = strdup(str);
	return path;
}

static struct fy_node *
fy_document_load_node(struct fy_document *fyd, struct fy_parser *fyp,
		      struct fy_document_state **fydsp)
{
	struct fy_eventp *fyep = NULL;
	struct fy_event *fye = NULL;
	struct fy_node *fyn = NULL;
	int rc, depth;
	bool was_stream_start;

	if (!fyd || !fyp)
		return NULL;

	/* only single documents */
	fy_parser_set_next_single_document(fyp);
	fy_parser_set_default_document_state(fyp, fyd->fyds);

again:
	was_stream_start = false;
	do {
		/* get next event */
		fyep = fy_parse_private(fyp);

		/* no more */
		if (!fyep)
			return NULL;

		was_stream_start = fyep->e.type == FYET_STREAM_START;

		if (was_stream_start) {
			fy_parse_eventp_recycle(fyp, fyep);
			fyep = NULL;
		}

	} while (was_stream_start);

	fye = &fyep->e;

	/* STREAM_END */
	if (fye->type == FYET_STREAM_END) {
		fy_parse_eventp_recycle(fyp, fyep);

		/* final STREAM_END? */
		if (fyp->state == FYPS_END)
			return NULL;

		/* multi-stream */
		goto again;
	}

	FYD_TOKEN_ERROR_CHECK(fyd, fy_event_get_token(fye), FYEM_DOC,
			fye->type == FYET_DOCUMENT_START, err_out,
			"bad event");

	fy_parse_eventp_recycle(fyp, fyep);
	fyep = NULL;
	fye = NULL;

	fyd_doc_debug(fyd, "calling load_node() for root");
	depth = 0;
	rc = fy_parse_document_load_node(fyp, fyd, fy_parse_private(fyp), &fyn, &depth);
	fyd_error_check(fyd, !rc, err_out,
			"fy_parse_document_load_node() failed");

	rc = fy_parse_document_load_end(fyp, fyd, fy_parse_private(fyp));
	fyd_error_check(fyd, !rc, err_out,
			"fy_parse_document_load_node() failed");

	/* always resolve parents */
	fy_resolve_parent_node(fyd, fyn, NULL);

	if (fydsp)
		*fydsp = fy_document_state_ref(fyp->current_document_state);

	return fyn;

err_out:
	fy_parse_eventp_recycle(fyp, fyep);
	fyd->diag->on_error = false;
	return NULL;
}

static struct fy_node *
fy_node_build_internal(struct fy_document *fyd,
		int (*parser_setup)(struct fy_parser *fyp, void *user),
		void *user)
{
	struct fy_document_state *fyds = NULL;
	struct fy_node *fyn = NULL;
	struct fy_parser fyp_data, *fyp = &fyp_data;
	struct fy_parse_cfg cfg;
	struct fy_eventp *fyep;
	int rc;
	bool got_stream_end;

	if (!fyd || !parser_setup)
		return NULL;

	cfg = fyd->parse_cfg;
	cfg.diag = fyd->diag;
	rc = fy_parse_setup(fyp, &cfg);
	if (rc) {
		fyd->diag->on_error = false;
		return NULL;
	}

	rc = (*parser_setup)(fyp, user);
	fyd_error_check(fyd, !rc, err_out,
			"parser_setup() failed");

	fyn = fy_document_load_node(fyd, fyp, &fyds);
	fyd_error_check(fyd, fyn, err_out,
			"fy_document_load_node() failed");

	got_stream_end = false;
	while (!got_stream_end && (fyep = fy_parse_private(fyp)) != NULL) {
		if (fyep->e.type == FYET_STREAM_END)
			got_stream_end = true;
		fy_parse_eventp_recycle(fyp, fyep);
	}

	if (got_stream_end) {
		fyep = fy_parse_private(fyp);

		FYD_TOKEN_ERROR_CHECK(fyd, fy_event_get_token(&fyep->e), FYEM_DOC,
				!fyep, err_out,
				"trailing events after the last");

		fy_parse_eventp_recycle(fyp, fyep);
	}

	rc = fy_document_state_merge(fyd->fyds, fyds);
	fyd_error_check(fyd, !rc, err_out,
			"fy_document_state_merge() failed");

	fy_document_state_unref(fyds);

	fy_parse_cleanup(fyp);

	return fyn;

err_out:
	fy_node_detach_and_free(fyn);
	fy_document_state_unref(fyds);
	fy_parse_cleanup(fyp);
	fyd->diag->on_error = false;
	return NULL;
}

struct fy_node *fy_node_build_from_string(struct fy_document *fyd, const char *str, size_t len)
{
	struct fy_document_build_string_ctx ctx = {
		.str = str,
		.len = len,
	};

	return fy_node_build_internal(fyd, parser_setup_from_string, &ctx);
}

struct fy_node *fy_node_build_from_malloc_string(struct fy_document *fyd, char *str, size_t len)
{
	struct fy_document_build_malloc_string_ctx ctx = {
		.str = str,
		.len = len,
	};

	return fy_node_build_internal(fyd, parser_setup_from_malloc_string, &ctx);
}

struct fy_node *fy_node_build_from_file(struct fy_document *fyd, const char *file)
{
	struct fy_document_build_file_ctx ctx = {
		.file = file,
	};

	return fy_node_build_internal(fyd, parser_setup_from_file, &ctx);
}

struct fy_node *fy_node_build_from_fp(struct fy_document *fyd, FILE *fp)
{
	struct fy_document_build_fp_ctx ctx = {
		.name = NULL,
		.fp = fp,
	};

	return fy_node_build_internal(fyd, parser_setup_from_fp, &ctx);
}

int fy_document_set_root(struct fy_document *fyd, struct fy_node *fyn)
{
	if (!fyd)
		return -1;

	if (fyn && fyn->attached)
		return -1;

	fy_node_detach_and_free(fyd->root);
	fyd->root = NULL;

	fyn->parent = NULL;
	fyd->root = fyn;

	if (fyn)
		fyn->attached = true;

	return 0;
}

#define FYNCSIF_ALIAS		FY_BIT(0)
#define FYNCSIF_SIMPLE		FY_BIT(1)
#define FYNCSIF_COPY		FY_BIT(2)
#define FYNCSIF_MALLOCED	FY_BIT(3)

static struct fy_node *
fy_node_create_scalar_internal(struct fy_document *fyd, const char *data, size_t size,
			       unsigned int flags)
{
	const bool alias = !!(flags & FYNCSIF_ALIAS);
	const bool simple = !!(flags & FYNCSIF_SIMPLE);
	const bool copy = !!(flags & FYNCSIF_COPY);
	const bool malloced = !!(flags & FYNCSIF_MALLOCED);
	struct fy_node *fyn = NULL;
	struct fy_input *fyi;
	struct fy_atom handle;
	enum fy_scalar_style style;
	char *data_copy = NULL;

	if (!fyd)
		return NULL;

	if (data && size == (size_t)-1)
		size = strlen(data);

	fyn = fy_node_alloc(fyd, FYNT_SCALAR);
	fyd_error_check(fyd, fyn, err_out,
			"fy_node_alloc() failed");

	if (copy) {
		data_copy = malloc(size);
		fyd_error_check(fyd, data_copy, err_out,
				"malloc() failed");
		memcpy(data_copy, data, size);
		fyi = fy_input_from_malloc_data(data_copy, size, &handle, simple);
	} else if (malloced)
		fyi = fy_input_from_malloc_data((void *)data, size, &handle, simple);
	else
		fyi = fy_input_from_data(data, size, &handle, simple);
	fyd_error_check(fyd, fyi, err_out,
			"fy_input_from_data() failed");
	data_copy = NULL;

	if (!alias) {
		style = handle.style == FYAS_PLAIN ? FYSS_PLAIN : FYSS_DOUBLE_QUOTED;
		fyn->scalar = fy_token_create(FYTT_SCALAR, &handle, style);
	} else
		fyn->scalar = fy_token_create(FYTT_ALIAS, &handle, NULL);

	fyd_error_check(fyd, fyn->scalar, err_out,
			"fy_token_create() failed");

	fyn->style = !alias ? (style == FYSS_PLAIN ? FYNS_PLAIN : FYNS_DOUBLE_QUOTED) : FYNS_ALIAS;

	/* take away the input reference */
	fy_input_unref(fyi);

	return fyn;

err_out:
	if (data_copy)
		free(data_copy);
	fy_node_detach_and_free(fyn);
	fyd->diag->on_error = false;
	return NULL;
}

struct fy_node *fy_node_create_scalar(struct fy_document *fyd, const char *data, size_t size)
{
	return fy_node_create_scalar_internal(fyd, data, size, 0);
}

struct fy_node *fy_node_create_alias(struct fy_document *fyd, const char *data, size_t size)
{
	return fy_node_create_scalar_internal(fyd, data, size, FYNCSIF_ALIAS);
}

struct fy_node *fy_node_create_scalar_copy(struct fy_document *fyd, const char *data, size_t size)
{
	return fy_node_create_scalar_internal(fyd, data, size, FYNCSIF_COPY);
}

struct fy_node *fy_node_create_alias_copy(struct fy_document *fyd, const char *data, size_t size)
{
	return fy_node_create_scalar_internal(fyd, data, size, FYNCSIF_ALIAS | FYNCSIF_COPY);
}

struct fy_node *fy_node_create_vscalarf(struct fy_document *fyd, const char *fmt, va_list ap)
{
    char *str;

	if (!fyd || !fmt)
		return NULL;

    alloca_vsprintf(&str, fmt, ap);
	return fy_node_create_scalar_internal(fyd, str, FY_NT, FYNCSIF_COPY);
}

struct fy_node *fy_node_create_scalarf(struct fy_document *fyd, const char *fmt, ...)
{
	va_list ap;
	struct fy_node *fyn;

	va_start(ap, fmt);
	fyn = fy_node_create_vscalarf(fyd, fmt, ap);
	va_end(ap);

	return fyn;
}

int fy_node_set_tag(struct fy_node *fyn, const char *data, size_t len)
{
	struct fy_document *fyd;
	struct fy_tag_scan_info info;
	int handle_length, uri_length, prefix_length;
	const char *handle_start;
	int rc;
	struct fy_atom handle;
	struct fy_input *fyi = NULL;
	struct fy_token *fyt = NULL, *fyt_td = NULL;

	if (!fyn || !data || !len || !fyn->fyd)
		return -1;

	fyd = fyn->fyd;

	if (len == (size_t)-1)
		len = strlen(data);

	memset(&info, 0, sizeof(info));

	rc = fy_tag_scan(data, len, &info);
	if (rc)
		goto err_out;

	handle_length = info.handle_length;
	uri_length = info.uri_length;
	prefix_length = info.prefix_length;

	handle_start = data + prefix_length;

	fyt_td = fy_document_state_lookup_tag_directive(fyd->fyds,
			handle_start, handle_length);
	if (!fyt_td)
		goto err_out;

	fyi = fy_input_from_data(data, len, &handle, true);
	if (!fyi)
		goto err_out;

	handle.style = FYAS_URI;
	handle.direct_output = false;
	handle.storage_hint = 0;
	handle.storage_hint_valid = false;

	fyt = fy_token_create(FYTT_TAG, &handle, prefix_length,
				handle_length, uri_length, fyt_td);
	if (!fyt)
		goto err_out;

	fy_token_unref(fyn->tag);
	fyn->tag = fyt;

	/* take away the input reference */
	fy_input_unref(fyi);

	return 0;
err_out:
	fyd->diag->on_error = false;
	return -1;
}

int fy_node_remove_tag(struct fy_node *fyn)
{
	if (!fyn || !fyn->tag)
		return -1;

	fy_token_unref(fyn->tag);
	fyn->tag = NULL;

	return 0;
}

struct fy_node *fy_node_create_sequence(struct fy_document *fyd)
{
	struct fy_node *fyn;

	fyn = fy_node_alloc(fyd, FYNT_SEQUENCE);
	if (!fyn)
		return NULL;

	return fyn;
}

struct fy_node *fy_node_create_mapping(struct fy_document *fyd)
{
	struct fy_node *fyn;

	fyn = fy_node_alloc(fyd, FYNT_MAPPING);
	if (!fyn)
		return NULL;

	return fyn;
}

static int fy_node_sequence_insert_prepare(struct fy_node *fyn_seq, struct fy_node *fyn)
{
	struct fy_document *fyd;

	if (!fyn_seq || !fyn || fyn_seq->type != FYNT_SEQUENCE)
		return -1;

	/* can't insert a node that's attached already */
	if (fyn->attached)
		return -1;

	/* a document must be associated with the sequence */
	fyd = fyn_seq->fyd;
	if (!fyd)
		return -1;

	/* the documents of the nodes must match */
	if (fyn->fyd != fyd)
		return -1;

	fyn->parent = fyn_seq;

	return 0;
}

int fy_node_sequence_append(struct fy_node *fyn_seq, struct fy_node *fyn)
{
	int ret;

	ret = fy_node_sequence_insert_prepare(fyn_seq, fyn);
	if (ret)
		return ret;

	fy_node_mark_synthetic(fyn_seq);
	fy_node_list_add_tail(&fyn_seq->sequence, fyn);
	fyn->attached = true;
	return 0;
}

int fy_node_sequence_prepend(struct fy_node *fyn_seq, struct fy_node *fyn)
{
	int ret;

	ret = fy_node_sequence_insert_prepare(fyn_seq, fyn);
	if (ret)
		return ret;

	fy_node_mark_synthetic(fyn_seq);
	fy_node_list_add(&fyn_seq->sequence, fyn);
	fyn->attached = true;
	return 0;
}

static bool fy_node_sequence_contains_node(struct fy_node *fyn_seq, struct fy_node *fyn)
{
	struct fy_node *fyni;

	if (!fyn_seq || !fyn || fyn_seq->type != FYNT_SEQUENCE)
		return false;

	for (fyni = fy_node_list_head(&fyn_seq->sequence); fyni; fyni = fy_node_next(&fyn_seq->sequence, fyni))
		if (fyni == fyn)
			return true;

	return false;
}

int fy_node_sequence_insert_before(struct fy_node *fyn_seq,
				   struct fy_node *fyn_mark, struct fy_node *fyn)
{
	int ret;

	if (!fy_node_sequence_contains_node(fyn_seq, fyn_mark))
		return -1;

	ret = fy_node_sequence_insert_prepare(fyn_seq, fyn);
	if (ret)
		return ret;

	fy_node_mark_synthetic(fyn_seq);
	fy_node_list_insert_before(&fyn_seq->sequence, fyn_mark, fyn);
	fyn->attached = true;

	return 0;
}

int fy_node_sequence_insert_after(struct fy_node *fyn_seq,
				   struct fy_node *fyn_mark, struct fy_node *fyn)
{
	int ret;

	if (!fy_node_sequence_contains_node(fyn_seq, fyn_mark))
		return -1;

	ret = fy_node_sequence_insert_prepare(fyn_seq, fyn);
	if (ret)
		return ret;

	fy_node_mark_synthetic(fyn_seq);
	fy_node_list_insert_after(&fyn_seq->sequence, fyn_mark, fyn);
	fyn->attached = true;

	return 0;
}

struct fy_node *fy_node_sequence_remove(struct fy_node *fyn_seq, struct fy_node *fyn)
{
	if (!fy_node_sequence_contains_node(fyn_seq, fyn))
		return NULL;

	fy_node_list_del(&fyn_seq->sequence, fyn);
	fyn->parent = NULL;
	fyn->attached = false;

	fy_node_mark_synthetic(fyn_seq);

	return fyn;
}

static struct fy_node_pair *
fy_node_mapping_pair_insert_prepare(struct fy_node *fyn_map,
				    struct fy_node *fyn_key, struct fy_node *fyn_value)
{
	struct fy_document *fyd;
	struct fy_node_pair *fynp;

	if (!fyn_map || fyn_map->type != FYNT_MAPPING)
		return NULL;

	/* a document must be associated with the mapping */
	fyd = fyn_map->fyd;
	if (!fyd)
		return NULL;

	/* if not NULL, the documents of the nodes must match */
	if ((fyn_key && fyn_key->fyd != fyd) ||
	    (fyn_value && fyn_value->fyd != fyd))
		return NULL;

	/* if not NULL neither the key nor the value must be attached */
	if ((fyn_key && fyn_key->attached) ||
	    (fyn_value && fyn_value->attached))
		return NULL;

	/* if we don't allow duplicate keys */
	if (!(fyd->parse_cfg.flags & FYPCF_ALLOW_DUPLICATE_KEYS)) {

		 if (fy_node_mapping_key_is_duplicate(fyn_map, fyn_key))
			return NULL;
	}

	fynp = fy_node_pair_alloc(fyd);
	if (!fynp)
		return NULL;

	if (fyn_key) {
		fyn_key->parent = fyn_map;
		fyn_key->key_root = true;
	}
	if (fyn_value)
		fyn_value->parent = fyn_map;

	fynp->key = fyn_key;
	fynp->value = fyn_value;
	fynp->parent = fyn_map;

	return fynp;
}

int fy_node_mapping_append(struct fy_node *fyn_map,
			   struct fy_node *fyn_key, struct fy_node *fyn_value)
{
	struct fy_node_pair *fynp;

	fynp = fy_node_mapping_pair_insert_prepare(fyn_map, fyn_key, fyn_value);
	if (!fynp)
		return -1;

	fy_node_pair_list_add_tail(&fyn_map->mapping, fynp);
	if (fyn_map->xl)
		fy_accel_insert(fyn_map->xl , fyn_key, fynp);

	if (fyn_key)
		fyn_key->attached = true;
	if (fyn_value)
		fyn_value->attached = true;

	fy_node_mark_synthetic(fyn_map);

	return 0;
}

int fy_node_mapping_prepend(struct fy_node *fyn_map,
			    struct fy_node *fyn_key, struct fy_node *fyn_value)
{
	struct fy_node_pair *fynp;

	fynp = fy_node_mapping_pair_insert_prepare(fyn_map, fyn_key, fyn_value);
	if (!fynp)
		return -1;

	if (fyn_key)
		fyn_key->attached = true;
	if (fyn_value)
		fyn_value->attached = true;
	fy_node_pair_list_add(&fyn_map->mapping, fynp);
	if (fyn_map->xl)
		fy_accel_insert(fyn_map->xl, fyn_key, fynp);

	fy_node_mark_synthetic(fyn_map);

	return 0;
}

bool fy_node_mapping_contains_pair(struct fy_node *fyn_map, struct fy_node_pair *fynp)
{
	struct fy_node_pair *fynpi;

	if (!fyn_map || !fynp || fyn_map->type != FYNT_MAPPING)
		return false;

	if (fyn_map->xl) {
		fynpi = fy_node_accel_lookup_by_node(fyn_map, fynp->key);
		if (fynpi == fynp)
			return true;
	} else {
		for (fynpi = fy_node_pair_list_head(&fyn_map->mapping); fynpi; fynpi = fy_node_pair_next(&fyn_map->mapping, fynpi))
			if (fynpi == fynp)
				return true;
	}

	return false;
}

int fy_node_mapping_remove(struct fy_node *fyn_map, struct fy_node_pair *fynp)
{
	if (!fy_node_mapping_contains_pair(fyn_map, fynp))
		return -1;

	fy_node_pair_list_del(&fyn_map->mapping, fynp);
	if (fyn_map->xl)
		fy_accel_remove(fyn_map->xl, fynp->key);

	if (fynp->key) {
		fynp->key->parent = NULL;
		fynp->key->attached = false;
	}

	if (fynp->value) {
		fynp->value->parent = NULL;
		fynp->value->attached = false;
	}

	fynp->parent = NULL;

	return 0;
}

/* returns value */
struct fy_node *fy_node_mapping_remove_by_key(struct fy_node *fyn_map, struct fy_node *fyn_key)
{
	struct fy_node_pair *fynp;
	struct fy_node *fyn_value;

	fynp = fy_node_mapping_lookup_pair(fyn_map, fyn_key);
	if (!fynp)
		return NULL;

	fyn_value = fynp->value;
	if (fyn_value) {
		fyn_value->parent = NULL;
		fyn_value->attached = false;
	}

	/* do not free the key if it's the same pointer */
	if (fyn_key != fynp->key)
		fy_node_detach_and_free(fyn_key);
	fynp->value = NULL;

	fy_node_pair_list_del(&fyn_map->mapping, fynp);
	if (fyn_map->xl)
		fy_accel_remove(fyn_map->xl, fynp->key);

	fy_node_pair_detach_and_free(fynp);

	fy_node_mark_synthetic(fyn_map);

	return fyn_value;
}

void *fy_node_mapping_sort_ctx_arg(struct fy_node_mapping_sort_ctx *ctx)
{
	return ctx->arg;
}

static int fy_node_mapping_sort_cmp(
#ifdef __APPLE__
void *arg, const void *a, const void *b
#else
const void *a, const void *b, void *arg
#endif
)
{
	struct fy_node_mapping_sort_ctx *ctx = arg;
	struct fy_node_pair * const *fynppa = a, * const *fynppb = b;

	assert(fynppa >= ctx->fynpp && fynppa < ctx->fynpp + ctx->count);
	assert(fynppb >= ctx->fynpp && fynppb < ctx->fynpp + ctx->count);

	return ctx->key_cmp(*fynppa, *fynppb, ctx->arg);
}

/* not! thread safe! */
#if !defined(HAVE_QSORT_R) || !HAVE_QSORT_R || defined(__EMSCRIPTEN__) || defined(_MSC_VER)
static struct fy_node_mapping_sort_ctx *fy_node_mapping_sort_ctx_no_qsort_r;

static int fy_node_mapping_sort_cmp_no_qsort_r(const void *a, const void *b)
{
#ifdef __APPLE__
	return fy_node_mapping_sort_cmp(
			fy_node_mapping_sort_ctx_no_qsort_r,
			a, b);
#else
	return fy_node_mapping_sort_cmp( a, b,
			fy_node_mapping_sort_ctx_no_qsort_r);
#endif
}

#endif

static int fy_node_scalar_cmp_default(struct fy_node *fyn_a,
				      struct fy_node *fyn_b,
				      void *arg)
{
	/* handles NULL cases */
	if (fyn_a == fyn_b)
		return 0;
	if (!fyn_a)
		return 1;
	if (!fyn_b)
		return -1;
	return fy_token_cmp(fyn_a->scalar, fyn_b->scalar);
}

/* the default sort method */
static int fy_node_mapping_sort_cmp_default(const struct fy_node_pair *fynp_a,
					    const struct fy_node_pair *fynp_b,
					    void *arg)
{
	int idx_a, idx_b;
	bool alias_a, alias_b, scalar_a, scalar_b;
	struct fy_node_cmp_arg *cmp_arg;
	fy_node_scalar_compare_fn cmp_fn;
	void *cmp_fn_arg;

	cmp_arg = arg;
	cmp_fn = cmp_arg ? cmp_arg->cmp_fn : fy_node_scalar_cmp_default;
	cmp_fn_arg = cmp_arg ? cmp_arg->arg : NULL;

	/* order is: maps first, followed by sequences, and last scalars sorted */
	scalar_a = !fynp_a->key || fy_node_is_scalar(fynp_a->key);
	scalar_b = !fynp_b->key || fy_node_is_scalar(fynp_b->key);

	/* scalar? perform comparison */
	if (scalar_a && scalar_b) {

		/* if both are aliases, sort skipping the '*' */
		alias_a = fy_node_is_alias(fynp_a->key);
		alias_b = fy_node_is_alias(fynp_b->key);

		/* aliases win */
		if (alias_a && !alias_b)
			return -1;

		if (!alias_a && alias_b)
			return 1;

		return cmp_fn(fynp_a->key, fynp_b->key, cmp_fn_arg);
	}

	/* b is scalar, a is not */
	if (!scalar_a && scalar_b)
		return -1;

	/* a is scalar, b is not */
	if (scalar_a && !scalar_b)
		return 1;

	/* different types, mappings win */
	if (fynp_a->key->type != fynp_b->key->type)
		return fynp_a->key->type == FYNT_MAPPING ? -1 : 1;

	/* ok, need to compare indices now */
	idx_a = fy_node_mapping_get_pair_index(fynp_a->parent, fynp_a);
	idx_b = fy_node_mapping_get_pair_index(fynp_b->parent, fynp_b);

	return idx_a > idx_b ? 1 : (idx_a < idx_b ? -1 : 0);
}

void fy_node_mapping_fill_array(struct fy_node *fyn_map,
		struct fy_node_pair **fynpp, int count)
{
	struct fy_node_pair *fynpi;
	int i;

	for (i = 0, fynpi = fy_node_pair_list_head(&fyn_map->mapping); i < count && fynpi;
		fynpi = fy_node_pair_next(&fyn_map->mapping, fynpi), i++)
		fynpp[i] = fynpi;

	/* if there's enough space, put down a NULL at the end */
	if (i < count)
		fynpp[i++] = NULL;
	assert(i == count);

}

void fy_node_mapping_perform_sort(struct fy_node *fyn_map,
		fy_node_mapping_sort_fn key_cmp, void *arg,
		struct fy_node_pair **fynpp, int count)
{
	struct fy_node_mapping_sort_ctx ctx;
	struct fy_node_cmp_arg def_arg;

	if (!key_cmp) {
		def_arg.cmp_fn = fy_node_scalar_cmp_default;
		def_arg.arg = arg;
	} else {
		def_arg.cmp_fn = NULL;
		def_arg.arg = NULL;
	}
	ctx.key_cmp = key_cmp ? key_cmp : fy_node_mapping_sort_cmp_default;
	ctx.arg = key_cmp ? arg : &def_arg;
	ctx.fynpp = fynpp;
	ctx.count = count;
#if defined(HAVE_QSORT_R) && HAVE_QSORT_R && !defined(__EMSCRIPTEN__) && !defined(_MSC_VER)
#ifdef __APPLE__
	qsort_r(fynpp, count, sizeof(*fynpp), &ctx, fy_node_mapping_sort_cmp);
#else
	qsort_r(fynpp, count, sizeof(*fynpp), fy_node_mapping_sort_cmp, &ctx);
#endif
#else
	/* caution, not thread safe */
	fy_node_mapping_sort_ctx_no_qsort_r = &ctx;
	qsort(fynpp, count, sizeof(*fynpp), fy_node_mapping_sort_cmp_no_qsort_r);
	fy_node_mapping_sort_ctx_no_qsort_r = NULL;
#endif
}

struct fy_node_pair **fy_node_mapping_sort_array(struct fy_node *fyn_map,
		fy_node_mapping_sort_fn key_cmp, void *arg, int *countp)
{
	int count;
	struct fy_node_pair **fynpp;

	count = fy_node_mapping_item_count(fyn_map);
	if (count < 0)
		return NULL;

	fynpp = malloc((count + 1) * sizeof(*fynpp));
	if (!fynpp)
		return NULL;

	memset(fynpp, 0, (count + 1) * sizeof(*fynpp));

	fy_node_mapping_fill_array(fyn_map, fynpp, count);
	fy_node_mapping_perform_sort(fyn_map, key_cmp, arg, fynpp, count);

	if (countp)
		*countp = count;

	return fynpp;
}

void fy_node_mapping_release_array(struct fy_node *fyn_map, struct fy_node_pair **fynpp)
{
	if (!fyn_map || !fynpp)
		return;

	free(fynpp);
}

int fy_node_mapping_sort(struct fy_node *fyn_map,
		fy_node_mapping_sort_fn key_cmp,
		void *arg)
{
	int count, i;
	struct fy_node_pair **fynpp, *fynpi;

	fynpp = fy_node_mapping_sort_array(fyn_map, key_cmp, arg, &count);
	if (!fynpp)
		return -1;

	fy_node_pair_list_init(&fyn_map->mapping);
	for (i = 0; i < count; i++) {
		fynpi = fynpp[i];
		fy_node_pair_list_add_tail(&fyn_map->mapping, fynpi);
	}

	fy_node_mapping_release_array(fyn_map, fynpp);

	return 0;
}

int fy_node_sort(struct fy_node *fyn, fy_node_mapping_sort_fn key_cmp, void *arg)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp, *fynpi;
	int ret;

	if (!fyn)
		return 0;

	switch (fyn->type) {
	case FYNT_SCALAR:
		break;

	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni)) {

			fy_node_sort(fyni, key_cmp, arg);
		}
		break;

	case FYNT_MAPPING:
		ret = fy_node_mapping_sort(fyn, key_cmp, arg);
		if (ret)
			return ret;

		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp; fynp = fynpi) {

			fynpi = fy_node_pair_next(&fyn->mapping, fynp);

			/* the parent of the key is always NULL */
			ret = fy_node_sort(fynp->key, key_cmp, arg);
			if (ret)
				return ret;

			ret = fy_node_sort(fynp->value, key_cmp, arg);
			if (ret)
				return ret;

			fynp->parent = fyn;
		}
		break;
	}

	return 0;
}

struct fy_node *fy_node_vbuildf(struct fy_document *fyd, const char *fmt, va_list ap)
{
	struct fy_document_vbuildf_ctx vctx;
	struct fy_node *fyn;

	vctx.fmt = fmt;
	va_copy(vctx.ap, ap);
	fyn = fy_node_build_internal(fyd, parser_setup_from_fmt_ap, &vctx);
	va_end(ap);

	return fyn;
}

struct fy_node *fy_node_buildf(struct fy_document *fyd, const char *fmt, ...)
{
	struct fy_node *fyn;
	va_list ap;

	va_start(ap, fmt);
	fyn = fy_node_vbuildf(fyd, fmt, ap);
	va_end(ap);

	return fyn;
}

struct fy_document *fy_document_vbuildf(const struct fy_parse_cfg *cfg, const char *fmt, va_list ap)
{
	struct fy_document *fyd;
	struct fy_document_vbuildf_ctx vctx;

	vctx.fmt = fmt;
	va_copy(vctx.ap, ap);
	fyd = fy_document_build_internal(cfg, parser_setup_from_fmt_ap, &vctx);
	va_end(ap);

	return fyd;
}

struct fy_document *fy_document_buildf(const struct fy_parse_cfg *cfg, const char *fmt, ...)
{
	struct fy_document *fyd;
	va_list ap;

	va_start(ap, fmt);
	fyd = fy_document_vbuildf(cfg, fmt, ap);
	va_end(ap);

	return fyd;
}

struct flow_reader_container {
	struct fy_reader reader;
	const struct fy_parse_cfg *cfg;
};

static struct fy_diag *flow_reader_get_diag(struct fy_reader *fyr)
{
	struct flow_reader_container *frc = fy_container_of(fyr, struct flow_reader_container, reader);
	return frc->cfg ? frc->cfg->diag : NULL;
}

static const struct fy_reader_ops reader_ops = {
	.get_diag = flow_reader_get_diag,
};

struct fy_document *
fy_flow_document_build_from_string(const struct fy_parse_cfg *cfg,
				   const char *str, size_t len, size_t *consumed)
{
	struct flow_reader_container frc;
	struct fy_reader *fyr = NULL;
	struct fy_parser fyp_data, *fyp = &fyp_data;
	struct fy_parse_cfg cfg_data;
	struct fy_input *fyi;
	struct fy_document *fyd;
	struct fy_mark mark;
	int rc;

	if (!str)
		return NULL;

	if (consumed)
		*consumed = 0;

	if (!cfg) {
		memset(&cfg_data, 0, sizeof(cfg_data));
		cfg_data.flags = FYPCF_DEFAULT_PARSE;
		cfg = &cfg_data;
	}

	memset(&frc, 0, sizeof(frc));
	fyr = &frc.reader;
	frc.cfg = cfg;

	fy_reader_setup(fyr, &reader_ops);

	rc = fy_parse_setup(fyp, cfg);
	if (rc)
		goto err_no_parse;

	fyi = fy_input_from_data(str, len, NULL, false);
	if (!fyi)
		goto err_no_input;

	rc = fy_reader_input_open(fyr, fyi, NULL);
	if (rc)
		goto err_no_input_open;

	fy_parser_set_reader(fyp, fyr);
	fy_parser_set_flow_only_mode(fyp, true);

	fyd = fy_parse_load_document(fyp);

	fy_parse_cleanup(fyp);

	if (fyd && consumed) {
		fy_reader_get_mark(fyr, &mark);
		*consumed = mark.input_pos;
	}

	fy_reader_cleanup(fyr);
	fy_input_unref(fyi);

	return fyd;

err_no_input_open:
	fy_input_unref(fyi);
err_no_input:
	fy_parse_cleanup(fyp);
err_no_parse:
	fy_reader_cleanup(fyr);
	return NULL;
}

int fy_node_vscanf(struct fy_node *fyn, const char *fmt, va_list ap)
{
	size_t len;
	char *fmt_cpy, *s, *e, *t, *te, *key, *fmtspec;
	const char *value;
	char *value0;
	size_t value_len, value0_len;
	int count, ret;
	struct fy_node *fynv;
	va_list apt;

	if (!fyn || !fmt)
		goto err_out;

	len = strlen(fmt);
	fmt_cpy = FY_ALLOCA(len + 1);
	memcpy(fmt_cpy, fmt, len + 1);
	s = fmt_cpy;
	e = s + len;

	/* the format is of the form 'access key' %fmt[...] */
	/* so we search for a (non escaped '%) */
	value0 = NULL;
	value0_len = 0;
	count = 0;
	while (s < e) {
		/* a '%' format must exist */
		t = strchr(s, '%');
		if (!t)
			goto err_out;

		/* skip escaped % */
		if (t + 1 < e && t[1] == '%') {
			s = t + 2;
			continue;
		}

		/* trim spaces from key */
		while (isspace(*s))
			s++;
		te = t;
		while (te > s && isspace(te[-1]))
			*--te = '\0';

		key = s;

		/* we have to scan until the next space that's not in char set */
		fmtspec = t;
		while (t < e) {
			if (isspace(*t))
				break;
			/* character set (may include space) */
			if (*t == '[') {
				t++;
				/* skip caret */
				if (t < e && *t == '^')
					t++;
				/* if first character in the set is ']' accept it */
				if (t < e && *t == ']')
					t++;
				/* now skip until end of character set */
				while (t < e && *t != ']')
					t++;
				continue;
			}
			t++;
		}
		if (t < e)
			*t++ = '\0';

		/* find by (relative) path */
		fynv = fy_node_by_path(fyn, key, t - s, FYNWF_DONT_FOLLOW);
		if (!fynv || fynv->type != FYNT_SCALAR)
			break;

		/* there must be a text */
		value = fy_token_get_text(fynv->scalar, &value_len);
		if (!value)
			break;

		/* allocate buffer it's smaller than the one we have already */
		if (!value0 || value0_len < value_len) {
			value0 = FY_ALLOCA(value_len + 1);
			value0_len = value_len;
		}

		memcpy(value0, value, value_len);
		value0[value_len] = '\0';

		va_copy(apt, ap);
		/* scanf, all arguments are pointers */
		(void)va_arg(ap, void *);	/* advance argument pointer */

		/* pass it to the system's scanf method */
		ret = vsscanf(value0, fmtspec, apt);

		/* since it's a single specifier, it must be one on success */
		if (ret != 1)
			break;

		s = t;
		count++;
	}

	return count;

err_out:
	errno = -EINVAL;
	return -1;
}

int fy_node_scanf(struct fy_node *fyn, const char *fmt, ...)
{
	va_list ap;
	int ret;

	va_start(ap, fmt);
	ret = fy_node_vscanf(fyn, fmt, ap);
	va_end(ap);

	return ret;
}

int fy_document_vscanf(struct fy_document *fyd, const char *fmt, va_list ap)
{
	return fy_node_vscanf(fyd->root, fmt, ap);
}

int fy_document_scanf(struct fy_document *fyd, const char *fmt, ...)
{
	va_list ap;
	int ret;

	va_start(ap, fmt);
	ret = fy_document_vscanf(fyd, fmt, ap);
	va_end(ap);

	return ret;
}

bool fy_document_has_directives(const struct fy_document *fyd)
{
	struct fy_document_state *fyds;

	if (!fyd)
		return false;

	fyds = fyd->fyds;
	if (!fyds)
		return false;

	return fyds->fyt_vd || !fy_token_list_empty(&fyds->fyt_td);
}

bool fy_document_has_explicit_document_start(const struct fy_document *fyd)
{
	return fyd ? !fyd->fyds->start_implicit : false;
}

bool fy_document_has_explicit_document_end(const struct fy_document *fyd)
{
	return fyd ? !fyd->fyds->end_implicit : false;
}

void *fy_node_get_meta(struct fy_node *fyn)
{
	return fyn && fyn->has_meta ? fyn->meta : NULL;
}

int fy_node_set_meta(struct fy_node *fyn, void *meta)
{
	struct fy_document *fyd;

	if (!fyn || !fyn->fyd)
		return -1;

	fyd = fyn->fyd;
	if (fyn->has_meta && fyd->meta_clear_fn)
		fyd->meta_clear_fn(fyn, fyn->meta, fyd->meta_user);
	fyn->meta = meta;
	fyn->has_meta = true;

	return 0;
}

void fy_node_clear_meta(struct fy_node *fyn)
{
	struct fy_document *fyd;

	if (!fyn || !fyn->has_meta || !fyn->fyd)
		return;

	fyd = fyn->fyd;
	if (fyd->meta_clear_fn)
		fyd->meta_clear_fn(fyn, fyn->meta, fyd->meta_user);
	fyn->meta = NULL;
	fyn->has_meta = false;
}

static void fy_node_clear_meta_internal(struct fy_node *fyn)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp, *fynpi;

	if (!fyn)
		return;

	switch (fyn->type) {
	case FYNT_SCALAR:
		break;

	case FYNT_SEQUENCE:
		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
				fyni = fy_node_next(&fyn->sequence, fyni)) {

			fy_node_clear_meta_internal(fyni);
		}
		break;

	case FYNT_MAPPING:
		for (fynp = fy_node_pair_list_head(&fyn->mapping); fynp; fynp = fynpi) {

			fynpi = fy_node_pair_next(&fyn->mapping, fynp);

			fy_node_clear_meta_internal(fynp->key);
			fy_node_clear_meta_internal(fynp->value);
		}
		break;
	}

	fy_node_clear_meta(fyn);
}

int fy_document_register_meta(struct fy_document *fyd,
			      fy_node_meta_clear_fn clear_fn,
			      void *user)
{
	if (!fyd || !clear_fn || fyd->meta_clear_fn)
		return -1;

	fyd->meta_clear_fn = clear_fn;
	fyd->meta_user = user;

	return 0;
}

void fy_document_unregister_meta(struct fy_document *fyd)
{
	if (!fyd)
		return;

	fy_node_clear_meta_internal(fy_document_root(fyd));

	fyd->meta_clear_fn = NULL;
	fyd->meta_user = NULL;
}

int fy_document_set_userdata(struct fy_document *fyd, void *userdata)
{
	if (!fyd || !userdata)
		return -1;

	fyd->userdata = userdata;

	return 0;
}

void* fy_document_get_userdata(struct fy_document *fyd)
{
	return fyd->userdata;
}

int fy_document_register_on_destroy(struct fy_document *fyd,
				    fy_document_on_destroy_fn on_destroy_fn)
{
	if (!fyd || !on_destroy_fn)
		return -1;

	fyd->on_destroy_fn = on_destroy_fn;

	return 0;
}

void fy_document_unregister_on_destroy(struct fy_document *fyd)
{
	if (!fyd)
		return;

	fyd->on_destroy_fn = NULL;
}

bool fy_node_set_marker(struct fy_node *fyn, unsigned int marker)
{
	unsigned int prev_marks;

	if (!fyn || marker > FYNWF_MAX_USER_MARKER)
		return false;
	prev_marks = fyn->marks;
	fyn->marks |= FY_BIT(marker);
	return !!(prev_marks & FY_BIT(marker));
}

bool fy_node_clear_marker(struct fy_node *fyn, unsigned int marker)
{
	unsigned int prev_marks;

	if (!fyn || marker > FYNWF_MAX_USER_MARKER)
		return false;
	prev_marks = fyn->marks;
	fyn->marks &= ~FY_BIT(marker);
	return !!(prev_marks & FY_BIT(marker));
}

bool fy_node_is_marker_set(struct fy_node *fyn, unsigned int marker)
{
	if (!fyn || marker > FYNWF_MAX_USER_MARKER)
		return false;
	return !!(fyn->marks & FY_BIT(marker));
}

FILE *fy_document_get_error_fp(struct fy_document *fyd)
{
	/* just this for now */
	return stderr;
}

enum fy_parse_cfg_flags fy_document_get_cfg_flags(const struct fy_document *fyd)
{
	if (!fyd)
		return fy_parser_get_cfg_flags(NULL);

	return fyd->parse_cfg.flags;
}

bool fy_document_can_be_accelerated(struct fy_document *fyd)
{
	if (!fyd)
		return false;

	return !(fyd->parse_cfg.flags & FYPCF_DISABLE_ACCELERATORS);
}

bool fy_document_is_accelerated(struct fy_document *fyd)
{
	if (!fyd)
		return false;

	return fyd->axl && fyd->naxl;
}

static int hd_anchor_hash(struct fy_accel *xl, const void *key, void *userdata, void *hash)
{
	struct fy_token *fyt = (void *)key;
	unsigned int *hashp = hash;
	const char *text;
	size_t len;

	text = fy_token_get_text(fyt, &len);
	if (!text)
		return -1;

	*hashp = XXH32(text, len, 2654435761U);
	return 0;
}

static bool hd_anchor_eq(struct fy_accel *xl, const void *hash, const void *key1, const void *key2, void *userdata)
{
	struct fy_token *fyt1 = (void *)key1, *fyt2 = (void *)key2;
	const char *text1, *text2;
	size_t len1, len2;

	text1 = fy_token_get_text(fyt1, &len1);
	if (!text1)
		return false;
	text2 = fy_token_get_text(fyt2, &len2);
	if (!text2)
		return false;

	return len1 == len2 && !memcmp(text1, text2, len1);
}

static const struct fy_hash_desc hd_anchor = {
	.size = sizeof(unsigned int),
	.max_bucket_grow_limit = 6,	/* TODO allow tuning */
	.hash = hd_anchor_hash,
	.eq = hd_anchor_eq,
};

static int hd_nanchor_hash(struct fy_accel *xl, const void *key, void *userdata, void *hash)
{
	struct fy_node *fyn = (void *)key;
	unsigned int *hashp = hash;
	uintptr_t ptr = (uintptr_t)fyn;

	*hashp = XXH32(&ptr, sizeof(ptr), 2654435761U);

	return 0;
}

static bool hd_nanchor_eq(struct fy_accel *xl, const void *hash, const void *key1, const void *key2, void *userdata)
{
	struct fy_node *fyn1 = (void *)key1, *fyn2 = (void *)key2;

	return fyn1 == fyn2;
}

static const struct fy_hash_desc hd_nanchor = {
	.size = sizeof(unsigned int),
	.max_bucket_grow_limit = 6,	/* TODO allow tuning */
	.hash = hd_nanchor_hash,
	.eq = hd_nanchor_eq,
};


static int hd_mapping_hash(struct fy_accel *xl, const void *key, void *userdata, void *hash)
{
	return fy_node_hash_uint((struct fy_node *)key, hash);
}

static bool hd_mapping_eq(struct fy_accel *xl, const void *hash, const void *key1, const void *key2, void *userdata)
{
	return fy_node_compare((struct fy_node *)key1, (struct fy_node *)key2);
}

static const struct fy_hash_desc hd_mapping = {
	.size = sizeof(unsigned int),
	.max_bucket_grow_limit = 6,	/* TODO allow tuning */
	.hash = hd_mapping_hash,
	.eq = hd_mapping_eq,
};

typedef void (*fy_hash_update_fn)(void *state, const void *ptr, size_t size);

static int
fy_node_hash_internal(struct fy_node *fyn, fy_hash_update_fn update_fn, void *state)
{
	struct fy_node *fyni;
	struct fy_node_pair *fynp;
	struct fy_node_pair **fynpp;
	struct fy_token_iter iter;
	int i, count, rc;
	const struct fy_iter_chunk *ic;

	if (!fyn) {
		/* NULL */
		update_fn(state, "s", 1);	/* as zero length scalar */
		return 0;
	}

	switch (fyn->type) {
	case FYNT_SEQUENCE:
		/* SEQUENCE */
		update_fn(state, "S", 1);

		for (fyni = fy_node_list_head(&fyn->sequence); fyni;
		     fyni = fy_node_next(&fyn->sequence, fyni)) {

			rc = fy_node_hash_internal(fyni, update_fn, state);
			if (rc)
				return rc;
		}

		break;

	case FYNT_MAPPING:
		count = fy_node_mapping_item_count(fyn);

		fynpp = FY_ALLOCA(sizeof(*fynpp) * (count + 1));

		fy_node_mapping_fill_array(fyn, fynpp, count);
		fy_node_mapping_perform_sort(fyn, NULL, NULL, fynpp, count);

		/* MAPPING */
		update_fn(state, "M", 1);

		for (i = 0; i < count; i++) {
			fynp = fynpp[i];

			/* MAPPING KEY */
			update_fn(state, "K", 1);
			rc = fy_node_hash_internal(fynp->key, update_fn, state);
			if (rc)
				return rc;

			/* MAPPING VALUE */
			update_fn(state, "V", 1);
			rc = fy_node_hash_internal(fynp->value, update_fn, state);
			if (rc)
				return rc;
		}

		break;

	case FYNT_SCALAR:
		update_fn(state, !fy_node_is_alias(fyn) ? "s" : "A", 1);

		fy_token_iter_start(fyn->scalar, &iter);
		ic = NULL;
		while ((ic = fy_token_iter_chunk_next(&iter, ic, &rc)) != NULL)
			update_fn(state, ic->str, ic->len);
		fy_token_iter_finish(&iter);

		break;
	}

	return 0;
}

static void update_xx32(void *state, const void *ptr, size_t size)
{
	XXH32_update(state, ptr, size);
}

int fy_node_hash_uint(struct fy_node *fyn, unsigned int *hashp)
{
	XXH32_state_t state;
	int rc;

	XXH32_reset(&state, 2654435761U);

	rc = fy_node_hash_internal(fyn, update_xx32, &state);
	if (rc)
		return rc;

	*hashp = XXH32_digest(&state);
	return 0;
}

struct fy_document_state *fy_document_get_document_state(struct fy_document *fyd)
{
	return fyd ? fyd->fyds : NULL;
}

int fy_document_set_document_state(struct fy_document *fyd, struct fy_document_state *fyds)
{
	/* document must exist and not have any contents */
	if (!fyd || fyd->root)
		return -1;

	if (!fyds)
		fyds = fy_document_state_default(NULL, NULL);
	else
		fyds = fy_document_state_ref(fyds);

	if (!fyds)
		return -1;

	/* drop the previous document state */
	fy_document_state_unref(fyd->fyds);
	/* and use the new document state from now on */
	fyd->fyds = fyds;

	return 0;
}

struct fy_ptr_node *fy_ptr_node_create(struct fy_node *fyn)
{
	struct fy_ptr_node *fypn;

	if (!fyn)
		return NULL;

	fypn = malloc(sizeof(*fypn));
	if (!fypn)
		return NULL;
	memset(&fypn->node, 0, sizeof(fypn->node));
	fypn->fyn = fyn;
	return fypn;
}

void fy_ptr_node_destroy(struct fy_ptr_node *fypn)
{
	free(fypn);
}

void fy_ptr_node_list_free_all(struct fy_ptr_node_list *fypnl)
{
	struct fy_ptr_node *fypn;

	while ((fypn = fy_ptr_node_list_pop(fypnl)) != NULL)
		fy_ptr_node_destroy(fypn);
}

bool fy_ptr_node_list_contains(struct fy_ptr_node_list *fypnl, struct fy_node *fyn)
{
	struct fy_ptr_node *fypn;

	if (!fypnl || !fyn)
		return false;
	for (fypn = fy_ptr_node_list_head(fypnl); fypn; fypn = fy_ptr_node_next(fypnl, fypn)) {
		if (fypn->fyn == fyn)
			return true;
	}
	return false;
}

struct fy_document *
fy_document_create_from_event(struct fy_parser *fyp, struct fy_event *fye)
{
	struct fy_document *fyd;
	int rc;

	if (!fyp || !fye || fye->type != FYET_DOCUMENT_START)
		return NULL;

	/* TODO update document end */
	fyd = fy_document_create(&fyp->cfg);
	fyp_error_check(fyp, fyd, err_out,
		"fy_document_create() failed");

	rc = fy_document_set_document_state(fyd, fye->document_start.document_state);
	fyp_error_check(fyp, !rc, err_out,
		"fy_document_set_document_state() failed");

	return fyd;

err_out:
	fy_document_destroy(fyd);
	return NULL;
}

int
fy_document_update_from_event(struct fy_document *fyd, struct fy_parser *fyp, struct fy_event *fye)
{
	if (!fyd || !fyp || !fye || fye->type != FYET_DOCUMENT_END)
		return -1;

	/* nothing besides checks */
	return 0;
}

struct fy_node *
fy_node_create_from_event(struct fy_document *fyd, struct fy_parser *fyp, struct fy_event *fye)
{
	struct fy_node *fyn = NULL;
	struct fy_token *value = NULL, *anchor = NULL;
	int rc;

	if (!fyd || !fye)
		return NULL;

	switch (fye->type) {
	default:
		break;

	case FYET_SCALAR:
		fyn = fy_node_alloc(fyd, FYNT_SCALAR);
		fyp_error_check(fyp, fyn, err_out,
			"fy_node_alloc() scalar failed");

		value = fye->scalar.value;

		if (value)	/* NULL scalar */
			fyn->style = fy_node_style_from_scalar_style(value->scalar.style);
		else
			fyn->style = FYNS_PLAIN;

		/* NULLs are OK */
		fyn->tag = fy_token_ref(fye->scalar.tag);
		fyn->scalar = fy_token_ref(value);
		anchor = fye->scalar.anchor;
		break;

	case FYET_ALIAS:
		fyn = fy_node_alloc(fyd, FYNT_SCALAR);
		fyp_error_check(fyp, fyn, err_out,
			"fy_node_alloc() alias failed");

		value = fye->alias.anchor;
		fyn->style = FYNS_ALIAS;
		fyn->scalar = fy_token_ref(value);
		anchor = NULL;
		break;

	case FYET_MAPPING_START:
		fyn = fy_node_create_mapping(fyd);
		fyp_error_check(fyp, fyn, err_out,
			"fy_node_create_mapping() failed");

		value = fye->mapping_start.mapping_start;
		fyn->style = value->type == FYTT_FLOW_MAPPING_START ? FYNS_FLOW : FYNS_BLOCK;

		fyn->tag = fy_token_ref(fye->mapping_start.tag);
		fyn->mapping_start = fy_token_ref(value);
		fyn->mapping_end = NULL;
		anchor = fye->mapping_start.anchor;
		break;

	case FYET_SEQUENCE_START:
		fyn = fy_node_create_sequence(fyd);
		fyp_error_check(fyp, fyn, err_out,
			"fy_node_create_sequence() failed");

		value = fye->sequence_start.sequence_start;

		fyn->style = value->type == FYTT_FLOW_SEQUENCE_START ? FYNS_FLOW : FYNS_BLOCK;

		fyn->tag = fy_token_ref(fye->sequence_start.tag);
		fyn->sequence_start = fy_token_ref(value);
		fyn->sequence_end = NULL;
		anchor = fye->sequence_start.anchor;

		break;

	}

	if (fyn && anchor) {
		rc = fy_document_register_anchor(fyd, fyn, fy_token_ref(anchor));
		fyp_error_check(fyp, !rc, err_out,
			"fy_document_register_anchor() failed");
	}

	return fyn;

err_out:
	/* NULL OK */
	fy_node_free(fyn);
	return NULL;
}

int
fy_node_update_from_event(struct fy_node *fyn, struct fy_parser *fyp, struct fy_event *fye)
{
	if (!fyn || !fyp || !fye)
		return -1;

	switch (fye->type) {

	case FYET_MAPPING_END:
		if (!fy_node_is_mapping(fyn))
			return -1;
		fy_token_unref(fyn->mapping_end);
		fyn->mapping_end = fy_token_ref(fye->mapping_end.mapping_end);

		break;

	case FYET_SEQUENCE_END:
		if (!fy_node_is_sequence(fyn))
			return -1;
		fy_token_unref(fyn->sequence_end);
		fyn->sequence_end = fy_token_ref(fye->sequence_end.sequence_end);

		break;

	default:
		return -1;
	}

	return 0;
}

struct fy_node_pair *
fy_node_pair_create_with_key(struct fy_document *fyd, struct fy_node *fyn_parent, struct fy_node *fyn)
{
	struct fy_node_pair *fynp;
	bool is_duplicate;

	if (!fyd || !fyn_parent || !fy_node_is_mapping(fyn_parent))
		return NULL;

	/* if we don't allow duplicate keys */
	if (!(fyd->parse_cfg.flags & FYPCF_ALLOW_DUPLICATE_KEYS)) {

		/* make sure we don't add an already existing key */
		is_duplicate = fy_node_mapping_key_is_duplicate(fyn_parent, fyn);
		if (is_duplicate) {
			FYD_NODE_ERROR(fyd, fyn, FYEM_DOC,
					"duplicate mapping key");
			return NULL;
		}
	}

	fynp = fy_node_pair_alloc(fyd);
	fyd_error_check(fyd, fynp, err_out,
			"fy_node_pair_alloc() failed");

	fynp->parent = fyn_parent;

	fynp->key = fyn;
	if (fynp->key)
		fynp->key->attached = true;

	return fynp;

err_out:
	fy_node_pair_free(fynp);
	return NULL;

}

int
fy_node_pair_update_with_value(struct fy_node_pair *fynp, struct fy_node *fyn)
{
	struct fy_node *fyn_parent;
	int rc;

	/* node pair must exist and value must be NULL */
	if (!fynp || fynp->value || !fynp->parent || !fy_node_is_mapping(fynp->parent) || !fyn->fyd)
		return -1;

	fynp->value = fyn;
	if (fynp->value)
		fynp->value->attached = true;

	fyn_parent = fynp->parent;

	fy_node_pair_list_add_tail(&fyn_parent->mapping, fynp);
	if (fyn_parent->xl) {
		rc = fy_accel_insert(fyn_parent->xl, fynp->key, fynp);
		fyd_error_check(fyn->fyd, !rc, err_out,
			"fy_accel_insert() failed");
	}

	return 0;

err_out:
	fy_node_pair_list_del(&fyn_parent->mapping, fynp);
	if (fyn)
		fyn->attached = false;
	fynp->value = NULL;
	return -1;
}

int
fy_node_sequence_add_item(struct fy_node *fyn_parent, struct fy_node *fyn)
{
	/* node pair must exist and value must be NULL */
	if (!fyn_parent || !fyn || !fy_node_is_sequence(fyn_parent) || !fyn->fyd)
		return -1;

	fyn->parent = fyn_parent;
	fy_node_list_add_tail(&fyn_parent->sequence, fyn);
	fyn->attached = true;
	return 0;
}

void fy_document_iterator_setup(struct fy_document_iterator *fydi)
{
	memset(fydi, 0, sizeof(*fydi));
	fydi->state = FYDIS_WAITING_STREAM_START;
	fydi->fyd = NULL;
	fydi->iterate_root = NULL;

	/* suppress recycling if we must */
	fydi->suppress_recycling_force = getenv("FY_VALGRIND") && !getenv("FY_VALGRIND_RECYCLING");
	fydi->suppress_recycling = fydi->suppress_recycling_force;

	fy_eventp_list_init(&fydi->recycled_eventp);
	fy_token_list_init(&fydi->recycled_token);

	if (!fydi->suppress_recycling) {
		fydi->recycled_eventp_list = &fydi->recycled_eventp;
		fydi->recycled_token_list = &fydi->recycled_token;
	} else {
		fydi->recycled_eventp_list = NULL;
		fydi->recycled_token_list = NULL;
	}

	/* start with the stack pointing to the in place data */
	fydi->stack_top = (unsigned int)-1;
	fydi->stack_alloc = sizeof(fydi->in_place) / sizeof(fydi->in_place[0]);
	fydi->stack = fydi->in_place;
}

void fy_document_iterator_cleanup(struct fy_document_iterator *fydi)
{
	struct fy_token *fyt;
	struct fy_eventp *fyep;

	/* free the stack if it's not the inplace one */
	if (fydi->stack != fydi->in_place)
		free(fydi->stack);
	fydi->stack_top = (unsigned int)-1;
	fydi->stack_alloc = sizeof(fydi->in_place) / sizeof(fydi->in_place[0]);
	fydi->stack = fydi->in_place;

	while ((fyt = fy_token_list_pop(&fydi->recycled_token)) != NULL)
		fy_token_free(fyt);

	while ((fyep = fy_eventp_list_pop(&fydi->recycled_eventp)) != NULL)
		fy_eventp_free(fyep);

	fydi->state = FYDIS_WAITING_STREAM_START;
	fydi->fyd = NULL;
	fydi->iterate_root = NULL;
}

struct fy_document_iterator *fy_document_iterator_create(void)
{
	struct fy_document_iterator *fydi;

	fydi = malloc(sizeof(*fydi));
	if (!fydi)
		return NULL;
	fy_document_iterator_setup(fydi);
	return fydi;
}

void fy_document_iterator_destroy(struct fy_document_iterator *fydi)
{
	if (!fydi)
		return;
	fy_document_iterator_cleanup(fydi);
	free(fydi);
}

static struct fy_event *
fydi_event_create(struct fy_document_iterator *fydi, struct fy_node *fyn, bool start)
{
	struct fy_eventp *fyep;
	struct fy_event *fye;
	struct fy_anchor *fya;
	struct fy_token *anchor = NULL;

	fyep = fy_document_iterator_eventp_alloc(fydi);
	if (!fyep) {
		fydi->state = FYDIS_ERROR;
		return NULL;
	}
	fye = &fyep->e;

	if (start) {
		fya = fy_node_get_anchor(fyn);
		anchor = fya ? fya->anchor : NULL;
	}

	switch (fyn->type) {

	case FYNT_SCALAR:
		if (fyn->style != FYNS_ALIAS) {
			fye->type = FYET_SCALAR;
			fye->scalar.anchor = fy_token_ref(anchor);
			fye->scalar.tag = fy_token_ref(fyn->tag);
			fye->scalar.value = fy_token_ref(fyn->scalar);
		} else {
			fye->type = FYET_ALIAS;
			fye->alias.anchor = fy_token_ref(fyn->scalar);
		}
		break;

	case FYNT_SEQUENCE:
		if (start) {
			fye->type = FYET_SEQUENCE_START;
			fye->sequence_start.anchor = fy_token_ref(anchor);
			fye->sequence_start.tag = fy_token_ref(fyn->tag);
			fye->sequence_start.sequence_start = fy_token_ref(fyn->sequence_start);
		} else {
			fye->type = FYET_SEQUENCE_END;
			fye->sequence_end.sequence_end = fy_token_ref(fyn->sequence_end);
		}
		break;

	case FYNT_MAPPING:
		if (start) {
			fye->type = FYET_MAPPING_START;
			fye->mapping_start.anchor = fy_token_ref(anchor);
			fye->mapping_start.tag = fy_token_ref(fyn->tag);
			fye->mapping_start.mapping_start = fy_token_ref(fyn->mapping_start);
		} else {
			fye->type = FYET_MAPPING_END;
			fye->mapping_end.mapping_end = fy_token_ref(fyn->mapping_end);
		}
		break;
	}

	return fye;
}

struct fy_event *
fy_document_iterator_stream_start(struct fy_document_iterator *fydi)
{
	struct fy_event *fye;

	if (!fydi || fydi->state == FYDIS_ERROR)
		return NULL;

	/* both none and stream start are the same for this */
	if (fydi->state != FYDIS_WAITING_STREAM_START &&
	    fydi->state != FYDIS_WAITING_STREAM_END_OR_DOCUMENT_START)
		goto err_out;

	fye = fy_document_iterator_event_create(fydi, FYET_STREAM_START);
	if (!fye)
		goto err_out;

	fydi->state = FYDIS_WAITING_DOCUMENT_START;
	return fye;

err_out:
	fydi->state = FYDIS_ERROR;
	return NULL;
}

struct fy_event *
fy_document_iterator_stream_end(struct fy_document_iterator *fydi)
{
	struct fy_event *fye;

	if (!fydi || fydi->state == FYDIS_ERROR)
		return NULL;

	if (fydi->state != FYDIS_WAITING_STREAM_END_OR_DOCUMENT_START &&
	    fydi->state != FYDIS_WAITING_DOCUMENT_START)
		goto err_out;

	fye = fy_document_iterator_event_create(fydi, FYET_STREAM_END);
	if (!fye)
		goto err_out;

	fydi->state = FYDIS_WAITING_STREAM_START;
	return fye;

err_out:
	fydi->state = FYDIS_ERROR;
	return NULL;
}

struct fy_event *
fy_document_iterator_document_start(struct fy_document_iterator *fydi, struct fy_document *fyd)
{
	struct fy_event *fye = NULL;
	struct fy_eventp *fyep;

	if (!fydi || fydi->state == FYDIS_ERROR)
		return NULL;

	if (!fyd)
		goto err_out;

	/* we can transition to document start only from document start or stream end */
	if (fydi->state != FYDIS_WAITING_DOCUMENT_START &&
	    fydi->state != FYDIS_WAITING_STREAM_END_OR_DOCUMENT_START)
		goto err_out;

	fyep = fy_document_iterator_eventp_alloc(fydi);
	if (!fyep)
		goto err_out;
	fye = &fyep->e;

	fydi->fyd = fyd;

	/* the iteration root is the document root */
	fydi->iterate_root = fyd->root;

	/* suppress recycling if we must */
	fydi->suppress_recycling = (fyd->parse_cfg.flags & FYPCF_DISABLE_RECYCLING) ||
				   fydi->suppress_recycling_force;

	if (!fydi->suppress_recycling) {
		fydi->recycled_eventp_list = &fydi->recycled_eventp;
		fydi->recycled_token_list = &fydi->recycled_token;
	} else {
		fydi->recycled_eventp_list = NULL;
		fydi->recycled_token_list = NULL;
	}

	fye->type = FYET_DOCUMENT_START;
	fye->document_start.document_start = NULL;
	fye->document_start.document_state = fy_document_state_ref(fyd->fyds);
	fye->document_start.implicit = fyd->fyds->start_implicit;

	/* and go into body */
	fydi->state = FYDIS_WAITING_BODY_START_OR_DOCUMENT_END;

	return fye;

err_out:
	fy_document_iterator_event_free(fydi, fye);
	fydi->state = FYDIS_ERROR;
	return NULL;
}

struct fy_event *
fy_document_iterator_document_end(struct fy_document_iterator *fydi)
{
	struct fy_event *fye;

	if (!fydi || fydi->state == FYDIS_ERROR)
		return NULL;

	if (!fydi->fyd || !fydi->fyd->fyds ||
	    fydi->state != FYDIS_WAITING_DOCUMENT_END)
		goto err_out;

	fye = fy_document_iterator_event_create(fydi, FYET_DOCUMENT_END, (int)fydi->fyd->fyds->end_implicit);
	if (!fye)
		goto err_out;

	fydi->fyd = NULL;
	fydi->iterate_root = NULL;

	fydi->state = FYDIS_WAITING_STREAM_END_OR_DOCUMENT_START;
	return fye;

err_out:
	fydi->state = FYDIS_ERROR;
	return NULL;
}

static bool
fy_document_iterator_ensure_space(struct fy_document_iterator *fydi, unsigned int space)
{
	struct fy_document_iterator_body_state *new_stack;
	size_t new_size, copy_size;
	unsigned int new_stack_alloc;

	/* empty stack should always have enough space */
	if (fydi->stack_top == (unsigned int)-1) {
		assert(fydi->stack_alloc >= space);
		return true;
	}

	if (fydi->stack_top + space < fydi->stack_alloc)
		return true;

	/* make sure we have enough space */
	new_stack_alloc = fydi->stack_alloc * 2;
	while (fydi->stack_top + space >= new_stack_alloc)
		new_stack_alloc *= 2;

	new_size = new_stack_alloc * sizeof(*new_stack);

	if (fydi->stack == fydi->in_place) {
		new_stack = malloc(new_size);
		if (!new_stack)
			return false;
		copy_size = (fydi->stack_top + 1) * sizeof(*new_stack);
		memcpy(new_stack, fydi->stack, copy_size);
	} else {
		new_stack = realloc(fydi->stack, new_size);
		if (!new_stack)
			return false;
	}
	fydi->stack = new_stack;
	fydi->stack_alloc = new_stack_alloc;
	return true;
}

static bool
fydi_push_collection(struct fy_document_iterator *fydi, struct fy_node *fyn)
{
	struct fy_document_iterator_body_state *s;

	/* make sure there's enough space */
	if (!fy_document_iterator_ensure_space(fydi, 1))
		return false;

	/* get the next */
	fydi->stack_top++;
	s = &fydi->stack[fydi->stack_top];
	s->fyn = fyn;

	switch (fyn->type) {
	case FYNT_SEQUENCE:
		s->fyni = fy_node_list_head(&fyn->sequence);
		break;

	case FYNT_MAPPING:
		s->fynp = fy_node_pair_list_head(&fyn->mapping);
		s->processed_key = false;
		break;

	default:
		assert(0);
		break;
	}

	return true;
}

static inline void
fydi_pop_collection(struct fy_document_iterator *fydi)
{
	assert(fydi->stack_top != (unsigned int)-1);
	fydi->stack_top--;
}

static inline struct fy_document_iterator_body_state *
fydi_last_collection(struct fy_document_iterator *fydi)
{
	if (fydi->stack_top == (unsigned int)-1)
		return NULL;
	return &fydi->stack[fydi->stack_top];
}

bool
fy_document_iterator_body_next_internal(struct fy_document_iterator *fydi,
					struct fy_document_iterator_body_result *res)
{
	struct fy_document_iterator_body_state *s;
	struct fy_node *fyn, *fyn_col;
	bool end;

	if (!fydi || !res || fydi->state == FYDIS_ERROR)
		return false;

	if (fydi->state != FYDIS_WAITING_BODY_START_OR_DOCUMENT_END &&
	    fydi->state != FYDIS_BODY)
		goto err_out;

	end = false;
	s = fydi_last_collection(fydi);
	if (!s) {

		fyn = fydi->iterate_root;
		/* empty root, or last */
		if (!fyn || fydi->state == FYDIS_BODY) {
			fydi->state = FYDIS_WAITING_DOCUMENT_END;
			return false;
		}

		/* ok, in body proper */
		fydi->state = FYDIS_BODY;

	} else {

		fyn_col = s->fyn;
		assert(fyn_col);

		fyn = NULL;
		if (fyn_col->type == FYNT_SEQUENCE) {
			fyn = s->fyni;
			if (fyn)
				s->fyni = fy_node_next(&fyn_col->sequence, s->fyni);
		} else {
			assert(fyn_col->type == FYNT_MAPPING);
			if (s->fynp) {
				if (!s->processed_key) {
					fyn = s->fynp->key;
					s->processed_key = true;
				} else {
					fyn = s->fynp->value;
					s->processed_key = false;

					/* next in mapping after value */
					s->fynp = fy_node_pair_next(&fyn_col->mapping, s->fynp);
				}
			}
		}

		/* if no next node in the collection, it's the end of the collection */
		if (!fyn) {
			fyn = fyn_col;
			end = true;
		}
	}

	assert(fyn);

	/* only for collections */
	if (fyn->type != FYNT_SCALAR) {
		if (!end) {
			/* push the new sequence */
			if (!fydi_push_collection(fydi, fyn))
				goto err_out;
		} else
			fydi_pop_collection(fydi);
	}

	res->fyn = fyn;
	res->end = end;
	return true;

err_out:
	fydi->state = FYDIS_ERROR;
	return false;
}

struct fy_event *fy_document_iterator_body_next(struct fy_document_iterator *fydi)
{
	struct fy_document_iterator_body_result res;

	if (!fydi)
		return NULL;

	if (!fy_document_iterator_body_next_internal(fydi, &res))
		return NULL;

	return fydi_event_create(fydi, res.fyn, !res.end);
}

void
fy_document_iterator_node_start(struct fy_document_iterator *fydi, struct fy_node *fyn)
{
	/* do nothing on error */
	if (!fydi || fydi->state == FYDIS_ERROR)
		return;

	/* and go into body */
	fydi->state = FYDIS_WAITING_BODY_START_OR_DOCUMENT_END;
	fydi->iterate_root = fyn;
	fydi->fyd = NULL;
}

struct fy_node *fy_document_iterator_node_next(struct fy_document_iterator *fydi)
{
	struct fy_document_iterator_body_result res;

	if (!fydi)
		return NULL;

	/* do not return ending nodes, are not interested in them */
	do {
		if (!fy_document_iterator_body_next_internal(fydi, &res))
			return NULL;

	} while (res.end);

	return res.fyn;
}

bool fy_document_iterator_get_error(struct fy_document_iterator *fydi)
{
	if (!fydi)
		return true;

	if (fydi->state != FYDIS_ERROR)
		return false;

	fy_document_iterator_cleanup(fydi);

	return true;
}
