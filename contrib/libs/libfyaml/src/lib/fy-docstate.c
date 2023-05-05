/*
 * fy-docstate.c - YAML document state methods
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

#include <libfyaml.h>

#include "fy-parse.h"
#include "fy-doc.h"

#include "fy-docstate.h"

struct fy_document_state *fy_document_state_alloc(void)
{
	struct fy_document_state *fyds;

	fyds = malloc(sizeof(*fyds));
	if (!fyds)
		return NULL;
	memset(fyds, 0, sizeof(*fyds));

	fyds->fyt_vd = NULL;
	fy_token_list_init(&fyds->fyt_td);

	fyds->refs = 1;

	return fyds;
}

void fy_document_state_free(struct fy_document_state *fyds)
{
	if (!fyds)
		return;

	assert(fyds->refs == 1);

	fy_token_unref(fyds->fyt_vd);
	fy_token_list_unref_all(&fyds->fyt_td);

	free(fyds);
}

struct fy_document_state *fy_document_state_ref(struct fy_document_state *fyds)
{
	if (!fyds)
		return NULL;

	assert(fyds->refs + 1 > 0);

	fyds->refs++;

	return fyds;
}

void fy_document_state_unref(struct fy_document_state *fyds)
{
	if (!fyds)
		return;

	assert(fyds->refs > 0);

	if (fyds->refs == 1)
		fy_document_state_free(fyds);
	else
		fyds->refs--;
}

int fy_document_state_append_tag(struct fy_document_state *fyds,
				 const char *handle, const char *prefix,
				 bool is_default)
{
	struct fy_token *fyt = NULL;
	struct fy_input *fyi = NULL;
	char *data;
	size_t size, handle_size, prefix_size;
	struct fy_atom atom;

	size = strlen(handle) + 1 + strlen(prefix);
	data = malloc(size + 1);
	if (!data)
		goto err_out;

	snprintf(data, size + 1, "%s %s", handle, prefix);

	fyi = fy_input_from_malloc_data(data, size, &atom, true);
	if (!fyi)
		goto err_out;
	data = NULL;	/* ownership now at input */

	handle_size = strlen(handle);
	prefix_size = strlen(prefix);

	fyt = fy_token_create(FYTT_TAG_DIRECTIVE, &atom,
			     handle_size, prefix_size,
			     is_default);
	if (!fyt)
		goto err_out;

	fy_token_list_add_tail(&fyds->fyt_td, fyt);

	if (!fy_tag_is_default_internal(handle, handle_size, prefix, prefix_size))
		fyds->tags_explicit = true;

	/* take away the input reference */
	fy_input_unref(fyi);

	return 0;

err_out:
	fy_token_unref(fyt);
	fy_input_unref(fyi);
	if (data)
		free(data);
	return -1;
}

struct fy_document_state *fy_document_state_default(
		const struct fy_version *default_version,
		const struct fy_tag * const *default_tags)
{
	struct fy_document_state *fyds = NULL;
	const struct fy_tag *fytag;
	int i, rc;

	if (!default_version)
		default_version = &fy_default_version;

	if (!default_tags)
		default_tags = fy_default_tags;

	fyds = fy_document_state_alloc();
	if (!fyds)
		goto err_out;

	fyds->version = *default_version;

	fyds->version_explicit = false;
	fyds->tags_explicit = false;
	fyds->start_implicit = true;
	fyds->end_implicit = true;
	fyds->json_mode = false;

	memset(&fyds->start_mark, 0, sizeof(fyds->start_mark));
	memset(&fyds->end_mark, 0, sizeof(fyds->end_mark));

	fyds->fyt_vd = NULL;
	fy_token_list_init(&fyds->fyt_td);

	for (i = 0; (fytag = default_tags[i]) != NULL; i++) {

		rc = fy_document_state_append_tag(fyds, fytag->handle, fytag->prefix, true);
		if (rc)
			goto err_out;
	}

	return fyds;
err_out:
	fy_document_state_unref(fyds);
	return NULL;
}

struct fy_document_state *fy_document_state_copy(struct fy_document_state *fyds)
{
	struct fy_document_state *fyds_new = NULL;
	struct fy_token *fyt_td, *fyt;

	fyds_new = fy_document_state_alloc();
	if (!fyds_new)
		goto err_out;

	fyds_new->version = fyds->version;
	fyds_new->version_explicit = fyds->version_explicit;
	fyds_new->tags_explicit = fyds->tags_explicit;
	fyds_new->start_implicit = fyds->start_implicit;
	fyds_new->end_implicit = fyds->end_implicit;
	fyds_new->json_mode = fyds->json_mode;

	fyds_new->start_mark = fyds->start_mark;
	fyds_new->end_mark = fyds->end_mark;

	if (fyds->fyt_vd) {
		fyt = fy_token_alloc();
		if (!fyt)
			goto err_out;

		fyt->type = FYTT_VERSION_DIRECTIVE;
		fyt->handle = fyds->fyt_vd->handle;
		fyt->version_directive.vers = fyds->fyt_vd->version_directive.vers;

		/* take reference */
		fy_input_ref(fyt->handle.fyi);

		fyds_new->fyt_vd = fyt;
	}

	for (fyt = fy_token_list_first(&fyds->fyt_td); fyt; fyt = fy_token_next(&fyds->fyt_td, fyt)) {
		fyt_td = fy_token_alloc();
		if (!fyt_td)
			goto err_out;

		fyt_td->type = FYTT_TAG_DIRECTIVE;
		fyt_td->tag_directive.tag_length = fyt->tag_directive.tag_length;
		fyt_td->tag_directive.uri_length = fyt->tag_directive.uri_length;
		fyt_td->tag_directive.is_default = fyt->tag_directive.is_default;
		fyt_td->handle = fyt->handle;
		fyt_td->tag_directive.prefix0 = NULL;
		fyt_td->tag_directive.handle0 = NULL;

		/* take reference */
		fy_input_ref(fyt_td->handle.fyi);

		/* append to the new document state */
		fy_token_list_add_tail(&fyds_new->fyt_td, fyt_td);
	}

	return fyds_new;

err_out:
	fy_document_state_unref(fyds_new);
	return NULL;
}

struct fy_token *fy_document_state_lookup_tag_directive(struct fy_document_state *fyds,
		const char *handle, size_t handle_size)
{
	const char *td_handle;
	size_t td_handle_size;
	struct fy_token *fyt;

	if (!fyds)
		return NULL;

	for (fyt = fy_token_list_first(&fyds->fyt_td); fyt; fyt = fy_token_next(&fyds->fyt_td, fyt)) {

		td_handle = fy_tag_directive_token_handle(fyt, &td_handle_size);
		assert(td_handle);

		if (handle_size == td_handle_size && !memcmp(handle, td_handle, handle_size))
			return fyt;

	}

	return NULL;
}

int fy_document_state_merge(struct fy_document_state *fyds,
			    struct fy_document_state *fydsc)
{
	const char *td_prefix, *tdc_handle, *tdc_prefix;
	size_t td_prefix_size, tdc_handle_size, tdc_prefix_size;
	struct fy_token *fyt, *fytc_td, *fyt_td;

	if (!fyds || !fydsc)
		return -1;

	/* check if there's a duplicate handle (which differs */
	for (fytc_td = fy_token_list_first(&fydsc->fyt_td); fytc_td;
			fytc_td = fy_token_next(&fydsc->fyt_td, fytc_td)) {

		tdc_handle = fy_tag_directive_token_handle(fytc_td, &tdc_handle_size);
		if (!tdc_handle)
			goto err_out;
		tdc_prefix = fy_tag_directive_token_prefix(fytc_td, &tdc_prefix_size);
		if (!tdc_prefix)
			goto err_out;

		fyt_td = fy_document_state_lookup_tag_directive(fyds, tdc_handle, tdc_handle_size);
		if (fyt_td) {
			/* exists, must check whether the prefixes match */
			td_prefix = fy_tag_directive_token_prefix(fyt_td, &td_prefix_size);
			assert(td_prefix);

			/* match? do nothing */
			if (tdc_prefix_size == td_prefix_size &&
			    !memcmp(tdc_prefix, td_prefix, td_prefix_size))
				continue;

			if (!fy_token_tag_directive_is_overridable(fyt_td))
				goto err_out;

			/* override tag directive */
			fy_token_list_del(&fyds->fyt_td, fyt_td);
			fy_token_unref(fyt_td);
		}

		fyt = fy_token_create(FYTT_TAG_DIRECTIVE,
				&fytc_td->handle,
				fytc_td->tag_directive.tag_length,
				fytc_td->tag_directive.uri_length,
				fytc_td->tag_directive.is_default);
		if (!fyt)
			goto err_out;

		fy_token_list_add_tail(&fyds->fyt_td, fyt);
	}

	/* merge other document state */
	fyds->version_explicit |= fydsc->version_explicit;
	fyds->tags_explicit |= fydsc->tags_explicit;
	/* NOTE: json mode is not carried over */

	if (fyds->version.major < fydsc->version.major ||
	    (fyds->version.major == fydsc->version.major &&
		fyds->version.minor < fydsc->version.minor))
		fyds->version = fydsc->version;

	return 0;

err_out:
	return -1;
}

const struct fy_version *
fy_document_state_version(struct fy_document_state *fyds)
{
	/* return the default if not set */
	return fyds ? &fyds->version : &fy_default_version;
}

const struct fy_mark *fy_document_state_start_mark(struct fy_document_state *fyds)
{
	return fyds ? &fyds->start_mark : NULL;
}

const struct fy_mark *fy_document_state_end_mark(struct fy_document_state *fyds)
{
	return fyds ? &fyds->end_mark : NULL;
}

bool fy_document_state_version_explicit(struct fy_document_state *fyds)
{
	return fyds ? fyds->version_explicit : false;
}

bool fy_document_state_tags_explicit(struct fy_document_state *fyds)
{
	return fyds ? fyds->tags_explicit : false;
}

bool fy_document_state_start_implicit(struct fy_document_state *fyds)
{
	return fyds ? fyds->start_implicit : true;
}

bool fy_document_state_end_implicit(struct fy_document_state *fyds)
{
	return fyds ? fyds->end_implicit : true;
}

bool fy_document_state_json_mode(struct fy_document_state *fyds)
{
	return fyds ? fyds->json_mode : true;
}

const struct fy_tag *
fy_document_state_tag_directive_iterate(struct fy_document_state *fyds, void **iterp)
{
	struct fy_token *fyt;
	const struct fy_tag *tag;

	if (!fyds || !iterp)
		return NULL;

	fyt = *iterp;
	fyt = !fyt ? fy_token_list_head(&fyds->fyt_td) : fy_token_next(&fyds->fyt_td, fyt);
	if (!fyt)
		return NULL;

	/* sanity check */
	assert(fyt->type == FYTT_TAG_DIRECTIVE);

	/* always refresh, should be relatively infrequent */
	fyt->tag_directive.tag.handle = fy_tag_directive_token_handle0(fyt);
	fyt->tag_directive.tag.prefix = fy_tag_directive_token_prefix0(fyt);

	tag = &fyt->tag_directive.tag;

	*iterp = fyt;

	return tag;
}
