/*
 * fy-docstate.h - YAML document state header.
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_DOCSTATE_H
#define FY_DOCSTATE_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdarg.h>

#include <libfyaml.h>

#include "fy-ctype.h"
#include "fy-list.h"
#include "fy-typelist.h"
#include "fy-token.h"

struct fy_document;

struct fy_document_state {
	int refs;
	struct fy_version version;
	bool version_explicit : 1;
	bool tags_explicit : 1;
	bool start_implicit : 1;
	bool end_implicit : 1;
	bool json_mode : 1;
	struct fy_mark start_mark;
	struct fy_mark end_mark;
	struct fy_token *fyt_vd;		/* version directive */
	struct fy_token_list fyt_td;		/* tag directives */
};

struct fy_document_state *fy_document_state_alloc(void);
void fy_document_state_free(struct fy_document_state *fyds);
struct fy_document_state *fy_document_state_ref(struct fy_document_state *fyds);
void fy_document_state_unref(struct fy_document_state *fyds);

int fy_document_state_append_tag(struct fy_document_state *fyds,
				 const char *handle, const char *prefix,
				 bool is_default);

struct fy_document_state *fy_document_state_default(
		const struct fy_version *default_version,
		const struct fy_tag * const *default_tags);

struct fy_document_state *fy_document_state_copy(struct fy_document_state *fyds);
int fy_document_state_merge(struct fy_document_state *fyds,
			    struct fy_document_state *fydsc);

struct fy_token *fy_document_state_lookup_tag_directive(struct fy_document_state *fyds,
		const char *handle, size_t handle_size);

#endif
