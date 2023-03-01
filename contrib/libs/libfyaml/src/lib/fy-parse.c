/*
 * fy-parse.c - Internal parse interface
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
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <sys/mman.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <sys/ioctl.h>
#endif
#include <fcntl.h>
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#endif
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <limits.h>

#include <libfyaml.h>

#include "fy-parse.h"

#include "fy-utils.h"

/* only check atom sizes on debug */
#ifndef NDEBUG
#define ATOM_SIZE_CHECK
#endif

const char *fy_library_version(void)
{
#ifndef VERSION
#warn No version defined
	return "UNKNOWN";
#else
	return VERSION;
#endif
}

int fy_parse_input_append(struct fy_parser *fyp, const struct fy_input_cfg *fyic)
{
	struct fy_input *fyi = NULL;

	fyi = fy_input_create(fyic);
	fyp_error_check(fyp, fyp != NULL, err_out,
			"fy_parse_input_create() failed!");

	fyi->state = FYIS_QUEUED;
	fy_input_list_add_tail(&fyp->queued_inputs, fyi);

	return 0;

err_out:
	fy_input_unref(fyi);
	return -1;
}

bool fy_parse_have_more_inputs(struct fy_parser *fyp)
{
	return !fy_input_list_empty(&fyp->queued_inputs);
}

int fy_parse_get_next_input(struct fy_parser *fyp)
{
	const char *s;
	struct fy_reader_input_cfg icfg;
	struct fy_input *fyi;
	int rc;
	bool json_mode;
	enum fy_reader_mode rdmode;

	assert(fyp);

	if (fy_reader_current_input(fyp->reader)) {
		fyp_scan_debug(fyp, "get next input: already exists");
		return 1;
	}

	/* get next queued input */
	fyi = fy_input_list_pop(&fyp->queued_inputs);

	/* none left? we're done */
	if (!fyi) {
		fyp_scan_debug(fyp, "get next input: all inputs exhausted");
		return 0;
	}

	json_mode = false;
	if ((fyp->cfg.flags & (FYPCF_JSON_MASK << FYPCF_JSON_SHIFT)) == FYPCF_JSON_AUTO) {
		/* detection only works for filenames (sucks) */
		if (fyi->cfg.type == fyit_file) {
			s = fyi->cfg.file.filename;
			if (s)
				s = strrchr(s, '.');
			json_mode = s && !strcmp(s, ".json");
		}
	} else if ((fyp->cfg.flags & (FYPCF_JSON_MASK << FYPCF_JSON_SHIFT)) == FYPCF_JSON_FORCE)
		json_mode = true;

	/* set the initial reader mode according to json option and default version */
	if (!json_mode)
		rdmode = fy_version_compare(&fyp->default_version, fy_version_make(1, 1)) <= 0 ? fyrm_yaml_1_1 : fyrm_yaml;
	else
		rdmode = fyrm_json;

	fy_reader_set_mode(fyp->reader, rdmode);

	memset(&icfg, 0, sizeof(icfg));
	icfg.disable_mmap_opt = !!(fyp->cfg.flags & FYPCF_DISABLE_MMAP_OPT);

	rc = fy_reader_input_open(fyp->reader, fyi, &icfg);
	fyp_error_check(fyp, !rc, err_out,
			"failed to open input");

	/* take off the reference; reader now owns */
	fy_input_unref(fyi);

	// inherit the JSON mode
	if (fyp->current_document_state)
		fyp->current_document_state->json_mode = fyp_json_mode(fyp);

	fyp_scan_debug(fyp, "get next input: new input - %s mode", json_mode ? "JSON" : "YAML");

	return 1;

err_out:
	fy_input_unref(fyi);
	return -1;
}

static inline void
fy_token_queue_epilogue(struct fy_parser *fyp, struct fy_token *fyt)
{
	/* special handling for zero indented scalars */
	fyp->token_activity_counter++;
	if (fyt->type == FYTT_DOCUMENT_START)
		fyp->document_first_content_token = true;
	else if (fyp->document_first_content_token && fy_token_type_is_content(fyt->type))
		fyp->document_first_content_token = false;
}

static inline struct fy_token *
fy_token_queue_simple_internal(struct fy_parser *fyp, struct fy_token_list *fytl, enum fy_token_type type, int advance_octets)
{
	struct fy_reader *fyr = fyp->reader;
	struct fy_token *fyt;

	/* allocate and copy in place */
	fyt = fy_token_alloc_rl(fyp->recycled_token_list);
	if (!fyt)
		return NULL;

	fyt->type = type;

	/* the advance is always octets */
	fy_reader_fill_atom_start(fyr, &fyt->handle);
	if (advance_octets > 0) {
		fy_reader_advance_octets(fyr, advance_octets);
		fyr->column += advance_octets;
	}
	fy_reader_fill_atom_end(fyr, &fyt->handle);

	fy_input_ref(fyt->handle.fyi);

	fy_token_list_add_tail(fytl, fyt);

	return fyt;
}

static inline struct fy_token *
fy_token_queue_simple(struct fy_parser *fyp, struct fy_token_list *fytl, enum fy_token_type type, int advance_octets)
{
	struct fy_token *fyt;

	fyt = fy_token_queue_simple_internal(fyp, fytl, type, advance_octets);
	if (!fyt)
		return NULL;

	fy_token_queue_epilogue(fyp, fyt);
	return fyt;
}

struct fy_token *
fy_token_vqueue_internal(struct fy_parser *fyp, struct fy_token_list *fytl,
			 enum fy_token_type type, va_list ap)
{
	struct fy_token *fyt;

	fyt = fy_token_vcreate_rl(fyp->recycled_token_list, type, ap);
	if (!fyt)
		return NULL;
	fy_token_list_add_tail(fytl, fyt);

	fy_token_queue_epilogue(fyp, fyt);
	return fyt;
}

struct fy_token *fy_token_queue_internal(struct fy_parser *fyp, struct fy_token_list *fytl,
					 enum fy_token_type type, ...)
{
	va_list ap;
	struct fy_token *fyt;

	va_start(ap, type);
	fyt = fy_token_vqueue_internal(fyp, fytl, type, ap);
	va_end(ap);

	return fyt;
}

struct fy_token *fy_token_vqueue(struct fy_parser *fyp, enum fy_token_type type, va_list ap)
{
	struct fy_token *fyt;

	fyt = fy_token_vqueue_internal(fyp, &fyp->queued_tokens, type, ap);
	if (fyt)
		fyp->token_activity_counter++;
	return fyt;
}

struct fy_token *fy_token_queue(struct fy_parser *fyp, enum fy_token_type type, ...)
{
	va_list ap;
	struct fy_token *fyt;

	va_start(ap, type);
	fyt = fy_token_vqueue(fyp, type, ap);
	va_end(ap);

	return fyt;
}

const struct fy_version fy_default_version = {
	.major = 1,
	.minor = 2
};

int fy_version_compare(const struct fy_version *va, const struct fy_version *vb)
{
	unsigned int vanum, vbnum;

	if (!va)
		va = &fy_default_version;
	if (!vb)
		vb = &fy_default_version;

#define FY_VERSION_UINT(_major, _minor) \
	((((unsigned int)(_major) & 0xff) << 8) | ((unsigned int)((_minor) & 0xff)))

	vanum = FY_VERSION_UINT(va->major, va->minor);
	vbnum = FY_VERSION_UINT(vb->major, vb->minor);

#undef FY_VERSION_UINT

	return vanum == vbnum ?  0 :
	       vanum <  vbnum ? -1 : 1;
}

const struct fy_version *
fy_version_default(void)
{
	return &fy_default_version;
}

static const struct fy_version * const fy_map_option_to_version[] = {
	[FYPCF_DEFAULT_VERSION_AUTO >> FYPCF_DEFAULT_VERSION_SHIFT] = &fy_default_version,
	[FYPCF_DEFAULT_VERSION_1_1  >> FYPCF_DEFAULT_VERSION_SHIFT] = fy_version_make(1, 1),
	[FYPCF_DEFAULT_VERSION_1_2  >> FYPCF_DEFAULT_VERSION_SHIFT] = fy_version_make(1, 2),
	[FYPCF_DEFAULT_VERSION_1_3  >> FYPCF_DEFAULT_VERSION_SHIFT] = fy_version_make(1, 3),
};

bool fy_version_is_supported(const struct fy_version *vers)
{
	unsigned int i;
	const struct fy_version *vers_chk;

	if (!vers)
		return true;	/* NULL means default, which is supported */

	for (i = 0; i < sizeof(fy_map_option_to_version)/sizeof(fy_map_option_to_version[0]); i++) {

		vers_chk = fy_map_option_to_version[i];
		if (!vers_chk)
			continue;

		if (fy_version_compare(vers, vers_chk) == 0)
			return true;
	}

	return false;
}

static const struct fy_version *
fy_parse_cfg_to_version(enum fy_parse_cfg_flags flags)
{
	unsigned int idx;

	idx = (flags >> FYPCF_DEFAULT_VERSION_SHIFT) & FYPCF_DEFAULT_VERSION_MASK;

	if (idx >= sizeof(fy_map_option_to_version)/sizeof(fy_map_option_to_version[0]))
		return NULL;

	return fy_map_option_to_version[idx];
}

const struct fy_version *fy_version_supported_iterate(void **prevp)
{
	const struct fy_version * const *versp;
	const struct fy_version *vers;
	unsigned int idx;

	if (!prevp)
		return NULL;

	versp = (const struct fy_version * const *)*prevp;
	if (!versp) {
		/* we skip over the first (which is the default) */
		versp = fy_map_option_to_version;
	}

	versp++;

	idx = versp - fy_map_option_to_version;
	if (idx >= sizeof(fy_map_option_to_version)/sizeof(fy_map_option_to_version[0]))
		return NULL;

	vers = *versp;
	*prevp = (void **)versp;

	return vers;
}

const struct fy_tag * const fy_default_tags[] = {
	&(struct fy_tag) { .handle = "!",  .prefix = "!", },
	&(struct fy_tag) { .handle = "!!", .prefix = "tag:yaml.org,2002:", },
	&(struct fy_tag) { .handle = "",   .prefix = "", },
	NULL
};

bool fy_tag_handle_is_default(const char *handle, size_t handle_size)
{
	int i;
	const struct fy_tag *fytag;

	if (handle_size == (size_t)-1)
		handle_size = strlen(handle);

	for (i = 0; (fytag = fy_default_tags[i]) != NULL; i++) {

		if (handle_size == strlen(fytag->handle) &&
		    !memcmp(handle, fytag->handle, handle_size))
			return true;

	}
	return false;
}

bool fy_tag_is_default_internal(const char *handle, size_t handle_size,
				const char *prefix, size_t prefix_size)
{
	int i;
	const struct fy_tag *fytag;

	if (handle_size == (size_t)-1)
		handle_size = strlen(handle);

	if (prefix_size == (size_t)-1)
		prefix_size = strlen(prefix);

	for (i = 0; (fytag = fy_default_tags[i]) != NULL; i++) {

		if (handle_size == strlen(fytag->handle) &&
		    !memcmp(handle, fytag->handle, handle_size) &&
		    prefix_size == strlen(fytag->prefix) &&
		    !memcmp(prefix, fytag->prefix, prefix_size))
			return true;

	}
	return false;
}

bool fy_document_state_tag_is_default(struct fy_document_state *fyds, const struct fy_tag *tag)
{
	struct fy_token *fyt_td;

	/* default tag, but it might be overriden */
	fyt_td = fy_document_state_lookup_tag_directive(fyds, tag->handle, strlen(tag->handle));
	if (!fyt_td)
		return false;	/* Huh? */

	return fyt_td->tag_directive.is_default;
}

bool fy_token_tag_directive_is_overridable(struct fy_token *fyt_td)
{
	const struct fy_tag *fytag;
	const char *handle, *prefix;
	size_t handle_size, prefix_size;
	int i;

	if (!fyt_td)
		return false;

	handle = fy_tag_directive_token_handle(fyt_td, &handle_size);
	prefix = fy_tag_directive_token_prefix(fyt_td, &prefix_size);
	if (!handle || !prefix)
		return false;

	for (i = 0; (fytag = fy_default_tags[i]) != NULL; i++) {

		if (handle_size == strlen(fytag->handle) &&
		    !memcmp(handle, fytag->handle, handle_size) &&
		    prefix_size == strlen(fytag->prefix) &&
		    !memcmp(prefix, fytag->prefix, prefix_size))
			return true;

	}
	return false;
}

int fy_reset_document_state(struct fy_parser *fyp)
{
	struct fy_document_state *fyds_new = NULL;

	fyp_scan_debug(fyp, "resetting document state");

	if (!fyp->default_document_state) {
		fyds_new = fy_document_state_default(&fyp->default_version, NULL);
		fyp_error_check(fyp, fyds_new, err_out,
				"fy_document_state_default() failed");
	} else {
		fyds_new = fy_document_state_copy(fyp->default_document_state);
		fyp_error_check(fyp, fyds_new, err_out,
				"fy_document_state_copy() failed");
	}
	// inherit the JSON mode
	fyds_new->json_mode = fyp_json_mode(fyp);

	if (fyp->current_document_state)
		fy_document_state_unref(fyp->current_document_state);
	fyp->current_document_state = fyds_new;

	/* TODO check when cleaning flow lists */
	fyp->flow_level = 0;
	fyp->flow = FYFT_NONE;
	fy_parse_flow_list_recycle_all(fyp, &fyp->flow_stack);

	return 0;

err_out:
	return -1;
}

int fy_parser_set_default_document_state(struct fy_parser *fyp,
					 struct fy_document_state *fyds)
{
	if (!fyp)
		return -1;

	/* only in a safe state */
	if (fyp->state != FYPS_NONE && fyp->state != FYPS_END)
		return -1;

	if (fyp->default_document_state != fyds) {
		if (fyp->default_document_state) {
			fy_document_state_unref(fyp->default_document_state);
			fyp->default_document_state = NULL;
		}

		if (fyds)
			fyp->default_document_state = fy_document_state_ref(fyds);
	}

	fy_reset_document_state(fyp);

	return 0;
}

void fy_parser_set_next_single_document(struct fy_parser *fyp)
{
	if (!fyp)
		return;

	fyp->next_single_document = true;
}

int fy_check_document_version(struct fy_parser *fyp)
{
	int major, minor;

	major = fyp->current_document_state->version.major;
	minor = fyp->current_document_state->version.minor;

	/* we only support YAML version 1.x */
	if (major == 1) {
		/* 1.1 is supported without warnings */
		if (minor == 1)
			goto ok;

		if (minor == 2 || minor == 3)
			goto experimental;
	}

	return -1;

experimental:
	fyp_scan_debug(fyp, "Experimental support for version %d.%d",
			major, minor);
ok:
	return 0;
}

int fy_parse_version_directive(struct fy_parser *fyp, struct fy_token *fyt, bool scan_mode)
{
	struct fy_document_state *fyds;
	const char *vs;
	size_t vs_len;
	char *vs0;
	char *s, *e;
	long v;
	int rc;

	fyp_error_check(fyp, fyt && fyt->type == FYTT_VERSION_DIRECTIVE, err_out,
			"illegal token (or missing) version directive token");

	fyds = fyp->current_document_state;
	fyp_error_check(fyp, fyds, err_out,
			"no current document state error");

	if (!scan_mode) {
		FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
				!fyds->fyt_vd, err_out,
				"duplicate version directive");
	} else {
		/* in scan mode, we just override everything */
		fy_token_unref_rl(fyp->recycled_token_list, fyds->fyt_vd);
		fyds->fyt_vd = NULL;
	}

	/* version directive of the form: MAJ.MIN */
	vs = fy_token_get_text(fyt, &vs_len);
	fyp_error_check(fyp, vs, err_out,
			"fy_token_get_text() failed");
	vs0 = FY_ALLOCA(vs_len + 1);
	memcpy(vs0, vs, vs_len);
	vs0[vs_len] = '\0';

	/* parse version numbers */
	v = strtol(vs0, &e, 10);
	fyp_error_check(fyp, e > vs0 && v >= 0 && v <= INT_MAX, err_out,
			"illegal major version number (%s)", vs0);
	fyp_error_check(fyp, *e == '.', err_out,
			"illegal version separator");
	fyds->version.major = (int)v;

	s = e + 1;
	v = strtol(s, &e, 10);
	fyp_error_check(fyp, e > s && v >= 0 && v <= INT_MAX, err_out,
			"illegal minor version number");
	fyp_error_check(fyp, *e == '\0', err_out,
			"garbage after version number");
	fyds->version.minor = (int)v;

	fyp_scan_debug(fyp, "document parsed YAML version: %d.%d",
			fyds->version.major,
			fyds->version.minor);

	rc = fy_check_document_version(fyp);
	fyp_error_check(fyp, !rc, err_out_rc,
			"unsupport version number %d.%d",
			fyds->version.major,
			fyds->version.minor);

	fyds->version_explicit = true;
	fyds->fyt_vd = fyt;

	return 0;
err_out:
	rc = -1;
err_out_rc:
	fy_token_unref_rl(fyp->recycled_token_list, fyt);
	return rc;
}

int fy_parse_tag_directive(struct fy_parser *fyp, struct fy_token *fyt, bool scan_mode)
{
	struct fy_document_state *fyds;
	struct fy_token *fyt_td;
	const char *handle, *prefix;
	size_t handle_size, prefix_size;
	bool can_override;

	fyds = fyp->current_document_state;
	fyp_error_check(fyp, fyds, err_out,
			"no current document state error");

	handle = fy_tag_directive_token_handle(fyt, &handle_size);
	fyp_error_check(fyp, handle, err_out,
			"bad tag directive token (handle)");

	prefix = fy_tag_directive_token_prefix(fyt, &prefix_size);
	fyp_error_check(fyp, prefix, err_out,
			"bad tag directive token (prefix)");

	fyt_td = fy_document_state_lookup_tag_directive(fyds, handle, handle_size);

	can_override = fyt_td && (fy_token_tag_directive_is_overridable(fyt_td) || scan_mode);

	FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
			!fyt_td || can_override, err_out,
			"duplicate tag directive");

	if (fyt_td) {
		/* fyp_notice(fyp, "overriding tag"); */
		fy_token_list_del(&fyds->fyt_td, fyt_td);
		fy_token_unref_rl(fyp->recycled_token_list, fyt_td);
		/* when we override a default tag the tags are explicit */
		fyds->tags_explicit = true;
	}

	fy_token_list_add_tail(&fyds->fyt_td, fyt);
	fyt = NULL;

	fyp_scan_debug(fyp, "document parsed tag directive with handle=%.*s",
			(int)handle_size, handle);

	if (!fy_tag_is_default_internal(handle, handle_size, prefix, prefix_size))
		fyds->tags_explicit = true;

	return 0;
err_out:
	fy_token_unref_rl(fyp->recycled_token_list, fyt);
	return -1;
}

static const struct fy_parse_cfg default_parse_cfg = {
	.flags = FYPCF_DEFAULT_PARSE,
};

static struct fy_diag *fy_parser_reader_get_diag(struct fy_reader *fyr)
{
	struct fy_parser *fyp = fy_container_of(fyr, struct fy_parser, builtin_reader);
	return fyp->diag;
}

static int fy_parser_reader_file_open(struct fy_reader *fyr, const char *name)
{
	struct fy_parser *fyp = fy_container_of(fyr, struct fy_parser, builtin_reader);
	char *sp, *s, *e, *t, *newp;
	size_t len, maxlen;
	int fd;

	if (!fyp || !name || name[0] == '\0')
		return -1;

	/* for a full path, or no search path, open directly */
	if (name[0] == '/' || !fyp->cfg.search_path || !fyp->cfg.search_path[0]) {
		fd = open(name, O_RDONLY);
		if (fd == -1)
			fyp_scan_debug(fyp, "failed to open file %s\n", name);
		else
			fyp_scan_debug(fyp, "opened file %s\n", name);
		return fd;
	}

	len = strlen(fyp->cfg.search_path);
	sp = FY_ALLOCA(len + 1);
	memcpy(sp, fyp->cfg.search_path, len + 1);

	/* allocate the maximum possible so that we don't deal with reallocations */
	maxlen = len + 1 + strlen(name);
	newp = malloc(maxlen + 1);
	if (!newp)
		return -1;

	s = sp;
	e = sp + strlen(s);
	while (s < e) {
		/* skip completely empty */
		if (*s == ':') {
			s++;
			continue;
		}

		t = strchr(s, ':');
		if (t)
			*t++ = '\0';
		else
			t = e;

		len = strlen(s) + 1 + strlen(name) + 1;
		snprintf(newp, maxlen, "%s/%s", s, name);

		/* try opening */
		fd = open(newp, O_RDONLY);
		if (fd != -1) {
			fyp_scan_debug(fyp, "opened file %s at %s", name, newp);
			free(newp);
			return fd;
		}

		s = t;
	}

	if (newp)
		free(newp);
	return -1;
}

static const struct fy_reader_ops fy_parser_reader_ops = {
	.get_diag = fy_parser_reader_get_diag,
	.file_open = fy_parser_reader_file_open,
};

int fy_parse_setup(struct fy_parser *fyp, const struct fy_parse_cfg *cfg)
{
	struct fy_diag *diag;
	struct fy_diag_cfg dcfg;
	const struct fy_version *vers;
	int rc;

	if (!fyp)
		return -1;

	memset(fyp, 0, sizeof(*fyp));

	diag = cfg ? cfg->diag : NULL;
	fyp->cfg = cfg ? *cfg : default_parse_cfg;

	/* supported version? */
	vers = fy_parse_cfg_to_version(fyp->cfg.flags);
	if (!vers)
		return -1;

	if (!diag) {
		fy_diag_cfg_default(&dcfg);
		diag = fy_diag_create(&dcfg);
		if (!diag)
			return -1;
	} else
		fy_diag_ref(diag);

	fyp->diag = diag;

	fy_reader_setup(&fyp->builtin_reader, &fy_parser_reader_ops);
	fyp->reader = &fyp->builtin_reader;

	fyp->default_version = *vers;

	fy_indent_list_init(&fyp->indent_stack);
	fy_indent_list_init(&fyp->recycled_indent);
	fyp->indent = -2;
	fyp->indent_line = -1;
	fyp->generated_block_map = false;
	fyp->last_was_comma = false;

	fy_simple_key_list_init(&fyp->simple_keys);
	fy_simple_key_list_init(&fyp->recycled_simple_key);

	fy_token_list_init(&fyp->queued_tokens);

	fy_input_list_init(&fyp->queued_inputs);

	fyp->state = FYPS_NONE;
	fy_parse_state_log_list_init(&fyp->state_stack);
	fy_parse_state_log_list_init(&fyp->recycled_parse_state_log);

	fy_eventp_list_init(&fyp->recycled_eventp);
	fy_token_list_init(&fyp->recycled_token);

	fy_flow_list_init(&fyp->flow_stack);
	fyp->flow = FYFT_NONE;
	fy_flow_list_init(&fyp->recycled_flow);

	fyp->pending_complex_key_column = -1;
	fyp->last_block_mapping_key_line = -1;

	fyp->suppress_recycling = !!(fyp->cfg.flags & FYPCF_DISABLE_RECYCLING) ||
		                  (getenv("FY_VALGRIND") &&
				   !getenv("FY_VALGRIND_RECYCLING"));

	if (fyp->suppress_recycling)
		fyp_parse_debug(fyp, "Suppressing recycling");

	if (!fyp->suppress_recycling) {
		fyp->recycled_eventp_list = &fyp->recycled_eventp;
		fyp->recycled_token_list = &fyp->recycled_token;
	} else {
		fyp->recycled_eventp_list = NULL;
		fyp->recycled_token_list = NULL;
	}

	fyp->current_document_state = NULL;

	rc = fy_reset_document_state(fyp);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_reset_document_state() failed");
	return 0;

err_out_rc:
	return rc;
}

void fy_parse_cleanup(struct fy_parser *fyp)
{
	struct fy_input *fyi, *fyin;
	struct fy_eventp *fyep;
	struct fy_token *fyt;

	fy_input_unref(fyp->last_event_handle.fyi);
	fy_atom_reset(&fyp->last_event_handle);

	fy_composer_destroy(fyp->fyc);
	fy_document_builder_destroy(fyp->fydb);

	fy_parse_indent_list_recycle_all(fyp, &fyp->indent_stack);
	fy_parse_simple_key_list_recycle_all(fyp, &fyp->simple_keys);
	fy_token_list_unref_all(&fyp->queued_tokens);

	fy_parse_parse_state_log_list_recycle_all(fyp, &fyp->state_stack);
	fy_parse_flow_list_recycle_all(fyp, &fyp->flow_stack);

	fy_token_unref_rl(fyp->recycled_token_list, fyp->stream_end_token);

	fy_document_state_unref(fyp->current_document_state);
	fy_document_state_unref(fyp->default_document_state);

	for (fyi = fy_input_list_head(&fyp->queued_inputs); fyi; fyi = fyin) {
		fyin = fy_input_next(&fyp->queued_inputs, fyi);
		fy_input_unref(fyi);
	}

	/* clean the builtin reader */
	fy_reader_cleanup(&fyp->builtin_reader);

	/* and vacuum (free everything) */
	fy_parse_indent_vacuum(fyp);
	fy_parse_simple_key_vacuum(fyp);
	fy_parse_parse_state_log_vacuum(fyp);
	fy_parse_flow_vacuum(fyp);

	/* free the recycled events */
	while ((fyep = fy_eventp_list_pop(&fyp->recycled_eventp)) != NULL) {
		/* catch double recycles */
		/* assert(fy_eventp_list_head(&fyp->recycled_eventp)!= fyep); */
		fy_eventp_free(fyep);
	}

	/* and the recycled tokens */
	while ((fyt = fy_token_list_pop(&fyp->recycled_token)) != NULL)
		fy_token_free(fyt);

	fy_diag_unref(fyp->diag);
}

static const char *state_txt[] __FY_DEBUG_UNUSED__ = {
	[FYPS_NONE] = "NONE",
	[FYPS_STREAM_START] = "STREAM_START",
	[FYPS_IMPLICIT_DOCUMENT_START] = "IMPLICIT_DOCUMENT_START",
	[FYPS_DOCUMENT_START] = "DOCUMENT_START",
	[FYPS_DOCUMENT_CONTENT] = "DOCUMENT_CONTENT",
	[FYPS_DOCUMENT_END] = "DOCUMENT_END",
	[FYPS_BLOCK_NODE] = "BLOCK_NODE",
	[FYPS_BLOCK_SEQUENCE_FIRST_ENTRY] = "BLOCK_SEQUENCE_FIRST_ENTRY",
	[FYPS_BLOCK_SEQUENCE_ENTRY] = "BLOCK_SEQUENCE_ENTRY",
	[FYPS_INDENTLESS_SEQUENCE_ENTRY] = "INDENTLESS_SEQUENCE_ENTRY",
	[FYPS_BLOCK_MAPPING_FIRST_KEY] = "BLOCK_MAPPING_FIRST_KEY",
	[FYPS_BLOCK_MAPPING_KEY] = "BLOCK_MAPPING_KEY",
	[FYPS_BLOCK_MAPPING_VALUE] = "BLOCK_MAPPING_VALUE",
	[FYPS_FLOW_SEQUENCE_FIRST_ENTRY] = "FLOW_SEQUENCE_FIRST_ENTRY",
	[FYPS_FLOW_SEQUENCE_ENTRY] = "FLOW_SEQUENCE_ENTRY",
	[FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_KEY] = "FLOW_SEQUENCE_ENTRY_MAPPING_KEY",
	[FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_VALUE] = "FLOW_SEQUENCE_ENTRY_MAPPING_VALUE",
	[FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_END] = "FLOW_SEQUENCE_ENTRY_MAPPING_END",
	[FYPS_FLOW_MAPPING_FIRST_KEY] = "FLOW_MAPPING_FIRST_KEY",
	[FYPS_FLOW_MAPPING_KEY] = "FLOW_MAPPING_KEY",
	[FYPS_FLOW_MAPPING_VALUE] = "FLOW_MAPPING_VALUE",
	[FYPS_FLOW_MAPPING_EMPTY_VALUE] = "FLOW_MAPPING_EMPTY_VALUE",
	[FYPS_SINGLE_DOCUMENT_END] = "SINGLE_DOCUMENT_END",
	[FYPS_END] = "END"
};

int fy_scan_comment(struct fy_parser *fyp, struct fy_atom *handle, bool single_line)
{
	int c, column, start_column, lines, scan_ahead;
	bool has_ws;

	c = fy_parse_peek(fyp);
	if (c != '#')
		return -1;

	/* if it's no comment parsing is enabled just consume it */
	if (!(fyp->cfg.flags & FYPCF_PARSE_COMMENTS) || !handle) {
		fy_advance(fyp, c);
		while (!(fyp_is_lbz(fyp, c = fy_parse_peek(fyp))))
			fy_advance(fyp, c);
		return 0;
	}

	if (handle->fyi)
		fy_input_unref(handle->fyi);

	memset(handle, 0, sizeof(*handle));

	fy_fill_atom_start(fyp, handle);

	lines = 0;
	start_column = fyp_column(fyp);
	column = fyp_column(fyp);
	scan_ahead = 0;

	has_ws = false;

	/* continuation must be a # on the same column */
	while (c == '#' && column == start_column) {

		lines++;
		if (c == '#') {
			/* chomp until line break */
			fy_advance(fyp, c);
			while (!(fyp_is_lbz(fyp, c = fy_parse_peek(fyp)))) {
				if (fy_is_ws(c))
					has_ws = true;
				fy_advance(fyp, c);
			}

			/* end of input break */
			if (fy_is_z(c))
				break;
		}

		if (fy_is_ws(c))
			has_ws = true;

		if (!fyp_is_lb(fyp, c))
			break;

		column = 0;

		scan_ahead = 1;	/* skipping over lb */
		while (fy_is_blank(c = fy_parse_peek_at(fyp, scan_ahead))) {
			scan_ahead++;
			column++;
		}

		if (fy_is_z(c) || single_line)
			break;

		if (c == '#' && column == start_column) {
			fy_advance_by(fyp, scan_ahead);
			c = fy_parse_peek(fyp);
		}
	}

	fy_fill_atom_end(fyp, handle);
	handle->style = FYAS_COMMENT;
	handle->direct_output = false;
	handle->storage_hint = 0;
	handle->storage_hint_valid = false;
	handle->empty = false;
	handle->has_lb = true;
	handle->has_ws = has_ws;
	handle->starts_with_ws = false; /* no-one cares for those */
	handle->starts_with_lb = false;
	handle->ends_with_ws = false;
	handle->ends_with_lb = false;
	handle->trailing_lb = false;
	handle->size0 = lines > 0;
	handle->valid_anchor = false;

	/* and take the ref */
	fy_input_ref(handle->fyi);

	return 0;
}

int fy_attach_comments_if_any(struct fy_parser *fyp, struct fy_token *fyt)
{
	struct fy_atom *handle;
	struct fy_mark fym;
	int c, rc;

	if (!fyp || !fyt)
		return -1;

	if (!(fyp->cfg.flags & FYPCF_PARSE_COMMENTS))
		return 0;

	/* if a last comment exists and is valid */
	if (fy_atom_is_set(&fyp->last_comment) &&
	    (handle = fy_token_comment_handle(fyt, fycp_top, true)) != NULL) {
		assert (!fy_atom_is_set(handle));
		*handle = fyp->last_comment;
		/* erase last comment */
		fy_atom_reset(&fyp->last_comment);
	}

	/* right hand comment */

	/* skip white space */
	while (fy_is_ws(c = fy_parse_peek(fyp)))
		fy_advance(fyp, c);

	if (c == '#') {
		fy_get_mark(fyp, &fym);

		/* it's a right comment only if it's on the same line */
		if (fym.line == fyt->handle.end_mark.line)
			handle = fy_token_comment_handle(fyt, fycp_right, true);
		else
			handle = &fyp->last_comment;	/* otherwise, last comment */

		rc = fy_scan_comment(fyp, handle, false);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_scan_comment() failed");
	}
	return 0;

err_out_rc:
	return rc;
}

int fy_scan_to_next_token(struct fy_parser *fyp)
{
	int c, c_after_ws, i, rc = 0;
	bool tabs_allowed, sloppy_flow, no_indent;
	ssize_t offset;
	struct fy_atom *handle;
	struct fy_reader *fyr;
	char *str;

	fyr = fyp->reader;

	rc = fy_reader_input_scan_token_mark(fyr);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_reader_input_scan_token_mark() failed");

	/* skip BOM at the start of the stream */
	if (fyr->current_input_pos == 0 && (c = fy_parse_peek(fyp)) == FY_UTF8_BOM) {

		fy_advance(fyp, c);
		/* reset column */
		fyr->column = 0;
	}

	/* json does not have comments or tab handling... */
	if (fyp_json_mode(fyp)) {
		fy_reader_skip_ws_cr_nl(fyr);
		goto done;
	}

	tabs_allowed = fyp->flow_level > 0 || !fyp->simple_key_allowed || fyp_tabsize(fyp) > 0;
	sloppy_flow = fyp->flow_level > 0 && (fyp->cfg.flags & FYPCF_SLOPPY_FLOW_INDENTATION);

	for (;;) {

		/* skip white space, tabs are allowed in flow context */
		/* tabs also allowed in block context but not at start of line or after -?: */

		/* if we're not in sloppy flow indent mode, a tab may not be used as indentation */
		if (!sloppy_flow) {
			// fyp_notice(fyp, "not sloppy flow check c='%c' col=%d indent=%d\n", fy_parse_peek(fyp), fyp_column(fyp), fyp->indent);
			c = -1;
			while (fyp_column(fyp) <= fyp->indent && fy_is_ws(c = fy_parse_peek(fyp))) {
				if (fy_is_tab(c))
					break;
				fy_advance(fyp, c);
			}

			/* it's an error, only if it is used for intentation */
			/* comments and empty lines are OK */
			if (fy_is_tab(c)) {

				/* skip all space and tabs */
				i = 0;
				offset = -1;
				while (fy_is_ws(c_after_ws = fy_parse_peek_at_internal(fyp, i, &offset)))
					i++;

				no_indent = c_after_ws == '#' || fyp_is_lb(fyp, c_after_ws);

				FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
						no_indent, err_out,
						"tab character may not be used as indentation");

				/* advance by that amount */
				fy_advance_by(fyp, i);
			}
		}

		if (!tabs_allowed) {
			/* skip space only */
			fy_reader_skip_space(fyr);
			c = fy_parse_peek(fyp);

			/* it's a tab, here we go */
			if (fy_is_tab(c)) {

				/* we need to see if after ws follows a flow start marker */

				/* skip all space and tabs */
				i = 0;
				offset = -1;
				while (fy_is_ws(c_after_ws = fy_parse_peek_at_internal(fyp, i, &offset)))
					i++;

				/* flow start marker after spaces? allow tabs */
				if (c_after_ws == '{' || c_after_ws == '[') {
					fy_advance_by(fyp, i);
					c = fy_parse_peek(fyp);
				}
			}
		} else {
			fy_reader_skip_ws(fyr);
			c = fy_parse_peek(fyp);
		}

		/* comment? */
		if (c == '#') {

			handle = NULL;
			if (fyp->cfg.flags & FYPCF_PARSE_COMMENTS)
				handle = &fyp->last_comment;

			rc = fy_scan_comment(fyp, handle, false);
			fyp_error_check(fyp, !rc, err_out_rc,
					"fy_scan_comment() failed");

			tabs_allowed = (fyp->flow_level || !fyp->simple_key_allowed) || fyp_tabsize(fyp);
		}

		c = fy_parse_peek(fyp);

		/* not linebreak? we're done */
		if (!fyp_is_lb(fyp, c))
			goto done;

		/* line break */
		fy_advance(fyp, c);

		/* may start simple key (in block ctx) */
		if (!fyp->flow_level && !fyp->simple_key_allowed) {
			fyp->simple_key_allowed = true;
			fyp->tab_used_for_ws = false;
			tabs_allowed = fyp->flow_level || !fyp->simple_key_allowed || fyp_tabsize(fyp);
			fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");
		}
	}

	fyp_scan_debug(fyp, "%s: no-next-token", __func__);
	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;

done:
	rc = fy_reader_input_scan_token_mark(fyr);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_reader_input_scan_token_mark() failed");

    fy_utf8_format_a(fy_parse_peek(fyp), fyue_singlequote, &str);
	fyp_scan_debug(fyp, "%s: next token starts with c='%s'", __func__, str);
	return 0;
}

static void fy_purge_required_simple_key_report(struct fy_parser *fyp,
		struct fy_token *fyt, enum fy_token_type next_type)
{
	bool is_anchor, is_tag;

	is_anchor = fyt && fyt->type == FYTT_ANCHOR;
	is_tag = fyt && fyt->type == FYTT_TAG;

	if (is_anchor || is_tag) {
		if ((fyp->state == FYPS_BLOCK_MAPPING_VALUE ||
		     fyp->state == FYPS_BLOCK_MAPPING_FIRST_KEY) &&
			next_type == FYTT_BLOCK_ENTRY) {

			FYP_TOKEN_ERROR(fyp, fyt, FYEM_SCAN,
					"invalid %s indent for sequence",
					is_anchor ? "anchor" : "tag");
			return;
		}

		if (fyp->state == FYPS_BLOCK_MAPPING_VALUE && next_type == FYTT_SCALAR) {
			FYP_TOKEN_ERROR(fyp, fyt, FYEM_SCAN,
					"invalid %s indent for mapping",
					is_anchor ? "anchor" : "tag");
			return;
		}
	}

	if (fyt)
		FYP_TOKEN_ERROR(fyp, fyt, FYEM_SCAN,
			"could not find expected ':'");
	else
		FYP_PARSE_ERROR(fyp, 0, 1, FYEM_SCAN,
			"could not find expected ':'");
}

static inline bool
fy_any_simple_keys(struct fy_parser *fyp)
{
	return !fy_simple_key_list_empty(&fyp->simple_keys);
}

static int fy_purge_stale_simple_keys(struct fy_parser *fyp, bool *did_purgep,
		enum fy_token_type next_type)
{
	struct fy_simple_key *fysk;
	bool purge;
	int line;

	*did_purgep = false;
	while ((fysk = fy_simple_key_list_head(&fyp->simple_keys)) != NULL) {

#ifdef FY_DEVMODE
		fyp_scan_debug(fyp, "purge-check: flow_level=%d fysk->flow_level=%d fysk->mark.line=%d line=%d",
				fyp->flow_level, fysk->flow_level,
				fysk->mark.line, fyp_line(fyp));

		fyp_debug_dump_simple_key(fyp, fysk, "purge-check: ");
#endif

		line = fysk->mark.line;

		/* in non-flow context we purge keys that are on different line */
		/* in flow context we purge only those with higher flow level */
		if (!fyp->flow_level) {
			purge = fyp_line(fyp) > line;
		} else {
			purge = fyp->flow_level < fysk->flow_level;
			/* also purge implicit complex keys on a different line */
			if (!purge && fysk->implicit_complex) {
				purge = fyp_line(fyp) > line;
			}
		}

		if (!purge)
			break;

		if (fysk->required) {
			fy_purge_required_simple_key_report(fyp, fysk->token, next_type);
			goto err_out;
		}

#ifdef FY_DEVMODE
		fyp_debug_dump_simple_key(fyp, fysk, "purging: ");
#endif

		fy_simple_key_list_del(&fyp->simple_keys, fysk);
		fy_parse_simple_key_recycle(fyp, fysk);

		*did_purgep = true;
	}

	if (*did_purgep && fy_simple_key_list_empty(&fyp->simple_keys))
		fyp_scan_debug(fyp, "(purge) simple key list is now empty!");

	return 0;

err_out:
	return -1;
}

int fy_push_indent(struct fy_parser *fyp, int indent, bool generated_block_map, int indent_line)
{
	struct fy_indent *fyit;

	fyit = fy_parse_indent_alloc(fyp);
	fyp_error_check(fyp, fyit != NULL, err_out,
		"fy_indent_alloc() failed");

	fyit->indent = fyp->indent;
	fyit->indent_line = fyp->indent_line;
	fyit->generated_block_map = fyp->generated_block_map;

	/* push */
	fy_indent_list_push(&fyp->indent_stack, fyit);

	/* update current state */
	fyp->parent_indent = fyp->indent;
	fyp->indent = indent;
	fyp->indent_line = indent_line;
	fyp->generated_block_map = generated_block_map;

	fyp_scan_debug(fyp, "push_indent %d -> %d - generated_block_map=%s\n",
			fyp->parent_indent, fyp->indent,
			fyp->generated_block_map ? "true" : "false");

	return 0;

err_out:
	return -1;
}

int fy_pop_indent(struct fy_parser *fyp)
{
	struct fy_indent *fyit;
	int prev_indent __FY_DEBUG_UNUSED__;

	fyit = fy_indent_list_pop(&fyp->indent_stack);
	if (!fyit)
		return -1;

	prev_indent = fyp->indent;

	/* pop the indent and update */
	fyp->indent = fyit->indent;
	fyp->generated_block_map = fyit->generated_block_map;
	fyp->indent_line = fyit->indent_line;

	/* pop and recycle */
	fy_parse_indent_recycle(fyp, fyit);

	/* update the parent indent */
	fyit = fy_indent_list_head(&fyp->indent_stack);
	fyp->parent_indent = fyit ? fyit->indent : -2;

	fyp_scan_debug(fyp, "pop indent %d -> %d (parent %d) - generated_block_map=%s\n",
			prev_indent, fyp->indent, fyp->parent_indent,
			fyp->generated_block_map ? "true" : "false");

	return 0;
}

int fy_parse_unroll_indent(struct fy_parser *fyp, int column)
{
	struct fy_token *fyt;
	int rc;

	/* do nothing in flow context */
	if (fyp->flow_level)
		return 0;

	/* pop while indentation level greater than argument */
	while (fyp->indent > column) {

		fyp_scan_debug(fyp, "unrolling: %d/%d", fyp->indent, column);

		/* create a block end token */
		fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, FYTT_BLOCK_END, 0);
		fyp_error_check(fyp, fyt, err_out,
				"fy_token_queue_simple() failed");

		rc = fy_pop_indent(fyp);
		fyp_error_check(fyp, !rc, err_out,
				"fy_pop_indent() failed");

		/* the ident line has now moved */
		fyp->indent_line = fyp_line(fyp);
	}
	return 0;
err_out:
	return -1;
}

void fy_remove_all_simple_keys(struct fy_parser *fyp)
{
	struct fy_simple_key *fysk;

	fyp_scan_debug(fyp, "SK: removing all");

	while ((fysk = fy_simple_key_list_pop(&fyp->simple_keys)) != NULL)
		fy_parse_simple_key_recycle(fyp, fysk);

	fyp->simple_key_allowed = true;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");
}

struct fy_simple_key *fy_would_remove_required_simple_key(struct fy_parser *fyp)
{
	struct fy_simple_key *fysk;

	/* no simple key? */
	for (fysk = fy_simple_key_list_head(&fyp->simple_keys);
			fysk && fysk->flow_level >= fyp->flow_level;
			fysk = fy_simple_key_next(&fyp->simple_keys, fysk)) {
		if (fysk->required)
			return fysk;
	}

	return NULL;
}

int fy_remove_simple_key(struct fy_parser *fyp, enum fy_token_type next_type)
{
	struct fy_simple_key *fysk;

	/* no simple key? */
	while ((fysk = fy_simple_key_list_first(&fyp->simple_keys)) != NULL &&
		fysk->flow_level >= fyp->flow_level) {

#ifdef FY_DEVMODE
		fyp_debug_dump_simple_key(fyp, fysk, "removing: ");
#endif

		/* remove it from the list */
		fy_simple_key_list_del(&fyp->simple_keys, fysk);

		if (fysk->required) {
			fy_purge_required_simple_key_report(fyp, fysk->token, next_type);
			goto err_out;
		}

		fy_parse_simple_key_recycle(fyp, fysk);
	}

	return 0;

err_out:
	fy_parse_simple_key_recycle(fyp, fysk);
	return -1;
}

struct fy_simple_key *fy_simple_key_find(struct fy_parser *fyp, const struct fy_token *fyt)
{
	struct fy_simple_key *fysk;

	if (!fyt)
		return NULL;

	/* no simple key? */
	for (fysk = fy_simple_key_list_head(&fyp->simple_keys); fysk;
			fysk = fy_simple_key_next(&fyp->simple_keys, fysk))
		if (fysk->token == fyt)
			return fysk;

	return NULL;
}

int fy_save_simple_key(struct fy_parser *fyp, struct fy_mark *mark, struct fy_mark *end_mark,
		struct fy_token *fyt, bool required, int flow_level,
		enum fy_token_type next_type)
{
	struct fy_simple_key *fysk;
	bool did_purge;
	int rc;

	fyp_error_check(fyp, fyt && mark && end_mark, err_out,
			"illegal arguments to fy_save_simple_key");

	if (fy_any_simple_keys(fyp)) {
		rc = fy_purge_stale_simple_keys(fyp, &did_purge, next_type);
		fyp_error_check(fyp, !rc, err_out_rc,
			"fy_purge_stale_simple_keys() failed");
	}

	/* if no simple key is allowed, don't save */
	if (!fyp->simple_key_allowed) {
		fyp_scan_debug(fyp, "not saving simple key; not allowed");
		return 0;
	}

	/* remove pending complex key mark if in non flow context and a new line */
	if (!fyp->flow_level && fyp->pending_complex_key_column >= 0 &&
	    mark->line > fyp->pending_complex_key_mark.line &&
	    mark->column <= fyp->pending_complex_key_mark.column ) {

		fyp_scan_debug(fyp, "resetting pending_complex_key mark->line=%d line=%d\n",
				mark->line, fyp->pending_complex_key_mark.line);

		fyp->pending_complex_key_column = -1;
		fyp_scan_debug(fyp, "pending_complex_key_column -> %d",
				fyp->pending_complex_key_column);
	}

	fysk = fy_simple_key_list_head(&fyp->simple_keys);

	/* create new simple key if it does not exist or if has flow level less */
	if (!fysk || fysk->flow_level < fyp->flow_level) {

		fysk = fy_parse_simple_key_alloc(fyp);
		fyp_error_check(fyp, fysk != NULL, err_out,
			"fy_simple_key_alloc()");

		fyp_scan_debug(fyp, "new simple key");

		fy_simple_key_list_push(&fyp->simple_keys, fysk);

	} else {
		fyp_error_check(fyp, !fysk->required, err_out,
				"cannot save simple key, top is required");

		if (fysk == fy_simple_key_list_tail(&fyp->simple_keys))
			fyp_scan_debug(fyp, "(reuse) simple key list is now empty!");

		fyp_scan_debug(fyp, "reusing simple key");

	}

	fysk->mark = *mark;
	fysk->end_mark = *end_mark;

	fysk->required = required;
	fysk->token = fyt;
	fysk->flow_level = flow_level;

	/* if this is a an implicit flow collection key */
	fysk->implicit_complex = fyp->pending_complex_key_column < 0 &&
				 (fyt->type == FYTT_FLOW_MAPPING_START || fyt->type == FYTT_FLOW_SEQUENCE_START);


#ifdef FY_DEVMODE
	fyp_debug_dump_simple_key_list(fyp, &fyp->simple_keys, fysk, "fyp->simple_keys (saved): ");
#endif

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

struct fy_simple_key_mark {
	struct fy_mark mark;
	bool required;
	int flow_level;
};

void fy_get_simple_key_mark(struct fy_parser *fyp, struct fy_simple_key_mark *fyskm)
{
	fy_get_mark(fyp, &fyskm->mark);
	fyskm->flow_level = fyp->flow_level;
	fyskm->required = !fyp->flow_level && fyp->indent == fyp_column(fyp);
}

int fy_save_simple_key_mark(struct fy_parser *fyp,
			struct fy_simple_key_mark *fyskm,
			enum fy_token_type next_type,
			struct fy_mark *end_markp)
{
	struct fy_mark end_mark;

	if (!end_markp) {
		fy_get_mark(fyp, &end_mark);
		end_markp = &end_mark;
	}

	return fy_save_simple_key(fyp, &fyskm->mark, end_markp,
			fy_token_list_last(&fyp->queued_tokens),
			fyskm->required, fyskm->flow_level,
			next_type);
}

int fy_parse_flow_push(struct fy_parser *fyp)
{
	struct fy_flow *fyf;

	fyf = fy_parse_flow_alloc(fyp);
	fyp_error_check(fyp, fyf != NULL, err_out,
			"fy_flow_alloc() failed!");
	fyf->flow = fyp->flow;

	fyf->pending_complex_key_column = fyp->pending_complex_key_column;
	fyf->pending_complex_key_mark = fyp->pending_complex_key_mark;

	fyp_scan_debug(fyp, "flow_push: flow=%d pending_complex_key_column=%d",
			(int)fyf->flow,
			fyf->pending_complex_key_column);

	fy_flow_list_push(&fyp->flow_stack, fyf);

	if (fyp->pending_complex_key_column >= 0) {
		fyp->pending_complex_key_column = -1;
		fyp_scan_debug(fyp, "pending_complex_key_column -> %d",
				fyp->pending_complex_key_column);
	}

	return 0;
err_out:
	return -1;
}

int fy_parse_flow_pop(struct fy_parser *fyp)
{
	struct fy_flow *fyf;

	fyf = fy_flow_list_pop(&fyp->flow_stack);
	fyp_error_check(fyp, fyf, err_out,
			"no flow to pop");

	fyp->flow = fyf->flow;
	fyp->pending_complex_key_column = fyf->pending_complex_key_column;
	fyp->pending_complex_key_mark = fyf->pending_complex_key_mark;

	fy_parse_flow_recycle(fyp, fyf);

	fyp_scan_debug(fyp, "flow_pop: flow=%d pending_complex_key_column=%d",
			(int)fyp->flow,
			fyp->pending_complex_key_column);

	return 0;

err_out:
	return -1;
}

/* special case for allowing whitespace (including tabs) after -?: */
static int fy_ws_indentation_check(struct fy_parser *fyp, bool *found_tabp, struct fy_mark *tab_mark)
{
	int c, adv, tab_adv;
	bool indentation, found_tab;

	found_tab = false;

	/* not meaning in flow mode */
	if (fyp->flow_level)
		goto out;

	/* scan forward, keeping track if we found a tab */
	adv = 0;
	tab_adv = -1;
	if (tab_mark)
		fy_get_mark(fyp, tab_mark);
	while (fy_is_ws(c = fy_parse_peek_at(fyp, adv))) {
		if (!found_tab && fy_is_tab(c)) {
			found_tab = true;
			tab_adv = adv;
			/* XXX somewhat hacky, space is 1 char only so adjust */
			if (tab_mark) {
				tab_mark->input_pos += adv;
				tab_mark->column += adv;
			}
		}
		adv++;
	}

	if (found_tab) {
		indentation = fy_utf8_strchr("?:|>", c) ||
				(c == '-' && fyp_is_blankz(fyp, fy_parse_peek_at(fyp, adv + 1)));

		/* any kind of block indentation is not allowed */
		FYP_PARSE_ERROR_CHECK(fyp, tab_adv, 1, FYEM_SCAN,
				!indentation, err_out,
				"cannot use tab for indentation of block entry");
		fy_advance_by(fyp, tab_adv + 1);
	}

	/* now chomp spaces only afterwards */
	while (fy_is_space(c = fy_parse_peek(fyp)))
		fy_advance(fyp, c);

out:
	if (found_tabp)
		*found_tabp = found_tab;

	return 0;

err_out:
	return -1;
}

int fy_fetch_stream_start(struct fy_parser *fyp)
{
	struct fy_token *fyt;

	/* simple key is allowed */
	fyp->simple_key_allowed = true;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, FYTT_STREAM_START, 0);
	fyp_error_check(fyp, fyt, err_out,
			"fy_token_queue_simple() failed");
	return 0;

err_out:
	return -1;
}

int fy_fetch_stream_end(struct fy_parser *fyp)
{
	struct fy_token *fyt;
	int rc;

	/* only reset the stream in regular mode */
	if (!fyp->parse_flow_only)
		fy_reader_stream_end(fyp->reader);

	fy_remove_all_simple_keys(fyp);

	if (fyp_block_mode(fyp)) {
		rc = fy_parse_unroll_indent(fyp, -1);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_parse_unroll_indent() failed");
	}

	fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, FYTT_STREAM_END, 0);
	fyp_error_check(fyp, fyt, err_out,
			"fy_token_queue_simple() failed");

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_scan_tag_uri_length(struct fy_parser *fyp, int start)
{
	int c, cn, length;
	ssize_t offset, offset1;

	/* first find the utf8 length of the uri */
	length = 0;
	offset = -1;
	while (fy_is_uri(c = fy_parse_peek_at_internal(fyp, start + length, &offset))) {

		offset1 = offset;
		cn = fy_parse_peek_at_internal(fyp, start + length + 1, &offset1);

		/* special handling for detecting URIs ending in ,}] */
		if (fyp_is_blankz(fyp, cn) && fy_utf8_strchr(",}]", c))
			break;

		length++;
	}

	return length;
}

bool fy_scan_tag_uri_is_valid(struct fy_parser *fyp, int start, int length)
{
	int i, j, k, width, c, w;
	uint8_t octet, esc_octets[4];
	ssize_t offset;

	offset = -1;
	for (i = 0; i < length; i++) {
		c = fy_parse_peek_at_internal(fyp, start + i, &offset);
		if (c != '%')
			continue;
		/* reset cursor */
		offset = -1;

		width = 0;
		k = 0;
		do {
			/* % escape */
			FYP_PARSE_ERROR_CHECK(fyp, start + i, 1, FYEM_SCAN,
					(length - i) >= 3, err_out,
					"short URI escape");

			if (width > 0) {
				c = fy_parse_peek_at(fyp, start + i);

				FYP_PARSE_ERROR_CHECK(fyp, start + i, 1, FYEM_SCAN,
						c == '%', err_out,
						"missing URI escape");
			}

			octet = 0;

			for (j = 0; j < 2; j++) {
				c = fy_parse_peek_at(fyp, start + i + 1 + j);

				FYP_PARSE_ERROR_CHECK(fyp, start + i + 1 + j, 1, FYEM_SCAN,
						fy_is_hex(c), err_out,
						"non hex URI escape");
				octet <<= 4;
				if (c >= '0' && c <= '9')
					octet |= c - '0';
				else if (c >= 'a' && c <= 'f')
					octet |= 10 + c - 'a';
				else
					octet |= 10 + c - 'A';
			}
			if (!width) {
				width = fy_utf8_width_by_first_octet(octet);

				FYP_PARSE_ERROR_CHECK(fyp, start + i + 1 + j, 1, FYEM_SCAN,
						width >= 1 && width <= 4, err_out,
						"bad width for hex URI escape");
				k = 0;
			}
			esc_octets[k++] = octet;

			/* skip over the 3 character escape */
			i += 3;

		} while (--width > 0);

		/* now convert to utf8 */
		c = fy_utf8_get(esc_octets, k, &w);

		FYP_PARSE_ERROR_CHECK(fyp, start + i,  1 + j, FYEM_SCAN,
				c >= 0, err_out,
				 "bad utf8 URI escape");
	}
	return true;
err_out:
	return false;
}

int fy_scan_tag_handle_length(struct fy_parser *fyp, int start)
{
	int c, length, i, width;
	ssize_t offset;
	uint8_t octet;
	bool first, was_esc;

	length = 0;

	offset = -1;
	c = fy_parse_peek_at_internal(fyp, start + length, &offset);

	FYP_PARSE_ERROR_CHECK(fyp, start + length, 1, FYEM_SCAN,
			c == '!', err_out,
			"invalid tag handle start");
	length++;

	/* get first character of the tag */
	c = fy_parse_peek_at_internal(fyp, start + length, &offset);

	if (fy_is_ws(c))
		return length;

	/* if first character is !, empty handle */
	if (c == '!') {
		length++;
		return length;
	}

	first = true;
	was_esc = false;

	/* now loop while it's alphanumeric */
	for (;;) {
		if (c == '%') {

			octet = 0;

			for (i = 0; i < 2; i++) {
				c = fy_parse_peek_at_internal(fyp, start + length + 1 + i, &offset);
				FYP_PARSE_ERROR_CHECK(fyp, start + length + 1 + i, 1, FYEM_SCAN,
						fy_is_hex(c), err_out,
						"non hex URI escape");
				octet <<= 4;
				if (c >= '0' && c <= '9')
					octet |= c - '0';
				else if (c >= 'a' && c <= 'f')
					octet |= 10 + c - 'a';
				else
					octet |= 10 + c - 'A';
			}

			width = fy_utf8_width_by_first_octet(octet);
			FYP_PARSE_ERROR_CHECK(fyp, start + length, 3, FYEM_SCAN,
					width == 1, err_out,
					"Illegal non 1 byte utf8 tag handle character");
			c = octet;
			was_esc = true;

		} else
			was_esc = false;

		if ((first && fy_is_first_alnum(c)) || (!first && fy_is_alnum(c)))
			length += was_esc ? 3 : 1;
		else
			break;

		first = false;
		c = fy_parse_peek_at_internal(fyp, start + length, &offset);
	}

	/* if last character is !, copy it */
	if (c == '!')
		length++;

	return length;

err_out:
	return -1;
}

int fy_scan_yaml_version(struct fy_parser *fyp, struct fy_version *vers)
{
	int c, length, start_length, num;
	ssize_t offset;

	memset(vers, 0, sizeof(*vers));

	/* now loop while it's numeric */
	length = 0;
	offset = -1;
	num = 0;
	while (fy_is_num(c = fy_parse_peek_at_internal(fyp, length, &offset))) {
		length++;
		num = num * 10;
		num += c - '0';
	}
	vers->major = num;

	FYP_PARSE_ERROR_CHECK(fyp, length, 1, FYEM_SCAN,
			length > 0, err_out,
			"version directive missing major number");

	FYP_PARSE_ERROR_CHECK(fyp, length, 1, FYEM_SCAN,
			c == '.', err_out,
			"version directive missing dot separator");
	length++;

	start_length = length;
	num = 0;
	while (fy_is_num(c = fy_parse_peek_at_internal(fyp, length, &offset))) {
		length++;
		num = num * 10;
		num += c - '0';
	}
	vers->minor = num;

	/* note that the version is not checked for validity here */

	FYP_PARSE_ERROR_CHECK(fyp, length, 1, FYEM_SCAN,
			length > start_length, err_out,
			"version directive missing minor number");

	return length;

err_out:
	return -1;
}

int fy_scan_tag_handle(struct fy_parser *fyp, bool is_directive,
		struct fy_atom *handle)
{
	int length;

	length = fy_scan_tag_handle_length(fyp, 0);
	fyp_error_check(fyp, length > 0, err_out,
			"fy_scan_tag_handle_length() failed");

	fy_fill_atom(fyp, length, handle);

	return 0;

err_out:
	return -1;
}


int fy_scan_tag_uri(struct fy_parser *fyp, bool is_directive,
		struct fy_atom *handle)
{
	int length;
	bool is_valid;

	length = fy_scan_tag_uri_length(fyp, 0);
	fyp_error_check(fyp, length > 0, err_out,
			"fy_scan_tag_uri_length() failed");

	is_valid = fy_scan_tag_uri_is_valid(fyp, 0, length);
	fyp_error_check(fyp, is_valid, err_out,
			"tag URI is invalid");

	fy_fill_atom(fyp, length, handle);
	handle->style = FYAS_URI;	/* this is a URI, need to handle URI escapes */

	return 0;

err_out:
	return -1;
}

int fy_scan_directive(struct fy_parser *fyp)
{
	int c, advance, version_length, tag_length, uri_length;
	struct fy_version vers;
	enum fy_reader_mode rdmode;
	enum fy_token_type type = FYTT_NONE;
	struct fy_atom handle;
	bool is_uri_valid;
	struct fy_token *fyt;
	int i, lastc;

	if (!fy_parse_strcmp(fyp, "YAML") && fy_is_ws(fy_parse_peek_at(fyp, 4))) {
		advance = 5;
		type = FYTT_VERSION_DIRECTIVE;
	} else if (!fy_parse_strcmp(fyp, "TAG") && fy_is_ws(fy_parse_peek_at(fyp, 3))) {
		advance = 4;
		type = FYTT_TAG_DIRECTIVE;
	} else {
		/* skip until linebreak (or #) */
		i = 0;
		lastc = -1;
		while ((c = fy_parse_peek_at(fyp, i)) != -1 && !fyp_is_lb(fyp, c)) {
			if (fy_is_ws(lastc) && c == '#')
				break;
			lastc = c;
			i++;
		}

		FYP_PARSE_WARNING(fyp, 0, i, FYEM_SCAN,
			"Unsupported directive");

		if (fy_is_ws(lastc) && c == '#') {
			while ((c = fy_parse_peek_at(fyp, i)) != -1 && !fyp_is_lb(fyp, c))
				i++;
		}

		fy_advance_by(fyp, i);

		/* skip over linebreak too */
		if (fyp_is_lb(fyp, c))
			fy_advance(fyp, c);

		/* bump activity counter */
		fyp->token_activity_counter++;

		return 0;
	}

	fyp_error_check(fyp, type != FYTT_NONE, err_out,
			"neither YAML|TAG found");

	/* advance */
	fy_advance_by(fyp, advance);

	/* skip white space */
	while (fy_is_ws(c = fy_parse_peek(fyp)))
		fy_advance(fyp, c);

	fy_fill_atom_start(fyp, &handle);

	/* for version directive, parse it */
	if (type == FYTT_VERSION_DIRECTIVE) {

		version_length = fy_scan_yaml_version(fyp, &vers);
		fyp_error_check(fyp, version_length > 0, err_out,
				"fy_scan_yaml_version() failed");

		/* set the reader's mode according to the version just scanned */
		rdmode = fy_version_compare(&vers, fy_version_make(1, 1)) <= 0 ? fyrm_yaml_1_1 : fyrm_yaml;
		fy_reader_set_mode(fyp->reader, rdmode);

		fy_advance_by(fyp, version_length);

		fy_fill_atom_end(fyp, &handle);

		fyt = fy_token_queue(fyp, FYTT_VERSION_DIRECTIVE, &handle, &vers);
		fyp_error_check(fyp, fyt, err_out,
				"fy_token_queue() failed");

	} else {

		tag_length = fy_scan_tag_handle_length(fyp, 0);
		fyp_error_check(fyp, tag_length > 0, err_out,
				"fy_scan_tag_handle_length() failed");

		fy_advance_by(fyp, tag_length);

		c = fy_parse_peek(fyp);
		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				fy_is_ws(c), err_out,
				"missing whitespace after TAG");

		/* skip white space */
		while (fy_is_ws(c = fy_parse_peek(fyp)))
			fy_advance(fyp, c);

		uri_length = fy_scan_tag_uri_length(fyp, 0);
		fyp_error_check(fyp, uri_length > 0, err_out,
				"fy_scan_tag_uri_length() failed");

		is_uri_valid = fy_scan_tag_uri_is_valid(fyp, 0, uri_length);
		fyp_error_check(fyp, is_uri_valid, err_out,
				"tag URI is invalid");

		fy_advance_by(fyp, uri_length);

		fy_fill_atom_end(fyp, &handle);
		handle.style = FYAS_URI;

		fyt = fy_token_queue(fyp, FYTT_TAG_DIRECTIVE, &handle, tag_length, uri_length, false);
		fyp_error_check(fyp, fyt, err_out,
				"fy_token_queue() failed");
	}

	/* skip until linebreak (or #) */
	i = 0;
	lastc = -1;
	while ((c = fy_parse_peek_at(fyp, i)) != -1 && !fyp_is_lb(fyp, c)) {
		if (fy_is_ws(lastc) && c == '#')
			break;

		FYP_PARSE_ERROR_CHECK(fyp, i, 1, FYEM_SCAN,
				fy_is_ws(c) || fyp_is_lb(fyp, c), err_out,
				"garbage after %s directive",
				type == FYTT_VERSION_DIRECTIVE ? "version" : "tag");
		lastc = c;
		i++;
	}

	fy_advance_by(fyp, i);

	/* skip over linebreak */
	if (fyp_is_lb(fyp, c))
		fy_advance(fyp, c);

	return 0;
err_out:
	return -1;
}

int fy_fetch_directive(struct fy_parser *fyp)
{
	int rc;

	fy_remove_all_simple_keys(fyp);

	if (fyp_block_mode(fyp)) {
		rc = fy_parse_unroll_indent(fyp, -1);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_parse_unroll_indent() failed");
	}

	rc = fy_scan_directive(fyp);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_scan_directive() failed");

	return 0;

err_out_rc:
	return rc;
}

int fy_fetch_document_indicator(struct fy_parser *fyp, enum fy_token_type type)
{
	int rc, c;
	struct fy_token *fyt;

	fy_remove_all_simple_keys(fyp);

	if (fyp_block_mode(fyp)) {
		rc = fy_parse_unroll_indent(fyp, -1);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_parse_unroll_indent() failed");
	}

	fyp->simple_key_allowed = false;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, type, 3);
	fyp_error_check(fyp, fyt, err_out,
			"fy_token_queue_simple() failed");

	/* skip whitespace after the indicator */
	while (fy_is_ws(c = fy_parse_peek(fyp)))
		fy_advance(fyp, c);

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

static inline bool fy_flow_indent_check_internal(struct fy_parser *fyp, int column, int indent)
{
	return (!fyp->flow_level || column > indent) ||
		((fyp->cfg.flags & FYPCF_SLOPPY_FLOW_INDENTATION) && fyp->flow_level);
}

static inline bool fy_flow_indent_check(struct fy_parser *fyp)
{
	return fy_flow_indent_check_internal(fyp, fyp_column(fyp), fyp->indent);
}

static inline bool fy_block_indent_check(struct fy_parser *fyp)
{
	return fyp->flow_level > 0 || fyp_column(fyp) > fyp->indent;
}

int fy_fetch_flow_collection_mark_start(struct fy_parser *fyp, int c)
{
	enum fy_token_type type;
	struct fy_simple_key_mark skm;
	const char *typestr;
	int rc = -1;
	struct fy_token *fyt;

	if (c == '[') {
		type = FYTT_FLOW_SEQUENCE_START;
		typestr = "sequence";
	} else {
		type = FYTT_FLOW_MAPPING_START;
		typestr = "mapping";
	}

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented %s start in flow mode", typestr);

	fy_get_simple_key_mark(fyp, &skm);

	fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, type, 1);
	fyp_error_check(fyp, fyt, err_out,
			"fy_token_queue_simple() failed");

	rc = fy_save_simple_key_mark(fyp, &skm, type, NULL);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_save_simple_key_mark() failed");

	/* increase flow level */
	fyp->flow_level++;
	fyp_error_check(fyp, fyp->flow_level, err_out,
			"overflow for the flow level counter");

	/* push the current flow to the stack */
	rc = fy_parse_flow_push(fyp);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_parse_flow_push() failed");
	/* set the current flow mode */
	fyp->flow = c == '[' ? FYFT_SEQUENCE : FYFT_MAP;

	fyp->simple_key_allowed = true;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	/* the comment indicator must have at least a space */
	c = fy_parse_peek(fyp);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			c != '#', err_out,
			"invalid comment after %s start", typestr);
	return 0;

err_out:
	rc = -1;

err_out_rc:
	return rc;
}

int fy_fetch_flow_collection_mark_end(struct fy_parser *fyp, int c)
{
	enum fy_token_type type = FYTT_NONE;
	enum fy_flow_type flow;
	const char *typestr, *markerstr;
	int i, rc;
	bool did_purge;
	struct fy_mark mark;
	struct fy_token *fyt;

	fy_get_mark(fyp, &mark);

	if (c == ']') {
		flow = FYFT_SEQUENCE;
		type = FYTT_FLOW_SEQUENCE_END;
		typestr = "sequence";
		markerstr = "bracket";
	} else {
		flow = FYFT_MAP;
		type = FYTT_FLOW_MAPPING_END;
		typestr = "mapping";
		markerstr = "brace";
	}

	FYP_MARK_ERROR_CHECK(fyp, &fyp->last_comma_mark, &fyp->last_comma_mark, FYEM_SCAN,
			!fyp_json_mode(fyp) || !fyp->last_was_comma, err_out,
			"JSON disallows trailing comma before closing %s", markerstr);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented %s end in flow mode", typestr);

	rc = fy_remove_simple_key(fyp, type);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_remove_simple_key() failed");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fyp->flow_level, err_out,
			"flow %s with invalid extra closing %s",
				typestr, markerstr);

	fyp->flow_level--;

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fyp->flow == flow, err_out,
			"mismatched flow %s end", typestr);

	/* pop the flow type */
	rc = fy_parse_flow_pop(fyp);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_parse_flow_pop() failed");

	fyp->simple_key_allowed = false;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n",
				fyp->simple_key_allowed ? "true" : "false");

	fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, type, 1);
	fyp_error_check(fyp, fyt, err_out,
			"fy_token_queue_simple() failed");

	if (fyp->parse_flow_only && fyp->flow_level == 0) {
		rc = fy_fetch_stream_end(fyp);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_stream_end() failed");
		return 0;
	}

	/* the comment indicator must have at least a space */
	c = fy_parse_peek(fyp);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			c != '#', err_out,
			"invalid comment after end of flow %s", typestr);

	/* due to the weirdness with simple keys and multiline flow keys scan forward
	* until a linebreak, ';', or anything else */
	for (i = 0; ; i++) {
		c = fy_parse_peek_at(fyp, i);
		if (c < 0 || c == ':' || fyp_is_lb(fyp, c) || !fy_is_ws(c))
			break;
	}

	/* we must be a key, purge */
	if (c == ':') {
		if (fy_any_simple_keys(fyp)) {
			rc = fy_purge_stale_simple_keys(fyp, &did_purge, type);
			fyp_error_check(fyp, !rc, err_out_rc,
					"fy_purge_stale_simple_keys() failed");

			/* if we did purge and the the list is now empty, we're hosed */
			if (did_purge && fy_simple_key_list_empty(&fyp->simple_keys)) {
				FYP_PARSE_ERROR(fyp, 0, 1, FYEM_SCAN,
						"invalid multiline flow %s key ", typestr);
				goto err_out;
			}
		}
	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_fetch_flow_collection_entry(struct fy_parser *fyp, int c)
{
	enum fy_token_type type = FYTT_NONE;
	struct fy_token *fyt, *fyt_last;
	struct fy_atom *handle;
	int rc;

	type = FYTT_FLOW_ENTRY;

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented entry seperator in flow mode");

	/* transform '? a,' to '? a: ,' */
	if (fyp->pending_complex_key_column >= 0) {

		fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, FYTT_VALUE, 0);
		fyp_error_check(fyp, fyt, err_out,
				"fy_token_queue_simple() failed");

		fyp->pending_complex_key_column = -1;

	}

	rc = fy_remove_simple_key(fyp, type);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_remove_simple_key() failed");

	fyp->simple_key_allowed = true;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	fyt_last = fy_token_list_tail(&fyp->queued_tokens);

	fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, type, 1);
	fyp_error_check(fyp, fyt, err_out,
			"fy_token_queue_simple() failed");

	/* the comment indicator must have at least a space */
	c = fy_parse_peek(fyp);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			c != '#', err_out,
			"invalid comment after comma");

	/* skip white space */
	while (fy_is_ws(c = fy_parse_peek(fyp)))
		fy_advance(fyp, c);

	if (c == '#') {
		if (fyt_last)
			fyt = fyt_last;

		handle = NULL;
		if (fyp->cfg.flags & FYPCF_PARSE_COMMENTS)
			handle = fy_token_comment_handle(fyt, fycp_right, true);

		rc = fy_scan_comment(fyp, handle, true);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_scan_comment() failed");
	}

	return 0;
err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_fetch_block_entry(struct fy_parser *fyp, int c)
{
	int rc;
	struct fy_mark mark;
	struct fy_simple_key *fysk;
	struct fy_token *fyt;

	fyp_error_check(fyp, c == '-', err_out,
			"illegal block entry");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			(!fyp->flow_level || (fyp_column(fyp) + 2) > fyp->indent) ||
			((fyp->cfg.flags & FYPCF_SLOPPY_FLOW_INDENTATION) && fyp->flow_level), err_out,
			"wrongly indented block sequence in flow mode");

	if (!(fyp->flow_level || fyp->simple_key_allowed)) {
		if (!fyp->simple_key_allowed && fyp->state == FYPS_BLOCK_MAPPING_VALUE)
			FYP_PARSE_ERROR(fyp, 0, 1, FYEM_SCAN,
					"block sequence on the same line as a mapping key");
		else if (fyp->state == FYPS_BLOCK_SEQUENCE_FIRST_ENTRY ||
			 fyp->state == FYPS_BLOCK_SEQUENCE_ENTRY)
			FYP_PARSE_ERROR(fyp, 0, 1, FYEM_SCAN,
					"block sequence on the same line as a previous item");
		else
			FYP_PARSE_ERROR(fyp, 0, 1, FYEM_SCAN,
					"block sequence entries not allowed in this context");
		goto err_out;
	}

	/* we have to save the start mark */
	fy_get_mark(fyp, &mark);

	if (fyp_block_mode(fyp) && fyp->indent < fyp_column(fyp)) {

		/* push the new indent level */
		rc = fy_push_indent(fyp, fyp_column(fyp), false, fyp_line(fyp));
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_push_indent() failed");

		fyt = fy_token_queue_simple_internal(fyp, &fyp->queued_tokens, FYTT_BLOCK_SEQUENCE_START, 0);
		fyp_error_check(fyp, fyt, err_out,
				"fy_token_queue_simple_internal() failed");
	}

	if (c == '-' && fyp->flow_level) {
		/* this is an error, but we let the parser catch it */
		;
	}

	fysk = fy_would_remove_required_simple_key(fyp);

	if (fysk) {
		if (fysk->token) {
			if (fysk->token->type == FYTT_ANCHOR || fysk->token->type == FYTT_TAG)
				FYP_TOKEN_ERROR(fyp, fysk->token, FYEM_SCAN,
					"invalid %s indent for sequence",
						fysk->token->type == FYTT_ANCHOR	?
							"anchor" : "tag");
			else
				FYP_TOKEN_ERROR(fyp, fysk->token, FYEM_SCAN,
					"missing ':'");
		} else
			FYP_PARSE_ERROR(fyp, 0, 1, FYEM_SCAN,
				"missing ':'");
		goto err_out;
	}

	rc = fy_remove_simple_key(fyp, FYTT_BLOCK_ENTRY);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_remove_simple_key() failed");

	fyp->simple_key_allowed = true;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, FYTT_BLOCK_ENTRY, 1);
	fyp_error_check(fyp, fyt, err_out,
			"fy_token_queue_simple() failed");

	rc = fy_ws_indentation_check(fyp, NULL, NULL);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_ws_indentation_check() failed");

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_fetch_key(struct fy_parser *fyp, int c)
{
	int rc;
	struct fy_mark mark;
	struct fy_simple_key_mark skm;
	bool target_simple_key_allowed;
	struct fy_token *fyt;
	struct fy_atom *handle;
	bool found_tab;
	struct fy_mark tab_mark;

	fyp_error_check(fyp, c == '?', err_out,
			"illegal block entry or key mark");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented mapping key in flow mode");

	fy_get_simple_key_mark(fyp, &skm);

	/* we have to save the start mark */
	fy_get_mark(fyp, &mark);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fyp->flow_level || fyp->simple_key_allowed, err_out,
			"invalid mapping key (not allowed in this context)");

	if (fyp_block_mode(fyp) && fyp->indent < fyp_column(fyp)) {

		/* push the new indent level */
		rc = fy_push_indent(fyp, fyp_column(fyp), true, fyp_line(fyp));
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_push_indent() failed");

		fyt = fy_token_queue_simple_internal(fyp, &fyp->queued_tokens, FYTT_BLOCK_MAPPING_START, 0);
		fyp_error_check(fyp, fyt, err_out,
				"fy_token_queue_simple_internal() failed");
	}

	rc = fy_remove_simple_key(fyp, FYTT_KEY);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_remove_simple_key() failed");

	target_simple_key_allowed = !fyp->flow_level;

	fyp->pending_complex_key_column = fyp_column(fyp);
	fyp->pending_complex_key_mark = mark;
	fyp_scan_debug(fyp, "pending_complex_key_column %d",
			fyp->pending_complex_key_column);

	fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, FYTT_KEY, 1);
	fyp_error_check(fyp, fyt, err_out_rc,
			"fy_token_queue_simple() failed");
	/* extra KEY data */
	fyt->key.flow_level = fyp->flow_level;

	fyp->simple_key_allowed = target_simple_key_allowed;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	rc = fy_ws_indentation_check(fyp, &found_tab, &tab_mark);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_ws_indentation_check() failed");

	/* record whether a tab was used for indentation */
	if (fyp->simple_key_allowed && found_tab) {
		fyp->tab_used_for_ws = true;
		fyp->last_tab_used_for_ws_mark = tab_mark;
	} else
		fyp->tab_used_for_ws = false;	// XXX

	/* comment? */
	if (c == '#') {

		handle = NULL;
		if (fyp->cfg.flags & FYPCF_PARSE_COMMENTS)
			handle = fy_token_comment_handle(fyt, fycp_right, true);

		rc = fy_scan_comment(fyp, handle, false);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_scan_comment() failed");
	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_fetch_value(struct fy_parser *fyp, int c)
{
	struct fy_token_list sk_tl;
	struct fy_simple_key *fysk = NULL;
	struct fy_mark mark, mark_insert, mark_end_insert;
	struct fy_token *fyt_insert, *fyt;
	bool target_simple_key_allowed, is_complex, has_bmap;
	bool push_bmap_start, push_key_only, did_purge, final_complex_key;
	bool is_multiline __FY_DEBUG_UNUSED__;
	struct fy_atom *chandle;
	bool found_tab;
	struct fy_mark tab_mark;
	int rc;

	fyp_error_check(fyp, c == ':', err_out,
		"illegal value mark");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			!fyp_json_mode(fyp) || fyp->flow == FYFT_MAP, err_out,
			"JSON considers keys when not in mapping context invalid");

	/* special handling for :: weirdness */
	if (!fyp_json_mode(fyp) && fyp->flow_level > 0) {
		int adv, nextc, nextcol, tabsize, indent;

		/* this requires some explanation...
		 * we need to detect x::x, x: :x, and x:\n:x as the same
		 */
		adv = 1;
		indent = fyp->indent;
		nextcol = fyp_column(fyp) + 1;
		tabsize = fyp_tabsize(fyp);

		while ((nextc = fy_parse_peek_at(fyp, adv)) > 0) {

			if (fyp_is_lb(fyp, nextc))
				nextcol = 0;
			else if (fy_is_tab(nextc)) {
				if (tabsize)
					nextcol += tabsize - (nextcol % tabsize);
				else
					nextcol++;
			} else if (fy_is_space(nextc))
				nextcol++;
			else {
				if (!fy_flow_indent_check_internal(fyp, nextcol, indent))
					nextc = -1;
				break;
			}

			adv++;
		}

		fyp->colon_follows_colon = nextc == ':';
	} else
		fyp->colon_follows_colon = false;

	fy_get_mark(fyp, &mark);

	fy_token_list_init(&sk_tl);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented mapping value in flow mode");

	if (fy_any_simple_keys(fyp)) {
		rc = fy_purge_stale_simple_keys(fyp, &did_purge, FYTT_VALUE);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_purge_stale_simple_keys() failed");
	}

	/* get the simple key (if available) for the value */
	fysk = fy_simple_key_list_head(&fyp->simple_keys);
	if (fysk && fysk->flow_level == fyp->flow_level)
		fy_simple_key_list_del(&fyp->simple_keys, fysk);
	else
		fysk = NULL;

	if (!fysk) {
		fyp_scan_debug(fyp, "no simple key flow_level=%d", fyp->flow_level);

		fyt_insert = fy_token_list_tail(&fyp->queued_tokens);
		mark_insert = mark;
		mark_end_insert = mark;
	} else {
		assert(fysk->flow_level == fyp->flow_level);
		fyt_insert = fysk->token;
		mark_insert = fysk->mark;
		mark_end_insert = fysk->end_mark;

		fyp_scan_debug(fyp, "have simple key flow_level=%d", fyp->flow_level);
	}

	fyp_scan_debug(fyp, "flow_level=%d, column=%d parse_indent=%d",
			fyp->flow_level, mark_insert.column, fyp->indent);

	is_complex = fyp->pending_complex_key_column >= 0;
	final_complex_key = is_complex && (fyp->flow_level || fyp_column(fyp) <= fyp->pending_complex_key_mark.column);
	is_multiline = mark_end_insert.line < fyp_line(fyp);
	has_bmap = fyp->generated_block_map;
	push_bmap_start = (!fyp->flow_level && mark_insert.column > fyp->indent);
	push_key_only = (!is_complex && (fyp->flow_level || has_bmap)) ||
		        (is_complex && !final_complex_key);

	fyp_scan_debug(fyp, "mark_insert.line=%d/%d mark_end_insert.line=%d/%d fyp->line=%d",
			mark_insert.line, mark_insert.column,
			mark_end_insert.line, mark_end_insert.column,
			fyp_line(fyp));

	fyp_scan_debug(fyp, "simple_key_allowed=%s is_complex=%s final_complex_key=%s is_multiline=%s has_bmap=%s push_bmap_start=%s push_key_only=%s",
			fyp->simple_key_allowed ? "true" : "false",
			is_complex ? "true" : "false",
			final_complex_key ? "true" : "false",
			is_multiline ? "true" : "false",
			has_bmap ? "true" : "false",
			push_bmap_start ? "true" : "false",
			push_key_only ? "true" : "false");

	if (!is_complex && is_multiline && (!fyp->flow_level || fyp->flow != FYFT_MAP)) {
		FYP_PARSE_ERROR(fyp, 0, 1, FYEM_SCAN, "Illegal placement of ':' indicator");
		goto err_out;
	}

	/* special handling for ?: */
	if (fyp->tab_used_for_ws) {
		FYP_PARSE_ERROR(fyp, 0, 1, FYEM_SCAN, "Indentation used tabs for ':' indicator");
		goto err_out;
	}

	if (push_bmap_start) {

		assert(!fyp->flow_level);

		fyp_scan_debug(fyp, "--- parse_roll");

		/* push the new indent level */
		rc = fy_push_indent(fyp, mark_insert.column, true, mark_insert.line);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_push_indent() failed");

		fyt = fy_token_queue_simple_internal(fyp, &sk_tl, FYTT_BLOCK_MAPPING_START, 0);
		fyp_error_check(fyp, fyt, err_out,
				"fy_token_queue_simple_internal() failed");

		/* update with this mark */
		fyt->handle.start_mark = fyt->handle.end_mark = mark_insert;
	}

	if (push_bmap_start || push_key_only) {

		fyt = fy_token_queue_simple_internal(fyp, &sk_tl, FYTT_KEY, 0);
		fyp_error_check(fyp, fyt, err_out,
				"fy_token_queue_simple_internal() failed");

		/* update with the flow level */
		fyt->key.flow_level = fyp->flow_level;
	}

#ifdef FY_DEVMODE
	fyp_debug_dump_token(fyp, fyt_insert, "insert-token: ");
	fyp_debug_dump_token_list(fyp, &fyp->queued_tokens, fyt_insert, "fyp->queued_tokens (before): ");
	fyp_debug_dump_token_list(fyp, &sk_tl, NULL, "sk_tl: ");
#endif

	if (fyt_insert) {

		if (fysk)
			fy_token_list_splice_before(&fyp->queued_tokens, fyt_insert, &sk_tl);
		else
			fy_token_list_splice_after(&fyp->queued_tokens, fyt_insert, &sk_tl);
	} else
		fy_token_lists_splice(&fyp->queued_tokens, &sk_tl);

#ifdef FY_DEVMODE
	fyp_debug_dump_token_list(fyp, &fyp->queued_tokens, fyt_insert, "fyp->queued_tokens (after): ");
#endif

	target_simple_key_allowed = fysk ? false : !fyp->flow_level;

	fyt = fy_token_queue_simple(fyp, &fyp->queued_tokens, FYTT_VALUE, 1);
	fyp_error_check(fyp, fyt, err_out,
			"fy_token_queue_simple() failed");

	if (fysk) {
		fy_parse_simple_key_recycle(fyp, fysk);
		fysk = NULL;
	}


	fyp->simple_key_allowed = target_simple_key_allowed;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	if (is_complex) {
		rc = fy_ws_indentation_check(fyp, &found_tab, &tab_mark);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_ws_indentation_check() failed");

		/* record whether a tab was used for indentation */
		if (fyp->simple_key_allowed && found_tab) {
			fyp->tab_used_for_ws = true;
			fyp->last_tab_used_for_ws_mark = tab_mark;
		} else
			fyp->tab_used_for_ws = false;	// XXX
	} else
		fyp->tab_used_for_ws = false;

	if (final_complex_key) {
		fyp->pending_complex_key_column = -1;
		fyp_scan_debug(fyp, "pending_complex_key_column -> %d",
				fyp->pending_complex_key_column);
	}

	if (fyt_insert) {
		/* eat whitespace */
		while (fy_is_blank(c = fy_parse_peek(fyp)))
			fy_advance(fyp, c);

		/* comment? */
		if (c == '#') {
			chandle = NULL;
			if (fyp->cfg.flags & FYPCF_PARSE_COMMENTS)
				chandle = fy_token_comment_handle(fyt_insert, fycp_right, true);

			rc = fy_scan_comment(fyp, chandle, false);
			fyp_error_check(fyp, !rc, err_out_rc,
					"fy_scan_comment() failed");
		}

	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	fy_parse_simple_key_recycle(fyp, fysk);
	return rc;
}

int fy_fetch_anchor_or_alias(struct fy_parser *fyp, int c)
{
	struct fy_atom handle;
	enum fy_token_type type;
	int i = 0, rc = -1, length;
	struct fy_simple_key_mark skm;
	struct fy_token *fyt;
	const char *typestr;

	fyp_error_check(fyp, c == '*' || c == '&', err_out,
			"illegal anchor mark (not '*' or '&')");

	if (c == '*') {
		type = FYTT_ALIAS;
		typestr = "alias";
	} else {
		type = FYTT_ANCHOR;
		typestr = "anchor";
	}

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented %s in flow mode", typestr);

	/* we have to save the start mark (including the anchor/alias start) */
	fy_get_simple_key_mark(fyp, &skm);

	/* skip over the anchor mark */
	fy_advance(fyp, c);

	/* start mark */
	fy_fill_atom_start(fyp, &handle);

	length = 0;

	while ((c = fy_parse_peek(fyp)) >= 0) {
		if (fyp_is_blankz(fyp, c) || fy_is_flow_indicator(c) ||
		    fy_is_unicode_control(c) || fy_is_unicode_space(c))
			break;
		fy_advance(fyp, c);
		length++;
	}

	if (!fyp_is_blankz(fyp, c) && !fy_is_flow_indicator(c)) {

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				fy_is_unicode_control(c), err_out,
				"illegal unicode control character in %s", typestr);

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				fy_is_unicode_space(c), err_out,
				"illegal unicode space character in %s", typestr);
	}

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			c != FYUG_INV, err_out,
			"invalid character in %s", typestr);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			c != FYUG_PARTIAL, err_out,
			"partial character in %s", typestr);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			length > 0, err_out,
			"invalid %s detected", typestr);

	fy_fill_atom_end(fyp, &handle);

	handle.storage_hint = length;
	handle.storage_hint_valid = true;
	handle.direct_output = true;
	handle.empty = false;
	handle.has_lb = false;
	handle.has_ws = false;
	handle.starts_with_ws = false;
	handle.starts_with_lb = false;
	handle.ends_with_ws = false;
	handle.ends_with_lb = false;
	handle.trailing_lb = false;
	handle.size0 = false;
	handle.valid_anchor = true;

	if (type == FYTT_ALIAS)
		fyt = fy_token_queue(fyp, type, &handle, NULL);
	else
		fyt = fy_token_queue(fyp, type, &handle);
	fyp_error_check(fyp, fyt, err_out_rc,
			"fy_token_queue() failed");

	/* scan forward for '-' block sequence indicator */
	if (type == FYTT_ANCHOR && !fyp->flow_level) {
		for (i = 0; ; i++) {
			c = fy_parse_peek_at(fyp, i);
			if (c < 0 || fyp_is_lb(fyp, c) || !fy_is_ws(c))
				break;
		}

		/* if it's '-' followed by ws we have a problem */
		FYP_PARSE_ERROR_CHECK(fyp, i, 1, FYEM_SCAN,
				!(c == '-' && fy_is_ws(fy_parse_peek_at(fyp, i + 1))), err_out,
				"illegal block sequence on the same line as anchor");
	}


	rc = fy_save_simple_key_mark(fyp, &skm, type, NULL);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_save_simple_key_mark() failed");

	fyp->simple_key_allowed = false;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_fetch_tag(struct fy_parser *fyp, int c)
{
	struct fy_atom handle;
	int rc = -1, total_length, handle_length, uri_length, i, prefix_length, suffix_length;
	const char *handlep;
	bool is_valid;
	struct fy_simple_key_mark skm;
	struct fy_document_state *fyds;
	struct fy_token *fyt_td;
	struct fy_token *fyt;

	fyp_error_check(fyp, c == '!', err_out,
			"illegal tag mark (not '!')");

	FYP_PARSE_ERROR_CHECK(fyp, 0 ,1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented tag in flow mode");

	fyds = fyp->current_document_state;

	fy_get_simple_key_mark(fyp, &skm);

	if (fy_parse_peek_at(fyp, 1) == '<') {
		/* skip over '!<' and '>' */
		prefix_length = 2;
		suffix_length = 1;
	} else
		prefix_length = suffix_length = 0;

	if (prefix_length)
		handle_length = 0; /* set the handle to '' */
	else {
		/* either !suffix or !handle!suffix */
		/* we scan back to back, and split handle/suffix */
		handle_length = fy_scan_tag_handle_length(fyp, prefix_length);
		fyp_error_check(fyp, handle_length > 0, err_out,
				"fy_scan_tag_handle_length() failed");
	}

	uri_length = fy_scan_tag_uri_length(fyp, prefix_length + handle_length);
	fyp_error_check(fyp, uri_length >= 0, err_out,
			"fy_scan_tag_uri_length() failed");

	/* a handle? */
	if (!prefix_length && (handle_length == 0 || fy_parse_peek_at(fyp, handle_length - 1) != '!')) {
		/* special case, '!', handle set to '' and suffix to '!' */
		if (handle_length == 1 && uri_length == 0) {
			handle_length = 0;
			uri_length = 1;
		} else {
			uri_length = handle_length - 1 + uri_length;
			handle_length = 1;
		}
	}

	is_valid = fy_scan_tag_uri_is_valid(fyp, prefix_length + handle_length, uri_length);
	fyp_error_check(fyp, is_valid, err_out,
			"tag URI is invalid");

	if (suffix_length > 0) {
		c = fy_parse_peek_at(fyp, prefix_length + handle_length + uri_length);

		FYP_PARSE_ERROR_CHECK(fyp, prefix_length + handle_length + uri_length, 1, FYEM_SCAN,
				c == '>', err_out,
				"missing '>' uri terminator");
	}

	total_length = prefix_length + handle_length + uri_length + suffix_length;
	fy_fill_atom(fyp, total_length, &handle);
	handle.style = FYAS_URI;	/* this is a URI, need to handle URI escapes */

	c = fy_parse_peek(fyp);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fyp_is_blankz(fyp, c) || fy_utf8_strchr(",}]", c), err_out,
			"invalid tag terminator");

	handlep = fy_atom_data(&handle) + prefix_length;

	fyt_td = fy_document_state_lookup_tag_directive(fyds, handlep, handle_length);

	FYP_MARK_ERROR_CHECK(fyp, &handle.start_mark, &handle.end_mark, FYEM_PARSE,
			fyt_td, err_out,
			"undefined tag prefix");

	fyt = fy_token_queue(fyp, FYTT_TAG, &handle, prefix_length, handle_length, uri_length, fyt_td);
	fyp_error_check(fyp, fyt, err_out_rc,
			"fy_token_queue() failed");

	/* scan forward for '-' block sequence indicator */
	if (!fyp->flow_level) {
		for (i = 0; ; i++) {
			c = fy_parse_peek_at(fyp, i);
			if (c < 0 || fyp_is_lb(fyp, c) || !fy_is_ws(c))
				break;
		}

		/* if it's '-' followed by ws we have a problem */
		FYP_PARSE_ERROR_CHECK(fyp, i ,1, FYEM_SCAN,
				!(c == '-' && fy_is_ws(fy_parse_peek_at(fyp, i + 1))), err_out,
				"illegal block sequence on the same line as the tag");
	}

	rc = fy_save_simple_key_mark(fyp, &skm, FYTT_TAG, NULL);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_save_simple_key_mark() failed");

	fyp->simple_key_allowed = false;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_scan_block_scalar_indent(struct fy_parser *fyp, int indent, int *breaks, int *breaks_length,
				int *presentation_breaks_length, int *first_break_length, int *lastc)
{
	int c, max_indent = 0, min_indent, break_length;

	*breaks = 0;
	*breaks_length = 0;
	*presentation_breaks_length = 0;
	*first_break_length = 0;

	/* minimum indent is 0 for zero indent scalars */
	min_indent = fyp->document_first_content_token ? 0 : 1;

	/* scan over the indentation spaces */
	/* we don't format content for display */
	for (;;) {

		/* skip over indentation */

		if (!fyp_tabsize(fyp)) {
			/* we must respect the enclosed indent */
			while (fyp_column(fyp) <= fyp->indent && fy_is_ws(c = fy_parse_peek(fyp))) {
				FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
						!fy_is_tab(c), err_out,
						"invalid tab character as indent instead of space");
				fy_advance(fyp, c);
			}

			/* skip over spaces only */
			while ((c = fy_parse_peek(fyp)) == ' ' &&
					(!indent || fyp_column(fyp) < indent)) {
				fy_advance(fyp, c);
			}
		} else {
			while (fy_is_ws((c = fy_parse_peek(fyp))) &&
				(!indent || fyp_column(fyp) < indent))
				fy_advance(fyp, c);
		}

		if (fyp_column(fyp) > max_indent)
			max_indent = fyp_column(fyp);

		/* non-empty line or EOF */
		if (!fyp_is_lb(fyp, c)) {
			*lastc = c;
			break;
		}

		fy_advance(fyp, c);

		break_length = fy_utf8_width(c);

		(*breaks)++;
		(*breaks_length) += 1;

		if (fy_is_lb_LS_PS(c))
			(*presentation_breaks_length) += break_length;

		if (!*first_break_length)
			*first_break_length = break_length;
	}

	if (!indent) {
		indent = max_indent;
		if (indent < fyp->indent)
			indent = fyp->indent;
		if (indent < min_indent)
			indent = min_indent;
	}

	return indent;

err_out:
	return -1;
}

int fy_fetch_block_scalar(struct fy_parser *fyp, bool is_literal, int c)
{
	struct fy_atom handle;
	enum fy_atom_chomp chomp = FYAC_CLIP;	/* default */
	int lastc, rc, increment = 0, current_indent, new_indent, indent = 0;
	int breaks, breaks_length, presentation_breaks_length, first_break_length;
	bool doc_start_end_detected, empty, empty_line, prev_empty_line, indented, prev_indented, first;
	bool has_ws, has_lb, starts_with_ws, starts_with_lb, ends_with_ws, ends_with_lb, trailing_lb;
	bool pending_nl, ends_with_eof, starts_with_eof;
	struct fy_token *fyt;
	size_t length, line_length, trailing_ws, trailing_breaks_length;
	size_t leading_ws;
	size_t prefix_length, suffix_length;
	unsigned int chomp_amt;
	int actual_lb_length, pending_lb_length;
	struct fy_mark indicator_mark;
	bool generated_indent;
	char *str;
#ifdef ATOM_SIZE_CHECK
	size_t tlength;
#endif

    fy_utf8_format_a(c, fyue_singlequote, &str);
	fyp_error_check(fyp, c == '|' || c == '>', err_out,
			"bad start of block scalar ('%s')",
				str);

	fy_get_mark(fyp, &indicator_mark);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented block scalar in flow mode");

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_block_indent_check(fyp), err_out,
			"wrongly indented block scalar in block mode");

	rc = fy_remove_simple_key(fyp, FYTT_SCALAR);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_remove_simple_key() failed");

	fyp->simple_key_allowed = true;
	fyp->tab_used_for_ws = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	/* skip over block scalar start */
	fy_advance(fyp, c);

	/* intentation indicator (either [-+]<digit> or <digit>[-+] */
	c = fy_parse_peek(fyp);
	if (c == '+' || c == '-') {

		chomp = c == '+' ? FYAC_KEEP : FYAC_STRIP;

		fy_advance(fyp, c);

		c = fy_parse_peek(fyp);
		if (fy_is_num(c)) {
			increment = c - '0';
			fyp_error_check(fyp, increment != 0, err_out,
					"indentation indicator 0");
			fy_advance(fyp, c);
		}
	} else if (fy_is_num(c)) {

		increment = c - '0';
		fyp_error_check(fyp, increment != 0, err_out,
				"indentation indicator 0");
		fy_advance(fyp, c);

		c = fy_parse_peek(fyp);
		if (c == '+' || c == '-') {
			chomp = c == '+' ? FYAC_KEEP : FYAC_STRIP;
			fy_advance(fyp, c);
		}
	}

	/* the comment indicator must have at least a space */
	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			c != '#', err_out,
			"invalid comment without whitespace after block scalar indicator");

	/* eat whitespace */
	while (fy_is_blank(c = fy_parse_peek(fyp)))
		fy_advance(fyp, c);

	/* comment? */
	if (c == '#') {
		/* XXX just ignore this one */
		rc = fy_scan_comment(fyp, NULL, true);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_scan_comment() failed");
	}

	c = fy_parse_peek(fyp);

	/* end of the line? */
	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fyp_is_lbz(fyp, c), err_out,
			"block scalar no linebreak found");

	/* if the block scalar indicator is on a different line we need a new indent */
	generated_indent = false;
	if (!increment && indicator_mark.line != fyp->indent_line) {
		fyp_scan_debug(fyp, "generating indent %d/%d\n", indicator_mark.line, fyp->indent_line);
		rc = fy_push_indent(fyp, indicator_mark.column, false, indicator_mark.line);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_push_indent() failed");
		generated_indent = true;
	}

	/* advance */
	fy_advance(fyp, c);

	fy_fill_atom_start(fyp, &handle);

	starts_with_eof = c < 0;

	current_indent = fyp->indent >= 0 ? fyp->indent : 0;
	indent = increment ? current_indent + increment : 0;

	length = 0;
	trailing_breaks_length = 0;

	empty = true;
	has_ws = false;
	has_lb = false;
	starts_with_ws = false;
	starts_with_lb = false;
	ends_with_ws = false;
	ends_with_lb = false;
	trailing_lb = false;

	new_indent = fy_scan_block_scalar_indent(fyp, indent, &breaks, &breaks_length, &presentation_breaks_length, &first_break_length, &lastc);
	fyp_error_check(fyp, new_indent >= 0, err_out,
			"fy_scan_block_scalar_indent() failed");

	length = breaks_length;
	length += presentation_breaks_length;
	indent = new_indent;

	doc_start_end_detected = false;
	prev_empty_line = true;

	prefix_length = 0;
	suffix_length = 0;

	prev_indented = false;
	first = true;
	pending_nl = false;
	pending_lb_length = 0;

	chomp_amt = increment ? (unsigned int)(current_indent + increment) : (unsigned int)-1;

	actual_lb_length = 1;
	while ((c = fy_parse_peek(fyp)) > 0 && fyp_column(fyp) >= indent) {

		lastc = c;

		if (first) {
			if (fy_is_ws(c))
				starts_with_ws = true;
			else if (fyp_is_lb(fyp, c))
				starts_with_lb = true;
		}

		/* consume the list */
		line_length = 0;
		trailing_ws = 0;
		empty_line = true;
		leading_ws = 0;

		indented = fy_is_ws(fy_parse_peek(fyp));

		while (!(fyp_is_lbz(fyp, c = fy_parse_peek(fyp)))) {

			lastc = c;

			if (fyp_column(fyp) == 0 &&
			    (!fy_parse_strncmp(fyp, "...", 3) ||
			     !fy_parse_strncmp(fyp, "---", 3)) &&
			    fy_is_blankz_at_offset(fyp, 3)) {
				doc_start_end_detected = true;
				break;
			}

			if (!fy_is_space(c)) {
				empty = false;
				empty_line = false;
				trailing_ws = 0;
				if (chomp_amt == (unsigned int)-1)
					chomp_amt = fyp_column(fyp);
			} else {
				has_ws = true;
				if (empty_line)
					leading_ws++;
				trailing_ws++;
			}

			fy_advance(fyp, c);

			line_length += fy_utf8_width(c);
		}

		if (doc_start_end_detected)
			break;

		if (!fy_is_z(c)) {
			/* eat line break */
			actual_lb_length = fy_utf8_width(c);
			fy_advance(fyp, c);

			has_lb = true;
			new_indent = fy_scan_block_scalar_indent(fyp, indent, &breaks, &breaks_length, &presentation_breaks_length, &first_break_length, &lastc);
			fyp_error_check(fyp, new_indent >= 0, err_out,
					"fy_scan_block_scalar_indent() failed");
			if (fy_is_lb_LS_PS(c))
				presentation_breaks_length += actual_lb_length;
		} else {
			has_lb = false;
			new_indent = indent;
			// was chomp = FYAC_STRIP, very very wrong

			breaks = 0;
			breaks_length = 0;
			presentation_breaks_length = 0;
			first_break_length = 0;

			actual_lb_length = 0;

		}

		if (is_literal) {

			prefix_length = 0;

			if (pending_nl) {
				pending_nl = false;
				prefix_length += pending_lb_length;
				pending_lb_length = 0;
			}

			prefix_length += trailing_breaks_length;
			trailing_breaks_length = 0;

			suffix_length = 0;

			if (fy_is_lb_LS_PS(c)) {
				trailing_breaks_length += breaks_length;
				trailing_breaks_length += presentation_breaks_length;

				if (actual_lb_length > 1)
					presentation_breaks_length -= actual_lb_length;

				pending_nl = true;
				pending_lb_length = 0;
			} else {
				trailing_breaks_length += breaks_length;
				trailing_breaks_length += presentation_breaks_length;

				pending_nl = !empty_line || indented;
				pending_lb_length = pending_nl ? 1 : 0;
			}

		} else {

			prefix_length = 0;

			if (!trailing_breaks_length) {
				if (prev_indented || (prev_empty_line && !first) || indented) {
					/* previous line was indented or empty, force output newline */
					if (pending_nl) {
						pending_nl = false;
						prefix_length += 1; // pending_lb_length;
						pending_lb_length = 0;
					}
				} else if (!prev_empty_line && !prev_indented && !indented && !empty_line) {
					/* previous line was not empty and not indented
					* while this is not indented and not empty need sep */
					if (pending_nl) {
						pending_nl = false;
						prefix_length += 1; // pending_lb_length;
						pending_lb_length = 0;
					}
				}
			} else {
				prefix_length += trailing_breaks_length;
				if (prev_indented || indented)
					prefix_length++;
			}
			pending_nl = true;
			pending_lb_length = actual_lb_length;

			trailing_breaks_length = 0;

			suffix_length = 0;
			trailing_breaks_length += breaks_length;
			trailing_breaks_length += presentation_breaks_length;
		}


		length += prefix_length + line_length + suffix_length;
		indent = new_indent;

		prev_empty_line = empty_line;
		prev_indented = indented;

		prefix_length = 0;
		suffix_length = 0;

		first = false;
	}

	if (empty) {
		trailing_breaks_length = breaks_length;
		trailing_breaks_length += presentation_breaks_length;
		length = 0;
	}

	/* end... */
	fy_fill_atom_end(fyp, &handle);

	if (c == FYUG_INV || c == FYUG_PARTIAL) {
		FYP_MARK_ERROR(fyp, &handle.start_mark, &handle.end_mark, FYEM_SCAN,
			"block scalar is malformed UTF8");
		goto err_out;
	}

	/* are we ended with EOF? */
	ends_with_eof = starts_with_eof || (c == FYUG_EOF && !fyp_is_lb(fyp, lastc) && !breaks);

	/* detect wrongly indented block scalar */
	if (c != FYUG_EOF && !(!empty || fyp_column(fyp) <= fyp->indent || c == '#' || doc_start_end_detected)) {
		FYP_MARK_ERROR(fyp, &handle.start_mark, &handle.end_mark, FYEM_SCAN,
			"block scalar with wrongly indented line after spaces only");
		goto err_out;
	}

	if (empty && c == '#' && fyp_column(fyp) > fyp->indent) {
		FYP_MARK_ERROR(fyp, &handle.start_mark, &handle.end_mark, FYEM_SCAN,
			"empty block scalar with wrongly indented comment line after spaces only");
		goto err_out;
	}

	if (chomp_amt == (unsigned int)-1)
		chomp_amt = current_indent;

	switch (chomp) {
	case FYAC_CLIP:
		if (pending_nl || (!starts_with_eof && ends_with_eof)) {
			if (actual_lb_length <= 2)
				length += 1;
			else
				length += actual_lb_length;

			ends_with_lb = true;
			ends_with_ws = false;
		} else {
			if (trailing_breaks_length > 0)
				ends_with_lb = true;
			else if (fy_is_ws(lastc))
				ends_with_ws = true;
		}
		break;
	case FYAC_KEEP:
		if (pending_nl || (!starts_with_eof && ends_with_eof))
			length += actual_lb_length;

		length += breaks + presentation_breaks_length;

		trailing_lb = trailing_breaks_length > 0;
		if (pending_nl || (!starts_with_eof && ends_with_eof) || trailing_breaks_length) {
			ends_with_lb = true;
			ends_with_ws = false;
		} else if (fy_is_ws(lastc)) {
			ends_with_ws = true;
			ends_with_lb = false;
		}
		break;
	case FYAC_STRIP:
		ends_with_lb = false;
		if (fy_is_ws(lastc))
			ends_with_ws = true;
		break;
	}

	/* need to process to present */
	handle.style = is_literal ? FYAS_LITERAL : FYAS_FOLDED;
	handle.chomp = chomp;
	handle.increment = increment ? (unsigned int)(current_indent + increment) : chomp_amt;

	/* no point in trying to do direct output in a block scalar */
	/* TODO maybe revisit in the future */
	handle.direct_output = false;
	handle.empty = empty;
	handle.has_lb = has_lb;
	handle.has_ws = has_ws;
	handle.starts_with_ws = starts_with_ws;
	handle.starts_with_lb = starts_with_lb;
	handle.ends_with_ws = ends_with_ws;
	handle.ends_with_lb = ends_with_lb;
	handle.trailing_lb = trailing_lb;
	handle.size0 = length == 0;
	handle.valid_anchor = false;
	handle.json_mode = fyp_json_mode(fyp);
	handle.lb_mode = fyp_lb_mode(fyp);
	handle.fws_mode = fyp_fws_mode(fyp);
	handle.tabsize = fyp_tabsize(fyp);
	handle.ends_with_eof = ends_with_eof;

#ifdef ATOM_SIZE_CHECK
	tlength = fy_atom_format_text_length(&handle);
	if (tlength != length) {
        fy_utf8_format_text_a(fy_atom_data(&handle), fy_atom_size(&handle), fyue_doublequote, &str);
		fyp_warning(fyp, "%s: storage hint calculation failed real %zu != hint %zu - \"%s\"", __func__,
			tlength, length,
			str);
		length = tlength;
	}
#endif

	handle.storage_hint = length;
	handle.storage_hint_valid = true;

	fyt = fy_token_queue(fyp, FYTT_SCALAR, &handle, is_literal ? FYSS_LITERAL : FYSS_FOLDED);
	fyp_error_check(fyp, fyt, err_out_rc,
			"fy_token_queue() failed");

	if (fyp->cfg.flags & FYPCF_PARSE_COMMENTS) {
		rc = fy_attach_comments_if_any(fyp, fyt);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_attach_right_hand_comment() failed");
	}

	if (generated_indent) {
		rc = fy_pop_indent(fyp);
		fyp_error_check(fyp, !rc, err_out,
				"fy_pop_indent() failed");

		/* the ident line has now moved */
		fyp->indent_line = fyp_line(fyp);
	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_reader_fetch_flow_scalar_handle(struct fy_reader *fyr, int c, int indent, struct fy_atom *handle, bool sloppy_indent)
{
	size_t length;
	int code_length, i = 0, j, end_c, last_line, lastc;
	int breaks_found, blanks_found, break_run, total_code_length;
	int breaks_found_length, first_break_length, value;
	uint32_t hi_surrogate, lo_surrogate;
	bool is_single, is_multiline, esc_lb, ws_lb_only, has_ws, has_lb, has_esc;
	bool first, starts_with_ws, starts_with_lb, ends_with_ws, ends_with_lb, trailing_lb = false;
	bool unicode_esc, is_json_unesc, has_json_esc;
	int last_esc_lb, break_length, presentation_breaks_length;
	struct fy_mark mark, mark2;
	char escbuf[1 + FY_UTF8_FORMAT_BUFMIN];
	size_t escbuf_len;
	enum fy_utf8_escape esc_mode;
	const char *ep;
	char *str;
#ifdef ATOM_SIZE_CHECK
	size_t tlength;
#endif

	(void)last_esc_lb;

	is_single = c == '\'';
	end_c = c;

    fy_utf8_format_a(c, fyue_singlequote, &str);
	fyr_error_check(fyr, c == '\'' || c == '"', err_out,
			"bad start of flow scalar ('%s')",
				str);

	fy_reader_get_mark(fyr, &mark);

	/* skip over block scalar start */
	fy_reader_advance(fyr, c);

	fy_reader_fill_atom_start(fyr, handle);

	length = 0;
	breaks_found = 0;
	breaks_found_length = 0;
	first_break_length = 0;
	presentation_breaks_length = 0;
	blanks_found = 0;
	esc_lb = false;
	last_esc_lb = -1;
	ws_lb_only = true;
	has_ws = false;
	has_lb = false;
	starts_with_ws = false;
	starts_with_lb = false;
	ends_with_ws = false;
	ends_with_lb = false;
	has_esc = false;
	break_run = 0;
	first = true;
	has_json_esc = false;

	esc_mode = fy_reader_json_mode(fyr) ? fyue_doublequote_json :
			fy_reader_lb_mode(fyr) == fylb_cr_nl ? fyue_doublequote : fyue_doublequote_yaml_1_1;

	last_line = -1;
	lastc = -1;
	for (;;) {
		if (!fy_reader_json_mode(fyr)) {
			/* no document indicators please */
			FYR_PARSE_ERROR_CHECK(fyr, 0, 3, FYEM_SCAN,
				!(fy_reader_column(fyr) == 0 &&
					(!fy_reader_strncmp(fyr, "---", 3) ||
					!fy_reader_strncmp(fyr, "...", 3)) &&
					fy_reader_is_blankz_at_offset(fyr, 3)), err_out,
				"invalid document-%s marker in %s scalar",
					c == '-' ? "start" : "end",
					is_single ? "single-quoted" : "double-quoted");
		}

		/* no EOF either */
		c = fy_reader_peek(fyr);

		if (c <= 0) {
			fy_reader_get_mark(fyr, &mark);

			if (!c || c == FYUG_EOF)
				FYR_MARK_ERROR(fyr, &handle->start_mark, &mark, FYEM_SCAN,
						"%s scalar without closing quote",
							is_single ? "single-quoted" : "double-quoted");
			else
				FYR_MARK_ERROR(fyr, &handle->start_mark, &mark, FYEM_SCAN,
						"%s scalar is malformed UTF8",
							is_single ? "single-quoted" : "double-quoted");
			goto err_out;
		}

		if (first) {
			if (fy_reader_is_flow_ws(fyr, c))
				starts_with_ws = true;
			else if (fy_reader_is_lb(fyr, c))
				starts_with_lb = true;
		}

		while (!fy_reader_is_flow_blankz(fyr, c = fy_reader_peek(fyr))) {

			if (ws_lb_only && !(fy_reader_is_flow_ws(fyr, c) || fy_reader_is_lb(fyr, c)) && c != end_c)
				ws_lb_only = false;

			esc_lb = false;
			last_esc_lb = -1;
			/* track line change (and first non blank) */
			if (last_line != fy_reader_line(fyr)) {
				last_line = fy_reader_line(fyr);

				if ((indent >= 0 && fy_reader_column(fyr) <= indent) && !sloppy_indent) {

					fy_reader_advance(fyr, c);
					fy_reader_get_mark(fyr, &mark2);
					FYR_MARK_ERROR(fyr, &mark, &mark2, FYEM_SCAN,
						"wrongly indented %s scalar",
							is_single ? "single-quoted" : "double-quoted");
					goto err_out;
				}
			}

			if (breaks_found) {
				length += breaks_found > 1 ? (breaks_found_length - first_break_length) : 1;
				length += presentation_breaks_length;
				breaks_found = 0;
				blanks_found = 0;
				presentation_breaks_length = 0;
			} else if (blanks_found) {
				length += blanks_found;
				lastc = ' ';
				blanks_found = 0;
			}

			if (c >= 0 && c <= 0x7f && (fy_utf8_low_ascii_flags[c] & F_SIMPLE_SCALAR)) {
				size_t len, consumed;
				const char *p, *s, *e;
				int8_t cc;
				int run;

				run = 0;
				while ((p = fy_reader_ensure_lookahead(fyr, 1, &len)) != NULL) {

					s = p;
					e = s + len;

					while (s < e && (cc = (int8_t)*s) >= 0 && (fy_utf8_low_ascii_flags[cc] & F_SIMPLE_SCALAR))
						s++;

					consumed = s - p;
					if (consumed) {
						fy_reader_advance_octets(fyr, consumed);
						fyr->column += consumed;
						lastc = (int)cc;
					}
					run += consumed;

					/* we're done if stopped earlier */
					if (s < e)
						break;
				}
				length += run;
				break_run = 0;
				continue;
			}

			/* escaped single quote? */
			if (is_single && c == '\'' && fy_reader_peek_at(fyr, 1) == '\'') {
				length++;
				fy_reader_advance_by(fyr, 2);
				break_run = 0;
				lastc = '\'';
				continue;
			}

			/* right quote? */
			if (c == end_c)
				break;

			/* escaped line break (any linebreak will do) */
			if (!is_single && c == '\\' && fy_reader_is_lb(fyr, fy_reader_peek_at(fyr, 1))) {

				esc_lb = true;
				last_esc_lb = fy_reader_peek_at(fyr, 1);

				fy_reader_advance_by(fyr, 2);

				c = fy_reader_peek(fyr);
				break_run = 0;
				lastc = c;

				has_esc = true;

				break;
			}

			/* escaped sequence? */
			if (!is_single && c == '\\') {

				/* note we don't generate formatted output */
				/* we are merely checking for validity */
				c = fy_reader_peek_at(fyr, 1);

				/* hex, unicode marks - json only supports u */
				unicode_esc = !fy_reader_json_mode(fyr) ?
						(c == 'x' || c == 'u' || c == 'U') :
						c == 'u';
				if (unicode_esc) {

					total_code_length = 0;
					j = 0;
					hi_surrogate = lo_surrogate = 0;
					for (;;) {
						total_code_length += 2;

						code_length = c == 'x' ? 2 :
							c == 'u' ? 4 : 8;
						value = 0;
						for (i = 0; i < code_length; i++) {
							c = fy_reader_peek_at(fyr, total_code_length + i);

							FYR_PARSE_ERROR_CHECK(fyr, 0, total_code_length + i + 1, FYEM_SCAN,
								fy_is_hex(c), err_out,
								"double-quoted scalar has invalid hex escape");

							value <<= 4;
							if (c >= '0' && c <= '9')
								value |= c - '0';
							else if (c >= 'a' && c <= 'f')
								value |= 10 + c - 'a';
							else
								value |= 10 + c - 'A';
						}

						total_code_length += code_length;
						j++;

						/* 0x10000 + (HI - 0xd800) * 0x400 + (LO - 0xdc00) */

						/* high surrogate */
						if (j == 1 && code_length == 4 && value >= 0xd800 && value <= 0xdbff &&
						    fy_reader_peek_at(fyr, total_code_length) == '\\' &&
						    fy_reader_peek_at(fyr, total_code_length + 1) == 'u') {
							hi_surrogate = value;
							c = 'u';
							continue;
						}

						if (j == 2 && code_length == 4 && hi_surrogate) {

							FYR_PARSE_ERROR_CHECK(fyr, total_code_length - 6, 6, FYEM_SCAN,
								value >= 0xdc00 && value <= 0xdfff, err_out,
								"Invalid low surrogate value");

							lo_surrogate = value;
							value = 0x10000 + (hi_surrogate - 0xd800) * 0x400 + (lo_surrogate - 0xdc00);
						}

						break;
					}

					/* check for validity */
					FYR_PARSE_ERROR_CHECK(fyr, 0, total_code_length, FYEM_SCAN,
						!(value < 0 || (value >= 0xd800 && value <= 0xdfff) ||
							value > 0x10ffff), err_out,
						"double-quoted scalar has invalid UTF8 escape");

					fy_reader_advance_by(fyr, total_code_length);

				} else {
					escbuf[0] = '\\';
					fy_utf8_put_unchecked(escbuf + 1, c);
					escbuf_len = 1 + fy_utf8_width(c);

					ep = escbuf;

					fy_utf8_format_a(c, fyue_singlequote, &str);
					value = fy_utf8_parse_escape(&ep, escbuf_len, esc_mode);
					FYR_PARSE_ERROR_CHECK(fyr, 0, 2, FYEM_SCAN,
						value >= 0, err_out,
						"invalid escape '%s' in %s string",
						   str,
							is_single ? "single-quoted" : "double-quoted");

					fy_reader_advance_by(fyr, 2);
				}

				length += fy_utf8_width(value);

				lastc = value;

				if (lastc == '\n')
					break_run++;

				has_esc = true;

				continue;
			}

			/* check whether we have a JSON unescaped character */
			is_json_unesc = fy_is_json_unescaped_range_only(c);
			if (!is_json_unesc)
				has_json_esc = true;

			if (!is_single && fy_reader_json_mode(fyr) && has_json_esc) {
				FYR_PARSE_ERROR(fyr, 0, 2, FYEM_SCAN,
					"Invalid JSON unescaped character");
				goto err_out;
			}

			lastc = c;

			/* regular character */
			fy_reader_advance(fyr, c);

			length += fy_utf8_width(c);
			break_run = 0;
		}

		/* end of scalar */
		if (c == end_c)
			break;

		/* consume blanks */
		breaks_found = 0;
		breaks_found_length = 0;
		blanks_found = 0;
		while (fy_reader_is_flow_blank(fyr, c = fy_reader_peek(fyr)) || fy_reader_is_lb(fyr, c)) {

			if (!has_json_esc && !fy_is_json_unescaped(c))
				has_json_esc = true;

			break_run = 0;

			/* check for tab used as indentation */
			if (!fy_reader_tabsize(fyr) && fy_is_tab(c)) {
				FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_SCAN,
						fy_reader_column(fyr) > indent, err_out,
						"invalid tab used as indentation");
			}

			fy_reader_advance(fyr, c);

			if (fy_reader_is_lb(fyr, c)) {

				if (!fy_is_lb_LS_PS(c)) {
					break_length = 1;
				} else {
					break_length = fy_utf8_width(c);
					presentation_breaks_length += break_length;
				}

				has_lb = true;
				if (!breaks_found)
					first_break_length = break_length;
				breaks_found++;
				breaks_found_length += break_length;
				blanks_found = 0;
				esc_lb = false;
			} else {
				has_ws = true;
				if (!esc_lb)
					blanks_found++;
			}
		}
		first = false;
	}

	if (break_run > 0)
		ends_with_lb = true;
	else if (fy_reader_is_flow_ws(fyr, lastc))
		ends_with_ws = true;
	trailing_lb = break_run > 1;

	/* end... */
	fy_reader_fill_atom_end(fyr, handle);

	is_multiline = handle->end_mark.line > handle->start_mark.line;

	/* need to process to present */
	handle->style = is_single ? FYAS_SINGLE_QUOTED : FYAS_DOUBLE_QUOTED;
	handle->direct_output = !is_multiline && !has_esc && !has_json_esc &&
				fy_atom_size(handle) == length;
	handle->empty = ws_lb_only;
	handle->has_lb = has_lb;
	handle->has_ws = has_ws;
	handle->starts_with_ws = starts_with_ws;
	handle->starts_with_lb = starts_with_lb;
	handle->ends_with_ws = ends_with_ws;
	handle->ends_with_lb = ends_with_lb;
	handle->trailing_lb = trailing_lb;
	handle->size0 = length == 0;
	handle->valid_anchor = false;
	handle->json_mode = fy_reader_json_mode(fyr);
	handle->lb_mode = fy_reader_lb_mode(fyr);
	handle->fws_mode = fy_reader_flow_ws_mode(fyr);
	handle->tabsize = fy_reader_tabsize(fyr);
	handle->ends_with_eof = false;	/* flow scalars never end with EOF and be valid */

	/* skip over block scalar end */
	fy_reader_advance_by(fyr, 1);

#ifdef ATOM_SIZE_CHECK
	tlength = fy_atom_format_text_length(handle);
	fy_utf8_format_text_a(fy_atom_data(handle), fy_atom_size(handle), fyue_doublequote, &str);
	if (tlength != length) {
		fyr_warning(fyr, "%s: storage hint calculation failed real %zu != hint %zu - \"%s\"", __func__,
			tlength, length,
			str);
		length = tlength;
	}
#endif

	handle->storage_hint = length;
	handle->storage_hint_valid = true;

	FYR_MARK_ERROR_CHECK(fyr, &handle->start_mark, &handle->end_mark, FYEM_SCAN,
			!fy_reader_json_mode(fyr) || !is_multiline, err_out,
			"Multi line double quoted scalars not supported in JSON mode");

	return 0;

err_out:
	return -1;
}

int fy_reader_fetch_plain_scalar_handle(struct fy_reader *fyr, int c, int indent, int flow_level, struct fy_atom *handle, bool directive0)
{
	size_t length;
	int rc = -1, run, nextc, lastc, breaks_found, blanks_found;
	int breaks_found_length, first_break_length, break_length, presentation_breaks_length;
	bool has_leading_blanks;
	bool last_ptr;
	struct fy_mark mark, last_mark;
	bool is_multiline, has_lb, has_ws, ends_with_eof;
	bool has_json_esc;
	char *str;
#ifdef ATOM_SIZE_CHECK
	size_t tlength;
#endif

	FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_SCAN,
			!fy_reader_is_blankz(fyr, c), err_out,
			"plain scalar cannot start with blank or zero");

	/* may not start with any of ,[]{}#&*!|>'\"%@` */
	FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_SCAN,
			!fy_utf8_strchr(",[]{}#&*!|>'\"%@`", c), err_out,
			"plain scalar cannot start with '%c'", c);

	/* may not start with - not followed by blankz */
	FYR_PARSE_ERROR_CHECK(fyr, 0, 2, FYEM_SCAN,
			c != '-' || !fy_reader_is_blank_at_offset(fyr, 1), err_out,
			"plain scalar cannot start with '%c' followed by blank", c);

	/* may not start with -?: not followed by blankz (in block context) */
	FYR_PARSE_ERROR_CHECK(fyr, 0, 2, FYEM_SCAN,
			flow_level > 0 || !((c == '?' || c == ':') && fy_reader_is_blank_at_offset(fyr, 1)), err_out,
			"plain scalar cannot start with '%c' followed by blank (in block context)", c);

	/* may not start with - followed by ",[]{}" in flow context */
	FYR_PARSE_ERROR_CHECK(fyr, 0, 2, FYEM_SCAN,
			flow_level == 0 || !(c == '-' && fy_utf8_strchr(",[]{}", fy_reader_peek_at(fyr, 1))), err_out,
			"plain scalar cannot start with '%c' followed by ,[]{} (in flow context)", c);

	fy_reader_get_mark(fyr, &mark);

	fy_reader_fill_atom_start(fyr, handle);

	has_leading_blanks = false;
	has_lb = false;
	has_ws = false;
	has_json_esc = false;

	length = 0;
	breaks_found = 0;
	breaks_found_length = 0;
	first_break_length = 0;
	presentation_breaks_length = 0;
	blanks_found = 0;
	last_ptr = false;
	memset(&last_mark, 0, sizeof(last_mark));
	c = FYUG_EOF;
	lastc = FYUG_EOF;

	for (;;) {
		/* break for document indicators */
		if (fy_reader_column(fyr) == 0 &&
		    ((!fy_reader_strncmp(fyr, "---", 3) || !fy_reader_strncmp(fyr, "...", 3)) &&
			fy_reader_is_blankz_at_offset(fyr, 3)))
			break;

		c = fy_reader_peek(fyr);
		if (c == '#')
			break;

		/* for YAML 1.1 check % directive break */
		if (directive0 && fy_reader_column(fyr) == 0 && c == '%')
			break;

		/* quickly deal with runs */
		run = 0;
		if (c >= 0 && c <= 0x7f && (fy_utf8_low_ascii_flags[c] & F_SIMPLE_SCALAR)) {
			size_t len, consumed;
			const char *p, *s, *e;
			int8_t cc;

			while ((p = fy_reader_ensure_lookahead(fyr, 1, &len)) != NULL) {

				s = p;
				e = s + len;

				while (s < e && (cc = (int8_t)*s) >= 0 && (fy_utf8_low_ascii_flags[cc] & F_SIMPLE_SCALAR))
					s++;

				consumed = s - p;
				if (consumed) {
					fy_reader_advance_octets(fyr, consumed);
					fyr->column += consumed;
				}
				run += consumed;

				/* we're done if stopped earlier */
				if (s < e)
					break;
			}

		}
		if (run > 0) {
			length += run;
			if (breaks_found) {
				/* minimum 1 sep, or more for consecutive */
				length += breaks_found > 1 ? (breaks_found_length - first_break_length) : 1;
				length += presentation_breaks_length;
				breaks_found = 0;
				blanks_found = 0;
				presentation_breaks_length = 0;
			} else if (blanks_found) {
				/* just the blanks mam' */
				length += blanks_found;
				blanks_found = 0;
			}
		}

		while (!fy_reader_is_blankz(fyr, c = fy_reader_peek(fyr))) {


			if (c == ':') {

				nextc = fy_reader_peek_at(fyr, 1);

				/* ':' followed by space terminates */
				if (fy_reader_is_blankz(fyr, nextc)) {
					/* super rare case :: not followed by space  */
					/* :: not followed by space */
					if (lastc != ':' || fy_reader_is_blankz(fyr, nextc))
						break;
				}

				/* in flow context ':' followed by flow markers */
				if (flow_level > 0 && fy_utf8_strchr(",[]{}", nextc))
					break;
			}

			/* in flow context any or , [ ] { } */
			if (flow_level > 0 && (c == ',' || c == '[' || c == ']' || c == '{' || c == '}'))
				break;

			if (breaks_found) {
				/* minimum 1 sep, or more for consecutive */
				length += breaks_found > 1 ? (breaks_found_length - first_break_length) : 1;
				length += presentation_breaks_length;
				breaks_found = 0;
				blanks_found = 0;
				presentation_breaks_length = 0;
			} else if (blanks_found) {
				/* just the blanks mam' */
				length += blanks_found;
				blanks_found = 0;
			}

			/* check whether we have a JSON unescaped character */
			if (!has_json_esc && !fy_is_json_unescaped(c))
				has_json_esc = true;

			fy_reader_advance(fyr, c);
			run++;

			length += fy_utf8_width(c);

			lastc = c;
		}

		/* save end mark if we processed more than one non-blank */
		if (run > 0) {
			/* fyp_scan_debug(fyp, "saving mark"); */
			last_ptr = true;
			fy_reader_get_mark(fyr, &last_mark);
		}

		/* end? */
		if (!(fy_is_blank(c) || fy_reader_is_lb(fyr, c)))
			break;

		has_json_esc = true;

		/* consume blanks */
		breaks_found = 0;
		breaks_found_length = 0;
		first_break_length = 0;
		blanks_found = 0;
		do {
			fy_reader_advance(fyr, c);

			if (!fy_reader_tabsize(fyr)) {
				/* check for tab */
				FYR_PARSE_ERROR_CHECK(fyr, 0, 1, FYEM_SCAN,
						c != '\t' || !has_leading_blanks || indent < 0 || fy_reader_column(fyr) >= (indent + 1), err_out,
						"invalid tab used as indentation");
			}

			nextc = fy_reader_peek(fyr);

			/* if it's a break */
			if (fy_reader_is_lb(fyr, c)) {

				if (!fy_is_lb_LS_PS(c)) {
					break_length = 1;
				} else {
					break_length = fy_utf8_width(c);
					presentation_breaks_length += break_length;
				}

				/* first break, turn on leading blanks */
				if (!has_leading_blanks)
					has_leading_blanks = true;
				if (!breaks_found)
					first_break_length = break_length;
				breaks_found++;
				breaks_found_length += break_length;
				blanks_found = 0;
				has_lb = true;
			} else {
				blanks_found++;
				has_ws = true;
			}

			c = nextc;

		} while (fy_is_blank(c) || fy_reader_is_lb(fyr, c));

		/* break out if indentation is less */
		if (flow_level <= 0 && indent >= 0 && fy_reader_column(fyr) < (indent + 1))
			break;
	}

	/* end... */
	if (!last_ptr)
		fy_reader_fill_atom_end(fyr, handle);
	else
		fy_reader_fill_atom_end_at(fyr, handle, &last_mark);

	if (c == FYUG_INV || c == FYUG_PARTIAL) {
		FYR_MARK_ERROR(fyr, &handle->start_mark, &handle->end_mark, FYEM_SCAN,
			"plain scalar is malformed UTF8");
		goto err_out;
	}
	ends_with_eof = c == FYUG_EOF && !fy_reader_is_lb(fyr, lastc);

	is_multiline = handle->end_mark.line > handle->start_mark.line;

	handle->style = FYAS_PLAIN;
	handle->chomp = FYAC_STRIP;
	handle->direct_output = !is_multiline && !has_json_esc && fy_atom_size(handle) == length;
	handle->empty = false;
	handle->has_lb = has_lb;
	handle->has_ws = has_ws;
	handle->starts_with_ws = false;
	handle->starts_with_lb = false;
	handle->ends_with_ws = false;
	handle->ends_with_lb = false;
	handle->trailing_lb = false;
	handle->size0 = length == 0;
	handle->valid_anchor = false;
	handle->json_mode = fy_reader_json_mode(fyr);
	handle->lb_mode = fy_reader_lb_mode(fyr);
	handle->fws_mode = fy_reader_flow_ws_mode(fyr);
	handle->tabsize = fy_reader_tabsize(fyr);
	handle->ends_with_eof = ends_with_eof;

#ifdef ATOM_SIZE_CHECK
	tlength = fy_atom_format_text_length(handle);
	if (tlength != length) {
		fy_utf8_format_text_a(fy_atom_data(handle), fy_atom_size(handle), fyue_doublequote, &str);
		fyr_warning(fyr, "%s: storage hint calculation failed real %zu != hint %zu - \"%s\"", __func__,
			tlength, length,
			str);
		length = tlength;
	}
#endif

	handle->storage_hint = length;
	handle->storage_hint_valid = true;

	/* extra check in json mode */
	if (fy_reader_json_mode(fyr)) {
		FYR_MARK_ERROR_CHECK(fyr, &handle->start_mark, &handle->end_mark, FYEM_SCAN,
				!is_multiline, err_out,
				"Multi line plain scalars not supported in JSON mode");

		FYR_MARK_ERROR_CHECK(fyr, &handle->start_mark, &handle->end_mark, FYEM_SCAN,
				!fy_atom_strcmp(handle, "false") ||
				!fy_atom_strcmp(handle, "true") ||
				!fy_atom_strcmp(handle, "null") ||
				fy_atom_is_number(handle), err_out,
				"Invalid JSON plain scalar");
	}

	return 0;

err_out:
	rc = -1;
	return rc;
}


int fy_fetch_flow_scalar(struct fy_parser *fyp, int c)
{
	struct fy_atom handle;
	bool is_single, is_complex, is_multiline;
	struct fy_mark mark;
	struct fy_simple_key_mark skm;
	struct fy_token *fyt;
	int i = 0, rc = -1;
	char *str;

	is_single = c == '\'';

	fy_utf8_format_a(c, fyue_singlequote, &str);
	fyp_error_check(fyp, c == '\'' || c == '"', err_out,
			"bad start of flow scalar ('%s')",
			str);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented %s scalar in flow mode",
				is_single ? "single-quoted" : "double-quoted");

	fy_get_mark(fyp, &mark);
	fy_get_simple_key_mark(fyp, &skm);

	/* errors are generated by reader */
	rc = fy_reader_fetch_flow_scalar_handle(fyp->reader, c, fyp->indent, &handle, !!(fyp->cfg.flags & FYPCF_SLOPPY_FLOW_INDENTATION));
	if (rc) {
		fyp->stream_error = true;
		goto err_out_rc;
	}

	/* and we're done */
	fyt = fy_token_queue(fyp, FYTT_SCALAR, &handle, is_single ? FYSS_SINGLE_QUOTED : FYSS_DOUBLE_QUOTED);
	fyp_error_check(fyp, fyt, err_out_rc,
			"fy_token_queue() failed");

	if (fyp->parse_flow_only && fyp->flow_level == 0) {
		rc = fy_fetch_stream_end(fyp);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_stream_end() failed");
		return 0;
	}

	is_complex = fyp->pending_complex_key_column >= 0;
	is_multiline = handle.end_mark.line > handle.start_mark.line;

	if (!fyp->flow_level) {
		/* due to the weirdness with simple keys scan forward
		* until a linebreak, ';', or anything else */
		for (i = 0; ; i++) {
			c = fy_parse_peek_at(fyp, i);
			if (c < 0 || c == ':' || fyp_is_lb(fyp, c) || !fyp_is_flow_ws(fyp, c))
				break;
		}

		/* if we're a multiline key that's bad */
		FYP_MARK_ERROR_CHECK(fyp, &mark, &mark, FYEM_SCAN,
			!(is_multiline && !is_complex && c == ':'), err_out,
				"invalid multiline %s scalar used as key",
					is_single ? "single-quoted" : "double-quoted");

		FYP_PARSE_ERROR_CHECK(fyp, i, 1, FYEM_SCAN,
				c < 0 || c == ':' || c == '#' || fyp_is_lb(fyp, c), err_out,
				"invalid trailing content after %s scalar",
					is_single ? "single-quoted" : "double-quoted");
	}

	/* a plain scalar could be simple key */
	rc = fy_save_simple_key_mark(fyp, &skm, FYTT_SCALAR, &handle.end_mark);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_save_simple_key_mark() failed");

	/* cannot follow a flow scalar */
	fyp->simple_key_allowed = false;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	/* make sure that no comment follows directly afterwards */
	c = fy_parse_peek(fyp);

	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			c != '#', err_out,
			"invalid comment without whitespace after %s scalar",
				is_single ? "single-quoted" : "double-quoted");

	if (fyp->cfg.flags & FYPCF_PARSE_COMMENTS) {
		rc = fy_attach_comments_if_any(fyp, fyt);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_attach_right_hand_comment() failed");
	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

void fy_reader_skip_ws_cr_nl(struct fy_reader *fyr)
{
	const char *p, *s, *e;
	char cc;
	size_t len;
	int line, column;

	assert(fyr);

	column = fyr->column;
	line = fyr->line;
	while ((p = fy_reader_ensure_lookahead(fyr, 1, &len)) != NULL) {

		s = p;
		e = s + len;

		while (s < e) {
			cc = *s;
			if (cc == ' ') {
				column++;
			} else if (cc == '\n') {
				column = 0;
				line++;
			} else if (cc == '\t') {
				if (fyr->tabsize)
					column += (fyr->tabsize - (column % fyr->tabsize));
				else
					column++;
			} else if (cc == '\r') {
				column = 0;
				line++;

				if (s + 1 > e) {
					/* we have a dangling cr at the end of a block */

					/* advance up to the point here */
					fy_reader_advance_octets(fyr, s - p);

					/* try again (should return enough or NULL) */
					p = fy_reader_ensure_lookahead(fyr, 1, &len);

					/* if we couldn't pull enough we're done */
					if (!p || len < 1)
						goto done;

					s = p;
					e = s + len;

					if (*s == '\n')
						s++;
				}
				/* \n followed, gulp it down */
				if (*s == '\n')
					s++;
			} else {
				if (s > p)
					fy_reader_advance_octets(fyr, s - p);
				goto done;
			}

			s++;
		}

		fy_reader_advance_octets(fyr, s - p);
	}

done:
	fyr->line = line;
	fyr->column = column;
}

void fy_reader_skip_ws(struct fy_reader *fyr)
{
	const char *p, *s, *e;
	size_t len, consumed;
	int column;

	assert(fyr);

	while ((p = fy_reader_ensure_lookahead(fyr, 1, &len)) != NULL) {

		s = p;
		e = s + len;

		column = fyr->column;
		if (!fyr->tabsize) {
			while (s < e && fy_is_ws(*s)) {
				column++;
				s++;
			}
		} else {
			while (s < e && fy_is_ws(*s)) {
				if (fy_is_tab(*s))
					column += fyr->tabsize - (column % fyr->tabsize);
				else
					column++;
				s++;
			}
		}

		consumed = s - p;
		if (consumed) {
			fy_reader_advance_octets(fyr, consumed);
			fyr->column = column;
		}

		/* we're done if stopped earlier */
		if (s < e)
			break;
	}
}

void fy_reader_skip_space(struct fy_reader *fyr)
{
	const char *p, *s, *e;
	size_t len, consumed;

	assert(fyr);

	while ((p = fy_reader_ensure_lookahead(fyr, 1, &len)) != NULL) {

		s = p;
		e = s + len;

		while (s < e && fy_is_space(*s))
			s++;

		consumed = s - p;
		if (consumed) {
			fy_reader_advance_octets(fyr, consumed);
			fyr->column += consumed;
		}

		if (s < e)
			break;
	}
}

void fy_reader_skip_ws_lb(struct fy_reader *fyr)
{
	const char *p, *s, *e;
	size_t len, consumed;
	int line, column, c, w;
	bool dangling_cr;
	enum fy_lb_mode lb_mode;

	assert(fyr);

	/* punt to json mode */
	lb_mode = fy_reader_lb_mode(fyr);

	if (fy_reader_json_mode(fyr) || lb_mode == fylb_cr_nl) {
		fy_reader_skip_ws_cr_nl(fyr);
		return;
	}

	column = fyr->column;
	line = fyr->line;
	dangling_cr = false;
	while ((p = fy_reader_ensure_lookahead(fyr, 1, &len)) != NULL) {

		s = p;
		e = s + len;

		if (dangling_cr) {
			if (*s == '\n')
				s++;
			dangling_cr = false;
		}

		while (s < e) {
			c = (int)*s;

			/* single byte utf8? */
			if (c < 0x80) {
				if (c == ' ') {
					column++;
				} else if (c == '\n') {
					column = 0;
					line++;
				} else if (c == '\t') {
					if (fyr->tabsize)
						column += (fyr->tabsize - (column % fyr->tabsize));
					else
						column++;
				} else if (c == '\r') {
					column = 0;
					line++;
					/* check for '\n' following */
					if (s < e) {
						if (*s == '\n')
							s++;
					} else {
						/* we have a dangling cr at the end of a block */
						dangling_cr = true;
					}
				} else {
					consumed = s - p;
					if (consumed)
						fy_reader_advance_octets(fyr, consumed);
					goto done;
				}
				s++;
			} else {
				c = fy_utf8_get(s, (int)(e - s), &w);

				if (c == FYUG_PARTIAL) {
					/* get the width (from the first octet */
					w = fy_utf8_width_by_first_octet((uint8_t)*s);
					/* copy the partial utf8 in the buffer */

					/* advance up to the point here */
					consumed = s - p;
					if (consumed)
						fy_reader_advance_octets(fyr, consumed);

					/* try again (should return enough or NULL) */
					p = fy_reader_ensure_lookahead(fyr, w, &len);
					if (!p)
						break;

					/* if we couldn't pull enough we're done */
					if (len < (size_t)w)
						goto done;

					continue;
				}

				if (lb_mode == fylb_cr_nl_N_L_P && fy_is_unicode_lb(c)) {
					column = 0;
					line++;
				} else {
					consumed = s - p;
					if (consumed)
						fy_reader_advance_octets(fyr, consumed);
					goto done;
				}

				s += w;
			}
		}

		consumed = s - p;
		if (consumed)
			fy_reader_advance_octets(fyr, consumed);
	}

done:
	fyr->line = line;
	fyr->column = column;
}

int fy_fetch_plain_scalar(struct fy_parser *fyp, int c)
{
	struct fy_atom handle;
	struct fy_simple_key_mark skm;
	struct fy_token *fyt;
	bool is_multiline, is_complex, is_tab_start = false;
	struct fy_mark tab_mark;
	int rc = -1, i;

	/* Extremely bad case, a tab... so, either an indentation or separation space in block mode */
	if (!fyp->flow && fy_is_tab(c)) {

		fy_get_mark(fyp, &tab_mark);
		is_tab_start = true;

		/* skip all whitespace now */
		fy_reader_skip_ws(fyp->reader);
		c = fy_parse_peek(fyp);

		/* if it's a linebreak or a comment start, just try again */
		if (fyp_is_lb(fyp, c) || c == '#') {
			/* will need to scan more */
			fyp->token_activity_counter++;
			return 0;
		}
	}

	/* check indentation */
	FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
			fy_flow_indent_check(fyp), err_out,
			"wrongly indented flow %s",
				fyp->flow == FYFT_SEQUENCE ? "sequence" : "mapping");

	fy_get_simple_key_mark(fyp, &skm);

	rc = fy_reader_fetch_plain_scalar_handle(fyp->reader, c, fyp->indent, fyp->flow_level, &handle,
						fy_document_state_version_compare(fyp->current_document_state, fy_version_make(1, 1)) <= 0);
	if (rc) {
		fyp->stream_error = true;
		goto err_out_rc;
	}

	is_multiline = handle.end_mark.line > handle.start_mark.line;
	is_complex = fyp->pending_complex_key_column >= 0;

	/* and we're done */
	fyt = fy_token_queue(fyp, FYTT_SCALAR, &handle, FYSS_PLAIN);
	fyp_error_check(fyp, fyt, err_out_rc,
			"fy_token_queue() failed");

	if (fyp->parse_flow_only && fyp->flow_level == 0) {
		rc = fy_fetch_stream_end(fyp);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_stream_end() failed");
		return 0;
	}

	if (!fyp->flow_level && !is_complex && (is_multiline || is_tab_start)) {
		/* due to the weirdness with simple keys scan forward
		* until a linebreak, ':', or anything else */
		for (i = 0; ; i++) {
			c = fy_parse_peek_at(fyp, i);
			if (c < 0 || (c == ':' && fy_is_blankz_at_offset(fyp, i + 1)) ||
					fyp_is_lb(fyp, c) || !fy_is_ws(c))
				break;
		}

		/* if we're a key, that's invalid */
		if (c == ':') {

			if (is_multiline)
				FYP_MARK_ERROR(fyp, &handle.start_mark, &handle.end_mark, FYEM_SCAN,
						"invalid multiline plain key");
			else
				FYP_MARK_ERROR(fyp, &tab_mark, &tab_mark, FYEM_SCAN,
						"invalid tab as indendation in a mapping");

			goto err_out;
		}
	}

	rc = fy_save_simple_key_mark(fyp, &skm, FYTT_SCALAR, &handle.end_mark);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_save_simple_key_mark() failed");

	fyp->simple_key_allowed = handle.has_lb;
	fyp_scan_debug(fyp, "simple_key_allowed -> %s\n", fyp->simple_key_allowed ? "true" : "false");

	if (fyp->cfg.flags & FYPCF_PARSE_COMMENTS) {
		rc = fy_attach_comments_if_any(fyp, fyt);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_attach_right_hand_comment() failed");
	}

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_fetch_tokens(struct fy_parser *fyp)
{
	struct fy_mark m;
	bool was_double_colon;
	int c, rc;

	/* do not fetch any more when stream end is reached */
	if (fyp->stream_end_reached)
		return 0;

	if (!fyp->stream_start_produced) {
		rc = fy_parse_get_next_input(fyp);
		fyp_error_check(fyp, rc >= 0, err_out_rc,
			"fy_parse_get_next_input() failed");

		if (rc > 0) {
			rc = fy_fetch_stream_start(fyp);
			fyp_error_check(fyp, !rc, err_out_rc,
					"fy_fetch_stream_start() failed");
		}
		return 0;
	}

	fyp_scan_debug(fyp, "-------------------------------------------------");
	rc = fy_scan_to_next_token(fyp);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_scan_to_next_token() failed");

	if (fyp_block_mode(fyp)) {
		rc = fy_parse_unroll_indent(fyp, fyp_column(fyp));
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_parse_unroll_indent() failed");
	}

	c = fy_parse_peek(fyp);
	if (c < 0 || c == '\0') {

		fyp->stream_end_reached = true;

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				!fyp_json_mode(fyp) || c != '\0', err_out,
				"JSON disallows '\\0' in the input stream");

		if (c >= 0)
			fy_advance(fyp, c);
		rc = fy_fetch_stream_end(fyp);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_stream_end() failed");
		return 0;
	}

	if (fyp_column(fyp) == 0 && c == '%') {

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				!fyp_json_mode(fyp), err_out,
				"directives not supported in JSON mode");

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				!fyp->bare_document_only, err_out,
				"invalid directive in bare document mode");

		fy_advance(fyp, c);
		rc = fy_fetch_directive(fyp);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_directive() failed");
		goto out;
	}

	/* probable document start/end indicator */
	if (fyp_column(fyp) == 0 &&
	    (!fy_parse_strncmp(fyp, "---", 3) ||
	     !fy_parse_strncmp(fyp, "...", 3)) &&
	    fy_is_blankz_at_offset(fyp, 3)) {

		FYP_PARSE_ERROR_CHECK(fyp, 0, 3, FYEM_SCAN,
				!fyp_json_mode(fyp), err_out,
				"document %s indicator not supported in JSON mode",
					c == '-' ? "start" : "end");

		FYP_PARSE_ERROR_CHECK(fyp, 0, 3, FYEM_SCAN,
				!fyp->bare_document_only, err_out,
				"invalid document %s indicator in bare document mode",
					c == '-' ? "start" : "end");

		rc = fy_fetch_document_indicator(fyp,
				c == '-' ? FYTT_DOCUMENT_START :
					FYTT_DOCUMENT_END);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_document_indicator() failed");

		fyp->indent_line = fyp_line(fyp);

		/* for document end, nothing must follow except whitespace and comment */
		if (c == '.') {
			c = fy_parse_peek(fyp);

			FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
					c == -1 || c == '#' || fyp_is_lb(fyp, c), err_out,
					"invalid content after document end marker");
		}

		goto out;
	}

	fyp_scan_debug(fyp, "indent=%d, parent indent=%d\n",
			fyp->indent, fyp->parent_indent);

	if (c == '[' || c == '{') {

		fyp->indent_line = fyp_line(fyp);

		fyp_scan_debug(fyp, "calling fy_fetch_flow_collection_mark_start(%c)", c);
		rc = fy_fetch_flow_collection_mark_start(fyp, c);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_flow_collection_mark_start() failed");
		goto out;
	}

	if (c == ']' || c == '}') {

		fyp->indent_line = fyp_line(fyp);

		fyp_scan_debug(fyp, "fy_fetch_flow_collection_mark_end(%c)", c);
		rc = fy_fetch_flow_collection_mark_end(fyp, c);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_flow_collection_mark_end() failed");
		goto out;
	}


	if (c == ',') {

		fyp->indent_line = fyp_line(fyp);

		fy_get_mark(fyp, &m);

		fyp_scan_debug(fyp, "fy_fetch_flow_collection_entry(%c)", c);
		rc = fy_fetch_flow_collection_entry(fyp, c);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_flow_collection_entry() failed");

		fyp->last_was_comma = true;
		fyp->last_comma_mark = m;

		goto out;
	}

	if (c == '-' && fy_is_blankz_at_offset(fyp, 1)) {

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				!fyp_json_mode(fyp), err_out,
				"block entries not supported in JSON mode");

		fyp->indent_line = fyp_line(fyp);

		fyp_scan_debug(fyp, "fy_fetch_block_entry(%c)", c);
		rc = fy_fetch_block_entry(fyp, c);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_block_entry() failed");
		goto out;
	}

	if (c == '?' && fy_is_blankz_at_offset(fyp, 1)) {

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				!fyp_json_mode(fyp), err_out,
				"complex keys not supported in JSON mode");

		fyp->indent_line = fyp_line(fyp);

		fyp_scan_debug(fyp, "fy_fetch_key(%c)", c);
		rc = fy_fetch_key(fyp, c);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_key() failed");
		goto out;
	}

	if (c == ':') {
		was_double_colon = c == ':' && fyp->colon_follows_colon && fyp->flow_level > 0;
		fyp->colon_follows_colon = false;

		if (((fyp->flow_level && !fyp->simple_key_allowed) || fy_is_blankz_at_offset(fyp, 1)) &&
				!was_double_colon) {

			fyp->indent_line = fyp_line(fyp);

			fyp_scan_debug(fyp, "fy_fetch_value(%c)", c);
			rc = fy_fetch_value(fyp, c);
			fyp_error_check(fyp, !rc, err_out_rc,
					"fy_fetch_value() failed");
			goto out;
		}
	}

	if (c == '*' || c == '&') {

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				!fyp_json_mode(fyp), err_out,
				"%s not supported in JSON mode",
					c == '&' ? "anchor" : "alias");

		fyp_scan_debug(fyp, "fy_fetch_anchor_or_alias(%c)", c);
		rc = fy_fetch_anchor_or_alias(fyp, c);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_anchor_or_alias() failed");
		goto out;
	}

	if (c == '!') {

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				!fyp_json_mode(fyp), err_out,
				"tag not supported in JSON mode");

		fyp_scan_debug(fyp, "fy_fetch_tag(%c)", c);
		rc = fy_fetch_tag(fyp, c);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_tag() failed");
		goto out;
	}

	if (!fyp->flow_level && (c == '|' || c == '>')) {

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				!fyp_json_mode(fyp), err_out,
				"block scalars not supported in JSON mode");

		fyp_scan_debug(fyp, "fy_fetch_block_scalar(%c)", c);
		rc = fy_fetch_block_scalar(fyp, c == '|', c);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_block_scalar() failed");
		goto out;
	}

	if (c == '\'' || c == '"') {

		FYP_PARSE_ERROR_CHECK(fyp, 0, 1, FYEM_SCAN,
				c == '"' || !fyp_json_mode(fyp), err_out,
				"single quoted scalars not supported in JSON mode");

		fyp_scan_debug(fyp, "fy_fetch_flow_scalar(%c)", c);
		rc = fy_fetch_flow_scalar(fyp, c);
		fyp_error_check(fyp, !rc, err_out_rc,
				"fy_fetch_flow_scalar() failed");
		goto out;
	}

	fyp_scan_debug(fyp, "fy_fetch_plain_scalar(%c)", c);
	rc = fy_fetch_plain_scalar(fyp, c);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_fetch_plain_scalar() failed");

out:
	if (c != ',' && fyp->last_was_comma)
		fyp->last_was_comma = false;

	return 0;

err_out:
	rc = -1;
err_out_rc:
	return rc;
}

struct fy_token *fy_scan_peek(struct fy_parser *fyp)
{
	struct fy_token *fyt;
	int rc, last_token_activity_counter;
	bool have_simple_keys;

	/* nothing if stream end produced (and no stream end token in queue) */
	if (fyp->stream_end_produced) {
		fyt = fy_token_list_head(&fyp->queued_tokens);
		if (fyt && fyt->type == FYTT_STREAM_END)
			return fyt;

		/* OK, we're done, flush everything */
		fy_token_list_unref_all(&fyp->queued_tokens);

		/* try to get the next input */
		rc = fy_parse_get_next_input(fyp);
		fyp_error_check(fyp, rc >= 0, err_out,
			"fy_parse_get_next_input() failed");

		/* no more inputs */
		if (rc == 0) {
			fyp_scan_debug(fyp, "token stream ends");
			return NULL;
		}

		fyp_scan_debug(fyp, "starting new token stream");

		fyp->stream_start_produced = false;
		fyp->stream_end_produced = false;
		fyp->stream_end_reached = false;
	}

	/* we loop until we have a token and the simple key list is empty */
	for (;;) {
		fyt = fy_token_list_head(&fyp->queued_tokens);
		have_simple_keys = !fy_simple_key_list_empty(&fyp->simple_keys);

		/* we can produce a token when:
		* a) one exists
		* b) no simple keys exist at all
		*/
		if (fyt && !have_simple_keys)
			break;

		/* on stream error we're done */
		if (fyp->stream_error)
			return NULL;

		/* keep track of token activity, if it didn't change
		* after the fetch tokens call, the state machine is stuck
		*/
		last_token_activity_counter = fyp->token_activity_counter;

		/* fetch more then */
		rc = fy_fetch_tokens(fyp);
		fyp_error_check(fyp, !rc, err_out,
				"fy_fetch_tokens() failed");

		fyp_error_check(fyp, last_token_activity_counter != fyp->token_activity_counter, err_out,
				"out of tokens and failed to produce anymore");
	}

	switch (fyt->type) {
	case FYTT_STREAM_START:
		fyp_scan_debug(fyp, "setting stream_start_produced to true");
		fyp->stream_start_produced = true;
		break;
	case FYTT_STREAM_END:
		fyp_scan_debug(fyp, "setting stream_end_produced to true");
		fyp->stream_end_produced = true;

		if (!fyp->parse_flow_only) {
			rc = fy_reader_input_done(fyp->reader);
			fyp_error_check(fyp, !rc, err_out,
					"fy_parse_input_done() failed");
		}
		break;
	default:
		break;
	}

	return fyt;

err_out:
	return NULL;
}

static inline struct fy_token *
fy_scan_remove(struct fy_parser *fyp, struct fy_token *fyt)
{
	if (!fyp || !fyt)
		return NULL;

	fy_token_list_del(&fyp->queued_tokens, fyt);

	return fyt;
}

static inline struct fy_token *
fy_scan_remove_peek(struct fy_parser *fyp, struct fy_token *fyt)
{
	if (fyt != NULL) {
		(void)fy_scan_remove(fyp, fyt);
		fy_token_unref_rl(fyp->recycled_token_list, fyt);
	}

	return fy_scan_peek(fyp);
}

struct fy_token *fy_scan(struct fy_parser *fyp)
{
	struct fy_token *fyt;

	fyt = fy_scan_remove(fyp, fy_scan_peek(fyp));

	if (fyt && (fyt->type == FYTT_VERSION_DIRECTIVE || fyt->type == FYTT_TAG_DIRECTIVE)) {

		/*
		* NOTE: we need to update the document state with the contents of
		* directives, so that tags etc, work correctly.
		* This is arguably a big hack, but so is using the scanner in such
		* a low level.
		*
		* This is not very good because we don't keep track of parser state
		* so tag directives in the middle of the document are AOK.
		* But we don't really care, if you care about stream validity do
		* a proper parse.
		*/

		/* we take a reference because the parse methods take ownership */
		fy_token_ref(fyt);

		/* we ignore errors, because... they are parse errors, not scan errors */

		if (fyt->type == FYTT_VERSION_DIRECTIVE)
			(void)fy_parse_version_directive(fyp, fyt, true);
		else
			(void)fy_parse_tag_directive(fyp, fyt, true);
	}

#ifdef FY_DEVMODE
	if (fyt)
		fyp_debug_dump_token(fyp, fyt, "producing: ");
#endif
	return fyt;
}

void fy_scan_token_free(struct fy_parser *fyp, struct fy_token *fyt)
{
	fy_token_unref_rl(fyp->recycled_token_list, fyt);
}

int fy_parse_state_push(struct fy_parser *fyp, enum fy_parser_state state)
{
	struct fy_parse_state_log *fypsl;

	fypsl = fy_parse_parse_state_log_alloc(fyp);
	fyp_error_check(fyp, fypsl != NULL, err_out,
			"fy_parse_state_log_alloc() failed!");
	fypsl->state = state;
	fy_parse_state_log_list_push(&fyp->state_stack, fypsl);

	return 0;
err_out:
	return -1;
}

enum fy_parser_state fy_parse_state_pop(struct fy_parser *fyp)
{
	struct fy_parse_state_log *fypsl;
	enum fy_parser_state state;

	fypsl = fy_parse_state_log_list_pop(&fyp->state_stack);
	if (!fypsl)
		return FYPS_NONE;

	state = fypsl->state;

	fy_parse_parse_state_log_recycle(fyp, fypsl);

	return state;
}

void fy_parse_state_set(struct fy_parser *fyp, enum fy_parser_state state)
{
	fyp_parse_debug(fyp, "state %s -> %s\n", state_txt[fyp->state], state_txt[state]);
	fyp->state = state;
}

enum fy_parser_state fy_parse_state_get(struct fy_parser *fyp)
{
	return fyp->state;
}

static struct fy_eventp *
fy_parse_node(struct fy_parser *fyp, struct fy_token *fyt, bool is_block)
{
	struct fy_eventp *fyep = NULL;
	struct fy_event *fye = NULL;
	struct fy_document_state *fyds = NULL;
	struct fy_token *anchor = NULL, *tag = NULL;
	const char *handle;
	size_t handle_size;
	struct fy_token *fyt_td;
	struct fy_token *fytn;
	struct fy_atom *ev_handle = NULL;

	fyds = fyp->current_document_state;
	assert(fyds);

	fyp_parse_debug(fyp, "parse_node: is_block=%s - fyt %s",
			is_block ? "true" : "false",
			fy_token_type_txt[fyt->type]);

	if (fyt->type == FYTT_ALIAS) {
		fy_parse_state_set(fyp, fy_parse_state_pop(fyp));

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_ALIAS;
		fye->alias.anchor = fy_scan_remove(fyp, fyt);

		ev_handle = &fye->alias.anchor->handle;
		goto return_ok;
	}

	while ((!anchor && fyt->type == FYTT_ANCHOR) || (!tag && fyt->type == FYTT_TAG)) {
		if (fyt->type == FYTT_ANCHOR)
			anchor = fy_scan_remove(fyp, fyt);
		else
			tag = fy_scan_remove(fyp, fyt);

		fyt = fy_scan_peek(fyp);
		fyp_error_check(fyp, fyt, err_out,
				"failed to peek token");

		fyp_parse_debug(fyp, "parse_node: ANCHOR|TAG got -  fyt %s",
			fy_token_type_txt[fyt->type]);

		FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
				fyt->type != FYTT_ALIAS, err_out,
				"unexpected alias");
	}

	/* check tag prefix */
	if (tag && tag->tag.handle_length) {
		handle = fy_atom_data(&tag->handle) + tag->tag.skip;
		handle_size = tag->tag.handle_length;

		fyt_td = fy_document_state_lookup_tag_directive(fyds, handle, handle_size);

		FYP_TOKEN_ERROR_CHECK(fyp, tag, FYEM_PARSE,
				fyt_td, err_out,
				"undefined tag prefix '%.*s'", (int)handle_size, handle);
	}

	if ((fyp->state == FYPS_BLOCK_MAPPING_VALUE ||
	     fyp->state == FYPS_BLOCK_MAPPING_FIRST_KEY)
		&& fyt->type == FYTT_BLOCK_ENTRY) {

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_SEQUENCE_START;
		fye->sequence_start.anchor = anchor;
		fye->sequence_start.tag = tag;

		/* allocate and copy in place */
		fytn = fy_token_alloc_rl(fyp->recycled_token_list);
		fyp_error_check(fyp, fytn, err_out,
				"fy_token_alloc_rl() failed!");
		fytn->type = FYTT_BLOCK_SEQUENCE_START;
		fytn->handle = fyt->handle;
		fytn->handle.end_mark = fytn->handle.start_mark;	/* no extent */
		fy_input_ref(fytn->handle.fyi);

		fye->sequence_start.sequence_start = fytn;

		fy_parse_state_set(fyp, FYPS_INDENTLESS_SEQUENCE_ENTRY);

		ev_handle = &fye->sequence_start.sequence_start->handle;
		goto return_ok;
	}

	if (fyt->type == FYTT_SCALAR) {
		fy_parse_state_set(fyp, fy_parse_state_pop(fyp));

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_SCALAR;
		fye->scalar.anchor = anchor;
		fye->scalar.tag = tag;
		fye->scalar.value = fy_scan_remove(fyp, fyt);

		ev_handle = &fye->scalar.value->handle;
		goto return_ok;
	}

	if (fyt->type == FYTT_FLOW_SEQUENCE_START) {

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_SEQUENCE_START;
		fye->sequence_start.anchor = anchor;
		fye->sequence_start.tag = tag;
		fye->sequence_start.sequence_start = fy_scan_remove(fyp, fyt);
		fy_parse_state_set(fyp, FYPS_FLOW_SEQUENCE_FIRST_ENTRY);

		ev_handle = &fye->sequence_start.sequence_start->handle;
		goto return_ok;
	}

	if (fyt->type == FYTT_FLOW_MAPPING_START) {

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_MAPPING_START;
		fye->mapping_start.anchor = anchor;
		fye->mapping_start.tag = tag;
		fye->mapping_start.mapping_start = fy_scan_remove(fyp, fyt);
		fy_parse_state_set(fyp, FYPS_FLOW_MAPPING_FIRST_KEY);

		ev_handle = &fye->mapping_start.mapping_start->handle;
		goto return_ok;
	}

	if (is_block && fyt->type == FYTT_BLOCK_SEQUENCE_START) {

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_SEQUENCE_START;
		fye->sequence_start.anchor = anchor;
		fye->sequence_start.tag = tag;
		fye->sequence_start.sequence_start = fy_scan_remove(fyp, fyt);
		fy_parse_state_set(fyp, FYPS_BLOCK_SEQUENCE_FIRST_ENTRY);

		ev_handle = &fye->sequence_start.sequence_start->handle;
		goto return_ok;
	}

	if (is_block && fyt->type == FYTT_BLOCK_MAPPING_START) {

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_MAPPING_START;
		fye->mapping_start.anchor = anchor;
		fye->mapping_start.tag = tag;
		fye->mapping_start.mapping_start = fy_scan_remove(fyp, fyt);
		fy_parse_state_set(fyp, FYPS_BLOCK_MAPPING_FIRST_KEY);

		ev_handle = &fye->mapping_start.mapping_start->handle;
		goto return_ok;
	}

	if (!anchor && !tag) {

		if (fyt->type == FYTT_FLOW_ENTRY &&
			(fyp->state == FYPS_FLOW_SEQUENCE_FIRST_ENTRY ||
			fyp->state == FYPS_FLOW_SEQUENCE_ENTRY))

			FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
					"flow sequence with invalid %s",
						fyp->state == FYPS_FLOW_SEQUENCE_FIRST_ENTRY ?
						"comma in the beginning" : "extra comma");

		else if ((fyt->type == FYTT_DOCUMENT_START || fyt->type == FYTT_DOCUMENT_END) &&
				(fyp->state == FYPS_FLOW_SEQUENCE_FIRST_ENTRY ||
				fyp->state == FYPS_FLOW_SEQUENCE_ENTRY))

			FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
					"invalid document %s indicator in a flow sequence",
						fyt->type == FYTT_DOCUMENT_START ?
							"start" : "end");
		else
			FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
					"did not find expected node content");

		goto err_out;
	}

	fyp_parse_debug(fyp, "parse_node: empty scalar...");

	/* empty scalar */
	fy_parse_state_set(fyp, fy_parse_state_pop(fyp));

	fyep = fy_parse_eventp_alloc(fyp);
	fyp_error_check(fyp, fyep, err_out,
			"fy_eventp_alloc() failed!");
	fye = &fyep->e;

	fye->type = FYET_SCALAR;
	fye->scalar.anchor = anchor;
	fye->scalar.tag = tag;

	/* copy atom from the token and set to zero size at start */
	fye->scalar.value = fy_token_create_rl(
		fyp->recycled_token_list,  FYTT_SCALAR, &fyp->last_event_handle, FYSS_PLAIN);
	fyp_error_check(fyp, fye->scalar.value, err_out,
			"failed to allocate SCALAR token()");
	/* mark it as a special value */
	fye->scalar.value->scalar.is_null = true;

return_ok:
	if (ev_handle) {
		fy_input_unref(fyp->last_event_handle.fyi);
		fyp->last_event_handle = *ev_handle;
		fyp->last_event_handle.start_mark = fyp->last_event_handle.end_mark;
		fy_atom_reset_storage_hints(&fyp->last_event_handle);
		fy_input_ref(fyp->last_event_handle.fyi);
	}

	fyp_parse_debug(fyp, "parse_node: > %s",
			fy_event_type_txt[fye->type]);

	return fyep;

err_out:
	fy_token_unref_rl(fyp->recycled_token_list, anchor);
	fy_token_unref_rl(fyp->recycled_token_list, tag);
	fy_parse_eventp_recycle(fyp, fyep);

	return NULL;
}

static struct fy_eventp *
fy_parse_empty_scalar(struct fy_parser *fyp)
{
	struct fy_eventp *fyep;
	struct fy_event *fye;

	fyep = fy_parse_eventp_alloc(fyp);
	fyp_error_check(fyp, fyep, err_out,
			"fy_eventp_alloc() failed!");
	fye = &fyep->e;

	fye->type = FYET_SCALAR;
	fye->scalar.anchor = NULL;
	fye->scalar.tag = NULL;

	/* for empty scalar the last event handle does not change, only  */
	fye->scalar.value = fy_token_create_rl(
		fyp->recycled_token_list,  FYTT_SCALAR, &fyp->last_event_handle, FYSS_PLAIN);
	fyp_error_check(fyp, fye->scalar.value, err_out,
			"failed to allocate SCALAR token()");
	/* mark it as a special value */
	fye->scalar.value->scalar.is_null = true;
	return fyep;

err_out:
	return NULL;
}

int fy_parse_stream_start(struct fy_parser *fyp)
{
	fyp->indent = -2;
	fyp->indent_line = -1;
	fyp->generated_block_map = false;
	fyp->last_was_comma = false;
	fyp->flow = FYFT_NONE;
	fyp->pending_complex_key_column = -1;

	fy_parse_indent_list_recycle_all(fyp, &fyp->indent_stack);
	fy_parse_simple_key_list_recycle_all(fyp, &fyp->simple_keys);
	fy_parse_parse_state_log_list_recycle_all(fyp, &fyp->state_stack);
	fy_parse_flow_list_recycle_all(fyp, &fyp->flow_stack);

	fy_token_unref_rl(fyp->recycled_token_list, fyp->stream_end_token);
	fyp->stream_end_token = NULL;

	return 0;
}

int fy_parse_stream_end(struct fy_parser *fyp)
{
	fy_token_unref_rl(fyp->recycled_token_list, fyp->stream_end_token);
	fyp->stream_end_token = NULL;

	return 0;
}

static struct fy_eventp *fy_parse_internal(struct fy_parser *fyp)
{
	struct fy_eventp *fyep = NULL;
	struct fy_event *fye = NULL;
	struct fy_token *fyt = NULL;
	struct fy_document_state *fyds = NULL;
	bool is_block, is_seq, is_value, is_first, had_doc_end, had_directives;
	enum fy_parser_state orig_state;
	struct fy_token *version_directive;
	struct fy_token_list tag_directives;
	const struct fy_mark *fym;
	struct fy_atom handle, *ev_handle;
	struct fy_token *fytn;
	char tbuf[16] __FY_DEBUG_UNUSED__;
	int rc;

	version_directive = NULL;
	fy_token_list_init(&tag_directives);

	/* are we done? */
	if (fyp->stream_error || fyp->state == FYPS_END)
		return NULL;

	fyt = fy_scan_peek(fyp);

	/* special case without an error message for start */
	if (!fyt && fyp->state == FYPS_NONE)
		return NULL;

	/* keep a copy of stream end */
	if (fyt && fyt->type == FYTT_STREAM_END && !fyp->stream_end_token) {
		fyp->stream_end_token = fy_token_ref(fyt);
		fyp_parse_debug(fyp, "kept copy of STRM-");
	}

	/* keep on producing STREAM_END */
	if (!fyt && fyp->stream_end_token) {
		fyt = fyp->stream_end_token;
		fy_token_list_add_tail(&fyp->queued_tokens, fyt);

		fyp_parse_debug(fyp, "generated copy of STRM-");
	}

	fyp_error_check(fyp, fyt, err_out,
			"failed to peek token");

	assert(fyt->handle.fyi);

	fyp_parse_debug(fyp, "[%s] <- %s", state_txt[fyp->state],
			fy_token_dump_format(fyt, tbuf, sizeof(tbuf)));

	is_first = false;
	had_doc_end = false;

	fyep = NULL;
	fye = NULL;
	ev_handle = NULL;

	orig_state = fyp->state;
	switch (fyp->state) {
	case FYPS_NONE:
		fy_parse_state_set(fyp, FYPS_STREAM_START);
		/* fallthrough */

	case FYPS_STREAM_START:

		fyp_error_check(fyp, fyt->type == FYTT_STREAM_START, err_out,
				"failed to get valid stream start token");

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_STREAM_START;

		fye->stream_start.stream_start = fy_scan_remove(fyp, fyt);

		rc = fy_parse_stream_start(fyp);
		fyp_error_check(fyp, !rc, err_out,
				"stream start failed");

		fy_parse_state_set(fyp, FYPS_IMPLICIT_DOCUMENT_START);

		fyp->stream_has_content = false;

		ev_handle = &fye->stream_start.stream_start->handle;
		break;

	case FYPS_IMPLICIT_DOCUMENT_START:

		/* fallthrough */

	case FYPS_DOCUMENT_START:

		had_doc_end = false;

		if (!fyp->stream_has_content && fyt->type != FYTT_STREAM_END)
			fyp->stream_has_content = true;

		/* remove all extra document end indicators */
		while (fyt->type == FYTT_DOCUMENT_END) {

			/* reset document has content flag */
			fyp->document_has_content = false;
			fyp->document_first_content_token = true;

			/* explicit end indicator, no more directives checking */
			fyp->had_directives = false;

			fyt = fy_scan_remove_peek(fyp, fyt);
			fyp_error_check(fyp, fyt, err_out,
					"failed to peek token");

#ifdef FY_DEVMODE
			fyp_debug_dump_token(fyp, fyt, "next: ");
#endif

			had_doc_end = true;
		}

		if (!fyp->current_document_state) {

			rc = fy_reset_document_state(fyp);
			fyp_error_check(fyp, !rc, err_out,
					"fy_reset_document_state() failed");
		}

		fyds = fyp->current_document_state;
		fyp_error_check(fyp, fyds, err_out,
				"no current document state error");

		/* process directives */
		had_directives = false;
		while (fyt->type == FYTT_VERSION_DIRECTIVE ||
			fyt->type == FYTT_TAG_DIRECTIVE) {

			had_directives = true;
			fyp->had_directives = true;

			if (fyt->type == FYTT_VERSION_DIRECTIVE) {

				rc = fy_parse_version_directive(fyp, fy_scan_remove(fyp, fyt), false);
				fyt = NULL;
				fyp_error_check(fyp, !rc, err_out,
						"failed to fy_parse_version_directive()");
			} else {
				rc = fy_parse_tag_directive(fyp, fy_scan_remove(fyp, fyt), false);
				fyt = NULL;

				fyp_error_check(fyp, !rc, err_out,
						"failed to fy_parse_tag_directive()");
			}

			fyt = fy_scan_peek(fyp);
			fyp_error_check(fyp, fyt, err_out,
					"failed to peek token");
#ifdef FY_DEVMODE
			fyp_debug_dump_token(fyp, fyt, "next: ");
#endif
		}

		/* the end */
		if (fyt->type == FYTT_STREAM_END) {

			/* empty content is not allowed in JSON mode */
			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
					!fyp_json_mode(fyp) ||
						fyp->stream_has_content, err_out,
					"JSON does not allow empty root content");

			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
					!fyp->had_directives || fyp->document_has_content ||
					!fyds->start_implicit, err_out,
					"stream with directives without content");

			rc = fy_parse_stream_end(fyp);
			fyp_error_check(fyp, !rc, err_out,
					"stream end failed");

			fyep = fy_parse_eventp_alloc(fyp);
			fyp_error_check(fyp, fyep, err_out,
					"fy_eventp_alloc() failed!");
			fye = &fyep->e;

			fye->type = FYET_STREAM_END;
			fye->stream_end.stream_end = fy_scan_remove(fyp, fyt);

			fy_parse_state_set(fyp,
				fy_parse_have_more_inputs(fyp) ? FYPS_NONE : FYPS_END);

			ev_handle = &fye->stream_end.stream_end->handle;
			break;
		}

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		/* document start */
		fye->type = FYET_DOCUMENT_START;
		fye->document_start.document_start = NULL;
		fye->document_start.document_state = NULL;

		if (!(fyp->state == FYPS_IMPLICIT_DOCUMENT_START || had_doc_end || fyt->type == FYTT_DOCUMENT_START)) {
			fyds = fyp->current_document_state;

			/* not BLOCK_MAPPING_START */
			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
					fyt->type == FYTT_BLOCK_MAPPING_START, err_out,
					"missing document start");

			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
					fyds->start_implicit ||
					fyds->start_mark.line != fy_token_start_line(fyt), err_out,
					"invalid mapping starting at --- line");

			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
					false, err_out,
					"invalid mapping in plain multiline");
		}

		fym = fy_token_start_mark(fyt);
		if (fym)
			fyds->start_mark = *fym;
		else
			memset(&fyds->start_mark, 0, sizeof(fyds->start_mark));

		if (fyt->type != FYTT_DOCUMENT_START) {

			/* copy atom from the token and set to zero size at start */
			handle = fyt->handle;
			handle.end_mark = handle.start_mark;
			fy_atom_reset_storage_hints(&handle);

			fye->document_start.document_start = fy_token_create_rl(
					fyp->recycled_token_list, FYTT_DOCUMENT_START, &handle);
			fyp_error_check(fyp, fye->document_start.document_start, err_out,
					"failed to allocate DOCUMENT_START token()");

			fyds->start_implicit = true;
			fyp_parse_debug(fyp, "document_start_implicit=true");

			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
					fyt->type != FYTT_DOCUMENT_END || !had_directives, err_out,
					"directive(s) without a document");

			fy_parse_state_set(fyp, FYPS_BLOCK_NODE);
		} else {
			fye->document_start.document_start = fy_scan_remove(fyp, fyt);

			fyds->start_implicit = false;
			fyp_parse_debug(fyp, "document_start_implicit=false");

			fy_parse_state_set(fyp, FYPS_DOCUMENT_CONTENT);
		}

		rc = fy_parse_state_push(fyp, FYPS_DOCUMENT_END);
		fyp_error_check(fyp, !rc, err_out,
				"failed to fy_parse_state_push()");

		// update document state with json mode
		fyds->json_mode = fyp_json_mode(fyp);
		fye->document_start.document_state = fy_document_state_ref(fyds);
		fye->document_start.implicit = fyds->start_implicit;

		ev_handle = &fye->document_start.document_start->handle;
		break;

	case FYPS_DOCUMENT_END:

		fyds = fyp->current_document_state;
		fyp_error_check(fyp, fyds, err_out,
				"no current document state error");

		if (fyt && (fyt->type == FYTT_VERSION_DIRECTIVE ||
			    fyt->type == FYTT_TAG_DIRECTIVE)) {
			int cmpval = fy_document_state_version_compare(fyds, fy_version_make(1, 1));

			fyp_scan_debug(fyp, "version %d.%d %s %d.%d\n",
				fyds->version.major, fyds->version.minor,
				cmpval == 0 ? "=" : cmpval > 0 ? ">" : "<",
				1, 1);

			/* YAML 1.1 allows directives without document end */
			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
				cmpval <= 0, err_out,
				"missing explicit document end marker before directive(s)");

		}

		fym = fy_token_end_mark(fyt);
		if (fym)
			fyds->end_mark = *fym;
		else
			memset(&fyds->end_mark, 0, sizeof(fyds->end_mark));

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		/* document end */
		fye->type = FYET_DOCUMENT_END;
		if (fyt->type == FYTT_DOCUMENT_END) {

			handle = fyt->handle;
			fy_atom_reset_storage_hints(&handle);

			fye->document_end.document_end = fy_token_create_rl(
					fyp->recycled_token_list, FYTT_DOCUMENT_END, &handle);
			fyp_error_check(fyp, fye->document_end.document_end, err_out,
					"failed to allocate DOCUMENT_END token()");
			fyds->end_implicit = false;

			/* reset document has content flag */
			fyp->document_has_content = false;
			fyp->document_first_content_token = true;

			/* reset directives */
			fyp->had_directives = false;

		} else {

			handle = fyt->handle;
			handle.end_mark = handle.start_mark;
			fy_atom_reset_storage_hints(&handle);

			fye->document_end.document_end = fy_token_create_rl(
					fyp->recycled_token_list, FYTT_DOCUMENT_END, &handle);
			fyp_error_check(fyp, fye->document_end.document_end, err_out,
					"failed to allocate DOCUMENT_END token()");
			fyds->end_implicit = true;
		}

		fye->document_end.implicit = fyds->end_implicit;

		if (!fyp->next_single_document) {
			/* multi document mode */
			fy_parse_state_set(fyp, FYPS_DOCUMENT_START);
			fyp->had_directives = false;

			/* and reset document state */
			rc = fy_reset_document_state(fyp);
			fyp_error_check(fyp, !rc, err_out,
					"fy_reset_document_state() failed");
		} else {
			/* single document mode */
			fyp->next_single_document = false;

			fy_parse_state_set(fyp, FYPS_SINGLE_DOCUMENT_END);
		}

		ev_handle = &fye->document_end.document_end->handle;
		break;

	case FYPS_DOCUMENT_CONTENT:

		if (fyt->type == FYTT_VERSION_DIRECTIVE ||
		    fyt->type == FYTT_TAG_DIRECTIVE ||
		    fyt->type == FYTT_DOCUMENT_START ||
		    fyt->type == FYTT_DOCUMENT_END ||
		    fyt->type == FYTT_STREAM_END) {

			if (fyt->type == FYTT_DOCUMENT_START ||
			    fyt->type == FYTT_DOCUMENT_END) {
				fyp->document_has_content = false;
				fyp->document_first_content_token = true;
				fyp->had_directives = false;
			}

			fy_parse_state_set(fyp, fy_parse_state_pop(fyp));

			fyep = fy_parse_empty_scalar(fyp);
			fyp_error_check(fyp, fyep, err_out,
					"fy_parse_empty_scalar() failed");
			break;
		}

		fyp->document_has_content = true;
		fyp_parse_debug(fyp, "document has content now");
		/* fallthrough */

	case FYPS_BLOCK_NODE:

		fyep = fy_parse_node(fyp, fyt,
				fyp->state == FYPS_BLOCK_NODE ||
				fyp->state == FYPS_DOCUMENT_CONTENT);
		fyp_error_check(fyp, fyep, err_out,
				"fy_parse_node() failed");
		break;

	case FYPS_BLOCK_SEQUENCE_FIRST_ENTRY:
		is_first = true;
		/* fallthrough */

	case FYPS_BLOCK_SEQUENCE_ENTRY:
	case FYPS_INDENTLESS_SEQUENCE_ENTRY:

		if ((fyp->state == FYPS_BLOCK_SEQUENCE_ENTRY ||
		     fyp->state == FYPS_BLOCK_SEQUENCE_FIRST_ENTRY) &&
		    !(fyt->type == FYTT_BLOCK_ENTRY ||
		      fyt->type == FYTT_BLOCK_END)) {


			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
					!(fyt->type == FYTT_SCALAR), err_out,
					"invalid scalar at the end of block sequence");

			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
					!(fyt->type == FYTT_BLOCK_SEQUENCE_START), err_out,
					"wrongly indented sequence item");

			FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
					false, err_out,
					"did not find expected '-' indicator");
		}

		if (fyt->type == FYTT_BLOCK_ENTRY) {

			/* BLOCK entry */
			fyt = fy_scan_remove_peek(fyp, fyt);
			fyp_error_check(fyp, fyt, err_out,
					"failed to peek token");
#ifdef FY_DEVMODE
			fyp_debug_dump_token(fyp, fyt, "next: ");
#endif

			/* check whether it's a sequence entry or not */
			is_seq = fyt->type != FYTT_BLOCK_ENTRY && fyt->type != FYTT_BLOCK_END;
			if (!is_seq && fyp->state == FYPS_INDENTLESS_SEQUENCE_ENTRY)
				is_seq = fyt->type != FYTT_KEY && fyt->type != FYTT_VALUE;

			if (is_seq) {
				rc = fy_parse_state_push(fyp, fyp->state);
				fyp_error_check(fyp, !rc, err_out,
						"failed to push state");

				fyep = fy_parse_node(fyp, fyt, true);
				fyp_error_check(fyp, fyep, err_out,
						"fy_parse_node() failed");
				break;
			}
			fy_parse_state_set(fyp, FYPS_BLOCK_SEQUENCE_ENTRY);

			fyep = fy_parse_empty_scalar(fyp);
			fyp_error_check(fyp, fyep, err_out,
					"fy_parse_empty_scalar() failed");
			break;
		}

		/* FYTT_BLOCK_END */
		fy_parse_state_set(fyp, fy_parse_state_pop(fyp));

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_SEQUENCE_END;
		if (orig_state == FYPS_INDENTLESS_SEQUENCE_ENTRY) {

			/* allocate and copy in place */
			fytn = fy_token_alloc_rl(fyp->recycled_token_list);
			fyp_error_check(fyp, fytn, err_out,
					"fy_token_alloc_rl() failed!");
			fytn->type = FYTT_BLOCK_END;
			fytn->handle = fyt->handle;
			fytn->handle.end_mark = fytn->handle.start_mark;	/* no extent */
			fy_input_ref(fytn->handle.fyi);

			fye->sequence_end.sequence_end = fytn;
		} else
			fye->sequence_end.sequence_end = fy_scan_remove(fyp, fyt);

		ev_handle = &fye->sequence_end.sequence_end->handle;
		break;

	case FYPS_BLOCK_MAPPING_FIRST_KEY:
		is_first = true;
		/* fallthrough */

	case FYPS_BLOCK_MAPPING_KEY:

		if (!(fyt->type == FYTT_KEY || fyt->type == FYTT_BLOCK_END || fyt->type == FYTT_STREAM_END)) {

			if (fyt->type == FYTT_SCALAR)
				FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
						!fyp->simple_key_allowed && !fyp->flow_level && fy_parse_peek(fyp) == ':' ?
							"invalid block mapping key on same line as previous key" :
							"invalid value after mapping");
			else if (fyt->type == FYTT_BLOCK_SEQUENCE_START)
				FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
						"wrong indendation in sequence while in mapping");
			else if (fyt->type == FYTT_ANCHOR)
				FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
						"two anchors for a single value while in mapping");
			else if (fyt->type == FYTT_BLOCK_MAPPING_START)
				FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
						!fyp->flow_level && fyp->last_block_mapping_key_line == fy_token_start_line(fyt) ?
							"invalid nested block mapping on the same line" :
							"invalid indentation in mapping");
			else if (fyt->type == FYTT_ALIAS)
				FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
						"invalid combination of anchor plus alias");
			else
				FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
						"did not find expected key");
			goto err_out;
		}

		if (fyt->type == FYTT_KEY) {

			fyp->last_block_mapping_key_line = fy_token_end_line(fyt);

			/* KEY entry */
			fyt = fy_scan_remove_peek(fyp, fyt);
			fyp_error_check(fyp, fyt, err_out,
					"failed to peek token");
#ifdef FY_DEVMODE
			fyp_debug_dump_token(fyp, fyt, "next: ");
#endif

			/* check whether it's a block entry or not */
			is_block = fyt->type != FYTT_KEY && fyt->type != FYTT_VALUE &&
				fyt->type != FYTT_BLOCK_END;

			if (is_block) {
				rc = fy_parse_state_push(fyp, FYPS_BLOCK_MAPPING_VALUE);
				fyp_error_check(fyp, !rc, err_out,
						"failed to push state");

				fyep = fy_parse_node(fyp, fyt, true);
				fyp_error_check(fyp, fyep, err_out,
						"fy_parse_node() failed");
				break;
			}
			fy_parse_state_set(fyp, FYPS_BLOCK_MAPPING_VALUE);

			fyep = fy_parse_empty_scalar(fyp);
			fyp_error_check(fyp, fyep, err_out,
					"fy_parse_empty_scalar() failed");
			break;
		}

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		/* FYTT_BLOCK_END */
		fy_parse_state_set(fyp, fy_parse_state_pop(fyp));
		fye->type = FYET_MAPPING_END;
		fye->mapping_end.mapping_end = fy_scan_remove(fyp, fyt);

		ev_handle = &fye->mapping_end.mapping_end->handle;
		break;

	case FYPS_BLOCK_MAPPING_VALUE:

		if (fyt->type == FYTT_VALUE) {

			/* VALUE entry */
			fyt = fy_scan_remove_peek(fyp, fyt);
			fyp_error_check(fyp, fyt, err_out,
					"failed to peek token");
#ifdef FY_DEVMODE
			fyp_debug_dump_token(fyp, fyt, "next: ");
#endif

			/* check whether it's a block entry or not */
			is_value = fyt->type != FYTT_KEY && fyt->type != FYTT_VALUE &&
				fyt->type != FYTT_BLOCK_END;

			if (is_value) {
				rc = fy_parse_state_push(fyp, FYPS_BLOCK_MAPPING_KEY);
				fyp_error_check(fyp, !rc, err_out,
						"failed to push state");

				fyep = fy_parse_node(fyp, fyt, true);
				fyp_error_check(fyp, fyep, err_out,
						"fy_parse_node() failed");
				break;
			}
		}

		fy_parse_state_set(fyp, FYPS_BLOCK_MAPPING_KEY);

		fyep = fy_parse_empty_scalar(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_parse_empty_scalar() failed");
		break;

	case FYPS_FLOW_SEQUENCE_FIRST_ENTRY:
		is_first = true;
		/* fallthrough */

	case FYPS_FLOW_SEQUENCE_ENTRY:

		if (fyt->type != FYTT_FLOW_SEQUENCE_END &&
		    fyt->type != FYTT_STREAM_END) {

			if (!is_first) {
				FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
						fyt->type == FYTT_FLOW_ENTRY, err_out,
						"missing comma in flow %s",
							fyp->state == FYPS_FLOW_SEQUENCE_ENTRY ?
								"sequence" : "mapping");

				fyt = fy_scan_remove_peek(fyp, fyt);
				fyp_error_check(fyp, fyt, err_out,
						"failed to peek token");
#ifdef FY_DEVMODE
				fyp_debug_dump_token(fyp, fyt, "next: ");
#endif
			}

			if (fyt->type == FYTT_KEY) {
				fy_parse_state_set(fyp, FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_KEY);

				fyep = fy_parse_eventp_alloc(fyp);
				fyp_error_check(fyp, fyep, err_out,
						"fy_eventp_alloc() failed!");
				fye = &fyep->e;

				/* convert KEY token to either block or flow mapping start */
				if (!fyt->key.flow_level)
					fyt->type = FYTT_BLOCK_MAPPING_START;
				else
					fyt->type = FYTT_FLOW_MAPPING_START;

				fye->type = FYET_MAPPING_START;
				fye->mapping_start.anchor = NULL;
				fye->mapping_start.tag = NULL;
				fye->mapping_start.mapping_start = fy_scan_remove(fyp, fyt);

				ev_handle = &fye->mapping_start.mapping_start->handle;
				break;
			}

			if (fyt->type != FYTT_FLOW_SEQUENCE_END) {
				rc = fy_parse_state_push(fyp, FYPS_FLOW_SEQUENCE_ENTRY);
				fyp_error_check(fyp, !rc, err_out,
						"failed to push state");

				fyep = fy_parse_node(fyp, fyt, false);
				fyp_error_check(fyp, fyep, err_out,
						"fy_parse_node() failed");
				break;
			}
		}

		if (fyt->type == FYTT_STREAM_END && fyp->flow_level) {
			FYP_TOKEN_ERROR(fyp, fyt, FYEM_PARSE,
					"flow sequence without a closing bracket");
			goto err_out;
		}

		/* FYTT_FLOW_SEQUENCE_END */
		fy_parse_state_set(fyp, fy_parse_state_pop(fyp));

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_SEQUENCE_END;
		fye->sequence_end.sequence_end = fy_scan_remove(fyp, fyt);

		ev_handle = &fye->sequence_end.sequence_end->handle;
		break;

	case FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_KEY:
		if (fyt->type != FYTT_VALUE && fyt->type != FYTT_FLOW_ENTRY &&
		    fyt->type != FYTT_FLOW_SEQUENCE_END) {
			rc = fy_parse_state_push(fyp, FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_VALUE);
			fyp_error_check(fyp, !rc, err_out,
					"failed to push state");

			fyep = fy_parse_node(fyp, fyt, false);
			fyp_error_check(fyp, fyep, err_out,
					"fy_parse_node() failed");
			break;
		}

		/* empty keys are not allowed in JSON mode */
		FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
				!fyp_json_mode(fyp), err_out,
				"JSON does not allow empty keys of a mapping");

		fy_parse_state_set(fyp, FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_VALUE);

		fyep = fy_parse_empty_scalar(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_parse_empty_scalar() failed");
		break;

	case FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_VALUE:
		if (fyt->type == FYTT_VALUE) {
			fyt = fy_scan_remove_peek(fyp, fyt);
			fyp_error_check(fyp, fyt, err_out,
					"failed to peek token");
#ifdef FY_DEVMODE
			fyp_debug_dump_token(fyp, fyt, "next: ");
#endif

			if (fyt->type != FYTT_FLOW_ENTRY && fyt->type != FYTT_FLOW_SEQUENCE_END) {
				rc = fy_parse_state_push(fyp, FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_END);
				fyp_error_check(fyp, !rc, err_out,
						"failed to push state");

				fyep = fy_parse_node(fyp, fyt, false);
				fyp_error_check(fyp, fyep, err_out,
						"fy_parse_node() failed");
				break;
			}
		}

		/* empty values are not allowed in JSON mode */
		FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
				!fyp_json_mode(fyp), err_out,
				"JSON does not allow empty values in a mapping");

		fy_parse_state_set(fyp, FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_END);

		fyep = fy_parse_empty_scalar(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_parse_empty_scalar() failed");
		break;

	case FYPS_FLOW_SEQUENCE_ENTRY_MAPPING_END:
		fy_parse_state_set(fyp, FYPS_FLOW_SEQUENCE_ENTRY);

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_MAPPING_END;

		/* allocate and copy in place */
		fytn = fy_token_alloc_rl(fyp->recycled_token_list);
		fyp_error_check(fyp, fytn, err_out,
				"fy_token_alloc_rl() failed!");
		fytn->type = FYTT_BLOCK_END;
		fytn->handle = fyt->handle;
		fytn->handle.end_mark = fytn->handle.start_mark;	/* no extent */
		fy_input_ref(fytn->handle.fyi);

		fye->mapping_end.mapping_end = fytn;

		ev_handle = &fye->mapping_end.mapping_end->handle;
		break;

	case FYPS_FLOW_MAPPING_FIRST_KEY:
		is_first = true;
		/* fallthrough */

	case FYPS_FLOW_MAPPING_KEY:
		if (fyt->type != FYTT_FLOW_MAPPING_END) {

			if (!is_first) {
				FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
						fyt->type == FYTT_FLOW_ENTRY, err_out,
						"missing comma in flow %s",
							fyp->state == FYPS_FLOW_SEQUENCE_ENTRY ?
								"sequence" : "mapping");

				fyt = fy_scan_remove_peek(fyp, fyt);
				fyp_error_check(fyp, fyt, err_out,
						"failed to peek token");
#ifdef FY_DEVMODE
				fyp_debug_dump_token(fyp, fyt, "next: ");
#endif
			}

			if (fyt->type == FYTT_KEY) {
				/* next token */
				fyt = fy_scan_remove_peek(fyp, fyt);
				fyp_error_check(fyp, fyt, err_out,
						"failed to peek token");
#ifdef FY_DEVMODE
				fyp_debug_dump_token(fyp, fyt, "next: ");
#endif

				/* JSON key checks */
				FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
						!fyp_json_mode(fyp) || fyt->type != FYTT_VALUE,
						err_out, "JSON does not allow empty keys");
				FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
						!fyp_json_mode(fyp) ||
							(fyt->type == FYTT_SCALAR &&
							 fyt->scalar.style == FYSS_DOUBLE_QUOTED),
						err_out, "JSON only allows double quoted scalar keys");

				if (fyt->type != FYTT_VALUE &&
				    fyt->type != FYTT_FLOW_ENTRY &&
				    fyt->type != FYTT_FLOW_MAPPING_END) {

					rc = fy_parse_state_push(fyp, FYPS_FLOW_MAPPING_VALUE);
					fyp_error_check(fyp, !rc, err_out,
							"failed to push state");

					fyep = fy_parse_node(fyp, fyt, false);
					fyp_error_check(fyp, fyep, err_out,
							"fy_parse_node() failed");
					break;
				}

				fy_parse_state_set(fyp, FYPS_FLOW_MAPPING_VALUE);

				fyep = fy_parse_empty_scalar(fyp);
				fyp_error_check(fyp, fyep, err_out,
						"fy_parse_empty_scalar() failed");
				break;
			}

			if (fyt->type != FYTT_FLOW_MAPPING_END) {

				/* empty values are not allowed in JSON mode */
				FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
						!fyp_json_mode(fyp), err_out,
						"JSON does not allow empty values in a mapping");

				rc = fy_parse_state_push(fyp, FYPS_FLOW_MAPPING_EMPTY_VALUE);
				fyp_error_check(fyp, !rc, err_out,
						"failed to push state");

				fyep = fy_parse_node(fyp, fyt, false);
				fyp_error_check(fyp, fyep, err_out,
						"fy_parse_node() failed");
				break;
			}
		}

		/* FYTT_FLOW_MAPPING_END */
		fy_parse_state_set(fyp, fy_parse_state_pop(fyp));

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out, "fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_MAPPING_END;
		fye->mapping_end.mapping_end = fy_scan_remove(fyp, fyt);

		ev_handle = &fye->mapping_end.mapping_end->handle;
		break;

	case FYPS_FLOW_MAPPING_VALUE:
		if (fyt->type == FYTT_VALUE) {
			/* next token */
			fyt = fy_scan_remove_peek(fyp, fyt);
			fyp_error_check(fyp, fyt, err_out,
					"failed to peek token");
#ifdef FY_DEVMODE
			fyp_debug_dump_token(fyp, fyt, "next: ");
#endif

			if (fyt->type != FYTT_FLOW_ENTRY &&
			    fyt->type != FYTT_FLOW_MAPPING_END) {

				rc = fy_parse_state_push(fyp, FYPS_FLOW_MAPPING_KEY);
				fyp_error_check(fyp, !rc, err_out,
						"failed to push state");

				fyep = fy_parse_node(fyp, fyt, false);
				fyp_error_check(fyp, fyep, err_out,
						"fy_parse_node() failed");
				break;
			}
		}

		/* empty values are not allowed in JSON mode */
		FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
				!fyp_json_mode(fyp), err_out,
				"JSON does not allow empty values in a mapping");

		fy_parse_state_set(fyp, FYPS_FLOW_MAPPING_KEY);

		fyep = fy_parse_empty_scalar(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_parse_empty_scalar() failed");
		break;

	case FYPS_FLOW_MAPPING_EMPTY_VALUE:
		fy_parse_state_set(fyp, FYPS_FLOW_MAPPING_KEY);

		fyep = fy_parse_empty_scalar(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_parse_empty_scalar() failed");
		break;

	case FYPS_SINGLE_DOCUMENT_END:

		FYP_TOKEN_ERROR_CHECK(fyp, fyt, FYEM_PARSE,
				fyt->type == FYTT_STREAM_END, err_out,
				"Did not find expected stream end");

		rc = fy_parse_stream_end(fyp);
		fyp_error_check(fyp, !rc, err_out,
				"stream end failed");

		fyep = fy_parse_eventp_alloc(fyp);
		fyp_error_check(fyp, fyep, err_out,
				"fy_eventp_alloc() failed!");
		fye = &fyep->e;

		fye->type = FYET_STREAM_END;
		fye->stream_end.stream_end = fy_scan_remove(fyp, fyt);

		fy_parse_state_set(fyp,
			fy_parse_have_more_inputs(fyp) ? FYPS_NONE : FYPS_END);

		ev_handle = &fye->stream_end.stream_end->handle;
		break;

	case FYPS_END:
		/* should never happen */
		assert(0);
		break;
	}

	assert(fyep);

	if (ev_handle) {
		fy_input_unref(fyp->last_event_handle.fyi);
		fyp->last_event_handle = *ev_handle;
		fyp->last_event_handle.start_mark = fyp->last_event_handle.end_mark;
		fy_atom_reset_storage_hints(&handle);
		fy_input_ref(fyp->last_event_handle.fyi);
	}

	return fyep;

err_out:
	fy_token_unref_rl(fyp->recycled_token_list, version_directive);
	fy_token_list_unref_all_rl(fyp->recycled_token_list, &tag_directives);
	fy_parse_eventp_recycle(fyp, fyep);
	fyp->stream_error = true;
	return NULL;
}

const char *fy_event_type_txt[] = {
	[FYET_NONE]		= "NONE",
	[FYET_STREAM_START]	= "+STR",
	[FYET_STREAM_END]	= "-STR",
	[FYET_DOCUMENT_START]	= "+DOC",
	[FYET_DOCUMENT_END]	= "-DOC",
	[FYET_MAPPING_START]	= "+MAP",
	[FYET_MAPPING_END]	= "-MAP",
	[FYET_SEQUENCE_START]	= "+SEQ",
	[FYET_SEQUENCE_END]	= "-SEQ",
	[FYET_SCALAR]		= "=VAL",
	[FYET_ALIAS]		= "=ALI",
};

const char *fy_event_type_get_text(enum fy_event_type type)
{
	if ((unsigned int)type >= ARRAY_SIZE(fy_event_type_txt))
		return "*BAD";
	return fy_event_type_txt[type];
}

struct fy_eventp *fy_parse_private(struct fy_parser *fyp)
{
	struct fy_eventp *fyep = NULL;

	fyep = fy_parse_internal(fyp);
	fyp_parse_debug(fyp, "> %s", fyep ? fy_event_type_txt[fyep->e.type] : "NULL");

	return fyep;
}

struct fy_parser *fy_parser_create(const struct fy_parse_cfg *cfg)
{
	struct fy_parser *fyp;
	int rc;

	if (!cfg)
		return NULL;

	fyp = malloc(sizeof(*fyp));
	if (!fyp)
		return NULL;

	rc = fy_parse_setup(fyp, cfg);
	if (rc) {
		free(fyp);
		return NULL;
	}

	return fyp;
}

void fy_parser_destroy(struct fy_parser *fyp)
{
	if (!fyp)
		return;

	fy_parse_cleanup(fyp);

	free(fyp);
}

const struct fy_parse_cfg *fy_parser_get_cfg(struct fy_parser *fyp)
{
	if (!fyp)
		return NULL;
	return &fyp->cfg;
}

struct fy_diag *fy_parser_get_diag(struct fy_parser *fyp)
{
	if (!fyp || !fyp->diag)
		return NULL;
	return fy_diag_ref(fyp->diag);
}

int fy_parser_set_diag(struct fy_parser *fyp, struct fy_diag *diag)
{
	struct fy_diag_cfg dcfg;

	if (!fyp)
		return -1;

	/* default? */
	if (!diag) {
		fy_diag_cfg_default(&dcfg);
		diag = fy_diag_create(&dcfg);
		if (!diag)
			return -1;
	}

	fy_diag_unref(fyp->diag);
	fyp->diag = fy_diag_ref(diag);

	return 0;
}

static void fy_parse_input_reset(struct fy_parser *fyp)
{
	struct fy_input *fyi, *fyin;

	for (fyi = fy_input_list_head(&fyp->queued_inputs); fyi; fyi = fyin) {
		fyin = fy_input_next(&fyp->queued_inputs, fyi);
		fy_input_unref(fyi);
	}

	fy_parse_parse_state_log_list_recycle_all(fyp, &fyp->state_stack);

	fyp->stream_start_produced = false;
	fyp->stream_end_produced = false;
	fyp->stream_end_reached = false;
	fyp->state = FYPS_NONE;

	fyp->pending_complex_key_column = -1;
	fyp->last_block_mapping_key_line = -1;

	fy_input_unref(fyp->last_event_handle.fyi);
	fy_atom_reset(&fyp->last_event_handle);
}

int fy_parser_set_input_file(struct fy_parser *fyp, const char *file)
{
	struct fy_input_cfg fyic;
	int rc;

	if (!fyp || !file)
		return -1;

	memset(&fyic, 0, sizeof(fyic));

	if (!strcmp(file, "-")) {
		fyic.type = fyit_stream;
		fyic.stream.name = "stdin";
		fyic.stream.fp = stdin;
	} else {
		fyic.type = fyit_file;
		fyic.file.filename = file;
	}
	fyic.ignore_stdio = !!(fyp->cfg.flags & FYPCF_DISABLE_BUFFERING);

	/* must not be in the middle of something */
	fyp_error_check(fyp, fyp->state == FYPS_NONE || fyp->state == FYPS_END,
			err_out, "parser cannot be reset at state '%s'",
				state_txt[fyp->state]);

	fy_parse_input_reset(fyp);

	rc = fy_parse_input_append(fyp, &fyic);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_parse_input_append() failed");

	return 0;
err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_parser_set_string(struct fy_parser *fyp, const char *str, size_t len)
{
	struct fy_input_cfg fyic;
	int rc;

	if (!fyp || !str)
		return -1;

	if (len == (size_t)-1)
		len = strlen(str);

	memset(&fyic, 0, sizeof(fyic));

	fyic.type = fyit_memory;
	fyic.memory.data = str;
	fyic.memory.size = len;

	/* must not be in the middle of something */
	fyp_error_check(fyp, fyp->state == FYPS_NONE || fyp->state == FYPS_END,
			err_out, "parser cannot be reset at state '%s'",
				state_txt[fyp->state]);

	fy_parse_input_reset(fyp);

	rc = fy_parse_input_append(fyp, &fyic);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_parse_input_append() failed");

	return 0;
err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_parser_set_malloc_string(struct fy_parser *fyp, char *str, size_t len)
{
	struct fy_input_cfg fyic;
	int rc;

	if (!fyp || !str)
		return -1;

	if (len == (size_t)-1)
		len = strlen(str);

	memset(&fyic, 0, sizeof(fyic));

	fyic.type = fyit_alloc;
	fyic.alloc.data = str;
	fyic.alloc.size = len;

	/* must not be in the middle of something */
	fyp_error_check(fyp, fyp->state == FYPS_NONE || fyp->state == FYPS_END,
			err_out, "parser cannot be reset at state '%s'",
				state_txt[fyp->state]);

	fy_parse_input_reset(fyp);

	rc = fy_parse_input_append(fyp, &fyic);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_parse_input_append() failed");

	return 0;
err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_parser_set_input_fp(struct fy_parser *fyp, const char *name, FILE *fp)
{
	struct fy_input_cfg fyic;
	int rc;

	if (!fyp || !fp)
		return -1;

	memset(&fyic, 0, sizeof(fyic));

	fyic.type = fyit_stream;
	fyic.stream.name = name ? name : "<stream>";
	fyic.stream.fp = fp;
	fyic.ignore_stdio = !!(fyp->cfg.flags & FYPCF_DISABLE_BUFFERING);

	/* must not be in the middle of something */
	fyp_error_check(fyp, fyp->state == FYPS_NONE || fyp->state == FYPS_END,
			err_out, "parser cannot be reset at state '%s'",
				state_txt[fyp->state]);

	fy_parse_input_reset(fyp);

	rc = fy_parse_input_append(fyp, &fyic);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_parse_input_append() failed");

	return 0;
err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_parser_set_input_callback(struct fy_parser *fyp, void *user,
		ssize_t (*callback)(void *user, void *buf, size_t count))
{
	struct fy_input_cfg fyic;
	int rc;

	if (!fyp || !callback)
		return -1;

	memset(&fyic, 0, sizeof(fyic));

	fyic.type = fyit_callback;
	fyic.userdata = user;
	fyic.callback.input = callback;
	fyic.ignore_stdio = !!(fyp->cfg.flags & FYPCF_DISABLE_BUFFERING);

	/* must not be in the middle of something */
	fyp_error_check(fyp, fyp->state == FYPS_NONE || fyp->state == FYPS_END,
			err_out, "parser cannot be reset at state '%s'",
				state_txt[fyp->state]);

	fy_parse_input_reset(fyp);

	rc = fy_parse_input_append(fyp, &fyic);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_parse_input_append() failed");

	return 0;
err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_parser_set_input_fd(struct fy_parser *fyp, int fd)
{
	struct fy_input_cfg fyic;
	int rc;

	if (!fyp || fd < 0)
		return -1;

	memset(&fyic, 0, sizeof(fyic));

	fyic.type = fyit_fd;
	fyic.fd.fd = fd;
	fyic.ignore_stdio = !!(fyp->cfg.flags & FYPCF_DISABLE_BUFFERING);

	/* must not be in the middle of something */
	fyp_error_check(fyp, fyp->state == FYPS_NONE || fyp->state == FYPS_END,
			err_out, "parser cannot be reset at state '%s'",
				state_txt[fyp->state]);

	fy_parse_input_reset(fyp);

	rc = fy_parse_input_append(fyp, &fyic);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_parse_input_append() failed");

	return 0;
err_out:
	rc = -1;
err_out_rc:
	return rc;
}

int fy_parser_reset(struct fy_parser *fyp)
{
	int rc;

	if (!fyp)
		return -1;

	fy_parse_input_reset(fyp);

	fy_reader_reset(fyp->reader);

	fyp->next_single_document = false;
	fyp->stream_error = false;
	fyp->generated_block_map = false;
	fyp->last_was_comma = false;
	fyp->document_has_content = false;
	fyp->document_first_content_token = false;
	fyp->bare_document_only = false;
	fyp->stream_has_content = false;
	fyp->had_directives = false;

	assert(fyp->diag);
	fyp->diag->on_error = false;

	rc = fy_reset_document_state(fyp);
	fyp_error_check(fyp, !rc, err_out_rc,
			"fy_parse_input_reset() failed");

	return 0;
err_out_rc:
	return rc;
}

struct fy_event *fy_parser_parse(struct fy_parser *fyp)
{
	struct fy_eventp *fyep;
	enum fy_composer_return ret;

	if (!fyp)
		return NULL;

	fyep = fy_parse_private(fyp);
	if (!fyep)
		return NULL;

	if (fyp->fyc) {
		ret = fy_composer_process_event(fyp->fyc, &fyep->e);
		if (ret == FYCR_ERROR) {
			fyp->stream_error = true;
			fy_parse_eventp_recycle(fyp, fyep);
			return NULL;
		}
		/* note that the stop should be handled by
		 * an out of band mechanism */
	}

	return &fyep->e;
}

bool fy_parser_get_stream_error(struct fy_parser *fyp)
{
	if (!fyp)
		return true;

	return fyp->stream_error;
}

enum fy_parse_cfg_flags fy_parser_get_cfg_flags(const struct fy_parser *fyp)
{
	if (!fyp)
		return 0;

	return fyp->cfg.flags;
}

struct fy_document_state *fy_parser_get_document_state(struct fy_parser *fyp)
{
	return fyp ? fyp->current_document_state : NULL;
}

static enum fy_composer_return
parse_process_event(struct fy_composer *fyc, struct fy_path *path, struct fy_event *fye)
{
	struct fy_parser *fyp = fy_composer_get_cfg_userdata(fyc);

	assert(fyp);
	assert(fyp->fyc_cb);
	return fyp->fyc_cb(fyp, fye, path, fyp->fyc_userdata);
}

struct fy_document_builder *
parse_create_document_builder(struct fy_composer *fyc)
{
	struct fy_parser *fyp = fy_composer_get_cfg_userdata(fyc);
	struct fy_document_builder *fydb = NULL;
	struct fy_document_builder_cfg cfg;
	struct fy_document_state *fyds;
	int rc;

	memset(&cfg, 0, sizeof(cfg));
	cfg.parse_cfg = fyp->cfg;
	cfg.diag = fy_diag_ref(fyp->diag);

	fydb = fy_document_builder_create(&cfg);
	fyp_error_check(fyp, fydb, err_out,
			"fy_document_builder_create() failed\n");

	/* start with this document state */
	fyds = fy_parser_get_document_state(fyp);
	rc = fy_document_builder_set_in_document(fydb, fyds, true);
	fyp_error_check(fyp, !rc, err_out,
			"fy_document_builder_set_in_document() failed\n");

	return fydb;

err_out:
	fy_document_builder_destroy(fydb);
	return NULL;
}

static const struct fy_composer_ops parser_composer_ops = {
	.process_event = parse_process_event,
	.create_document_builder = parse_create_document_builder,
};

int fy_parse_set_composer(struct fy_parser *fyp, fy_parse_composer_cb cb, void *userdata)
{
	struct fy_composer_cfg ccfg;

	if (!fyp)
		return -1;

	/* must not be in the middle of something */
	fyp_error_check(fyp, fyp->state == FYPS_NONE || fyp->state == FYPS_END,
			err_out, "cannot change composer state at state '%s'",
				state_txt[fyp->state]);

	/* clear */
	if (!cb) {
		if (fyp->fyc) {
			fy_composer_destroy(fyp->fyc);
			fyp->fyc = NULL;
		}
		fyp->fyc_cb = NULL;
		fyp->fyc_userdata = NULL;
		return 0;
	}

	/* already exists */
	if (fyp->fyc) {
		fyp->fyc_cb = cb;
		fyp->fyc_userdata = userdata;
		return 0;
	}

	/* prepare the composer configuration */
	memset(&ccfg, 0, sizeof(ccfg));
	ccfg.ops = &parser_composer_ops;
	ccfg.userdata = fyp;
	ccfg.diag = fy_parser_get_diag(fyp);
	fyp->fyc = fy_composer_create(&ccfg);
	fyp_error_check(fyp, fyp->fyc, err_out,
			"fy_composer_create() failed");

	fyp->fyc_cb = cb;
	fyp->fyc_userdata = userdata;

	return 0;
err_out:
	return -1;
}

static enum fy_composer_return fy_parse_compose_internal(struct fy_parser *fyp)
{
	struct fy_composer *fyc;
	struct fy_document_iterator *fydi;
	struct fy_event *fye;
	struct fy_eventp *fyep;
	struct fy_document *fyd = NULL;
	enum fy_composer_return ret;

	assert(fyp);

	fyc = fyp->fyc;
	assert(fyc);

	/* simple, without resolution */
	if (!(fyp->cfg.flags & FYPCF_RESOLVE_DOCUMENT)) {

		ret = FYCR_OK_STOP;
		while ((fyep = fy_parse_private(fyp)) != NULL) {
			ret = fy_composer_process_event(fyc, &fyep->e);
			fy_parse_eventp_recycle(fyp, fyep);
			if (ret != FYCR_OK_CONTINUE)
				break;
		}
		return ret;
	}

	fydi = fy_document_iterator_create();
	fyp_error_check(fyp, fydi, err_out,
			"fy_document_iterator_create() failed");

	/* stream start event generation and processing */
	fye = fy_document_iterator_stream_start(fydi);
	fyp_error_check(fyp, fye, err_out,
			"fy_document_iterator_stream_start() failed");
	ret = fy_composer_process_event(fyc, fye);
	fy_document_iterator_event_free(fydi, fye);
	fye = NULL;
	if (ret != FYCR_OK_CONTINUE)
		goto out;

	/* convert to document and then process the generator event stream it */
	while ((fyd = fy_parse_load_document(fyp)) != NULL) {

		/* document start event generation and processing */
		fye = fy_document_iterator_document_start(fydi, fyd);
		fyp_error_check(fyp, fye, err_out,
				"fy_document_iterator_document_start() failed");
		ret = fy_composer_process_event(fyc, fye);
		fy_document_iterator_event_free(fydi, fye);
		fye = NULL;
		if (ret != FYCR_OK_CONTINUE)
			goto out;

		/* and now process the body */
		ret = FYCR_OK_CONTINUE;
		while ((fye = fy_document_iterator_body_next(fydi)) != NULL) {
			ret = fy_composer_process_event(fyc, fye);
			fy_document_iterator_event_free(fydi, fye);
			fye = NULL;
			if (ret != FYCR_OK_CONTINUE)
				goto out;
		}

		/* document end event generation and processing */
		fye = fy_document_iterator_document_end(fydi);
		fyp_error_check(fyp, fye, err_out,
				"fy_document_iterator_document_end() failed");
		ret = fy_composer_process_event(fyc, fye);
		fy_document_iterator_event_free(fydi, fye);
		fye = NULL;
		if (ret != FYCR_OK_CONTINUE)
			goto out;

		/* and destroy the document */
		fy_parse_document_destroy(fyp, fyd);
		fyd = NULL;
	}

	/* stream end event generation and processing */
	fye = fy_document_iterator_stream_end(fydi);
	fyp_error_check(fyp, fye, err_out,
			"fy_document_iterator_stream_end() failed");
	ret = fy_composer_process_event(fyc, fye);
	fy_document_iterator_event_free(fydi, fye);
	fye = NULL;
	if (ret != FYCR_OK_CONTINUE)
		goto out;

out:
	/* NULLs are OK */
	fy_parse_document_destroy(fyp, fyd);
	fy_document_iterator_destroy(fydi);
	return ret;

err_out:
	ret = FYCR_ERROR;
	goto out;
}

int fy_parse_compose(struct fy_parser *fyp, fy_parse_composer_cb cb, void *userdata)
{
	enum fy_composer_return ret;
	int rc, rc_out;

	if (!fyp || !cb)
		return -1;

	/* set the composer callback */
	rc = fy_parse_set_composer(fyp, cb, userdata);
	fyp_error_check(fyp, !rc, err_out,
			"fy_parse_set_composer() failed\n");

	/* use the composer to parse */
	ret = fy_parse_compose_internal(fyp);
	/* on error set the stream error */
	if (ret == FYCR_ERROR) {
		fyp->stream_error = true;
		rc_out = -1;
	} else
		rc_out = 0;

	/* reset the parser; the composer clear must always succeed */
	fy_parser_reset(fyp);

	/* clear composer */
	rc = fy_parse_set_composer(fyp, NULL, NULL);
	fyp_error_check(fyp, !rc, err_out,
			"fy_parse_set_composer() failed\n");

	return rc_out;

err_out:
	return -1;
}
