/*
 * fy-ctype.h - ctype like macros header
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_CTYPE_H
#define FY_CTYPE_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <libfyaml.h>

#include "fy-utf8.h"

enum fy_lb_mode {
	fylb_cr_nl,			/* only \r, \n (json + >= yaml1.2 */
	fylb_cr_nl_N_L_P,		/* NEL/LS/PS (yaml1.1) */
};

enum fy_flow_ws_mode {
	fyfws_space_tab,		/* space + TAB (yaml) */
	fyfws_space,			/* only space (json) */
};

static inline bool fy_is_first_alpha(int c)
{
	return (c >= 'a' && c <= 'z') ||
	       (c >= 'A' && c <= 'Z') ||
	       c == '_';
}

static inline bool fy_is_alpha(int c)
{
	return fy_is_first_alpha(c) || c == '-';
}

static inline bool fy_is_num(int c)
{
	return c >= '0' && c <= '9';
}

static inline bool fy_is_first_alnum(int c)
{
	return fy_is_first_alpha(c);
}

static inline bool fy_is_alnum(int c)
{
	return fy_is_alpha(c) || fy_is_num(c);
}

static inline bool fy_is_space(int c)
{
	return c == ' ';
}

static inline bool fy_is_tab(int c)
{
	return c == '\t';
}

static inline bool fy_is_ws(int c)
{
	return fy_is_space(c) || fy_is_tab(c);
}

static inline bool fy_is_hex(int c)
{
	return (c >= '0' && c <= '9') ||
	       (c >= 'a' && c <= 'f') ||
	       (c >= 'A' && c <= 'F');
}

static inline bool fy_is_uri(int c)
{
	return fy_is_alnum(c) || fy_utf8_strchr(";/?:@&=+$,.!~*\'()[]%", c);
}

static inline bool fy_is_lb_r_n(int c)
{
	return c == '\r' || c == '\n';
}

static inline bool fy_is_lb_NEL(int c)
{
	return c == 0x85;
}

static inline bool fy_is_lb_LS_PS(int c)
{
	return c == 0x2028 || c == 0x2029;
}

static inline bool fy_is_unicode_lb(int c)
{
	/* note that YAML1.1 supports NEL #x85, LS #x2028 and PS #x2029 as linebreaks */
	/* YAML1.2 and higher does not */
	return fy_is_lb_NEL(c) || fy_is_lb_LS_PS(c);
}

static inline bool fy_is_any_lb(int c)
{
	return fy_is_lb_r_n(c) || fy_is_unicode_lb(c);
}

static inline bool fy_is_z(int c)
{
	return c <= 0;
}

static inline bool fy_is_blank(int c)
{
	return c == ' ' || c == '\t';
}

#define FY_UTF8_BOM	0xfeff

static inline bool fy_is_print(int c)
{
	return c == '\n' || c == '\r' ||
	       (c >= 0x0020 && c <= 0x007e) ||
	       (c >= 0x00a0 && c <= 0xd7ff) ||
	       (c >= 0xe000 && c <= 0xfffd && c != FY_UTF8_BOM);
}

static inline bool fy_is_printq(int c)
{
	return c != '\t' && c != 0xa0 && !fy_is_any_lb(c) && fy_is_print(c);
}

static inline bool fy_is_nb_char(int c)
{
	return (c >= 0x0020 && c <= 0x007e) ||
	       (c >= 0x00a0 && c <= 0xd7ff) ||
	       (c >= 0xe000 && c <= 0xfffd && c != FY_UTF8_BOM);
}

static inline bool fy_is_ns_char(int c)
{
	return fy_is_nb_char(c) && !fy_is_ws(c);
}

static inline bool fy_is_start_indicator(int c)
{
	return !!fy_utf8_strchr(",[]{}#&*!|>'\"%%@`", c);
}

static inline bool fy_is_indicator_before_space(int c)
{
	return !!fy_utf8_strchr("-:?`", c);
}

static inline bool fy_is_flow_indicator(int c)
{
	return !!fy_utf8_strchr(",[]{}", c);
}

static inline bool fy_is_path_flow_scalar_start(int c)
{
	return c == '\'' || c == '"';
}

static inline bool fy_is_path_flow_key_start(int c)
{
	return c == '"' || c == '\'' || c == '{' || c == '[';
}

static inline bool fy_is_path_flow_key_end(int c)
{
	return c == '"' || c == '\'' || c == '}' || c == ']';
}

static inline bool fy_is_unicode_control(int c)
{
        return (c >= 0 && c <= 0x1f) || (c >= 0x80 && c <= 0x9f);
}

static inline bool fy_is_unicode_space(int c)
{
        return c == 0x20 || c == 0xa0 ||
               (c >= 0x2000 && c <= 0x200a) ||
               c == 0x202f || c == 0x205f || c == 0x3000;
}

static inline bool fy_is_json_unescaped(int c)
{
	return c >= 0x20 && c <= 0x110000 && c != '"' && c != '\\';
}

static inline bool fy_is_json_unescaped_range_only(int c)
{
	return c >= 0x20 && c <= 0x110000;
}

static inline bool fy_is_lb_m(int c, enum fy_lb_mode lb_mode)
{
	if (fy_is_lb_r_n(c))
		return true;
	return lb_mode == fylb_cr_nl_N_L_P && fy_is_unicode_lb(c);
}

static inline bool fy_is_generic_lb_m(int c, enum fy_lb_mode lb_mode)
{
	if (fy_is_lb_r_n(c))
		return true;
	return lb_mode == fylb_cr_nl_N_L_P && fy_is_lb_NEL(c);
}

static inline bool fy_is_lbz_m(int c, enum fy_lb_mode lb_mode)
{
	return fy_is_lb_m(c, lb_mode) || fy_is_z(c);
}

static inline bool fy_is_generic_lbz_m(int c, enum fy_lb_mode lb_mode)
{
	return fy_is_generic_lb_m(c, lb_mode) || fy_is_z(c);
}

static inline bool fy_is_blankz_m(int c, enum fy_lb_mode lb_mode)
{
	return fy_is_ws(c) || fy_is_lbz_m(c, lb_mode);
}

static inline bool fy_is_generic_blankz_m(int c, enum fy_lb_mode lb_mode)
{
	return fy_is_ws(c) || fy_is_generic_lbz_m(c, lb_mode);
}

static inline bool fy_is_flow_ws_m(int c, enum fy_flow_ws_mode fws_mode)
{
	return fy_is_space(c) || (fws_mode == fyfws_space_tab && fy_is_tab(c));
}

#define FY_CTYPE_AT_BUILDER(_kind) \
static inline const void * \
fy_find_ ## _kind (const void *s, size_t len) \
{ \
	const char *cs = (char *)s; \
	const char *e = cs + len; \
	int c, w; \
	for (; cs < e && (c = fy_utf8_get(cs,  e - cs, &w)) >= 0; cs += w) { \
		assert(w); \
		if (fy_is_ ## _kind (c)) \
			return cs; \
	} \
	return NULL; \
} \
static inline const void * \
fy_find_non_ ## _kind (const void *s, size_t len) \
{ \
	const char *cs = (char *)s; \
	const char *e = cs + len; \
	int c, w; \
	for (; cs < e && (c = fy_utf8_get(cs,  e - cs, &w)) >= 0; cs += w) { \
		assert(w); \
		if (!(fy_is_ ## _kind (c))) \
			return cs; \
		assert(w); \
	} \
	return NULL; \
} \
struct useless_struct_for_semicolon

FY_CTYPE_AT_BUILDER(first_alpha);
FY_CTYPE_AT_BUILDER(alpha);
FY_CTYPE_AT_BUILDER(num);
FY_CTYPE_AT_BUILDER(first_alnum);
FY_CTYPE_AT_BUILDER(alnum);
FY_CTYPE_AT_BUILDER(space);
FY_CTYPE_AT_BUILDER(tab);
FY_CTYPE_AT_BUILDER(ws);
FY_CTYPE_AT_BUILDER(hex);
FY_CTYPE_AT_BUILDER(uri);
FY_CTYPE_AT_BUILDER(z);
FY_CTYPE_AT_BUILDER(any_lb);
FY_CTYPE_AT_BUILDER(blank);
FY_CTYPE_AT_BUILDER(print);
FY_CTYPE_AT_BUILDER(printq);
FY_CTYPE_AT_BUILDER(nb_char);
FY_CTYPE_AT_BUILDER(ns_char);
FY_CTYPE_AT_BUILDER(start_indicator);
FY_CTYPE_AT_BUILDER(indicator_before_space);
FY_CTYPE_AT_BUILDER(flow_indicator);
FY_CTYPE_AT_BUILDER(path_flow_key_start);
FY_CTYPE_AT_BUILDER(path_flow_key_end);
FY_CTYPE_AT_BUILDER(unicode_control);
FY_CTYPE_AT_BUILDER(unicode_space);
FY_CTYPE_AT_BUILDER(json_unescaped);

/*
 * Very special linebreak/ws methods
 * Things get interesting due to \r\n and
 * unicode linebreaks/spaces
 */

/* skip for a _single_ linebreak */
static inline const void *fy_skip_lb(const void *ptr, int left)
{
	int c, width;

	/* get the utf8 character at this point */
	c = fy_utf8_get(ptr, left, &width);
	if (c < 0 || !fy_is_any_lb(c))
		return NULL;

	/* MS-DOS: check if next character is '\n' */
	if (c == '\r' && left > width && *(char *)ptr == '\n')
		width++;

	return (char *)ptr + width;
}

/* given a pointer to a chunk of memory, return pointer to first
 * ws character after the last non-ws character, or the end
 * of the chunk
 */
static inline const void *fy_last_non_ws(const void *ptr, int left)
{
	const char *s, *e;
	int c;

	s = ptr;
	e = s + left;
	while (e > s) {
		c = e[-1];
		if (c != ' ' && c != '\t')
			return e;
		e--;

	}
	return NULL;
}

const char *fy_uri_esc(const char *s, size_t len, uint8_t *code, int *code_len);

#endif
