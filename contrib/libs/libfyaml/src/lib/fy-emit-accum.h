/*
 * fy-emit-accum.h - internal YAML emitter accumulator header
 *
 * Copyright (c) 2019 Pantelis Antoniou <pantelis.antoniou@konsulko.com>
 *
 * SPDX-License-Identifier: MIT
 */
#ifndef FY_EMIT_ACCUM_H
#define FY_EMIT_ACCUM_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>

#include <libfyaml.h>

#include "fy-utf8.h"
#include "fy-event.h"

struct fy_emit_accum {
	char *accum;
	size_t alloc;
	size_t next;
	char *inplace;
	size_t inplacesz;
	int col, row;
	int ts;
	enum fy_lb_mode lb_mode;
};

static inline void
fy_emit_accum_init(struct fy_emit_accum *ea,
		   void *inplace, size_t inplacesz,
		   int ts, enum fy_lb_mode lb_mode)
{
	memset(ea, 0, sizeof(*ea));

	ea->inplace = inplace;
	ea->inplacesz = inplacesz;
	ea->accum = ea->inplace;
	ea->alloc = ea->inplacesz;
	ea->ts = ts ? ts : 8;
	ea->lb_mode = lb_mode;
}

static inline void
fy_emit_accum_reset(struct fy_emit_accum *ea)
{
	ea->next = 0;
	ea->col = 0;
	ea->row = 0;
}

static inline void
fy_emit_accum_cleanup(struct fy_emit_accum *ea)
{
	if (ea->accum && ea->accum != ea->inplace)
		free(ea->accum);
	ea->accum = ea->inplace;
	ea->alloc = ea->inplacesz;
	fy_emit_accum_reset(ea);
}

static inline void
fy_emit_accum_start(struct fy_emit_accum *ea, int col, enum fy_lb_mode lb_mode)
{
	fy_emit_accum_reset(ea);
	ea->col = col;
	ea->lb_mode = lb_mode;
}

static inline void
fy_emit_accum_finish(struct fy_emit_accum *ea)
{
	fy_emit_accum_reset(ea);
}

static inline int
fy_emit_accum_grow(struct fy_emit_accum *ea, size_t need)
{
	size_t atleast, asz;
	char *new_accum;

	atleast = ea->alloc + need;
	asz = ea->alloc;
	/* minimum buffer is 32 */
	if (asz < 32)
		asz = 32;
	do {
		asz *= 2;
	} while (asz < atleast);
	assert(asz > ea->inplacesz);
	new_accum = realloc(ea->accum == ea->inplace ? NULL : ea->accum, asz);
	if (!new_accum)	/* out of memory */
		return -1;
	if (ea->accum && ea->accum == ea->inplace)
		memcpy(new_accum, ea->accum, ea->next);
	ea->alloc = asz;
	ea->accum = new_accum;

	return 0;
}

static inline int
fy_emit_accum_utf8_put_raw(struct fy_emit_accum *ea, int c)
{
	size_t w, avail;
	int ret;

	/* grow if needed */
	w = fy_utf8_width(c);
	if (w > (avail = (ea->alloc - ea->next))) {
		ret = fy_emit_accum_grow(ea, w - avail);
		if (ret != 0)
			return ret;
	}
	(void)fy_utf8_put_unchecked(ea->accum + ea->next, c);
	ea->next += w;

	return 0;
}

static inline int
fy_emit_accum_put_raw(struct fy_emit_accum *ea, int c)
{
	int ret;

	/* only lower ascii please */
	if (c >= 0x80)
		return -1;

	/* grow if needed */
	if (ea->next >= ea->alloc) {
		ret = fy_emit_accum_grow(ea, 1);
		if (ret != 0)
			return ret;
	}
	*(ea->accum + ea->next) = (char)c;
	ea->next++;

	return 0;
}

static inline int
fy_emit_accum_utf8_put(struct fy_emit_accum *ea, int c)
{
	int ret;

	if (!fy_utf8_is_valid(c))
		return -1;

	if (fy_is_lb_m(c, ea->lb_mode)) {
		ret = fy_emit_accum_put_raw(ea, '\n');
		if (ret)
			return ret;
		ea->col = 0;
		ea->row++;
	} else if (fy_is_tab(c)) {
		ret = fy_emit_accum_put_raw(ea, '\t');
		if (ret)
			return ret;
		ea->col += (ea->ts - (ea->col % ea->ts));
	} else {
		if (c < 0x80) {
			ret = fy_emit_accum_put_raw(ea, c);
			if (ret)
				return ret;
		} else {
			ret = fy_emit_accum_utf8_put_raw(ea, c);
		}
		ea->col++;
	}

	return 0;
}

static inline int
fy_emit_accum_utf8_write_raw(struct fy_emit_accum *ea, const void *data, size_t len)
{
	size_t avail;
	int ret;

	/* grow if needed */
	if (len > (avail = (ea->alloc - ea->next))) {
		ret = fy_emit_accum_grow(ea, len - avail);
		if (ret != 0)
			return ret;
	}
	memcpy(ea->accum + ea->next, data, len);
	ea->next += len;

	return 0;
}

static inline int
fy_emit_accum_utf8_write(struct fy_emit_accum *ea, const void *data, size_t len)
{
	const char *s, *e;
	int c, w, ret;

	for (s = data, e = s + len; (c = fy_utf8_get(s, (e - s), &w)) >= 0; s += w) {
		ret = fy_emit_accum_utf8_put(ea, c);
		if (ret)
			break;
	}
	return c == FYUG_EOF ? 0 : -1;
}

static inline int
fy_emit_accum_utf8_printf_raw(struct fy_emit_accum *ea, const char *fmt, ...)
		FY_ATTRIBUTE(format(printf, 2, 3));

static inline int
fy_emit_accum_utf8_printf_raw(struct fy_emit_accum *ea, const char *fmt, ...)
{
	va_list ap;
	size_t avail, len;
	int ret;

	/* get the size of the string */
	va_start(ap, fmt);
	len = vsnprintf(NULL, 0, fmt, ap);
	va_end(ap);

	/* grow if needed */
	if ((len + 1) > (avail = (ea->alloc - ea->next))) {
		ret = fy_emit_accum_grow(ea, (len + 1) - avail);
		if (ret != 0)
			return ret;
	}

	va_start(ap, fmt);
	(void)vsnprintf(ea->accum + ea->next, len + 1, fmt, ap);
	va_end(ap);

	ea->next += len;

	return 0;
}

static inline const char *
fy_emit_accum_get(struct fy_emit_accum *ea, size_t *lenp)
{
	*lenp = ea->next;
	if (!ea->next) {
		return "";
	}
	return ea->accum;
}

static inline int
fy_emit_accum_make_0_terminated(struct fy_emit_accum *ea)
{
	int ret;

	/* the empty case is special cased */
	if (!ea->next)
		return 0;

	/* grow if needed for the '\0' */
	if (ea->next >= ea->alloc) {
		ret = fy_emit_accum_grow(ea, 1);
		if (ret != 0)
			return ret;
	}
	assert(ea->next < ea->alloc);
	*(ea->accum + ea->next) = '\0';
	return 0;
}

static inline const char *
fy_emit_accum_get0(struct fy_emit_accum *ea)
{
	int ret;

	ret = fy_emit_accum_make_0_terminated(ea);
	if (ret)
		return NULL;
	return ea->accum;
}

static inline char *
fy_emit_accum_steal(struct fy_emit_accum *ea, size_t *lenp)
{
	int ret;
	char *buf;

	/* empty, return a malloc'ed buffer to "" */
	if (!ea->next) {
		buf = strdup("");
		if (!buf) {
			*lenp = 0;
			return NULL;
		}
		*lenp = ea->next;
	} else if (ea->inplace && ea->accum == ea->inplace) {
		buf = malloc(ea->next + 1);
		if (!buf) {
			*lenp = 0;
			return NULL;
		}
		memcpy(buf, ea->accum, ea->next);
		buf[ea->next] = '\0';
		*lenp = ea->next;
	} else {
		ret = fy_emit_accum_make_0_terminated(ea);
		if (ret) {
			*lenp = 0;
			return NULL;
		}
		assert(ea->accum && ea->accum != ea->inplace);
		buf = ea->accum;
		*lenp = ea->next;
		/* reset to inplace */
		ea->accum = ea->inplace;
		ea->alloc = ea->inplacesz;
	}

	fy_emit_accum_cleanup(ea);
	return buf;
}

static inline char *
fy_emit_accum_steal0(struct fy_emit_accum *ea)
{
	size_t len;

	return fy_emit_accum_steal(ea, &len);
}

static inline bool
fy_emit_accum_empty(struct fy_emit_accum *ea)
{
	return ea->next == 0;
}

static inline int
fy_emit_accum_size(struct fy_emit_accum *ea)
{
	return ea->next;
}

static inline int
fy_emit_accum_column(struct fy_emit_accum *ea)
{
	return ea->col;
}

static inline int
fy_emit_accum_row(struct fy_emit_accum *ea)
{
	return ea->row;
}

struct fy_emit_accum_state {
	int col;
	int row;
	size_t next;
};

static inline void
fy_emit_accum_get_state(struct fy_emit_accum *ea, struct fy_emit_accum_state *s)
{
	s->col = ea->col;
	s->row = ea->row;
	s->next = ea->next;
}

static inline void
fy_emit_accum_rewind_state(struct fy_emit_accum *ea, const struct fy_emit_accum_state *s)
{
	/* we can only go back */
	assert(s->next <= ea->next);
	ea->col = s->col;
	ea->row = s->row;
	ea->next = s->next;
}

#endif
