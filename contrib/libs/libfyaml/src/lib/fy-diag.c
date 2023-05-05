/*
 * fy-diag.c - diagnostics
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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#endif
#include <assert.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <ctype.h>

#include <libfyaml.h>

#include "fy-parse.h"

static const char *error_type_txt[] = {
	[FYET_DEBUG]   = "debug",
	[FYET_INFO]    = "info",
	[FYET_NOTICE]  = "notice",
	[FYET_WARNING] = "warning",
	[FYET_ERROR]   = "error",
};

int fy_diag_diag(struct fy_diag *diag, enum fy_error_type level, const char* fmt, ...)
{
    int ret;

    va_list args;
       struct fy_diag_ctx ctx = {
               .level = level,
               .module = FYEM_UNKNOWN,
               .source_func = __func__,
               .source_file = __FILE__,
               .source_line = __LINE__,
               .file = NULL,
               .line = 0,
               .column = 0,
       };

    va_start(args, fmt);
    ret = fy_diagf(diag, &ctx, fmt, args);
    va_end(args);

    return ret;
}

const char *fy_error_type_to_string(enum fy_error_type type)
{

	if ((unsigned int)type >= FYET_MAX)
		return "";
	return error_type_txt[type];
}

enum fy_error_type fy_string_to_error_type(const char *str)
{
	unsigned int i;
	int level;

	if (!str)
		return FYET_MAX;

	if (isdigit(*str)) {
		level = atoi(str);
		if (level >= 0 && level < FYET_MAX)
			return (enum fy_error_type)level;
	}

	for (i = 0; i < FYET_MAX; i++) {
		if (!strcmp(str, error_type_txt[i]))
			return (enum fy_error_type)i;
	}

	return FYET_MAX;
}

static const char *error_module_txt[] = {
	[FYEM_UNKNOWN]	= "unknown",
	[FYEM_ATOM]	= "atom",
	[FYEM_SCAN]	= "scan",
	[FYEM_PARSE]	= "parse",
	[FYEM_DOC]	= "doc",
	[FYEM_BUILD]	= "build",
	[FYEM_INTERNAL]	= "internal",
	[FYEM_SYSTEM]	= "system",
};

const char *fy_error_module_to_string(enum fy_error_module module)
{

	if ((unsigned int)module >= FYEM_MAX)
		return "";
	return error_module_txt[module];
}

enum fy_error_module fy_string_to_error_module(const char *str)
{
	unsigned int i;

	if (!str)
		return FYEM_MAX;

	for (i = 0; i < FYEM_MAX; i++) {
		if (!strcmp(str, error_module_txt[i]))
			return (enum fy_error_module)i;
	}

	return FYEM_MAX;
}

static const char *fy_error_level_str(enum fy_error_type level)
{
	static const char *txt[] = {
		[FYET_DEBUG]   = "DBG",
		[FYET_INFO]    = "INF",
		[FYET_NOTICE]  = "NOT",
		[FYET_WARNING] = "WRN",
		[FYET_ERROR]   = "ERR",
	};

	if ((unsigned int)level >= FYET_MAX)
		return "*unknown*";
	return txt[level];
}

static const char *fy_error_module_str(enum fy_error_module module)
{
	static const char *txt[] = {
		[FYEM_UNKNOWN]	= "UNKWN",
		[FYEM_ATOM]	= "ATOM ",
		[FYEM_SCAN]	= "SCAN ",
		[FYEM_PARSE]	= "PARSE",
		[FYEM_DOC]	= "DOC  ",
		[FYEM_BUILD]	= "BUILD",
		[FYEM_INTERNAL]	= "INTRL",
		[FYEM_SYSTEM]	= "SYSTM",
	};

	if ((unsigned int)module >= FYEM_MAX)
		return "*unknown*";
	return txt[module];
}

/* really concervative options */
static const struct fy_diag_term_info default_diag_term_info_template = {
	.rows		= 25,
	.columns	= 80
};

static const struct fy_diag_cfg default_diag_cfg_template = {
	.fp		= NULL,	/* must be overriden */
	.level		= FYET_INFO,
	.module_mask	= (1U << FYEM_MAX) - 1,	/* all modules */
	.show_source	= false,
	.show_position	= false,
	.show_type	= true,
	.show_module	= false,
	.colorize	= false, /* can be overriden */
	.source_width	= 50,
	.position_width	= 10,
	.type_width	= 5,
	.module_width	= 6,
};

void fy_diag_cfg_default(struct fy_diag_cfg *cfg)
{
	if (!cfg)
		return;

	*cfg = default_diag_cfg_template;
	cfg->fp = stderr;
	cfg->colorize = isatty(fileno(stderr)) == 1;
}

void fy_diag_cfg_from_parser_flags(struct fy_diag_cfg *cfg, enum fy_parse_cfg_flags pflags)
{
	/* nothing */
}

static bool fy_diag_isatty(struct fy_diag *diag)
{
	return diag && diag->cfg.fp && isatty(fileno(diag->cfg.fp));
}

static void fy_diag_update_term_info(struct fy_diag *diag)
{
	int fd, rows, columns, ret;

	/* start by setting things to the default */
	diag->term_info = default_diag_term_info_template;

	fd = diag->cfg.fp && isatty(fileno(diag->cfg.fp)) ?
		fileno(diag->cfg.fp) : -1;

	if (fd == -1)
		goto out;

	rows = columns = 0;
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
	ret = fy_term_query_size(fd, &rows, &columns);
	if (ret != 0)
		goto out;

	if (rows > 0 && columns > 0) {
		diag->term_info.rows = rows;
		diag->term_info.columns = columns;
	}
#endif
out:
	diag->terminal_probed = true;
}

void fy_diag_errorp_free(struct fy_diag_errorp *errp)
{
	if (errp->space)
		free(errp->space);
	fy_token_unref(errp->e.fyt);
	free(errp);
}

struct fy_diag *fy_diag_create(const struct fy_diag_cfg *cfg)
{
	struct fy_diag *diag;

	diag = malloc(sizeof(*diag));
	if (!diag)
		return NULL;
	memset(diag, 0, sizeof(*diag));

	if (!cfg)
		fy_diag_cfg_default(&diag->cfg);
	else
		diag->cfg = *cfg;
	diag->on_error = false;
	diag->refs = 1;

	diag->terminal_probed = false;
	if (!fy_diag_isatty(diag))
		fy_diag_update_term_info(diag);

	fy_diag_errorp_list_init(&diag->errors);

	return diag;
}

void fy_diag_destroy(struct fy_diag *diag)
{
	struct fy_diag_errorp *errp;

	if (!diag)
		return;

	diag->destroyed = true;

	/* free everything */
	while ((errp = fy_diag_errorp_list_pop(&diag->errors)) != NULL)
		fy_diag_errorp_free(errp);

	return fy_diag_unref(diag);
}

bool fy_diag_got_error(struct fy_diag *diag)
{
	return diag && diag->on_error;
}

void fy_diag_reset_error(struct fy_diag *diag)
{
	struct fy_diag_errorp *errp;

	if (!diag)
		return;

	diag->on_error = false;

	while ((errp = fy_diag_errorp_list_pop(&diag->errors)) != NULL)
		fy_diag_errorp_free(errp);
}

void fy_diag_set_collect_errors(struct fy_diag *diag, bool collect_errors)
{
	struct fy_diag_errorp *errp;

	if (!diag || diag->destroyed)
		return;

	diag->collect_errors = collect_errors;

	/* clear collected errors on disable */
	if (!diag->collect_errors) {
		while ((errp = fy_diag_errorp_list_pop(&diag->errors)) != NULL)
			fy_diag_errorp_free(errp);
	}
}

struct fy_diag_error *fy_diag_errors_iterate(struct fy_diag *diag, void **prevp)
{
	struct fy_diag_errorp *errp;

	if (!diag || !prevp)
		return NULL;

	if (!*prevp)
		errp = fy_diag_errorp_list_head(&diag->errors);
	else {
		errp = *prevp;
		errp = fy_diag_errorp_next(&diag->errors, errp);
	}

	if (!errp)
		return NULL;
	*prevp = errp;
	return &errp->e;
}

void fy_diag_free(struct fy_diag *diag)
{
	if (!diag)
		return;
	free(diag);
}

const struct fy_diag_cfg *fy_diag_get_cfg(struct fy_diag *diag)
{
	if (!diag)
		return NULL;
	return &diag->cfg;
}

void fy_diag_set_cfg(struct fy_diag *diag, const struct fy_diag_cfg *cfg)
{
	if (!diag)
		return;

	if (!cfg)
		fy_diag_cfg_default(&diag->cfg);
	else
		diag->cfg = *cfg;

	fy_diag_update_term_info(diag);
}

void fy_diag_set_level(struct fy_diag *diag, enum fy_error_type level)
{
	if (!diag || (unsigned int)level >= FYET_MAX)
		return;
	diag->cfg.level = level;
}

void fy_diag_set_colorize(struct fy_diag *diag, bool colorize)
{
	if (!diag)
		return;
	diag->cfg.colorize = colorize;
}

struct fy_diag *fy_diag_ref(struct fy_diag *diag)
{
	if (!diag)
		return NULL;

	assert(diag->refs + 1 > 0);
	diag->refs++;

	return diag;
}

void fy_diag_unref(struct fy_diag *diag)
{
	if (!diag)
		return;

	assert(diag->refs > 0);

	if (diag->refs == 1)
		fy_diag_free(diag);
	else
		diag->refs--;
}

ssize_t fy_diag_write(struct fy_diag *diag, const void *buf, size_t count)
{
	size_t ret;

	if (!diag || !buf)
		return -1;

	/* no more output */
	if (diag->destroyed)
		return 0;

	ret = 0;
	if (diag->cfg.fp) {
		ret = fwrite(buf, 1, count, diag->cfg.fp);
	} else if (diag->cfg.output_fn) {
		diag->cfg.output_fn(diag, diag->cfg.user, buf, count);
		ret = count;
	}

	return ret == count ? (ssize_t)count : -1;
}

int fy_diag_vprintf(struct fy_diag *diag, const char *fmt, va_list ap)
{
	char *buf;
	int rc;

	if (!diag || !fmt)
		return -1;

	/* no more output */
	if (diag->destroyed)
		return 0;

	if (diag->cfg.fp)
		return vfprintf(diag->cfg.fp, fmt, ap);

	if (diag->cfg.output_fn) {
		rc = vasprintf(&buf, fmt, ap);
		if (rc < 0)
			return rc;
		diag->cfg.output_fn(diag, diag->cfg.user, buf, (size_t)rc);
		free(buf);
		return rc;
	}

	return -1;
}

int fy_diag_printf(struct fy_diag *diag, const char *fmt, ...)
{
	va_list ap;
	int rc;

	va_start(ap, fmt);
	rc = fy_diag_vprintf(diag, fmt, ap);
	va_end(ap);

	return rc;
}

int fy_vdiag(struct fy_diag *diag, const struct fy_diag_ctx *fydc,
	     const char *fmt, va_list ap)
{
	char *msg = NULL;
	char *source = NULL, *position = NULL, *typestr = NULL, *modulestr = NULL;
	const char *file_stripped = NULL;
	const char *color_start = NULL, *color_end = NULL;
	enum fy_error_type level;
	int rc;

	if (!diag || !fydc || !fmt)
		return -1;

	level = fydc->level;

	/* turn errors into debugs while not reset */
	if (level >= FYET_ERROR && diag->on_error)
		level = FYET_DEBUG;

	if (level < diag->cfg.level) {
		rc = 0;
		goto out;
	}

	/* check module enable mask */
	if (!(diag->cfg.module_mask & FY_BIT(fydc->module))) {
		rc = 0;
		goto out;
	}

	alloca_vsprintf(&msg, fmt, ap);

	/* source part */
	if (diag->cfg.show_source) {
		if (fydc->source_file) {
			file_stripped = strrchr(fydc->source_file, '/');
			if (!file_stripped)
				file_stripped = fydc->source_file;
			else
				file_stripped++;
		} else
			file_stripped = "";
		alloca_sprintf(&source, "%s:%d @%s()%s",
				file_stripped, fydc->source_line, fydc->source_func, " ");
	}

	/* position part */
	if (diag->cfg.show_position && fydc->line >= 0 && fydc->column >= 0)
		alloca_sprintf(&position, "<%3d:%2d>%s", fydc->line, fydc->column, ": ");

	/* type part */
	if (diag->cfg.show_type)
		alloca_sprintf(&typestr, "[%s]%s", fy_error_level_str(level), ": ");

	/* module part */
	if (diag->cfg.show_module)
		alloca_sprintf(&modulestr, "<%s>%s", fy_error_module_str(fydc->module), ": ");

	if (diag->cfg.colorize) {
		switch (level) {
		case FYET_DEBUG:
			color_start = "\x1b[37m";	/* normal white */
			break;
		case FYET_INFO:
			color_start = "\x1b[37;1m";	/* bright white */
			break;
		case FYET_NOTICE:
			color_start = "\x1b[34;1m";	/* bright blue */
			break;
		case FYET_WARNING:
			color_start = "\x1b[33;1m";	/* bright yellow */
			break;
		case FYET_ERROR:
			color_start = "\x1b[31;1m";	/* bright red */
			break;
		default:	/* handles FYET_MAX */
			break;
		}
		if (color_start)
			color_end = "\x1b[0m";
	}

	rc = fy_diag_printf(diag, "%s" "%*s" "%*s" "%*s" "%*s" "%s" "%s\n",
			color_start ? color_start : "",
			source    ? diag->cfg.source_width : 0, source ? source : "",
			position  ? diag->cfg.position_width : 0, position ? position : "",
			typestr   ? diag->cfg.type_width : 0, typestr ? typestr : "",
			modulestr ? diag->cfg.module_width : 0, modulestr ? modulestr : "",
			msg,
			color_end ? color_end : "");

	if (rc > 0)
		rc++;

out:
	/* if it's the first error we're generating set the
	 * on_error flag until the top caller clears it
	 */
	if (!diag->on_error && fydc->level >= FYET_ERROR)
		diag->on_error = true;

	return rc;
}

int fy_diagf(struct fy_diag *diag, const struct fy_diag_ctx *fydc,
	     const char *fmt, ...)
{
	va_list ap;
	int rc;

	va_start(ap, fmt);
	rc = fy_vdiag(diag, fydc, fmt, ap);
	va_end(ap);

	return rc;
}

static void fy_diag_get_error_colors(struct fy_diag *diag, enum fy_error_type type,
				     const char **start, const char **end, const char **white)
{
	if (!diag->cfg.colorize) {
		*start = *end = *white = "";
		return;
	}

	switch (type) {
	case FYET_DEBUG:
		*start = "\x1b[37m";	/* normal white */
		break;
	case FYET_INFO:
		*start = "\x1b[37;1m";	/* bright white */
		break;
	case FYET_NOTICE:
		*start = "\x1b[34;1m";	/* bright blue */
		break;
	case FYET_WARNING:
		*start = "\x1b[33;1m";	/* bright yellow */
		break;
	case FYET_ERROR:
		*start = "\x1b[31;1m";	/* bright red */
		break;
	default:
		*start = "\x1b[0m";	/* catch-all reset */
		break;
	}
	*end = "\x1b[0m";
	*white = "\x1b[37;1m";
}

void fy_diag_error_atom_display(struct fy_diag *diag, enum fy_error_type type, struct fy_atom *atom)
{
	const struct fy_raw_line *l, *ln;
	struct fy_raw_line l_tmp;
	struct fy_atom_raw_line_iter iter;
	int content_start_col, content_end_col, content_width;
	int pass, cols, min_col, max_col, total_lines, max_line_count, max_line_col8, max_width;
	int start_col, end_col;
	const char *color_start, *color_end, *white;
	bool first_line, last_line;
	const char *display;
	int display_len, line_shift;
	char qc, first_mark;
	char *rowbuf = NULL, *rbs = NULL, *rbe = NULL;
	const char *s, *e;
	int col8, c, w;
	int tab8_len, tilde_start, tilde_width, tilde_width_m1;
	size_t rowbufsz;

	(void)end_col;

	if (!diag || !atom)
		return;

	fy_diag_get_error_colors(diag, type, &color_start, &color_end, &white);

	/* two passes, first one collects extents */

	start_col = -1;
	end_col = -1;
	min_col = -1;
	max_col = -1;
	max_line_count = -1;
	max_line_col8 = -1;
	total_lines = 0;
	line_shift = -1;
	for (pass = 0; pass < 2; pass++) {

		/* on the start of the second pass */
		if (pass > 0) {

			cols = 0;

			/* if it's probed, use what's there */
			if (diag->terminal_probed && diag->term_info.columns > 0)
				cols = diag->term_info.columns;

			/* heuristic, avoid probing terminal size if maximum column is less than 80
			 * columns. This is faster and avoid problems with terminals...
			 */
			if (!cols && max_line_col8 < 80)
				cols = 80;

			/* no choice but to probe */
			if (!cols) {
				/* only need the terminal width when outputting an error */
				if (!diag->terminal_probed && fy_diag_isatty(diag))
					fy_diag_update_term_info(diag);

				cols = diag->term_info.columns;
			}

			/* worse case utf8 + 2 color sequences + zero terminated */
			rowbufsz = cols * 4 + 2 * 16 + 1;
			rowbuf = FY_ALLOCA(rowbufsz);
			rbe = rowbuf + rowbufsz;

			/* if the maximum column number is less than the terminal
			 * width everything fits, and we're fine */ 
			if (max_line_col8 < cols) {
				line_shift = 0;
			} else {
				max_width = max_col - min_col;

				/* try to center */
				line_shift = min_col + (max_width - cols) / 2;

				/* the start of the content must always be included */
				if (start_col < line_shift)
					line_shift = start_col;
			}
		}

		fy_atom_raw_line_iter_start(atom, &iter);
		l = fy_atom_raw_line_iter_next(&iter);
		for (; l != NULL; l = ln) {

			/* save it */
			l_tmp = *l;
			l = &l_tmp;

			/* get the next too */
			ln = fy_atom_raw_line_iter_next(&iter);

			first_line = l->lineno <= 1;
			last_line = ln == NULL;

			content_start_col = l->content_start_col8;
			content_end_col = l->content_end_col8;

			/* adjust for single and double quoted to include the quote marks (usually works) */
			if (fy_atom_style_is_quoted(atom->style)) {
				qc = atom->style == FYAS_SINGLE_QUOTED ? '\'' : '"';
				if (first_line && l->content_start > l->line_start &&
						l->content_start[-1] == qc)
					content_start_col--;
				if (last_line && (l->content_start + l->content_len) < (l->line_start + l->line_len) &&
						l->content_start[l->content_len] == qc)
					content_end_col++;
			}

			content_width = content_end_col - content_start_col;

			if (pass == 0) {

				total_lines++;

				if (min_col < 0 || content_start_col < min_col)
					min_col = content_start_col;
				if (max_col < 0 || content_end_col > max_col)
					max_col = content_end_col;
				if (max_line_count < 0 || (int)l->line_count > max_line_count)
					max_line_count = (int)l->line_count;
				if (first_line)
					start_col = content_start_col;
				if (last_line)
					end_col = content_end_col;

				/* optimize by using the content end as a starting point */
				s = l->content_start + l->content_len;
				e = l->line_start + l->line_count;
				col8 = l->content_end_col8;
				while ((c = fy_utf8_get(s, (e - s), &w)) >= 0) {
					s += w;
					if (fy_is_tab(c))
						col8 += 8 - (col8 % 8);
					else
						col8++;
				}
				/* update the max column number of the lines */
				if (max_line_col8 < 0 || col8 > max_line_col8)
					max_line_col8 = col8;

				continue;
			}

			/* output pass */

			/* the defaults if everything fits */
			first_mark = first_line ? '^' : '~';

			tab8_len = 0;

			/* find the starting point */
			s = l->line_start;
			e = s + l->line_len;
			col8 = 0;
			while (col8 < line_shift && (c = fy_utf8_get(s, (e - s), &w)) >= 0) {
				if (fy_is_tab(c))
					col8 += 8 - (col8 % 8);
				else
					col8++;
				s += w;
			}
			if (col8 > line_shift)
				tab8_len = col8 - line_shift;	/* the remaining of the tab */
			else
				tab8_len = 0;

			/* start filling the row buffer */
			assert(rowbuf);
			rbs = rowbuf;
			rbe = rowbuf + rowbufsz;

			/* remaining tabs */
			while (tab8_len > 0) {
				*rbs++ = ' ';
				tab8_len--;
			}

			/* go forward until end of line or cols */
			while (col8 < (line_shift + cols) && (c = fy_utf8_get(s, (e - s), &w)) >= 0 && rbs < rbe) {
				if (fy_is_tab(c)) {
					s++;
					tab8_len = 8 - (col8 % 8);
					col8 += tab8_len;
					while (tab8_len > 0 && rbs < rbe) {
						*rbs++ = ' ';
						tab8_len--;
					}
				} else {
					while (w > 0 && rbs < rbe) {
						*rbs++ = *s++;
						w--;
					}
					col8++;
				}
			}
			display = rowbuf;
			display_len = rbs - rowbuf;

			tilde_start = content_start_col - line_shift;
			tilde_width = content_width;
			if (tilde_start + tilde_width > cols)
				tilde_width = cols - tilde_start;
			if ((size_t)tilde_width >= rowbufsz)
				tilde_width = rowbufsz - 1;	/* guard */
			tilde_width_m1 = tilde_width > 0 ? (tilde_width - 1) : 0;

			/* output the line */
			fy_diag_write(diag, display, display_len);

			/* set the tildes */
			assert((int)rowbufsz > tilde_width_m1 + 1);
			memset(rowbuf, '~', tilde_width_m1);
			rowbuf[tilde_width_m1] = '\0';

			fy_diag_printf(diag, "\n%*s%s%c%.*s%s\n",
				       tilde_start, "",
				       color_start, first_mark, tilde_width_m1, rowbuf, color_end);

		}
		fy_atom_raw_line_iter_finish(&iter);
	}
}

void fy_diag_error_token_display(struct fy_diag *diag, enum fy_error_type type, struct fy_token *fyt)
{
	if (!diag || !fyt)
		return;

	fy_diag_error_atom_display(diag, type, fy_token_atom(fyt));
}

void fy_diag_vreport(struct fy_diag *diag,
		     const struct fy_diag_report_ctx *fydrc,
		     const char *fmt, va_list ap)
{
	const char *name, *color_start = NULL, *color_end = NULL, *white = NULL;
	char *msg_str = NULL, *name_str = NULL;
	const struct fy_mark *start_mark;
	int line, column;
	struct fy_diag_errorp *errp;
	struct fy_diag_error *err;
	size_t spacesz, msgsz, filesz;
	char *s;

	if (!diag || !fydrc || !fmt || !fydrc->fyt)
		return;

	start_mark = fy_token_start_mark(fydrc->fyt);

	if (fydrc->has_override) {
		name = fydrc->override_file;
		line = fydrc->override_line;
		column = fydrc->override_column;
	} else {
		name = fy_input_get_filename(fy_token_get_input(fydrc->fyt));
		line = start_mark->line + 1;
		column = start_mark->column + 1;
	}

	/* it will strip trailing newlines */
	alloca_vsprintf(&msg_str, fmt, ap);

	/* get the colors */
	fy_diag_get_error_colors(diag, fydrc->type, &color_start, &color_end, &white);

	if (name || (line > 0 && column > 0)) {
		if (line > 0 && column > 0)
			alloca_sprintf(&name_str, "%s%s:%d:%d: ", white, name, line, column);
		else
			alloca_sprintf(&name_str, "%s%s: ", white, name);
	}

	if (!diag->collect_errors) {
		fy_diag_printf(diag, "%s" "%s%s: %s" "%s\n",
			name_str ? name_str : "",
			color_start, fy_error_type_to_string(fydrc->type), color_end,
			msg_str);

		fy_diag_error_token_display(diag, fydrc->type, fydrc->fyt);

		fy_token_unref(fydrc->fyt);

	} else if ((errp = malloc(sizeof(*errp))) != NULL) {

		msgsz = strlen(msg_str) + 1;
		filesz = strlen(name) + 1;
		spacesz = msgsz + filesz;

		errp->space = malloc(spacesz);
		if (!errp->space) {
			free(errp);
			goto out;
		}
		s = errp->space;

		err = &errp->e;
		memset(err, 0, sizeof(*err));
		err->type = fydrc->type;
		err->module = fydrc->module;
		err->fyt = fydrc->fyt;
		err->msg = s;
		memcpy(s, msg_str, msgsz);
		s += msgsz;
		err->file = s;
		memcpy(s, name, filesz);
		s += filesz;
		err->line = line;
		err->column = column;

		fy_diag_errorp_list_add_tail(&diag->errors, errp);
	}
out:
	if (!diag->on_error && fydrc->type == FYET_ERROR)
		diag->on_error = true;
}

void fy_diag_report(struct fy_diag *diag,
		    const struct fy_diag_report_ctx *fydrc,
		    const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_diag_vreport(diag, fydrc, fmt, ap);
	va_end(ap);
}

/* parser */

int fy_parser_vdiag(struct fy_parser *fyp, unsigned int flags,
		    const char *file, int line, const char *func,
		    const char *fmt, va_list ap)
{
	struct fy_diag_ctx fydc;
	int rc;

	if (!fyp || !fyp->diag || !fmt)
		return -1;

	/* perform the enable tests early to avoid the overhead */
	if (((flags & FYDF_LEVEL_MASK) >> FYDF_LEVEL_SHIFT) < fyp->diag->cfg.level)
		return 0;

	/* fill in fy_diag_ctx */
	memset(&fydc, 0, sizeof(fydc));

	fydc.level = (flags & FYDF_LEVEL_MASK) >> FYDF_LEVEL_SHIFT;
	fydc.module = (flags & FYDF_MODULE_MASK) >> FYDF_MODULE_SHIFT;
	fydc.source_file = file;
	fydc.source_line = line;
	fydc.source_func = func;
	fydc.line = fyp_line(fyp);
	fydc.column = fyp_column(fyp);

	rc = fy_vdiag(fyp->diag, &fydc, fmt, ap);

	if (fyp && !fyp->stream_error && fyp->diag->on_error)
		fyp->stream_error = true;

	return rc;
}

int fy_parser_diag(struct fy_parser *fyp, unsigned int flags,
	    const char *file, int line, const char *func,
	    const char *fmt, ...)
{
	va_list ap;
	int rc;

	va_start(ap, fmt);
	rc = fy_parser_vdiag(fyp, flags, file, line, func, fmt, ap);
	va_end(ap);

	return rc;
}

void fy_parser_diag_vreport(struct fy_parser *fyp,
		            const struct fy_diag_report_ctx *fydrc,
			    const char *fmt, va_list ap)
{
	struct fy_diag *diag;

	if (!fyp || !fyp->diag || !fydrc || !fmt)
		return;

	diag = fyp->diag;

	fy_diag_vreport(diag, fydrc, fmt, ap);

	if (fyp && !fyp->stream_error && diag->on_error)
		fyp->stream_error = true;
}

void fy_parser_diag_report(struct fy_parser *fyp,
			   const struct fy_diag_report_ctx *fydrc,
			   const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_parser_diag_vreport(fyp, fydrc, fmt, ap);
	va_end(ap);
}

/* document */

int fy_document_vdiag(struct fy_document *fyd, unsigned int flags,
		      const char *file, int line, const char *func,
		      const char *fmt, va_list ap)
{
	struct fy_diag_ctx fydc;
	int rc;

	if (!fyd || !fmt || !fyd->diag)
		return -1;

	/* perform the enable tests early to avoid the overhead */
	if (((flags & FYDF_LEVEL_MASK) >> FYDF_LEVEL_SHIFT) < fyd->diag->cfg.level)
		return 0;

	/* fill in fy_diag_ctx */
	memset(&fydc, 0, sizeof(fydc));

	fydc.level = (flags & FYDF_LEVEL_MASK) >> FYDF_LEVEL_SHIFT;
	fydc.module = (flags & FYDF_MODULE_MASK) >> FYDF_MODULE_SHIFT;
	fydc.source_file = file;
	fydc.source_line = line;
	fydc.source_func = func;
	fydc.line = -1;
	fydc.column = -1;

	rc = fy_vdiag(fyd->diag, &fydc, fmt, ap);

	return rc;
}

int fy_document_diag(struct fy_document *fyd, unsigned int flags,
		     const char *file, int line, const char *func,
		     const char *fmt, ...)
{
	va_list ap;
	int rc;

	va_start(ap, fmt);
	rc = fy_document_vdiag(fyd, flags, file, line, func, fmt, ap);
	va_end(ap);

	return rc;
}

void fy_document_diag_vreport(struct fy_document *fyd,
			      const struct fy_diag_report_ctx *fydrc,
			      const char *fmt, va_list ap)
{
	if (!fyd || !fyd->diag || !fydrc || !fmt)
		return;

	fy_diag_vreport(fyd->diag, fydrc, fmt, ap);
}

void fy_document_diag_report(struct fy_document *fyd,
			     const struct fy_diag_report_ctx *fydrc,
			     const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_document_diag_vreport(fyd, fydrc, fmt, ap);
	va_end(ap);
}

/* composer */
int fy_composer_vdiag(struct fy_composer *fyc, unsigned int flags,
		      const char *file, int line, const char *func,
		      const char *fmt, va_list ap)
{
	struct fy_diag_ctx fydc;
	int rc;

	if (!fyc || !fmt || !fyc->cfg.diag)
		return -1;

	/* perform the enable tests early to avoid the overhead */
	if (((flags & FYDF_LEVEL_MASK) >> FYDF_LEVEL_SHIFT) < fyc->cfg.diag->cfg.level)
		return 0;

	/* fill in fy_diag_ctx */
	memset(&fydc, 0, sizeof(fydc));

	fydc.level = (flags & FYDF_LEVEL_MASK) >> FYDF_LEVEL_SHIFT;
	fydc.module = (flags & FYDF_MODULE_MASK) >> FYDF_MODULE_SHIFT;
	fydc.source_file = file;
	fydc.source_line = line;
	fydc.source_func = func;
	fydc.line = -1;
	fydc.column = -1;

	rc = fy_vdiag(fyc->cfg.diag, &fydc, fmt, ap);

	return rc;
}

int fy_composer_diag(struct fy_composer *fyc, unsigned int flags,
		     const char *file, int line, const char *func,
		     const char *fmt, ...)
{
	va_list ap;
	int rc;

	va_start(ap, fmt);
	rc = fy_composer_vdiag(fyc, flags, file, line, func, fmt, ap);
	va_end(ap);

	return rc;
}

void fy_composer_diag_vreport(struct fy_composer *fyc,
			      const struct fy_diag_report_ctx *fydrc,
			      const char *fmt, va_list ap)
{
	if (!fyc || !fyc->cfg.diag || !fydrc || !fmt)
		return;

	fy_diag_vreport(fyc->cfg.diag, fydrc, fmt, ap);
}

void fy_composer_diag_report(struct fy_composer *fyc,
			     const struct fy_diag_report_ctx *fydrc,
			     const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_composer_diag_vreport(fyc, fydrc, fmt, ap);
	va_end(ap);
}

/* document_builder */
int fy_document_builder_vdiag(struct fy_document_builder *fydb, unsigned int flags,
		      const char *file, int line, const char *func,
		      const char *fmt, va_list ap)
{
	struct fy_diag_ctx fydc;
	int rc;

	if (!fydb || !fmt || !fydb->cfg.diag)
		return -1;

	/* perform the enable tests early to avoid the overhead */
	if (((flags & FYDF_LEVEL_MASK) >> FYDF_LEVEL_SHIFT) < fydb->cfg.diag->cfg.level)
		return 0;

	/* fill in fy_diag_ctx */
	memset(&fydc, 0, sizeof(fydc));

	fydc.level = (flags & FYDF_LEVEL_MASK) >> FYDF_LEVEL_SHIFT;
	fydc.module = (flags & FYDF_MODULE_MASK) >> FYDF_MODULE_SHIFT;
	fydc.source_file = file;
	fydc.source_line = line;
	fydc.source_func = func;
	fydc.line = -1;
	fydc.column = -1;

	rc = fy_vdiag(fydb->cfg.diag, &fydc, fmt, ap);

	return rc;
}

int fy_document_builder_diag(struct fy_document_builder *fydb, unsigned int flags,
		     const char *file, int line, const char *func,
		     const char *fmt, ...)
{
	va_list ap;
	int rc;

	va_start(ap, fmt);
	rc = fy_document_builder_vdiag(fydb, flags, file, line, func, fmt, ap);
	va_end(ap);

	return rc;
}

void fy_document_builder_diag_vreport(struct fy_document_builder *fydb,
			      const struct fy_diag_report_ctx *fydrc,
			      const char *fmt, va_list ap)
{
	if (!fydb || !fydb->cfg.diag || !fydrc || !fmt)
		return;

	fy_diag_vreport(fydb->cfg.diag, fydrc, fmt, ap);
}

void fy_document_builder_diag_report(struct fy_document_builder *fydb,
			     const struct fy_diag_report_ctx *fydrc,
			     const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_document_builder_diag_vreport(fydb, fydrc, fmt, ap);
	va_end(ap);
}

/* reader */

int fy_reader_vdiag(struct fy_reader *fyr, unsigned int flags,
		    const char *file, int line, const char *func,
		    const char *fmt, va_list ap)
{
	struct fy_diag_ctx fydc;
	int fydc_level, fyd_level;

	if (!fyr || !fyr->diag || !fmt)
		return -1;

	/* perform the enable tests early to avoid the overhead */
	fydc_level = (flags & FYDF_LEVEL_MASK) >> FYDF_LEVEL_SHIFT;
	fyd_level = fyr->diag->cfg.level;

	if (fydc_level < fyd_level)
		return 0;

	/* fill in fy_diag_ctx */
	memset(&fydc, 0, sizeof(fydc));

	fydc.level = fydc_level;
	fydc.module = FYEM_SCAN;	/* reader is always scanner */
	fydc.source_file = file;
	fydc.source_line = line;
	fydc.source_func = func;
	fydc.line = fyr->line;
	fydc.column = fyr->column;

	return fy_vdiag(fyr->diag, &fydc, fmt, ap);
}

int fy_reader_diag(struct fy_reader *fyr, unsigned int flags,
		   const char *file, int line, const char *func,
		   const char *fmt, ...)
{
	va_list ap;
	int rc;

	va_start(ap, fmt);
	rc = fy_reader_vdiag(fyr, flags, file, line, func, fmt, ap);
	va_end(ap);

	return rc;
}

void fy_reader_diag_vreport(struct fy_reader *fyr,
		            const struct fy_diag_report_ctx *fydrc,
			    const char *fmt, va_list ap)
{
	if (!fyr || !fyr->diag || !fydrc || !fmt)
		return;

	fy_diag_vreport(fyr->diag, fydrc, fmt, ap);
}

void fy_reader_diag_report(struct fy_reader *fyr,
			   const struct fy_diag_report_ctx *fydrc,
			   const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_reader_diag_vreport(fyr, fydrc, fmt, ap);
	va_end(ap);
}

void fy_diag_node_vreport(struct fy_diag *diag, struct fy_node *fyn,
			  enum fy_error_type type, const char *fmt, va_list ap)
{
	struct fy_diag_report_ctx drc;
	bool save_on_error;

	if (!fyn || !diag)
		return;

	save_on_error = diag->on_error;
	diag->on_error = false;

	memset(&drc, 0, sizeof(drc));
	drc.type = type;
	drc.module = FYEM_UNKNOWN;
	drc.fyt = fy_node_token(fyn);
	fy_diag_vreport(diag, &drc, fmt, ap);

	diag->on_error = save_on_error;
}

void fy_diag_node_report(struct fy_diag *diag, struct fy_node *fyn,
			 enum fy_error_type type, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_diag_node_vreport(diag, fyn, type, fmt, ap);
	va_end(ap);
}

void fy_diag_node_override_vreport(struct fy_diag *diag, struct fy_node *fyn,
				   enum fy_error_type type, const char *file,
				   int line, int column,
				   const char *fmt, va_list ap)
{
	struct fy_diag_report_ctx drc;
	bool save_on_error;

	if (!fyn || !diag)
		return;

	save_on_error = diag->on_error;
	diag->on_error = false;

	memset(&drc, 0, sizeof(drc));
	drc.type = type;
	drc.module = FYEM_UNKNOWN;
	drc.fyt = fy_node_token(fyn);
	drc.has_override = true;
	drc.override_file = file;
	drc.override_line = line;
	drc.override_column = column;
	fy_diag_vreport(diag, &drc, fmt, ap);

	diag->on_error = save_on_error;
}

void fy_diag_node_override_report(struct fy_diag *diag, struct fy_node *fyn,
				  enum fy_error_type type, const char *file,
				  int line, int column, const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_diag_node_override_vreport(diag, fyn, type, file, line, column, fmt, ap);
	va_end(ap);
}

void fy_node_vreport(struct fy_node *fyn, enum fy_error_type type,
		     const char *fmt, va_list ap)
{
	if (!fyn || !fyn->fyd)
		return;

	fy_diag_node_vreport(fyn->fyd->diag, fyn, type, fmt, ap);
}

void fy_node_report(struct fy_node *fyn, enum fy_error_type type,
		    const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_node_vreport(fyn, type, fmt, ap);
	va_end(ap);
}

void fy_node_override_vreport(struct fy_node *fyn, enum fy_error_type type,
			      const char *file, int line, int column,
			      const char *fmt, va_list ap)
{
	if (!fyn || !fyn->fyd)
		return;

	fy_diag_node_override_vreport(fyn->fyd->diag, fyn, type,
				      file, line, column, fmt, ap);
}

void fy_node_override_report(struct fy_node *fyn, enum fy_error_type type,
			     const char *file, int line, int column,
			     const char *fmt, ...)
{
	va_list ap;

	va_start(ap, fmt);
	fy_node_override_vreport(fyn, type, file, line, column, fmt, ap);
	va_end(ap);
}
