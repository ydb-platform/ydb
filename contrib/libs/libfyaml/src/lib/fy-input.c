/*
 * fy-input.c - YAML input methods
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
#include <stdarg.h>
#include <fcntl.h>
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <unistd.h>
#include <sys/mman.h>
#endif
#include <sys/types.h>
#include <sys/stat.h>
#if defined (__unix__) || (defined (__APPLE__) && defined (__MACH__))
#include <sys/ioctl.h>
#endif
#include <errno.h>

#include <libfyaml.h>

#include "fy-parse.h"
#include "fy-ctype.h"

#include "fy-input.h"

/* amount of multiplication of page size for CHOP size
 * for a 4K page this is 64K blocks
 */
#ifndef FYI_CHOP_MULT
#define FYI_CHOP_MULT	16
#endif

struct fy_input *fy_input_alloc(void)
{
	struct fy_input *fyi;

	fyi = malloc(sizeof(*fyi));
	if (!fyi)
		return NULL;
	memset(fyi, 0, sizeof(*fyi));

	fyi->state = FYIS_NONE;
	fyi->refs = 1;

	return fyi;
}

void fy_input_free(struct fy_input *fyi)
{
	if (!fyi)
		return;

	assert(fyi->refs == 1);

	switch (fyi->state) {
	case FYIS_NONE:
	case FYIS_QUEUED:
		/* nothing to do */
		break;
	case FYIS_PARSE_IN_PROGRESS:
	case FYIS_PARSED:
		fy_input_close(fyi);
		break;
	}

	/* always release the memory of the alloc memory */
	switch (fyi->cfg.type) {
	case fyit_alloc:
		free(fyi->cfg.alloc.data);
		break;

	default:
		break;
	}
	if (fyi->name)
		free(fyi->name);

	free(fyi);
}

const char *fy_input_get_filename(struct fy_input *fyi)
{
	if (!fyi)
		return NULL;

	return fyi->name;
}

static void fy_input_from_data_setup(struct fy_input *fyi,
				     struct fy_atom *handle, bool simple)
{
	const char *data;
	size_t size;
	unsigned int aflags;

	/* this is an internal method, you'd better to pass garbage */
	data = fy_input_start(fyi);
	size = fy_input_size(fyi);

	fyi->buffer = NULL;
	fyi->allocated = 0;
	fyi->read = 0;
	fyi->chunk = 0;
	fyi->chop = 0;
	fyi->fp = NULL;

	if (!handle)
		goto out;

	memset(handle, 0, sizeof(*handle));

	if (size > 0)
		aflags = fy_analyze_scalar_content(data, size,
				false, fylb_cr_nl, fyfws_space_tab);	/* hardcoded yaml mode */
	else
		aflags = FYACF_EMPTY | FYACF_FLOW_PLAIN | FYACF_BLOCK_PLAIN | FYACF_SIZE0;

	handle->start_mark.input_pos = 0;
	handle->start_mark.line = 0;
	handle->start_mark.column = 0;
	handle->end_mark.input_pos = size;
	handle->end_mark.line = 0;
	handle->end_mark.column = fy_utf8_count(data, size);
	/* if it's plain, all is good */
	if (simple || (aflags & FYACF_FLOW_PLAIN)) {
		handle->storage_hint = size;	/* maximum */
		handle->storage_hint_valid = false;
		handle->direct_output = !!(aflags & FYACF_JSON_ESCAPE);
		handle->style = FYAS_PLAIN;
	} else {
		handle->storage_hint = 0;	/* just calculate */
		handle->storage_hint_valid = false;
		handle->direct_output = false;
		handle->style = FYAS_DOUBLE_QUOTED_MANUAL;
	}
	handle->empty = !!(aflags & FYACF_EMPTY);
	handle->has_lb = !!(aflags & FYACF_LB);
	handle->has_ws = !!(aflags & FYACF_WS);
	handle->starts_with_ws = !!(aflags & FYACF_STARTS_WITH_WS);
	handle->starts_with_lb = !!(aflags & FYACF_STARTS_WITH_LB);
	handle->ends_with_ws = !!(aflags & FYACF_ENDS_WITH_WS);
	handle->ends_with_lb = !!(aflags & FYACF_ENDS_WITH_LB);
	handle->trailing_lb = !!(aflags & FYACF_TRAILING_LB);
	handle->size0 = !!(aflags & FYACF_SIZE0);
	handle->valid_anchor = !!(aflags & FYACF_VALID_ANCHOR);

	handle->chomp = FYAC_STRIP;
	handle->increment = 0;
	handle->fyi = fyi;
	handle->fyi_generation = fyi->generation;
	handle->tabsize = 0;
	handle->json_mode = false;	/* XXX hardcoded */
	handle->lb_mode = fylb_cr_nl;
	handle->fws_mode = fyfws_space_tab;
out:
	fyi->state = FYIS_PARSED;
}

struct fy_input *fy_input_from_data(const char *data, size_t size,
				    struct fy_atom *handle, bool simple)
{
	struct fy_input *fyi;

	if (data && size == (size_t)-1)
		size = strlen(data);

	fyi = fy_input_alloc();
	if (!fyi)
		return NULL;

	fyi->cfg.type = fyit_memory;
	fyi->cfg.userdata = NULL;
	fyi->cfg.memory.data = data;
	fyi->cfg.memory.size = size;

	fy_input_from_data_setup(fyi, handle, simple);

	return fyi;
}

struct fy_input *fy_input_from_malloc_data(char *data, size_t size,
					   struct fy_atom *handle, bool simple)
{
	struct fy_input *fyi;

	if (data && size == (size_t)-1)
		size = strlen(data);

	fyi = fy_input_alloc();
	if (!fyi)
		return NULL;

	fyi->cfg.type = fyit_alloc;
	fyi->cfg.userdata = NULL;
	fyi->cfg.alloc.data = data;
	fyi->cfg.alloc.size = size;

	fy_input_from_data_setup(fyi, handle, simple);

	return fyi;
}

void fy_input_close(struct fy_input *fyi)
{
	if (!fyi)
		return;

	switch (fyi->cfg.type) {

	case fyit_file:
	case fyit_fd:

#if !defined(_MSC_VER)
		if (fyi->addr) {
			munmap(fyi->addr, fyi->length);
			fyi->addr = NULL;
		}
#endif

		if (fyi->fd != -1) {
			if (!fyi->cfg.no_close_fd)
				close(fyi->fd);
			fyi->fd = -1;
		}

		if (fyi->buffer) {
			free(fyi->buffer);
			fyi->buffer = NULL;
		}
		if (fyi->fp) {
			if (!fyi->cfg.no_fclose_fp)
				fclose(fyi->fp);
			fyi->fp = NULL;
		}
		break;

	case fyit_stream:
	case fyit_callback:
		if (fyi->buffer) {
			free(fyi->buffer);
			fyi->buffer = NULL;
		}
		break;

	case fyit_memory:
		/* nothing */
		break;

	case fyit_alloc:
		/* nothing */
		break;

	default:
		break;
	}
}

struct fy_diag *fy_reader_get_diag(struct fy_reader *fyr)
{
	if (fyr && fyr->ops && fyr->ops->get_diag)
		return fyr->ops->get_diag(fyr);

	return NULL;
}

int fy_reader_file_open(struct fy_reader *fyr, const char *filename)
{
	if (!fyr || !filename)
		return -1;

	if (fyr->ops && fyr->ops->file_open)
		return fyr->ops->file_open(fyr, filename);

	return open(filename, O_RDONLY);
}

void fy_reader_reset(struct fy_reader *fyr)
{
	const struct fy_reader_ops *ops;
	struct fy_diag *diag;

	if (!fyr)
		return;

	ops = fyr->ops;
	diag = fyr->diag;

	fy_input_unref(fyr->current_input);

	memset(fyr, 0, sizeof(*fyr));

	/* by default we're always in yaml mode */
	fyr->mode = fyrm_yaml;
	fyr->ops = ops;
	fyr->diag = diag;
	fyr->current_c = -1;
}

void fy_reader_setup(struct fy_reader *fyr, const struct fy_reader_ops *ops)
{
	if (!fyr)
		return;

	fyr->ops = ops;
	fyr->diag = fy_reader_get_diag(fyr);
	fyr->current_input = NULL;
	fy_reader_reset(fyr);
}

void fy_reader_cleanup(struct fy_reader *fyr)
{
	if (!fyr)
		return;

	fy_input_unref(fyr->current_input);
	fyr->current_input = NULL;
	fy_reader_reset(fyr);
}

void fy_reader_apply_mode(struct fy_reader *fyr)
{
	struct fy_input *fyi;

	assert(fyr);

	/* set input mode from the current reader settings */
	switch (fyr->mode) {
	case fyrm_yaml:
		fyr->json_mode = false;
		fyr->lb_mode = fylb_cr_nl;
		fyr->fws_mode = fyfws_space_tab;
		break;
	case fyrm_json:
		fyr->json_mode = true;
		fyr->lb_mode = fylb_cr_nl;
		fyr->fws_mode = fyfws_space;
		break;
	case fyrm_yaml_1_1:
		fyr->json_mode = false;
		fyr->lb_mode = fylb_cr_nl_N_L_P;
		fyr->fws_mode = fyfws_space_tab;
		break;
	}
	fyi = fyr->current_input;
	if (fyi) {
		fyi->json_mode = fyr->json_mode;
		fyi->lb_mode = fyr->lb_mode;
		fyi->fws_mode = fyr->fws_mode;
	}
}

int fy_reader_input_open(struct fy_reader *fyr, struct fy_input *fyi, const struct fy_reader_input_cfg *icfg)
{
	struct stat sb;
	int rc;

	if (!fyi)
		return -1;

	/* unref any previous input */
	fy_input_unref(fyr->current_input);
	fyr->current_input = fy_input_ref(fyi);

	fy_reader_apply_mode(fyr);

	if (!icfg)
		memset(&fyr->current_input_cfg, 0, sizeof(fyr->current_input_cfg));
	else
		fyr->current_input_cfg = *icfg;

	/* reset common data */
	fyi->buffer = NULL;
	fyi->allocated = 0;
	fyi->read = 0;
	fyi->chunk = 0;
	fyi->chop = 0;
	fyi->fp = NULL;

	switch (fyi->cfg.type) {

	case fyit_file:
	case fyit_fd:

		switch (fyi->cfg.type) {
		case fyit_file:
			fyi->fd = fy_reader_file_open(fyr, fyi->cfg.file.filename);
			fyr_error_check(fyr, fyi->fd != -1, err_out,
					"failed to open %s",  fyi->cfg.file.filename);
			break;

		case fyit_fd:
			fyi->fd = fyi->cfg.fd.fd;
			fyr_error_check(fyr, fyi->fd >= 0, err_out,
					"bad file.fd %d",  fyi->cfg.fd.fd);
			break;
		default:
			assert(0);	// will never happen
		}

		rc = fstat(fyi->fd, &sb);
		fyr_error_check(fyr, rc != -1, err_out,
				"failed to fstat %s", fyi->cfg.file.filename);

		fyi->length = sb.st_size;

		/* only map if not zero (and is not disabled) */
#if !defined(_MSC_VER)
		if (sb.st_size > 0 && !fyr->current_input_cfg.disable_mmap_opt) {
			fyi->addr = mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE, fyi->fd, 0);

			/* convert from MAP_FAILED to NULL */
			if (fyi->addr == MAP_FAILED) {
				fyr_debug(fyr, "mmap failed for file %s",
						fyi->cfg.file.filename);
				fyi->addr = NULL;
			}
		}
#endif
		/* if we've managed to mmap, we' good */
		if (fyi->addr)
			break;

		/* if we're not ignoring stdio, open a FILE* using the fd */
		if (!fyi->cfg.ignore_stdio) {
			fyi->fp = fdopen(fyi->fd, "r");
			fyr_error_check(fyr, rc != -1, err_out, "failed to fdopen %s", fyi->name);
		} else
			fyi->fp = NULL;

		break;

	case fyit_stream:
		if (!fyi->cfg.ignore_stdio)
			fyi->fp = fyi->cfg.stream.fp;
		else
			fyi->fd = fileno(fyi->cfg.stream.fp);
		break;

	case fyit_memory:
		/* nothing to do for memory */
		break;

	case fyit_alloc:
		/* nothing to do for memory */
		break;

	case fyit_callback:
		break;

	default:
		assert(0);
		break;
	}

	switch (fyi->cfg.type) {

		/* those two need no memory */
	case fyit_memory:
	case fyit_alloc:
		break;

		/* all the rest need it */
	default:
		/* if we're not in mmap mode */
#if !defined(_MSC_VER)
		if (fyi->addr && !fyr->current_input_cfg.disable_mmap_opt)
			break;
#endif

		fyi->chunk = fyi->cfg.chunk;
		if (!fyi->chunk)
			fyi->chunk = fy_get_pagesize();
		fyi->chop = fyi->chunk * FYI_CHOP_MULT;
		fyi->buffer = malloc(fyi->chunk);
		fyr_error_check(fyr, fyi->buffer, err_out,
				"fy_alloc() failed");
		fyi->allocated = fyi->chunk;
		break;
	}

	fyr->this_input_start = 0;
	fyr->current_input_pos = 0;
	fyr->line = 0;
	fyr->column = 0;
	fyr->current_c = -1;
	fyr->current_ptr = NULL;
	fyr->current_w = 0;
	fyr->current_left = 0;

	fyi->state = FYIS_PARSE_IN_PROGRESS;

	return 0;

err_out:
	fy_input_close(fyi);
	return -1;
}

int fy_reader_input_done(struct fy_reader *fyr)
{
	struct fy_input *fyi;
	void *buf;

	if (!fyr)
		return -1;

	fyi = fyr->current_input;
	if (!fyi)
		return 0;

	switch (fyi->cfg.type) {
	case fyit_file:
	case fyit_fd:
		if (fyi->addr)
			break;

		/* fall-through */

	case fyit_stream:
	case fyit_callback:
		/* chop extra buffer */
		buf = realloc(fyi->buffer, fyr->current_input_pos);
		fyr_error_check(fyr, buf || !fyr->current_input_pos, err_out,
				"realloc() failed");

		fyi->buffer = buf;
		fyi->allocated = fyr->current_input_pos;
		/* increate input generation; required for direct input to work */
		fyi->generation++;
		break;

	default:
		break;

	}

	fyi->state = FYIS_PARSED;
	fy_input_unref(fyi);
	fyr->current_input = NULL;

	return 0;

err_out:
	return -1;
}

int fy_reader_input_scan_token_mark_slow_path(struct fy_reader *fyr)
{
	struct fy_input *fyi, *fyi_new = NULL;

	assert(fyr);

	if (!fy_reader_input_chop_active(fyr))
		return 0;

	fyi = fyr->current_input;
	assert(fyi);

	fyi_new = fy_input_alloc();
	fyr_error_check(fyr, fyi_new, err_out,
			"fy_input_alloc() failed\n");

	/* copy the config over */
	fyi_new->cfg = fyi->cfg;
	fyi_new->name = strdup(fyi->name);
	fyr_error_check(fyr, fyi_new->name, err_out,
			"strdup() failed\n");

	fyi_new->chunk = fyi->chunk;
	fyi_new->chop = fyi->chop;
	fyi_new->buffer = malloc(fyi->chunk);
	fyr_error_check(fyr, fyi_new->buffer, err_out,
			"fy_alloc() failed");
	fyi_new->allocated = fyi->chunk;
	fyi_new->fp = fyi->fp;

	fyi->fp = NULL;	/* the file pointer now assigned to the new */

	fyi_new->lb_mode = fyi->lb_mode;
	fyi_new->fws_mode = fyi->fws_mode;

	fyi_new->state = FYIS_PARSE_IN_PROGRESS;

	/* adjust and copy the left over reads */
	assert(fyi->read >= fyr->current_input_pos);
	fyi_new->read = fyi->read - fyr->current_input_pos;
	if (fyi_new->read > 0)
		memcpy(fyi_new->buffer, (char *)fyi->buffer + fyr->current_input_pos, fyi_new->read);

	fyr->this_input_start += fyr->current_input_pos;

	/* update the reader to point to the new input */
	fyr->current_input = fyi_new;
	fyr->current_input_pos = 0;
	fyr->current_ptr = fyi_new->buffer;

	fyr_debug(fyr, "chop at this_input_start=%zu chop=%zu\n", fyr->this_input_start, fyi->chop);

	/* free the old input - while references to it exist it will hang around */
	fyi->state = FYIS_PARSED;
	fy_input_unref(fyi);
	fyi = NULL;

	return 0;
err_out:
	fy_input_unref(fyi_new);
	return -1;
}

const void *fy_reader_ptr_slow_path(struct fy_reader *fyr, size_t *leftp)
{
	struct fy_input *fyi;
	const void *p;
	int left;

	if (fyr->current_ptr) {
		if (leftp)
			*leftp = fyr->current_left;
		return fyr->current_ptr;
	}

	fyi = fyr->current_input;
	if (!fyi)
		return NULL;

	/* tokens cannot cross boundaries */
	switch (fyi->cfg.type) {
	case fyit_file:
	case fyit_fd:
		if (fyi->addr) {
			left = fyi->length - (fyr->this_input_start + fyr->current_input_pos);
			p = (char *)fyi->addr + fyr->current_input_pos;
			break;
		}

		/* fall-through */

	case fyit_stream:
	case fyit_callback:
		left = fyi->read - (fyr->this_input_start + fyr->current_input_pos);
		p = (char *)fyi->buffer + fyr->current_input_pos;
		break;

	case fyit_memory:
		left = fyi->cfg.memory.size - fyr->current_input_pos;
		p = (char *)fyi->cfg.memory.data + fyr->current_input_pos;
		break;

	case fyit_alloc:
		left = fyi->cfg.alloc.size - fyr->current_input_pos;
		p = (char *)fyi->cfg.alloc.data + fyr->current_input_pos;
		break;


	default:
		assert(0);	/* no streams */
		p = NULL;
		left = 0;
		break;
	}

	if (leftp)
		*leftp = left;

	fyr->current_ptr = p;
	fyr->current_left = left;
	fyr->current_c = fy_utf8_get(p, left, &fyr->current_w);

	return p;
}

const void *fy_reader_input_try_pull(struct fy_reader *fyr, struct fy_input *fyi,
				     size_t pull, size_t *leftp)
{
	const void *p;
	size_t left, pos, size, nread, nreadreq, missing;
	ssize_t snread;
	size_t space __FY_DEBUG_UNUSED__;
	void *buf;

	if (!fyr || !fyi) {
		if (leftp)
			*leftp = 0;
		return NULL;
	}

	p = NULL;
	left = 0;
	pos = fyr->current_input_pos;

	switch (fyi->cfg.type) {
	case fyit_file:
	case fyit_fd:

		if (fyi->addr) {
			assert(fyi->length >= (fyr->this_input_start + pos));

			left = fyi->length - (fyr->this_input_start + pos);
			if (!left) {
				fyr_debug(fyr, "file input exhausted");
				break;
			}
			p = (char *)fyi->addr + pos;
			break;
		}

		/* fall-through */

	case fyit_stream:
	case fyit_callback:

		assert(fyi->read >= pos);

		left = fyi->read - pos;
		p = (char *)fyi->buffer + pos;

		/* enough to satisfy directly */
		if (left >= pull)
			break;

		/* no more */
		if (fyi->eof) {
			if (!left) {
				fyr_debug(fyr, "input exhausted (EOF)");
				p = NULL;
			}
			break;
		}

		space = fyi->allocated - pos;

		/* if we're missing more than the buffer space */
		missing = pull - left;

		fyr_debug(fyr, "input: allocated=%zu read=%zu pos=%zu pull=%zu left=%zu space=%zu missing=%zu",
				fyi->allocated, fyi->read, pos, pull, left, space, missing);

		if (pos + pull > fyi->allocated) {

			/* align size to chunk */
			size = fyi->allocated + missing + fyi->chunk - 1;
			size = size - size % fyi->chunk;

			fyr_debug(fyr, "input buffer missing %zu bytes (pull=%zu)", missing, pull);

			buf = realloc(fyi->buffer, size);
			if (!buf) {
				fyr_error(fyr, "realloc() failed");
				goto err_out;
			}

			fyr_debug(fyr, "input read allocated=%zu new-size=%zu", fyi->allocated, size);

			fyi->buffer = buf;
			fyi->allocated = size;
			fyi->generation++;

			space = fyi->allocated - pos;
			p = (char *)fyi->buffer + pos;
		}

		/* always try to read up to the allocated space */
		do {
			nreadreq = fyi->allocated - fyi->read;
			assert(nreadreq > 0);

			if (fyi->cfg.type == fyit_callback) {

				fyr_debug(fyr, "performing callback request of %zu", nreadreq);

				nread = fyi->cfg.callback.input(fyi->cfg.userdata, (char *)fyi->buffer + fyi->read, nreadreq);

				fyr_debug(fyr, "callback returned %zu", nread);

				if (nread <= 0) {
					if (!nread) {
						fyi->eof = true;
						fyr_debug(fyr, "callback got EOF");
					} else {
						fyi->err = true;
						fyr_debug(fyr, "callback got error");
					}
					break;
				}

			} else if (fyi->fp) {

				fyr_debug(fyr, "performing fread request of %zu", nreadreq);

				nread = fread((char *)fyi->buffer + fyi->read, 1, nreadreq, fyi->fp);

				fyr_debug(fyr, "fread returned %zu", nread);

				if (nread <= 0) {
					fyi->err = ferror(fyi->fp);
					if (fyi->err) {
						fyi->eof = true;
						fyr_debug(fyr, "fread got ERROR");
						goto err_out;
					}

					fyi->eof = feof(fyi->fp);
					if (fyi->eof)
						fyr_debug(fyr, "fread got EOF");
					nread = 0;
					break;
				}

			} else if (fyi->fd >= 0) {

				fyr_debug(fyr, "performing read request of %zu", nreadreq);

				do {
					snread = read(fyi->fd, (char *)fyi->buffer + fyi->read, nreadreq);
				} while (snread == -1 && errno == EAGAIN);

				fyr_debug(fyr, "read returned %zd", snread);

				if (snread == -1) {
					fyi->err = true;
					fyi->eof = true;
					fyr_error(fyr, "read() failed: %s", strerror(errno));
					goto err_out;
				}

				if (!snread) {
					fyi->eof = true;
					nread = 0;
					break;
				}

				nread = snread;
			} else {
				fyr_error(fyr, "No FILE* nor fd available?");
				fyi->eof = true;
				nread = 0;
				goto err_out;
			}

			assert(nread > 0);

			fyi->read += nread;
			left = fyi->read - pos;

		} while (left < pull);

		/* no more, move it to parsed input chunk list */
		if (!left) {
			fyr_debug(fyr, "input exhausted");
			p = NULL;
		}
		break;

	case fyit_memory:
		assert(fyi->cfg.memory.size >= pos);

		left = fyi->cfg.memory.size - pos;
		if (!left) {
			fyr_debug(fyr, "memory input exhausted");
			break;
		}
		p = (char *)fyi->cfg.memory.data + pos;
		break;

	case fyit_alloc:
		assert(fyi->cfg.alloc.size >= pos);

		left = fyi->cfg.alloc.size - pos;
		if (!left) {
			fyr_debug(fyr, "alloc input exhausted");
			break;
		}
		p = (char *)fyi->cfg.alloc.data + pos;
		break;


	default:
		assert(0);
		break;
	}

	if (leftp)
		*leftp = left;
	return p;

err_out:
	if (leftp)
		*leftp = 0;
	return NULL;
}

void
fy_reader_advance_slow_path(struct fy_reader *fyr, int c)
{
	bool is_line_break = false;
	size_t w;

	/* skip this character (optimize case of being the current) */
	w = c == fyr->current_c ? (size_t)fyr->current_w : fy_utf8_width(c);
	fy_reader_advance_octets(fyr, w);

	/* first check for CR/LF */
	if (c == '\r' && fy_reader_peek(fyr) == '\n') {
		fy_reader_advance_octets(fyr, 1);
		is_line_break = true;
	} else if (fy_reader_is_lb(fyr, c))
		is_line_break = true;

	if (is_line_break) {
		fyr->column = 0;
		fyr->line++;
	} else if (fyr->tabsize && fy_is_tab(c))
		fyr->column += (fyr->tabsize - (fyr->column % fyr->tabsize));
	else
		fyr->column++;
}

struct fy_input *fy_input_create(const struct fy_input_cfg *fyic)
{
	struct fy_input *fyi = NULL;
	int ret;

	fyi = fy_input_alloc();
	if (!fyi)
		return NULL;
	fyi->cfg = *fyic;

	/* copy filename pointers and switch */
	switch (fyic->type) {

	case fyit_file:
		fyi->name = strdup(fyic->file.filename);
		break;

	case fyit_fd:
		ret = asprintf(&fyi->name, "<fd-%d>", fyic->fd.fd);
		if (ret == -1)
			fyi->name = NULL;
		break;

	case fyit_stream:
		if (fyic->stream.name)
			fyi->name = strdup(fyic->stream.name);
		else if (fyic->stream.fp == stdin)
			fyi->name = strdup("<stdin>");
		else {
			ret = asprintf(&fyi->name, "<stream-%d>",
					fileno(fyic->stream.fp));
			if (ret == -1)
				fyi->name = NULL;
		}
		break;
	case fyit_memory:
		ret = asprintf(&fyi->name, "<memory-@%p-%p>",
			fyic->memory.data, (char *)fyic->memory.data + fyic->memory.size - 1);
		if (ret == -1)
			fyi->name = NULL;
		break;
	case fyit_alloc:
		ret = asprintf(&fyi->name, "<alloc-@%p-%p>",
			fyic->memory.data, (char *)fyic->memory.data + fyic->memory.size - 1);
		if (ret == -1)
			fyi->name = NULL;
		break;
	case fyit_callback:
		ret = asprintf(&fyi->name, "<callback>");
		if (ret == -1)
			fyi->name = NULL;
		break;
	default:
		assert(0);
		break;
	}
	if (!fyi->name)
		goto err_out;

	fyi->buffer = NULL;
	fyi->allocated = 0;
	fyi->read = 0;
	fyi->chunk = 0;
	fyi->chop = 0;
	fyi->fp = NULL;
	fyi->fd = -1;
	fyi->addr = NULL;
	fyi->length = -1;

	/* default modes */
	fyi->lb_mode = fylb_cr_nl;
	fyi->fws_mode = fyfws_space_tab;

	return fyi;

err_out:
	fy_input_unref(fyi);
	return NULL;
}

/* ensure that there are at least size octets available */
const void *fy_reader_ensure_lookahead_slow_path(struct fy_reader *fyr, size_t size, size_t *leftp)
{
	const void *p;
	size_t left;

	if (!leftp)
		leftp = &left;

	p = fy_reader_ptr(fyr, leftp);
	if (!p || *leftp < size) {

		fyr_debug(fyr, "ensure lookahead size=%zd left=%zd (%s - %zu)",
				size, *leftp,
				fy_input_get_filename(fyr->current_input),
				fyr->current_input_pos);

		p = fy_reader_input_try_pull(fyr, fyr->current_input, size, leftp);
		if (!p || *leftp < size)
			return NULL;

		fyr->current_ptr = p;
		fyr->current_left = *leftp;
		fyr->current_c = fy_utf8_get(fyr->current_ptr, fyr->current_left, &fyr->current_w);
	}
	return p;
}

