/* -*- mode: C; c-file-style: "linux" -*- */
/* 
 * GdkPixbuf library - TGA image loader
 * Copyright (C) 1999 Nicola Girardi <nikke@swlibero.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 *
 */

/*
 * Some NOTES about the TGA loader (2001/06/07, nikke@swlibero.org)
 *
 * - The TGAFooter isn't present in all TGA files.  In fact, there's an older
 *   format specification, still in use, which doesn't cover the TGAFooter.
 *   Actually, most TGA files I have are of the older type.  Anyway I put the 
 *   struct declaration here for completeness.
 *
 * - Error handling was designed to be very paranoid.
 */

#include <contrib/restricted/glib/config.h>
#include <stdio.h>
#include <string.h>

#include "gdk-pixbuf-private.h"

#undef DEBUG_TGA

#define TGA_INTERLEAVE_MASK     0xc0
#define TGA_INTERLEAVE_NONE     0x00
#define TGA_INTERLEAVE_2WAY     0x40
#define TGA_INTERLEAVE_4WAY     0x80

#define TGA_ORIGIN_MASK         0x30
#define TGA_ORIGIN_RIGHT        0x10
#define TGA_ORIGIN_UPPER        0x20

enum {
	TGA_TYPE_NODATA = 0,
	TGA_TYPE_PSEUDOCOLOR = 1,
	TGA_TYPE_TRUECOLOR = 2,
	TGA_TYPE_GRAYSCALE = 3,
	TGA_TYPE_RLE_PSEUDOCOLOR = 9,
	TGA_TYPE_RLE_TRUECOLOR = 10,
	TGA_TYPE_RLE_GRAYSCALE = 11
};

#define LE16(p) ((p)[0] + ((p)[1] << 8))

typedef struct _IOBuffer IOBuffer;

typedef struct _TGAHeader TGAHeader;
typedef struct _TGAFooter TGAFooter;

typedef struct _TGAColor TGAColor;
typedef struct _TGAColormap TGAColormap;

typedef struct _TGAContext TGAContext;

struct _TGAHeader {
	guint8 infolen;
	guint8 has_cmap;
	guint8 type;
	
	guint8 cmap_start[2];
	guint8 cmap_n_colors[2];
	guint8 cmap_bpp;
	
	guint8 x_origin[2];
	guint8 y_origin[2];
	
	guint8 width[2];
	guint8 height[2];
	guint8 bpp;
	
	guint8 flags;
};

struct _TGAFooter {
	guint32 extension_area_offset;
	guint32 developer_directory_offset;

	/* Standard TGA signature, "TRUEVISION-XFILE.\0". */
	union {
		gchar sig_full[18];
		struct {
			gchar sig_chunk[16];
			gchar dot, null;
		} sig_struct;
	} sig;
};

struct _TGAColor {
	guchar r, g, b, a;
};

struct _TGAColormap {
	guint n_colors;
	TGAColor colors[1];
};

struct _TGAContext {
	TGAHeader *hdr;
	guint rowstride;
	guint completed_lines;
	gboolean run_length_encoded;

	TGAColormap *cmap;
	guint cmap_size;

	GdkPixbuf *pbuf;
	guint pbuf_bytes;
	guint pbuf_bytes_done;
	guchar *pptr;

	IOBuffer *in;

	gboolean skipped_info;
	gboolean prepared;
	gboolean done;

	GdkPixbufModuleSizeFunc sfunc;
	GdkPixbufModulePreparedFunc pfunc;
	GdkPixbufModuleUpdatedFunc ufunc;
	gpointer udata;
};

struct _IOBuffer {
	guchar *data;
	guint size;
};

static IOBuffer *io_buffer_new(GError **err)
{
	IOBuffer *buffer;
	buffer = g_try_malloc(sizeof(IOBuffer));
	if (!buffer) {
		g_set_error_literal(err, GDK_PIXBUF_ERROR,
                                    GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                    _("Cannot allocate memory for IOBuffer struct"));
		return NULL;
	}
	buffer->data = NULL;
	buffer->size = 0;
	return buffer;
}

static IOBuffer *io_buffer_append(IOBuffer *buffer, 
				  const guchar *data, guint len, 
				  GError **err)
{
	if (!buffer)
		return NULL;
	if (!buffer->data) {
		buffer->data = g_try_malloc(len);
		if (!buffer->data) {
			g_set_error_literal(err, GDK_PIXBUF_ERROR,
                                            GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                            _("Cannot allocate memory for IOBuffer data"));
			g_free(buffer);
			return NULL;
		}
		g_memmove(buffer->data, data, len);
		buffer->size = len;
	} else {
		guchar *tmp = g_try_realloc (buffer->data, buffer->size + len);
		if (!tmp) {
			g_set_error_literal(err, GDK_PIXBUF_ERROR,
                                            GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                            _("Cannot realloc IOBuffer data"));
			g_free(buffer);
			return NULL;
		}
		buffer->data = tmp;
		g_memmove(&buffer->data[buffer->size], data, len);
		buffer->size += len;
	}
	return buffer;
}

static IOBuffer *io_buffer_free_segment(IOBuffer *buffer, 
					guint count,
                                        GError **err)
{
	g_return_val_if_fail(buffer != NULL, NULL);
	g_return_val_if_fail(buffer->data != NULL, NULL);
	if (count == buffer->size) {
		g_free(buffer->data);
		buffer->data = NULL;
		buffer->size = 0;
	} else {
		guchar *new_buf;
		guint new_size;

		new_size = buffer->size - count;
		new_buf = g_try_malloc(new_size);
		if (!new_buf) {
			g_set_error_literal(err, GDK_PIXBUF_ERROR,
                                            GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                            _("Cannot allocate temporary IOBuffer data"));
			g_free(buffer->data);
			g_free(buffer);
			return NULL;
		}

		g_memmove(new_buf, &buffer->data[count], new_size);
		g_free(buffer->data);
		buffer->data = new_buf;
		buffer->size = new_size;
	}
	return buffer;
}

static void io_buffer_free(IOBuffer *buffer)
{
	g_return_if_fail(buffer != NULL);
	g_free(buffer->data);
	g_free(buffer);
}

static void free_buffer(guchar *pixels, gpointer data)
{
	g_free(pixels);
}

static TGAColormap *
colormap_new (guint n_colors)
{
  TGAColormap *cmap;

  g_assert (n_colors <= G_MAXUINT16);

  cmap = g_try_malloc0 (sizeof (TGAColormap) + (MAX (n_colors, 1) - 1) * sizeof (TGAColor));
  if (cmap == NULL)
    return NULL;

  cmap->n_colors = n_colors;

  return cmap;
}

static const TGAColor *
colormap_get_color (TGAColormap *cmap,
                    guint        id)
{
  static const TGAColor transparent_black = { 0, 0, 0, 0 };

  if (id >= cmap->n_colors)
    return &transparent_black;

  return &cmap->colors[id];
}

static void
colormap_set_color (TGAColormap    *cmap,
                    guint           id,
                    const TGAColor *color)
{
  if (id >= cmap->n_colors)
    return;

  cmap->colors[id] = *color;
}

static void
colormap_free (TGAColormap *cmap)
{
  g_free (cmap);
}

static GdkPixbuf *get_contiguous_pixbuf (guint width, 
					 guint height, 
					 gboolean has_alpha)
{
	guchar *pixels;
	guint channels, rowstride;
	
	if (has_alpha) 
		channels = 4;
	else 
		channels = 3;
	
	rowstride = width * channels;
	
	if (rowstride / channels != width)
                return NULL;                

        pixels = g_try_malloc_n (height, rowstride);

	if (!pixels)
		return NULL;
	
	return gdk_pixbuf_new_from_data (pixels, GDK_COLORSPACE_RGB, has_alpha, 8,
					 width, height, rowstride, free_buffer, NULL);
}

static void pixbuf_flip_row (GdkPixbuf *pixbuf, guchar *ph)
{
	guchar *p, *s;
	guchar tmp;
	gint count;

	p = ph;
	s = p + pixbuf->n_channels * (pixbuf->width - 1);
	while (p < s) {
		for (count = pixbuf->n_channels; count > 0; count--, p++, s++) {
			tmp = *p;
			*p = *s;
			*s = tmp;
		}
		s -= 2 * pixbuf->n_channels;
	}		
}

static void pixbuf_flip_vertically (GdkPixbuf *pixbuf)
{
	guchar *ph, *sh, *p, *s;
	guchar tmp;
	gint count;

	ph = pixbuf->pixels;
	sh = pixbuf->pixels + pixbuf->height*pixbuf->rowstride;
	while (ph < sh - pixbuf->rowstride) {
		p = ph;
		s = sh - pixbuf->rowstride;
		for (count = pixbuf->n_channels * pixbuf->width; count > 0; count--, p++, s++) {
			tmp = *p;
			*p = *s;
			*s = tmp;
		}
		sh -= pixbuf->rowstride;
		ph += pixbuf->rowstride;
	}
}

static gboolean fill_in_context(TGAContext *ctx, GError **err)
{
	gboolean alpha;
	guint w, h;

	g_return_val_if_fail(ctx != NULL, FALSE);

	ctx->run_length_encoded =
		((ctx->hdr->type == TGA_TYPE_RLE_PSEUDOCOLOR)
		 || (ctx->hdr->type == TGA_TYPE_RLE_TRUECOLOR)
		 || (ctx->hdr->type == TGA_TYPE_RLE_GRAYSCALE));

        ctx->cmap_size = ((ctx->hdr->cmap_bpp + 7) >> 3) *
                LE16(ctx->hdr->cmap_n_colors);

	alpha = ((ctx->hdr->bpp == 16) || 
		 (ctx->hdr->bpp == 32) ||
		 (ctx->hdr->has_cmap && (ctx->hdr->cmap_bpp == 32)));

	w = LE16(ctx->hdr->width);
	h = LE16(ctx->hdr->height);

	if (ctx->sfunc) {
		gint wi = w;
		gint hi = h;
		
		(*ctx->sfunc) (&wi, &hi, ctx->udata);
		
		if (wi == 0 || hi == 0) 
			return FALSE;
	}

	ctx->pbuf = get_contiguous_pixbuf (w, h, alpha);

	if (!ctx->pbuf) {
		g_set_error_literal(err, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                    _("Cannot allocate new pixbuf"));
		return FALSE;
	}

	ctx->pbuf_bytes = ctx->pbuf->rowstride * ctx->pbuf->height;
	if ((ctx->hdr->flags & TGA_ORIGIN_UPPER) || ctx->run_length_encoded)
		ctx->pptr = ctx->pbuf->pixels;
	else
		ctx->pptr = ctx->pbuf->pixels + (ctx->pbuf->height - 1)*ctx->pbuf->rowstride;

	if (ctx->hdr->type == TGA_TYPE_PSEUDOCOLOR)
	  ctx->rowstride = ctx->pbuf->width;
	else if (ctx->hdr->type == TGA_TYPE_GRAYSCALE)
	  ctx->rowstride = (alpha ? ctx->pbuf->width * 2 : ctx->pbuf->width);
	else if (ctx->hdr->type == TGA_TYPE_TRUECOLOR)
		ctx->rowstride = ctx->pbuf->rowstride;

	ctx->completed_lines = 0;
	return TRUE;
}

static void parse_data_for_row_pseudocolor(TGAContext *ctx)
{
	guchar *s = ctx->in->data;
	guint upper_bound = ctx->pbuf->width;
	guchar *p = ctx->pptr;

	for (; upper_bound; upper_bound--, s++) {
                const TGAColor *color = colormap_get_color (ctx->cmap, *s);
		*p++ = color->r;
		*p++ = color->g;
		*p++ = color->b;
		if (ctx->hdr->cmap_bpp == 32)
			*p++ = color->a;
	}
}

static void swap_channels(TGAContext *ctx)
{
	guchar swap;
	guint count;
	guchar *p = ctx->pptr;
	for (count = ctx->pbuf->width; count; count--) {
	  swap = p[0];
	  p[0] = p[2];
	  p[2] = swap;
	  p += ctx->pbuf->n_channels;
	}
}

static void parse_data_for_row_truecolor(TGAContext *ctx)
{
	g_memmove(ctx->pptr, ctx->in->data, ctx->pbuf->rowstride);
	swap_channels(ctx);
}

static void parse_data_for_row_grayscale(TGAContext *ctx)
{
	guchar *s = ctx->in->data;
	guint upper_bound = ctx->pbuf->width;

	guchar *p = ctx->pptr;
	for (; upper_bound; upper_bound--) {
		p[0] = p[1] = p[2] = *s++;
		if (ctx->pbuf->n_channels == 4)
		  p[3] = *s++;
		p += ctx->pbuf->n_channels;
	}
}

static gboolean parse_data_for_row(TGAContext *ctx, GError **err)
{
	guint row;

	if (ctx->hdr->type == TGA_TYPE_PSEUDOCOLOR)
		parse_data_for_row_pseudocolor(ctx);
	else if (ctx->hdr->type == TGA_TYPE_TRUECOLOR)
		parse_data_for_row_truecolor(ctx);
	else if (ctx->hdr->type == TGA_TYPE_GRAYSCALE)
		parse_data_for_row_grayscale(ctx);

	if (ctx->hdr->flags & TGA_ORIGIN_RIGHT)
		pixbuf_flip_row (ctx->pbuf, ctx->pptr);
	if (ctx->hdr->flags & TGA_ORIGIN_UPPER)
		ctx->pptr += ctx->pbuf->rowstride;
	else
		ctx->pptr -= ctx->pbuf->rowstride;
	ctx->pbuf_bytes_done += ctx->pbuf->rowstride;
	if (ctx->pbuf_bytes_done == ctx->pbuf_bytes)
		ctx->done = TRUE;
	
	ctx->in = io_buffer_free_segment(ctx->in, ctx->rowstride, err);
	if (!ctx->in)
		return FALSE;
	row = (ctx->pptr - ctx->pbuf->pixels) / ctx->pbuf->rowstride - 1;
	if (ctx->ufunc)
		(*ctx->ufunc) (ctx->pbuf, 0, row, ctx->pbuf->width, 1, ctx->udata);
	return TRUE;
}

static void write_rle_data(TGAContext *ctx, const TGAColor *color, guint *rle_count)
{
	for (; *rle_count; (*rle_count)--) {
		g_memmove(ctx->pptr, (guchar *) color, ctx->pbuf->n_channels);
		ctx->pptr += ctx->pbuf->n_channels;
		ctx->pbuf_bytes_done += ctx->pbuf->n_channels;
		if (ctx->pbuf_bytes_done == ctx->pbuf_bytes)
			return;
	}
}

static guint parse_rle_data_pseudocolor(TGAContext *ctx)
{
	guint rle_num, raw_num;
	guchar *s, tag;
	guint n;

	g_return_val_if_fail(ctx->in->size > 0, 0);
	s = ctx->in->data;

	for (n = 0; n < ctx->in->size; ) {
		tag = *s;
		s++, n++;
		if (tag & 0x80) {
			if (n == ctx->in->size) {
				return --n;
			} else {
				rle_num = (tag & 0x7f) + 1;
				write_rle_data(ctx, colormap_get_color (ctx->cmap, *s), &rle_num);
				s++, n++;
				if (ctx->pbuf_bytes_done == ctx->pbuf_bytes) {
					ctx->done = TRUE;
					return n;
				}
			}
		} else {
			raw_num = tag + 1;
			if (n + raw_num >= ctx->in->size) {
				return --n;
			} else {
				for (; raw_num; raw_num--) {
                                        const TGAColor *color = colormap_get_color (ctx->cmap, *s);
					*ctx->pptr++ = color->r;
					*ctx->pptr++ = color->g;
					*ctx->pptr++ = color->b;
					if (ctx->pbuf->n_channels == 4)
						*ctx->pptr++ = color->a;
					s++, n++;
					ctx->pbuf_bytes_done += ctx->pbuf->n_channels;
					if (ctx->pbuf_bytes_done == ctx->pbuf_bytes) {
						ctx->done = TRUE;
						return n;
					}
				}
			}
		}
	}

	if (ctx->pbuf_bytes_done == ctx->pbuf_bytes) 
		ctx->done = TRUE;
	
	return n;
}

static guint parse_rle_data_truecolor(TGAContext *ctx)
{
	TGAColor col;
	guint rle_num, raw_num;
	guchar *s, tag;
	guint n = 0;

	g_return_val_if_fail(ctx->in->size > 0, 0);
	s = ctx->in->data;

	for (n = 0; n < ctx->in->size; ) {
		tag = *s;
		s++, n++;
		if (tag & 0x80) {
			if (n + ctx->pbuf->n_channels >= ctx->in->size) {
				return --n;
			} else {
				rle_num = (tag & 0x7f) + 1;
				col.b = *s++;
				col.g = *s++;
				col.r = *s++;
				if (ctx->hdr->bpp == 32)
					col.a = *s++;
				n += ctx->pbuf->n_channels;
				write_rle_data(ctx, &col, &rle_num);
				if (ctx->pbuf_bytes_done == ctx->pbuf_bytes) {
					ctx->done = TRUE;
					return n;
				}
			}
		} else {
			raw_num = tag + 1;
			if (n + (raw_num * ctx->pbuf->n_channels) >= ctx->in->size) {
				return --n;
			} else {
				for (; raw_num; raw_num--) {
					ctx->pptr[2] = *s++;
					ctx->pptr[1] = *s++;
					ctx->pptr[0] = *s++;
					if (ctx->hdr->bpp == 32)
						ctx->pptr[3] = *s++;
					n += ctx->pbuf->n_channels;
					ctx->pptr += ctx->pbuf->n_channels;
					ctx->pbuf_bytes_done += ctx->pbuf->n_channels;
					if (ctx->pbuf_bytes_done == ctx->pbuf_bytes) {
						ctx->done = TRUE;
						return n;
					}
				}
				
				if (ctx->pbuf_bytes_done == ctx->pbuf_bytes) {
					ctx->done = TRUE;
					return n;
				}
			}
		}
	}
	if (ctx->pbuf_bytes_done == ctx->pbuf_bytes)
		ctx->done = TRUE;
	return n;
}

static guint parse_rle_data_grayscale(TGAContext *ctx)
{
	TGAColor tone;
	guint rle_num, raw_num;
	guchar *s, tag;
	guint n;

	g_return_val_if_fail(ctx->in->size > 0, 0);
	s = ctx->in->data;

	for (n = 0; n < ctx->in->size; ) {
		tag = *s;
		s++, n++;
		if (tag & 0x80) {
			if (n + (ctx->pbuf->n_channels == 4 ? 2 : 1) >= ctx->in->size) {
				return --n;
			} else {
				rle_num = (tag & 0x7f) + 1;
				tone.r = tone.g = tone.b = *s;
				s++, n++;
				if (ctx->pbuf->n_channels == 4) {
					tone.a = *s++;
					n++;
				}
				write_rle_data(ctx, &tone, &rle_num);
				if (ctx->pbuf_bytes_done == ctx->pbuf_bytes) {
					ctx->done = TRUE;
					return n;
				}
			}
		} else {
			raw_num = tag + 1;
			if (n + raw_num * (ctx->pbuf->n_channels == 4 ? 2 : 1) >= ctx->in->size) {
				return --n;
			} else {
				for (; raw_num; raw_num--) {
					ctx->pptr[0] = ctx->pptr[1] = ctx->pptr[2] = *s;
					s++, n++;
					if (ctx->pbuf->n_channels == 4) {
						ctx->pptr[3] = *s++;
						n++;
					}
					ctx->pptr += ctx->pbuf->n_channels;
					ctx->pbuf_bytes_done += ctx->pbuf->n_channels;
					if (ctx->pbuf_bytes_done == ctx->pbuf_bytes) {
						ctx->done = TRUE;
						return n;
					}
				}
			}
		}
	}
	if (ctx->pbuf_bytes_done == ctx->pbuf_bytes)
		ctx->done = TRUE;
	return n;
}

static gboolean parse_rle_data(TGAContext *ctx, GError **err)
{
	guint rows = 0;
	guint count = 0;
	guint bytes_done_before = ctx->pbuf_bytes_done;

	if (ctx->hdr->type == TGA_TYPE_RLE_PSEUDOCOLOR)
		count = parse_rle_data_pseudocolor(ctx);
	else if (ctx->hdr->type == TGA_TYPE_RLE_TRUECOLOR)
		count = parse_rle_data_truecolor(ctx);
	else if (ctx->hdr->type == TGA_TYPE_RLE_GRAYSCALE)
		count = parse_rle_data_grayscale(ctx);

	if (ctx->hdr->flags & TGA_ORIGIN_RIGHT) {
		guchar *row = ctx->pbuf->pixels + (bytes_done_before / ctx->pbuf->rowstride) * ctx->pbuf->rowstride;
		guchar *row_after = ctx->pbuf->pixels + (ctx->pbuf_bytes_done / ctx->pbuf->rowstride) * ctx->pbuf->rowstride;
		for (; row < row_after; row += ctx->pbuf->rowstride)
			pixbuf_flip_row (ctx->pbuf, row);
	}

	ctx->in = io_buffer_free_segment(ctx->in, count, err);
	if (!ctx->in)
		return FALSE;

	if (ctx->done) {
		/* FIXME doing the vertical flipping afterwards is not
		 * perfect, but doing it during the rle decoding in place
		 * is considerably more work. 
		 */
		if (!(ctx->hdr->flags & TGA_ORIGIN_UPPER)) {
			pixbuf_flip_vertically (ctx->pbuf);
			ctx->hdr->flags |= TGA_ORIGIN_UPPER;
		}

	}
		
	rows = ctx->pbuf_bytes_done / ctx->pbuf->rowstride - bytes_done_before / ctx->pbuf->rowstride;
	if (ctx->ufunc)
		(*ctx->ufunc) (ctx->pbuf, 0, bytes_done_before / ctx->pbuf->rowstride,
			       ctx->pbuf->width, rows,
			       ctx->udata);

	return TRUE;
}

static gboolean try_colormap(TGAContext *ctx, GError **err)
{
        TGAColor color;
	guchar *p;
	guint i, n_colors;

	g_return_val_if_fail(ctx != NULL, FALSE);

        n_colors = LE16(ctx->hdr->cmap_n_colors);
        ctx->cmap = colormap_new (n_colors);
	if (!ctx->cmap) {
		g_set_error_literal(err, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                    _("Cannot allocate colormap"));
		return FALSE;
	}

	p = ctx->in->data;
        color.a = 255;

	for (i = 0; i < n_colors; i++) {
		if ((ctx->hdr->cmap_bpp == 15) || (ctx->hdr->cmap_bpp == 16)) {
			guint16 col = p[0] + (p[1] << 8);
			color.b = (col >> 7) & 0xf8;
			color.g = (col >> 2) & 0xf8;
			color.r = col << 3;
			p += 2;
		}
		else if ((ctx->hdr->cmap_bpp == 24) || (ctx->hdr->cmap_bpp == 32)) {
			color.b = *p++;
			color.g = *p++;
			color.r = *p++;
			if (ctx->hdr->cmap_bpp == 32)
				color.a = *p++;
		} else {
			g_set_error_literal(err, GDK_PIXBUF_ERROR, 
                                            GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                            _("Unexpected bitdepth for colormap entries"));
			return FALSE;
		}
                colormap_set_color (ctx->cmap, i, &color);
	}
	ctx->in = io_buffer_free_segment(ctx->in, ctx->cmap_size, err);
	if (!ctx->in)
		return FALSE;
	return TRUE;
}

static gboolean try_preload(TGAContext *ctx, GError **err)
{
	if (!ctx->hdr) {
		if (ctx->in->size >= sizeof(TGAHeader)) {
			ctx->hdr = g_try_malloc(sizeof(TGAHeader));
			if (!ctx->hdr) {
				g_set_error_literal(err, GDK_PIXBUF_ERROR,
                                                    GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                                    _("Cannot allocate TGA header memory"));
				return FALSE;
			}
			g_memmove(ctx->hdr, ctx->in->data, sizeof(TGAHeader));
			ctx->in = io_buffer_free_segment(ctx->in, sizeof(TGAHeader), err);
#ifdef DEBUG_TGA
			g_print ("infolen %d "
				 "has_cmap %d "
				 "type %d "
				 "cmap_start %d "
				 "cmap_n_colors %d "
				 "cmap_bpp %d "
				 "x %d y %d width %d height %d bpp %d "
				 "flags %#x",
				 ctx->hdr->infolen,
				 ctx->hdr->has_cmap,
				 ctx->hdr->type,
				 LE16(ctx->hdr->cmap_start),
				 LE16(ctx->hdr->cmap_n_colors),
				 ctx->hdr->cmap_bpp,
				 LE16(ctx->hdr->x_origin),
				 LE16(ctx->hdr->y_origin),
				 LE16(ctx->hdr->width),
				 LE16(ctx->hdr->height),
				 ctx->hdr->bpp,
				 ctx->hdr->flags);
#endif
			if (!ctx->in)
				return FALSE;
			if (LE16(ctx->hdr->width) == 0 || 
			    LE16(ctx->hdr->height) == 0) {
				g_set_error_literal(err, GDK_PIXBUF_ERROR,
                                                    GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                                    _("TGA image has invalid dimensions"));
				return FALSE;
			}
			if ((ctx->hdr->flags & TGA_INTERLEAVE_MASK) != TGA_INTERLEAVE_NONE) {
				g_set_error_literal(err, GDK_PIXBUF_ERROR, 
                                                    GDK_PIXBUF_ERROR_UNKNOWN_TYPE,
                                                    _("TGA image type not supported"));
				return FALSE;
			}
			switch (ctx->hdr->type) {
			    case TGA_TYPE_PSEUDOCOLOR:
			    case TGA_TYPE_RLE_PSEUDOCOLOR:
				    if (ctx->hdr->bpp != 8) {
					    g_set_error_literal(err, GDK_PIXBUF_ERROR, 
                                                                GDK_PIXBUF_ERROR_UNKNOWN_TYPE,
                                                                _("TGA image type not supported"));
					    return FALSE;
				    }
				    break;
			    case TGA_TYPE_TRUECOLOR:
			    case TGA_TYPE_RLE_TRUECOLOR:
				    if (ctx->hdr->bpp != 24 &&
					ctx->hdr->bpp != 32) {
					    g_set_error_literal(err, GDK_PIXBUF_ERROR, 
                                                                GDK_PIXBUF_ERROR_UNKNOWN_TYPE,
                                                                _("TGA image type not supported"));
					    return FALSE;
				    }			      
				    break;
			    case TGA_TYPE_GRAYSCALE:
			    case TGA_TYPE_RLE_GRAYSCALE:
				    if (ctx->hdr->bpp != 8 &&
					ctx->hdr->bpp != 16) {
					    g_set_error_literal(err, GDK_PIXBUF_ERROR, 
                                                                GDK_PIXBUF_ERROR_UNKNOWN_TYPE,
                                                                _("TGA image type not supported"));
					    return FALSE;
				    }
				    break;
			    default:
				    g_set_error_literal(err, GDK_PIXBUF_ERROR, 
                                                        GDK_PIXBUF_ERROR_UNKNOWN_TYPE,
                                                        _("TGA image type not supported"));
				    return FALSE;    
			}
			if (!fill_in_context(ctx, err))
				return FALSE;
		} else {
			return TRUE;
		}
	}
	if (!ctx->skipped_info) {
		if (ctx->in->size >= ctx->hdr->infolen) {
			ctx->in = io_buffer_free_segment(ctx->in, ctx->hdr->infolen, err);
			if (!ctx->in)
				return FALSE;
			ctx->skipped_info = TRUE;
		} else {
			return TRUE;
		}
	}
	if (!ctx->cmap) {
		if (ctx->in->size >= ctx->cmap_size) {
			if (!try_colormap(ctx, err))
				return FALSE;
		} else {
			return TRUE;
		}
	}
	if (!ctx->prepared) {
		if (ctx->pfunc)
			(*ctx->pfunc) (ctx->pbuf, NULL, ctx->udata);
		ctx->prepared = TRUE;
	}
	/* We shouldn't get here anyway. */
	return TRUE;
}

static gpointer gdk_pixbuf__tga_begin_load(GdkPixbufModuleSizeFunc f0,
                                           GdkPixbufModulePreparedFunc f1,
					   GdkPixbufModuleUpdatedFunc f2,
					   gpointer udata, GError **err)
{
	TGAContext *ctx;

	ctx = g_try_malloc(sizeof(TGAContext));
	if (!ctx) {
		g_set_error_literal(err, GDK_PIXBUF_ERROR, 
                                    GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                    _("Cannot allocate memory for TGA context struct"));
		return NULL;
	}

	ctx->hdr = NULL;
	ctx->rowstride = 0;
	ctx->run_length_encoded = FALSE;

	ctx->cmap = NULL;
	ctx->cmap_size = 0;

	ctx->pbuf = NULL;
	ctx->pbuf_bytes = 0;
	ctx->pbuf_bytes_done = 0;
	ctx->pptr = NULL;

	ctx->in = io_buffer_new(err);
	if (!ctx->in) {
		g_free(ctx);
		return NULL;
	}

	ctx->skipped_info = FALSE;
	ctx->prepared = FALSE;
	ctx->done = FALSE;

	ctx->sfunc = f0;
	ctx->pfunc = f1;
	ctx->ufunc = f2;
	ctx->udata = udata;

	return ctx;
}

static gboolean gdk_pixbuf__tga_load_increment(gpointer data,
					       const guchar *buffer,
					       guint size,
					       GError **err)
{
	TGAContext *ctx = (TGAContext*) data;
	g_return_val_if_fail(ctx != NULL, FALSE);

	if (ctx->done)
		return TRUE;

	g_return_val_if_fail(buffer != NULL, TRUE);
	ctx->in = io_buffer_append(ctx->in, buffer, size, err);
	if (!ctx->in)
		return FALSE;
	if (!ctx->prepared) {
		if (!try_preload(ctx, err))
			return FALSE;
		if (!ctx->prepared)
			return TRUE;
		if (ctx->in->size == 0)
			return TRUE;
	}

	if (ctx->run_length_encoded) {
		if (!parse_rle_data(ctx, err))
			return FALSE;
	} else {
		while (ctx->in->size >= ctx->rowstride) {
			if (ctx->completed_lines >= ctx->pbuf->height) {
				g_set_error_literal(err, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_FAILED,
                                                    _("Excess data in file"));
				return FALSE;
			}
			if (!parse_data_for_row(ctx, err))
				return FALSE;
			ctx->completed_lines++;
		}
	}

	return TRUE;
}

static gboolean gdk_pixbuf__tga_stop_load(gpointer data, GError **err)
{
	TGAContext *ctx = (TGAContext *) data;
	g_return_val_if_fail(ctx != NULL, FALSE);

	if (ctx->hdr &&
            (ctx->hdr->flags & TGA_ORIGIN_UPPER) == 0 && 
            ctx->run_length_encoded && 
            ctx->pbuf) {
		pixbuf_flip_vertically (ctx->pbuf);
		if (ctx->ufunc)
			(*ctx->ufunc) (ctx->pbuf, 0, 0,
				       ctx->pbuf->width, ctx->pbuf->height,
			       	       ctx->udata);
	}
	g_free (ctx->hdr);
	if (ctx->cmap)
          colormap_free (ctx->cmap);
	if (ctx->pbuf)
		g_object_unref (ctx->pbuf);
	if (ctx->in && ctx->in->size)
		ctx->in = io_buffer_free_segment (ctx->in, ctx->in->size, err);
	if (!ctx->in) {
		g_free (ctx);
		return FALSE;
	}
	io_buffer_free (ctx->in);
	g_free (ctx);
	return TRUE;
}

#ifndef INCLUDE_tga
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__tga_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule *module)
{
	module->begin_load = gdk_pixbuf__tga_begin_load;
	module->stop_load = gdk_pixbuf__tga_stop_load;
	module->load_increment = gdk_pixbuf__tga_load_increment;
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat *info)
{
	static const GdkPixbufModulePattern signature[] = {
		{ " \x1\x1", "x  ", 100 },
		{ " \x1\x9", "x  ", 100 },
		{ "  \x2", "xz ",  99 }, /* only 99 since .CUR also matches this */
		{ "  \x3", "xz ", 100 },
		{ "  \xa", "xz ", 100 },
		{ "  \xb", "xz ", 100 },
		{ NULL, NULL, 0 }
	};
	static const gchar *mime_types[] = {
		"image/x-tga",
		NULL
	};
	static const gchar *extensions[] = {
		"tga",
		"targa",
		NULL
	};

	info->name = "tga";
	info->signature = (GdkPixbufModulePattern *) signature;
	info->description = NC_("image format", "Targa");
	info->mime_types = (gchar **) mime_types;
	info->extensions = (gchar **) extensions;
	info->flags = GDK_PIXBUF_FORMAT_THREADSAFE;
	info->license = "LGPL";
}
