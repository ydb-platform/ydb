/* -*- Mode: C; tab-width: 8; indent-tabs-mode: t; c-basic-offset: 8 -*- */
/* GdkPixbuf library - PNM image loader
 *
 * Copyright (C) 1999 Red Hat, Inc.
 *
 * Authors: Jeffrey Stedfast <fejj@helixcode.com>
 *          Michael Fulbright <drmike@redhat.com>
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
 */

#include <contrib/restricted/glib/config.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <setjmp.h>
#include "gdk-pixbuf-private.h"

#define PNM_BUF_SIZE 4096

#define PNM_FATAL_ERR  -1
#define PNM_SUSPEND     0
#define PNM_OK          1

typedef enum {
	PNM_FORMAT_PGM = 1,
	PNM_FORMAT_PGM_RAW,
	PNM_FORMAT_PPM,
	PNM_FORMAT_PPM_RAW,
	PNM_FORMAT_PBM,
	PNM_FORMAT_PBM_RAW
} PnmFormat;

typedef struct {
	guchar buffer[PNM_BUF_SIZE];
	guchar *byte;
	guint nbytes;
} PnmIOBuffer;

typedef struct {
	GdkPixbufModuleUpdatedFunc updated_func;
	GdkPixbufModulePreparedFunc prepared_func;
	GdkPixbufModuleSizeFunc size_func;
	gpointer user_data;
	
	GdkPixbuf *pixbuf;
	guchar *pixels;        /* incoming pixel data buffer */
	guchar *dptr;          /* current position in pixbuf */
	
	PnmIOBuffer inbuf;
	
	guint width;
	guint height;
	guint maxval;
	guint rowstride;
	PnmFormat type;
	
	guint output_row;      /* last row to be completed */
	guint output_col;
	gboolean did_prescan;  /* are we in image data yet? */
	gboolean got_header;   /* have we loaded pnm header? */
	
	guint scan_state;

	GError **error;
	
} PnmLoaderContext;

static GdkPixbuf   *gdk_pixbuf__pnm_image_load          (FILE *f, GError **error);
static gpointer    gdk_pixbuf__pnm_image_begin_load     (GdkPixbufModuleSizeFunc size_func, 
                                                         GdkPixbufModulePreparedFunc func, 
							 GdkPixbufModuleUpdatedFunc func2,
							 gpointer user_data,
							 GError **error);
static gboolean    gdk_pixbuf__pnm_image_stop_load      (gpointer context, GError **error);
static gboolean    gdk_pixbuf__pnm_image_load_increment (gpointer context,
							 const guchar *buf, guint size,
							 GError **error);

static void explode_bitmap_into_buf              (PnmLoaderContext *context);
static void explode_gray_into_buf                (PnmLoaderContext *context);


/* explode bitmap data into rgb components         */
/* we need to know what the row so we can          */
/* do sub-byte expansion (since 1 byte = 8 pixels) */
/* context->dptr MUST point at first byte in incoming data  */
/* which corresponds to first pixel of row y       */
static void
explode_bitmap_into_buf (PnmLoaderContext *context)
{
	gint j;
	guchar *from, *to, data;
	gint bit;
	guchar *dptr;
	gint wid, x;
	
	g_return_if_fail (context != NULL);
	g_return_if_fail (context->dptr != NULL);
	
	/* I'm no clever bit-hacker so I'm sure this can be optimized */
	dptr = context->dptr;
	wid  = context->width;
	
	from = dptr + ((wid - 1) / 8);
	to   = dptr + (wid - 1) * 3;
	bit  = 7 - ((wid-1) % 8);
	
	/* get first byte and align properly */
	data = from[0];
	for (j = 0; j < bit; j++, data >>= 1);
	
	for (x = wid-1; x >= 0; x--) {
/*		g_print ("%c",  (data & 1) ? '*' : ' '); */
		
		to[0] = to[1] = to[2] = (data & 0x01) ? 0x00 : 0xff;
		
		to -= 3;
		bit++;
		
		if (bit > 7 && x > 0) {
			from--;
			data = from[0];
			bit = 0;
		} else {
			data >>= 1;
		}
	}
	
/*	g_print ("\n"); */
}

/* explode gray image row into rgb components in pixbuf */
static void
explode_gray_into_buf (PnmLoaderContext *context)
{
	gint j;
	guchar *from, *to;
	guint w;
	
	g_return_if_fail (context != NULL);
	g_return_if_fail (context->dptr != NULL);
	
	/* Expand grey->colour.  Expand from the end of the
	 * memory down, so we can use the same buffer.
	 */
	w = context->width;
	from = context->dptr + w - 1;
	to = context->dptr + (w - 1) * 3;
	for (j = w - 1; j >= 0; j--) {
		to[0] = from[0];
		to[1] = from[0];
		to[2] = from[0];
		to -= 3;
		from--;
	}
}

/* skip over whitespace and comments in input buffer */
static gint
pnm_skip_whitespace (PnmIOBuffer *inbuf, GError **error)
{
	register guchar *inptr;
	guchar *inend;
	
	g_return_val_if_fail (inbuf != NULL, PNM_FATAL_ERR);
	g_return_val_if_fail (inbuf->byte != NULL, PNM_FATAL_ERR);
	
	inend = inbuf->byte + inbuf->nbytes;
	inptr = inbuf->byte;
	
	for ( ; inptr < inend; inptr++) {
		if (*inptr == '#') {
			/* in comment - skip to the end of this line */
			for ( ; *inptr != '\n' && inptr < inend; inptr++)
				;
			
			if ( inptr == inend || *inptr != '\n' ) {
				/* couldn't read whole comment */
				return PNM_SUSPEND;
			}
			
		} else if (!g_ascii_isspace (*inptr)) {
			inbuf->byte = inptr;
			inbuf->nbytes = (guint) (inend - inptr);
			return PNM_OK;
		}
	}
	
	inbuf->byte = inptr;
	inbuf->nbytes = (guint) (inend - inptr);
	
	return PNM_SUSPEND;
}

/* read next number from buffer */
static gint
pnm_read_next_value (PnmIOBuffer *inbuf, gint max_length, guint *value, GError **error)
{
	register guchar *inptr, *word, *p;
	guchar *inend, buf[129];
	gchar *endptr;
	gint retval;
	glong result;
	
	g_return_val_if_fail (inbuf != NULL, PNM_FATAL_ERR);
	g_return_val_if_fail (inbuf->byte != NULL, PNM_FATAL_ERR);
	g_return_val_if_fail (value != NULL, PNM_FATAL_ERR);
	
	if (max_length < 0)
		max_length = 128;

	/* skip white space */
	if ((retval = pnm_skip_whitespace (inbuf, error)) != PNM_OK)
		return retval;
	
	inend = inbuf->byte + inbuf->nbytes;
	inptr = inbuf->byte;
	
	/* copy this pnm 'word' into a temp buffer */
	for (p = inptr, word = buf; (p < inend) && !g_ascii_isspace (*p) && (*p != '#') && (p - inptr < max_length); p++, word++)
		*word = *p;
	*word = '\0';
	
	/* hmmm, there must be more data to this 'word' */
	if (p == inend || (!g_ascii_isspace (*p) && (*p != '#')  && (p - inptr < max_length)))
	    return PNM_SUSPEND;
	
	/* get the value */
	result = strtol ((gchar *)buf, &endptr, 10);
	if (*endptr != '\0' || result < 0 || result > G_MAXUINT) {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("PNM loader expected to find an integer, but didn't"));
		return PNM_FATAL_ERR;
	}
	*value = result;

	inbuf->byte = p;
	inbuf->nbytes = (guint) (inend - p);
	
	return PNM_OK;
}

/* returns PNM_OK, PNM_SUSPEND, or PNM_FATAL_ERR */
static gint
pnm_read_header (PnmLoaderContext *context)
{
	PnmIOBuffer *inbuf;
	gint retval;
	
	g_return_val_if_fail (context != NULL, PNM_FATAL_ERR);
	
	inbuf = &context->inbuf;
	
	if (!context->type) {
		/* file must start with a 'P' followed by a numeral  */
		/* so loop till we get enough data to determine type */
		if (inbuf->nbytes < 2)
			return PNM_SUSPEND;
		
		if (*inbuf->byte != 'P') {
			g_set_error_literal (context->error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("PNM file has an incorrect initial byte"));
			return PNM_FATAL_ERR;
		}
		
		inbuf->byte++;
		inbuf->nbytes--;
		
		switch (*inbuf->byte) {
		case '1':
			context->type = PNM_FORMAT_PBM;
			break;
		case '2':
			context->type = PNM_FORMAT_PGM;
			break;
		case '3':
			context->type = PNM_FORMAT_PPM;
			break;
		case '4':
			context->type = PNM_FORMAT_PBM_RAW;
			break;
		case '5':
			context->type = PNM_FORMAT_PGM_RAW;
			break;
		case '6':
			context->type = PNM_FORMAT_PPM_RAW;
			break;
		default:
			g_set_error_literal (context->error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("PNM file is not in a recognized PNM subformat"));
			return PNM_FATAL_ERR;
		}
		
		if (!inbuf->nbytes)
			return PNM_SUSPEND;
		
		inbuf->byte++;
		inbuf->nbytes--;
	}
	
	if (!context->width) {
		/* read the pixmap width */
		guint width = 0;
		
		retval = pnm_read_next_value (inbuf, -1, &width,
					      context->error);
		
		if (retval != PNM_OK) 
			return retval;
		
		if (!width) {
			g_set_error_literal (context->error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("PNM file has an image width of 0"));
			return PNM_FATAL_ERR;
		}
		
		context->width = width;
	}
	
	if (!context->height) {
		/* read the pixmap height */
		guint height = 0;
		
		retval = pnm_read_next_value (inbuf, -1, &height,
					      context->error);
		
		if (retval != PNM_OK)
			return retval;
		
		if (!height) {
			g_set_error_literal (context->error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("PNM file has an image height of 0"));
			return PNM_FATAL_ERR;
		}
		
		context->height = height;
	}
	
	switch (context->type) {
	case PNM_FORMAT_PPM:
	case PNM_FORMAT_PPM_RAW:
	case PNM_FORMAT_PGM:
	case PNM_FORMAT_PGM_RAW:
		if (!context->maxval) {
			retval = pnm_read_next_value (inbuf, -1, &context->maxval,
						      context->error);
			
			if (retval != PNM_OK)
				return retval;
			
			if (context->maxval == 0) {
				g_set_error_literal (context->error,
                                                     GDK_PIXBUF_ERROR,
                                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                                     _("Maximum color value in PNM file is 0"));
				return PNM_FATAL_ERR;
			}

			if (context->maxval > 65535) {
				g_set_error_literal (context->error,
                                                     GDK_PIXBUF_ERROR,
                                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                                     _("Maximum color value in PNM file is too large"));
				return PNM_FATAL_ERR;
			}

		}
		break;
	default:
		break;
	}
	
	return PNM_OK;
}

static gint
pnm_read_raw_scanline (PnmLoaderContext *context)
{
	PnmIOBuffer *inbuf;
	guint numbytes, offset;
	guint numpix = 0;
	guchar *dest;
	guint i;
	
	g_return_val_if_fail (context != NULL, PNM_FATAL_ERR);
	
	inbuf = &context->inbuf;
	
	switch (context->type) {
	case PNM_FORMAT_PBM_RAW:
		numpix = inbuf->nbytes * 8;
		break;
	case PNM_FORMAT_PGM_RAW:
		numpix = inbuf->nbytes;
		break;
	case PNM_FORMAT_PPM_RAW:
		numpix = inbuf->nbytes / 3;
		break;
	default:
		g_set_error_literal (context->error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("Raw PNM image type is invalid"));
		return PNM_FATAL_ERR;
	}
	if(context->maxval>255) 
		numpix/=2;
	
	numpix = MIN (numpix, context->width - context->output_col);
	
	if (!numpix)
		return PNM_SUSPEND;
	
	context->dptr = context->pixels + context->output_row * context->rowstride;
	
	switch (context->type) {
	case PNM_FORMAT_PBM_RAW:
		numbytes = (numpix / 8) + ((numpix % 8) ? 1 : 0);
		offset = context->output_col / 8;
		break;
	case PNM_FORMAT_PGM_RAW:
		numbytes = numpix;
		offset = context->output_col;
		break;
	case PNM_FORMAT_PPM_RAW:
		numbytes = numpix * 3;
		offset = context->output_col * 3;
		break;
	default:
		g_set_error_literal (context->error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("Raw PNM image type is invalid"));
		return PNM_FATAL_ERR;
	}
	if(context->maxval>255) 
		numbytes*=2;				
	
	switch (context->type) {
	case PNM_FORMAT_PBM_RAW:
		dest = context->dptr + offset;		
		memcpy (dest, inbuf->byte, numbytes);
		break;
	case PNM_FORMAT_PGM_RAW:
	case PNM_FORMAT_PPM_RAW:
		dest = context->dptr + offset;
		
		if (context->maxval == 255) {
			/* special-case optimization */
			memcpy (dest, inbuf->byte, numbytes);
		} else if(context->maxval == 65535) {
			/* optimized version of the next case */
			for(i=0; i < numbytes ; i+=2) {
				*dest++=inbuf->byte[i];
			}
		} else if(context->maxval > 255) {
			/* scale down to 256 colors */
			for(i=0; i < numbytes ; i+=2) {
				guint v=inbuf->byte[i]*256+inbuf->byte[i+1];
				*dest++=v*255/context->maxval;
			}
		} else {
			for (i = 0; i < numbytes; i++) {
				guchar *byte = inbuf->byte + i;
				
				/* scale the color to an 8-bit color depth */
				if (*byte > context->maxval)
					*dest++ = 255;
				else
					*dest++ = (guchar) (255 * *byte / context->maxval);
			}
		}
		break;
	default:
		g_set_error_literal (context->error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("Raw PNM image type is invalid"));
		return PNM_FATAL_ERR;
	}
	
	inbuf->byte += numbytes;
	inbuf->nbytes -= numbytes;
	
	context->output_col += numpix;
	if (context->output_col == context->width) {
		if (context->type == PNM_FORMAT_PBM_RAW)
			explode_bitmap_into_buf (context);
		else if (context->type == PNM_FORMAT_PGM_RAW)
			explode_gray_into_buf (context);
		
		context->output_col = 0;
		context->output_row++;
	} else {
		return PNM_SUSPEND;
	}
	
	return PNM_OK;
}

static gint
pnm_read_ascii_mono_scanline (PnmLoaderContext *context)
{
	PnmIOBuffer *inbuf;
	guint value;
	gint retval;
	guchar *dptr;
	gint max_length;

	if (context->type == PNM_FORMAT_PBM)
		max_length = 1;
	else
		max_length = -1;

	inbuf = &context->inbuf;

	context->dptr = context->pixels + context->output_row * context->rowstride;

	dptr = context->dptr + context->output_col * 3;

	while (TRUE) {
		retval = pnm_read_next_value (inbuf, max_length, &value, context->error);
		if (retval != PNM_OK)
			return retval;

		if (context->type == PNM_FORMAT_PBM) {
			value = value ? 0 : 0xff;
		}
		else {
			/* scale the color up or down to an 8-bit color depth */
			if (value > context->maxval)
				value = 255;
			else
				value = (guchar)(255 * value / context->maxval);
		}
			
		*dptr++ = value;
		*dptr++ = value;
		*dptr++ = value;

		context->output_col++;

		if (context->output_col == context->width) {
			context->output_col = 0;
			context->output_row++;
			break;
		}
	}

	return PNM_OK;
}

static gint
pnm_read_ascii_color_scanline (PnmLoaderContext *context)
{
	PnmIOBuffer *inbuf;
	guint value, i;
	guchar *dptr;
	gint retval;
	
	inbuf = &context->inbuf;
	
	context->dptr = context->pixels + context->output_row * context->rowstride;
	
	dptr = context->dptr + context->output_col * 3 + context->scan_state;
	
	while (TRUE) {
		for (i = context->scan_state; i < 3; i++) {
			retval = pnm_read_next_value (inbuf, -1, &value, context->error);
			if (retval != PNM_OK) {
				/* save state and return */
				context->scan_state = i;
				return retval;
			}
			
			if (value > context->maxval)
				*dptr++ = 255;
			else
				*dptr++ = (guchar)(255 * value / context->maxval);
		}
		
		context->scan_state = 0;
		context->output_col++;
		
		if (context->output_col == context->width) {
			context->output_col = 0;
			context->output_row++;
			break;
		}
	}
	
	return PNM_OK;
}

/* returns 1 if a scanline was converted, 0 means we ran out of data */
static gint
pnm_read_scanline (PnmLoaderContext *context)
{
	gint retval;
	
	g_return_val_if_fail (context != NULL, PNM_FATAL_ERR);
	
	/* read in image data */
	/* for raw formats this is trivial */
	switch (context->type) {
	case PNM_FORMAT_PBM_RAW:
	case PNM_FORMAT_PGM_RAW:
	case PNM_FORMAT_PPM_RAW:
		retval = pnm_read_raw_scanline (context);
		if (retval != PNM_OK)
			return retval;
		break;
	case PNM_FORMAT_PBM:
	case PNM_FORMAT_PGM:
		retval = pnm_read_ascii_mono_scanline (context);
		if (retval != PNM_OK)
			return retval;
		break;		
	case PNM_FORMAT_PPM:
		retval = pnm_read_ascii_color_scanline (context);
		if (retval != PNM_OK)
			return retval;
		break;
	default:
		g_set_error_literal (context->error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_UNKNOWN_TYPE,
                                     _("PNM image loader does not support this PNM subformat"));

		return PNM_FATAL_ERR;
	}
	
	return PNM_OK;
}

/* Shared library entry point */
static GdkPixbuf *
gdk_pixbuf__pnm_image_load (FILE *f, GError **error)
{
	PnmLoaderContext context;
	PnmIOBuffer *inbuf;
	gint nbytes;
	gint retval;
	
	/* pretend to be doing progressive loading */
	context.updated_func = NULL;
	context.prepared_func = NULL;
	context.user_data = NULL;
	context.type = 0;
	context.inbuf.nbytes = 0;
	context.inbuf.byte = NULL;
	context.width = 0;
	context.height = 0;
	context.maxval = 0;
	context.pixels = NULL;
	context.pixbuf = NULL;
	context.got_header = FALSE;
	context.did_prescan = FALSE;
	context.scan_state = 0;
	context.error = error;
	
	inbuf = &context.inbuf;
	
	while (TRUE) {
		guint num_to_read;
		
		/* keep buffer as full as possible */
		num_to_read = PNM_BUF_SIZE - inbuf->nbytes;
		
		if (inbuf->byte != NULL && inbuf->nbytes > 0)
			memmove (inbuf->buffer, inbuf->byte, inbuf->nbytes);
		
		nbytes = fread (inbuf->buffer + inbuf->nbytes, 1, num_to_read, f);
		
		/* error checking */
		if (nbytes == 0) {
			/* we ran out of data? */
			if (context.pixbuf)
				g_object_unref (context.pixbuf);
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Premature end-of-file encountered"));
			return NULL;
		}
		
		inbuf->nbytes += nbytes;
		inbuf->byte = inbuf->buffer;
		
		/* get header if needed */
		if (!context.got_header) {
			retval = pnm_read_header (&context);
			if (retval == PNM_FATAL_ERR)
				return NULL;
			else if (retval == PNM_SUSPEND)
				continue;
			
			context.got_header = TRUE;
		}
		
		/* scan until we hit image data */
		if (!context.did_prescan) {
			switch (context.type) {
			case PNM_FORMAT_PBM_RAW:
			case PNM_FORMAT_PGM_RAW:
			case PNM_FORMAT_PPM_RAW:
				if (inbuf->nbytes <= 0)
					continue;
				/* raw formats require exactly one whitespace */
				if (!g_ascii_isspace(*(inbuf->byte))) 
					{
						g_set_error_literal (error,
                                                                     GDK_PIXBUF_ERROR,
                                                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                                                     _("Raw PNM formats require exactly one whitespace before sample data"));
						return NULL;
					}
				inbuf->nbytes--;
				inbuf->byte++;
				break;
			default:
				retval = pnm_skip_whitespace (inbuf,
							      context.error);
				if (retval == PNM_FATAL_ERR)
					return NULL;
				else if (retval == PNM_SUSPEND)
					continue;
				break;
			}
			context.did_prescan = TRUE;
			context.output_row = 0;
			context.output_col = 0;
			
			context.pixbuf = gdk_pixbuf_new (GDK_COLORSPACE_RGB, FALSE, 8,
							 context.width, context.height);
			
			if (!context.pixbuf) {
				/* Failed to allocate memory */
				g_set_error_literal (error,
                                                     GDK_PIXBUF_ERROR,
                                                     GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                                     _("Cannot allocate memory for loading PNM image"));
				return NULL;
			}

			context.rowstride = context.pixbuf->rowstride;
			context.pixels = context.pixbuf->pixels;
		}
		
		/* if we got here we're reading image data */
		while (context.output_row < context.height) {
			retval = pnm_read_scanline (&context);
			
			if (retval == PNM_SUSPEND) {
				break;
			} else if (retval == PNM_FATAL_ERR) {
				if (context.pixbuf)
					g_object_unref (context.pixbuf);

				return NULL;
			}
		}
		
		if (context.output_row < context.height)
			continue;
		else
			break;
	}
	
	return context.pixbuf;
}

/* 
 * func - called when we have pixmap created (but no image data)
 * user_data - passed as arg 1 to func
 * return context (opaque to user)
 */

static gpointer
gdk_pixbuf__pnm_image_begin_load (GdkPixbufModuleSizeFunc size_func, 
                                  GdkPixbufModulePreparedFunc prepared_func, 
				  GdkPixbufModuleUpdatedFunc  updated_func,
				  gpointer user_data,
				  GError **error)
{
	PnmLoaderContext *context;
	
	context = g_try_malloc (sizeof (PnmLoaderContext));
	if (!context) {
		g_set_error_literal (error, GDK_PIXBUF_ERROR, 
                                     GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                     _("Insufficient memory to load PNM context struct"));
		return NULL;
	}
	memset (context, 0, sizeof (PnmLoaderContext));
	context->size_func = size_func;
	context->prepared_func = prepared_func;
	context->updated_func  = updated_func;
	context->user_data = user_data;
	context->width = 0;
	context->height = 0;
	context->maxval = 0;
	context->pixbuf = NULL;
	context->pixels = NULL;
	context->got_header = FALSE;
	context->did_prescan = FALSE;
	context->scan_state = 0;
	
	context->inbuf.nbytes = 0;
	context->inbuf.byte  = NULL;

	context->error = error;
	
	return (gpointer) context;
}

/*
 * context - returned from image_begin_load
 *
 * free context, unref gdk_pixbuf
 */
static gboolean
gdk_pixbuf__pnm_image_stop_load (gpointer data,
				 GError **error)
{
	PnmLoaderContext *context = (PnmLoaderContext *) data;
	gboolean retval = TRUE;
	
	g_return_val_if_fail (context != NULL, TRUE);
	
	if (context->pixbuf)
		g_object_unref (context->pixbuf);

#if 0
	/* We should ignore trailing newlines and we can't
	   generally complain about trailing stuff at all, since 
	   pnm allows to put multiple images in a file
	*/
	if (context->inbuf.nbytes > 0) {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("Unexpected end of PNM image data"));
		retval = FALSE;
	}
#endif
	
	g_free (context);

	return retval;
}

/*
 * context - from image_begin_load
 * buf - new image data
 * size - length of new image data
 *
 * append image data onto inrecrementally built output image
 */
static gboolean
gdk_pixbuf__pnm_image_load_increment (gpointer data,
				      const guchar *buf, guint size,
				      GError **error)
{
	PnmLoaderContext *context = (PnmLoaderContext *)data;
	PnmIOBuffer *inbuf;
	const guchar *bufhd;
	guint num_left, spinguard;
	gint retval;
	
	g_return_val_if_fail (context != NULL, FALSE);
	g_return_val_if_fail (buf != NULL, FALSE);

	context->error = error;
	
	bufhd = buf;
	inbuf = &context->inbuf;
	
	num_left = size;
	spinguard = 0;
	while (TRUE) {
		guint num_to_copy;
		
		/* keep buffer as full as possible */
		num_to_copy = MIN (PNM_BUF_SIZE - inbuf->nbytes, num_left);
		
		if (num_to_copy == 0)
			spinguard++;
		
		if (spinguard > 1)
			return TRUE;
		
		if (inbuf->byte != NULL && inbuf->nbytes > 0)
			memmove (inbuf->buffer, inbuf->byte, inbuf->nbytes);
		
		memcpy (inbuf->buffer + inbuf->nbytes, bufhd, num_to_copy);
		bufhd += num_to_copy;
		inbuf->nbytes += num_to_copy;
		inbuf->byte = inbuf->buffer;
		num_left -= num_to_copy;
		
		/* ran out of data and we haven't exited main loop */
		if (inbuf->nbytes == 0)
			return TRUE;
		
		/* get header if needed */
		if (!context->got_header) {
			retval = pnm_read_header (context);
			
			if (retval == PNM_FATAL_ERR)
				return FALSE;
			else if (retval == PNM_SUSPEND)
				continue;
			
			context->got_header = TRUE;
		}

		if (context->size_func) {
			gint w = context->width;
			gint h = context->height;
			(*context->size_func) (&w, &h, context->user_data);
			
			if (w == 0 || h == 0) 
				return FALSE;
		}
		
		
		/* scan until we hit image data */
		if (!context->did_prescan) {
			switch (context->type) {
			case PNM_FORMAT_PBM_RAW:
			case PNM_FORMAT_PGM_RAW:
			case PNM_FORMAT_PPM_RAW:
				if (inbuf->nbytes <= 0)
					continue;
				/* raw formats require exactly one whitespace */
				if (!g_ascii_isspace(*(inbuf->byte)))
					{
						g_set_error_literal (error,
                                                                     GDK_PIXBUF_ERROR,
                                                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                                                     _("Raw PNM formats require exactly one whitespace before sample data"));
						return FALSE;
					}
				inbuf->nbytes--;
				inbuf->byte++;
				break;
			default:
				retval = pnm_skip_whitespace (inbuf,
							      context->error);
				if (retval == PNM_FATAL_ERR)
					return FALSE;
				else if (retval == PNM_SUSPEND)
					continue;
				break;
			}
			context->did_prescan = TRUE;
			context->output_row = 0;
			context->output_col = 0;
			
			context->pixbuf = gdk_pixbuf_new (GDK_COLORSPACE_RGB, 
							  FALSE,
							  8, 
							  context->width,
							  context->height);
			
			if (context->pixbuf == NULL) {
				g_set_error_literal (error,
                                                     GDK_PIXBUF_ERROR,
                                                     GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                                     _("Insufficient memory to load PNM file"));
				return FALSE;
			}
			
			context->pixels = context->pixbuf->pixels;
			context->rowstride = context->pixbuf->rowstride;
			
			/* Notify the client that we are ready to go */
			if (context->prepared_func)
				(* context->prepared_func) (context->pixbuf,
							    NULL,
							    context->user_data);
		}
		
		/* if we got here we're reading image data */
		while (context->output_row < context->height) {
			retval = pnm_read_scanline (context);
			
			if (retval == PNM_SUSPEND) {
				break;
			} else if (retval == PNM_FATAL_ERR) {
				return FALSE;
			} else if (retval == PNM_OK && context->updated_func) {	
				/* send updated signal */
				(* context->updated_func) (context->pixbuf,
							   0, 
							   context->output_row-1,
							   context->width, 
							   1,
							   context->user_data);
			}
		}
		
		if (context->output_row < context->height)
			continue;
		else
			break;
	}
	
	return TRUE;
}

#ifndef INCLUDE_pnm
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__pnm_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule *module)
{
	module->load = gdk_pixbuf__pnm_image_load;
	module->begin_load = gdk_pixbuf__pnm_image_begin_load;
	module->stop_load = gdk_pixbuf__pnm_image_stop_load;
	module->load_increment = gdk_pixbuf__pnm_image_load_increment;
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat *info)
{
	static const GdkPixbufModulePattern signature[] = {
		{ "P1", NULL, 100 },
		{ "P2", NULL, 100 },
		{ "P3", NULL, 100 },
		{ "P4", NULL, 100 },
		{ "P5", NULL, 100 },
		{ "P6", NULL, 100 },
		{ NULL, NULL, 0 }
	};
	static const gchar *mime_types[] = {
		"image/x-portable-anymap",
		"image/x-portable-bitmap",
		"image/x-portable-graymap",
		"image/x-portable-pixmap",
		NULL
	};
	static const gchar *extensions[] = {
		"pnm",
		"pbm",
		"pgm",
		"ppm",
		NULL
	};

	info->name = "pnm";
	info->signature = (GdkPixbufModulePattern *) signature;
	info->description = NC_("image format", "PNM/PBM/PGM/PPM");
	info->mime_types = (gchar **) mime_types;
	info->extensions = (gchar **) extensions;
	info->flags = GDK_PIXBUF_FORMAT_THREADSAFE;
	info->license = "LGPL";
}
