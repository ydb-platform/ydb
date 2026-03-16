/* -*- mode: C; c-file-style: "linux" -*- */
/* GdkPixbuf library - Windows Bitmap image loader
 *
 * Copyright (C) 1999 The Free Software Foundation
 *
 * Authors: Arjan van de Ven <arjan@fenrus.demon.nl>
 *          Federico Mena-Quintero <federico@gimp.org>
 *
 * Based on io-ras.c
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#include <contrib/restricted/glib/config.h>
#include <stdio.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <string.h>
#include "gdk-pixbuf-private.h"

#define DUMPBIH 0



#if 0
/* If these structures were unpacked, they would define the two headers of the
 * BMP file.  After them comes the palette, and then the image data.
 *
 * We do not use these structures; we just keep them here for reference.
 */
struct BitmapFileHeader {
	guint16 magic;
	guint32 file_size;
	guint32 reserved;
	guint32 data_offset;
};

struct BitmapInfoHeader {
	guint32 header_size;
	guint32 width;
	guint32 height;
	guint16 planes;
	guint16 bpp;
	guint32 compression;
	guint32 data_size;
	guint32 x_ppm;
	guint32 y_ppm;
	guint32 n_colors;
	guint32 n_important_colors;
};
#endif

/* Compression values */

#define BI_RGB 0
#define BI_RLE8 1
#define BI_RLE4 2
#define BI_BITFIELDS 3

/* State machine */
typedef enum {
	READ_STATE_HEADERS,	/* Reading the bitmap file header and bitmap info header */
	READ_STATE_PALETTE,	/* Reading the palette */
	READ_STATE_BITMASKS,	/* Reading the bitmasks for BI_BITFIELDS */
	READ_STATE_DATA,	/* Reading the actual image data */
	READ_STATE_ERROR,	/* An error occurred; further data will be ignored */
	READ_STATE_DONE		/* Done reading the image; further data will be ignored */
} ReadState;

/*

DumpBIH printf's the values in a BitmapInfoHeader to the screen, for
debugging purposes.

*/
#if DUMPBIH
static void DumpBIH(unsigned char *BIH)
{
	printf("biSize      = %i \n",
	       (int) (BIH[3] << 24) + (BIH[2] << 16) + (BIH[1] << 8) +
	       (BIH[0]));
	printf("biWidth     = %i \n",
	       (int) (BIH[7] << 24) + (BIH[6] << 16) + (BIH[5] << 8) +
	       (BIH[4]));
	printf("biHeight    = %i \n",
	       (int) (BIH[11] << 24) + (BIH[10] << 16) + (BIH[9] << 8) +
	       (BIH[8]));
	printf("biPlanes    = %i \n", (int) (BIH[13] << 8) + (BIH[12]));
	printf("biBitCount  = %i \n", (int) (BIH[15] << 8) + (BIH[14]));
	printf("biCompress  = %i \n",
	       (int) (BIH[19] << 24) + (BIH[18] << 16) + (BIH[17] << 8) +
	       (BIH[16]));
	printf("biSizeImage = %i \n",
	       (int) (BIH[23] << 24) + (BIH[22] << 16) + (BIH[21] << 8) +
	       (BIH[20]));
	printf("biXPels     = %i \n",
	       (int) (BIH[27] << 24) + (BIH[26] << 16) + (BIH[25] << 8) +
	       (BIH[24]));
	printf("biYPels     = %i \n",
	       (int) (BIH[31] << 24) + (BIH[30] << 16) + (BIH[29] << 8) +
	       (BIH[28]));
	printf("biClrUsed   = %i \n",
	       (int) (BIH[35] << 24) + (BIH[34] << 16) + (BIH[33] << 8) +
	       (BIH[32]));
	printf("biClrImprtnt= %i \n",
	       (int) (BIH[39] << 24) + (BIH[38] << 16) + (BIH[37] << 8) +
	       (BIH[36]));
}
#endif
/* struct headerpair contains the decoded width/height/depth info for
   the current bitmap */

struct headerpair {
	guint32 size;
	gint32 width;
	gint32 height;
	guint depth;
	guint Negative;		/* Negative = 1 -> top down BMP,
				   Negative = 0 -> bottom up BMP */
	guint  n_colors;
};

/* Data needed for the "state" during decompression */
struct bmp_compression_state {
	gint phase;
	gint run;
	gint count;
	gint x, y;
	guchar *p;
};

/* Progressive loading */

struct bmp_progressive_state {
	GdkPixbufModuleSizeFunc size_func;
	GdkPixbufModulePreparedFunc prepared_func;
	GdkPixbufModuleUpdatedFunc updated_func;
	gpointer user_data;

	ReadState read_state;

	guint LineWidth;
	guint Lines;		/* # of finished lines */

	guchar *buff;
	guint BufferSize;
	guint BufferPadding;
	guint BufferDone;

	guchar (*Colormap)[3];

	gint Type;		/*
				   32 = RGB + alpha
				   24 = RGB
				   16 = RGB
				   4  = 4 bpp colormapped
				   8  = 8 bpp colormapped
				   1  = 1 bit bitonal
				 */
	guint Compressed;
	struct bmp_compression_state compr;


	struct headerpair Header;	/* Decoded (BE->CPU) header */

	/* Bit masks, shift amounts, and significant bits for BI_BITFIELDS coding */
	int r_mask, r_shift, r_bits;
	int g_mask, g_shift, g_bits;
	int b_mask, b_shift, b_bits;
	int a_mask, a_shift, a_bits;

	GdkPixbuf *pixbuf;	/* Our "target" */
};

static gpointer
gdk_pixbuf__bmp_image_begin_load(GdkPixbufModuleSizeFunc size_func,
                                 GdkPixbufModulePreparedFunc prepared_func,
				 GdkPixbufModuleUpdatedFunc updated_func,
                                 gpointer user_data,
                                 GError **error);

static gboolean gdk_pixbuf__bmp_image_stop_load(gpointer data, GError **error);
static gboolean gdk_pixbuf__bmp_image_load_increment(gpointer data,
                                                     const guchar * buf,
                                                     guint size,
                                                     GError **error);


/* Picks up a 32-bit little-endian integer starting at the specified location.
 * Does it by hand instead of dereferencing a simple (gint *) cast due to
 * alignment constraints many platforms.
 */
static int
lsb_32 (guchar *src)
{
	return src[0] | (src[1] << 8) | (src[2] << 16) | (src[3] << 24);
}

/* Same as above, but for 16-bit little-endian integers. */
static short
lsb_16 (guchar *src)
{
	return src[0] | (src[1] << 8);
}

static gboolean grow_buffer (struct bmp_progressive_state *State,
                             GError **error)
{
  guchar *tmp;

  if (State->BufferSize == 0) {
    g_set_error_literal (error,
                         GDK_PIXBUF_ERROR,
                         GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                         _("BMP image has bogus header data"));
    State->read_state = READ_STATE_ERROR;
    return FALSE;
  }

  tmp = g_try_realloc (State->buff, State->BufferSize);

  if (!tmp) {
    g_set_error_literal (error,
                         GDK_PIXBUF_ERROR,
                         GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                         _("Not enough memory to load bitmap image"));
    State->read_state = READ_STATE_ERROR;
    return FALSE;
  }

  State->buff = tmp;
  return TRUE;
}

static gboolean
decode_bitmasks (guchar *buf,
		 struct bmp_progressive_state *State, 
		 GError **error);

static gboolean DecodeHeader(unsigned char *BFH, unsigned char *BIH,
                             struct bmp_progressive_state *State,
                             GError **error)
{
	gint clrUsed;

	/* First check for the two first bytes content. A sane
	   BMP file must start with bytes 0x42 0x4D.  */
	if (*BFH != 0x42 || *(BFH + 1) != 0x4D) {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("BMP image has bogus header data"));
		State->read_state = READ_STATE_ERROR;
		return FALSE;
	}

        /* FIXME this is totally unrobust against bogus image data. */
	if (State->BufferSize < lsb_32 (&BIH[0]) + 14) {
		State->BufferSize = lsb_32 (&BIH[0]) + 14;
		if (!grow_buffer (State, error))
			return FALSE;
		return TRUE;
	}

#if DUMPBIH
	DumpBIH(BIH);
#endif    

	State->Header.size = lsb_32 (&BIH[0]);
	if (State->Header.size == 124) {
                /* BMP v5 */
		State->Header.width = lsb_32 (&BIH[4]);
		State->Header.height = lsb_32 (&BIH[8]);
		State->Header.depth = lsb_16 (&BIH[14]);
		State->Compressed = lsb_32 (&BIH[16]);
	} else if (State->Header.size == 108) {
                /* BMP v4 */
		State->Header.width = lsb_32 (&BIH[4]);
		State->Header.height = lsb_32 (&BIH[8]);
		State->Header.depth = lsb_16 (&BIH[14]);
		State->Compressed = lsb_32 (&BIH[16]);
	} else if (State->Header.size == 64) {
                /* BMP OS/2 v2 */
		State->Header.width = lsb_32 (&BIH[4]);
		State->Header.height = lsb_32 (&BIH[8]);
		State->Header.depth = lsb_16 (&BIH[14]);
		State->Compressed = lsb_32 (&BIH[16]);
	} else if (State->Header.size == 40) {
                /* BMP v3 */ 
		State->Header.width = lsb_32 (&BIH[4]);
		State->Header.height = lsb_32 (&BIH[8]);
		State->Header.depth = lsb_16 (&BIH[14]);
		State->Compressed = lsb_32 (&BIH[16]);
	} else if (State->Header.size == 12) {
                /* BMP OS/2 */
		State->Header.width = lsb_16 (&BIH[4]);
		State->Header.height = lsb_16 (&BIH[6]);
		State->Header.depth = lsb_16 (&BIH[10]);
		State->Compressed = BI_RGB;
	} else {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("BMP image has unsupported header size"));
		State->read_state = READ_STATE_ERROR;
		return FALSE;
	}

        if (State->Header.depth > 32)
          {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("BMP image has unsupported depth"));
		State->read_state = READ_STATE_ERROR;
		return FALSE;
          }

	if (State->Header.size == 12)
		clrUsed = 1 << State->Header.depth;
	else
		clrUsed = (int) (BIH[35] << 24) + (BIH[34] << 16) + (BIH[33] << 8) + (BIH[32]);

        if (clrUsed > (1 << State->Header.depth))
          {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("BMP image has oversize palette"));
		State->read_state = READ_STATE_ERROR;
		return FALSE;
          }

	if (clrUsed != 0)
		State->Header.n_colors = clrUsed;
	else
            State->Header.n_colors = (1 << State->Header.depth);

	State->Type = State->Header.depth;	/* This may be less trivial someday */

	/* Negative heights indicates bottom-down pixelorder */
	if (State->Header.height < 0) {
		State->Header.height = -State->Header.height;
		State->Header.Negative = 1;
	}

	if (State->Header.Negative && 
	    (State->Compressed != BI_RGB && State->Compressed != BI_BITFIELDS))
	{
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("Topdown BMP images cannot be compressed"));
		State->read_state = READ_STATE_ERROR;
		return FALSE;
	}

	if (State->Header.width <= 0 || State->Header.height == 0 ||
	    (State->Compressed == BI_RLE4 && State->Type != 4)    ||
	    (State->Compressed == BI_RLE8 && State->Type != 8)	  ||
	    (State->Compressed == BI_BITFIELDS && !(State->Type == 16 || State->Type == 32)) ||
	    (State->Compressed > BI_BITFIELDS)) {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("BMP image has bogus header data"));
		State->read_state = READ_STATE_ERROR;
		return FALSE;
	}

	if (State->Type == 32)
		State->LineWidth = State->Header.width * 4;
	else if (State->Type == 24)
		State->LineWidth = State->Header.width * 3;
	else if (State->Type == 16)
		State->LineWidth = State->Header.width * 2;
	else if (State->Type == 8)
		State->LineWidth = State->Header.width * 1;
	else if (State->Type == 4)
		State->LineWidth = (State->Header.width + 1) / 2;
	else if (State->Type == 1) {
		State->LineWidth = State->Header.width / 8;
		if ((State->Header.width & 7) != 0)
			State->LineWidth++;
	} else {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("BMP image has bogus header data"));
		State->read_state = READ_STATE_ERROR;
		return FALSE;
	}

	/* Pad to a 32 bit boundary */
	if (((State->LineWidth % 4) > 0)
	    && (State->Compressed == BI_RGB || State->Compressed == BI_BITFIELDS))
		State->LineWidth = (State->LineWidth / 4) * 4 + 4;

	if (State->pixbuf == NULL) {
		if (State->size_func) {
			gint width = State->Header.width;
			gint height = State->Header.height;

			(*State->size_func) (&width, &height, State->user_data);
			if (width == 0 || height == 0) {
				State->read_state = READ_STATE_DONE;
				State->BufferSize = 0;
				return TRUE;
			}
		}

		if (State->Type == 32 || 
		    State->Compressed == BI_RLE4 || 
		    State->Compressed == BI_RLE8)
			State->pixbuf =
				gdk_pixbuf_new(GDK_COLORSPACE_RGB, TRUE, 8,
					       (gint) State->Header.width,
					       (gint) State->Header.height);
		else
			State->pixbuf =
				gdk_pixbuf_new(GDK_COLORSPACE_RGB, FALSE, 8,
					       (gint) State->Header.width,
					       (gint) State->Header.height);
		
		if (State->pixbuf == NULL) {
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Not enough memory to load bitmap image"));
			State->read_state = READ_STATE_ERROR;
			return FALSE;
			}
		
		if (State->prepared_func != NULL)
			/* Notify the client that we are ready to go */
			(*State->prepared_func) (State->pixbuf, NULL, State->user_data);
		
		/* make all pixels initially transparent */
		if (State->Compressed == BI_RLE4 || State->Compressed == BI_RLE8) {
			memset (State->pixbuf->pixels, 0, State->pixbuf->rowstride * State->Header.height);
			State->compr.p = State->pixbuf->pixels 
				+ State->pixbuf->rowstride * (State->Header.height- 1);
		}
	}
	
	State->BufferDone = 0;
	if (State->Type <= 8) {
                gint samples;

		State->read_state = READ_STATE_PALETTE;

		/* Allocate enough to hold the palette */
	        samples = (State->Header.size == 12 ? 3 : 4);
		State->BufferSize = State->Header.n_colors * samples;

		/* Skip over everything between the palette and the data.
		   This protects us against a malicious BFH[10] value.
                */
		State->BufferPadding = (lsb_32 (&BFH[10]) - 14 - State->Header.size) - State->BufferSize;

	} else if (State->Compressed == BI_RGB) {
		if (State->BufferSize < lsb_32 (&BFH[10]))
		{
			/* skip over padding between headers and image data */
			State->read_state = READ_STATE_HEADERS;
			State->BufferDone = State->BufferSize;
			State->BufferSize = lsb_32 (&BFH[10]);
		}
		else
		{
			State->read_state = READ_STATE_DATA;
			State->BufferSize = State->LineWidth;
		}
	} else if (State->Compressed == BI_BITFIELDS) {
               if (State->Header.size == 108 || State->Header.size == 124) 
               {
			/* v4 and v5 have the bitmasks in the header */
			if (!decode_bitmasks (&BIH[40], State, error)) {
			       State->read_state = READ_STATE_ERROR;
			       return FALSE;
                        }
               }
               else 
               {
		       State->read_state = READ_STATE_BITMASKS;
		       State->BufferSize = 12;
               }
	} else {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("BMP image has bogus header data"));
		State->read_state = READ_STATE_ERROR;
		return FALSE;
	}

	if (!grow_buffer (State, error)) 
		return FALSE;

        return TRUE;
}

static gboolean DecodeColormap (guchar *buff,
				struct bmp_progressive_state *State,
				GError **error)
{
	gint i;
	gint samples;

	g_assert (State->read_state == READ_STATE_PALETTE);

	samples = (State->Header.size == 12 ? 3 : 4);
	if (State->BufferSize < State->Header.n_colors * samples) {
		State->BufferSize = State->Header.n_colors * samples;
		if (!grow_buffer (State, error))
			return FALSE;
		return TRUE;
	}

	State->Colormap = g_malloc0 ((1 << State->Header.depth) * sizeof (*State->Colormap));
	for (i = 0; i < State->Header.n_colors; i++)

	{
		State->Colormap[i][0] = buff[i * samples];
		State->Colormap[i][1] = buff[i * samples + 1];
		State->Colormap[i][2] = buff[i * samples + 2];
#ifdef DUMPCMAP
		g_print ("color %d %x %x %x\n", i,
			 State->Colormap[i][0],
			 State->Colormap[i][1],
			 State->Colormap[i][2]);
#endif
	}

	State->read_state = READ_STATE_DATA;

	State->BufferDone = 0;
	if (!(State->Compressed == BI_RGB || State->Compressed == BI_BITFIELDS))
		State->BufferSize = 2;
	else
		State->BufferSize = State->LineWidth;
	
	if (!grow_buffer (State, error))
		return FALSE;

	return TRUE;
}

/* Finds the lowest set bit and the number of set bits */
static void
find_bits (int n, int *lowest, int *n_set)
{
	int i;

	*n_set = 0;

	for (i = 31; i >= 0; i--)
		if (n & (1 << i)) {
			*lowest = i;
			(*n_set)++;
		}
}

/* Decodes the bitmasks for BI_BITFIELDS coding */
static gboolean
decode_bitmasks (guchar *buf,
		 struct bmp_progressive_state *State, 
		 GError **error)
{
        State->a_mask = State->a_shift = State->a_bits = 0;
	State->r_mask = buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24);
	buf += 4;

	State->g_mask = buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24);
	buf += 4;

	State->b_mask = buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24);

	find_bits (State->r_mask, &State->r_shift, &State->r_bits);
	find_bits (State->g_mask, &State->g_shift, &State->g_bits);
	find_bits (State->b_mask, &State->b_shift, &State->b_bits);

        /* v4 and v5 have an alpha mask */
        if (State->Header.size == 108 || State->Header.size == 124) {
	      buf += 4;
	      State->a_mask = buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24);
	      find_bits (State->a_mask, &State->a_shift, &State->a_bits);
        }

	if (State->r_bits == 0 || State->g_bits == 0 || State->b_bits == 0) {
                if (State->Type == 16) {
		       State->r_mask = 0x7c00;
		       State->r_shift = 10;
		       State->g_mask = 0x03e0;
		       State->g_shift = 5;
		       State->b_mask = 0x001f;
		       State->b_shift = 0;

		       State->r_bits = State->g_bits = State->b_bits = 5;
                }
                else {
		       State->r_mask = 0x00ff0000;
		       State->r_shift = 16;
		       State->g_mask = 0x0000ff00;
		       State->g_shift = 8;
		       State->b_mask = 0x000000ff;
		       State->b_shift = 0;
		       State->a_mask = 0xff000000;
		       State->a_shift = 24;

		       State->r_bits = State->g_bits = State->b_bits = State->a_bits = 8;
                }
	}

        if (State->r_bits > 8) {
          State->r_shift += State->r_bits - 8;
          State->r_bits = 8;
        }
        if (State->g_bits > 8) {
          State->g_shift += State->g_bits - 8;
          State->g_bits = 8;
        }
        if (State->b_bits > 8) {
          State->b_shift += State->b_bits - 8;
          State->b_bits = 8;
        }
        if (State->a_bits > 8) {
          State->a_shift += State->a_bits - 8;
          State->a_bits = 8;
        }

	State->read_state = READ_STATE_DATA;
	State->BufferDone = 0;
	State->BufferSize = State->LineWidth;
	if (!grow_buffer (State, error)) 
		return FALSE;

	return TRUE;
}

/*
 * func - called when we have pixmap created (but no image data)
 * user_data - passed as arg 1 to func
 * return context (opaque to user)
 */

static gpointer
gdk_pixbuf__bmp_image_begin_load(GdkPixbufModuleSizeFunc size_func,
                                 GdkPixbufModulePreparedFunc prepared_func,
				 GdkPixbufModuleUpdatedFunc updated_func,
                                 gpointer user_data,
                                 GError **error)
{
	struct bmp_progressive_state *context;
	
	context = g_new0(struct bmp_progressive_state, 1);
	context->size_func = size_func;
	context->prepared_func = prepared_func;
	context->updated_func = updated_func;
	context->user_data = user_data;

	context->read_state = READ_STATE_HEADERS;

	context->BufferSize = 26;
	context->BufferPadding = 0;
	context->buff = g_malloc(26);
	context->BufferDone = 0;
	/* 14 for the BitmapFileHeader, 12 for the BitmapImageHeader */

	context->Colormap = NULL;

	context->Lines = 0;

	context->Type = 0;

	memset(&context->Header, 0, sizeof(struct headerpair));
	memset(&context->compr, 0, sizeof(struct bmp_compression_state));


	context->pixbuf = NULL;
	
	return (gpointer) context;
}

/*
 * context - returned from image_begin_load
 *
 * free context, unref gdk_pixbuf
 */
static gboolean gdk_pixbuf__bmp_image_stop_load(gpointer data, GError **error)
{
	gboolean retval = TRUE;
	
	struct bmp_progressive_state *context =
	    (struct bmp_progressive_state *) data;

        /* FIXME this thing needs to report errors if
         * we have unused image data
         */
	
	g_return_val_if_fail(context != NULL, TRUE);

	g_free(context->Colormap);

	if (context->pixbuf)
		g_object_unref(context->pixbuf);

	if (context->read_state == READ_STATE_HEADERS) {
                if (error && *error == NULL) {
                        g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                             _("Premature end-of-file encountered"));
                }
		retval = FALSE;
	}
	
	g_free(context->buff);
	g_free(context);

        return retval;
}


/*
The OneLineXX functions are called when 1 line worth of data is present.
OneLine24 is the 24 bpp-version.
*/
static void OneLine32(struct bmp_progressive_state *context)
{
	int i;
	guchar *pixels;
	guchar *src;

	if (!context->Header.Negative)
		pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride * (context->Header.height - context->Lines - 1));
	else
		pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride * context->Lines);

	src = context->buff;

	if (context->Compressed == BI_BITFIELDS) {
		int r_lshift, r_rshift;
		int g_lshift, g_rshift;
		int b_lshift, b_rshift;
		int a_lshift, a_rshift;

		r_lshift = 8 - context->r_bits;
		g_lshift = 8 - context->g_bits;
		b_lshift = 8 - context->b_bits;
		a_lshift = 8 - context->a_bits;

		r_rshift = context->r_bits - r_lshift;
		g_rshift = context->g_bits - g_lshift;
		b_rshift = context->b_bits - b_lshift;
		a_rshift = context->a_bits - a_lshift;

		for (i = 0; i < context->Header.width; i++) {
			unsigned int v, r, g, b, a;

			v = src[0] | (src[1] << 8) | (src[2] << 16) | (src[3] << 24);

			r = (v & context->r_mask) >> context->r_shift;
			g = (v & context->g_mask) >> context->g_shift;
			b = (v & context->b_mask) >> context->b_shift;
			a = (v & context->a_mask) >> context->a_shift;

			*pixels++ = (r << r_lshift) | (r >> r_rshift);
			*pixels++ = (g << g_lshift) | (g >> g_rshift);
			*pixels++ = (b << b_lshift) | (b >> b_rshift);
                        if (context->a_bits)
			  *pixels++ = (a << a_lshift) | (a >> a_rshift);
                        else
                          *pixels++ = 0xff;

			src += 4;
		}
	} else
		for (i = 0; i < context->Header.width; i++) {
			*pixels++ = src[2];
			*pixels++ = src[1];
			*pixels++ = src[0];
			*pixels++ = 0xff;

			src += 4;
		}
}

static void OneLine24(struct bmp_progressive_state *context)
{
	gint X;
	guchar *Pixels;

	X = 0;
	if (context->Header.Negative == 0)
		Pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride *
			  (context->Header.height - context->Lines - 1));
	else
		Pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride *
			  context->Lines);
	while (X < context->Header.width) {
		Pixels[X * 3 + 0] = context->buff[X * 3 + 2];
		Pixels[X * 3 + 1] = context->buff[X * 3 + 1];
		Pixels[X * 3 + 2] = context->buff[X * 3 + 0];
		X++;
	}

}

static void OneLine16(struct bmp_progressive_state *context)
{
	int i;
	guchar *pixels;
	guchar *src;

	if (!context->Header.Negative)
		pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride * (context->Header.height - context->Lines - 1));
	else
		pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride * context->Lines);

	src = context->buff;

	if (context->Compressed == BI_BITFIELDS) {
		int r_lshift, r_rshift;
		int g_lshift, g_rshift;
		int b_lshift, b_rshift;

		r_lshift = 8 - context->r_bits;
		g_lshift = 8 - context->g_bits;
		b_lshift = 8 - context->b_bits;

		r_rshift = context->r_bits - r_lshift;
		g_rshift = context->g_bits - g_lshift;
		b_rshift = context->b_bits - b_lshift;

		for (i = 0; i < context->Header.width; i++) {
			int v, r, g, b;

			v = (int) src[0] | ((int) src[1] << 8);

			r = (v & context->r_mask) >> context->r_shift;
			g = (v & context->g_mask) >> context->g_shift;
			b = (v & context->b_mask) >> context->b_shift;

			*pixels++ = (r << r_lshift) | (r >> r_rshift);
			*pixels++ = (g << g_lshift) | (g >> g_rshift);
			*pixels++ = (b << b_lshift) | (b >> b_rshift);

			src += 2;
		}
	} else
		for (i = 0; i < context->Header.width; i++) {
			int v, r, g, b;

			v = src[0] | (src[1] << 8);

			r = (v >> 10) & 0x1f;
			g = (v >> 5) & 0x1f;
			b = v & 0x1f;

			*pixels++ = (r << 3) | (r >> 2);
			*pixels++ = (g << 3) | (g >> 2);
			*pixels++ = (b << 3) | (b >> 2);

			src += 2;
		}
}

static void OneLine8(struct bmp_progressive_state *context)
{
	gint X;
	guchar *Pixels;

	X = 0;
	if (context->Header.Negative == 0)
		Pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride *
			  (context->Header.height - context->Lines - 1));
	else
		Pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride *
			  context->Lines);
	while (X < context->Header.width) {
		Pixels[X * 3 + 0] =
		    context->Colormap[context->buff[X]][2];
		Pixels[X * 3 + 1] =
		    context->Colormap[context->buff[X]][1];
		Pixels[X * 3 + 2] =
		    context->Colormap[context->buff[X]][0];
		X++;
	}
}

static void OneLine4(struct bmp_progressive_state *context)
{
	gint X;
	guchar *Pixels;

	X = 0;
	if (context->Header.Negative == 0)
		Pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride *
			  (context->Header.height - context->Lines - 1));
	else
		Pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride *
			  context->Lines);

	while (X < context->Header.width) {
		guchar Pix;

		Pix = context->buff[X / 2];

		Pixels[X * 3 + 0] =
		    context->Colormap[Pix >> 4][2];
		Pixels[X * 3 + 1] =
		    context->Colormap[Pix >> 4][1];
		Pixels[X * 3 + 2] =
		    context->Colormap[Pix >> 4][0];
		X++;
		if (X < context->Header.width) {
			/* Handle the other 4 bit pixel only when there is one */
			Pixels[X * 3 + 0] =
			    context->Colormap[Pix & 15][2];
			Pixels[X * 3 + 1] =
			    context->Colormap[Pix & 15][1];
			Pixels[X * 3 + 2] =
			    context->Colormap[Pix & 15][0];
			X++;
		}
	}

}

static void OneLine1(struct bmp_progressive_state *context)
{
	gint X;
	guchar *Pixels;

	X = 0;
	if (context->Header.Negative == 0)
		Pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride *
			  (context->Header.height - context->Lines - 1));
	else
		Pixels = (context->pixbuf->pixels +
			  context->pixbuf->rowstride *
			  context->Lines);
	while (X < context->Header.width) {
		gint Bit;

		Bit = (context->buff[X / 8]) >> (7 - (X & 7));
		Bit = Bit & 1;
		Pixels[X * 3 + 0] = context->Colormap[Bit][2];
		Pixels[X * 3 + 1] = context->Colormap[Bit][1];
		Pixels[X * 3 + 2] = context->Colormap[Bit][0];
		X++;
	}
}


static void OneLine(struct bmp_progressive_state *context)
{
	context->BufferDone = 0;
	if (context->Lines >= context->Header.height)
		return;

	if (context->Type == 32)
		OneLine32(context);
	else if (context->Type == 24)
		OneLine24(context);
	else if (context->Type == 16)
		OneLine16(context);
	else if (context->Type == 8)
		OneLine8(context);
	else if (context->Type == 4)
		OneLine4(context);
	else if (context->Type == 1)
		OneLine1(context);
	else
		g_assert_not_reached ();

	context->Lines++;

	if (context->updated_func != NULL) {
		(*context->updated_func) (context->pixbuf,
					  0,
					  (context->Header.Negative ?
					   (context->Lines - 1) :
					   (context->Header.height - context->Lines)),
					  context->Header.width,
					  1,
					  context->user_data);

	}
}

#define NEUTRAL       0
#define ENCODED       1
#define ESCAPE        2   
#define DELTA_X       3
#define DELTA_Y       4
#define ABSOLUTE      5
#define SKIP          6

#define END_OF_LINE   0
#define END_OF_BITMAP 1
#define DELTA         2

static gboolean 
DoCompressed(struct bmp_progressive_state *context, GError **error)
{
	gint i, j;
	gint y;
	guchar c;
	gint idx;

	/* context->compr.y might be past the last line because we are
	 * on padding past the end of a valid data, or we might have hit
	 * out-of-bounds data. Either way we just eat-and-ignore the
	 * rest of the file. Doing the check only here and not when
	 * we change y below is fine since BufferSize is always 2 here
	 * and the BMP file format always starts new data on 16-bit
	 * boundaries.
	 */
	if (context->compr.y >= context->Header.height) {
		context->BufferDone = 0;
		return TRUE;
	}

	y = context->compr.y;

 	for (i = 0; i < context->BufferSize; i++) {
		c = context->buff[i];
		switch (context->compr.phase) {
		    case NEUTRAL:
			    if (c) {
				    context->compr.run = c;
				    context->compr.phase = ENCODED;
			    }
			    else
				    context->compr.phase = ESCAPE;
			    break;
		    case ENCODED:
			    for (j = 0; j < context->compr.run; j++) {
				    if (context->Compressed == BI_RLE8)
					    idx = c;
				    else if (j & 1) 
					    idx = c & 0x0f;
				    else 
					    idx = (c >> 4) & 0x0f;
				    if (context->compr.x < context->Header.width) {
					    *context->compr.p++ = context->Colormap[idx][2];
					    *context->compr.p++ = context->Colormap[idx][1];
					    *context->compr.p++ = context->Colormap[idx][0];
					    *context->compr.p++ = 0xff;
					    context->compr.x++;    
				    }
			    }
			    context->compr.phase = NEUTRAL;
			    break;
		    case ESCAPE:
			    switch (c) {
				case END_OF_LINE:
					context->compr.x = 0;
					context->compr.y++;
					context->compr.p = context->pixbuf->pixels 
						+ (context->pixbuf->rowstride * (context->Header.height - context->compr.y - 1))
						+ (4 * context->compr.x);
					context->compr.phase = NEUTRAL;
					break;
				case END_OF_BITMAP:
					context->compr.x = 0;
					context->compr.y = context->Header.height;
					context->compr.phase = NEUTRAL;
					break;
				case DELTA:
					context->compr.phase = DELTA_X;
					break;
				default:
					context->compr.run = c;
					context->compr.count = 0;
					context->compr.phase = ABSOLUTE;
					break;
			    }
			    break;
		    case DELTA_X:
			    context->compr.x += c;
			    context->compr.phase = DELTA_Y;
			    break;
		    case DELTA_Y:
			    context->compr.y += c;
			    context->compr.p = context->pixbuf->pixels 
				    + (context->pixbuf->rowstride * (context->Header.height - context->compr.y - 1))
				    + (4 * context->compr.x);
			    context->compr.phase = NEUTRAL;
			    break;
		    case ABSOLUTE:
			    if (context->Compressed == BI_RLE8) {
				    idx = c;
				    if (context->compr.x < context->Header.width) {
					    *context->compr.p++ = context->Colormap[idx][2];
					    *context->compr.p++ = context->Colormap[idx][1];
					    *context->compr.p++ = context->Colormap[idx][0];
					    *context->compr.p++ = 0xff;
					    context->compr.x++;    
				    }
				    context->compr.count++;

				    if (context->compr.count == context->compr.run) {
					    if (context->compr.run & 1)
						    context->compr.phase = SKIP;
					    else
						    context->compr.phase = NEUTRAL;
				    }
			    }
			    else {
				    for (j = 0; j < 2; j++) {
					    if (context->compr.count & 1)
						    idx = c & 0x0f;
					    else 
						    idx = (c >> 4) & 0x0f;
					    if (context->compr.x < context->Header.width) {
						    *context->compr.p++ = context->Colormap[idx][2];
						    *context->compr.p++ = context->Colormap[idx][1];
						    *context->compr.p++ = context->Colormap[idx][0];
						    *context->compr.p++ = 0xff;
						    context->compr.x++;    
					    }
					    context->compr.count++;

					    if (context->compr.count == context->compr.run) {
						    if ((context->compr.run & 3) == 1
							|| (context->compr.run & 3) == 2) 
							    context->compr.phase = SKIP;
						    else
							    context->compr.phase = NEUTRAL;
						    break;
					    }
				    }
			    }
			    break;
		    case SKIP:
			    context->compr.phase = NEUTRAL;
			    break;
		}
	}
	if (context->updated_func != NULL) {
		if (context->compr.y > y)
		{
			gint new_y = MIN (context->compr.y, context->Header.height);
			(*context->updated_func) (context->pixbuf,
						  0,
						  context->Header.height - new_y,
						  context->Header.width,
						  new_y - y,
						  context->user_data);
		}

	}

	context->BufferDone = 0;
	return TRUE;
}

/*
 * context - from image_begin_load
 * buf - new image data
 * size - length of new image data
 *
 * append image data onto incrementally built output image
 */
static gboolean
gdk_pixbuf__bmp_image_load_increment(gpointer data,
                                     const guchar * buf,
                                     guint size,
                                     GError **error)
{
	struct bmp_progressive_state *context =
	    (struct bmp_progressive_state *) data;

	gint BytesToCopy;
	gint BytesToRemove;

	if (context->read_state == READ_STATE_DONE)
		return TRUE;
	else if (context->read_state == READ_STATE_ERROR)
		return FALSE;

	while (size > 0) {
		if (context->BufferDone < context->BufferSize) {	/* We still
									   have headerbytes to do */
			BytesToCopy =
			    context->BufferSize - context->BufferDone;
			if (BytesToCopy > size)
				BytesToCopy = size;

			memmove(context->buff + context->BufferDone,
				buf, BytesToCopy);

			size -= BytesToCopy;
			buf += BytesToCopy;
			context->BufferDone += BytesToCopy;

			if (context->BufferDone != context->BufferSize)
				break;
		}

		/* context->buff is full.  Now we discard all "padding" */
		if (context->BufferPadding != 0) {
			BytesToRemove = context->BufferPadding - size;
			if (BytesToRemove > size) {
				BytesToRemove = size;
			}
			size -= BytesToRemove;
			context->BufferPadding -= BytesToRemove;

			if (context->BufferPadding != 0)
				break;
		}

		switch (context->read_state) {
		case READ_STATE_HEADERS:
			if (!DecodeHeader (context->buff,
					   context->buff + 14, context,
					   error))
				return FALSE;

			break;

		case READ_STATE_PALETTE:
			if (!DecodeColormap (context->buff, context, error))
				return FALSE;
			break;

		case READ_STATE_BITMASKS:
			if (!decode_bitmasks (context->buff, context, error))
				return FALSE;
			break;

		case READ_STATE_DATA:
			if (context->Compressed == BI_RGB || context->Compressed == BI_BITFIELDS)
				OneLine (context);
			else if (!DoCompressed (context, error))
				return FALSE;

			break;
		case READ_STATE_DONE:
			return TRUE;
			break;

		default:
			g_assert_not_reached ();
		}
	}

	return TRUE;
}

/* for our convenience when filling file header */
#define put16(buf,data)	{ guint16 x; \
			  x = GUINT16_TO_LE (data); \
			  memcpy(buf, &x, 2); \
			  buf += 2; }
#define put32(buf,data)	{ guint32 x; \
			  x = GUINT32_TO_LE (data); \
			  memcpy(buf, &x, 4); \
			  buf += 4; }

static gboolean
gdk_pixbuf__bmp_image_save_to_callback (GdkPixbufSaveFunc   save_func,
					gpointer            user_data,
					GdkPixbuf          *pixbuf, 
					gchar             **keys,
					gchar             **values,
					GError            **error)
{
	guint width, height, channel, size, stride, src_stride, x, y;
	guchar BFH_BIH[54], *pixels, *buf, *src, *dst, *dst_line;
	gboolean ret;

	width = gdk_pixbuf_get_width (pixbuf);
	height = gdk_pixbuf_get_height (pixbuf);
	channel = gdk_pixbuf_get_n_channels (pixbuf);
	pixels = gdk_pixbuf_get_pixels (pixbuf);
	src_stride = gdk_pixbuf_get_rowstride (pixbuf);
	stride = (width * 3 + 3) & ~3;
	size = stride * height;

	/* filling BFH */
	dst = BFH_BIH;
	*dst++ = 'B';			/* bfType */
	*dst++ = 'M';
	put32 (dst, size + 14 + 40);	/* bfSize */
	put32 (dst, 0);			/* bfReserved1 + bfReserved2 */
	put32 (dst, 14 + 40);		/* bfOffBits */

	/* filling BIH */
	put32 (dst, 40);		/* biSize */
	put32 (dst, width);		/* biWidth */
	put32 (dst, height);		/* biHeight */
	put16 (dst, 1);			/* biPlanes */
	put16 (dst, 24);		/* biBitCount */
	put32 (dst, BI_RGB);		/* biCompression */
	put32 (dst, size);		/* biSizeImage */
	put32 (dst, 0);			/* biXPelsPerMeter */
	put32 (dst, 0);			/* biYPelsPerMeter */
	put32 (dst, 0);			/* biClrUsed */
	put32 (dst, 0);			/* biClrImportant */

	if (!save_func ((gchar *)BFH_BIH, 14 + 40, error, user_data))
		return FALSE;

	dst_line = buf = g_try_malloc (size);
	if (!buf) {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                     _("Couldn't allocate memory for saving BMP file"));
		return FALSE;
	}

	/* saving as a bottom-up bmp */
	pixels += (height - 1) * src_stride;
	for (y = 0; y < height; ++y, pixels -= src_stride, dst_line += stride) {
		dst = dst_line;
		src = pixels;
		for (x = 0; x < width; ++x, dst += 3, src += channel) {
			dst[0] = src[2];
			dst[1] = src[1];
			dst[2] = src[0];
		}
	}
	ret = save_func ((gchar *)buf, size, error, user_data);
	g_free (buf);

	return ret;
}

static gboolean
save_to_file_cb (const gchar *buf,
		 gsize count,
		 GError **error,
		 gpointer data)
{
	gint bytes;
	
	while (count > 0) {
		bytes = fwrite (buf, sizeof (gchar), count, (FILE *) data);
		if (bytes <= 0)
			break;
		count -= bytes;
		buf += bytes;
	}

	if (count) {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_FAILED,
                                     _("Couldn't write to BMP file"));
		return FALSE;
	}
	
	return TRUE;
}

static gboolean
gdk_pixbuf__bmp_image_save (FILE          *f, 
                            GdkPixbuf     *pixbuf, 
                            gchar        **keys,
                            gchar        **values,
                            GError       **error)
{
	return gdk_pixbuf__bmp_image_save_to_callback (save_to_file_cb,
						       f, pixbuf, keys,
						       values, error);
}

#ifndef INCLUDE_bmp
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__bmp_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule *module)
{
	module->begin_load = gdk_pixbuf__bmp_image_begin_load;
	module->stop_load = gdk_pixbuf__bmp_image_stop_load;
	module->load_increment = gdk_pixbuf__bmp_image_load_increment;
	module->save = gdk_pixbuf__bmp_image_save;
	module->save_to_callback = gdk_pixbuf__bmp_image_save_to_callback;
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat *info)
{
	static const GdkPixbufModulePattern signature[] = {
		{ "BM", NULL, 100 },
		{ NULL, NULL, 0 }
	};
	static const gchar * mime_types[] = {
		"image/bmp",
		"image/x-bmp",
		"image/x-MS-bmp",
		NULL
	};
	static const gchar * extensions[] = {
		"bmp",
		NULL
	};

	info->name = "bmp";
	info->signature = (GdkPixbufModulePattern *) signature;
	info->description = NC_("image format", "BMP");
	info->mime_types = (gchar **) mime_types;
	info->extensions = (gchar **) extensions;
	info->flags = GDK_PIXBUF_FORMAT_WRITABLE | GDK_PIXBUF_FORMAT_THREADSAFE;
	info->license = "LGPL";
}

