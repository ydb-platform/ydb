/* -*- mode: C; c-file-style: "linux" -*- */
/* GdkPixbuf library - Windows Icon/Cursor image loader
 *
 * Copyright (C) 1999 The Free Software Foundation
 *
 * Authors: Arjan van de Ven <arjan@fenrus.demon.nl>
 *          Federico Mena-Quintero <federico@gimp.org>
 *
 * Based on io-bmp.c
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

#undef DUMPBIH
/*

Icons are just like BMP's, except for the header.
 
Known bugs:
	* bi-tonal files aren't tested 

*/

#include <contrib/restricted/glib/config.h>
#include <stdio.h>
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#include <string.h>
#include <errno.h>
#include "gdk-pixbuf-private.h"



/* 

These structures are actually dummies. These are according to
the "Windows API reference guide volume II" as written by 
Borland International, but GCC fiddles with the alignment of
the internal members.

*/

struct BitmapFileHeader {
	gushort bfType;
	guint bfSize;
	guint reserverd;
	guint bfOffbits;
};

struct BitmapInfoHeader {
	guint biSize;
	guint biWidth;
	guint biHeight;
	gushort biPlanes;
	gushort biBitCount;
	guint biCompression;
	guint biSizeImage;
	guint biXPelsPerMeter;
	guint biYPelsPerMeter;
	guint biClrUsed;
	guint biClrImportant;
};

#ifdef DUMPBIH
/* 

DumpBIH printf's the values in a BitmapInfoHeader to the screen, for 
debugging purposes.

*/
static void DumpBIH(unsigned char *BIH)
{				
	printf("biSize      = %i \n",
	       (int)(BIH[3] << 24) + (BIH[2] << 16) + (BIH[1] << 8) + (BIH[0]));
	printf("biWidth     = %i \n",
	       (int)(BIH[7] << 24) + (BIH[6] << 16) + (BIH[5] << 8) + (BIH[4]));
	printf("biHeight    = %i \n",
	       (int)(BIH[11] << 24) + (BIH[10] << 16) + (BIH[9] << 8) +
	       (BIH[8]));
	printf("biPlanes    = %i \n", (int)(BIH[13] << 8) + (BIH[12]));
	printf("biBitCount  = %i \n", (int)(BIH[15] << 8) + (BIH[14]));
	printf("biCompress  = %i \n",
	       (int)(BIH[19] << 24) + (BIH[18] << 16) + (BIH[17] << 8) +
	       (BIH[16]));
	printf("biSizeImage = %i \n",
	       (int)(BIH[23] << 24) + (BIH[22] << 16) + (BIH[21] << 8) +
	       (BIH[20]));
	printf("biXPels     = %i \n",
	       (int)(BIH[27] << 24) + (BIH[26] << 16) + (BIH[25] << 8) +
	       (BIH[24]));
	printf("biYPels     = %i \n",
	       (int)(BIH[31] << 24) + (BIH[30] << 16) + (BIH[29] << 8) +
	       (BIH[28]));
	printf("biClrUsed   = %i \n",
	       (int)(BIH[35] << 24) + (BIH[34] << 16) + (BIH[33] << 8) +
	       (BIH[32]));
	printf("biClrImprtnt= %i \n",
	       (int)(BIH[39] << 24) + (BIH[38] << 16) + (BIH[37] << 8) +
	       (BIH[36]));
}
#endif

/* Progressive loading */
struct headerpair {
	gint width;
	gint height;
	guint depth;
	guint Negative;		/* Negative = 1 -> top down BMP,  
				   Negative = 0 -> bottom up BMP */
};

/* Score the various parts of the icon */
struct ico_direntry_data {
	gint ImageScore;
	gint DIBoffset;
	gint x_hot;
	gint y_hot;
};

struct ico_progressive_state {
	GdkPixbufModuleSizeFunc size_func;
	GdkPixbufModulePreparedFunc prepared_func;
	GdkPixbufModuleUpdatedFunc updated_func;
	gpointer user_data;

	gint HeaderSize;	/* The size of the header-part (incl colormap) */
	guchar *HeaderBuf;	/* The buffer for the header (incl colormap) */
	gint BytesInHeaderBuf;  /* The size of the allocated HeaderBuf */
	gint HeaderDone;	/* The nr of bytes actually in HeaderBuf */

	gint LineWidth;		/* The width of a line in bytes */
	guchar *LineBuf;	/* Buffer for 1 line */
	gint LineDone;		/* # of bytes in LineBuf */
	gint Lines;		/* # of finished lines */

	gint Type;		/*  
				   32 = RGBA
				   24 = RGB
				   16 = 555 RGB
				   8 = 8 bit colormapped
				   4 = 4 bpp colormapped
				   1  = 1 bit bitonal 
				 */
        gboolean cursor;
        gint x_hot;
        gint y_hot;

	struct headerpair Header;	/* Decoded (BE->CPU) header */
	GList *entries;
	gint			DIBoffset;

	GdkPixbuf *pixbuf;	/* Our "target" */
};

static gpointer
gdk_pixbuf__ico_image_begin_load(GdkPixbufModuleSizeFunc size_func,
                                 GdkPixbufModulePreparedFunc prepared_func,
				 GdkPixbufModuleUpdatedFunc updated_func,
				 gpointer user_data,
                                 GError **error);
static gboolean gdk_pixbuf__ico_image_stop_load(gpointer data, GError **error);
static gboolean gdk_pixbuf__ico_image_load_increment(gpointer data,
                                                     const guchar * buf, guint size,
                                                     GError **error);

static void 
context_free (struct ico_progressive_state *context)
{
	g_free (context->LineBuf);
	context->LineBuf = NULL;
	g_free (context->HeaderBuf);
	g_list_free_full (context->entries, g_free);
	if (context->pixbuf)
		g_object_unref (context->pixbuf);

	g_free (context);
}

static gint
compare_direntry_scores (gconstpointer a,
                         gconstpointer b)
{
	const struct ico_direntry_data *ia = a;
	const struct ico_direntry_data *ib = b;

	/* Backwards, so largest first */
	return ib->ImageScore - ia->ImageScore;
}

static void DecodeHeader(guchar *Data, gint Bytes,
			 struct ico_progressive_state *State,
			 GError **error)
{
/* For ICO's we have to be very clever. There are multiple images possible
   in an .ICO. As a simple heuristic, we select the image which occupies the 
   largest number of bytes.
 */   
	struct ico_direntry_data *entry;
	gint IconCount = 0; /* The number of icon-versions in the file */
	guchar *BIH; /* The DIB for the used icon */
 	guchar *Ptr;
 	gint I;
	guint16 imgtype; /* 1 = icon, 2 = cursor */
	GList *l;

 	/* Step 1: The ICO header */

	/* First word should be 0 according to specs */
	if (((Data[1] << 8) + Data[0]) != 0) {
		g_set_error_literal (error,
				     GDK_PIXBUF_ERROR,
				     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
				     _("Invalid header in icon"));
		return;

	}

	imgtype = (Data[3] << 8) + Data[2];

	State->cursor = (imgtype == 2) ? TRUE : FALSE;

	/* If it is not a cursor make sure it is actually an icon */
	if (!State->cursor && imgtype != 1) {
		g_set_error_literal (error,
				     GDK_PIXBUF_ERROR,
				     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
				     _("Invalid header in icon"));
		return;
	}


 	IconCount = (Data[5] << 8) + (Data[4]);
	
 	State->HeaderSize = 6 + IconCount*16;

 	if (State->HeaderSize>State->BytesInHeaderBuf) {
 		guchar *tmp=g_try_realloc(State->HeaderBuf,State->HeaderSize);
		if (!tmp) {
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Not enough memory to load icon"));
			return;
		}
		State->HeaderBuf = tmp;
 		State->BytesInHeaderBuf = State->HeaderSize;
 	}
 	if (Bytes < State->HeaderSize)
 		return;

	/* Now iterate through the ICONDIRENTRY structures, and sort them by
	 * which one we think is "best" (essentially the largest) */
	g_list_free_full (State->entries, g_free);
	State->entries = 0;
	Ptr = Data + 6;
	for (I=0;I<IconCount;I++) {
		entry = g_new0 (struct ico_direntry_data, 1);
		entry->ImageScore = (Ptr[11] << 24) + (Ptr[10] << 16) + (Ptr[9] << 8) + (Ptr[8]);
		if (entry->ImageScore == 0)
			entry->ImageScore = 256;
		entry->x_hot = (Ptr[5] << 8) + Ptr[4];
		entry->y_hot = (Ptr[7] << 8) + Ptr[6];
		entry->DIBoffset = (Ptr[15]<<24)+(Ptr[14]<<16)+
		                   (Ptr[13]<<8) + (Ptr[12]);
		State->entries = g_list_insert_sorted (State->entries, entry, compare_direntry_scores);
		Ptr += 16;
	} 

	/* Now go through and find one we can parse */
	entry = NULL;
	for (l = State->entries; l != NULL; l = g_list_next (l)) {
		entry = l->data;

		if (entry->DIBoffset < 0) {
			g_set_error_literal (error,
			                     GDK_PIXBUF_ERROR,
			                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
			                     _("Invalid header in icon"));
			return;
		}

		/* We know how many bytes are in the "header" part. */
		State->HeaderSize = entry->DIBoffset + 40; /* 40 = sizeof(InfoHeader) */

		if (State->HeaderSize < 0) {
			g_set_error_literal (error,
			                     GDK_PIXBUF_ERROR,
			                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
			                     _("Invalid header in icon"));
			return;
		}

		if (State->HeaderSize>State->BytesInHeaderBuf) {
			guchar *tmp=g_try_realloc(State->HeaderBuf,State->HeaderSize);
			if (!tmp) {
				g_set_error_literal (error,
				                     GDK_PIXBUF_ERROR,
				                     GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
				                     _("Not enough memory to load icon"));
				return;
			}
			State->HeaderBuf = tmp;
			State->BytesInHeaderBuf = State->HeaderSize;
		}
		if (Bytes<State->HeaderSize)
			return;

		BIH = Data+entry->DIBoffset;

		/* A compressed icon, try the next one */
		if ((BIH[16] != 0) || (BIH[17] != 0) || (BIH[18] != 0)
		    || (BIH[19] != 0))
			continue;

		/* If we made it to here then we have selected a BIH structure
		 * in a format that we can parse */
		break;
	}

	/* No valid icon found, because all are compressed? */
	if (l == NULL) {
		g_set_error_literal (error,
		                     GDK_PIXBUF_ERROR,
		                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
		                     _("Compressed icons are not supported"));
		return;
	}

	/* This is the one we're going with */
	State->DIBoffset = entry->DIBoffset;
	State->x_hot = entry->x_hot;
	State->y_hot = entry->y_hot;

#ifdef DUMPBIH
	DumpBIH(BIH);
#endif	
	/* Add the palette to the headersize */
		
	State->Header.width =
	    (int)(BIH[7] << 24) + (BIH[6] << 16) + (BIH[5] << 8) + (BIH[4]);
	if (State->Header.width == 0)
		State->Header.width = 256;

	State->Header.height =
	    (int)((BIH[11] << 24) + (BIH[10] << 16) + (BIH[9] << 8) + (BIH[8]))/2;
	    /* /2 because the BIH height includes the transparency mask */
	if (State->Header.height == 0)
		State->Header.height = 256;
	State->Header.depth = (BIH[15] << 8) + (BIH[14]);

	State->Type = State->Header.depth;	
	if (State->Lines>=State->Header.height)
		State->Type = 1; /* The transparency mask is 1 bpp */
	
	/* Determine the  palette size. If the header indicates 0, it
	   is actually the maximum for the bpp. You have to love the
	   guys who made the spec. */
	I = (int)(BIH[35] << 24) + (BIH[34] << 16) + (BIH[33] << 8) + (BIH[32]);
	I = I*4;
	if ((I==0)&&(State->Type==1))
		I = 2*4;
	if ((I==0)&&(State->Type==4))
		I = 16*4;
	if ((I==0)&&(State->Type==8))
		I = 256*4;
	
	State->HeaderSize+=I;
	
	if (State->HeaderSize < 0) {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("Invalid header in icon"));
		return;
	}

 	if (State->HeaderSize>State->BytesInHeaderBuf) {
	        guchar *tmp=g_try_realloc(State->HeaderBuf,State->HeaderSize);
		if (!tmp) {
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Not enough memory to load icon"));
			return;
		}
		State->HeaderBuf = tmp;
 		State->BytesInHeaderBuf = State->HeaderSize;
 	}
 	if (Bytes < State->HeaderSize)
 		return;

	/* Negative heights mean top-down pixel-order */
	if (State->Header.height < 0) {
		State->Header.height = -State->Header.height;
		State->Header.Negative = 1;
	}
	if (State->Header.width < 0) {
		State->Header.width = -State->Header.width;
	}
	g_assert (State->Header.width > 0);
	g_assert (State->Header.height > 0);

        if (State->Type == 32)
                State->LineWidth = State->Header.width * 4;
        else if (State->Type == 24)
		State->LineWidth = State->Header.width * 3;
        else if (State->Type == 16)
                State->LineWidth = State->Header.width * 2;
        else if (State->Type == 8)
		State->LineWidth = State->Header.width * 1;
        else if (State->Type == 4)
		State->LineWidth = (State->Header.width+1)/2;
        else if (State->Type == 1) {
		State->LineWidth = State->Header.width / 8;
		if ((State->Header.width & 7) != 0)
			State->LineWidth++;
        } else {
          g_set_error_literal (error,
                               GDK_PIXBUF_ERROR,
                               GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                               _("Unsupported icon type"));
          return;
	}

	/* Pad to a 32 bit boundary */
	if (((State->LineWidth % 4) > 0))
		State->LineWidth = (State->LineWidth / 4) * 4 + 4;


	if (State->LineBuf == NULL) {
		State->LineBuf = g_try_malloc(State->LineWidth);
		if (!State->LineBuf) {
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Not enough memory to load icon"));
			return;
		}
	}

	g_assert(State->LineBuf != NULL);


	if (State->pixbuf == NULL) {
#if 1
		if (State->size_func) {
			gint width = State->Header.width;
			gint height = State->Header.height;

			(*State->size_func) (&width, &height, State->user_data);
			if (width == 0 || height == 0) {
				State->LineWidth = 0;
				return;
			}
		}
#endif

		State->pixbuf =
		    gdk_pixbuf_new(GDK_COLORSPACE_RGB, TRUE, 8,
				   State->Header.width,
				   State->Header.height);
		if (!State->pixbuf) {
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Not enough memory to load icon"));
			return;
		}
		if (State->cursor) {
			gchar hot[10];
			g_snprintf (hot, 10, "%d", State->x_hot);
			gdk_pixbuf_set_option (State->pixbuf, "x_hot", hot);
			g_snprintf (hot, 10, "%d", State->y_hot);
			gdk_pixbuf_set_option (State->pixbuf, "y_hot", hot);
		}

		if (State->prepared_func != NULL)
			/* Notify the client that we are ready to go */
			(*State->prepared_func) (State->pixbuf,
                                                 NULL,
						 State->user_data);

	}

}

/* 
 * func - called when we have pixmap created (but no image data)
 * user_data - passed as arg 1 to func
 * return context (opaque to user)
 */

static gpointer
gdk_pixbuf__ico_image_begin_load(GdkPixbufModuleSizeFunc size_func,
                                 GdkPixbufModulePreparedFunc prepared_func,
				 GdkPixbufModuleUpdatedFunc updated_func,
				 gpointer user_data,
                                 GError **error)
{
	struct ico_progressive_state *context;

	context = g_new0(struct ico_progressive_state, 1);
	context->size_func = size_func;
	context->prepared_func = prepared_func;
	context->updated_func = updated_func;
	context->user_data = user_data;

	context->HeaderSize = 54;
	context->HeaderBuf = g_try_malloc(14 + 40 + 4*256 + 512);
	if (!context->HeaderBuf) {
		g_free (context);
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                     _("Not enough memory to load ICO file"));
		return NULL;
	}
	/* 4*256 for the colormap */
	context->BytesInHeaderBuf = 14 + 40 + 4*256 + 512 ;
	context->HeaderDone = 0;

	context->LineWidth = 0;
	context->LineBuf = NULL;
	context->LineDone = 0;
	context->Lines = 0;

	context->Type = 0;

	memset(&context->Header, 0, sizeof(struct headerpair));


	context->pixbuf = NULL;


	return (gpointer) context;
}

/*
 * context - returned from image_begin_load
 *
 * free context, unref gdk_pixbuf
 */
static gboolean 
gdk_pixbuf__ico_image_stop_load(gpointer data,
				GError **error)
{
	struct ico_progressive_state *context =
	    (struct ico_progressive_state *) data;

        /* FIXME this thing needs to report errors if
         * we have unused image data
         */

	g_return_val_if_fail(context != NULL, TRUE);

	context_free (context);
        return TRUE;
}

static void
OneLine32 (struct ico_progressive_state *context)
{
        gint X;
        guchar *Pixels;

        X = 0;
        if (context->Header.Negative == 0)
                Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
                          (context->Header.height - context->Lines - 1));
        else
                Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
                          context->Lines);
        while (X < context->Header.width) {
                Pixels[X * 4 + 0] = context->LineBuf[X * 4 + 2];
                Pixels[X * 4 + 1] = context->LineBuf[X * 4 + 1];
                Pixels[X * 4 + 2] = context->LineBuf[X * 4 + 0];
                Pixels[X * 4 + 3] = context->LineBuf[X * 4 + 3];
                X++;
        }
}

static void OneLine24(struct ico_progressive_state *context)
{
	gint X;
	guchar *Pixels;

	X = 0;
	if (context->Header.Negative == 0)
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  (context->Header.height - context->Lines - 1));
	else
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  context->Lines);
	while (X < context->Header.width) {
		Pixels[X * 4 + 0] = context->LineBuf[X * 3 + 2];
		Pixels[X * 4 + 1] = context->LineBuf[X * 3 + 1];
		Pixels[X * 4 + 2] = context->LineBuf[X * 3 + 0];
		X++;
	}

}

static void
OneLine16 (struct ico_progressive_state *context)
{
        int i;
        guchar *pixels;
        guchar *src;

        if (context->Header.Negative == 0)
                pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
                          (context->Header.height - context->Lines - 1));
        else
                pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
                          context->Lines);

        src = context->LineBuf;

        for (i = 0; i < context->Header.width; i++) {
                int v, r, g, b;

                v = (int) src[0] | ((int) src[1] << 8);
                src += 2;

                /* Extract 5-bit RGB values */

                r = (v >> 10) & 0x1f;
                g = (v >> 5) & 0x1f;
                b = v & 0x1f;

                /* Fill the rightmost bits to form 8-bit values */

                *pixels++ = (r << 3) | (r >> 2);
                *pixels++ = (g << 3) | (g >> 2);
                *pixels++ = (b << 3) | (b >> 2);
                pixels++; /* skip alpha channel */
        }
}


static void OneLine8(struct ico_progressive_state *context)
{
	gint X;
	guchar *Pixels;

	X = 0;
	if (context->Header.Negative == 0)
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  (context->Header.height - context->Lines - 1));
	else
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  context->Lines);
	while (X < context->Header.width) {
		/* The joys of having a BGR byteorder */
		Pixels[X * 4 + 0] =
		    context->HeaderBuf[4 * context->LineBuf[X] + 42+context->DIBoffset];
		Pixels[X * 4 + 1] =
		    context->HeaderBuf[4 * context->LineBuf[X] + 41+context->DIBoffset];
		Pixels[X * 4 + 2] =
		    context->HeaderBuf[4 * context->LineBuf[X] + 40+context->DIBoffset];
		X++;
	}
}
static void OneLine4(struct ico_progressive_state *context)
{
	gint X;
	guchar *Pixels;

	X = 0;
	if (context->Header.Negative == 0)
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  (context->Header.height - context->Lines - 1));
	else
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  context->Lines);
	
	while (X < context->Header.width) {
		guchar Pix;
		
		Pix = context->LineBuf[X/2];

		Pixels[X * 4 + 0] =
		    context->HeaderBuf[4 * (Pix>>4) + 42+context->DIBoffset];
		Pixels[X * 4 + 1] =
		    context->HeaderBuf[4 * (Pix>>4) + 41+context->DIBoffset];
		Pixels[X * 4 + 2] =
		    context->HeaderBuf[4 * (Pix>>4) + 40+context->DIBoffset];
		X++;
		if (X<context->Header.width) { 
			/* Handle the other 4 bit pixel only when there is one */
			Pixels[X * 4 + 0] =
			    context->HeaderBuf[4 * (Pix&15) + 42+context->DIBoffset];
			Pixels[X * 4 + 1] =
			    context->HeaderBuf[4 * (Pix&15) + 41+context->DIBoffset];
			Pixels[X * 4 + 2] =
			    context->HeaderBuf[4 * (Pix&15) + 40+context->DIBoffset];
			X++;
		}
	}
	
}

static void OneLine1(struct ico_progressive_state *context)
{
	gint X;
	guchar *Pixels;

	X = 0;
	if (context->Header.Negative == 0)
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  (context->Header.height - context->Lines - 1));
	else
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  context->Lines);
	while (X < context->Header.width) {
		int Bit;

		Bit = (context->LineBuf[X / 8]) >> (7 - (X & 7));
		Bit = Bit & 1;
		/* The joys of having a BGR byteorder */
		Pixels[X * 4 + 0] = Bit*255;
		Pixels[X * 4 + 1] = Bit*255;
		Pixels[X * 4 + 2] = Bit*255;
		X++;
	}
}

static void OneLineTransp(struct ico_progressive_state *context)
{
	gint X;
	guchar *Pixels;

	/* Ignore the XOR mask for XP style 32-bpp icons with alpha */ 
	if (context->Header.depth == 32)
		return;

	X = 0;
	if (context->Header.Negative == 0)
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  (2*context->Header.height - context->Lines - 1));
	else
		Pixels = (context->pixbuf->pixels +
			  (gsize) context->pixbuf->rowstride *
			  (context->Lines-context->Header.height));
	while (X < context->Header.width) {
		int Bit;

		Bit = (context->LineBuf[X / 8]) >> (7 - (X & 7));
		Bit = Bit & 1;
		/* The joys of having a BGR byteorder */
		Pixels[X * 4 + 3] = 255-Bit*255;
#if 0
		if (Bit){
		  Pixels[X*4+0] = 255;
		  Pixels[X*4+1] = 255;
		} else {
		  Pixels[X*4+0] = 0;
		  Pixels[X*4+1] = 0;
		}
#endif		
		X++;
	}
}


static void OneLine(struct ico_progressive_state *context)
{
	context->LineDone = 0;
	
	if (context->Lines >= context->Header.height*2) {
		return;
	}
		
	if (context->Lines <context->Header.height) {		
                if (context->Type == 32)
                        OneLine32 (context);
		else if (context->Type == 24)
			OneLine24(context);
                else if (context->Type == 16)
                        OneLine16 (context);
		else if (context->Type == 8)
			OneLine8(context);
		else if (context->Type == 4)
			OneLine4(context);
		else if (context->Type == 1)
			OneLine1(context);
		else 
			g_assert_not_reached ();
	} else
		OneLineTransp(context);
	
	context->Lines++;
	if (context->Lines>=context->Header.height) {
	 	context->Type = 1;
		context->LineWidth = context->Header.width / 8;
		if ((context->Header.width & 7) != 0)
			context->LineWidth++;
		/* Pad to a 32 bit boundary */
		if (((context->LineWidth % 4) > 0))
			context->LineWidth = (context->LineWidth / 4) * 4 + 4;
			
	}
	  

	if (context->updated_func != NULL) {
		(*context->updated_func) (context->pixbuf,
					  0,
					  context->Lines % context->Header.height,
					  context->Header.width,
					  1,
					  context->user_data);

	}
}

/*
 * context - from image_begin_load
 * buf - new image data
 * size - length of new image data
 *
 * append image data onto inrecrementally built output image
 */
static gboolean
gdk_pixbuf__ico_image_load_increment(gpointer data,
                                     const guchar * buf,
                                     guint size,
                                     GError **error)
{
	struct ico_progressive_state *context =
	    (struct ico_progressive_state *) data;

	gint BytesToCopy;

	while (size > 0) {
		g_assert(context->LineDone >= 0);
		if (context->HeaderDone < context->HeaderSize) {	/* We still 
									   have headerbytes to do */
			BytesToCopy =
			    context->HeaderSize - context->HeaderDone;
			if (BytesToCopy > size)
				BytesToCopy = size;

			memmove(context->HeaderBuf + context->HeaderDone,
			       buf, BytesToCopy);

			size -= BytesToCopy;
			buf += BytesToCopy;
			context->HeaderDone += BytesToCopy;
		} 
		else {
			BytesToCopy =
			    context->LineWidth - context->LineDone;
			if (BytesToCopy > size)
				BytesToCopy = size;

			if (BytesToCopy > 0) {
				memmove(context->LineBuf +
				       context->LineDone, buf,
				       BytesToCopy);

				size -= BytesToCopy;
				buf += BytesToCopy;
				context->LineDone += BytesToCopy;
			}
			if ((context->LineDone >= context->LineWidth) &&
			    (context->LineWidth > 0))
				OneLine(context);


		}

		if (context->HeaderDone >= 6 && context->pixbuf == NULL) {
			GError *decode_err = NULL;
			DecodeHeader(context->HeaderBuf,
				     context->HeaderDone, context, &decode_err);
			if (context->LineBuf != NULL && context->LineWidth == 0)
				return TRUE;

			if (decode_err) {
				g_propagate_error (error, decode_err);
				return FALSE;
			}
		}
	}

	return TRUE;
}

/* saving ICOs */ 

static gint
write8 (FILE     *f,
	guint8   *data,
	gint      count)
{
  gint bytes;
  gint written;

  written = 0;
  while (count > 0)
    {
      bytes = fwrite ((char*) data, sizeof (char), count, f);
      if (bytes <= 0)
        break;
      count -= bytes;
      data += bytes;
      written += bytes;
    }

  return written;
}

static gint
write16 (FILE     *f,
	 guint16  *data,
	 gint      count)
{
  gint i;

  for (i = 0; i < count; i++)
	  data[i] = GUINT16_TO_LE (data[i]);

  return write8 (f, (guint8*) data, count * 2);
}

static gint
write32 (FILE     *f,
	 guint32  *data,
	 gint      count)
{
  gint i;

  for (i = 0; i < count; i++)
	  data[i] = GUINT32_TO_LE (data[i]);
  
  return write8 (f, (guint8*) data, count * 4);
}

typedef struct _IconEntry IconEntry;
struct _IconEntry {
	gint width;
	gint height;
	gint depth;
	gint hot_x;
	gint hot_y;

	guint8 n_colors;
	guint32 *colors;
	guint xor_rowstride;
	guint8 *xor;
	guint and_rowstride;
	guint8 *and;
};

static gboolean
fill_entry (IconEntry *icon, 
	    GdkPixbuf *pixbuf, 
	    gint       hot_x, 
	    gint       hot_y, 
	    GError   **error) 
 {
	guchar *p, *pixels, *and, *xor;
	gint n_channels, v, x, y;

	if (icon->width > 256 || icon->height > 256) {
		g_set_error_literal (error,
                                     GDK_PIXBUF_ERROR,
                                     GDK_PIXBUF_ERROR_BAD_OPTION,
                                     _("Image too large to be saved as ICO"));
		return FALSE;
	} 
	
	if (hot_x > -1 && hot_y > -1) {
		icon->hot_x = hot_x;
		icon->hot_y = hot_y;
		if (icon->hot_x >= icon->width || icon->hot_y >= icon->height) {
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_BAD_OPTION,
                                             _("Cursor hotspot outside image"));
			return FALSE;
		}
	}
	else {
		icon->hot_x = -1;
		icon->hot_y = -1;
	}
	
	switch (icon->depth) {
	case 32:
		icon->xor_rowstride = icon->width * 4;
		break;
	case 24:
		icon->xor_rowstride = icon->width * 3;
		break;
	case 16:
		icon->xor_rowstride = icon->width * 2;
		break;
	default:
		g_set_error (error,
			     GDK_PIXBUF_ERROR,
			     GDK_PIXBUF_ERROR_BAD_OPTION,
			     _("Unsupported depth for ICO file: %d"), icon->depth);
		return FALSE;
	}

	if ((icon->xor_rowstride % 4) != 0)
		icon->xor_rowstride = 4 * ((icon->xor_rowstride / 4) + 1);
	icon->xor = g_new0 (guchar, icon->xor_rowstride * icon->height);

	icon->and_rowstride = (icon->width + 7) / 8;
	if ((icon->and_rowstride % 4) != 0)
		icon->and_rowstride = 4 * ((icon->and_rowstride / 4) + 1);
	icon->and = g_new0 (guchar, icon->and_rowstride * icon->height);

	pixels = gdk_pixbuf_get_pixels (pixbuf);
	n_channels = gdk_pixbuf_get_n_channels (pixbuf);
	for (y = 0; y < icon->height; y++) {
		p = pixels + (gsize) gdk_pixbuf_get_rowstride (pixbuf) * (icon->height - 1 - y);
		and = icon->and + icon->and_rowstride * y;
		xor = icon->xor + icon->xor_rowstride * y;
		for (x = 0; x < icon->width; x++) {
			switch (icon->depth) {
			case 32:
				xor[0] = p[2];
				xor[1] = p[1];
				xor[2] = p[0];
				xor[3] = 0xff;
				if (n_channels == 4) {
					xor[3] = p[3];
					if (p[3] < 0x80)
						*and |= 1 << (7 - x % 8);
				}
				xor += 4;
				break;
			case 24:
				xor[0] = p[2];
				xor[1] = p[1];
				xor[2] = p[0];
				if (n_channels == 4 && p[3] < 0x80)
					*and |= 1 << (7 - x % 8);
				xor += 3;
				break;
			case 16:
				v = ((p[0] >> 3) << 10) | ((p[1] >> 3) << 5) | (p[2] >> 3);
				xor[0] = v & 0xff;
				xor[1] = v >> 8;
				if (n_channels == 4 && p[3] < 0x80)
					*and |= 1 << (7 - x % 8);
				xor += 2;
				break;
			}
			
			p += n_channels;
			if (x % 8 == 7) 
				and++;
		}
	}

	return TRUE;
}

static void
free_entry (IconEntry *icon)
{
	g_free (icon->colors);
	g_free (icon->and);
	g_free (icon->xor);
	g_free (icon);
}

static void
write_icon (FILE *f, GSList *entries)
{
	IconEntry *icon;
	GSList *entry;
	guint8 bytes[4];
	guint16 words[4];
	guint32 dwords[6];
	gint type;
	gint n_entries;
	gint offset;
	gint size;

	if (((IconEntry *)entries->data)->hot_x > -1)
		type = 2;
	else 
		type = 1;
	n_entries = g_slist_length (entries);

	/* header */
	words[0] = 0;
	words[1] = type;
	words[2] = n_entries;
	write16 (f, words, 3);
	
	offset = 6 + 16 * n_entries;

	for (entry = entries; entry; entry = entry->next) {
		icon = (IconEntry *)entry->data;
		size = 40 + icon->height * (icon->and_rowstride + icon->xor_rowstride);
		
		/* directory entry */
		if (icon->width == 256)
			bytes[0] = 0;
		else
			bytes[0] = icon->width;
		if (icon->height == 256)
			bytes[1] = 0;
		else
			bytes[1] = icon->height;
		bytes[2] = icon->n_colors;
		bytes[3] = 0;
		write8 (f, bytes, 4);
		if (type == 1) {
			words[0] = 1;
			words[1] = icon->depth;
		}
		else {
			words[0] = icon->hot_x;
			words[1] = icon->hot_y;
		}
		write16 (f, words, 2);
		dwords[0] = size;
		dwords[1] = offset;
		write32 (f, dwords, 2);

		offset += size;
	}

	for (entry = entries; entry; entry = entry->next) {
		icon = (IconEntry *)entry->data;

		/* bitmap header */
		dwords[0] = 40;
		dwords[1] = icon->width;
		dwords[2] = icon->height * 2;
		write32 (f, dwords, 3);
		words[0] = 1;
		words[1] = icon->depth;
		write16 (f, words, 2);
		dwords[0] = 0;
		dwords[1] = 0;
		dwords[2] = 0;
		dwords[3] = 0;
		dwords[4] = 0;
		dwords[5] = 0;
		write32 (f, dwords, 6);

		/* image data */
		write8 (f, icon->xor, icon->xor_rowstride * icon->height);
		write8 (f, icon->and, icon->and_rowstride * icon->height);
	}
}

static gboolean
gdk_pixbuf__ico_image_save (FILE          *f, 
                            GdkPixbuf     *pixbuf, 
                            gchar        **keys,
                            gchar        **values,
                            GError       **error)
{
	gint hot_x, hot_y;
	IconEntry *icon;
	GSList *entries = NULL;

	/* support only single-image ICOs for now */
	icon = g_new0 (IconEntry, 1);
	icon->width = gdk_pixbuf_get_width (pixbuf);
	icon->height = gdk_pixbuf_get_height (pixbuf);
	icon->depth = gdk_pixbuf_get_has_alpha (pixbuf) ? 32 : 24;
	hot_x = -1;
	hot_y = -1;
	
	/* parse options */
	if (keys && *keys) {
		gchar **kiter;
		gchar **viter;
		
		for (kiter = keys, viter = values; *kiter && *viter; kiter++, viter++) {
			char *endptr;
			if (strcmp (*kiter, "depth") == 0) {
				sscanf (*viter, "%d", &icon->depth);
			}
			else if (strcmp (*kiter, "x_hot") == 0) {
				hot_x = strtol (*viter, &endptr, 10);
			}
			else if (strcmp (*kiter, "y_hot") == 0) {
				hot_y = strtol (*viter, &endptr, 10);
			}

		}
	}

	if (!fill_entry (icon, pixbuf, hot_x, hot_y, error)) {
		free_entry (icon);
		return FALSE;
	}

	entries = g_slist_append (entries, icon); 
	write_icon (f, entries);

	g_slist_foreach (entries, (GFunc)free_entry, NULL);
	g_slist_free (entries);

	return TRUE;
}

#ifndef INCLUDE_ico
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__ico_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule *module)
{
	module->begin_load = gdk_pixbuf__ico_image_begin_load;
	module->stop_load = gdk_pixbuf__ico_image_stop_load;
	module->load_increment = gdk_pixbuf__ico_image_load_increment;
        module->save = gdk_pixbuf__ico_image_save;
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat *info)
{
	static const GdkPixbufModulePattern signature[] = {
		{ "  \x1   ", "zz znz", 100 }, 
		{ "  \x2   ", "zz znz", 100 },
		{ NULL, NULL, 0 }
	};
	static const gchar *mime_types[] = {
		"image/x-icon",
		"image/x-ico",
		"image/x-win-bitmap",
                "image/vnd.microsoft.icon",
                "application/ico",
                "image/ico",
                "image/icon",
                "text/ico",
		NULL
	};
	static const gchar *extensions[] = {
		"ico",
		"cur",
		NULL
	};

	info->name = "ico";
	info->signature = (GdkPixbufModulePattern *) signature;
	info->description = NC_("image format", "Windows icon");
	info->mime_types = (gchar **) mime_types;
	info->extensions = (gchar **) extensions;
	info->flags = GDK_PIXBUF_FORMAT_WRITABLE | GDK_PIXBUF_FORMAT_THREADSAFE;
	info->license = "LGPL";
}




