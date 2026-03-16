/* JPEG 2000 loader
 *
 * Copyright (c) 2007 Bastien Nocera <hadess@hadess.net>
 * Inspired by work by Ben Karel <web+moz@eschew.org>
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
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "gdk-pixbuf-private.h"

#include <jasper/jasper.h>

G_MODULE_EXPORT void fill_vtable (GdkPixbufModule * module);
G_MODULE_EXPORT void fill_info (GdkPixbufFormat * info);

struct jasper_context {
	GdkPixbuf *pixbuf;

	GdkPixbufModuleSizeFunc size_func;
	GdkPixbufModuleUpdatedFunc updated_func;
	GdkPixbufModulePreparedFunc prepared_func;
	gpointer user_data;

	jas_stream_t *stream;

	int width, height;
};

static void
free_jasper_context (struct jasper_context *context)
{
	if (!context)
		return;

	if (context->stream) {
		jas_stream_close (context->stream);
		context->stream = NULL;
	}

	g_free (context);
}

static gpointer
jasper_image_begin_load (GdkPixbufModuleSizeFunc size_func,
			 GdkPixbufModulePreparedFunc prepared_func,
			 GdkPixbufModuleUpdatedFunc updated_func,
			 gpointer user_data, GError **error)
{
	struct jasper_context *context;
	jas_stream_t *stream;

	jas_init ();

	stream = jas_stream_memopen (NULL, -1);
	if (!stream) {
		g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                     _("Couldn't allocate memory for stream"));
		return NULL;
	}

	context = g_new0 (struct jasper_context, 1);
	if (!context)
		return NULL;

	context->size_func = size_func;
	context->updated_func = updated_func;
	context->prepared_func = prepared_func;
	context->user_data = user_data;
	context->width = context->height = -1;

	context->stream = stream;

	return context;
}

static gboolean
jasper_image_try_load (struct jasper_context *context, GError **error)
{
	jas_image_t *raw_image, *image;
	int num_components, colourspace_family;
	int i, rowstride, shift;
	guchar *pixels;

	raw_image = jas_image_decode (context->stream, -1, 0);
	if (!raw_image) {
		g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                     _("Couldn't decode image"));
		return FALSE;
	}

	if (context->width == -1 && context->height == -1) {
		int width, height;

		context->width = width = jas_image_cmptwidth (raw_image, 0);
		context->height = height = jas_image_cmptheight (raw_image, 0);

		if (context->size_func) {
			(*context->size_func) (&width, &height, context->user_data);

			if (width == 0 || height == 0) {
				jas_image_destroy(raw_image);
				g_set_error_literal (error,
                                                     GDK_PIXBUF_ERROR,
                                                     GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                                                     _("Transformed JPEG2000 has zero width or height"));
				return FALSE;
			}
		}
	}

	/* We only know how to handle grayscale and RGB images */
	num_components = jas_image_numcmpts (raw_image);
	colourspace_family = jas_clrspc_fam (jas_image_clrspc (raw_image));

	if ((num_components != 3 && num_components != 4 && num_components != 1) ||
	    (colourspace_family != JAS_CLRSPC_FAM_RGB  && colourspace_family != JAS_CLRSPC_FAM_GRAY)) {
	    	jas_image_destroy (raw_image);
		g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_UNKNOWN_TYPE,
                                     _("Image type currently not supported"));
		return FALSE;
	}

	/* Apply the colour profile to the image, creating a new one */
	if (jas_image_clrspc (raw_image) != JAS_CLRSPC_SRGB) {
		jas_cmprof_t *profile;

		profile = jas_cmprof_createfromclrspc (JAS_CLRSPC_SRGB);
		if (!profile) {
			jas_image_destroy (raw_image);
			g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Couldn't allocate memory for color profile"));
			return FALSE;
		}

		image = jas_image_chclrspc (raw_image, profile, JAS_CMXFORM_INTENT_PER);
		if (!image) {
			jas_image_destroy (raw_image);
			g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Couldn't allocate memory for color profile"));
			return FALSE;
		}
	} else {
		image = raw_image;
	}

	if (!context->pixbuf) {
		int bits_per_sample;

		/* Unfortunately, gdk-pixbuf doesn't support 16 bpp images
		 * bits_per_sample = jas_image_cmptprec (image, 0);
		if (bits_per_sample < 8)
			bits_per_sample = 8;
		else if (bits_per_sample > 8)
			bits_per_sample = 16;
		*/
		bits_per_sample = 8;

		context->pixbuf = gdk_pixbuf_new (GDK_COLORSPACE_RGB,
						  FALSE, bits_per_sample,
						  context->width, context->height);
		if (context->pixbuf == NULL) {
			g_set_error_literal (error,
                                             GDK_PIXBUF_ERROR,
                                             GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                             _("Insufficient memory to open JPEG 2000 file"));
			return FALSE;
		}
		if (context->prepared_func)
			context->prepared_func (context->pixbuf, NULL, context->user_data);
	}

	/* We calculate how much we should shift the pixel
	 * data by to make it fit into our pixbuf */
	shift = MAX (jas_image_cmptprec (image, 0) - gdk_pixbuf_get_bits_per_sample (context->pixbuf), 0);

	/* Loop over the 3 colourspaces */
	rowstride = gdk_pixbuf_get_rowstride (context->pixbuf);
	pixels = gdk_pixbuf_get_pixels (context->pixbuf);

	for (i = 0; i < num_components; i++) {
		jas_matrix_t *matrix;
		int j;

		matrix = jas_matrix_create (context->height, context->width);

		/* in libjasper, R is 0, G is 1, etc. we're lucky :)
		 * but we need to handle the "opacity" channel ourselves */
		if (i != 4) {
			jas_image_readcmpt (image, i, 0, 0, context->width, context->height, matrix);
		} else {
			jas_image_readcmpt (image, JAS_IMAGE_CT_OPACITY, 0, 0, context->width, context->height, matrix);
		}

		for (j = 0; j < context->height; j++) {
			int k;

			for (k = 0; k < context->width; k++) {
				if (num_components == 3 || num_components == 4) {
					pixels[j * rowstride + k * 3 + i] = jas_matrix_get (matrix, j, k) >> shift;
				} else {
					pixels[j * rowstride + k * 3] =
						pixels[j * rowstride + k * 3 + 1] =
						pixels[j * rowstride + k * 3 + 2] = jas_matrix_get (matrix, j, k) >> shift;
				}
			}
			/* Update once per line for the last component, otherwise
			 * we might contain garbage */
			if (context->updated_func && (i == num_components - 1) && k != 0) {
				context->updated_func (context->pixbuf, 0, j, k, 1, context->user_data);
			}
		}

		jas_matrix_destroy (matrix);
	}

	if (image != raw_image)
		jas_image_destroy (image);
	jas_image_destroy (raw_image);

	return TRUE;
}

static gboolean
jasper_image_stop_load (gpointer data, GError **error)
{
	struct jasper_context *context = (struct jasper_context *) data;
	gboolean ret;

	jas_stream_rewind (context->stream);
	ret = jasper_image_try_load (context, error);

	free_jasper_context (context);

	return ret;
}

static gboolean
jasper_image_load_increment (gpointer data, const guchar *buf, guint size, GError **error)
{
	struct jasper_context *context = (struct jasper_context *) data;

	if (jas_stream_write (context->stream, buf, size) < 0) {
		g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY,
                                     _("Couldn't allocate memory to buffer image data"));
		return FALSE;
	}

	return TRUE;
}

#ifndef INCLUDE_jasper
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__jasper_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule * module)
{
	module->begin_load = jasper_image_begin_load;
	module->stop_load = jasper_image_stop_load;
	module->load_increment = jasper_image_load_increment;
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat * info)
{
	static const GdkPixbufModulePattern signature[] = {
		{ "    jP", "!!!!  ", 100 },		/* file begins with 'jP' at offset 4 */
		{ "\xff\x4f\xff\x51\x00", NULL, 100 },	/* file starts with FF 4F FF 51 00 */
		{ NULL, NULL, 0 }
	};
	static const gchar *mime_types[] = {
		"image/jp2",
		"image/jpeg2000",
		"image/jpx",
		NULL
	};
	static const gchar *extensions[] = {
		"jp2",
		"jpc",
		"jpx",
		"j2k",
		"jpf",
		NULL
	};

	info->name = "jpeg2000";
	info->signature = (GdkPixbufModulePattern *) signature;
	info->description = NC_("image format", "JPEG 2000");
	info->mime_types = (gchar **) mime_types;
	info->extensions = (gchar **) extensions;
	info->flags = GDK_PIXBUF_FORMAT_THREADSAFE;
	info->license = "LGPL";
	info->disabled = FALSE;
}

