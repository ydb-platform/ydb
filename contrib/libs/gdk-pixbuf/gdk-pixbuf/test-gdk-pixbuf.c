/* -*- Mode: C; indent-tabs-mode: t; c-basic-offset: 8; tab-width: 8 -*- */

/* GdkPixbuf library - test program
 *
 * Copyright (C) 1999 The Free Software Foundation
 *
 * Author: Federico Mena-Quintero <federico@gimp.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, see <http://www.gnu.org/licenses/>.
 */

#include <contrib/restricted/glib/config.h>

#include <stdlib.h>
#include "gdk-pixbuf.h"
#include <glib-object.h>



static void
store_pixel (guchar *pixels,
	     int pixel,
	     gboolean alpha)
{
	if (alpha) {
		pixels[0] = pixel >> 24;
		pixels[1] = pixel >> 16;
		pixels[2] = pixel >> 8;
		pixels[3] = pixel;
	} else {
		pixels[0] = pixel >> 16;
		pixels[1] = pixel >> 8;
		pixels[2] = pixel;
	}
}

static void
fill_with_pixel (GdkPixbuf *pixbuf,
		 int pixel)
{
	int x, y;
	
	for (x = 0; x < gdk_pixbuf_get_width (pixbuf); x++) {
		for (y = 0; y < gdk_pixbuf_get_height (pixbuf); y++) {
			store_pixel (gdk_pixbuf_get_pixels (pixbuf)
				     + y * gdk_pixbuf_get_rowstride (pixbuf)
				     + x * gdk_pixbuf_get_n_channels (pixbuf),
				     pixel,
				     gdk_pixbuf_get_has_alpha (pixbuf));
		}
	}
}

static int
load_pixel (const guchar *pixels,
	    gboolean alpha)
{
	if (alpha)
		return (((((pixels[0] << 8) | pixels[1]) << 8) | pixels[2]) << 8) | pixels[3];
	else
		return (((pixels[0] << 8) | pixels[1]) << 8) | pixels[2];
}

static gboolean
simple_composite_test_one (GdkInterpType type,
			   int source_pixel,
			   gboolean source_alpha,
			   int destination_pixel,
			   gboolean destination_alpha,
			   int expected_result)
{
	GdkPixbuf *source_pixbuf;
	GdkPixbuf *destination_pixbuf;
	int result_pixel;

	source_pixbuf = gdk_pixbuf_new (GDK_COLORSPACE_RGB, source_alpha, 8, 32, 32);
	destination_pixbuf = gdk_pixbuf_new (GDK_COLORSPACE_RGB, destination_alpha, 8, 32, 32);
	
	fill_with_pixel (source_pixbuf, source_pixel);
	fill_with_pixel (destination_pixbuf, destination_pixel);

	gdk_pixbuf_composite (source_pixbuf, destination_pixbuf,
			      0, 0, 32, 32, 0, 0, 1, 1, type, 0xFF);

	result_pixel = load_pixel (gdk_pixbuf_get_pixels (destination_pixbuf)
				   + 16 * gdk_pixbuf_get_rowstride (destination_pixbuf)
				   + 16 * gdk_pixbuf_get_n_channels (destination_pixbuf),
				   destination_alpha);
	  
	g_object_unref (source_pixbuf);
	g_object_unref (destination_pixbuf);

	if (result_pixel != expected_result) {
		char *interpolation_type, *source_string, *destination_string, *result_string, *expected_string;

		switch (type) {
		case GDK_INTERP_NEAREST:  interpolation_type = "GDK_INTERP_NEAREST"; break;
		case GDK_INTERP_TILES:    interpolation_type = "GDK_INTERP_TILES"; break;
		case GDK_INTERP_BILINEAR: interpolation_type = "GDK_INTERP_BILINEAR"; break;
		case GDK_INTERP_HYPER:    interpolation_type = "GDK_INTERP_HYPER"; break;
		default:                  interpolation_type = "???";
		}

		if (source_alpha) {
			source_string = g_strdup_printf ("0x%08X", source_pixel);
		} else {
			source_string = g_strdup_printf ("0x%06X", source_pixel);
		}

		if (destination_alpha) {
			destination_string = g_strdup_printf ("0x%08X", destination_pixel);
			result_string = g_strdup_printf ("0x%08X", result_pixel);
			expected_string = g_strdup_printf ("0x%08X", expected_result);
		} else {
			destination_string = g_strdup_printf ("0x%06X", destination_pixel);
			result_string = g_strdup_printf ("0x%06X", result_pixel);
			expected_string = g_strdup_printf ("0x%06X", expected_result);
		}

		g_message ("simple_composite_test (%s): composite %s on top of %s, expected %s, got %s",
			   interpolation_type,
			   source_string, destination_string, expected_string, result_string);
		return FALSE;
	}

	return TRUE;
}

static gboolean
simple_composite_test_one_type (GdkInterpType type)
{
	gboolean success;

	success = TRUE;

	/* There are only a few trivial cases in here.
	 * But these were enough to expose the problems in the old composite code.
	 */

	/* Non-alpha into non-alpha. */
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0x000000, FALSE, 0x000000);
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0xFFFFFF, FALSE, 0x000000);
	success &= simple_composite_test_one (type, 0xFF0000, FALSE, 0x000000, FALSE, 0xFF0000);
	success &= simple_composite_test_one (type, 0x00FF00, FALSE, 0x000000, FALSE, 0x00FF00);
	success &= simple_composite_test_one (type, 0x0000FF, FALSE, 0x000000, FALSE, 0x0000FF);
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0xFF0000, FALSE, 0x000000);
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0x00FF00, FALSE, 0x000000);
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0x0000FF, FALSE, 0x000000);
	success &= simple_composite_test_one (type, 0x00FF00, FALSE, 0xFFFFFF, FALSE, 0x00FF00);
	success &= simple_composite_test_one (type, 0xFFFFFF, FALSE, 0xFFFFFF, FALSE, 0xFFFFFF);

	/* Alpha into non-alpha. */
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0x000000, FALSE, 0x000000);
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0xFFFFFF, FALSE, 0xFFFFFF);
	success &= simple_composite_test_one (type, 0x0000007F, TRUE, 0xFFFFFF, FALSE, 0x808080);
	success &= simple_composite_test_one (type, 0x00000080, TRUE, 0xFFFFFF, FALSE, 0x7F7F7F);
	success &= simple_composite_test_one (type, 0x000000FF, TRUE, 0xFFFFFF, FALSE, 0x000000);
	success &= simple_composite_test_one (type, 0x000000FF, TRUE, 0xFFFFFF, FALSE, 0x000000);
	success &= simple_composite_test_one (type, 0xFF0000FF, TRUE, 0x000000, FALSE, 0xFF0000);
	success &= simple_composite_test_one (type, 0x00FF00FF, TRUE, 0x000000, FALSE, 0x00FF00);
	success &= simple_composite_test_one (type, 0x0000FFFF, TRUE, 0x000000, FALSE, 0x0000FF);
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0xFF0000, FALSE, 0xFF0000);
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0x00FF00, FALSE, 0x00FF00);
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0x0000FF, FALSE, 0x0000FF);
	success &= simple_composite_test_one (type, 0x00FF0080, TRUE, 0xFFFFFF, FALSE, 0x7FFF7F);
	success &= simple_composite_test_one (type, 0xFFFFFFFF, TRUE, 0xFFFFFF, FALSE, 0xFFFFFF);

	/* Non-alpha into alpha. */
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0x00000000, TRUE, 0x000000FF);
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0xFFFFFFFF, TRUE, 0x000000FF);
	success &= simple_composite_test_one (type, 0xFF0000, FALSE, 0x00000000, TRUE, 0xFF0000FF);
	success &= simple_composite_test_one (type, 0x00FF00, FALSE, 0x00000000, TRUE, 0x00FF00FF);
	success &= simple_composite_test_one (type, 0x0000FF, FALSE, 0x00000000, TRUE, 0x0000FFFF);
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0xFF0000FF, TRUE, 0x000000FF);
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0x00FF00FF, TRUE, 0x000000FF);
	success &= simple_composite_test_one (type, 0x000000, FALSE, 0x0000FFFF, TRUE, 0x000000FF);
	success &= simple_composite_test_one (type, 0x00FF00, FALSE, 0xFFFFFF00, TRUE, 0x00FF00FF);
	success &= simple_composite_test_one (type, 0xFFFFFF, FALSE, 0xFFFFFFFF, TRUE, 0xFFFFFFFF);

	/* Alpha into alpha. */
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0x00000000, TRUE, 0x00000000);
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0xFFFFFFFF, TRUE, 0xFFFFFFFF);
	success &= simple_composite_test_one (type, 0x0000007F, TRUE, 0xFFFFFFFF, TRUE, 0x808080FF);
	success &= simple_composite_test_one (type, 0x00000080, TRUE, 0xFFFFFFFF, TRUE, 0x7F7F7FFF);
	success &= simple_composite_test_one (type, 0x000000FF, TRUE, 0xFFFFFFFF, TRUE, 0x000000FF);
	success &= simple_composite_test_one (type, 0xFF0000FF, TRUE, 0x00000000, TRUE, 0xFF0000FF);
	success &= simple_composite_test_one (type, 0x00FF00FF, TRUE, 0x00000000, TRUE, 0x00FF00FF);
	success &= simple_composite_test_one (type, 0x0000FFFF, TRUE, 0x00000000, TRUE, 0x0000FFFF);
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0xFF0000FF, TRUE, 0xFF0000FF);
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0x00FF00FF, TRUE, 0x00FF00FF);
	success &= simple_composite_test_one (type, 0x00000000, TRUE, 0x0000FFFF, TRUE, 0x0000FFFF);
	success &= simple_composite_test_one (type, 0x00FF0080, TRUE, 0xFFFFFF00, TRUE, 0x00FF0080);
	success &= simple_composite_test_one (type, 0xFF000080, TRUE, 0x00FF0040, TRUE, 0xCC32009F);
	success &= simple_composite_test_one (type, 0xFFFFFFFF, TRUE, 0xFFFFFFFF, TRUE, 0xFFFFFFFF);

	return success;
}

static gboolean
simple_composite_test (void)
{
	gboolean success;

	success = TRUE;

	success &= simple_composite_test_one_type (GDK_INTERP_NEAREST);
	success &= simple_composite_test_one_type (GDK_INTERP_TILES);
	success &= simple_composite_test_one_type (GDK_INTERP_BILINEAR);
	success &= simple_composite_test_one_type (GDK_INTERP_HYPER);

	return success;
}

int
main (int argc, char **argv)
{
	int result;

	result = EXIT_SUCCESS;

	/* Run some tests. */
	if (!simple_composite_test ()) {
		result = EXIT_FAILURE;
	}

	return result;
}
