/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 8 -*- */
/* GdkPixbuf library - Basic memory management
 *
 * Copyright (C) 1999 The Free Software Foundation
 *
 * Authors: Mark Crichton <crichton@gimp.org>
 *          Miguel de Icaza <miguel@gnu.org>
 *          Federico Mena-Quintero <federico@gimp.org>
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

/**
 * GdkPixbuf:
 *
 * A pixel buffer.
 *
 * `GdkPixbuf` contains information about an image's pixel data,
 * its color space, bits per sample, width and height, and the
 * rowstride (the number of bytes between the start of one row
 * and the start of the next).
 *
 * ## Creating new `GdkPixbuf`
 * 
 * The most basic way to create a pixbuf is to wrap an existing pixel
 * buffer with a [class@GdkPixbuf.Pixbuf] instance. You can use the
 * [`ctor@GdkPixbuf.Pixbuf.new_from_data`] function to do this.
 * 
 * Every time you create a new `GdkPixbuf` instance for some data, you
 * will need to specify the destroy notification function that will be
 * called when the data buffer needs to be freed; this will happen when
 * a `GdkPixbuf` is finalized by the reference counting functions. If
 * you have a chunk of static data compiled into your application, you
 * can pass in `NULL` as the destroy notification function so that the
 * data will not be freed.
 * 
 * The [`ctor@GdkPixbuf.Pixbuf.new`] constructor function can be used
 * as a convenience to create a pixbuf with an empty buffer; this is
 * equivalent to allocating a data buffer using `malloc()` and then
 * wrapping it with `gdk_pixbuf_new_from_data()`. The `gdk_pixbuf_new()`
 * function will compute an optimal rowstride so that rendering can be
 * performed with an efficient algorithm.
 *
 * You can also copy an existing pixbuf with the [method@Pixbuf.copy]
 * function. This is not the same as just acquiring a reference to
 * the old pixbuf instance: the copy function will actually duplicate
 * the pixel data in memory and create a new [class@Pixbuf] instance
 * for it.
 *
 * ## Reference counting
 * 
 * `GdkPixbuf` structures are reference counted. This means that an
 * application can share a single pixbuf among many parts of the
 * code. When a piece of the program needs to use a pixbuf, it should
 * acquire a reference to it by calling `g_object_ref()`; when it no
 * longer needs the pixbuf, it should release the reference it acquired
 * by calling `g_object_unref()`. The resources associated with a
 * `GdkPixbuf` will be freed when its reference count drops to zero.
 * Newly-created `GdkPixbuf` instances start with a reference count
 * of one.
 *
 * ## Image Data
 *
 * Image data in a pixbuf is stored in memory in an uncompressed,
 * packed format. Rows in the image are stored top to bottom, and
 * in each row pixels are stored from left to right.
 *
 * There may be padding at the end of a row.
 *
 * The "rowstride" value of a pixbuf, as returned by [`method@GdkPixbuf.Pixbuf.get_rowstride`],
 * indicates the number of bytes between rows.
 *
 * **NOTE**: If you are copying raw pixbuf data with `memcpy()` note that the
 * last row in the pixbuf may not be as wide as the full rowstride, but rather
 * just as wide as the pixel data needs to be; that is: it is unsafe to do
 * `memcpy (dest, pixels, rowstride * height)` to copy a whole pixbuf. Use
 * [method@GdkPixbuf.Pixbuf.copy] instead, or compute the width in bytes of the
 * last row as:
 *
 * ```c
 * last_row = width * ((n_channels * bits_per_sample + 7) / 8);
 * ```
 *
 * The same rule applies when iterating over each row of a `GdkPixbuf` pixels
 * array.
 *
 * The following code illustrates a simple `put_pixel()`
 * function for RGB pixbufs with 8 bits per channel with an alpha
 * channel.
 *
 * ```c
 * static void
 * put_pixel (GdkPixbuf *pixbuf,
 *            int x,
 * 	   int y,
 * 	   guchar red,
 * 	   guchar green,
 * 	   guchar blue,
 * 	   guchar alpha)
 * {
 *   int n_channels = gdk_pixbuf_get_n_channels (pixbuf);
 *
 *   // Ensure that the pixbuf is valid
 *   g_assert (gdk_pixbuf_get_colorspace (pixbuf) == GDK_COLORSPACE_RGB);
 *   g_assert (gdk_pixbuf_get_bits_per_sample (pixbuf) == 8);
 *   g_assert (gdk_pixbuf_get_has_alpha (pixbuf));
 *   g_assert (n_channels == 4);
 *
 *   int width = gdk_pixbuf_get_width (pixbuf);
 *   int height = gdk_pixbuf_get_height (pixbuf);
 *
 *   // Ensure that the coordinates are in a valid range
 *   g_assert (x >= 0 && x < width);
 *   g_assert (y >= 0 && y < height);
 *
 *   int rowstride = gdk_pixbuf_get_rowstride (pixbuf);
 *
 *   // The pixel buffer in the GdkPixbuf instance
 *   guchar *pixels = gdk_pixbuf_get_pixels (pixbuf);
 *
 *   // The pixel we wish to modify
 *   guchar *p = pixels + y * rowstride + x * n_channels;
 *   p[0] = red;
 *   p[1] = green;
 *   p[2] = blue;
 *   p[3] = alpha;
 * }
 * ```
 *
 * ## Loading images
 *
 * The `GdkPixBuf` class provides a simple mechanism for loading
 * an image from a file in synchronous and asynchronous fashion.
 *
 * For GUI applications, it is recommended to use the asynchronous
 * stream API to avoid blocking the control flow of the application.
 *
 * Additionally, `GdkPixbuf` provides the [class@GdkPixbuf.PixbufLoader`]
 * API for progressive image loading.
 *
 * ## Saving images
 *
 * The `GdkPixbuf` class provides methods for saving image data in
 * a number of file formats. The formatted data can be written to a
 * file or to a memory buffer. `GdkPixbuf` can also call a user-defined
 * callback on the data, which allows to e.g. write the image
 * to a socket or store it in a database.
 */

#include "config.h"

#include <math.h>
#include <stdlib.h>
#include <string.h>

#define GDK_PIXBUF_C_COMPILATION
#include "gdk-pixbuf-private.h"
#include "gdk-pixbuf-features.h"
#include "gdk-pixbuf-enum-types.h"

/* Include the marshallers */
#include <glib-object.h>
#include <gio/gio.h>
#include "gdk-pixbuf-marshal.h"

static void gdk_pixbuf_finalize     (GObject        *object);
static void gdk_pixbuf_set_property (GObject        *object,
				     guint           prop_id,
				     const GValue   *value,
				     GParamSpec     *pspec);
static void gdk_pixbuf_get_property (GObject        *object,
				     guint           prop_id,
				     GValue         *value,
				     GParamSpec     *pspec);
static void gdk_pixbuf_constructed  (GObject        *object);


enum 
{
  PROP_0,
  PROP_COLORSPACE,
  PROP_N_CHANNELS,
  PROP_HAS_ALPHA,
  PROP_BITS_PER_SAMPLE,
  PROP_WIDTH,
  PROP_HEIGHT,
  PROP_ROWSTRIDE,
  PROP_PIXELS,
  PROP_PIXEL_BYTES
};

static void gdk_pixbuf_icon_iface_init (GIconIface *iface);
static void gdk_pixbuf_loadable_icon_iface_init (GLoadableIconIface *iface);

G_DEFINE_TYPE_WITH_CODE (GdkPixbuf, gdk_pixbuf, G_TYPE_OBJECT,
                         G_IMPLEMENT_INTERFACE (G_TYPE_ICON, gdk_pixbuf_icon_iface_init)
                         G_IMPLEMENT_INTERFACE (G_TYPE_LOADABLE_ICON, gdk_pixbuf_loadable_icon_iface_init))

static void 
gdk_pixbuf_init (GdkPixbuf *pixbuf)
{
  pixbuf->colorspace = GDK_COLORSPACE_RGB;
  pixbuf->n_channels = 3;
  pixbuf->bits_per_sample = 8;
  pixbuf->has_alpha = FALSE;
  pixbuf->storage = STORAGE_UNINITIALIZED;
}

static void
gdk_pixbuf_class_init (GdkPixbufClass *klass)
{
        GObjectClass *object_class = G_OBJECT_CLASS (klass);

        _gdk_pixbuf_init_gettext ();

        object_class->finalize = gdk_pixbuf_finalize;
        object_class->set_property = gdk_pixbuf_set_property;
        object_class->get_property = gdk_pixbuf_get_property;
        object_class->constructed = gdk_pixbuf_constructed;

#define PIXBUF_PARAM_FLAGS      (G_PARAM_READWRITE | \
                                G_PARAM_CONSTRUCT_ONLY | \
                                G_PARAM_EXPLICIT_NOTIFY | \
                                G_PARAM_STATIC_NAME | \
                                G_PARAM_STATIC_NICK | \
                                G_PARAM_STATIC_BLURB)
        /**
         * GdkPixbuf:n-channels:
         *
         * The number of samples per pixel.
         *
         * Currently, only 3 or 4 samples per pixel are supported.
         */
        g_object_class_install_property (object_class,
                                         PROP_N_CHANNELS,
                                         g_param_spec_int ("n-channels",
                                                           _("Number of Channels"),
                                                           _("The number of samples per pixel"),
                                                           0,
                                                           G_MAXINT,
                                                           3,
                                                           PIXBUF_PARAM_FLAGS));
        /**
         * GdkPixbuf:colorspace:
         *
         * The color space of the pixbuf.
         *
         * Currently, only `GDK_COLORSPACE_RGB` is supported.
         */
        g_object_class_install_property (object_class,
                                         PROP_COLORSPACE,
                                         g_param_spec_enum ("colorspace",
                                                            _("Colorspace"),
                                                            _("The colorspace in which the samples are interpreted"),
                                                            GDK_TYPE_COLORSPACE,
                                                            GDK_COLORSPACE_RGB,
                                                            PIXBUF_PARAM_FLAGS));
        /**
         * GdkPixbuf:has-alpha:
         *
         * Whether the pixbuf has an alpha channel.
         */
        g_object_class_install_property (object_class,
                                         PROP_HAS_ALPHA,
                                         g_param_spec_boolean ("has-alpha",
                                                               _("Has Alpha"),
                                                               _("Whether the pixbuf has an alpha channel"),
                                                               FALSE,
                                                               PIXBUF_PARAM_FLAGS));
        /**
         * GdkPixbuf:bits-per-sample:
         *
         * The number of bits per sample. 
         *
         * Currently only 8 bit per sample are supported.
         */
        g_object_class_install_property (object_class,
                                         PROP_BITS_PER_SAMPLE,
                                         g_param_spec_int ("bits-per-sample",
                                                           _("Bits per Sample"),
                                                           _("The number of bits per sample"),
                                                           1,
                                                           16,
                                                           8,
                                                           PIXBUF_PARAM_FLAGS));
        /**
         * GdkPixbuf:width:
         *
         * The number of columns of the pixbuf.
         */
        g_object_class_install_property (object_class,
                                         PROP_WIDTH,
                                         g_param_spec_int ("width",
                                                           _("Width"),
                                                           _("The number of columns of the pixbuf"),
                                                           1,
                                                           G_MAXINT,
                                                           1,
                                                           PIXBUF_PARAM_FLAGS));
        /**
         * GdkPixbuf:height:
         *
         * The number of rows of the pixbuf.
         */
        g_object_class_install_property (object_class,
                                         PROP_HEIGHT,
                                         g_param_spec_int ("height",
                                                           _("Height"),
                                                           _("The number of rows of the pixbuf"),
                                                           1,
                                                           G_MAXINT,
                                                           1,
                                                           PIXBUF_PARAM_FLAGS));
        /**
         * GdkPixbuf:rowstride:
         *
         * The number of bytes between the start of a row and 
         * the start of the next row.
         *
         * This number must (obviously) be at least as large as the
         * width of the pixbuf.
         */
        g_object_class_install_property (object_class,
                                         PROP_ROWSTRIDE,
                                         g_param_spec_int ("rowstride",
                                                           _("Rowstride"),
                                                           _("The number of bytes between the start of a row and the start of the next row"),
                                                           1,
                                                           G_MAXINT,
                                                           1,
                                                           PIXBUF_PARAM_FLAGS));
        /**
         * GdkPixbuf:pixels:
         *
         * A pointer to the pixel data of the pixbuf.
         */
        g_object_class_install_property (object_class,
                                         PROP_PIXELS,
                                         g_param_spec_pointer ("pixels",
                                                               _("Pixels"),
                                                               _("A pointer to the pixel data of the pixbuf"),
                                                               PIXBUF_PARAM_FLAGS));
        /**
         * GdkPixbuf::pixel-bytes:
         *
         * If set, this pixbuf was created from read-only #GBytes.
         *
         * Replaces GdkPixbuf::pixels.
         * 
         * Since: 2.32
         */
        g_object_class_install_property (object_class,
                                         PROP_PIXEL_BYTES,
                                         g_param_spec_boxed ("pixel-bytes",
                                                             _("Pixel Bytes"),
                                                             _("Readonly pixel data"),
                                                             G_TYPE_BYTES,
                                                             PIXBUF_PARAM_FLAGS));
}

static void
free_pixels (GdkPixbuf *pixbuf)
{
        g_assert (pixbuf->storage == STORAGE_PIXELS);

        if (pixbuf->s.pixels.pixels && pixbuf->s.pixels.destroy_fn) {
                (* pixbuf->s.pixels.destroy_fn) (pixbuf->s.pixels.pixels, pixbuf->s.pixels.destroy_fn_data);
        }

        pixbuf->s.pixels.pixels = NULL;
}

static void
free_bytes (GdkPixbuf *pixbuf)
{
        g_assert (pixbuf->storage == STORAGE_BYTES);

        g_clear_pointer (&pixbuf->s.bytes.bytes, g_bytes_unref);
}

static void
gdk_pixbuf_finalize (GObject *object)
{
        GdkPixbuf *pixbuf = GDK_PIXBUF (object);

        switch (pixbuf->storage) {
        case STORAGE_PIXELS:
                free_pixels (pixbuf);
                break;

        case STORAGE_BYTES:
                free_bytes (pixbuf);
                break;

        default:
                g_assert_not_reached ();
        }
        
        G_OBJECT_CLASS (gdk_pixbuf_parent_class)->finalize (object);
}


/**
 * gdk_pixbuf_ref: (skip)
 * @pixbuf: A pixbuf.
 *
 * Adds a reference to a pixbuf.
 *
 * Return value: The same as the @pixbuf argument.
 *
 * Deprecated: 2.0: Use g_object_ref().
 **/
GdkPixbuf *
gdk_pixbuf_ref (GdkPixbuf *pixbuf)
{
        return (GdkPixbuf *) g_object_ref (pixbuf);
}

/**
 * gdk_pixbuf_unref: (skip)
 * @pixbuf: A pixbuf.
 *
 * Removes a reference from a pixbuf.
 *
 * Deprecated: 2.0: Use g_object_unref().
 **/
void
gdk_pixbuf_unref (GdkPixbuf *pixbuf)
{
        g_object_unref (pixbuf);
}

static GBytes *
gdk_pixbuf_make_bytes (GdkPixbuf  *pixbuf,
                       GError    **error)
{
  gchar *buffer;
  gsize size;

  if (!gdk_pixbuf_save_to_buffer (pixbuf, &buffer, &size, "png", error, NULL))
    return NULL;

  return g_bytes_new_take (buffer, size);
}

static GVariant *
gdk_pixbuf_serialize (GIcon *icon)
{
  GError *error = NULL;
  GVariant *result;
  GBytes *bytes;

  bytes = gdk_pixbuf_make_bytes (GDK_PIXBUF (icon), &error);
  if (!bytes)
    {
      g_critical ("Unable to serialise GdkPixbuf to png (via g_icon_serialize()): %s", error->message);
      g_error_free (error);
      return NULL;
    }
  result = g_variant_new_from_bytes (G_VARIANT_TYPE_BYTESTRING, bytes, TRUE);
  g_bytes_unref (bytes);

  return g_variant_new ("(sv)", "bytes", result);
}

static GInputStream *
gdk_pixbuf_load (GLoadableIcon  *icon,
                 int             size,
                 char          **type,
                 GCancellable   *cancellable,
                 GError        **error)
{
  GInputStream *stream;
  GBytes *bytes;

  bytes = gdk_pixbuf_make_bytes (GDK_PIXBUF (icon), error);
  if (!bytes)
    return NULL;

  stream = g_memory_input_stream_new_from_bytes (bytes);
  g_bytes_unref (bytes);

  if (type)
    *type = g_strdup ("image/png");

  return stream;
}

static void
gdk_pixbuf_load_async (GLoadableIcon       *icon,
                       int                  size,
                       GCancellable        *cancellable,
                       GAsyncReadyCallback  callback,
                       gpointer             user_data)
{
  GTask *task;

  task = g_task_new (icon, cancellable, callback, user_data);
  g_task_return_pointer (task, icon, NULL);
  g_object_unref (task);
}

static GInputStream *
gdk_pixbuf_load_finish (GLoadableIcon  *icon,
                        GAsyncResult   *res,
                        char          **type,
                        GError        **error)
{
  g_return_val_if_fail (g_task_is_valid (res, icon), NULL);

  if (!g_task_propagate_pointer (G_TASK (res), error))
    return NULL;

  return gdk_pixbuf_load (icon, 0, type, NULL, error);
}

static void
gdk_pixbuf_loadable_icon_iface_init (GLoadableIconIface *iface)
{
  iface->load = gdk_pixbuf_load;

  /* In theory encoding a png could be time-consuming but we're talking
   * about icons here, so assume it's probably going to be OK and handle
   * the async variant of the call in-thread instead of having the
   * default implementation dispatch it to a worker.
   */
  iface->load_async = gdk_pixbuf_load_async;
  iface->load_finish = gdk_pixbuf_load_finish;
}

static void
gdk_pixbuf_icon_iface_init (GIconIface *iface)
{
        iface->hash = (guint (*) (GIcon *)) g_direct_hash;
        iface->equal = (gboolean (*) (GIcon *, GIcon *)) g_direct_equal;
        iface->serialize = gdk_pixbuf_serialize;
}

/* Used as the destroy notification function for gdk_pixbuf_new() */
static void
free_buffer (guchar *pixels, gpointer data)
{
	g_free (pixels);
}

/**
 * gdk_pixbuf_calculate_rowstride:
 * @colorspace: Color space for image
 * @has_alpha: Whether the image should have transparency information
 * @bits_per_sample: Number of bits per color sample
 * @width: Width of image in pixels, must be > 0
 * @height: Height of image in pixels, must be > 0
 *
 * Calculates the rowstride that an image created with those values would
 * have.
 *
 * This function is useful for front-ends and backends that want to check
 * image values without needing to create a `GdkPixbuf`.
 *
 * Return value: the rowstride for the given values, or -1 in case of error.
 *
 * Since: 2.36.8
 */
gint
gdk_pixbuf_calculate_rowstride (GdkColorspace colorspace,
				gboolean      has_alpha,
				int           bits_per_sample,
				int           width,
				int           height)
{
	unsigned int channels;

	g_return_val_if_fail (colorspace == GDK_COLORSPACE_RGB, -1);
	g_return_val_if_fail (bits_per_sample == 8, -1);
	g_return_val_if_fail (width > 0, -1);
	g_return_val_if_fail (height > 0, -1);

	channels = has_alpha ? 4 : 3;

	/* Overflow? */
	if (width > (G_MAXINT - 3) / channels)
		return -1;

	/* Always align rows to 32-bit boundaries */
	return (width * channels + 3) & ~3;
}

/**
 * gdk_pixbuf_new:
 * @colorspace: Color space for image
 * @has_alpha: Whether the image should have transparency information
 * @bits_per_sample: Number of bits per color sample
 * @width: Width of image in pixels, must be > 0
 * @height: Height of image in pixels, must be > 0
 *
 * Creates a new `GdkPixbuf` structure and allocates a buffer for it.
 *
 * If the allocation of the buffer failed, this function will return `NULL`.
 *
 * The buffer has an optimal rowstride. Note that the buffer is not cleared;
 * you will have to fill it completely yourself.
 *
 * Return value: (transfer full) (nullable): A newly-created pixel buffer
 **/
GdkPixbuf *
gdk_pixbuf_new (GdkColorspace colorspace, 
                gboolean      has_alpha,
                int           bits_per_sample,
                int           width,
                int           height)
{
	guchar *buf;
	int rowstride;

	rowstride = gdk_pixbuf_calculate_rowstride (colorspace,
						    has_alpha,
						    bits_per_sample,
						    width,
						    height);
	if (rowstride <= 0)
		return NULL;

	buf = g_try_malloc0_n (height, rowstride);
	if (!buf)
		return NULL;

	return gdk_pixbuf_new_from_data (buf, colorspace, has_alpha, bits_per_sample,
					 width, height, rowstride,
					 free_buffer, NULL);
}

/**
 * gdk_pixbuf_copy:
 * @pixbuf: A pixbuf.
 * 
 * Creates a new `GdkPixbuf` with a copy of the information in the specified
 * `pixbuf`.
 *
 * Note that this does not copy the options set on the original `GdkPixbuf`,
 * use gdk_pixbuf_copy_options() for this.
 * 
 * Return value: (nullable) (transfer full): A newly-created pixbuf
 **/
GdkPixbuf *
gdk_pixbuf_copy (const GdkPixbuf *pixbuf)
{
	guchar *buf;
	int size;

	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), NULL);

	/* Calculate a semi-exact size.  Here we copy with full rowstrides;
	 * maybe we should copy each row individually with the minimum
	 * rowstride?
	 */

	size = gdk_pixbuf_get_byte_length (pixbuf);

	buf = g_try_malloc (size);
	if (!buf)
		return NULL;

	memcpy (buf, gdk_pixbuf_read_pixels (pixbuf), size);

	return gdk_pixbuf_new_from_data (buf,
					 pixbuf->colorspace, pixbuf->has_alpha,
					 pixbuf->bits_per_sample,
					 pixbuf->width, pixbuf->height,
					 pixbuf->rowstride,
					 free_buffer,
					 NULL);
}

/**
 * gdk_pixbuf_new_subpixbuf:
 * @src_pixbuf: a `GdkPixbuf`
 * @src_x: X coord in @src_pixbuf
 * @src_y: Y coord in @src_pixbuf
 * @width: width of region in @src_pixbuf
 * @height: height of region in @src_pixbuf
 * 
 * Creates a new pixbuf which represents a sub-region of `src_pixbuf`.
 *
 * The new pixbuf shares its pixels with the original pixbuf, so
 * writing to one affects both.  The new pixbuf holds a reference to
 * `src_pixbuf`, so `src_pixbuf` will not be finalized until the new
 * pixbuf is finalized.
 *
 * Note that if `src_pixbuf` is read-only, this function will force it
 * to be mutable.
 *
 * Return value: (transfer full): a new pixbuf 
 **/
GdkPixbuf*
gdk_pixbuf_new_subpixbuf (GdkPixbuf *src_pixbuf,
                          int        src_x,
                          int        src_y,
                          int        width,
                          int        height)
{
        guchar *pixels;
        GdkPixbuf *sub;

        g_return_val_if_fail (GDK_IS_PIXBUF (src_pixbuf), NULL);
        g_return_val_if_fail (src_x >= 0 && src_x + width <= src_pixbuf->width, NULL);
        g_return_val_if_fail (src_y >= 0 && src_y + height <= src_pixbuf->height, NULL);
        
        /* Note causes an implicit copy where src_pixbuf owns the data */
        pixels = (gdk_pixbuf_get_pixels (src_pixbuf)
                  + src_y * src_pixbuf->rowstride
                  + src_x * src_pixbuf->n_channels);

        sub = gdk_pixbuf_new_from_data (pixels,
                                        src_pixbuf->colorspace,
                                        src_pixbuf->has_alpha,
                                        src_pixbuf->bits_per_sample,
                                        width, height,
                                        src_pixbuf->rowstride,
                                        NULL, NULL);

        /* Keep a reference to src_pixbuf */
        g_object_ref (src_pixbuf);
  
        g_object_set_qdata_full (G_OBJECT (sub),
                                 g_quark_from_static_string ("gdk-pixbuf-subpixbuf-src"),
                                 src_pixbuf,
                                 (GDestroyNotify) g_object_unref);

        return sub;
}



/* Accessors */

/**
 * gdk_pixbuf_get_colorspace:
 * @pixbuf: A pixbuf.
 *
 * Queries the color space of a pixbuf.
 *
 * Return value: Color space.
 **/
GdkColorspace
gdk_pixbuf_get_colorspace (const GdkPixbuf *pixbuf)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), GDK_COLORSPACE_RGB);

	return pixbuf->colorspace;
}

/**
 * gdk_pixbuf_get_n_channels:
 * @pixbuf: A pixbuf.
 *
 * Queries the number of channels of a pixbuf.
 *
 * Return value: Number of channels.
 **/
int
gdk_pixbuf_get_n_channels (const GdkPixbuf *pixbuf)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), -1);

	return pixbuf->n_channels;
}

/**
 * gdk_pixbuf_get_has_alpha:
 * @pixbuf: A pixbuf.
 *
 * Queries whether a pixbuf has an alpha channel (opacity information).
 *
 * Return value: `TRUE` if it has an alpha channel, `FALSE` otherwise.
 **/
gboolean
gdk_pixbuf_get_has_alpha (const GdkPixbuf *pixbuf)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), FALSE);

	return pixbuf->has_alpha ? TRUE : FALSE;
}

/**
 * gdk_pixbuf_get_bits_per_sample:
 * @pixbuf: A pixbuf.
 *
 * Queries the number of bits per color sample in a pixbuf.
 *
 * Return value: Number of bits per color sample.
 **/
int
gdk_pixbuf_get_bits_per_sample (const GdkPixbuf *pixbuf)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), -1);

	return pixbuf->bits_per_sample;
}

/**
 * gdk_pixbuf_get_pixels:
 * @pixbuf: A pixbuf.
 *
 * Queries a pointer to the pixel data of a pixbuf.
 *
 * This function will cause an implicit copy of the pixbuf data if the
 * pixbuf was created from read-only data.
 *
 * Please see the section on [image data](class.Pixbuf.html#image-data) for information
 * about how the pixel data is stored in memory.
 *
 * Return value: (array): A pointer to the pixbuf's pixel data.
 **/
guchar *
gdk_pixbuf_get_pixels (const GdkPixbuf *pixbuf)
{
        return gdk_pixbuf_get_pixels_with_length (pixbuf, NULL);
}

static void
downgrade_to_pixels (const GdkPixbuf *pixbuf)
{
        switch (pixbuf->storage) {
        case STORAGE_PIXELS:
                return;

        case STORAGE_BYTES: {
                GdkPixbuf *mut_pixbuf = (GdkPixbuf *) pixbuf;
                gsize len;
                Pixels pixels;

                pixels.pixels = g_bytes_unref_to_data (pixbuf->s.bytes.bytes, &len);
                pixels.destroy_fn = free_buffer;
                pixels.destroy_fn_data = NULL;

                mut_pixbuf->storage = STORAGE_PIXELS;
                mut_pixbuf->s.pixels = pixels;
                break;
        }

        default:
                g_assert_not_reached ();
        }
}

/**
 * gdk_pixbuf_get_pixels_with_length: (rename-to gdk_pixbuf_get_pixels)
 * @pixbuf: A pixbuf.
 * @length: (out): The length of the binary data.
 *
 * Queries a pointer to the pixel data of a pixbuf.
 *
 * This function will cause an implicit copy of the pixbuf data if the
 * pixbuf was created from read-only data.
 *
 * Please see the section on [image data](class.Pixbuf.html#image-data) for information
 * about how the pixel data is stored in memory.
 *
 * Return value: (array length=length): A pointer to the pixbuf's
 * pixel data.
 *
 * Since: 2.26
 */
guchar *
gdk_pixbuf_get_pixels_with_length (const GdkPixbuf *pixbuf,
                                   guint           *length)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), NULL);

        downgrade_to_pixels (pixbuf);
        g_assert (pixbuf->storage == STORAGE_PIXELS);

        if (length)
                *length = gdk_pixbuf_get_byte_length (pixbuf);

	return pixbuf->s.pixels.pixels;
}

/**
 * gdk_pixbuf_read_pixels:
 * @pixbuf: A pixbuf
 *
 * Provides a read-only pointer to the raw pixel data.
 *
 * This function allows skipping the implicit copy that must be made
 * if gdk_pixbuf_get_pixels() is called on a read-only pixbuf.
 *
 * Returns: a read-only pointer to the raw pixel data
 *
 * Since: 2.32
 */
const guint8*
gdk_pixbuf_read_pixels (const GdkPixbuf  *pixbuf)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), NULL);

        switch (pixbuf->storage) {
        case STORAGE_PIXELS:
                return pixbuf->s.pixels.pixels;

        case STORAGE_BYTES: {
                gsize len;
                /* Ignore len; callers know the size via other variables */
                return g_bytes_get_data (pixbuf->s.bytes.bytes, &len);
        }

        default:
                g_assert_not_reached ();
                return NULL;
        }
}

/**
 * gdk_pixbuf_read_pixel_bytes:
 * @pixbuf: A pixbuf
 *
 * Provides a #GBytes buffer containing the raw pixel data; the data
 * must not be modified.
 *
 * This function allows skipping the implicit copy that must be made
 * if gdk_pixbuf_get_pixels() is called on a read-only pixbuf.
 *
 * Returns: (transfer full): A new reference to a read-only copy of
 *   the pixel data.  Note that for mutable pixbufs, this function will
 *   incur a one-time copy of the pixel data for conversion into the
 *   returned #GBytes.
 *
 * Since: 2.32
 */
GBytes *
gdk_pixbuf_read_pixel_bytes (const GdkPixbuf  *pixbuf)
{
        g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), NULL);

        switch (pixbuf->storage) {
        case STORAGE_PIXELS:
                return g_bytes_new (pixbuf->s.pixels.pixels,
                                    gdk_pixbuf_get_byte_length (pixbuf));

        case STORAGE_BYTES:
                return g_bytes_ref (pixbuf->s.bytes.bytes);

        default:
                g_assert_not_reached ();
        }
}

/**
 * gdk_pixbuf_get_width:
 * @pixbuf: A pixbuf.
 *
 * Queries the width of a pixbuf.
 *
 * Return value: Width in pixels.
 **/
int
gdk_pixbuf_get_width (const GdkPixbuf *pixbuf)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), -1);

	return pixbuf->width;
}

/**
 * gdk_pixbuf_get_height:
 * @pixbuf: A pixbuf.
 *
 * Queries the height of a pixbuf.
 *
 * Return value: Height in pixels.
 **/
int
gdk_pixbuf_get_height (const GdkPixbuf *pixbuf)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), -1);

	return pixbuf->height;
}

/**
 * gdk_pixbuf_get_rowstride:
 * @pixbuf: A pixbuf.
 *
 * Queries the rowstride of a pixbuf, which is the number of bytes between
 * the start of a row and the start of the next row.
 *
 * Return value: Distance between row starts.
 **/
int
gdk_pixbuf_get_rowstride (const GdkPixbuf *pixbuf)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), -1);

	return pixbuf->rowstride;
}

/**
 * gdk_pixbuf_get_byte_length:
 * @pixbuf: A pixbuf
 *
 * Returns the length of the pixel data, in bytes.
 *
 * Return value: The length of the pixel data.
 *
 * Since: 2.26
 */
gsize
gdk_pixbuf_get_byte_length (const GdkPixbuf *pixbuf)
{
	g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), -1);

        return ((pixbuf->height - 1) * pixbuf->rowstride +
                pixbuf->width * ((pixbuf->n_channels * pixbuf->bits_per_sample + 7) / 8));
}



/* General initialization hooks */
const guint gdk_pixbuf_major_version = GDK_PIXBUF_MAJOR;
const guint gdk_pixbuf_minor_version = GDK_PIXBUF_MINOR;
const guint gdk_pixbuf_micro_version = GDK_PIXBUF_MICRO;

const char *gdk_pixbuf_version = GDK_PIXBUF_VERSION;

/* Error quark */
GQuark
gdk_pixbuf_error_quark (void)
{
  return g_quark_from_static_string ("gdk-pixbuf-error-quark");
}

/**
 * gdk_pixbuf_fill:
 * @pixbuf: a `GdkPixbuf`
 * @pixel: RGBA pixel to used to clear (`0xffffffff` is opaque white,
 *   `0x00000000` transparent black)
 *
 * Clears a pixbuf to the given RGBA value, converting the RGBA value into
 * the pixbuf's pixel format.
 *
 * The alpha component will be ignored if the pixbuf doesn't have an alpha
 * channel.
 */
void
gdk_pixbuf_fill (GdkPixbuf *pixbuf,
                 guint32    pixel)
{
        guchar *pixels;
        guint r, g, b, a;
        guchar *p;
        guint w, h;

        g_return_if_fail (GDK_IS_PIXBUF (pixbuf));

        if (pixbuf->width == 0 || pixbuf->height == 0)
                return;

        /* Force an implicit copy */
        pixels = gdk_pixbuf_get_pixels (pixbuf);

        r = (pixel & 0xff000000) >> 24;
        g = (pixel & 0x00ff0000) >> 16;
        b = (pixel & 0x0000ff00) >> 8;
        a = (pixel & 0x000000ff);

        h = pixbuf->height;
        
        while (h--) {
                w = pixbuf->width;
                p = pixels;

                switch (pixbuf->n_channels) {
                case 3:
                        while (w--) {
                                p[0] = r;
                                p[1] = g;
                                p[2] = b;
                                p += 3;
                        }
                        break;
                case 4:
                        while (w--) {
                                p[0] = r;
                                p[1] = g;
                                p[2] = b;
                                p[3] = a;
                                p += 4;
                        }
                        break;
                default:
                        break;
                }
                
                pixels += pixbuf->rowstride;
        }
}



/**
 * gdk_pixbuf_get_option:
 * @pixbuf: a `GdkPixbuf`
 * @key: a nul-terminated string.
 * 
 * Looks up @key in the list of options that may have been attached to the
 * @pixbuf when it was loaded, or that may have been attached by another
 * function using gdk_pixbuf_set_option().
 *
 * For instance, the ANI loader provides "Title" and "Artist" options. 
 * The ICO, XBM, and XPM loaders provide "x_hot" and "y_hot" hot-spot 
 * options for cursor definitions. The PNG loader provides the tEXt ancillary
 * chunk key/value pairs as options. Since 2.12, the TIFF and JPEG loaders
 * return an "orientation" option string that corresponds to the embedded 
 * TIFF/Exif orientation tag (if present). Since 2.32, the TIFF loader sets
 * the "multipage" option string to "yes" when a multi-page TIFF is loaded.
 * Since 2.32 the JPEG and PNG loaders set "x-dpi" and "y-dpi" if the file
 * contains image density information in dots per inch.
 * Since 2.36.6, the JPEG loader sets the "comment" option with the comment
 * EXIF tag.
 * 
 * Return value: (transfer none) (nullable): the value associated with `key`
 **/
const gchar *
gdk_pixbuf_get_option (GdkPixbuf   *pixbuf,
                       const gchar *key)
{
        gchar **options;
        gint i;

        g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), NULL);
        g_return_val_if_fail (key != NULL, NULL);
  
        options = g_object_get_qdata (G_OBJECT (pixbuf), 
                                      g_quark_from_static_string ("gdk_pixbuf_options"));
        if (options) {
                for (i = 0; options[2*i]; i++) {
                        if (strcmp (options[2*i], key) == 0)
                                return options[2*i+1];
                }
        }
        
        return NULL;
}

/**
 * gdk_pixbuf_get_options:
 * @pixbuf: a `GdkPixbuf`
 *
 * Returns a `GHashTable` with a list of all the options that may have been
 * attached to the `pixbuf` when it was loaded, or that may have been
 * attached by another function using [method@GdkPixbuf.Pixbuf.set_option].
 *
 * Return value: (transfer container) (element-type utf8 utf8): a #GHashTable
 *   of key/values pairs
 *
 * Since: 2.32
 **/
GHashTable *
gdk_pixbuf_get_options (GdkPixbuf *pixbuf)
{
        GHashTable *ht;
        gchar **options;

        g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), NULL);

        ht = g_hash_table_new (g_str_hash, g_str_equal);

        options = g_object_get_qdata (G_OBJECT (pixbuf),
                                      g_quark_from_static_string ("gdk_pixbuf_options"));
        if (options) {
                gint i;

                for (i = 0; options[2*i]; i++)
                        g_hash_table_insert (ht, options[2*i], options[2*i+1]);
        }

        return ht;
}

/**
 * gdk_pixbuf_remove_option:
 * @pixbuf: a `GdkPixbuf`
 * @key: a nul-terminated string representing the key to remove.
 *
 * Removes the key/value pair option attached to a `GdkPixbuf`.
 *
 * Return value: `TRUE` if an option was removed, `FALSE` if not.
 *
 * Since: 2.36
 **/
gboolean
gdk_pixbuf_remove_option (GdkPixbuf   *pixbuf,
                          const gchar *key)
{
        GQuark  quark;
        gchar **options;
        guint n;
        GPtrArray *array;
        gboolean found;

        g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), FALSE);
        g_return_val_if_fail (key != NULL, FALSE);

        quark = g_quark_from_static_string ("gdk_pixbuf_options");

        options = g_object_get_qdata (G_OBJECT (pixbuf), quark);
        if (!options)
                return FALSE;

        g_object_steal_qdata (G_OBJECT (pixbuf), quark);

        /* There's at least a nul-terminator */
        array = g_ptr_array_new_full (1, g_free);

        found = FALSE;
        for (n = 0; options[2*n]; n++) {
                if (strcmp (options[2*n], key) != 0) {
                        g_ptr_array_add (array, g_strdup (options[2*n]));   /* key */
                        g_ptr_array_add (array, g_strdup (options[2*n+1])); /* value */
                } else {
                        found = TRUE;
                }
        }

        if (array->len == 0) {
                g_ptr_array_unref (array);
                g_strfreev (options);
                return found;
        }

        if (!found) {
                g_ptr_array_free (array, TRUE);
                g_object_set_qdata_full (G_OBJECT (pixbuf), quark,
                                         options, (GDestroyNotify) g_strfreev);
                return FALSE;
        }

        g_ptr_array_add (array, NULL);
        g_object_set_qdata_full (G_OBJECT (pixbuf), quark,
                                 g_ptr_array_free (array, FALSE), (GDestroyNotify) g_strfreev);
        g_strfreev (options);

        return TRUE;
}

/**
 * gdk_pixbuf_set_option:
 * @pixbuf: a `GdkPixbuf`
 * @key: a nul-terminated string.
 * @value: a nul-terminated string.
 * 
 * Attaches a key/value pair as an option to a `GdkPixbuf`.
 *
 * If `key` already exists in the list of options attached to the `pixbuf`,
 * the new value is ignored and `FALSE` is returned.
 *
 * Return value: `TRUE` on success
 *
 * Since: 2.2
 **/
gboolean
gdk_pixbuf_set_option (GdkPixbuf   *pixbuf,
                       const gchar *key,
                       const gchar *value)
{
        GQuark  quark;
        gchar **options;
        gint n = 0;
 
        g_return_val_if_fail (GDK_IS_PIXBUF (pixbuf), FALSE);
        g_return_val_if_fail (key != NULL, FALSE);
        g_return_val_if_fail (value != NULL, FALSE);

        quark = g_quark_from_static_string ("gdk_pixbuf_options");

        options = g_object_get_qdata (G_OBJECT (pixbuf), quark);

        if (options) {
                for (n = 0; options[2*n]; n++) {
                        if (strcmp (options[2*n], key) == 0)
                                return FALSE;
                }

                g_object_steal_qdata (G_OBJECT (pixbuf), quark);
                options = g_renew (gchar *, options, 2*(n+1) + 1);
        } else {
                options = g_new (gchar *, 3);
        }
        
        options[2*n]   = g_strdup (key);
        options[2*n+1] = g_strdup (value);
        options[2*n+2] = NULL;

        g_object_set_qdata_full (G_OBJECT (pixbuf), quark,
                                 options, (GDestroyNotify) g_strfreev);
        
        return TRUE;
}

/**
 * gdk_pixbuf_copy_options:
 * @src_pixbuf: the source pixbuf
 * @dest_pixbuf: the destination pixbuf
 *
 * Copies the key/value pair options attached to a `GdkPixbuf` to another
 * `GdkPixbuf`.
 *
 * This is useful to keep original metadata after having manipulated
 * a file. However be careful to remove metadata which you've already
 * applied, such as the "orientation" option after rotating the image.
 *
 * Return value: `TRUE` on success.
 *
 * Since: 2.36
 **/
gboolean
gdk_pixbuf_copy_options (GdkPixbuf *src_pixbuf,
                         GdkPixbuf *dest_pixbuf)
{
        GQuark  quark;
        gchar **options;

        g_return_val_if_fail (GDK_IS_PIXBUF (src_pixbuf), FALSE);
        g_return_val_if_fail (GDK_IS_PIXBUF (dest_pixbuf), FALSE);

        quark = g_quark_from_static_string ("gdk_pixbuf_options");

        options = g_object_dup_qdata (G_OBJECT (src_pixbuf),
                                      quark,
                                      (GDuplicateFunc) g_strdupv,
                                      NULL);

        if (options == NULL)
                return TRUE;

        g_object_set_qdata_full (G_OBJECT (dest_pixbuf), quark,
                                 options, (GDestroyNotify) g_strfreev);

        return TRUE;
}

static void
gdk_pixbuf_set_property (GObject         *object,
			 guint            prop_id,
			 const GValue    *value,
			 GParamSpec      *pspec)
{
        GdkPixbuf *pixbuf = GDK_PIXBUF (object);
        gboolean notify = TRUE;

        switch (prop_id) {
        case PROP_COLORSPACE:
                notify = pixbuf->colorspace != g_value_get_enum (value);
                pixbuf->colorspace = g_value_get_enum (value);
                break;
        case PROP_N_CHANNELS:
                notify = pixbuf->n_channels != g_value_get_int (value);
                pixbuf->n_channels = g_value_get_int (value);
                break;
        case PROP_HAS_ALPHA:
                notify = pixbuf->has_alpha != g_value_get_boolean (value);
                pixbuf->has_alpha = g_value_get_boolean (value);
                break;
        case PROP_BITS_PER_SAMPLE:
                notify = pixbuf->bits_per_sample != g_value_get_int (value);
                pixbuf->bits_per_sample = g_value_get_int (value);
                break;
        case PROP_WIDTH:
                notify = pixbuf->width != g_value_get_int (value);
                pixbuf->width = g_value_get_int (value);
                break;
        case PROP_HEIGHT:
                notify = pixbuf->height != g_value_get_int (value);
                pixbuf->height = g_value_get_int (value);
                break;
        case PROP_ROWSTRIDE:
                notify = pixbuf->rowstride != g_value_get_int (value);
                pixbuf->rowstride = g_value_get_int (value);
                break;

        /* The following two are a bit strange.  Both PROP_PIXELS and
         * PROP_PIXEL_BYTES are G_PARAM_CONSTRUCT_ONLY properties, which means
         * that GObject will generate default values for any missing one and
         * call us for *both*.  So, we need to check whether the passed value is
         * not NULL before actually setting pixbuf->storage.
         */
        case PROP_PIXELS: {
                guchar *pixels = g_value_get_pointer (value);

                if (pixels) {
                        g_assert (pixbuf->storage == STORAGE_UNINITIALIZED);

                        pixbuf->storage = STORAGE_PIXELS;
                        pixbuf->s.pixels.pixels = pixels;
                        pixbuf->s.pixels.destroy_fn = NULL;
                        pixbuf->s.pixels.destroy_fn_data = NULL;
                } else {
                        notify = FALSE;
                }

                break;
        }

        case PROP_PIXEL_BYTES: {
                GBytes *bytes = g_value_get_boxed (value);

                if (bytes) {
                        g_assert (pixbuf->storage == STORAGE_UNINITIALIZED);

                        pixbuf->storage = STORAGE_BYTES;
                        pixbuf->s.bytes.bytes = g_value_dup_boxed (value);
                } else {
                        notify = FALSE;
                }

                break;
        }

        default:
                G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
                break;
        }

        if (notify)
                g_object_notify_by_pspec (G_OBJECT (object), pspec);
}

static void
gdk_pixbuf_get_property (GObject         *object,
			 guint            prop_id,
			 GValue          *value,
			 GParamSpec      *pspec)
{
        GdkPixbuf *pixbuf = GDK_PIXBUF (object);
  
        switch (prop_id) {
        case PROP_COLORSPACE:
                g_value_set_enum (value, gdk_pixbuf_get_colorspace (pixbuf));
                break;
        case PROP_N_CHANNELS:
                g_value_set_int (value, gdk_pixbuf_get_n_channels (pixbuf));
                break;
        case PROP_HAS_ALPHA:
                g_value_set_boolean (value, gdk_pixbuf_get_has_alpha (pixbuf));
                break;
        case PROP_BITS_PER_SAMPLE:
                g_value_set_int (value, gdk_pixbuf_get_bits_per_sample (pixbuf));
                break;
        case PROP_WIDTH:
                g_value_set_int (value, gdk_pixbuf_get_width (pixbuf));
                break;
        case PROP_HEIGHT:
                g_value_set_int (value, gdk_pixbuf_get_height (pixbuf));
                break;
        case PROP_ROWSTRIDE:
                g_value_set_int (value, gdk_pixbuf_get_rowstride (pixbuf));
                break;
        case PROP_PIXELS:
                g_value_set_pointer (value, gdk_pixbuf_get_pixels (pixbuf));
                break;
        case PROP_PIXEL_BYTES:
                g_value_take_boxed (value, gdk_pixbuf_read_pixel_bytes (pixbuf));
                break;
        default:
                G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
                break;
        }
}

static void
make_storage_invalid (GdkPixbuf *pixbuf)
{
        char *buf;
        gsize bufsize = 3;

        buf = g_new0 (char, bufsize);

        pixbuf->storage = STORAGE_BYTES;
        pixbuf->s.bytes.bytes = g_bytes_new_take (buf, bufsize);

        pixbuf->colorspace = GDK_COLORSPACE_RGB;
        pixbuf->n_channels = 3;
        pixbuf->bits_per_sample = 8;
        pixbuf->width = 1;
        pixbuf->height = 1;
        pixbuf->rowstride = 3;
        pixbuf->has_alpha = FALSE;
}

static void
gdk_pixbuf_constructed (GObject *object)
{
        GdkPixbuf *pixbuf = GDK_PIXBUF (object);

        G_OBJECT_CLASS (gdk_pixbuf_parent_class)->constructed (object);

        switch (pixbuf->storage) {
        case STORAGE_UNINITIALIZED:
                /* This means that neither of the construct properties "pixels" nor "pixel-bytes"
                 * was specified during a call to g_object_new().
                 *
                 * To avoid breaking ABI, we don't emit this warning.  We'll want
                 * to emit it once we can have fallible construction.
                 *
                 * g_warning ("pixbuf needs to be constructed with the 'pixels' or 'pixel-bytes' properties");
                 */

                make_storage_invalid (pixbuf);
                break;

        case STORAGE_PIXELS:
                g_assert (pixbuf->s.pixels.pixels != NULL);
                break;

        case STORAGE_BYTES: {
                gsize bytes_size;
                gint width, height;
                gboolean has_alpha;

                g_assert (pixbuf->s.bytes.bytes != NULL);

                bytes_size = g_bytes_get_size (pixbuf->s.bytes.bytes);
                width = pixbuf->width;
                height = pixbuf->height;
                has_alpha = pixbuf->has_alpha;

                /* This is the same check as in gdk_pixbuf_new_from_bytes() */
                if (!(bytes_size >= width * height * (has_alpha ? 4 : 3))) {
                        g_error ("GBytes is too small to fit the pixbuf's declared width and height");
                }
                break;
        }

        default:
                g_assert_not_reached ();
        }

        g_assert (pixbuf->storage != STORAGE_UNINITIALIZED);
}
