/* -*- Mode: C; indent-tabs-mode: nil; c-basic-offset: 4 -*- */
/* vim: set sw=4 sts=4 expandtab: */
/*
   rsvg-pixbuf.h: SAX-based renderer for SVG files using GDK-Pixbuf.

   Copyright (C) 2000 Eazel, Inc.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation; either
   version 2.1 of the License, or (at your option) any later version.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
   Lesser General Public License for more details.

   You should have received a copy of the GNU Lesser General Public
   License along with this library; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

   Author: Raph Levien <raph@artofcode.com>
*/

#if !defined (__RSVG_RSVG_H_INSIDE__) && !defined (RSVG_COMPILATION)
#warning "Including <librsvg/rsvg-pixbuf.h> directly is deprecated."
#endif

#ifndef RSVG_PIXBUF_H
#define RSVG_PIXBUF_H

#include <gdk-pixbuf/gdk-pixbuf.h>

G_BEGIN_DECLS

/**
 * rsvg_handle_get_pixbuf:
 * @handle: An [class@Rsvg.Handle]
 *
 * Returns the pixbuf loaded by @handle.  The pixbuf returned will be reffed, so
 * the caller of this function must assume that ref.
 *
 * API ordering: This function must be called on a fully-loaded @handle.  See
 * the section "[API ordering](class.Handle.html#api-ordering)" for details.
 *
 * This function depends on the [class@Rsvg.Handle]'s dots-per-inch value (DPI) to compute the
 * "natural size" of the document in pixels, so you should call [method@Rsvg.Handle.set_dpi]
 * beforehand.
 *
 * Returns: (transfer full) (nullable): A pixbuf, or %NULL on error
 * during rendering.
 * Deprecated: 2.58.  Use [method@Rsvg.Handle.get_pixbuf_and_error].
 **/
RSVG_DEPRECATED_FOR(rsvg_handle_get_pixbuf_and_error)
GdkPixbuf *rsvg_handle_get_pixbuf (RsvgHandle *handle);

/**
 * rsvg_handle_get_pixbuf_and_error:
 * @handle: An [class@Rsvg.Handle]
 * @error: return location for a `GError`
 *
 * Returns the pixbuf loaded by @handle.  The pixbuf returned will be reffed, so
 * the caller of this function must assume that ref.
 *
 * API ordering: This function must be called on a fully-loaded @handle.  See
 * the section "[API ordering](class.Handle.html#api-ordering)" for details.
 *
 * This function depends on the [class@Rsvg.Handle]'s dots-per-inch value (DPI) to compute the
 * "natural size" of the document in pixels, so you should call [method@Rsvg.Handle.set_dpi]
 * beforehand.
 *
 * Returns: (transfer full) (nullable): A pixbuf, or %NULL on error
 * during rendering.
 *
 * Since: 2.59
 **/
RSVG_API
GdkPixbuf *rsvg_handle_get_pixbuf_and_error (RsvgHandle *handle, GError **error);

/**
 * rsvg_handle_get_pixbuf_sub:
 * @handle: An #RsvgHandle
 * @id: (nullable): An element's id within the SVG, starting with "#" (a single
 * hash character), for example, `#layer1`.  This notation corresponds to a
 * URL's fragment ID.  Alternatively, pass `NULL` to use the whole SVG.
 *
 * Creates a `GdkPixbuf` the same size as the entire SVG loaded into @handle, but
 * only renders the sub-element that has the specified @id (and all its
 * sub-sub-elements recursively).  If @id is `NULL`, this function renders the
 * whole SVG.
 *
 * This function depends on the [class@Rsvg.Handle]'s dots-per-inch value (DPI) to compute the
 * "natural size" of the document in pixels, so you should call [method@Rsvg.Handle.set_dpi]
 * beforehand.
 *
 * If you need to render an image which is only big enough to fit a particular
 * sub-element of the SVG, consider using [method@Rsvg.Handle.render_element].
 *
 * Element IDs should look like an URL fragment identifier; for example, pass
 * `#foo` (hash `foo`) to get the geometry of the element that
 * has an `id="foo"` attribute.
 *
 * API ordering: This function must be called on a fully-loaded @handle.  See
 * the section "[API ordering](class.Handle.html#api-ordering)" for details.
 *
 * Returns: (transfer full) (nullable): a pixbuf, or `NULL` if an error occurs
 * during rendering.
 *
 * Since: 2.14
 **/
RSVG_API
GdkPixbuf *rsvg_handle_get_pixbuf_sub (RsvgHandle *handle, const char *id);

/* BEGIN deprecated APIs. Do not use! */

/**
 * rsvg-pixbuf:
 *
 * Years ago, GNOME and GTK used the gdk-pixbuf library as a general mechanism to load
 * raster images into memory (PNG, JPEG, etc.) and pass them around.  The general idiom
 * was, "load this image file and give me a `GdkPixbuf` object", which is basically a pixel
 * buffer.  Librsvg supports this kind of interface to load and render SVG documents, but
 * it is deprecated in favor of rendering to Cairo contexts.
 */

/**
 * rsvg_pixbuf_from_file:
 * @filename: A file name
 * @error: return location for a `GError`
 *
 * Loads a new `GdkPixbuf` from @filename and returns it.  The caller must
 * assume the reference to the reurned pixbuf. If an error occurred, @error is
 * set and `NULL` is returned.
 *
 * Returns: (transfer full) (nullable): A pixbuf, or %NULL on error.
 * Deprecated: Use [ctor@Rsvg.Handle.new_from_file] and [method@Rsvg.Handle.render_document] instead.
 **/
RSVG_DEPRECATED
GdkPixbuf *rsvg_pixbuf_from_file (const gchar *filename,
                                  GError     **error);

/**
 * rsvg_pixbuf_from_file_at_zoom:
 * @filename: A file name
 * @x_zoom: The horizontal zoom factor
 * @y_zoom: The vertical zoom factor
 * @error: return location for a `GError`
 *
 * Loads a new `GdkPixbuf` from @filename and returns it.  This pixbuf is scaled
 * from the size indicated by the file by a factor of @x_zoom and @y_zoom.  The
 * caller must assume the reference to the returned pixbuf. If an error
 * occurred, @error is set and `NULL` is returned.
 *
 * Returns: (transfer full) (nullable): A pixbuf, or %NULL on error.
 * Deprecated: Use [ctor@Rsvg.Handle.new_from_file] and [method@Rsvg.Handle.render_document] instead.
 **/
RSVG_DEPRECATED
GdkPixbuf *rsvg_pixbuf_from_file_at_zoom (const gchar *filename,
                                          double       x_zoom,
                                          double       y_zoom,
                                          GError     **error);

/**
 * rsvg_pixbuf_from_file_at_size:
 * @filename: A file name
 * @width: The new width, or -1
 * @height: The new height, or -1
 * @error: return location for a `GError`
 *
 * Loads a new `GdkPixbuf` from @filename and returns it.  This pixbuf is scaled
 * from the size indicated to the new size indicated by @width and @height.  If
 * both of these are -1, then the default size of the image being loaded is
 * used.  The caller must assume the reference to the returned pixbuf. If an
 * error occurred, @error is set and `NULL` is returned.
 *
 * Returns: (transfer full) (nullable): A pixbuf, or %NULL on error.
 * Deprecated: Use [ctor@Rsvg.Handle.new_from_file] and [method@Rsvg.Handle.render_document] instead.
 **/
RSVG_DEPRECATED
GdkPixbuf *rsvg_pixbuf_from_file_at_size (const gchar *filename,
                                          gint         width,
                                          gint         height,
                                          GError     **error);

/**
 * rsvg_pixbuf_from_file_at_max_size:
 * @filename: A file name
 * @max_width: The requested max width
 * @max_height: The requested max height
 * @error: return location for a `GError`
 *
 * Loads a new `GdkPixbuf` from @filename and returns it.  This pixbuf is uniformly
 * scaled so that the it fits into a rectangle of size `max_width * max_height`. The
 * caller must assume the reference to the returned pixbuf. If an error occurred,
 * @error is set and `NULL` is returned.
 *
 * Returns: (transfer full) (nullable): A pixbuf, or %NULL on error.
 * Deprecated: Use [ctor@Rsvg.Handle.new_from_file] and [method@Rsvg.Handle.render_document] instead.
 **/
RSVG_DEPRECATED
GdkPixbuf *rsvg_pixbuf_from_file_at_max_size (const gchar *filename,
                                              gint         max_width,
                                              gint         max_height,
                                              GError     **error);
/**
 * rsvg_pixbuf_from_file_at_zoom_with_max:
 * @filename: A file name
 * @x_zoom: The horizontal zoom factor
 * @y_zoom: The vertical zoom factor
 * @max_width: The requested max width
 * @max_height: The requested max height
 * @error: return location for a `GError`
 *
 * Loads a new `GdkPixbuf` from @filename and returns it.  This pixbuf is scaled
 * from the size indicated by the file by a factor of @x_zoom and @y_zoom. If the
 * resulting pixbuf would be larger than max_width/max_heigh it is uniformly scaled
 * down to fit in that rectangle. The caller must assume the reference to the
 * returned pixbuf. If an error occurred, @error is set and `NULL` is returned.
 *
 * Returns: (transfer full) (nullable): A pixbuf, or %NULL on error.
 * Deprecated: Use [ctor@Rsvg.Handle.new_from_file] and [method@Rsvg.Handle.render_document] instead.
 **/
RSVG_DEPRECATED
GdkPixbuf *rsvg_pixbuf_from_file_at_zoom_with_max (const gchar *filename,
                                                   double       x_zoom,
                                                   double       y_zoom,
                                                   gint         max_width,
                                                   gint         max_height,
                                                   GError     **error);

/* END deprecated APIs. */

G_END_DECLS

#endif
