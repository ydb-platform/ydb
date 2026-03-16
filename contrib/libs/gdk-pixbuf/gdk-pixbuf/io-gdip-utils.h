/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 8 -*- */
/* GdkPixbuf library - Win32 GDI+ Pixbuf Loader
 *
 * Copyright (C) 2008 Dominic Lachowicz
 * Copyright (C) 2008 Alberto Ruiz
 *
 * Authors: Dominic Lachowicz <domlachowicz@gmail.com>
 *          Alberto Ruiz <aruiz@gnome.org>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more  * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see <http://www.gnu.org/licenses/>.
 */

#ifndef _HAVE_IO_GDIP_UTILS_H
#define _HAVE_IO_GDIP_UTILS_H

#include "gdk-pixbuf.h"

#include "io-gdip-native.h"

gboolean
gdip_save_to_file_callback (const gchar *buf,
                            gsize        count,
                            GError     **error,
                            gpointer     data);

void
gdip_fill_vtable (GdkPixbufModule *module);

void
gdip_fill_vector_vtable (GdkPixbufModule *module);

gboolean
gdip_save_pixbuf (GdkPixbuf *pixbuf,
                  const WCHAR *format,
                  const EncoderParameters *encoder_params,
                  GdkPixbufSaveFunc save_func,
                  gpointer user_data,
                  GError **error);

#endif
