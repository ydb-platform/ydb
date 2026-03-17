/* -*- Mode: C; tab-width: 8; indent-tabs-mode: nil; c-basic-offset: 2 -*- */
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

#include <contrib/restricted/glib/config.h>
#include <glib/gi18n-lib.h>

#include "io-gdip-utils.h"

#ifndef INCLUDE_gdiplus
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__gdip_emf_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule *module)
{
  gdip_fill_vector_vtable (module);
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat *info)
{
  static const GdkPixbufModulePattern signature[] = {
    { "\x01\x00\x00\x00", NULL, 100 }, /* EMF */
    { NULL, NULL, 0 }
  };

  static const gchar *mime_types[] = {
    "application/emf",
    "application/x-emf",
    "image/x-emf",
    "image/x-mgx-emf",
    NULL
  };

  static const gchar *extensions[] = {
    "emf",
    NULL
  };

  info->name        = "emf";
  info->signature   = (GdkPixbufModulePattern *) signature;
  info->description = NC_("image format", "EMF");
  info->mime_types  = (gchar **) mime_types;
  info->extensions  = (gchar **) extensions;
  info->flags       = GDK_PIXBUF_FORMAT_THREADSAFE;
  info->license     = "LGPL";
}
