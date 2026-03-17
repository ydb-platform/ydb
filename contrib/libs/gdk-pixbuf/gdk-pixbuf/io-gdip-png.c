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

#define INITGUID
#include "io-gdip-utils.h"

DEFINE_GUID(EncoderCompression, 0xe09d739d,0xccd4,0x44ee,0x8e,0xba,0x3f,0xbf,0x8b,0xe4,0xfc,0x58);

static gboolean
gdk_pixbuf__gdip_image_save_PNG_to_callback (GdkPixbufSaveFunc   save_func,
                                             gpointer            user_data,
                                             GdkPixbuf          *pixbuf,
                                             gchar             **keys,
                                             gchar             **values,
                                             GError            **error)
{
  EncoderParameters encoder_params;
  LONG compression = 5;

  if (keys && *keys) {
    gchar **kiter = keys;
    gchar **viter = values;
    
    while (*kiter) {
      if (strncmp (*kiter, "tEXt::", 6) == 0) {
        /* TODO: support exif data and the like */
      }
      else if (strcmp (*kiter, "compression") == 0) {
        char *endptr = NULL;
        compression = strtol (*viter, &endptr, 10);
        
        if (endptr == *viter) {
          g_set_error (error,
                       GDK_PIXBUF_ERROR,
                       GDK_PIXBUF_ERROR_BAD_OPTION,
                       _("PNG compression level must be a value between 0 and 9; value '%s' could not be parsed."),
                       *viter);
          return FALSE;
        }
        if (compression < 0 || compression > 9) {
          /* This is a user-visible error;
           * lets people skip the range-checking
           * in their app.
           */
          g_set_error (error,
                       GDK_PIXBUF_ERROR,
                       GDK_PIXBUF_ERROR_BAD_OPTION,
                       _("PNG compression level must be a value between 0 and 9; value '%d' is not allowed."),
                       (int)compression);
          return FALSE;
        }       
      } else {
        g_warning ("Unrecognized parameter (%s) passed to PNG saver.", *kiter);
      }
      
      ++kiter;
      ++viter;
    }
  }

  encoder_params.Count = 1;
  encoder_params.Parameter[0].Guid = EncoderCompression;
  encoder_params.Parameter[0].Type = EncoderParameterValueTypeLong;
  encoder_params.Parameter[0].NumberOfValues = 1;
  encoder_params.Parameter[0].Value = &compression;

  return gdip_save_pixbuf (pixbuf, L"image/png", &encoder_params, save_func, user_data, error);
}

static gboolean
gdk_pixbuf__gdip_image_save_PNG (FILE          *f,
                                 GdkPixbuf     *pixbuf,
                                 gchar        **keys,
                                 gchar        **values,
                                 GError       **error)
{
  return gdk_pixbuf__gdip_image_save_PNG_to_callback (gdip_save_to_file_callback, f, pixbuf, keys, values, error);
}

#ifndef INCLUDE_gdip_png
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__gdip_png_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule *module)
{
  gdip_fill_vtable (module);

  module->save_to_callback = gdk_pixbuf__gdip_image_save_PNG_to_callback;
  module->save = gdk_pixbuf__gdip_image_save_PNG; /* for gtk < 2.14, you need to implement both. otherwise gdk-pixbuf-queryloaders fails */
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat *info)
{
  static const GdkPixbufModulePattern signature[] = {
    { "\x89PNG\r\n\x1a\x0a", NULL, 100 }, /* PNG */
    { NULL, NULL, 0 }
  };

  static const gchar *mime_types[] = {
    "image/png",
    NULL
  };

  static const gchar *extensions[] = {
    "png",
    NULL
  };

  info->name        = "png";
  info->signature   = (GdkPixbufModulePattern *) signature;
  info->description = NC_("image format", "PNG");
  info->mime_types  = (gchar **) mime_types;
  info->extensions  = (gchar **) extensions;
  info->flags       = GDK_PIXBUF_FORMAT_WRITABLE | GDK_PIXBUF_FORMAT_THREADSAFE;
  info->license     = "LGPL";
}
