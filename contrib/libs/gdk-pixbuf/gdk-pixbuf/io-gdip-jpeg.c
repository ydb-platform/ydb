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

#define INITGUID
#include "io-gdip-utils.h"

DEFINE_GUID(EncoderQuality, 0x1d5be4b5,0xfa4a,0x452d,0x9c,0xdd,0x5d,0xb3,0x51,0x05,0xe7,0xeb);

static gboolean
gdk_pixbuf__gdip_image_save_JPEG_to_callback (GdkPixbufSaveFunc   save_func,
                                              gpointer            user_data,
                                              GdkPixbuf          *pixbuf,
                                              gchar             **keys,
                                              gchar             **values,
                                              GError            **error)
{
  EncoderParameters encoder_params;
  LONG quality = 75; /* default; must be between 0 and 100 */

  if (keys && *keys) {
    gchar **kiter = keys;
    gchar **viter = values;
    
    while (*kiter) {
      if (strcmp (*kiter, "quality") == 0) {
        char *endptr = NULL;
        quality = strtol (*viter, &endptr, 10);
        
        if (endptr == *viter) {
          g_set_error (error,
                       GDK_PIXBUF_ERROR,
                       GDK_PIXBUF_ERROR_BAD_OPTION,
                       _("JPEG quality must be a value between 0 and 100; value '%s' could not be parsed."),
                       *viter);
          
          return FALSE;
        }
        
        if (quality < 0 ||
            quality > 100) {
          /* This is a user-visible error;
           * lets people skip the range-checking
           * in their app.
           */
          g_set_error (error,
                       GDK_PIXBUF_ERROR,
                       GDK_PIXBUF_ERROR_BAD_OPTION,
                       _("JPEG quality must be a value between 0 and 100; value '%d' is not allowed."),
                       (int)quality);
          
          return FALSE;
        }
      } else {
        g_warning ("Unrecognized parameter (%s) passed to JPEG saver.", *kiter);
      }
      
      ++kiter;
      ++viter;
    }
  }

  encoder_params.Count = 1;
  encoder_params.Parameter[0].Guid = EncoderQuality;
  encoder_params.Parameter[0].Type = EncoderParameterValueTypeLong;
  encoder_params.Parameter[0].NumberOfValues = 1;
  encoder_params.Parameter[0].Value = &quality;
     
  return gdip_save_pixbuf (pixbuf, L"image/jpeg", &encoder_params, save_func, user_data, error);
}

static gboolean
gdk_pixbuf__gdip_image_save_JPEG (FILE         *f,
                                 GdkPixbuf     *pixbuf,
                                 gchar        **keys,
                                 gchar        **values,
                                 GError       **error)
{
  return gdk_pixbuf__gdip_image_save_JPEG_to_callback (gdip_save_to_file_callback, f, pixbuf, keys, values, error);
}

#ifndef INCLUDE_gdiplus
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__gdip_jpeg_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule *module)
{
  gdip_fill_vtable (module);

  module->save_to_callback = gdk_pixbuf__gdip_image_save_JPEG_to_callback;
  module->save = gdk_pixbuf__gdip_image_save_JPEG; /* for gtk < 2.14, you need to implement both. otherwise gdk-pixbuf-queryloaders fails */
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat *info)
{
  static const GdkPixbufModulePattern signature[] = {
    { "\xff\xd8", NULL, 100 }, /* JPEG */
    { NULL, NULL, 0 }
  };

  static const gchar *mime_types[] = {
    "image/jpeg",
    NULL
  };

  static const gchar *extensions[] = {
    "jpeg",
    "jpe",
    "jpg",
    NULL
  };

  info->name        = "jpeg";
  info->signature   = (GdkPixbufModulePattern *) signature;
  info->description = NC_("image format", "JPEG");
  info->mime_types  = (gchar **) mime_types;
  info->extensions  = (gchar **) extensions;
  info->flags       = GDK_PIXBUF_FORMAT_WRITABLE | GDK_PIXBUF_FORMAT_THREADSAFE;
  info->license     = "LGPL";
}
