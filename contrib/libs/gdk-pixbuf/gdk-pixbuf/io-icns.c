/* Mac OS X .icns icons loader
 *
 * Copyright (c) 2007 Lyonel Vincent <lyonel@ezix.org>
 * Copyright (c) 2007 Bastien Nocera <hadess@hadess.net>
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

#ifndef _WIN32
#define _GNU_SOURCE
#endif
#include <contrib/restricted/glib/config.h>

#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "gdk-pixbuf-private.h"
#include "gdk-pixbuf-loader.h"

G_MODULE_EXPORT void fill_vtable (GdkPixbufModule * module);
G_MODULE_EXPORT void fill_info (GdkPixbufFormat * info);

#define IN /**/
#define OUT /**/
#define INOUT /**/

struct IcnsBlockHeader
{
  char id[4];
  guint32 size;			/* caution: bigendian */
};
typedef struct IcnsBlockHeader IcnsBlockHeader;

typedef struct
{
  GdkPixbufModuleSizeFunc size_func;
  GdkPixbufModulePreparedFunc prepared_func;
  GdkPixbufModuleUpdatedFunc updated_func;
  gpointer user_data;

  GByteArray *byte_array;
  GdkPixbuf *pixbuf;      /* Our "target" */
} IcnsProgressiveState;

/*
 * load raw icon data from 'icns' resource
 *
 * returns TRUE when successful
 */
static gboolean
load_resources (unsigned size, IN gpointer data, gsize datalen,
		OUT guchar ** picture, OUT gsize * plen,
		OUT guchar ** mask, OUT gsize * mlen)
{
  IcnsBlockHeader *header = NULL;
  const char *bytes = NULL;
  const char *current = NULL;
  guint32 blocklen = 0;
  guint32 icnslen = 0;
  gboolean needs_mask = TRUE;

  if (datalen < 2 * sizeof (guint32))
    return FALSE;
  if (!data)
    return FALSE;

  *picture = *mask = NULL;
  *plen = *mlen = 0;

  bytes = data;
  header = (IcnsBlockHeader *) data;
  if (memcmp (header->id, "icns", 4) != 0)
    return FALSE;

  icnslen = GUINT32_FROM_BE (header->size);
  if ((icnslen > datalen) || (icnslen < 2 * sizeof (guint32)))
    return FALSE;

  current = bytes + sizeof (IcnsBlockHeader);
  while ((current - bytes < icnslen) && (icnslen - (current - bytes) >= sizeof (IcnsBlockHeader)))
    {
      header = (IcnsBlockHeader *) current;
      blocklen = GUINT32_FROM_BE (header->size);

      /* Check that blocklen isn't garbage */
      if (blocklen > icnslen - (current - bytes))
        return FALSE;

      switch (size)
	{
	case 256:
	case 512:
          if (memcmp (header->id, "ic08", 4) == 0	/* 256x256 icon */
              || memcmp (header->id, "ic09", 4) == 0)	/* 512x512 icon */
            {
	      *picture = (gpointer) (current + sizeof (IcnsBlockHeader));
	      *plen = blocklen - sizeof (IcnsBlockHeader);
	    }
	    needs_mask = FALSE;
	  break;
	case 128:
	  if (memcmp (header->id, "it32", 4) == 0)	/* 128x128 icon */
	    {
	      *picture = (gpointer) (current + sizeof (IcnsBlockHeader));
	      *plen = blocklen - sizeof (IcnsBlockHeader);
	      if (memcmp (*picture, "\0\0\0\0", 4) == 0)
		{
		  *picture += 4;
		  *plen -= 4;
		}
	    }
	  if (memcmp (header->id, "t8mk", 4) == 0)	/* 128x128 mask */
	    {
	      *mask = (gpointer) (current + sizeof (IcnsBlockHeader));
	      *mlen = blocklen - sizeof (IcnsBlockHeader);
	    }
	  break;
	case 48:
	  if (memcmp (header->id, "ih32", 4) == 0)	/* 48x48 icon */
	    {
	      *picture = (gpointer) (current + sizeof (IcnsBlockHeader));
	      *plen = blocklen - sizeof (IcnsBlockHeader);
	    }
	  if (memcmp (header->id, "h8mk", 4) == 0)	/* 48x48 mask */
	    {
	      *mask = (gpointer) (current + sizeof (IcnsBlockHeader));
	      *mlen = blocklen - sizeof (IcnsBlockHeader);
	    }
	  break;
	case 32:
	  if (memcmp (header->id, "il32", 4) == 0)	/* 32x32 icon */
	    {
	      *picture = (gpointer) (current + sizeof (IcnsBlockHeader));
	      *plen = blocklen - sizeof (IcnsBlockHeader);
	    }
	  if (memcmp (header->id, "l8mk", 4) == 0)	/* 32x32 mask */
	    {
	      *mask = (gpointer) (current + sizeof (IcnsBlockHeader));
	      *mlen = blocklen - sizeof (IcnsBlockHeader);
	    }
	  break;
	case 16:
	  if (memcmp (header->id, "is32", 4) == 0)	/* 16x16 icon */
	    {
	      *picture = (gpointer) (current + sizeof (IcnsBlockHeader));
	      *plen = blocklen - sizeof (IcnsBlockHeader);
	    }
	  if (memcmp (header->id, "s8mk", 4) == 0)	/* 16x16 mask */
	    {
	      *mask = (gpointer) (current + sizeof (IcnsBlockHeader));
	      *mlen = blocklen - sizeof (IcnsBlockHeader);
	    }
	  break;
	default:
	  return FALSE;
	}

      current += blocklen;
    }

  if (!*picture)
    return FALSE;
  if (needs_mask && !*mask)
    return FALSE;
  return TRUE;
}

/*
 * uncompress RLE-encoded bytes into RGBA scratch zone:
 * if firstbyte >= 0x80, it indicates the number of identical bytes + 125
 * 	(repeated value is stored next: 1 byte)
 * otherwise, it indicates the number of non-repeating bytes - 1
 *	(non-repeating values are stored next: n bytes)
 */
static gboolean
uncompress (unsigned size, INOUT guchar ** source, OUT guchar * target, INOUT gsize * _remaining)
{
  guchar *data = *source;
  gsize remaining;
  gsize i = 0;

  /* The first time we're called, set remaining */
  if (*_remaining == 0) {
    remaining = size * size;
  } else {
    remaining = *_remaining;
  }

  while (remaining > 0)
    {
      guint8 count = 0;

      if (data[0] & 0x80)	/* repeating byte */
	{
	  count = data[0] - 125;

	  if (count > remaining)
	    return FALSE;

	  for (i = 0; i < count; i++)
	    {
	      *target = data[1];
	      target += 4;
	    }

	  data += 2;
	}
      else			/* non-repeating bytes */
	{
	  count = data[0] + 1;

	  if (count > remaining)
	    return FALSE;

	  for (i = 0; i < count; i++)
	    {
	      *target = data[i + 1];
	      target += 4;
	    }
	  data += count + 1;
	}

      remaining -= count;
    }

  *source = data;
  *_remaining = remaining;
  return TRUE;
}

static GdkPixbuf *
load_icon (unsigned size, IN gpointer data, gsize datalen)
{
  guchar *icon = NULL;
  guchar *mask = NULL;
  gsize isize = 0, msize = 0, i;
  guchar *image = NULL;

  if (!load_resources (size, data, datalen, &icon, &isize, &mask, &msize))
    return NULL;

  /* 256x256 icons don't use RLE or uncompressed data,
   * They're usually JPEG 2000 images */
  if (size == 256)
    {
      GdkPixbufLoader *loader;
      GdkPixbuf *pixbuf;

      loader = gdk_pixbuf_loader_new ();
      if (!gdk_pixbuf_loader_write (loader, icon, isize, NULL)
	  || !gdk_pixbuf_loader_close (loader, NULL))
        {
          g_object_unref (loader);
          return NULL;
	}

      pixbuf = gdk_pixbuf_loader_get_pixbuf (loader);
      g_object_ref (pixbuf);
      g_object_unref (loader);

      return pixbuf;
    }

  g_assert (mask);

  if (msize != size * size)	/* wrong mask size */
    return NULL;

  image = (guchar *) g_try_malloc0 (size * size * 4);	/* 4 bytes/pixel = RGBA */

  if (!image)
    return NULL;

  if (isize == size * size * 4)	/* icon data is uncompressed */
    for (i = 0; i < size * size; i++)	/* 4 bytes/pixel = ARGB (A: ignored) */
      {
	image[i * 4] = icon[4 * i + 1];	/* R */
	image[i * 4 + 1] = icon[4 * i + 2];	/* G */
	image[i * 4 + 2] = icon[4 * i + 3];	/* B */
      }
  else
    {
      guchar *data = icon;
      gsize remaining = 0;

      /* R */
      if (!uncompress (size, &data, image, &remaining))
        goto bail;
      /* G */
      if (!uncompress (size, &data, image + 1, &remaining))
        goto bail;
      /* B */
      if (!uncompress (size, &data, image + 2, &remaining))
        goto bail;
    }

  for (i = 0; i < size * size; i++)	/* copy mask to alpha channel */
    image[i * 4 + 3] = mask[i];

  return gdk_pixbuf_new_from_data ((guchar *) image, GDK_COLORSPACE_RGB,	/* RGB image */
				   TRUE,	/* with alpha channel */
				   8,	/* 8 bits per sample */
				   size,	/* width */
				   size,	/* height */
				   size * 4,	/* no gap between rows */
				   (GdkPixbufDestroyNotify)g_free,	/* free() function */
				   NULL);	/* param to free() function */

bail:
  g_free (image);
  return NULL;
}

static int sizes[] = {
  256, /* late-Tiger icons */
  128, /* Standard OS X */
  48,  /* Not very common */
  32,  /* Standard Mac OS Classic (8 & 9) */
  24,  /* OS X toolbars */
  16   /* used in Mac OS Classic and dialog boxes */
};

static GdkPixbuf *
icns_image_load (FILE *f, GError ** error)
{
  GByteArray *data;
  GdkPixbuf *pixbuf = NULL;
  guint i;

  data = g_byte_array_new ();
  while (!feof (f))
    {
      gint save_errno;
      guchar buf[4096];
      gsize bytes;

      bytes = fread (buf, 1, sizeof (buf), f);
      save_errno = errno;
      data = g_byte_array_append (data, buf, bytes);

      if (ferror (f))
        {
	  g_set_error (error,
		       G_FILE_ERROR,
		       g_file_error_from_errno (save_errno),
		       _("Error reading ICNS image: %s"),
		       g_strerror (save_errno));

	  g_byte_array_free (data, TRUE);

	  return NULL;
	}
    }

  for (i = 0; i < G_N_ELEMENTS(sizes) && !pixbuf; i++)
    pixbuf = load_icon (sizes[i], data->data, data->len);

  g_byte_array_free (data, TRUE);

  if (!pixbuf)
    g_set_error_literal (error, GDK_PIXBUF_ERROR,
                         GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                         _("Could not decode ICNS file"));

  return pixbuf;
}

static void
context_free (IcnsProgressiveState *context)
{
  g_byte_array_free (context->byte_array, TRUE);
  g_clear_object (&context->pixbuf);
  g_free (context);
}

static gpointer
gdk_pixbuf__icns_image_begin_load (GdkPixbufModuleSizeFunc      size_func,
				   GdkPixbufModulePreparedFunc  prepared_func,
				   GdkPixbufModuleUpdatedFunc   updated_func,
				   gpointer                     user_data,
				   GError                     **error)
{
  IcnsProgressiveState *context;

  context = g_new0 (IcnsProgressiveState, 1);
  context->size_func = size_func;
  context->prepared_func = prepared_func;
  context->updated_func = updated_func;
  context->user_data = user_data;
  context->byte_array = g_byte_array_new ();

  return context;
}

static gboolean
gdk_pixbuf__icns_image_stop_load (gpointer   data,
                                  GError   **error)
{
  IcnsProgressiveState *context = data;

  g_return_val_if_fail (context != NULL, TRUE);

  context_free (context);
  return TRUE;
}

static gboolean
gdk_pixbuf__icns_image_load_increment (gpointer       data,
                                       const guchar  *buf,
                                       guint          size,
                                       GError       **error)
{
  IcnsProgressiveState *context = data;
  int i;
  int filesize;
  gint w, h;

  context->byte_array = g_byte_array_append (context->byte_array, buf, size);

  if (context->byte_array->len < 8)
    return TRUE;

  filesize = (context->byte_array->data[4] << 24) |
    (context->byte_array->data[5] << 16) |
    (context->byte_array->data[6] << 8) |
    (context->byte_array->data[7]);

  if (context->byte_array->len < filesize)
    return TRUE;

  for (i = 0; i < G_N_ELEMENTS(sizes) && !context->pixbuf; i++)
    context->pixbuf = load_icon (sizes[i],
				 context->byte_array->data,
				 context->byte_array->len);

  if (!context->pixbuf)
    {
      g_set_error_literal (error, GDK_PIXBUF_ERROR,
                           GDK_PIXBUF_ERROR_CORRUPT_IMAGE,
                           _("Could not decode ICNS file"));
      return FALSE;
    }

  w = gdk_pixbuf_get_width (context->pixbuf);
  h = gdk_pixbuf_get_height (context->pixbuf);

  if (context->size_func != NULL)
    (*context->size_func) (&w,
			   &h,
			   context->user_data);

  if (context->prepared_func != NULL)
    (*context->prepared_func) (context->pixbuf,
			       NULL,
			       context->user_data);

  if (context->updated_func != NULL)
    (*context->updated_func) (context->pixbuf,
			      0,
			      0,
			      gdk_pixbuf_get_width (context->pixbuf),
			      gdk_pixbuf_get_height (context->pixbuf),
			      context->user_data);

  return TRUE;
}

#ifndef INCLUDE_icns
#define MODULE_ENTRY(function) G_MODULE_EXPORT void function
#else
#define MODULE_ENTRY(function) void _gdk_pixbuf__icns_ ## function
#endif

MODULE_ENTRY (fill_vtable) (GdkPixbufModule * module)
{
  module->load = icns_image_load;
  module->begin_load = gdk_pixbuf__icns_image_begin_load;
  module->stop_load = gdk_pixbuf__icns_image_stop_load;
  module->load_increment = gdk_pixbuf__icns_image_load_increment;
}

MODULE_ENTRY (fill_info) (GdkPixbufFormat * info)
{
  static const GdkPixbufModulePattern signature[] = {
    {"icns", NULL, 100},	/* file begins with 'icns' */
    {NULL, NULL, 0}
  };
  static const gchar *mime_types[] = {
    "image/x-icns",
    NULL
  };
  static const gchar *extensions[] = {
    "icns",
    NULL
  };

  info->name = "icns";
  info->signature = (GdkPixbufModulePattern *) signature;
  info->description = NC_("image format", "MacOS X icon");
  info->mime_types = (gchar **) mime_types;
  info->extensions = (gchar **) extensions;
  info->flags = GDK_PIXBUF_FORMAT_THREADSAFE;
  info->license = "GPL";
  info->disabled = FALSE;
}

