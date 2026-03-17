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
#include <contrib/restricted/glib/config.h>
#include <glib/gi18n-lib.h>

#include <ole2.h>

#include "io-gdip-utils.h"
#include "io-gdip-native.h"
#include "io-gdip-propertytags.h"
#include "io-gdip-animation.h"

#define LOAD_BUFFER_SIZE 65536

struct _GdipContext {
  GdkPixbufModuleUpdatedFunc  updated_func;
  GdkPixbufModulePreparedFunc prepared_func;
  GdkPixbufModuleSizeFunc     size_func;

  gpointer                    user_data;
  GByteArray                 *buffer;
  IStream                    *stream;
  HGLOBAL                     hg;
};
typedef struct _GdipContext GdipContext;

DEFINE_GUID(FrameDimensionTime, 0x6aedbd6d,0x3fb5,0x418a,0x83,0xa6,0x7f,0x45,0x22,0x9d,0xc8,0x72);
DEFINE_GUID(FrameDimensionPage, 0x7462dc86,0x6180,0x4c7e,0x8e,0x3f,0xee,0x73,0x33,0xa7,0xa4,0x83);

static void
gdip_set_error_from_hresult (GError **error, gint code, HRESULT hr, const char *format)
{
  gchar *msg;
  
  msg = g_win32_error_message (hr);
  
  if (msg) {
    g_set_error (error, GDK_PIXBUF_ERROR, code, format, msg);
    g_free (msg);
  }
}

static void
gdip_set_error_from_gpstatus (GError **error, gint code, GpStatus status)
{
  const char *msg;

  switch (status)
    {
#define CASE(x) case x: msg = #x; break
    CASE (GenericError);
    CASE (InvalidParameter);
    CASE (OutOfMemory);
    CASE (ObjectBusy);
    CASE (InsufficientBuffer);
    CASE (NotImplemented);
    CASE (Win32Error);
    CASE (WrongState);
    CASE (Aborted);
    CASE (FileNotFound);
    CASE (ValueOverflow);
    CASE (AccessDenied);
    CASE (UnknownImageFormat);
    CASE (FontFamilyNotFound);
    CASE (FontStyleNotFound);
    CASE (NotTrueTypeFont);
    CASE (UnsupportedGdiplusVersion);
    CASE (GdiplusNotInitialized);
    CASE (PropertyNotFound);
    CASE (PropertyNotSupported);
    CASE (ProfileNotFound);
    default:
      msg = "Unknown error";
    }
  g_set_error_literal (error, GDK_PIXBUF_ERROR, code, msg);
}

static gboolean
gdip_init (void)
{
  GdiplusStartupInput input;
  ULONG_PTR gdiplusToken = 0;
  static gboolean beenhere = FALSE;

  if (beenhere)
    return TRUE; /* gdip_init() is idempotent */

  beenhere = TRUE;

  input.GdiplusVersion = 1;
  input.DebugEventCallback = NULL;
  input.SuppressBackgroundThread = input.SuppressExternalCodecs = FALSE;
  
  return (GdiplusStartup (&gdiplusToken, &input, NULL) == 0 ? TRUE : FALSE);
}

static gboolean
GetEncoderClsid (const WCHAR *format, CLSID *pClsid)
{
  UINT num, size;
  int j;
  ImageCodecInfo *pImageCodecInfo;
    
  if (Ok != GdipGetImageEncodersSize (&num, &size))
    return FALSE;
    
  pImageCodecInfo = (ImageCodecInfo *) g_malloc (size);
    
  if (Ok != GdipGetImageEncoders (num, size, pImageCodecInfo)) {
    g_free (pImageCodecInfo);
    return FALSE;
  }

  for (j = 0; j < num; j++) {
    if (wcscmp (pImageCodecInfo[j].MimeType, format) == 0) {
      *pClsid = pImageCodecInfo[j].Clsid;
      g_free (pImageCodecInfo);
      return TRUE;
    }
  }
 
  g_free (pImageCodecInfo);

  return FALSE;
}

static HGLOBAL
gdip_buffer_to_hglobal (const gchar *buffer, size_t size, GError **error)
{
  HGLOBAL hg = NULL;

  hg = GlobalAlloc (GPTR, size);

  if (!hg) {
    gdip_set_error_from_hresult (error, GDK_PIXBUF_ERROR_FAILED, GetLastError (), _("Could not allocate memory: %s"));
    return NULL;
  }

  CopyMemory (hg, buffer, size);

  return hg;
}

static gboolean
gdip_save_bitmap_to_callback (GpBitmap *bitmap,
                              const CLSID *format,
                              const EncoderParameters *encoder_params,
                              GdkPixbufSaveFunc save_func,
                              gpointer user_data,
                              GError **error)
{
  HRESULT hr;  
  IStream *streamOut = NULL;
  gboolean success = FALSE;
  guint64 zero = 0;
  GpStatus status;

  hr = CreateStreamOnHGlobal (NULL, TRUE, &streamOut);
  if (!SUCCEEDED (hr)) {
    gdip_set_error_from_hresult (error, GDK_PIXBUF_ERROR_FAILED, hr, _("Could not create stream: %s"));
    return FALSE;
  }

  status = GdipSaveImageToStream ((GpImage *)bitmap, streamOut, format, encoder_params);
  if (Ok != status) {
    gdip_set_error_from_gpstatus (error, GDK_PIXBUF_ERROR_FAILED, status);
    IStream_Release (streamOut);
    return FALSE;
  }

  /* seek back to the beginning of the stream */
  hr = IStream_Seek (streamOut, *(LARGE_INTEGER *)&zero, STREAM_SEEK_SET, NULL);
  if (!SUCCEEDED (hr)) {
    gdip_set_error_from_hresult (error, GDK_PIXBUF_ERROR_FAILED, hr, _("Could not seek stream: %s"));
    IStream_Release (streamOut);
    return FALSE;
  }
  
  for (;;) {
    char buffer[LOAD_BUFFER_SIZE];
    ULONG nread;
    
    hr = IStream_Read (streamOut, buffer, sizeof(buffer), &nread);
    if (!SUCCEEDED (hr))
      {
        gdip_set_error_from_hresult (error, GDK_PIXBUF_ERROR_FAILED, hr, _("Could not read from stream: %s"));
        break;
      }
    else if (0 == nread) {
      success = TRUE; /* EOF */
      break;
    }
    else if (!(*save_func) (buffer, nread, error, user_data))
      break;
  }
  
  IStream_Release (streamOut);
  
  return success;
}                     

static GpBitmap *
gdip_pixbuf_to_bitmap (GdkPixbuf *pixbuf)
{
  GpBitmap *bitmap = NULL;

  int width, height, stride, n_channels;
  guint8 *pixels;

  width = gdk_pixbuf_get_width (pixbuf);
  height = gdk_pixbuf_get_height (pixbuf);
  stride = gdk_pixbuf_get_rowstride (pixbuf);
  n_channels = gdk_pixbuf_get_n_channels (pixbuf);
  pixels = gdk_pixbuf_get_pixels (pixbuf);

  if (n_channels == 3 || n_channels == 4) {
    /* rgbX. need to convert to argb. pass a null data to get an empty bitmap */
    GdipCreateBitmapFromScan0 (width, height, 0, PixelFormat32bppARGB, NULL, &bitmap);
    
    if (bitmap) {
      int x, y;
      
      for (y = 0; y < height; y++) {
        for (x = 0; x < width; x++) {
          ARGB p;
          guint8 alpha;
          guchar *base = pixels + (y * stride + (x * n_channels));
          
          if (n_channels == 4)
            alpha = base[3];
          else
            alpha = 0xff;
                  
          if (alpha == 0) 
            p = 0;
          else {
            guint8 red = base[0];
            guint8 green = base[1];
            guint8 blue = base[2];
            
            p = (alpha << 24) | (red << 16) | (green << 8) | (blue << 0);
          }
          
          GdipBitmapSetPixel (bitmap, x, y, p);
        }
      }
    }
  }
  else {
    g_warning ("Unsupported number of channels: %d\n", n_channels);
  }
  
  return bitmap;
}

static GpBitmap *
gdip_buffer_to_bitmap (GdipContext *context, GError **error)
{
  HRESULT hr;
  HGLOBAL hg = NULL;
  GpBitmap *bitmap = NULL;
  IStream *stream = NULL;
  GpStatus status;
  guint64 size64 = context->buffer->len;

  hg = gdip_buffer_to_hglobal (context->buffer->data, context->buffer->len, error);

  if (!hg)
    return NULL;

  hr = CreateStreamOnHGlobal (hg, FALSE, (LPSTREAM *)&stream);

  if (!SUCCEEDED (hr)) {
    gdip_set_error_from_hresult (error, GDK_PIXBUF_ERROR_FAILED, hr, _("Could not create stream: %s"));
    GlobalFree (hg);
    return NULL;
  }

  IStream_SetSize (stream, *(ULARGE_INTEGER *)&size64);

  status = GdipCreateBitmapFromStream (stream, &bitmap);

  if (Ok != status) {
    gdip_set_error_from_gpstatus (error, GDK_PIXBUF_ERROR_FAILED, status);
    IStream_Release (stream);
    GlobalFree (hg);
    return NULL;
  }

  context->stream = stream;
  context->hg = hg;

  return bitmap;
}

static GpImage *
gdip_buffer_to_image (GdipContext *context, GError **error)
{
  HRESULT hr;
  HGLOBAL hg = NULL;
  GpImage *image = NULL;
  IStream *stream = NULL;
  GpStatus status;
  guint64 size64 = context->buffer->len;

  hg = gdip_buffer_to_hglobal (context->buffer->data, context->buffer->len, error);

  if (!hg)
    return NULL;

  hr = CreateStreamOnHGlobal (hg, FALSE, (LPSTREAM *)&stream);

  if (!SUCCEEDED (hr)) {
    gdip_set_error_from_hresult (error, GDK_PIXBUF_ERROR_FAILED, hr, _("Could not create stream: %s"));
    GlobalFree (hg);
    return NULL;
  }
  
  IStream_SetSize (stream, *(ULARGE_INTEGER *)&size64);
  status = GdipLoadImageFromStream (stream, &image);

  if (Ok != status) {
    gdip_set_error_from_gpstatus (error, GDK_PIXBUF_ERROR_FAILED, status);
    IStream_Release (stream);
    GlobalFree (hg);
    return NULL;
  }

  context->stream = stream;
  context->hg = hg;

  return image;
}

static void
gdip_bitmap_get_size (GpBitmap *bitmap, guint *width, guint *height)
{
  if (bitmap == NULL || width == NULL || height == NULL)
    return;

  *width = *height = 0;

  GdipGetImageWidth ((GpImage *) bitmap, width);
  GdipGetImageHeight ((GpImage *) bitmap, height);
}

static void
gdip_bitmap_get_has_alpha (GpBitmap *bitmap, gboolean *has_alpha)
{
  guint flags = 0;

  if (bitmap == NULL || has_alpha == NULL)
    return;

  GdipGetImageFlags ((GpImage *) bitmap, &flags);
  *has_alpha = (flags & ImageFlagsHasAlpha);
}

static gboolean
gdip_bitmap_get_n_frames (GpBitmap *bitmap, guint *n_frames, gboolean timeDimension)
{
  if (bitmap == NULL || n_frames == NULL)
    return FALSE;

  *n_frames = 1;

  return (Ok == GdipImageGetFrameCount ((GpImage *) bitmap, (timeDimension ? &FrameDimensionTime : &FrameDimensionPage), n_frames));
}

static gboolean
gdip_bitmap_select_frame (GpBitmap *bitmap, guint frame, gboolean timeDimension)
{
  if (bitmap == NULL)
    return FALSE;

  return (Ok == GdipImageSelectActiveFrame ((GpImage *)bitmap, (timeDimension ? &FrameDimensionTime : &FrameDimensionPage), frame));
}

static gboolean
gdip_bitmap_get_property_as_string (GpBitmap *bitmap, guint propertyId, gchar **str)
{
  guint item_size;
  gboolean success = FALSE;

  if (bitmap == NULL || str == NULL)
    return FALSE;

  *str = 0;

  if (Ok == GdipGetPropertyItemSize ((GpImage *)bitmap, propertyId, &item_size)) {
    PropertyItem *item;
    
    item = (PropertyItem *)g_try_malloc (item_size);
    if (Ok == GdipGetPropertyItem ((GpImage *)bitmap, propertyId, item_size, item)) {
      GString *gstr;
      int i;
      
      gstr = g_string_new (NULL);
      
      success = TRUE;
      switch (item->type) {
      case PropertyTagTypeByte:
        for (i = 0; i < item->length / sizeof(guint8); i++) {
          guint8 *bytes = (guint8 *)item->value;
          
          if (gstr->len != 0)
            g_string_append_c(gstr, ',');
          g_string_append_printf (gstr, "%u", (guint32)bytes[i]);
        }
        break;
        
      case PropertyTagTypeASCII:
        g_string_append_len (gstr, (const char *)item->value, item->length);
        break;
        
      case PropertyTagTypeShort:
        for (i = 0; i < item->length / sizeof(guint16); i++) {
          guint16 *shorts = (guint16 *)item->value;
          
          if (gstr->len != 0)
            g_string_append_c (gstr, ',');
          g_string_append_printf (gstr, "%u", (guint32)shorts[i]);
        }
        break;
        
      case PropertyTagTypeLong:
        for (i = 0; i < item->length / sizeof(guint32); i++) {
          guint32 *longs = (guint32 *)item->value;
          
          if (gstr->len != 0)
            g_string_append_c (gstr, ',');
          g_string_append_printf (gstr, "%u", longs[i]);
        }
        break;

      case PropertyTagTypeSLONG:
        for (i = 0; i < item->length / sizeof(guint32); i++) {
          gint32 *longs = (gint32 *)item->value;
          
          if (gstr->len != 0)
            g_string_append_c (gstr, ',');
          g_string_append_printf (gstr, "%d", longs[i]);
        }
        break;
        
      default:
        success = FALSE;
        break;
      }
      
      if (gstr->len > 0)
        *str = g_string_free (gstr, FALSE);
      else
        g_string_free (gstr, TRUE);
    }
    
    g_free (item);
  }
  
  return success;
}

static gboolean
gdip_bitmap_get_frame_delay (GpBitmap *bitmap, guint frame, guint *delay)
{
  guint item_size, item_count;
  gboolean success = FALSE;

  if (bitmap == NULL || delay == NULL)
    return FALSE;

  *delay = 0;

  if (Ok == GdipGetPropertyItemSize ((GpImage *)bitmap, PropertyTagFrameDelay, &item_size)) {
    PropertyItem *item;
    
    item = (PropertyItem *)g_try_malloc (item_size);
    if (Ok == GdipGetPropertyItem ((GpImage *)bitmap, PropertyTagFrameDelay, item_size, item)) {
      item_count = item_size / sizeof(long);
      /* PropertyTagFrameDelay. Time delay, in hundredths of a second, between two frames in an animated GIF image. */
      *delay = ((long *)item->value)[(frame < item_count) ? frame : item_count - 1];
      success = TRUE;
    }
    
    g_free (item);
  }
  
  return success;
}

static gboolean
gdip_bitmap_get_n_loops (GpBitmap *bitmap, guint *loops)
{
  guint item_size;
  gboolean success = FALSE;

  if (bitmap == NULL || loops == NULL)
    return FALSE;

  *loops = 1;

  /* PropertyTagLoopCount. 0 == infinitely */
  if (Ok == GdipGetPropertyItemSize ((GpImage *)bitmap, PropertyTagLoopCount, &item_size)) {
    PropertyItem *item;
    
    item = (PropertyItem *)g_try_malloc (item_size);
    if (Ok == GdipGetPropertyItem ((GpImage *)bitmap, PropertyTagLoopCount, item_size, item)) {
      *loops = *((short *)item->value);
      success = TRUE;
    }
    
    g_free (item);
  }
  
  return success;
}

static void
destroy_gdipcontext (GdipContext *context)
{
  if (context != NULL) {
    if (context->stream != NULL) {
      IStream_Release(context->stream);
      GlobalFree (context->hg);
    }
    g_byte_array_free (context->buffer, TRUE);
    g_free (context);
  }
}

static void
emit_updated (GdipContext *context, GdkPixbuf *pixbuf)
{
  if (context->updated_func)
    (*context->updated_func) (pixbuf,
                              0, 0,
                              gdk_pixbuf_get_width (pixbuf),
                              gdk_pixbuf_get_height (pixbuf),
                              context->user_data);
}

static void
emit_prepared (GdipContext *context, GdkPixbuf *pixbuf, GdkPixbufAnimation *anim)
{
  if (context->prepared_func)
    (*context->prepared_func) (pixbuf, anim, context->user_data);
}

static gpointer
gdk_pixbuf__gdip_image_begin_load (GdkPixbufModuleSizeFunc size_func,
                                   GdkPixbufModulePreparedFunc prepared_func,
                                   GdkPixbufModuleUpdatedFunc  updated_func,
                                   gpointer user_data,
                                   GError **error)
{
  GdipContext *context = g_new0 (GdipContext, 1);

  context->size_func     = size_func;
  context->prepared_func = prepared_func;
  context->updated_func  = updated_func;
  context->user_data     = user_data;
  context->buffer        = g_byte_array_new ();

  return context;
}

static gboolean
gdk_pixbuf__gdip_image_load_increment (gpointer data,
                                       const guchar *buf, guint size,
                                       GError **error)
{
  GdipContext *context = (GdipContext *)data;
  GByteArray *image_buffer = context->buffer;

  g_byte_array_append (image_buffer, (guint8 *)buf, size);

  return TRUE;
}

static GdkPixbuf *
gdip_bitmap_to_pixbuf (GpBitmap *bitmap, GError **error)
{
  GdkPixbuf *pixbuf = NULL;
  guchar *cursor = NULL;
  gint rowstride;
  gboolean has_alpha = FALSE;
  gint n_channels = 0;
  gchar *option;

  guint width = 0, height = 0, x, y;

  gdip_bitmap_get_size (bitmap, &width, &height);
  gdip_bitmap_get_has_alpha (bitmap, &has_alpha);

  pixbuf = gdk_pixbuf_new (GDK_COLORSPACE_RGB, has_alpha, 8, width, height);

  if (!pixbuf) {
    g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_INSUFFICIENT_MEMORY, _("Couldn't load bitmap"));
    return NULL;
  }

  rowstride = gdk_pixbuf_get_rowstride (pixbuf);
  cursor = gdk_pixbuf_get_pixels (pixbuf);
  n_channels = gdk_pixbuf_get_n_channels (pixbuf);

  for (y = 0; y < height; y++) {
    for (x = 0; x < width; x++) {
      ARGB pixel;
      GpStatus status;
      guchar *b = cursor + (y * rowstride + (x * n_channels));
      
      if (Ok != (status = GdipBitmapGetPixel (bitmap, x, y, &pixel))) {
        gdip_set_error_from_gpstatus (error, GDK_PIXBUF_ERROR_FAILED, status);
        g_object_unref (pixbuf);
        return NULL;
      }
      
      b[0] = (pixel & 0xff0000) >> 16;
      b[1] = (pixel & 0x00ff00) >> 8;
      b[2] = (pixel & 0x0000ff) >> 0;
      
      if (has_alpha)      
        b[3] = (pixel & 0xff000000) >> 24;
    }
  }

  if (gdip_bitmap_get_property_as_string (bitmap, PropertyTagOrientation, &option)) {
    gdk_pixbuf_set_option (pixbuf, "orientation", option);
    g_free (option);
  }

  if (gdip_bitmap_get_property_as_string (bitmap, PropertyTagArtist, &option)) {
    gdk_pixbuf_set_option (pixbuf, "Author", option);
    g_free (option);
  }

  if (gdip_bitmap_get_property_as_string (bitmap, PropertyTagImageTitle, &option)) {
    gdk_pixbuf_set_option (pixbuf, "Title", option);
    g_free (option);
  }

  return pixbuf;
}

static gboolean
stop_load (GpBitmap *bitmap, GdipContext *context, GError **error)
{
  guint       n_frames = 1, i;
  GdkPixbufGdipAnim *animation = NULL;

  gdip_bitmap_get_n_frames (bitmap, &n_frames, TRUE);

  for (i = 0; i < n_frames; i++) {
    GdkPixbuf *pixbuf = NULL;
    GdkPixbufFrame *frame;
    guint frame_delay = 0;

    gdip_bitmap_select_frame (bitmap, i, TRUE);
    
    pixbuf = gdip_bitmap_to_pixbuf (bitmap, error);
    
    if (!pixbuf) {
      if (animation != NULL)
        g_object_unref (G_OBJECT (animation));

      GdipDisposeImage ((GpImage *)bitmap);
      destroy_gdipcontext (context);
      return FALSE;
    }
    
    if (animation == NULL) {
      guint n_loops = 1;

      animation = g_object_new (GDK_TYPE_PIXBUF_GDIP_ANIM, NULL);
      gdip_bitmap_get_n_loops (bitmap, &n_loops);
      animation->loop = n_loops;
    }

    frame = g_new (GdkPixbufFrame, 1);
    frame->pixbuf = pixbuf;

    gdip_bitmap_get_frame_delay (bitmap, i, &frame_delay);
  
    animation->n_frames++;
    animation->frames = g_list_append (animation->frames, frame);

    animation->width = gdk_pixbuf_get_width (pixbuf);
    animation->height = gdk_pixbuf_get_height (pixbuf);

    /* GIF delay is in hundredths, we want thousandths */
    frame->delay_time = frame_delay * 10;

    /* GIFs with delay time 0 are mostly broken, but they
     * just want a default, "not that fast" delay.
     */
    if (frame->delay_time == 0)
      frame->delay_time = 100;

    /* No GIFs gets to play faster than 50 fps. They just
     * lock up poor gtk.
     */
    else if (frame->delay_time < 20)
      frame->delay_time = 20; /* 20 = "fast" */

    frame->elapsed = animation->total_time;
    animation->total_time += frame->delay_time;

    if (i == 0)
      emit_prepared (context, pixbuf, GDK_PIXBUF_ANIMATION (animation));

    emit_updated (context, pixbuf);
  }

  if (animation != NULL)
    g_object_unref (G_OBJECT (animation));

  GdipDisposeImage ((GpImage *)bitmap);
  destroy_gdipcontext (context);
  
  return TRUE;
}

static gboolean
gdk_pixbuf__gdip_image_stop_load (gpointer data, GError **error)
{
  GdipContext *context = (GdipContext *)data;
  GpBitmap    *bitmap = NULL;

  bitmap = gdip_buffer_to_bitmap (context, error);

  if (!bitmap) {
    destroy_gdipcontext (context);
    return FALSE;
  }

  return stop_load (bitmap, context, error);
}

static gboolean
gdk_pixbuf__gdip_image_stop_vector_load (gpointer data, GError **error)
{
  GdipContext *context = (GdipContext *)data;

  GpImage *metafile;
  GpGraphics *graphics;
  GpBitmap *bitmap;
  GpStatus status;
  float metafile_xres, metafile_yres;
  guint width, height;

  metafile = gdip_buffer_to_image (context, error);
  if (!metafile) {
    destroy_gdipcontext (context);
    g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_CORRUPT_IMAGE, _("Couldn't load metafile"));
    return FALSE;
  }

  GdipGetImageWidth (metafile, &width);
  GdipGetImageHeight (metafile, &height);

  status = GdipCreateBitmapFromScan0 (width, height, 0, PixelFormat32bppARGB, NULL, &bitmap);
  if (Ok != status) {
    gdip_set_error_from_gpstatus (error, GDK_PIXBUF_ERROR_FAILED, status);
    GdipDisposeImage (metafile);
    
    return FALSE;
  }

  GdipGetImageHorizontalResolution (metafile, &metafile_xres);
  GdipGetImageVerticalResolution (metafile, &metafile_yres);
  GdipBitmapSetResolution (bitmap, metafile_xres, metafile_yres);

  status = GdipGetImageGraphicsContext ((GpImage *)bitmap, &graphics);
  if (Ok != status) {
    gdip_set_error_from_gpstatus (error, GDK_PIXBUF_ERROR_FAILED, status);
    GdipDisposeImage ((GpImage *)bitmap);
    GdipDisposeImage (metafile);
    
    return FALSE;
  }
  
  /* gotta clear the bitmap */
  GdipGraphicsClear (graphics, 0xffffffff);
  
  status = GdipDrawImageI (graphics, metafile, 0, 0);
  if (Ok != status) {
    gdip_set_error_from_gpstatus (error, GDK_PIXBUF_ERROR_FAILED, status);
    GdipDeleteGraphics (graphics);
    GdipDisposeImage ((GpImage *)bitmap);
    GdipDisposeImage (metafile);
    
    return FALSE;
  }
  
  GdipFlush (graphics, 1);
  
  GdipDeleteGraphics (graphics);
  GdipDisposeImage (metafile);

  return stop_load (bitmap, context, error);
}

gboolean
gdip_save_to_file_callback (const gchar *buf,
                            gsize        count,
                            GError     **error,
                            gpointer     data)
{
  FILE *filehandle = data;
  gsize n;
  
  n = fwrite (buf, 1, count, filehandle);
  if (n != count) {
    gint save_errno = errno;
    g_set_error (error,
                 G_FILE_ERROR,
                 g_file_error_from_errno (save_errno),
                 _("Error writing to image file: %s"),
                 g_strerror (save_errno));
    return FALSE;
  }
  
  return TRUE;
}

void
gdip_fill_vtable (GdkPixbufModule *module)
{
  if (gdip_init ()) {
    module->begin_load     = gdk_pixbuf__gdip_image_begin_load;
    module->stop_load      = gdk_pixbuf__gdip_image_stop_load;
    module->load_increment = gdk_pixbuf__gdip_image_load_increment;
  }
}

void
gdip_fill_vector_vtable (GdkPixbufModule *module)
{
  if (gdip_init ()) {
    module->begin_load     = gdk_pixbuf__gdip_image_begin_load;
    module->stop_load      = gdk_pixbuf__gdip_image_stop_vector_load;
    module->load_increment = gdk_pixbuf__gdip_image_load_increment;
  }
}

gboolean
gdip_save_pixbuf (GdkPixbuf *pixbuf,
                  const WCHAR *format,
                  const EncoderParameters *encoder_params,
                  GdkPixbufSaveFunc save_func,
                  gpointer user_data,
                  GError **error)
{
  GpBitmap *image;
  CLSID clsid;
  gboolean success;

  if (!GetEncoderClsid (format, &clsid)) {
    g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_FAILED, _("Unsupported image format for GDI+"));
    return FALSE;
  }
  
  image = gdip_pixbuf_to_bitmap (pixbuf);

  if (image == NULL) {
    g_set_error_literal (error, GDK_PIXBUF_ERROR, GDK_PIXBUF_ERROR_FAILED, _("Couldn't save"));
    return FALSE;
  }
  
  success = gdip_save_bitmap_to_callback (image, &clsid, encoder_params, save_func, user_data, error);

  GdipDisposeImage ((GpImage *)image);

  return success;
}
