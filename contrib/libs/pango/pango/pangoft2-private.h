/* Pango
 * pangoft2-private.h:
 *
 * Copyright (C) 1999 Red Hat Software
 * Copyright (C) 2000 Tor Lillqvist
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#ifndef __PANGOFT2_PRIVATE_H__
#define __PANGOFT2_PRIVATE_H__

#include <pango/pangoft2.h>
#include <pango/pangofc-fontmap-private.h>
#include <pango/pango-renderer.h>
#include <fontconfig/fontconfig.h>

/* Debugging... */
/*#define DEBUGGING 1*/

#if defined(DEBUGGING) && DEBUGGING
#ifdef __GNUC__
#define PING(printlist)					\
(g_print ("%s:%d ", __PRETTY_FUNCTION__, __LINE__),	\
 g_print printlist,					\
 g_print ("\n"))
#else
#define PING(printlist)					\
(g_print ("%s:%d ", __FILE__, __LINE__),		\
 g_print printlist,					\
 g_print ("\n"))
#endif
#else  /* !DEBUGGING */
#define PING(printlist)
#endif

typedef struct _PangoFT2Font      PangoFT2Font;
typedef struct _PangoFT2GlyphInfo PangoFT2GlyphInfo;
typedef struct _PangoFT2Renderer  PangoFT2Renderer;

struct _PangoFT2Font
{
  PangoFcFont font;

  FT_Face face;
  int load_flags;

  int size;

  GSList *metrics_by_lang;

  GHashTable *glyph_info;
  GDestroyNotify glyph_cache_destroy;
};

struct _PangoFT2GlyphInfo
{
  PangoRectangle logical_rect;
  PangoRectangle ink_rect;
  void *cached_glyph;
};

#define PANGO_TYPE_FT2_FONT              (pango_ft2_font_get_type ())
#define PANGO_FT2_FONT(object)           (G_TYPE_CHECK_INSTANCE_CAST ((object), PANGO_TYPE_FT2_FONT, PangoFT2Font))
#define PANGO_FT2_IS_FONT(object)        (G_TYPE_CHECK_INSTANCE_TYPE ((object), PANGO_TYPE_FT2_FONT))

_PANGO_EXTERN
GType pango_ft2_font_get_type (void) G_GNUC_CONST;

PangoFT2Font * _pango_ft2_font_new                (PangoFT2FontMap   *ft2fontmap,
						   FcPattern         *pattern);
FT_Library     _pango_ft2_font_map_get_library    (PangoFontMap      *fontmap);
void _pango_ft2_font_map_default_substitute (PangoFcFontMap *fcfontmap,
					     FcPattern      *pattern);

void *_pango_ft2_font_get_cache_glyph_data    (PangoFont      *font,
					       int             glyph_index);
void  _pango_ft2_font_set_cache_glyph_data    (PangoFont      *font,
					       int             glyph_index,
					       void           *cached_glyph);
void  _pango_ft2_font_set_glyph_cache_destroy (PangoFont      *font,
					       GDestroyNotify  destroy_notify);

#define PANGO_TYPE_FT2_RENDERER            (pango_ft2_renderer_get_type())
#define PANGO_FT2_RENDERER(object)         (G_TYPE_CHECK_INSTANCE_CAST ((object), PANGO_TYPE_FT2_RENDERER, PangoFT2Renderer))
#define PANGO_IS_FT2_RENDERER(object)      (G_TYPE_CHECK_INSTANCE_TYPE ((object), PANGO_TYPE_FT2_RENDERER))

_PANGO_EXTERN
GType pango_ft2_renderer_get_type    (void) G_GNUC_CONST;

PangoRenderer *_pango_ft2_font_map_get_renderer (PangoFT2FontMap *ft2fontmap);

#endif /* __PANGOFT2_PRIVATE_H__ */
