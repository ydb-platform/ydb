/* Pango
 * pangox-private.h:
 *
 * Copyright (C) 2000 Red Hat Software
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

#ifndef __PANGOXFT_PRIVATE_H__
#define __PANGOXFT_PRIVATE_H__

#include <pango/pangoxft.h>
#include <pango/pangofc-font-private.h>
#include <pango/pango-renderer.h>

G_BEGIN_DECLS

struct _PangoXftFont
{
  PangoFcFont parent_instance;

  XftFont *xft_font;		    /* created on demand */
  PangoFont *mini_font;		    /* font used to display missing glyphs */

  guint mini_width;		    /* metrics for missing glyph drawing */
  guint mini_height;
  guint mini_pad;

  GHashTable *glyph_info;	    /* Used only when we can't get
				     * glyph extents out of Xft because
				     * we have a transformation in effect
				     */
};

PangoXftFont *_pango_xft_font_new          (PangoXftFontMap  *xftfontmap,
					    FcPattern        *pattern);

void          _pango_xft_font_map_get_info (PangoFontMap     *fontmap,
					    Display         **display,
					    int              *screen);

PangoRenderer *_pango_xft_font_map_get_renderer (PangoXftFontMap *xftfontmap);

PangoFont *_pango_xft_font_get_mini_font (PangoXftFont *xfont);

G_END_DECLS

#endif /* __PANGOXFT_PRIVATE_H__ */
