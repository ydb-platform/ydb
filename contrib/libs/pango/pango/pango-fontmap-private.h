/* Pango
 * pango-font.h: Font handling
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

#ifndef __PANGO_FONTMAP_PRIVATE_H__
#define __PANGO_FONTMAP_PRIVATE_H__

#include <pango/pango-font-private.h>
#include <pango/pango-fontset.h>
#include <pango/pango-fontmap.h>

G_BEGIN_DECLS

typedef struct {
  PangoFont * (* reload_font) (PangoFontMap *fontmap,
                               PangoFont    *font,
                               double        scale,
                               PangoContext *context,
                               const char   *variations);

  gboolean (* add_font_file)  (PangoFontMap  *fontmap,
                               const char    *filename,
                               GError       **error);

} PangoFontMapClassPrivate;

PANGO_DEPRECATED_IN_1_38
const char   *pango_font_map_get_shape_engine_type (PangoFontMap *fontmap);

G_END_DECLS

#endif /* __PANGO_FONTMAP_PRIVATE_H__ */
