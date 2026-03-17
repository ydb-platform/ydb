/* Pango
 * pango-context-private.h: Internal structures of PangoContext
 *
 * Copyright (C) 2004 Red Hat Software
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Library General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#ifndef __PANGO_CONTEXT_PRIVATE_H__
#define __PANGO_CONTEXT_PRIVATE_H__

#include <pango/pango-context.h>

G_BEGIN_DECLS

struct _PangoContext
{
  GObject parent_instance;
  guint serial;
  guint fontmap_serial;

  PangoLanguage *set_language;
  PangoLanguage *language;
  PangoDirection base_dir;
  PangoGravity base_gravity;
  PangoGravity resolved_gravity;
  PangoGravityHint gravity_hint;

  PangoFontDescription *font_desc;

  PangoMatrix *matrix;

  PangoFontMap *font_map;

  PangoFontMetrics *metrics;

  gboolean round_glyph_positions;
};

G_END_DECLS

#endif /* __PANGO_CONTEXT_PRIVATE_H__ */

