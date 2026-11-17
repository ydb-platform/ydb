/* Pango
 * pangocairo-win32.h: Private header file for Cairo/Win32 combination
 *
 * Copyright (C) 2005 Red Hat, Inc.
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

#ifndef __PANGOCAIRO_WIN32_H__
#define __PANGOCAIRO_WIN32_H__

#include <pango/pangowin32.h>
#include "pangowin32-private.h"
#include <pango/pangocairo.h>

G_BEGIN_DECLS

#define PANGO_TYPE_CAIRO_WIN32_FONT_MAP       (pango_cairo_win32_font_map_get_type ())
#define PANGO_CAIRO_WIN32_FONT_MAP(object)    (G_TYPE_CHECK_INSTANCE_CAST ((object), PANGO_TYPE_CAIRO_WIN32_FONT_MAP, PangoCairoWin32FontMap))
#define PANGO_IS_CAIRO_WIN32_FONT_MAP(object) (G_TYPE_CHECK_INSTANCE_TYPE ((object), PANGO_TYPE_CAIRO_WIN32_FONT_MAP))

typedef struct _PangoCairoWin32FontMap PangoCairoWin32FontMap;

struct _PangoCairoWin32FontMap
{
  PangoWin32FontMap parent_instance;

  guint serial;
  double dpi;
};

PANGO_AVAILABLE_IN_ALL
GType pango_cairo_win32_font_map_get_type (void) G_GNUC_CONST;

PangoFont *_pango_cairo_win32_font_new (PangoCairoWin32FontMap       *cwfontmap,
					PangoContext                 *context,
					PangoWin32Face               *face,
					const PangoFontDescription   *desc);
G_END_DECLS

#endif /* __PANGOCAIRO_WIN32_H__ */

