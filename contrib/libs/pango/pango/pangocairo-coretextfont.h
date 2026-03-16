/* Pango
 * pangocairo-coretextfont.c
 *
 * Copyright (C) 2005 Imendio AB
 * Copyright (C) 2010  Kristian Rietveld  <kris@gtk.org>
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

#ifndef __PANGOCAIRO_CORETEXTFONT_H__
#define __PANGOCAIRO_CORETEXTFONT_H__

#define PANGO_TYPE_CAIRO_CORE_TEXT_FONT            (pango_cairo_core_text_font_get_type ())
#define PANGO_CAIRO_CORE_TEXT_FONT(object)         (G_TYPE_CHECK_INSTANCE_CAST ((object), PANGO_TYPE_CAIRO_CORE_TEXT_FONT, PangoCairoCoreTextFont))
#define PANGO_CAIRO_CORE_TEXT_FONT_CLASS(klass)    (G_TYPE_CHECK_CLASS_CAST ((klass), PANGO_TYPE_CAIRO_CORE_TEXT_FONT, PangoCairoCoreTextFontClass))
#define PANGO_IS_CAIRO_CORE_TEXT_FONT_CLASS(klass) (G_TYPE_CHECK_CLASS_TYPE ((klass), PANGO_TYPE_CAIRO_CORE_TEXT_FONT))
#define PANGO_CAIRO_CORE_TEXT_FONT_GET_CLASS(obj)  (G_TYPE_INSTANCE_GET_CLASS ((obj), PANGO_TYPE_CAIRO_CORE_TEXT_FONT, PangoCairoCoreTextFontClass))

typedef struct _PangoCairoCoreTextFont      PangoCairoCoreTextFont;
typedef struct _PangoCairoCoreTextFontClass PangoCairoCoreTextFontClass;

_PANGO_EXTERN
GType pango_cairo_core_text_font_get_type (void) G_GNUC_CONST;

#endif /* __PANGOCAIRO_CORETEXTFONT_H__ */
