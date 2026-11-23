/* Pango
 * pangofc-font.h: Base fontmap type for fontconfig-based backends
 *
 * Copyright (C) 2003 Red Hat Software
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

#ifndef __PANGO_FC_FONT_PRIVATE_H__
#define __PANGO_FC_FONT_PRIVATE_H__

#include <pango/pangofc-font.h>
#include <pango/pango-font-private.h>

G_BEGIN_DECLS

/**
 * PANGO_RENDER_TYPE_FC:
 *
 * A string constant used to identify shape engines that work
 * with the fontconfig based backends. See the @engine_type field
 * of `PangoEngineInfo`.
 **/
#define PANGO_RENDER_TYPE_FC "PangoRenderFc"

#define PANGO_FC_FONT_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), PANGO_TYPE_FC_FONT, PangoFcFontClass))
#define PANGO_IS_FC_FONT_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), PANGO_TYPE_FC_FONT))
#define PANGO_FC_FONT_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), PANGO_TYPE_FC_FONT, PangoFcFontClass))

/**
 * PangoFcFontClass:
 * @lock_face: Returns the FT_Face of the font and increases
 *  the reference count for the face by one.
 * @unlock_face: Decreases the reference count for the
 *  FT_Face of the font by one. When the count is zero,
 *  the `PangoFcFont` subclass is allowed to free the
 *  FT_Face.
 * @has_char: Return %TRUE if the the font contains a glyph
 *   corresponding to the specified character.
 * @get_glyph: Gets the glyph that corresponds to the given
 *   Unicode character.
 * @get_unknown_glyph: (nullable): Gets the glyph that
 *   should be used to display an unknown-glyph indication
 *   for the specified Unicode character.  May be %NULL.
 * @shutdown: (nullable): Performs any font-specific
 *   shutdown code that needs to be done when
 *   pango_fc_font_map_shutdown is called.  May be %NULL.
 *
 * Class structure for `PangoFcFont`.
 **/
struct _PangoFcFontClass
{
  /*< private >*/
  PangoFontClass parent_class;

  /*< public >*/
  FT_Face    (*lock_face)         (PangoFcFont      *font);
  void       (*unlock_face)       (PangoFcFont      *font);
  gboolean   (*has_char)          (PangoFcFont      *font,
				   gunichar          wc);
  guint      (*get_glyph)         (PangoFcFont      *font,
				   gunichar          wc);
  PangoGlyph (*get_unknown_glyph) (PangoFcFont      *font,
				   gunichar          wc);
  void       (*shutdown)          (PangoFcFont      *font);
  /*< private >*/

  /* Padding for future expansion */
  void (*_pango_reserved1) (void);
  void (*_pango_reserved2) (void);
  void (*_pango_reserved3) (void);
  void (*_pango_reserved4) (void);
};


G_END_DECLS
#endif /* __PANGO_FC_FONT_PRIVATE_H__ */
