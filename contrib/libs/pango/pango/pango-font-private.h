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

#ifndef __PANGO_FONT_PRIVATE_H__
#define __PANGO_FONT_PRIVATE_H__

#include <pango/pango-font.h>
#include <pango/pango-coverage.h>
#include <pango/pango-types.h>

#include <glib-object.h>

G_BEGIN_DECLS

PANGO_AVAILABLE_IN_ALL
PangoFontMetrics *pango_font_metrics_new (void);

typedef struct {
  PangoLanguage ** (* get_languages) (PangoFont *font);

  gboolean         (* is_hinted) (PangoFont *font);

  void             (* get_scale_factors) (PangoFont *font,
                                          double    *x_scale,
                                          double    *y_scale);

  gboolean         (* has_char) (PangoFont *font,
                                 gunichar   wc);
  PangoFontFace *  (* get_face) (PangoFont *font);
  void             (* get_matrix) (PangoFont   *font,
                                   PangoMatrix *matrix);
  int              (* get_absolute_size) (PangoFont *font);
  PangoVariant     (* get_variant) (PangoFont *font);
} PangoFontClassPrivate;

gboolean pango_font_is_hinted         (PangoFont *font);
void     pango_font_get_scale_factors (PangoFont *font,
                                       double    *x_scale,
                                       double    *y_scale);
void     pango_font_get_matrix        (PangoFont   *font,
                                       PangoMatrix *matrix);

static inline int pango_font_get_absolute_size (PangoFont *font)
{
  GTypeClass *klass = (GTypeClass *) PANGO_FONT_GET_CLASS (font);
  PangoFontClassPrivate *priv = (PangoFontClassPrivate *) g_type_class_get_private (klass, PANGO_TYPE_FONT);
  return priv->get_absolute_size (font);
}

static inline PangoVariant
pango_font_get_variant (PangoFont *font)
{
  GTypeClass *klass = (GTypeClass *) PANGO_FONT_GET_CLASS (font);
  PangoFontClassPrivate *priv = (PangoFontClassPrivate *) g_type_class_get_private (klass, PANGO_TYPE_FONT);
  if (priv->get_variant)
    return priv->get_variant (font);
  else
    {
      PangoFontDescription *desc;
      PangoVariant variant;

      desc = pango_font_describe (font);
      variant = pango_font_description_get_variant (desc);
      pango_font_description_free (desc);

      return variant;
    }
}

G_END_DECLS

#endif /* __PANGO_FONT_PRIVATE_H__ */
