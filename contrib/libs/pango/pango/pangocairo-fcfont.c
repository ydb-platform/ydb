/* Pango
 * pangocairofc-font.c: Cairo font handling, fontconfig backend
 *
 * Copyright (C) 2000-2005 Red Hat Software
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

#include "config.h"

/* FreeType has undefined macros in its header */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wundef"
#include <cairo-ft.h>
#pragma GCC diagnostic pop

#include "pangofc-fontmap-private.h"
#include "pangocairo-private.h"
#include "pangocairo-fc-private.h"
#include "pangofc-private.h"
#include "pango-impl-utils.h"

#include <hb-ot.h>
#include <freetype/ftmm.h>

#define PANGO_TYPE_CAIRO_FC_FONT           (pango_cairo_fc_font_get_type ())
#define PANGO_CAIRO_FC_FONT(object)        (G_TYPE_CHECK_INSTANCE_CAST ((object), PANGO_TYPE_CAIRO_FC_FONT, PangoCairoFcFont))
#define PANGO_CAIRO_FC_FONT_CLASS(klass)   (G_TYPE_CHECK_CLASS_CAST ((klass), PANGO_TYPE_CAIRO_FC_FONT, PangoCairoFcFontClass))
#define PANGO_CAIRO_IS_FONT_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), PANGO_TYPE_CAIRO_FC_FONT))
#define PANGO_CAIRO_FC_FONT_GET_CLASS(obj) (G_TYPE_INSTANCE_GET_CLASS ((obj), PANGO_TYPE_CAIRO_FC_FONT, PangoCairoFcFontClass))

typedef struct _PangoCairoFcFont      PangoCairoFcFont;
typedef struct _PangoCairoFcFontClass PangoCairoFcFontClass;

struct _PangoCairoFcFont
{
  PangoFcFont font;
  PangoCairoFontPrivate cf_priv;
};

struct _PangoCairoFcFontClass
{
  PangoFcFontClass  parent_class;
};

_PANGO_EXTERN
GType pango_cairo_fc_font_get_type (void);

/********************************
 *    Method implementations    *
 ********************************/

static cairo_font_face_t *
pango_cairo_fc_font_create_font_face (PangoCairoFont *cfont)
{
  PangoFcFont *fcfont = (PangoFcFont *) (cfont);

  return cairo_ft_font_face_create_for_pattern (fcfont->font_pattern);
}

static PangoFontMetrics *
pango_cairo_fc_font_create_base_metrics_for_context (PangoCairoFont *cfont,
						     PangoContext   *context)
{
  PangoCairoFcFont *cffont = (PangoCairoFcFont *) cfont;
  PangoFcFont *fcfont = (PangoFcFont *) cfont;
  PangoFontMetrics *metrics;

  metrics = pango_fc_font_create_base_metrics_for_context (fcfont, context);

  if (_pango_cairo_font_private_is_metrics_hinted (&cffont->cf_priv))
    {
      metrics->ascent = PANGO_PIXELS_CEIL (metrics->ascent) * PANGO_SCALE;
      metrics->descent = PANGO_PIXELS_CEIL (metrics->descent) * PANGO_SCALE;
      metrics->height = PANGO_PIXELS_CEIL (metrics->height) * PANGO_SCALE;
      metrics->underline_position = PANGO_PIXELS_CEIL (metrics->underline_position) * PANGO_SCALE;
      metrics->underline_thickness = PANGO_PIXELS_CEIL (metrics->underline_thickness) * PANGO_SCALE;
      metrics->strikethrough_position = PANGO_PIXELS_CEIL (metrics->strikethrough_position) * PANGO_SCALE;
      metrics->strikethrough_thickness = PANGO_PIXELS_CEIL (metrics->strikethrough_thickness) * PANGO_SCALE;
    }

  return metrics;
}

static void
cairo_font_iface_init (PangoCairoFontIface *iface)
{
  iface->create_font_face = pango_cairo_fc_font_create_font_face;
  iface->create_base_metrics_for_context = pango_cairo_fc_font_create_base_metrics_for_context;
  iface->cf_priv_offset = G_STRUCT_OFFSET (PangoCairoFcFont, cf_priv);
}

G_DEFINE_TYPE_WITH_CODE (PangoCairoFcFont, pango_cairo_fc_font, PANGO_TYPE_FC_FONT,
    { G_IMPLEMENT_INTERFACE (PANGO_TYPE_CAIRO_FONT, cairo_font_iface_init) })

static void
pango_cairo_fc_font_finalize (GObject *object)
{
  PangoCairoFcFont *cffont = (PangoCairoFcFont *) object;

  _pango_cairo_font_private_finalize (&cffont->cf_priv);

  G_OBJECT_CLASS (pango_cairo_fc_font_parent_class)->finalize (object);
}

/* we want get_glyph_extents extremely fast, so we use a small wrapper here
 * to avoid having to lookup the interface data like we do for get_metrics
 * in _pango_cairo_font_get_metrics(). */
static void
pango_cairo_fc_font_get_glyph_extents (PangoFont      *font,
				       PangoGlyph      glyph,
				       PangoRectangle *ink_rect,
				       PangoRectangle *logical_rect)
{
  PangoCairoFcFont *cffont = (PangoCairoFcFont *) (font);

  _pango_cairo_font_private_get_glyph_extents (&cffont->cf_priv,
					       glyph,
					       ink_rect,
					       logical_rect);
}

static FT_Face
pango_cairo_fc_font_lock_face (PangoFcFont *font)
{
  PangoCairoFcFont *cffont = (PangoCairoFcFont *) (font);
  cairo_scaled_font_t *scaled_font = _pango_cairo_font_private_get_scaled_font (&cffont->cf_priv);

  if (G_UNLIKELY (!scaled_font))
    return NULL;

  return cairo_ft_scaled_font_lock_face (scaled_font);
}

static void
pango_cairo_fc_font_unlock_face (PangoFcFont *font)
{
  PangoCairoFcFont *cffont = (PangoCairoFcFont *) (font);
  cairo_scaled_font_t *scaled_font = _pango_cairo_font_private_get_scaled_font (&cffont->cf_priv);

  if (G_UNLIKELY (!scaled_font))
    return;

  cairo_ft_scaled_font_unlock_face (scaled_font);
}

static void
pango_cairo_fc_font_class_init (PangoCairoFcFontClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);
  PangoFontClass *font_class = PANGO_FONT_CLASS (class);
  PangoFcFontClass *fc_font_class = PANGO_FC_FONT_CLASS (class);

  object_class->finalize = pango_cairo_fc_font_finalize;

  font_class->get_glyph_extents = pango_cairo_fc_font_get_glyph_extents;
  font_class->get_metrics = _pango_cairo_font_get_metrics;

  fc_font_class->lock_face = pango_cairo_fc_font_lock_face;
  fc_font_class->unlock_face = pango_cairo_fc_font_unlock_face;
}

static void
pango_cairo_fc_font_init (PangoCairoFcFont *cffont G_GNUC_UNUSED)
{
}

/********************
 *    Private API   *
 ********************/

static double
get_font_size (const FcPattern *pattern)
{
  double size;
  double dpi;

  if (FcPatternGetDouble (pattern, FC_PIXEL_SIZE, 0, &size) == FcResultMatch)
    return size;

  /* Just in case FC_PIXEL_SIZE got unset between pango_fc_make_pattern()
   * and here.  That would be very weird.
   */

  if (FcPatternGetDouble (pattern, FC_DPI, 0, &dpi) != FcResultMatch)
    dpi = 72;

  if (FcPatternGetDouble (pattern, FC_SIZE, 0, &size) == FcResultMatch)
    return size * dpi / 72.;

  /* Whatever */
  return 18.;
}

static gpointer
get_gravity_class (void)
{
  static GEnumClass *class = NULL; /* MT-safe */

  if (g_once_init_enter (&class))
    g_once_init_leave(&class, (gpointer)g_type_class_ref (PANGO_TYPE_GRAVITY));

  return class;
}

static PangoGravity
get_gravity (const FcPattern *pattern)
{
  char *s;

  if (FcPatternGetString (pattern, PANGO_FC_GRAVITY, 0, (FcChar8 **)(void *)&s) == FcResultMatch)
    {
      GEnumValue *value = g_enum_get_value_by_nick (get_gravity_class (), s);
      return value->value;
    }

  return PANGO_GRAVITY_SOUTH;
}

PangoFcFont *
_pango_cairo_fc_font_new (PangoCairoFcFontMap *cffontmap,
			  PangoFcFontKey      *key)
{
  PangoCairoFcFont *cffont;
  const FcPattern *pattern = pango_fc_font_key_get_pattern (key);
  cairo_matrix_t font_matrix;
  FcMatrix fc_matrix, *fc_matrix_val;
  double size;
  int i;
  cairo_font_options_t *options;

  g_return_val_if_fail (PANGO_IS_CAIRO_FC_FONT_MAP (cffontmap), NULL);
  g_return_val_if_fail (pattern != NULL, NULL);

  cffont = g_object_new (PANGO_TYPE_CAIRO_FC_FONT,
			 "pattern", pattern,
			 "fontmap", cffontmap,
			 NULL);

  size = get_font_size (pattern) /
	 pango_matrix_get_font_scale_factor (pango_fc_font_key_get_matrix (key));

  FcMatrixInit (&fc_matrix);
  for (i = 0; FcPatternGetMatrix (pattern, FC_MATRIX, i, &fc_matrix_val) == FcResultMatch; i++)
    FcMatrixMultiply (&fc_matrix, &fc_matrix, fc_matrix_val);

  cairo_matrix_init (&font_matrix,
		     fc_matrix.xx,
		     - fc_matrix.yx,
		     - fc_matrix.xy,
		     fc_matrix.yy,
		     0., 0.);

  cairo_matrix_scale (&font_matrix, size, size);

  options = pango_fc_font_key_get_context_key (key);

  _pango_cairo_font_private_initialize (&cffont->cf_priv,
					(PangoCairoFont *) cffont,
					get_gravity (pattern),
					options,
					pango_fc_font_key_get_matrix (key),
					&font_matrix);

  ((PangoFcFont *)(cffont))->is_hinted = _pango_cairo_font_private_is_metrics_hinted (&cffont->cf_priv);

  return (PangoFcFont *) cffont;
}
