/* Pango
 * pangofc-font.c: Shared interfaces for fontconfig-based backends
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

#include "config.h"

#include "pango-font-private.h"
#include "pangofc-font-private.h"
#include "pangofc-fontmap.h"
#include "pangofc-fontmap-private.h"
#include "pangofc-private.h"
#include "pango-layout.h"
#include "pango-impl-utils.h"

#include <hb-ot.h>

enum {
  PROP_0,
  PROP_PATTERN,
  PROP_FONTMAP
};

typedef struct _PangoFcFontPrivate PangoFcFontPrivate;

struct _PangoFcFontPrivate
{
  PangoFcDecoder *decoder;
  PangoFcFontKey *key;
};

static gboolean pango_fc_font_real_has_char  (PangoFcFont *font,
					      gunichar     wc);
static guint    pango_fc_font_real_get_glyph (PangoFcFont *font,
					      gunichar     wc);

static void                  pango_fc_font_finalize     (GObject          *object);
static void                  pango_fc_font_set_property (GObject          *object,
							 guint             prop_id,
							 const GValue     *value,
							 GParamSpec       *pspec);
static void                  pango_fc_font_get_property (GObject          *object,
							 guint             prop_id,
							 GValue           *value,
							 GParamSpec       *pspec);
static PangoCoverage *       pango_fc_font_get_coverage (PangoFont        *font,
							 PangoLanguage    *language);
static PangoFontMetrics *    pango_fc_font_get_metrics  (PangoFont        *font,
							 PangoLanguage    *language);
static PangoFontMap *        pango_fc_font_get_font_map (PangoFont        *font);
static PangoFontDescription *pango_fc_font_describe     (PangoFont        *font);
static PangoFontDescription *pango_fc_font_describe_absolute (PangoFont        *font);
static void                  pango_fc_font_get_features (PangoFont        *font,
                                                         hb_feature_t     *features,
                                                         guint             len,
                                                         guint            *num_features);
static hb_font_t *           pango_fc_font_create_hb_font (PangoFont        *font);
static PangoLanguage **     _pango_fc_font_get_languages  (PangoFont        *font);
static gboolean             _pango_fc_font_is_hinted      (PangoFont        *font);
static void                 _pango_fc_font_get_scale_factors (PangoFont     *font,
                                                              double        *x_scale,
                                                              double        *y_scale);
static void                 pango_fc_font_get_matrix      (PangoFont        *font,
                                                           PangoMatrix      *matrix);
static int                  pango_fc_font_get_absolute_size (PangoFont        *font);
static PangoVariant         pango_fc_font_get_variant       (PangoFont        *font);

#define PANGO_FC_FONT_LOCK_FACE(font)	(PANGO_FC_FONT_GET_CLASS (font)->lock_face (font))
#define PANGO_FC_FONT_UNLOCK_FACE(font)	(PANGO_FC_FONT_GET_CLASS (font)->unlock_face (font))

G_DEFINE_ABSTRACT_TYPE_WITH_CODE (PangoFcFont, pango_fc_font, PANGO_TYPE_FONT,
                                  G_ADD_PRIVATE (PangoFcFont))

static void
pango_fc_font_class_init (PangoFcFontClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);
  PangoFontClass *font_class = PANGO_FONT_CLASS (class);
  PangoFontClassPrivate *pclass;

  class->has_char = pango_fc_font_real_has_char;
  class->get_glyph = pango_fc_font_real_get_glyph;
  class->get_unknown_glyph = NULL;

  object_class->finalize = pango_fc_font_finalize;
  object_class->set_property = pango_fc_font_set_property;
  object_class->get_property = pango_fc_font_get_property;
  font_class->describe = pango_fc_font_describe;
  font_class->describe_absolute = pango_fc_font_describe_absolute;
  font_class->get_coverage = pango_fc_font_get_coverage;
  font_class->get_metrics = pango_fc_font_get_metrics;
  font_class->get_font_map = pango_fc_font_get_font_map;
  font_class->get_features = pango_fc_font_get_features;
  font_class->create_hb_font = pango_fc_font_create_hb_font;

  pclass = g_type_class_get_private ((GTypeClass *) class, PANGO_TYPE_FONT);

  pclass->get_languages = _pango_fc_font_get_languages;
  pclass->is_hinted = _pango_fc_font_is_hinted;
  pclass->get_scale_factors = _pango_fc_font_get_scale_factors;
  pclass->get_matrix = pango_fc_font_get_matrix;
  pclass->get_absolute_size = pango_fc_font_get_absolute_size;
  pclass->get_variant = pango_fc_font_get_variant;

  /**
   * PangoFcFont:pattern:
   *
   * The fontconfig pattern for this font.
   */
  g_object_class_install_property (object_class, PROP_PATTERN,
				   g_param_spec_pointer ("pattern",
							 "Pattern",
							 "The fontconfig pattern for this font",
							 G_PARAM_READWRITE | G_PARAM_CONSTRUCT_ONLY |
							 G_PARAM_STATIC_STRINGS));

  /**
   * PangoFcFont:fontmap:
   *
   * The PangoFc font map this font is associated with.
   */
  g_object_class_install_property (object_class, PROP_FONTMAP,
				   g_param_spec_object ("fontmap",
							"Font Map",
							"The PangoFc font map this font is associated with (Since: 1.26)",
							PANGO_TYPE_FC_FONT_MAP,
							G_PARAM_READWRITE |
							G_PARAM_STATIC_STRINGS));
}

static void
pango_fc_font_init (PangoFcFont *fcfont)
{
  fcfont->priv = pango_fc_font_get_instance_private (fcfont);
}

static void
free_metrics_info (PangoFcMetricsInfo *info)
{
  pango_font_metrics_unref (info->metrics);
  g_slice_free (PangoFcMetricsInfo, info);
}

static void
pango_fc_font_finalize (GObject *object)
{
  PangoFcFont *fcfont = PANGO_FC_FONT (object);
  PangoFcFontPrivate *priv = fcfont->priv;
  PangoFontMap *fontmap;

  g_slist_foreach (fcfont->metrics_by_lang, (GFunc)free_metrics_info, NULL);
  g_slist_free (fcfont->metrics_by_lang);

  fontmap = fcfont->fontmap;
  if (fontmap)
    {
      g_object_remove_weak_pointer (G_OBJECT (fontmap), (gpointer *) &fcfont->fontmap);
      _pango_fc_font_map_remove (PANGO_FC_FONT_MAP (fontmap), fcfont);
    }

  pango_font_description_free (fcfont->description);
  FcPatternDestroy (fcfont->font_pattern);

  if (priv->decoder)
    _pango_fc_font_set_decoder (fcfont, NULL);

  G_OBJECT_CLASS (pango_fc_font_parent_class)->finalize (object);
}

static gboolean
pattern_is_hinted (FcPattern *pattern)
{
  FcBool hinting;

  if (FcPatternGetBool (pattern,
			FC_HINTING, 0, &hinting) != FcResultMatch)
    hinting = FcTrue;

  return hinting;
}

static gboolean
pattern_is_transformed (FcPattern *pattern)
{
  FcMatrix *fc_matrix;

  if (FcPatternGetMatrix (pattern, FC_MATRIX, 0, &fc_matrix) == FcResultMatch)
    {
      return fc_matrix->xx != 1 || fc_matrix->xy != 0 ||
             fc_matrix->yx != 0 || fc_matrix->yy != 1;
    }
  else
    return FALSE;
}

static void
pango_fc_font_set_property (GObject       *object,
                            guint          prop_id,
                            const GValue  *value,
                            GParamSpec    *pspec)
{
  PangoFcFont *fcfont = PANGO_FC_FONT (object);

  switch (prop_id)
    {
    case PROP_PATTERN:
      {
        FcPattern *pattern = g_value_get_pointer (value);

        g_return_if_fail (pattern != NULL);
        g_return_if_fail (fcfont->font_pattern == NULL);

        FcPatternReference (pattern);
        fcfont->font_pattern = pattern;
        fcfont->description = font_description_from_pattern (pattern, TRUE, TRUE);
        fcfont->is_hinted = pattern_is_hinted (pattern);
        fcfont->is_transformed = pattern_is_transformed (pattern);
      }
      goto set_decoder;

    case PROP_FONTMAP:
      {
        PangoFcFontMap *fcfontmap = PANGO_FC_FONT_MAP (g_value_get_object (value));

        g_return_if_fail (fcfont->fontmap == NULL);

        fcfont->fontmap = (PangoFontMap *) fcfontmap;
        if (fcfontmap)
          g_object_add_weak_pointer (G_OBJECT (fcfontmap), (gpointer *) &fcfont->fontmap);
      }
      goto set_decoder;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
      return;
    }

set_decoder:
  /* set decoder if both pattern and fontmap are set now */
  if (fcfont->font_pattern && fcfont->fontmap)
    _pango_fc_font_set_decoder (fcfont,
				pango_fc_font_map_find_decoder  ((PangoFcFontMap *) fcfont->fontmap,
								 fcfont->font_pattern));
}

static void
pango_fc_font_get_property (GObject       *object,
			    guint          prop_id,
			    GValue        *value,
			    GParamSpec    *pspec)
{
  switch (prop_id)
    {
    case PROP_PATTERN:
      {
	PangoFcFont *fcfont = PANGO_FC_FONT (object);
	g_value_set_pointer (value, fcfont->font_pattern);
      }
      break;
    case PROP_FONTMAP:
      {
	PangoFcFont *fcfont = PANGO_FC_FONT (object);

	g_value_set_object (value, fcfont->fontmap);
      }
      break;
    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, prop_id, pspec);
      break;
    }
}

static PangoFontDescription *
pango_fc_font_describe (PangoFont *font)
{
  PangoFcFont *fcfont = (PangoFcFont *)font;

  return pango_font_description_copy (fcfont->description);
}

static PangoFontDescription *
pango_fc_font_describe_absolute (PangoFont *font)
{
  PangoFcFont *fcfont = (PangoFcFont *)font;
  PangoFontDescription *desc = pango_font_description_copy (fcfont->description);
  double size;

  if (FcPatternGetDouble (fcfont->font_pattern, FC_PIXEL_SIZE, 0, &size) == FcResultMatch)
    pango_font_description_set_absolute_size (desc, size * PANGO_SCALE);

  return desc;
}

static int
pango_fc_font_get_absolute_size (PangoFont *font)
{
  PangoFcFont *fcfont = (PangoFcFont *)font;
  double size;

  if (FcPatternGetDouble (fcfont->font_pattern, FC_PIXEL_SIZE, 0, &size) == FcResultMatch)
    return (int) (size * PANGO_SCALE);

  return 0;
}

static PangoVariant
pango_fc_font_get_variant (PangoFont *font)
{
  PangoFcFont *fcfont = (PangoFcFont *)font;
  return pango_font_description_get_variant (fcfont->description);
}

static PangoCoverage *
pango_fc_font_get_coverage (PangoFont     *font,
			    PangoLanguage *language G_GNUC_UNUSED)
{
  PangoFcFont *fcfont = (PangoFcFont *)font;
  PangoFcFontPrivate *priv = fcfont->priv;
  FcCharSet *charset;

  if (priv->decoder)
    {
      charset = pango_fc_decoder_get_charset (priv->decoder, fcfont);
      return _pango_fc_font_map_fc_to_coverage (charset);
    }

  if (!fcfont->fontmap)
    return pango_coverage_new ();

  return _pango_fc_font_map_get_coverage (PANGO_FC_FONT_MAP (fcfont->fontmap), fcfont);
}

/* For Xft, it would be slightly more efficient to simply to
 * call Xft, and also more robust against changes in Xft.
 * But for now, we simply use the same code for all backends.
 *
 * The code in this function is partly based on code from Xft,
 * Copyright 2000 Keith Packard
 */
static void
get_face_metrics (PangoFcFont      *fcfont,
                  PangoFontMetrics *metrics)
{
  hb_font_t *hb_font = pango_font_get_hb_font (PANGO_FONT (fcfont));
  hb_font_extents_t extents;
  hb_position_t position;

  FcMatrix *fc_matrix;
  gboolean have_transform = FALSE;

  hb_font_get_extents_for_direction (hb_font, HB_DIRECTION_LTR, &extents);

  if  (FcPatternGetMatrix (fcfont->font_pattern,
                           FC_MATRIX, 0, &fc_matrix) == FcResultMatch)
    {
      have_transform = (fc_matrix->xx != 1 || fc_matrix->xy != 0 ||
                        fc_matrix->yx != 0 || fc_matrix->yy != 1);
    }

  if (have_transform)
    {
      metrics->descent =  - extents.descender * fc_matrix->yy;
      metrics->ascent = extents.ascender * fc_matrix->yy;
      metrics->height = (extents.ascender - extents.descender + extents.line_gap) * fc_matrix->yy;
    }
  else
    {
      metrics->descent = - extents.descender;
      metrics->ascent = extents.ascender;
      metrics->height = extents.ascender - extents.descender + extents.line_gap;
    }

  if (hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_UNDERLINE_SIZE, &position) &&
      position != 0)
    metrics->underline_thickness = position;
  else
    metrics->underline_thickness = PANGO_SCALE;

  if (hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_UNDERLINE_OFFSET, &position) &&
      position != 0)
    metrics->underline_position = position;
  else
    metrics->underline_position = - PANGO_SCALE;

  if (hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_STRIKEOUT_SIZE, &position) &&
      position != 0)
    metrics->strikethrough_thickness = position;
  else
    metrics->strikethrough_thickness = PANGO_SCALE;

  if (hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_STRIKEOUT_OFFSET, &position) &&
      position != 0)
    metrics->strikethrough_position = position;
  else
    metrics->strikethrough_position = metrics->ascent / 2;
}

PangoFontMetrics *
pango_fc_font_create_base_metrics_for_context (PangoFcFont   *fcfont,
					       PangoContext  *context)
{
  PangoFontMetrics *metrics;
  metrics = pango_font_metrics_new ();

  get_face_metrics (fcfont, metrics);

  return metrics;
}

static int
max_glyph_width (PangoLayout *layout)
{
  int max_width = 0;
  GSList *l, *r;

  for (l = pango_layout_get_lines_readonly (layout); l; l = l->next)
    {
      PangoLayoutLine *line = l->data;

      for (r = line->runs; r; r = r->next)
	{
	  PangoGlyphString *glyphs = ((PangoGlyphItem *)r->data)->glyphs;
	  int i;

	  for (i = 0; i < glyphs->num_glyphs; i++)
	    if (glyphs->glyphs[i].geometry.width > max_width)
	      max_width = glyphs->glyphs[i].geometry.width;
	}
    }

  return max_width;
}

static PangoFontMetrics *
pango_fc_font_get_metrics (PangoFont     *font,
			   PangoLanguage *language)
{
  PangoFcFont *fcfont = PANGO_FC_FONT (font);
  PangoFcMetricsInfo *info = NULL; /* Quiet gcc */
  GSList *tmp_list;
  static int in_get_metrics;

  const char *sample_str = pango_language_get_sample_string (language);

  tmp_list = fcfont->metrics_by_lang;
  while (tmp_list)
    {
      info = tmp_list->data;

      if (info->sample_str == sample_str)    /* We _don't_ need strcmp */
	break;

      tmp_list = tmp_list->next;
    }

  if (!tmp_list)
    {
      PangoFontMap *fontmap;
      PangoContext *context;

      fontmap = fcfont->fontmap;
      if (!fontmap)
	return pango_font_metrics_new ();

      info = g_slice_new0 (PangoFcMetricsInfo);

      /* Note: we need to add info to the list before calling
       * into PangoLayout below, to prevent recursion
       */
      fcfont->metrics_by_lang = g_slist_prepend (fcfont->metrics_by_lang,
						 info);

      info->sample_str = sample_str;

      context = pango_font_map_create_context (fontmap);
      pango_context_set_language (context, language);

      info->metrics = pango_fc_font_create_base_metrics_for_context (fcfont, context);

      if (!in_get_metrics)
        {
          /* Compute derived metrics */
          PangoLayout *layout;
          PangoRectangle extents;
          PangoFontDescription *desc = pango_font_describe_with_absolute_size (font);
          gulong sample_str_width;

          in_get_metrics = 1;

          layout = pango_layout_new (context);
          pango_layout_set_font_description (layout, desc);
          pango_font_description_free (desc);

          pango_layout_set_text (layout, sample_str, -1);
          pango_layout_get_extents (layout, NULL, &extents);

          sample_str_width = pango_utf8_strwidth (sample_str);
          g_assert (sample_str_width > 0);
          info->metrics->approximate_char_width = extents.width / sample_str_width;

          pango_layout_set_text (layout, "0123456789", -1);
          info->metrics->approximate_digit_width = max_glyph_width (layout);

          g_object_unref (layout);

          in_get_metrics = 0;
        }

      g_object_unref (context);
    }

  return pango_font_metrics_ref (info->metrics);
}

static PangoFontMap *
pango_fc_font_get_font_map (PangoFont *font)
{
  PangoFcFont *fcfont = PANGO_FC_FONT (font);

  /* MT-unsafe.  Oh well...  The API is unsafe. */
  return fcfont->fontmap;
}

static gboolean
pango_fc_font_real_has_char (PangoFcFont *font,
			     gunichar     wc)
{
  FcCharSet *charset;

  if (FcPatternGetCharSet (font->font_pattern,
			   FC_CHARSET, 0, &charset) != FcResultMatch)
    return FALSE;

  return FcCharSetHasChar (charset, wc);
}

static guint
pango_fc_font_real_get_glyph (PangoFcFont *font,
			      gunichar     wc)
{
  hb_font_t *hb_font = pango_font_get_hb_font (PANGO_FONT (font));
  hb_codepoint_t glyph = PANGO_GET_UNKNOWN_GLYPH (wc);

  hb_font_get_nominal_glyph (hb_font, wc, &glyph);

  return glyph;
}

/**
 * pango_fc_font_lock_face: (skip)
 * @font: a `PangoFcFont`.
 *
 * Gets the FreeType `FT_Face` associated with a font.
 *
 * This face will be kept around until you call
 * [method@PangoFc.Font.unlock_face].
 *
 * Return value: the FreeType `FT_Face` associated with @font.
 *
 * Since: 1.4
 * Deprecated: 1.44: Use pango_font_get_hb_font() instead
 */
FT_Face
pango_fc_font_lock_face (PangoFcFont *font)
{
  g_return_val_if_fail (PANGO_IS_FC_FONT (font), NULL);

  return PANGO_FC_FONT_LOCK_FACE (font);
}

/**
 * pango_fc_font_unlock_face:
 * @font: a `PangoFcFont`.
 *
 * Releases a font previously obtained with
 * [method@PangoFc.Font.lock_face].
 *
 * Since: 1.4
 * Deprecated: 1.44: Use pango_font_get_hb_font() instead
 */
void
pango_fc_font_unlock_face (PangoFcFont *font)
{
  g_return_if_fail (PANGO_IS_FC_FONT (font));

  PANGO_FC_FONT_UNLOCK_FACE (font);
}

/**
 * pango_fc_font_has_char:
 * @font: a `PangoFcFont`
 * @wc: Unicode codepoint to look up
 *
 * Determines whether @font has a glyph for the codepoint @wc.
 *
 * Return value: %TRUE if @font has the requested codepoint.
 *
 * Since: 1.4
 * Deprecated: 1.44: Use [method@Pango.Font.has_char]
 */
gboolean
pango_fc_font_has_char (PangoFcFont *font,
			gunichar     wc)
{
  PangoFcFontPrivate *priv = font->priv;
  FcCharSet *charset;

  g_return_val_if_fail (PANGO_IS_FC_FONT (font), FALSE);

  if (priv->decoder)
    {
      charset = pango_fc_decoder_get_charset (priv->decoder, font);
      return FcCharSetHasChar (charset, wc);
    }

  return PANGO_FC_FONT_GET_CLASS (font)->has_char (font, wc);
}

/**
 * pango_fc_font_get_glyph:
 * @font: a `PangoFcFont`
 * @wc: Unicode character to look up
 *
 * Gets the glyph index for a given Unicode character
 * for @font.
 *
 * If you only want to determine whether the font has
 * the glyph, use [method@PangoFc.Font.has_char].
 *
 * Return value: the glyph index, or 0, if the Unicode
 *   character doesn't exist in the font.
 *
 * Since: 1.4
 */
PangoGlyph
pango_fc_font_get_glyph (PangoFcFont *font,
			 gunichar     wc)
{
  PangoFcFontPrivate *priv = font->priv;

  /* Replace NBSP with a normal space; it should be invariant that
   * they shape the same other than breaking properties.
   */
  if (wc == 0xA0)
	  wc = 0x20;

  if (priv->decoder)
    return pango_fc_decoder_get_glyph (priv->decoder, font, wc);

  return PANGO_FC_FONT_GET_CLASS (font)->get_glyph (font, wc);
}


/**
 * pango_fc_font_get_unknown_glyph:
 * @font: a `PangoFcFont`
 * @wc: the Unicode character for which a glyph is needed.
 *
 * Returns the index of a glyph suitable for drawing @wc
 * as an unknown character.
 *
 * Use PANGO_GET_UNKNOWN_GLYPH() instead.
 *
 * Return value: a glyph index into @font.
 *
 * Since: 1.4
 */
PangoGlyph
pango_fc_font_get_unknown_glyph (PangoFcFont *font,
				 gunichar     wc)
{
  if (font && PANGO_FC_FONT_GET_CLASS (font)->get_unknown_glyph)
    return PANGO_FC_FONT_GET_CLASS (font)->get_unknown_glyph (font, wc);

  return PANGO_GET_UNKNOWN_GLYPH (wc);
}

void
_pango_fc_font_shutdown (PangoFcFont *font)
{
  g_return_if_fail (PANGO_IS_FC_FONT (font));

  if (PANGO_FC_FONT_GET_CLASS (font)->shutdown)
    PANGO_FC_FONT_GET_CLASS (font)->shutdown (font);
}

/**
 * pango_fc_font_kern_glyphs:
 * @font: a `PangoFcFont`
 * @glyphs: a `PangoGlyphString`
 *
 * This function used to adjust each adjacent pair of glyphs
 * in @glyphs according to kerning information in @font.
 *
 * Since 1.44, it does nothing.
 *
 * Since: 1.4
 * Deprecated: 1.32
 */
void
pango_fc_font_kern_glyphs (PangoFcFont      *font,
			   PangoGlyphString *glyphs)
{
}

/**
 * _pango_fc_font_get_decoder:
 * @font: a `PangoFcFont`
 *
 * This will return any custom decoder set on this font.
 *
 * Return value: The custom decoder
 *
 * Since: 1.6
 */
PangoFcDecoder *
_pango_fc_font_get_decoder (PangoFcFont *font)
{
  PangoFcFontPrivate *priv = font->priv;

  return priv->decoder;
}

/**
 * _pango_fc_font_set_decoder:
 * @font: a `PangoFcFont`
 * @decoder: a `PangoFcDecoder` to set for this font
 *
 * This sets a custom decoder for this font.
 *
 * Any previous decoder will be released before this one is set.
 *
 * Since: 1.6
 */
void
_pango_fc_font_set_decoder (PangoFcFont    *font,
			    PangoFcDecoder *decoder)
{
  PangoFcFontPrivate *priv = font->priv;

  if (priv->decoder)
    g_object_unref (priv->decoder);

  priv->decoder = decoder;

  if (priv->decoder)
    g_object_ref (priv->decoder);
}

PangoFcFontKey *
_pango_fc_font_get_font_key (PangoFcFont *fcfont)
{
  PangoFcFontPrivate *priv = fcfont->priv;

  return priv->key;
}

void
_pango_fc_font_set_font_key (PangoFcFont    *fcfont,
			     PangoFcFontKey *key)
{
  PangoFcFontPrivate *priv = fcfont->priv;

  priv->key = key;
}

/**
 * pango_fc_font_get_raw_extents:
 * @fcfont: a `PangoFcFont`
 * @glyph: the glyph index to load
 * @ink_rect: (out) (optional): location to store ink extents of the
 *   glyph
 * @logical_rect: (out) (optional): location to store logical extents
 *   of the glyph
 *
 * Gets the extents of a single glyph from a font.
 *
 * The extents are in user space; that is, they are not transformed
 * by any matrix in effect for the font.
 *
 * Long term, this functionality probably belongs in the default
 * implementation of the get_glyph_extents() virtual function.
 * The other possibility would be to to make it public in something
 * like it's current form, and also expose glyph information
 * caching functionality similar to pango_ft2_font_set_glyph_info().
 *
 * Since: 1.6
 */
void
pango_fc_font_get_raw_extents (PangoFcFont    *fcfont,
			       PangoGlyph      glyph,
			       PangoRectangle *ink_rect,
			       PangoRectangle *logical_rect)
{
  g_return_if_fail (PANGO_IS_FC_FONT (fcfont));

  if (glyph == PANGO_GLYPH_EMPTY)
    {
      if (ink_rect)
	{
	  ink_rect->x = 0;
	  ink_rect->width = 0;
	  ink_rect->y = 0;
	  ink_rect->height = 0;
	}

      if (logical_rect)
	{
	  logical_rect->x = 0;
	  logical_rect->width = 0;
	  logical_rect->y = 0;
	  logical_rect->height = 0;
	}
    }
  else
    {
      hb_font_t *hb_font = pango_font_get_hb_font (PANGO_FONT (fcfont));
      hb_glyph_extents_t extents;
      hb_font_extents_t font_extents;

      hb_font_get_glyph_extents (hb_font, glyph, &extents);
      hb_font_get_extents_for_direction (hb_font, HB_DIRECTION_LTR, &font_extents);

      if (ink_rect)
	{
	  ink_rect->x = extents.x_bearing;
	  ink_rect->width = extents.width;
	  ink_rect->y = -extents.y_bearing;
	  ink_rect->height = -extents.height;
	}

      if (logical_rect)
	{
          hb_position_t x, y;

          hb_font_get_glyph_advance_for_direction (hb_font,
                                                   glyph,
                                                   HB_DIRECTION_LTR,
                                                   &x, &y);

	  logical_rect->x = 0;
	  logical_rect->width = x;
	  logical_rect->y = - font_extents.ascender;
	  logical_rect->height = font_extents.ascender - font_extents.descender;
	}
    }
}

static void
pango_fc_font_get_features (PangoFont    *font,
                            hb_feature_t *features,
                            guint         len,
                            guint        *num_features)
{
  /* Setup features from fontconfig pattern. */
  PangoFcFont *fc_font = PANGO_FC_FONT (font);
  if (fc_font->font_pattern)
    {
      char *s;
      while (*num_features < len &&
             FcResultMatch == FcPatternGetString (fc_font->font_pattern,
                                                  FC_FONT_FEATURES,
                                                  *num_features,
                                                  (FcChar8 **) &s))
        {
          gboolean ret = hb_feature_from_string (s, -1, &features[*num_features]);
          features[*num_features].start = 0;
          features[*num_features].end   = (unsigned int) -1;
          if (ret)
            (*num_features)++;
        }
    }
}

extern gpointer get_gravity_class (void);

static PangoGravity
pango_fc_font_key_get_gravity (PangoFcFontKey *key)
{
  const FcPattern *pattern;
  PangoGravity gravity = PANGO_GRAVITY_SOUTH;
  FcChar8 *s;

  pattern = pango_fc_font_key_get_pattern (key);
  if (FcPatternGetString (pattern, PANGO_FC_GRAVITY, 0, (FcChar8 **)&s) == FcResultMatch)
    {
      GEnumValue *value = g_enum_get_value_by_nick (get_gravity_class (), (char *)s);
      gravity = value->value;
    }

  return gravity;
}

static void
get_font_size (PangoFcFontKey *key,
               double         *pixel_size,
               double         *point_size)
{
  const FcPattern *pattern;
  double dpi;

  pattern = pango_fc_font_key_get_pattern (key);
  if (FcPatternGetDouble (pattern, FC_SIZE, 0, point_size) != FcResultMatch)
    *point_size = 13.;

  if (FcPatternGetDouble (pattern, FC_PIXEL_SIZE, 0, pixel_size) != FcResultMatch)
    {
      if (FcPatternGetDouble (pattern, FC_DPI, 0, &dpi) != FcResultMatch)
        dpi = 72.;

      *pixel_size = *point_size * dpi / 72.;
    }
}

static hb_font_t *
pango_fc_font_create_hb_font (PangoFont *font)
{
  PangoFcFont *fc_font = PANGO_FC_FONT (font);
  PangoFcFontKey *key;
  hb_face_t *hb_face;
  hb_font_t *hb_font;
  double x_scale_inv, y_scale_inv;
  double x_scale, y_scale;
  double pixel_size;
  double point_size;
  double slant G_GNUC_UNUSED;

  x_scale_inv = y_scale_inv = 1.0;
  pixel_size = 1.0;
  point_size = 1.0;
  slant = 0.0;

  key = _pango_fc_font_get_font_key (fc_font);
  if (key)
    {
      const FcPattern *pattern = pango_fc_font_key_get_pattern (key);
      const PangoMatrix *ctm;
      PangoMatrix font_matrix;
      PangoGravity gravity;
      FcMatrix fc_matrix, *fc_matrix_val;
      double x, y;
      int i;

      ctm = pango_fc_font_key_get_matrix (key);
      pango_matrix_get_font_scale_factors (ctm, &x_scale_inv, &y_scale_inv);

      FcMatrixInit (&fc_matrix);
      for (i = 0; FcPatternGetMatrix (pattern, FC_MATRIX, i, &fc_matrix_val) == FcResultMatch; i++)
        FcMatrixMultiply (&fc_matrix, &fc_matrix, fc_matrix_val);

      font_matrix.xx = fc_matrix.xx;
      font_matrix.yx = - fc_matrix.yx;
      font_matrix.xy = fc_matrix.xy;
      font_matrix.yy = - fc_matrix.yy;

      pango_matrix_get_font_scale_factors (&font_matrix, &x, &y);
      slant = pango_matrix_get_slant_ratio (&font_matrix);

      x_scale_inv /= x;
      y_scale_inv /= y;

      gravity = pango_fc_font_key_get_gravity (key);
      if (PANGO_GRAVITY_IS_IMPROPER (gravity))
        {
          x_scale_inv = -x_scale_inv;
          y_scale_inv = -y_scale_inv;
        }

      get_font_size (key, &pixel_size, &point_size);
    }

  x_scale = 1. / x_scale_inv;
  y_scale = 1. / y_scale_inv;

  hb_face = pango_fc_font_map_get_hb_face (PANGO_FC_FONT_MAP (fc_font->fontmap), fc_font);

  hb_font = hb_font_create (hb_face);
  hb_font_set_scale (hb_font,
                     pixel_size * PANGO_SCALE * x_scale,
                     pixel_size * PANGO_SCALE * y_scale);
  hb_font_set_ptem (hb_font, point_size);

#if HB_VERSION_ATLEAST (3, 3, 0)
  hb_font_set_synthetic_slant (hb_font, slant);
#endif

  if (key)
    {
      const FcPattern *pattern = pango_fc_font_key_get_pattern (key);
      const char *variations;
      int index;
      unsigned int n_axes;
      hb_ot_var_axis_info_t *axes;
      float *coords;
      int i;

      n_axes = hb_ot_var_get_axis_infos (hb_face, 0, NULL, NULL);
      if (n_axes == 0)
        goto done;

      axes = g_new0 (hb_ot_var_axis_info_t, n_axes);
      coords = g_new (float, n_axes);

      hb_ot_var_get_axis_infos (hb_face, 0, &n_axes, axes);
      for (i = 0; i < n_axes; i++)
        coords[axes[i].axis_index] = axes[i].default_value;

      if (FcPatternGetInteger (pattern, FC_INDEX, 0, &index) == FcResultMatch &&
          index != 0)
        {
          unsigned int instance = (index >> 16) - 1;
          hb_ot_var_named_instance_get_design_coords (hb_face, instance, &n_axes, coords);
        }

      if (FcPatternGetString (pattern, FC_FONT_VARIATIONS, 0, (FcChar8 **)&variations) == FcResultMatch)
        pango_parse_variations (variations, axes, n_axes, coords);

      variations = pango_fc_font_key_get_variations (key);
      if (variations)
        pango_parse_variations (variations, axes, n_axes, coords);

      hb_font_set_var_coords_design (hb_font, coords, n_axes);

      g_free (coords);
      g_free (axes);
    }

done:
  return hb_font;
}

/**
 * pango_fc_font_get_languages:
 * @font: a `PangoFcFont`
 *
 * Returns the languages that are supported by @font.
 *
 * This corresponds to the FC_LANG member of the FcPattern.
 *
 * The returned array is only valid as long as the font
 * and its fontmap are valid.
 *
 * Returns: (transfer none) (nullable) (array zero-terminated=1): a
 *   %NULL-terminated array of `PangoLanguage`*
 *
 * Since: 1.48
 * Deprecated: 1.50: Use pango_font_get_language()
 */
PangoLanguage **
pango_fc_font_get_languages (PangoFcFont *font)
{
  return pango_font_get_languages (PANGO_FONT (font));
}

static PangoLanguage **
_pango_fc_font_get_languages (PangoFont *font)
{
  PangoFcFont * fcfont = PANGO_FC_FONT (font);

  if (!fcfont->fontmap)
    return NULL;

  return _pango_fc_font_map_get_languages (PANGO_FC_FONT_MAP (fcfont->fontmap), fcfont);
}

/**
 * pango_fc_font_get_pattern: (skip)
 * @font: a `PangoFcFont`
 *
 * Returns the FcPattern that @font is based on.
 *
 * Returns: the fontconfig pattern for this font
 *
 * Since: 1.48
 */
FcPattern *
pango_fc_font_get_pattern (PangoFcFont *font)
{
  return font->font_pattern;
}

gboolean
_pango_fc_font_is_hinted (PangoFont *font)
{
  PangoFcFont *fcfont = PANGO_FC_FONT (font);

  return fcfont->is_hinted;
}

void
_pango_fc_font_get_scale_factors (PangoFont *font,
                                  double    *x_scale,
                                  double    *y_scale)
{
  PangoFcFont *fcfont = PANGO_FC_FONT (font);
  PangoFcFontPrivate *priv = fcfont->priv;

  pango_matrix_get_font_scale_factors (pango_fc_font_key_get_matrix (priv->key), x_scale, y_scale);
}

static void
pango_fc_font_get_matrix (PangoFont   *font,
                          PangoMatrix *matrix)
{
  PangoFcFont *fcfont = PANGO_FC_FONT (font);
  FcMatrix fc_matrix, *fc_matrix_val;

  FcMatrixInit (&fc_matrix);
  for (int i = 0; FcPatternGetMatrix (fcfont->font_pattern, FC_MATRIX, i, &fc_matrix_val) == FcResultMatch; i++)
    FcMatrixMultiply (&fc_matrix, &fc_matrix, fc_matrix_val);

  matrix->xx = fc_matrix.xx;
  matrix->xy = - fc_matrix.xy;
  matrix->yx = - fc_matrix.yx;
  matrix->yy = fc_matrix.yy;
  matrix->x0 = 0.;
  matrix->y0 = 0.;
}
