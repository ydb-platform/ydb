/* Pango
 * pangoft2.c: Routines for handling FreeType2 fonts
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

#include "config.h"

#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <glib.h>
#include <glib/gprintf.h>

#include "pangoft2.h"
#include "pangoft2-private.h"
#include "pangofc-fontmap-private.h"
#include "pangofc-private.h"

/* for compatibility with older freetype versions */
#ifndef FT_LOAD_TARGET_MONO
#define FT_LOAD_TARGET_MONO  FT_LOAD_MONOCHROME
#endif

#define PANGO_FT2_FONT_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), PANGO_TYPE_FT2_FONT, PangoFT2FontClass))
#define PANGO_FT2_IS_FONT_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), PANGO_TYPE_FT2_FONT))
#define PANGO_FT2_FONT_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), PANGO_TYPE_FT2_FONT, PangoFT2FontClass))

typedef struct _PangoFT2FontClass   PangoFT2FontClass;

struct _PangoFT2FontClass
{
  PangoFcFontClass parent_class;
};

static void     pango_ft2_font_finalize          (GObject        *object);

static void     pango_ft2_font_get_glyph_extents (PangoFont      *font,
                                                  PangoGlyph      glyph,
                                                  PangoRectangle *ink_rect,
                                                  PangoRectangle *logical_rect);

static FT_Face  pango_ft2_font_real_lock_face    (PangoFcFont    *font);
static void     pango_ft2_font_real_unlock_face  (PangoFcFont    *font);


PangoFT2Font *
_pango_ft2_font_new (PangoFT2FontMap *ft2fontmap,
		     FcPattern       *pattern)
{
  PangoFontMap *fontmap = PANGO_FONT_MAP (ft2fontmap);
  PangoFT2Font *ft2font;
  double d;

  g_return_val_if_fail (fontmap != NULL, NULL);
  g_return_val_if_fail (pattern != NULL, NULL);

  ft2font = (PangoFT2Font *)g_object_new (PANGO_TYPE_FT2_FONT,
					  "pattern", pattern,
					  "fontmap", fontmap,
					  NULL);

  if (FcPatternGetDouble (pattern, FC_PIXEL_SIZE, 0, &d) == FcResultMatch)
    ft2font->size = d*PANGO_SCALE;

  return ft2font;
}

static void
load_fallback_face (PangoFT2Font *ft2font,
		    const char   *original_file)
{
  PangoFcFont *fcfont = PANGO_FC_FONT (ft2font);
  FcPattern *sans;
  FcPattern *matched;
  FcResult result;
  FT_Error error;
  FcChar8 *filename2 = NULL;
  gchar *name;
  int id;

  sans = FcPatternBuild (NULL,
			 FC_FAMILY,     FcTypeString, "sans",
			 FC_PIXEL_SIZE, FcTypeDouble, (double)ft2font->size / PANGO_SCALE,
			 NULL);

  _pango_ft2_font_map_default_substitute ((PangoFcFontMap *)fcfont->fontmap, sans);

  matched = FcFontMatch (pango_fc_font_map_get_config ((PangoFcFontMap *)fcfont->fontmap), sans, &result);

  if (FcPatternGetString (matched, FC_FILE, 0, &filename2) != FcResultMatch)
    goto bail1;

  if (FcPatternGetInteger (matched, FC_INDEX, 0, &id) != FcResultMatch)
    goto bail1;

  error = FT_New_Face (_pango_ft2_font_map_get_library (fcfont->fontmap),
		       (char *) filename2, id, &ft2font->face);


  if (error)
    {
    bail1:
      name = pango_font_description_to_string (fcfont->description);
      g_error ("Unable to open font file %s for font %s, exiting\n", filename2, name);
    }
  else
    {
      name = pango_font_description_to_string (fcfont->description);
      g_warning ("Unable to open font file %s for font %s, falling back to %s\n", original_file, name, filename2);
      g_free (name);
    }

  FcPatternDestroy (sans);
  FcPatternDestroy (matched);
}

static void
set_transform (PangoFT2Font *ft2font)
{
  PangoFcFont *fcfont = (PangoFcFont *)ft2font;
  FcMatrix *fc_matrix;

  if (FcPatternGetMatrix (fcfont->font_pattern, FC_MATRIX, 0, &fc_matrix) == FcResultMatch)
    {
      FT_Matrix ft_matrix;

      ft_matrix.xx = 0x10000L * fc_matrix->xx;
      ft_matrix.yy = 0x10000L * fc_matrix->yy;
      ft_matrix.xy = 0x10000L * fc_matrix->xy;
      ft_matrix.yx = 0x10000L * fc_matrix->yx;

      FT_Set_Transform (ft2font->face, &ft_matrix, NULL);
    }
}

/**
 * pango_ft2_font_get_face: (skip)
 * @font: a `PangoFont`
 *
 * Returns the native FreeType2 `FT_Face` structure
 * used for this `PangoFont`.
 *
 * This may be useful if you want to use FreeType2
 * functions directly.
 *
 * Use [method@PangoFc.Font.lock_face] instead; when you are
 * done with a face from [method@PangoFc.Font.lock_face], you
 * must call [method@PangoFc.Font.unlock_face].
 *
 * Return value: (nullable): a pointer to a `FT_Face` structure,
 *   with the size set correctly
 */
FT_Face
pango_ft2_font_get_face (PangoFont *font)
{
  PangoFT2Font *ft2font = (PangoFT2Font *)font;
  PangoFcFont *fcfont = (PangoFcFont *)font;
  FT_Error error;
  FcPattern *pattern;
  FcChar8 *filename;
  FcBool antialias, hinting, autohint;
  int hintstyle;
  int id;

  if (G_UNLIKELY (!font))
    return NULL;

  pattern = fcfont->font_pattern;

  if (!ft2font->face)
    {
      ft2font->load_flags = 0;

      /* disable antialiasing if requested */
      if (FcPatternGetBool (pattern,
                            FC_ANTIALIAS, 0, &antialias) != FcResultMatch)
        antialias = FcTrue;

      if (antialias)
        ft2font->load_flags |= FT_LOAD_NO_BITMAP;
      else
        ft2font->load_flags |= FT_LOAD_TARGET_MONO;

      /* disable hinting if requested */
      if (FcPatternGetBool (pattern,
                            FC_HINTING, 0, &hinting) != FcResultMatch)
        hinting = FcTrue;

      if (FcPatternGetInteger (pattern, FC_HINT_STYLE, 0, &hintstyle) != FcResultMatch)
        hintstyle = FC_HINT_FULL;

      if (!hinting || hintstyle == FC_HINT_NONE)
          ft2font->load_flags |= FT_LOAD_NO_HINTING;

      switch (hintstyle) {
      case FC_HINT_SLIGHT:
      case FC_HINT_MEDIUM:
        ft2font->load_flags |= FT_LOAD_TARGET_LIGHT;
        break;
      default:
        ft2font->load_flags |= FT_LOAD_TARGET_NORMAL;
        break;
      }

      /* force autohinting if requested */
      if (FcPatternGetBool (pattern,
                            FC_AUTOHINT, 0, &autohint) != FcResultMatch)
        autohint = FcFalse;

      if (autohint)
        ft2font->load_flags |= FT_LOAD_FORCE_AUTOHINT;

      if (FcPatternGetString (pattern, FC_FILE, 0, &filename) != FcResultMatch)
              goto bail0;

      if (FcPatternGetInteger (pattern, FC_INDEX, 0, &id) != FcResultMatch)
              goto bail0;

      error = FT_New_Face (_pango_ft2_font_map_get_library (fcfont->fontmap),
                           (char *) filename, id, &ft2font->face);
      if (error != FT_Err_Ok)
        {
        bail0:
          load_fallback_face (ft2font, (char *) filename);
        }

      g_assert (ft2font->face);

      set_transform (ft2font);

      error = FT_Set_Char_Size (ft2font->face,
                                PANGO_PIXELS_26_6 (ft2font->size),
                                PANGO_PIXELS_26_6 (ft2font->size),
                                0, 0);
      if (error)
        g_warning ("Error in FT_Set_Char_Size: %d", error);
    }

  return ft2font->face;
}

G_DEFINE_TYPE (PangoFT2Font, pango_ft2_font, PANGO_TYPE_FC_FONT)

static void
pango_ft2_font_init (PangoFT2Font *ft2font)
{
  ft2font->face = NULL;

  ft2font->size = 0;

  ft2font->glyph_info = g_hash_table_new (NULL, NULL);
}

static void
pango_ft2_font_class_init (PangoFT2FontClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);
  PangoFontClass *font_class = PANGO_FONT_CLASS (class);
  PangoFcFontClass *fc_font_class = PANGO_FC_FONT_CLASS (class);

  object_class->finalize = pango_ft2_font_finalize;

  font_class->get_glyph_extents = pango_ft2_font_get_glyph_extents;

  fc_font_class->lock_face = pango_ft2_font_real_lock_face;
  fc_font_class->unlock_face = pango_ft2_font_real_unlock_face;
}

static PangoFT2GlyphInfo *
pango_ft2_font_get_glyph_info (PangoFont   *font,
			       PangoGlyph   glyph,
			       gboolean     create)
{
  PangoFT2Font *ft2font = (PangoFT2Font *)font;
  PangoFcFont *fcfont = (PangoFcFont *)font;
  PangoFT2GlyphInfo *info;

  info = g_hash_table_lookup (ft2font->glyph_info, GUINT_TO_POINTER (glyph));

  if ((info == NULL) && create)
    {
      info = g_slice_new0 (PangoFT2GlyphInfo);

      pango_fc_font_get_raw_extents (fcfont,
				     glyph,
				     &info->ink_rect,
				     &info->logical_rect);

      g_hash_table_insert (ft2font->glyph_info, GUINT_TO_POINTER(glyph), info);
    }

  return info;
}

static void
pango_ft2_font_get_glyph_extents (PangoFont      *font,
				  PangoGlyph      glyph,
				  PangoRectangle *ink_rect,
				  PangoRectangle *logical_rect)
{
  PangoFT2GlyphInfo *info;
  gboolean empty = FALSE;

  if (glyph == PANGO_GLYPH_EMPTY)
    {
      glyph = pango_fc_font_get_glyph ((PangoFcFont *) font, ' ');
      empty = TRUE;
    }

  if (glyph & PANGO_GLYPH_UNKNOWN_FLAG)
    {
      PangoFontMetrics *metrics = pango_font_get_metrics (font, NULL);

      if (metrics)
	{
	  if (ink_rect)
	    {
	      ink_rect->x = PANGO_SCALE;
	      ink_rect->width = metrics->approximate_char_width - 2 * PANGO_SCALE;
	      ink_rect->y = - (metrics->ascent - PANGO_SCALE);
	      ink_rect->height = metrics->ascent + metrics->descent - 2 * PANGO_SCALE;
	    }
	  if (logical_rect)
	    {
	      logical_rect->x = 0;
	      logical_rect->width = metrics->approximate_char_width;
	      logical_rect->y = -metrics->ascent;
	      logical_rect->height = metrics->ascent + metrics->descent;
	    }

	  pango_font_metrics_unref (metrics);
	}
      else
	{
	  if (ink_rect)
	    ink_rect->x = ink_rect->y = ink_rect->height = ink_rect->width = 0;
	  if (logical_rect)
	    logical_rect->x = logical_rect->y = logical_rect->height = logical_rect->width = 0;
	}
      return;
    }

  info = pango_ft2_font_get_glyph_info (font, glyph, TRUE);

  if (ink_rect)
    *ink_rect = info->ink_rect;
  if (logical_rect)
    *logical_rect = info->logical_rect;

  if (empty)
    {
      if (ink_rect)
	ink_rect->x = ink_rect->y = ink_rect->height = ink_rect->width = 0;
      if (logical_rect)
	logical_rect->x = logical_rect->width = 0;
      return;
    }
}

/**
 * pango_ft2_font_get_kerning:
 * @font: a `PangoFont`
 * @left: the left `PangoGlyph`
 * @right: the right `PangoGlyph`
 *
 * Retrieves kerning information for a combination of two glyphs.
 *
 * Use pango_fc_font_kern_glyphs() instead.
 *
 * Return value: The amount of kerning (in Pango units) to
 *   apply for the given combination of glyphs.
 */
int
pango_ft2_font_get_kerning (PangoFont *font,
			    PangoGlyph left,
			    PangoGlyph right)
{
  PangoFcFont *fc_font = PANGO_FC_FONT (font);

  FT_Face face;
  FT_Error error;
  FT_Vector kerning;

  face = pango_fc_font_lock_face (fc_font);
  if (!face)
    return 0;

  if (!FT_HAS_KERNING (face))
    {
      pango_fc_font_unlock_face (fc_font);
      return 0;
    }

  error = FT_Get_Kerning (face, left, right, ft_kerning_default, &kerning);
  if (error != FT_Err_Ok)
    {
      pango_fc_font_unlock_face (fc_font);
      return 0;
    }

  pango_fc_font_unlock_face (fc_font);
  return PANGO_UNITS_26_6 (kerning.x);
}

static FT_Face
pango_ft2_font_real_lock_face (PangoFcFont *font)
{
  return pango_ft2_font_get_face ((PangoFont *)font);
}

static void
pango_ft2_font_real_unlock_face (PangoFcFont *font G_GNUC_UNUSED)
{
}

static gboolean
pango_ft2_free_glyph_info_callback (gpointer key G_GNUC_UNUSED,
				    gpointer value,
				    gpointer data)
{
  PangoFT2Font *font = PANGO_FT2_FONT (data);
  PangoFT2GlyphInfo *info = value;

  if (font->glyph_cache_destroy && info->cached_glyph)
    (*font->glyph_cache_destroy) (info->cached_glyph);

  g_slice_free (PangoFT2GlyphInfo, info);
  return TRUE;
}

static void
pango_ft2_font_finalize (GObject *object)
{
  PangoFT2Font *ft2font = (PangoFT2Font *)object;

  if (ft2font->face)
    {
      FT_Done_Face (ft2font->face);
      ft2font->face = NULL;
    }

  g_hash_table_foreach_remove (ft2font->glyph_info,
			       pango_ft2_free_glyph_info_callback, object);
  g_hash_table_destroy (ft2font->glyph_info);

  G_OBJECT_CLASS (pango_ft2_font_parent_class)->finalize (object);
}

/**
 * pango_ft2_font_get_coverage:
 * @font: a Pango FT2 font
 * @language: a language tag.
 *
 * Gets the `PangoCoverage` for a `PangoFT2Font`.
 *
 * Use [method@Pango.Font.get_coverage] instead.
 *
 * Return value: (transfer full): a `PangoCoverage`
 */
PangoCoverage *
pango_ft2_font_get_coverage (PangoFont     *font,
			     PangoLanguage *language)
{
  return pango_font_get_coverage (font, language);
}

/* Utility functions */

/**
 * pango_ft2_get_unknown_glyph:
 * @font: a `PangoFont`
 *
 * Return the index of a glyph suitable for drawing unknown
 * characters with @font, or %PANGO_GLYPH_EMPTY if no suitable
 * glyph found.
 *
 * If you want to draw an unknown-box for a character that
 * is not covered by the font, use PANGO_GET_UNKNOWN_GLYPH()
 * instead.
 *
 * Return value: a glyph index into @font, or %PANGO_GLYPH_EMPTY
 */
PangoGlyph
pango_ft2_get_unknown_glyph (PangoFont *font)
{
  FT_Face face = pango_ft2_font_get_face (font);
  if (face && FT_IS_SFNT (face))
    /* TrueType fonts have an 'unknown glyph' box on glyph index 0 */
    return 0;
  else
    return PANGO_GLYPH_EMPTY;
}

void *
_pango_ft2_font_get_cache_glyph_data (PangoFont *font,
				     int        glyph_index)
{
  PangoFT2GlyphInfo *info;

  if (!PANGO_FT2_IS_FONT (font))
    return NULL;

  info = pango_ft2_font_get_glyph_info (font, glyph_index, FALSE);

  if (info == NULL)
    return NULL;

  return info->cached_glyph;
}

void
_pango_ft2_font_set_cache_glyph_data (PangoFont     *font,
				     int            glyph_index,
				     void          *cached_glyph)
{
  PangoFT2GlyphInfo *info;

  if (!PANGO_FT2_IS_FONT (font))
    return;

  info = pango_ft2_font_get_glyph_info (font, glyph_index, TRUE);

  info->cached_glyph = cached_glyph;

  /* TODO: Implement limiting of the number of cached glyphs */
}

void
_pango_ft2_font_set_glyph_cache_destroy (PangoFont      *font,
					 GDestroyNotify  destroy_notify)
{
  if (!PANGO_FT2_IS_FONT (font))
    return;

  PANGO_FT2_FONT (font)->glyph_cache_destroy = destroy_notify;
}
