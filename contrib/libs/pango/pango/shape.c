/* Pango
 * shape.c: Convert characters into glyphs.
 *
 * Copyright (C) 1999 Red Hat Software
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

#include "config.h"

#include <string.h>
#include <math.h>
#include <glib.h>

#include "pango-impl-utils.h"
#include "pango-glyph.h"

#include "pango-item-private.h"
#include "pango-font-private.h"

#include <hb-ot.h>

/* {{{ Harfbuzz shaping */
/* {{{ Buffer handling */

static hb_buffer_t *cached_buffer = NULL; /* MT-safe */
G_LOCK_DEFINE_STATIC (cached_buffer);

static hb_buffer_t *
acquire_buffer (gboolean *free_buffer)
{
  hb_buffer_t *buffer;

  if (G_LIKELY (G_TRYLOCK (cached_buffer)))
    {
      if (G_UNLIKELY (!cached_buffer))
        cached_buffer = hb_buffer_create ();

      buffer = cached_buffer;
      *free_buffer = FALSE;
    }
  else
    {
      buffer = hb_buffer_create ();
      *free_buffer = TRUE;
    }

  return buffer;
}

static void
release_buffer (hb_buffer_t *buffer,
                gboolean     free_buffer)
{
  if (G_LIKELY (!free_buffer))
    {
      hb_buffer_reset (buffer);
      G_UNLOCK (cached_buffer);
    }
  else
    hb_buffer_destroy (buffer);
}

/* }}} */
/* {{{ Use PangoFont with Harfbuzz */

typedef struct
{
  PangoFont *font;
  hb_font_t *parent;
  PangoShowFlags show_flags;
} PangoHbShapeContext;

static hb_bool_t
pango_hb_font_get_nominal_glyph (hb_font_t      *font,
                                 void           *font_data,
                                 hb_codepoint_t  unicode,
                                 hb_codepoint_t *glyph,
                                 void           *user_data G_GNUC_UNUSED)
{
  PangoHbShapeContext *context = (PangoHbShapeContext *) font_data;

  if (context->show_flags != 0)
    {
      if ((context->show_flags & PANGO_SHOW_SPACES) != 0 &&
          g_unichar_type (unicode) == G_UNICODE_SPACE_SEPARATOR)
        {
          /* Replace 0x20 by visible space, since we
           * don't draw a hex box for 0x20
           */
          if (unicode == 0x20)
            unicode = 0x2423;
          else
            {
              *glyph = PANGO_GET_UNKNOWN_GLYPH (unicode);
              return TRUE;
            }
        }

      if ((context->show_flags & PANGO_SHOW_IGNORABLES) != 0 &&
          pango_is_default_ignorable (unicode))
        {
          if (pango_get_ignorable (unicode))
            *glyph = PANGO_GET_UNKNOWN_GLYPH (unicode);
          else
            *glyph = PANGO_GLYPH_EMPTY;
          return TRUE;
        }

      if ((context->show_flags & PANGO_SHOW_LINE_BREAKS) != 0 &&
          unicode == 0x2028)
        {
          /* Always mark LS as unknown. If it ends up
           * at the line end, PangoLayout takes care of
           * hiding them, and if they end up in the middle
           * of a line, we are in single paragraph mode
           * and want to show the LS
           */
          *glyph = PANGO_GET_UNKNOWN_GLYPH (unicode);
          return TRUE;
        }
    }

  if (hb_font_get_nominal_glyph (context->parent, unicode, glyph))
    return TRUE;

  /* HarfBuzz knows how to synthesize other spaces from 0x20, so never
   * replace them with unknown glyphs, just tell HarfBuzz that we don't
   * have a glyph.
   *
   * For 0x20, on the other hand, we need to pretend that we have a glyph
   * and rely on our glyph extents code to provide a reasonable width for
   * PANGO_GET_UNKNOWN_WIDTH (0x20).
   */
  if (g_unichar_type (unicode) == G_UNICODE_SPACE_SEPARATOR)
    {
      if (unicode == 0x20)
        {
          *glyph = PANGO_GET_UNKNOWN_GLYPH (0x20);
          return TRUE;
        }

      return FALSE;
    }

  *glyph = PANGO_GET_UNKNOWN_GLYPH (unicode);

  /* We draw our own invalid-Unicode shape, so prevent HarfBuzz
   * from using REPLACEMENT CHARACTER.
   */
  if (unicode > 0x10FFFF)
    return TRUE;

  return FALSE;
}

static hb_position_t
pango_hb_font_get_glyph_h_advance (hb_font_t      *font,
                                   void           *font_data,
                                   hb_codepoint_t  glyph,
                                   void           *user_data G_GNUC_UNUSED)
{
  PangoHbShapeContext *context = (PangoHbShapeContext *) font_data;

  if (glyph & PANGO_GLYPH_UNKNOWN_FLAG)
    {
      PangoRectangle logical;

      pango_font_get_glyph_extents (context->font, glyph, NULL, &logical);
      return logical.width;
    }

  return hb_font_get_glyph_h_advance (context->parent, glyph);
}

static hb_position_t
pango_hb_font_get_glyph_v_advance (hb_font_t      *font,
                                   void           *font_data,
                                   hb_codepoint_t  glyph,
                                   void           *user_data G_GNUC_UNUSED)
{
  PangoHbShapeContext *context = (PangoHbShapeContext *) font_data;

  if (glyph & PANGO_GLYPH_UNKNOWN_FLAG)
    {
      PangoRectangle logical;

      pango_font_get_glyph_extents (context->font, glyph, NULL, &logical);
      return logical.height;
    }

  return hb_font_get_glyph_v_advance (context->parent, glyph);
}

static hb_bool_t
pango_hb_font_get_glyph_extents (hb_font_t          *font,
                                 void               *font_data,
                                 hb_codepoint_t      glyph,
                                 hb_glyph_extents_t *extents,
                                 void               *user_data G_GNUC_UNUSED)
{
  PangoHbShapeContext *context = (PangoHbShapeContext *) font_data;

  if (glyph & PANGO_GLYPH_UNKNOWN_FLAG)
    {
      PangoRectangle ink;

      pango_font_get_glyph_extents (context->font, glyph, &ink, NULL);

      extents->x_bearing = ink.x;
      extents->y_bearing = ink.y;
      extents->width     = ink.width;
      extents->height    = ink.height;

      return TRUE;
    }

  return hb_font_get_glyph_extents (context->parent, glyph, extents);
}

static hb_font_t *
pango_font_get_hb_font_for_context (PangoFont           *font,
                                    PangoHbShapeContext *context)
{
  hb_font_t *hb_font;
  static hb_font_funcs_t *funcs;

  hb_font = pango_font_get_hb_font (font);

  if (G_UNLIKELY (g_once_init_enter (&funcs)))
    {
      hb_font_funcs_t *f = hb_font_funcs_create ();

      hb_font_funcs_set_nominal_glyph_func (f, pango_hb_font_get_nominal_glyph, NULL, NULL);
      hb_font_funcs_set_glyph_h_advance_func (f, pango_hb_font_get_glyph_h_advance, NULL, NULL);
      hb_font_funcs_set_glyph_v_advance_func (f, pango_hb_font_get_glyph_v_advance, NULL, NULL);
      hb_font_funcs_set_glyph_extents_func (f, pango_hb_font_get_glyph_extents, NULL, NULL);

      hb_font_funcs_make_immutable (f);
      g_once_init_leave (&funcs, f);
    }

  context->font = font;
  context->parent = hb_font;

  hb_font = hb_font_create_sub_font (hb_font);
  hb_font_set_funcs (hb_font, funcs, context, NULL);

  return hb_font;
}

/* }}} */
/* {{{ Utilities */

static PangoShowFlags
find_show_flags (const PangoAnalysis *analysis)
{
  GSList *l;
  PangoShowFlags flags = 0;

  for (l = analysis->extra_attrs; l; l = l->next)
    {
      PangoAttribute *attr = l->data;

      if (attr->klass->type == PANGO_ATTR_SHOW)
        flags |= ((PangoAttrInt*)attr)->value;
    }

  return flags;
}

static PangoTextTransform
find_text_transform (const PangoAnalysis *analysis)
{
  GSList *l;
  PangoTextTransform transform = PANGO_TEXT_TRANSFORM_NONE;

  for (l = analysis->extra_attrs; l; l = l->next)
    {
      PangoAttribute *attr = l->data;

      if (attr->klass->type == PANGO_ATTR_TEXT_TRANSFORM)
        transform = (PangoTextTransform) ((PangoAttrInt*)attr)->value;
    }

  return transform;
}

static gboolean
font_has_color (hb_font_t *font)
{
  hb_face_t *face;

  face = hb_font_get_face (font);

#if HB_VERSION_ATLEAST (7, 0, 0)
  if (hb_ot_color_has_paint (face))
    return TRUE;
#endif

  return hb_ot_color_has_layers (face) ||
         hb_ot_color_has_png (face) ||
         hb_ot_color_has_svg (face);
}

static gboolean
glyph_has_color (hb_font_t      *font,
                 hb_codepoint_t  glyph)
{
  hb_face_t *face;
  hb_blob_t *blob;

  face = hb_font_get_face (font);

#if HB_VERSION_ATLEAST (7, 0, 0)
  if (hb_ot_color_glyph_has_paint (face, glyph))
    return TRUE;
#endif

  if (hb_ot_color_glyph_get_layers (face, glyph, 0, NULL, NULL) > 0)
    return TRUE;

  if (hb_ot_color_has_png (face))
    {
      blob = hb_ot_color_glyph_reference_png (font, glyph);
      if (blob)
        {
          guint length = hb_blob_get_length (blob);
          hb_blob_destroy (blob);
          if (length > 0)
            return TRUE;
        }
    }

  if (hb_ot_color_has_svg (face))
    {
      blob = hb_ot_color_glyph_reference_svg (face, glyph);
      if (blob)
        {
          guint length = hb_blob_get_length (blob);
          hb_blob_destroy (blob);
          if (length > 0)
            return TRUE;
        }
    }

  return FALSE;
}

/* }}} */

static void
pango_hb_shape (const char          *item_text,
                int                  item_length,
                const char          *paragraph_text,
                int                  paragraph_length,
                const PangoAnalysis *analysis,
                PangoLogAttr        *log_attrs,
                int                  num_chars,
                PangoGlyphString    *glyphs,
                PangoShapeFlags      flags)
{
  PangoHbShapeContext context = { 0, };
  hb_buffer_flags_t hb_buffer_flags;
  hb_font_t *hb_font;
  hb_buffer_t *hb_buffer;
  hb_direction_t hb_direction;
  gboolean free_buffer;
  hb_glyph_info_t *hb_glyph;
  hb_glyph_position_t *hb_position;
  int last_cluster;
  guint i, num_glyphs;
  unsigned int item_offset = item_text - paragraph_text;
  hb_feature_t features[32];
  unsigned int num_features = 0;
  PangoGlyphInfo *infos;
  PangoTextTransform transform;
  int hyphen_index;
  gboolean font_is_color;

  g_return_if_fail (analysis != NULL);
  g_return_if_fail (analysis->font != NULL);

  context.show_flags = find_show_flags (analysis);
  hb_font = pango_font_get_hb_font_for_context (analysis->font, &context);
  hb_buffer = acquire_buffer (&free_buffer);

  transform = find_text_transform (analysis);

  hb_direction = PANGO_GRAVITY_IS_VERTICAL (analysis->gravity) ? HB_DIRECTION_TTB : HB_DIRECTION_LTR;
  if (analysis->level % 2)
    hb_direction = HB_DIRECTION_REVERSE (hb_direction);
  if (PANGO_GRAVITY_IS_IMPROPER (analysis->gravity))
    hb_direction = HB_DIRECTION_REVERSE (hb_direction);

  hb_buffer_flags = HB_BUFFER_FLAG_BOT | HB_BUFFER_FLAG_EOT;

  if (context.show_flags & PANGO_SHOW_IGNORABLES)
    hb_buffer_flags |= HB_BUFFER_FLAG_PRESERVE_DEFAULT_IGNORABLES;

  /* setup buffer */

  hb_buffer_set_direction (hb_buffer, hb_direction);
  hb_buffer_set_script (hb_buffer, (hb_script_t) g_unicode_script_to_iso15924 (analysis->script));
  hb_buffer_set_language (hb_buffer, hb_language_from_string (pango_language_to_string (analysis->language), -1));
  hb_buffer_set_cluster_level (hb_buffer, HB_BUFFER_CLUSTER_LEVEL_MONOTONE_CHARACTERS);
  hb_buffer_set_flags (hb_buffer, hb_buffer_flags);
  hb_buffer_set_invisible_glyph (hb_buffer, PANGO_GLYPH_EMPTY);

  if (analysis->flags & PANGO_ANALYSIS_FLAG_NEED_HYPHEN)
    {
      const char *p = paragraph_text + item_offset + item_length;
      int last_char_len = p - g_utf8_prev_char (p);

      hyphen_index = item_offset + item_length - last_char_len;

      if (log_attrs && log_attrs[num_chars].break_removes_preceding)
        item_length -= last_char_len;
    }

  /* Add pre-context */
  hb_buffer_add_utf8 (hb_buffer, paragraph_text, item_offset, item_offset, 0);

  if (transform == PANGO_TEXT_TRANSFORM_NONE)
    {
      hb_buffer_add_utf8 (hb_buffer, paragraph_text, item_offset + item_length, item_offset, item_length);
    }
  else
    {
      const char *p;
      int i;

      /* Transform the item text according to text transform.
       * Note: we assume text transforms won't cross font boundaries
       */
      for (p = paragraph_text + item_offset, i = 0;
           p < paragraph_text + item_offset + item_length;
           p = g_utf8_next_char (p), i++)
        {
          int index = p - paragraph_text;
          gunichar ch = g_utf8_get_char (p);
          char *str = NULL;

          switch (transform)
            {
            case PANGO_TEXT_TRANSFORM_LOWERCASE:
              if (g_unichar_isalnum (ch))
                str = g_utf8_strdown (p, g_utf8_next_char (p) - p);
              break;

            case PANGO_TEXT_TRANSFORM_UPPERCASE:
              if (g_unichar_isalnum (ch))
                str = g_utf8_strup (p, g_utf8_next_char (p) - p);
              break;

            case PANGO_TEXT_TRANSFORM_CAPITALIZE:
              if (log_attrs && log_attrs[i].is_word_start)
                ch = g_unichar_totitle (ch);
              break;

            case PANGO_TEXT_TRANSFORM_NONE:
            default:
              g_assert_not_reached ();
            }

          if (str)
            {
              for (const char *q = str; *q; q = g_utf8_next_char (q))
                {
                  ch = g_utf8_get_char (q);
                  hb_buffer_add (hb_buffer, ch, index);
                }
              g_free (str);
            }
          else
            hb_buffer_add (hb_buffer, ch, index);
        }
    }

  /* Add post-context */
  hb_buffer_add_utf8 (hb_buffer, paragraph_text, paragraph_length, item_offset + item_length, 0);

  if (analysis->flags & PANGO_ANALYSIS_FLAG_NEED_HYPHEN)
    {
      /* Insert either a Unicode or ASCII hyphen. We may
       * want to look for script-specific hyphens here.
       */
      hb_codepoint_t glyph;

      /* Note: We rely on hb_buffer_add clearing existing post-context */
      if (hb_font_get_nominal_glyph (hb_font, 0x2010, &glyph))
        hb_buffer_add (hb_buffer, 0x2010, hyphen_index);
      else if (hb_font_get_nominal_glyph (hb_font, '-', &glyph))
        hb_buffer_add (hb_buffer, '-', hyphen_index);
    }

  pango_analysis_collect_features (analysis, features, G_N_ELEMENTS (features), &num_features);

  hb_shape (hb_font, hb_buffer, features, num_features);

  if (PANGO_GRAVITY_IS_IMPROPER (analysis->gravity))
    hb_buffer_reverse (hb_buffer);

  /* buffer output */
  num_glyphs = hb_buffer_get_length (hb_buffer);
  hb_glyph = hb_buffer_get_glyph_infos (hb_buffer, NULL);
  pango_glyph_string_set_size (glyphs, num_glyphs);
  infos = glyphs->glyphs;
  last_cluster = -1;
  font_is_color = font_has_color (hb_font);

  for (i = 0; i < num_glyphs; i++)
    {
      infos[i].glyph = hb_glyph->codepoint;
      glyphs->log_clusters[i] = hb_glyph->cluster - item_offset;
      infos[i].attr.is_cluster_start = glyphs->log_clusters[i] != last_cluster;
      infos[i].attr.is_color = font_is_color && glyph_has_color (hb_font, hb_glyph->codepoint);
      hb_glyph++;
      last_cluster = glyphs->log_clusters[i];
    }

  hb_position = hb_buffer_get_glyph_positions (hb_buffer, NULL);
  if (PANGO_GRAVITY_IS_VERTICAL (analysis->gravity))
    for (i = 0; i < num_glyphs; i++)
      {
        /* 90 degrees rotation counter-clockwise. */
        infos[i].geometry.width    = - hb_position->y_advance;
        infos[i].geometry.x_offset = - hb_position->y_offset;
        infos[i].geometry.y_offset = - hb_position->x_offset;
        hb_position++;
      }
  else /* horizontal */
    for (i = 0; i < num_glyphs; i++)
      {
        infos[i].geometry.width    =   hb_position->x_advance;
        infos[i].geometry.x_offset =   hb_position->x_offset;
        infos[i].geometry.y_offset = - hb_position->y_offset;
        hb_position++;
      }

  release_buffer (hb_buffer, free_buffer);
  hb_font_destroy (hb_font);
}

/* }}} */
/* {{{ Fallback shaping */

/* This is not meant to produce reasonable results */

static void
fallback_shape (const char          *text,
                unsigned int         length,
                const PangoAnalysis *analysis,
                PangoGlyphString    *glyphs)
{
  int n_chars;
  const char *p;
  int cluster = 0;
  int i;

  n_chars = text ? pango_utf8_strlen (text, length) : 0;

  pango_glyph_string_set_size (glyphs, n_chars);

  p = text;
  for (i = 0; i < n_chars; i++)
    {
      gunichar wc;
      PangoGlyph glyph;
      PangoRectangle logical_rect;

      wc = g_utf8_get_char (p);

      if (g_unichar_type (wc) != G_UNICODE_NON_SPACING_MARK)
        cluster = p - text;

      if (pango_is_zero_width (wc))
        glyph = PANGO_GLYPH_EMPTY;
      else
        glyph = PANGO_GET_UNKNOWN_GLYPH (wc);

      pango_font_get_glyph_extents (analysis->font, glyph, NULL, &logical_rect);

      glyphs->glyphs[i].glyph = glyph;

      glyphs->glyphs[i].geometry.x_offset = 0;
      glyphs->glyphs[i].geometry.y_offset = 0;
      glyphs->glyphs[i].geometry.width = logical_rect.width;

      glyphs->log_clusters[i] = cluster;

      p = g_utf8_next_char (p);
    }

  if (analysis->level & 1)
    pango_glyph_string_reverse_range (glyphs, 0, glyphs->num_glyphs);
}

/*  }}} */
/* {{{ Shaping implementation */

static void
pango_shape_internal (const char          *item_text,
                      int                  item_length,
                      const char          *paragraph_text,
                      int                  paragraph_length,
                      const PangoAnalysis *analysis,
                      PangoLogAttr        *log_attrs,
                      int                  num_chars,
                      PangoGlyphString    *glyphs,
                      PangoShapeFlags      flags)
{
  int i;
  int last_cluster;

  glyphs->num_glyphs = 0;

  if (item_length == -1)
    item_length = strlen (item_text);

  if (!paragraph_text)
    {
      paragraph_text = item_text;
      paragraph_length = item_length;
    }
  if (paragraph_length == -1)
    paragraph_length = strlen (paragraph_text);

  g_return_if_fail (paragraph_text <= item_text);
  g_return_if_fail (paragraph_text + paragraph_length >= item_text + item_length);

  if (analysis->font)
    {
      pango_hb_shape (item_text, item_length,
                      paragraph_text, paragraph_length,
                      analysis,
                      log_attrs, num_chars,
                      glyphs, flags);

      if (G_UNLIKELY (glyphs->num_glyphs == 0))
        {
          /* If a font has been correctly chosen, but no glyphs are output,
           * there's probably something wrong with the font.
           *
           * Trying to be informative, we print out the font description,
           * and the text, but to not flood the terminal with
           * zillions of the message, we set a flag to only err once per
           * font.
           */
          GQuark warned_quark = g_quark_from_static_string ("pango-shape-fail-warned");

          if (!g_object_get_qdata (G_OBJECT (analysis->font), warned_quark))
            {
              PangoFontDescription *desc;
              char *font_name;

              desc = pango_font_describe (analysis->font);
              font_name = pango_font_description_to_string (desc);
              pango_font_description_free (desc);

              g_warning ("shaping failure, expect ugly output. font='%s', text='%.*s'",
                         font_name, item_length, item_text);

              g_free (font_name);

              g_object_set_qdata (G_OBJECT (analysis->font), warned_quark,
                                  GINT_TO_POINTER (1));
            }
        }
    }
  else
    glyphs->num_glyphs = 0;

  if (G_UNLIKELY (!glyphs->num_glyphs))
    {
      fallback_shape (item_text, item_length, analysis, glyphs);
      if (G_UNLIKELY (!glyphs->num_glyphs))
        return;
    }

  /* make sure last_cluster is invalid */
  last_cluster = glyphs->log_clusters[0] - 1;
  for (i = 0; i < glyphs->num_glyphs; i++)
    {
      /* Set glyphs[i].attr.is_cluster_start based on log_clusters[] */
      if (glyphs->log_clusters[i] != last_cluster)
        {
          glyphs->glyphs[i].attr.is_cluster_start = TRUE;
          last_cluster = glyphs->log_clusters[i];
        }
      else
        glyphs->glyphs[i].attr.is_cluster_start = FALSE;


      /* Shift glyph if width is negative, and negate width.
       * This is useful for rotated font matrices and shouldn't
       * harm in normal cases.
       */
      if (glyphs->glyphs[i].geometry.width < 0)
        {
          glyphs->glyphs[i].geometry.width = -glyphs->glyphs[i].geometry.width;
          glyphs->glyphs[i].geometry.x_offset += glyphs->glyphs[i].geometry.width;
        }
    }

  /* Make sure glyphstring direction conforms to analysis->level */
  if (G_UNLIKELY ((analysis->level & 1) &&
                  glyphs->log_clusters[0] < glyphs->log_clusters[glyphs->num_glyphs - 1]))
    {
      g_warning ("Expected RTL run but got LTR. Fixing.");

      /* *Fix* it so we don't crash later */
      pango_glyph_string_reverse_range (glyphs, 0, glyphs->num_glyphs);
    }

  if (flags & PANGO_SHAPE_ROUND_POSITIONS)
    {
      if (analysis->font && pango_font_is_hinted (analysis->font))
        {
          double x_scale_inv, y_scale_inv;
          double x_scale, y_scale;

          pango_font_get_scale_factors (analysis->font, &x_scale_inv, &y_scale_inv);

          if (PANGO_GRAVITY_IS_IMPROPER (analysis->gravity))
            {
              x_scale_inv = -x_scale_inv;
              y_scale_inv = -y_scale_inv;
            }

          x_scale = 1.0 / x_scale_inv;
          y_scale = 1.0 / y_scale_inv;

          if (x_scale == 1.0 && y_scale == 1.0)
            {
              for (i = 0; i < glyphs->num_glyphs; i++)
                glyphs->glyphs[i].geometry.width = PANGO_UNITS_ROUND (glyphs->glyphs[i].geometry.width);
            }
          else
            {
    #if 0
                  if (PANGO_GRAVITY_IS_VERTICAL (analysis->gravity))
                    {
                      /* XXX */
                      double tmp = x_scale;
                      x_scale = y_scale;
                      y_scale = -tmp;
                    }
    #endif
    #define HINT(value, scale_inv, scale) (PANGO_UNITS_ROUND ((int) ((value) * scale)) * scale_inv)
    #define HINT_X(value) HINT ((value), x_scale, x_scale_inv)
    #define HINT_Y(value) HINT ((value), y_scale, y_scale_inv)
              for (i = 0; i < glyphs->num_glyphs; i++)
                {
                  glyphs->glyphs[i].geometry.width    = HINT_X (glyphs->glyphs[i].geometry.width);
                  glyphs->glyphs[i].geometry.x_offset = HINT_X (glyphs->glyphs[i].geometry.x_offset);
                  glyphs->glyphs[i].geometry.y_offset = HINT_Y (glyphs->glyphs[i].geometry.y_offset);
                }
    #undef HINT_Y
    #undef HINT_X
    #undef HINT
            }
        }
      else
        {
          for (i = 0; i < glyphs->num_glyphs; i++)
            {
              glyphs->glyphs[i].geometry.width =
                PANGO_UNITS_ROUND (glyphs->glyphs[i].geometry.width);
              glyphs->glyphs[i].geometry.x_offset =
                PANGO_UNITS_ROUND (glyphs->glyphs[i].geometry.x_offset);
              glyphs->glyphs[i].geometry.y_offset =
                PANGO_UNITS_ROUND (glyphs->glyphs[i].geometry.y_offset);
            }
        }
    }
}

/* }}} */
/* {{{ Public API */

/**
 * pango_shape:
 * @text: the text to process
 * @length: the length (in bytes) of @text
 * @analysis: `PangoAnalysis` structure from [func@Pango.itemize]
 * @glyphs: glyph string in which to store results
 *
 * Convert the characters in @text into glyphs.
 *
 * Given a segment of text and the corresponding `PangoAnalysis` structure
 * returned from [func@Pango.itemize], convert the characters into glyphs. You
 * may also pass in only a substring of the item from [func@Pango.itemize].
 *
 * It is recommended that you use [func@Pango.shape_full] instead, since
 * that API allows for shaping interaction happening across text item
 * boundaries.
 *
 * Some aspects of hyphen insertion and text transformation (in particular,
 * capitalization) require log attrs, and thus can only be handled by
 * [func@Pango.shape_item].
 *
 * Note that the extra attributes in the @analyis that is returned from
 * [func@Pango.itemize] have indices that are relative to the entire paragraph,
 * so you need to subtract the item offset from their indices before
 * calling [func@Pango.shape].
 */
void
pango_shape (const char          *text,
             int                  length,
             const PangoAnalysis *analysis,
             PangoGlyphString    *glyphs)
{
  pango_shape_full (text, length, text, length, analysis, glyphs);
}

/**
 * pango_shape_full:
 * @item_text: valid UTF-8 text to shape.
 * @item_length: the length (in bytes) of @item_text. -1 means nul-terminated text.
 * @paragraph_text: (nullable): text of the paragraph (see details).
 * @paragraph_length: the length (in bytes) of @paragraph_text. -1 means nul-terminated text.
 * @analysis: `PangoAnalysis` structure from [func@Pango.itemize].
 * @glyphs: glyph string in which to store results.
 *
 * Convert the characters in @text into glyphs.
 *
 * Given a segment of text and the corresponding `PangoAnalysis` structure
 * returned from [func@Pango.itemize], convert the characters into glyphs.
 * You may also pass in only a substring of the item from [func@Pango.itemize].
 *
 * This is similar to [func@Pango.shape], except it also can optionally take
 * the full paragraph text as input, which will then be used to perform
 * certain cross-item shaping interactions. If you have access to the broader
 * text of which @item_text is part of, provide the broader text as
 * @paragraph_text. If @paragraph_text is %NULL, item text is used instead.
 *
 * Some aspects of hyphen insertion and text transformation (in particular,
 * capitalization) require log attrs, and thus can only be handled by
 * [func@Pango.shape_item].
 *
 * Note that the extra attributes in the @analyis that is returned from
 * [func@Pango.itemize] have indices that are relative to the entire paragraph,
 * so you do not pass the full paragraph text as @paragraph_text, you need
 * to subtract the item offset from their indices before calling
 * [func@Pango.shape_full].
 *
 * Since: 1.32
 */
void
pango_shape_full (const char          *item_text,
                  int                  item_length,
                  const char          *paragraph_text,
                  int                  paragraph_length,
                  const PangoAnalysis *analysis,
                  PangoGlyphString    *glyphs)
{
  pango_shape_with_flags (item_text, item_length,
                          paragraph_text, paragraph_length,
                          analysis,
                          glyphs,
                          PANGO_SHAPE_NONE);
}

/**
 * pango_shape_with_flags:
 * @item_text: valid UTF-8 text to shape
 * @item_length: the length (in bytes) of @item_text.
 *     -1 means nul-terminated text.
 * @paragraph_text: (nullable): text of the paragraph (see details).
 * @paragraph_length: the length (in bytes) of @paragraph_text.
 *     -1 means nul-terminated text.
 * @analysis:  `PangoAnalysis` structure from [func@Pango.itemize]
 * @glyphs: glyph string in which to store results
 * @flags: flags influencing the shaping process
 *
 * Convert the characters in @text into glyphs.
 *
 * Given a segment of text and the corresponding `PangoAnalysis` structure
 * returned from [func@Pango.itemize], convert the characters into glyphs.
 * You may also pass in only a substring of the item from [func@Pango.itemize].
 *
 * This is similar to [func@Pango.shape_full], except it also takes flags
 * that can influence the shaping process.
 *
 * Some aspects of hyphen insertion and text transformation (in particular,
 * capitalization) require log attrs, and thus can only be handled by
 * [func@Pango.shape_item].
 *
 * Note that the extra attributes in the @analyis that is returned from
 * [func@Pango.itemize] have indices that are relative to the entire paragraph,
 * so you do not pass the full paragraph text as @paragraph_text, you need
 * to subtract the item offset from their indices before calling
 * [func@Pango.shape_with_flags].
 *
 * Since: 1.44
 */
void
pango_shape_with_flags (const char          *item_text,
                        int                  item_length,
                        const char          *paragraph_text,
                        int                  paragraph_length,
                        const PangoAnalysis *analysis,
                        PangoGlyphString    *glyphs,
                        PangoShapeFlags      flags)
{
  pango_shape_internal (item_text, item_length,
                        paragraph_text, paragraph_length,
                        analysis, NULL, 0,
                        glyphs, flags);
}

/**
 * pango_shape_item:
 * @item: `PangoItem` to shape
 * @paragraph_text: (nullable): text of the paragraph (see details).
 * @paragraph_length: the length (in bytes) of @paragraph_text.
 *     -1 means nul-terminated text.
 * @log_attrs: (nullable): array of `PangoLogAttr` for @item
 * @glyphs: glyph string in which to store results
 * @flags: flags influencing the shaping process
 *
 * Convert the characters in @item into glyphs.
 *
 * This is similar to [func@Pango.shape_with_flags], except it takes a
 * `PangoItem` instead of separate @item_text and @analysis arguments.
 *
 * It also takes @log_attrs, which are needed for implementing some aspects
 * of hyphen insertion and text transforms (in particular, capitalization).
 *
 * Note that the extra attributes in the @analyis that is returned from
 * [func@Pango.itemize] have indices that are relative to the entire paragraph,
 * so you do not pass the full paragraph text as @paragraph_text, you need
 * to subtract the item offset from their indices before calling
 * [func@Pango.shape_with_flags].
 *
 * Since: 1.50
 */
void
pango_shape_item (PangoItem        *item,
                  const char       *paragraph_text,
                  int               paragraph_length,
                  PangoLogAttr     *log_attrs,
                  PangoGlyphString *glyphs,
                  PangoShapeFlags   flags)
{
  pango_shape_internal (paragraph_text + item->offset, item->length,
                        paragraph_text, paragraph_length,
                        &item->analysis,
                        log_attrs, item->num_chars,
                        glyphs, flags);
}

/* }}} */

/* vim:set foldmethod=marker expandtab: */
