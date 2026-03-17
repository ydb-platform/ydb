/* Pango
 * pango-fontset.c:
 *
 * Copyright (C) 2001 Red Hat Software
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

/*
 * PangoFontset
 */

#include "pango-types.h"
#include "pango-font-private.h"
#include "pango-fontset.h"
#include "pango-impl-utils.h"

static PangoFontMetrics *pango_fontset_real_get_metrics (PangoFontset *fontset);


G_DEFINE_ABSTRACT_TYPE (PangoFontset, pango_fontset, G_TYPE_OBJECT);

static void
pango_fontset_init (PangoFontset *self)
{
}

static void
pango_fontset_class_init (PangoFontsetClass *class)
{
  class->get_metrics = pango_fontset_real_get_metrics;
}


/**
 * pango_fontset_get_font:
 * @fontset: a `PangoFontset`
 * @wc: a Unicode character
 *
 * Returns the font in the fontset that contains the best
 * glyph for a Unicode character.
 *
 * Returns: (transfer full): a `PangoFont`
 */
PangoFont *
pango_fontset_get_font (PangoFontset  *fontset,
                        guint          wc)
{

  g_return_val_if_fail (PANGO_IS_FONTSET (fontset), NULL);

  return PANGO_FONTSET_GET_CLASS (fontset)->get_font (fontset, wc);
}

/**
 * pango_fontset_get_metrics:
 * @fontset: a `PangoFontset`
 *
 * Get overall metric information for the fonts in the fontset.
 *
 * Returns: (transfer full): a `PangoFontMetrics` object
 */
PangoFontMetrics *
pango_fontset_get_metrics (PangoFontset  *fontset)
{
  g_return_val_if_fail (PANGO_IS_FONTSET (fontset), NULL);

  return PANGO_FONTSET_GET_CLASS (fontset)->get_metrics (fontset);
}

/**
 * pango_fontset_foreach:
 * @fontset: a `PangoFontset`
 * @func: (closure data) (scope call): Callback function
 * @data: (closure): data to pass to the callback function
 *
 * Iterates through all the fonts in a fontset, calling @func for
 * each one.
 *
 * If @func returns %TRUE, that stops the iteration.
 *
 * Since: 1.4
 */
void
pango_fontset_foreach (PangoFontset           *fontset,
                       PangoFontsetForeachFunc func,
                       gpointer                data)
{
  g_return_if_fail (PANGO_IS_FONTSET (fontset));
  g_return_if_fail (func != NULL);

  PANGO_FONTSET_GET_CLASS (fontset)->foreach (fontset, func, data);
}

static gboolean
get_first_metrics_foreach (PangoFontset  *fontset,
                           PangoFont     *font,
                           gpointer       data)
{
  PangoFontMetrics *fontset_metrics = data;
  PangoLanguage *language = PANGO_FONTSET_GET_CLASS (fontset)->get_language (fontset);
  PangoFontMetrics *font_metrics = pango_font_get_metrics (font, language);
  guint save_ref_count;

  /* Initialize the fontset metrics to metrics of the first font in the
   * fontset; saving the refcount and restoring it is a bit of hack but avoids
   * having to update this code for each metrics addition.
   */
  save_ref_count = fontset_metrics->ref_count;
  *fontset_metrics = *font_metrics;
  fontset_metrics->ref_count = save_ref_count;

  pango_font_metrics_unref (font_metrics);

  return TRUE;                  /* Stops iteration */
}

static PangoFontMetrics *
pango_fontset_real_get_metrics (PangoFontset  *fontset)
{
  PangoFontMetrics *metrics, *raw_metrics;
  const char *sample_str;
  const char *p;
  int count;
  GHashTable *fonts_seen;
  PangoFont *font;
  PangoLanguage *language;

  language = PANGO_FONTSET_GET_CLASS (fontset)->get_language (fontset);
  sample_str = pango_language_get_sample_string (language);

  count = 0;
  metrics = pango_font_metrics_new ();
  fonts_seen = g_hash_table_new_full (NULL, NULL, g_object_unref, NULL);

  /* Initialize the metrics from the first font in the fontset */
  pango_fontset_foreach (fontset, get_first_metrics_foreach, metrics);

  p = sample_str;
  while (*p)
    {
      gunichar wc = g_utf8_get_char (p);
      font = pango_fontset_get_font (fontset, wc);
      if (font)
        {
          if (g_hash_table_lookup (fonts_seen, font) == NULL)
            {
              raw_metrics = pango_font_get_metrics (font, language);
              g_hash_table_insert (fonts_seen, font, font);

              if (count == 0)
                {
                  metrics->ascent = raw_metrics->ascent;
                  metrics->descent = raw_metrics->descent;
                  metrics->approximate_char_width = raw_metrics->approximate_char_width;
                  metrics->approximate_digit_width = raw_metrics->approximate_digit_width;
                }
              else
                {
                  metrics->ascent = MAX (metrics->ascent, raw_metrics->ascent);
                  metrics->descent = MAX (metrics->descent, raw_metrics->descent);
                  metrics->approximate_char_width += raw_metrics->approximate_char_width;
                  metrics->approximate_digit_width += raw_metrics->approximate_digit_width;
                }
              count++;
              pango_font_metrics_unref (raw_metrics);
            }
          else
            g_object_unref (font);
        }

      p = g_utf8_next_char (p);
    }

  g_hash_table_destroy (fonts_seen);

  if (count)
    {
      metrics->approximate_char_width /= count;
      metrics->approximate_digit_width /= count;
    }

  return metrics;
}
