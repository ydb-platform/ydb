/* Pango
 * pango-context.c: Contexts for itemization and shaping
 *
 * Copyright (C) 2000, 2006 Red Hat Software
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
#include <stdlib.h>

#include "pango-context.h"
#include "pango-context-private.h"
#include "pango-impl-utils.h"

#include "pango-font-private.h"
#include "pango-item-private.h"
#include "pango-fontset.h"
#include "pango-fontmap-private.h"
#include "pango-script-private.h"
#include "pango-emoji-private.h"

/**
 * PangoContext:
 *
 * A `PangoContext` stores global information used to control the
 * itemization process.
 *
 * The information stored by `PangoContext` includes the fontmap used
 * to look up fonts, and default values such as the default language,
 * default gravity, or default font.
 *
 * To obtain a `PangoContext`, use [method@Pango.FontMap.create_context].
 */

struct _PangoContextClass
{
  GObjectClass parent_class;

};

static void pango_context_finalize    (GObject       *object);
static void context_changed           (PangoContext  *context);

G_DEFINE_TYPE (PangoContext, pango_context, G_TYPE_OBJECT)

static void
pango_context_init (PangoContext *context)
{
  context->base_dir = PANGO_DIRECTION_WEAK_LTR;
  context->resolved_gravity = context->base_gravity = PANGO_GRAVITY_SOUTH;
  context->gravity_hint = PANGO_GRAVITY_HINT_NATURAL;

  context->serial = 1;
  context->set_language = NULL;
  context->language = pango_language_get_default ();
  context->font_map = NULL;
  context->round_glyph_positions = TRUE;

  context->font_desc = pango_font_description_new ();
  pango_font_description_set_family_static (context->font_desc, "serif");
  pango_font_description_set_style (context->font_desc, PANGO_STYLE_NORMAL);
  pango_font_description_set_variant (context->font_desc, PANGO_VARIANT_NORMAL);
  pango_font_description_set_weight (context->font_desc, PANGO_WEIGHT_NORMAL);
  pango_font_description_set_stretch (context->font_desc, PANGO_STRETCH_NORMAL);
  pango_font_description_set_size (context->font_desc, 12 * PANGO_SCALE);
}

static void
pango_context_class_init (PangoContextClass *klass)
{
  GObjectClass *object_class = G_OBJECT_CLASS (klass);

  object_class->finalize = pango_context_finalize;
}

static void
pango_context_finalize (GObject *object)
{
  PangoContext *context;

  context = PANGO_CONTEXT (object);

  if (context->font_map)
    g_object_unref (context->font_map);

  pango_font_description_free (context->font_desc);
  if (context->matrix)
    pango_matrix_free (context->matrix);

  if (context->metrics)
    pango_font_metrics_unref (context->metrics);

  G_OBJECT_CLASS (pango_context_parent_class)->finalize (object);
}

/**
 * pango_context_new:
 *
 * Creates a new `PangoContext` initialized to default values.
 *
 * This function is not particularly useful as it should always
 * be followed by a [method@Pango.Context.set_font_map] call, and the
 * function [method@Pango.FontMap.create_context] does these two steps
 * together and hence users are recommended to use that.
 *
 * If you are using Pango as part of a higher-level system,
 * that system may have it's own way of create a `PangoContext`.
 * For instance, the GTK toolkit has, among others,
 * `gtk_widget_get_pango_context()`. Use those instead.
 *
 * Returns: (transfer full): the newly allocated `PangoContext`, which should
 *   be freed with g_object_unref().
 */
PangoContext *
pango_context_new (void)
{
  PangoContext *context;

  context = g_object_new (PANGO_TYPE_CONTEXT, NULL);

  return context;
}

static void
update_resolved_gravity (PangoContext *context)
{
  if (context->base_gravity == PANGO_GRAVITY_AUTO)
    context->resolved_gravity = pango_gravity_get_for_matrix (context->matrix);
  else
    context->resolved_gravity = context->base_gravity;
}

/**
 * pango_context_set_matrix:
 * @context: a `PangoContext`
 * @matrix: (nullable): a `PangoMatrix`, or %NULL to unset any existing
 * matrix. (No matrix set is the same as setting the identity matrix.)
 *
 * Sets the transformation matrix that will be applied when rendering
 * with this context.
 *
 * Note that reported metrics are in the user space coordinates before
 * the application of the matrix, not device-space coordinates after the
 * application of the matrix. So, they don't scale with the matrix, though
 * they may change slightly for different matrices, depending on how the
 * text is fit to the pixel grid.
 *
 * Since: 1.6
 */
void
pango_context_set_matrix (PangoContext      *context,
                          const PangoMatrix *matrix)
{
  g_return_if_fail (PANGO_IS_CONTEXT (context));

  if (context->matrix || matrix)
    context_changed (context);

  if (context->matrix)
    pango_matrix_free (context->matrix);
  if (matrix)
    context->matrix = pango_matrix_copy (matrix);
  else
    context->matrix = NULL;

  update_resolved_gravity (context);
}

/**
 * pango_context_get_matrix:
 * @context: a `PangoContext`
 *
 * Gets the transformation matrix that will be applied when
 * rendering with this context.
 *
 * See [method@Pango.Context.set_matrix].
 *
 * Returns: (transfer none) (nullable): the matrix, or %NULL if no
 *   matrix has been set (which is the same as the identity matrix).
 *   The returned matrix is owned by Pango and must not be modified
 *   or freed.
 *
 * Since: 1.6
 */
const PangoMatrix *
pango_context_get_matrix (PangoContext *context)
{
  g_return_val_if_fail (PANGO_IS_CONTEXT (context), NULL);

  return context->matrix;
}

/**
 * pango_context_set_font_map:
 * @context: a `PangoContext`
 * @font_map: (nullable): the `PangoFontMap` to set.
 *
 * Sets the font map to be searched when fonts are looked-up
 * in this context.
 *
 * This is only for internal use by Pango backends, a `PangoContext`
 * obtained via one of the recommended methods should already have a
 * suitable font map.
 */
void
pango_context_set_font_map (PangoContext *context,
                            PangoFontMap *font_map)
{
  g_return_if_fail (PANGO_IS_CONTEXT (context));
  g_return_if_fail (!font_map || PANGO_IS_FONT_MAP (font_map));

  if (font_map == context->font_map)
    return;

  context_changed (context);

  if (font_map)
    g_object_ref (font_map);

  if (context->font_map)
    g_object_unref (context->font_map);

  context->font_map = font_map;
  context->fontmap_serial = pango_font_map_get_serial (font_map);
}

/**
 * pango_context_get_font_map:
 * @context: a `PangoContext`
 *
 * Gets the `PangoFontMap` used to look up fonts for this context.
 *
 * Returns: (transfer none) (nullable): the font map for the.
 *   `PangoContext` This value is owned by Pango and should not be
 *   unreferenced.
 *
 * Since: 1.6
 */
PangoFontMap *
pango_context_get_font_map (PangoContext *context)
{
  g_return_val_if_fail (PANGO_IS_CONTEXT (context), NULL);

  return context->font_map;
}

/**
 * pango_context_list_families:
 * @context: a `PangoContext`
 * @families: (out) (array length=n_families) (transfer container): location
 *   to store a pointer to an array of `PangoFontFamily`. This array should
 *   be freed with g_free().
 * @n_families: (out): location to store the number of elements in @descs
 *
 * List all families for a context.
 */
void
pango_context_list_families (PangoContext      *context,
                             PangoFontFamily ***families,
                             int               *n_families)
{
  g_return_if_fail (context != NULL);
  g_return_if_fail (families == NULL || n_families != NULL);

  if (n_families == NULL)
    return;

  if (context->font_map == NULL)
    {
      *n_families = 0;
      if (families)
        *families = NULL;

      return;
    }
  else
    pango_font_map_list_families (context->font_map, families, n_families);
}

/**
 * pango_context_load_font:
 * @context: a `PangoContext`
 * @desc: a `PangoFontDescription` describing the font to load
 *
 * Loads the font in one of the fontmaps in the context
 * that is the closest match for @desc.
 *
 * Returns: (transfer full) (nullable): the newly allocated `PangoFont`
 *   that was loaded, or %NULL if no font matched.
 */
PangoFont *
pango_context_load_font (PangoContext               *context,
                         const PangoFontDescription *desc)
{
  g_return_val_if_fail (context != NULL, NULL);
  g_return_val_if_fail (context->font_map != NULL, NULL);

  return pango_font_map_load_font (context->font_map, context, desc);
}

/**
 * pango_context_load_fontset:
 * @context: a `PangoContext`
 * @desc: a `PangoFontDescription` describing the fonts to load
 * @language: a `PangoLanguage` the fonts will be used for
 *
 * Load a set of fonts in the context that can be used to render
 * a font matching @desc.
 *
 * Returns: (transfer full) (nullable): the newly allocated
 *   `PangoFontset` loaded, or %NULL if no font matched.
 */
PangoFontset *
pango_context_load_fontset (PangoContext               *context,
                            const PangoFontDescription *desc,
                            PangoLanguage             *language)
{
  g_return_val_if_fail (context != NULL, NULL);

  return pango_font_map_load_fontset (context->font_map, context, desc, language);
}

/**
 * pango_context_set_font_description:
 * @context: a `PangoContext`
 * @desc: (nullable): the new pango font description
 *
 * Set the default font description for the context
 */
void
pango_context_set_font_description (PangoContext               *context,
                                    const PangoFontDescription *desc)
{
  g_return_if_fail (context != NULL);
  g_return_if_fail (desc != NULL);

  if (desc != context->font_desc &&
      (!desc || !context->font_desc || !pango_font_description_equal(desc, context->font_desc)))
    {
      context_changed (context);

      pango_font_description_free (context->font_desc);
      context->font_desc = pango_font_description_copy (desc);
    }
}

/**
 * pango_context_get_font_description:
 * @context: a `PangoContext`
 *
 * Retrieve the default font description for the context.
 *
 * Returns: (transfer none) (nullable): a pointer to the context's default font
 *   description. This value must not be modified or freed.
 */
PangoFontDescription *
pango_context_get_font_description (PangoContext *context)
{
  g_return_val_if_fail (context != NULL, NULL);

  return context->font_desc;
}

/**
 * pango_context_set_language:
 * @context: a `PangoContext`
 * @language: (nullable): the new language tag.
 *
 * Sets the global language tag for the context.
 *
 * The default language for the locale of the running process
 * can be found using [func@Pango.Language.get_default].
 */
void
pango_context_set_language (PangoContext  *context,
                            PangoLanguage *language)
{
  g_return_if_fail (context != NULL);

  if (language != context->language)
    context_changed (context);

  context->set_language = language;
  if (language)
    context->language = language;
  else
    context->language = pango_language_get_default ();
}

/**
 * pango_context_get_language:
 * @context: a `PangoContext`
 *
 * Retrieves the global language tag for the context.
 *
 * Returns: (transfer none): the global language tag.
 */
PangoLanguage *
pango_context_get_language (PangoContext *context)
{
  g_return_val_if_fail (context != NULL, NULL);

  return context->set_language;
}

/**
 * pango_context_set_base_dir:
 * @context: a `PangoContext`
 * @direction: the new base direction
 *
 * Sets the base direction for the context.
 *
 * The base direction is used in applying the Unicode bidirectional
 * algorithm; if the @direction is %PANGO_DIRECTION_LTR or
 * %PANGO_DIRECTION_RTL, then the value will be used as the paragraph
 * direction in the Unicode bidirectional algorithm. A value of
 * %PANGO_DIRECTION_WEAK_LTR or %PANGO_DIRECTION_WEAK_RTL is used only
 * for paragraphs that do not contain any strong characters themselves.
 */
void
pango_context_set_base_dir (PangoContext   *context,
                            PangoDirection  direction)
{
  g_return_if_fail (context != NULL);

  if (direction != context->base_dir)
    context_changed (context);

  context->base_dir = direction;
}

/**
 * pango_context_get_base_dir:
 * @context: a `PangoContext`
 *
 * Retrieves the base direction for the context.
 *
 * See [method@Pango.Context.set_base_dir].
 *
 * Returns: the base direction for the context.
 */
PangoDirection
pango_context_get_base_dir (PangoContext *context)
{
  g_return_val_if_fail (context != NULL, PANGO_DIRECTION_LTR);

  return context->base_dir;
}

/**
 * pango_context_set_base_gravity:
 * @context: a `PangoContext`
 * @gravity: the new base gravity
 *
 * Sets the base gravity for the context.
 *
 * The base gravity is used in laying vertical text out.
 *
 * Since: 1.16
 */
void
pango_context_set_base_gravity (PangoContext *context,
                                PangoGravity  gravity)
{
  g_return_if_fail (context != NULL);

  if (gravity != context->base_gravity)
    context_changed (context);

  context->base_gravity = gravity;

  update_resolved_gravity (context);
}

/**
 * pango_context_get_base_gravity:
 * @context: a `PangoContext`
 *
 * Retrieves the base gravity for the context.
 *
 * See [method@Pango.Context.set_base_gravity].
 *
 * Returns: the base gravity for the context.
 *
 * Since: 1.16
 */
PangoGravity
pango_context_get_base_gravity (PangoContext *context)
{
  g_return_val_if_fail (context != NULL, PANGO_GRAVITY_SOUTH);

  return context->base_gravity;
}

/**
 * pango_context_get_gravity:
 * @context: a `PangoContext`
 *
 * Retrieves the gravity for the context.
 *
 * This is similar to [method@Pango.Context.get_base_gravity],
 * except for when the base gravity is %PANGO_GRAVITY_AUTO for
 * which [func@Pango.Gravity.get_for_matrix] is used to return the
 * gravity from the current context matrix.
 *
 * Returns: the resolved gravity for the context.
 *
 * Since: 1.16
 */
PangoGravity
pango_context_get_gravity (PangoContext *context)
{
  g_return_val_if_fail (context != NULL, PANGO_GRAVITY_SOUTH);

  return context->resolved_gravity;
}

/**
 * pango_context_set_gravity_hint:
 * @context: a `PangoContext`
 * @hint: the new gravity hint
 *
 * Sets the gravity hint for the context.
 *
 * The gravity hint is used in laying vertical text out, and
 * is only relevant if gravity of the context as returned by
 * [method@Pango.Context.get_gravity] is set to %PANGO_GRAVITY_EAST
 * or %PANGO_GRAVITY_WEST.
 *
 * Since: 1.16
 */
void
pango_context_set_gravity_hint (PangoContext     *context,
                                PangoGravityHint  hint)
{
  g_return_if_fail (context != NULL);

  if (hint != context->gravity_hint)
    context_changed (context);

  context->gravity_hint = hint;
}

/**
 * pango_context_get_gravity_hint:
 * @context: a `PangoContext`
 *
 * Retrieves the gravity hint for the context.
 *
 * See [method@Pango.Context.set_gravity_hint] for details.
 *
 * Returns: the gravity hint for the context.
 *
 * Since: 1.16
 */
PangoGravityHint
pango_context_get_gravity_hint (PangoContext *context)
{
  g_return_val_if_fail (context != NULL, PANGO_GRAVITY_HINT_NATURAL);

  return context->gravity_hint;
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
get_base_metrics (PangoFontset *fontset)
{
  PangoFontMetrics *metrics = pango_font_metrics_new ();

  /* Initialize the metrics from the first font in the fontset */
  pango_fontset_foreach (fontset, get_first_metrics_foreach, metrics);

  return metrics;
}

static void
update_metrics_from_items (PangoFontMetrics *metrics,
                           PangoLanguage    *language,
                           const char       *text,
                           unsigned int      text_len,
                           GList            *items)

{
  GHashTable *fonts_seen = g_hash_table_new (NULL, NULL);
  PangoGlyphString *glyphs = pango_glyph_string_new ();
  GList *l;
  glong text_width;

  /* This should typically be called with a sample text string. */
  g_return_if_fail (text_len > 0);

  metrics->approximate_char_width = 0;

  for (l = items; l; l = l->next)
    {
      PangoItem *item = l->data;
      PangoFont *font = item->analysis.font;

      if (font != NULL && g_hash_table_lookup (fonts_seen, font) == NULL)
        {
          PangoFontMetrics *raw_metrics = pango_font_get_metrics (font, language);
          g_hash_table_insert (fonts_seen, font, font);

          /* metrics will already be initialized from the first font in the fontset */
          metrics->ascent = MAX (metrics->ascent, raw_metrics->ascent);
          metrics->descent = MAX (metrics->descent, raw_metrics->descent);
          metrics->height = MAX (metrics->height, raw_metrics->height);
          pango_font_metrics_unref (raw_metrics);
        }

      pango_shape_full (text + item->offset, item->length,
                        text, text_len,
                        &item->analysis, glyphs);
      metrics->approximate_char_width += pango_glyph_string_get_width (glyphs);
    }

  pango_glyph_string_free (glyphs);
  g_hash_table_destroy (fonts_seen);

  text_width = pango_utf8_strwidth (text);
  g_assert (text_width > 0);
  metrics->approximate_char_width /= text_width;
}

/**
 * pango_context_get_metrics:
 * @context: a `PangoContext`
 * @desc: (nullable): a `PangoFontDescription` structure. %NULL means that the
 *   font description from the context will be used.
 * @language: (nullable): language tag used to determine which script to get
 *   the metrics for. %NULL means that the language tag from the context
 *   will be used. If no language tag is set on the context, metrics
 *   for the default language (as determined by [func@Pango.Language.get_default]
 *   will be returned.
 *
 * Get overall metric information for a particular font description.
 *
 * Since the metrics may be substantially different for different scripts,
 * a language tag can be provided to indicate that the metrics should be
 * retrieved that correspond to the script(s) used by that language.
 *
 * The `PangoFontDescription` is interpreted in the same way as by [func@itemize],
 * and the family name may be a comma separated list of names. If characters
 * from multiple of these families would be used to render the string, then
 * the returned fonts would be a composite of the metrics for the fonts loaded
 * for the individual families.
 *
 * Returns: (transfer full): a `PangoFontMetrics` object. The caller must call
 *   [method@Pango.FontMetrics.unref] when finished using the object.
 */
PangoFontMetrics *
pango_context_get_metrics (PangoContext               *context,
                           const PangoFontDescription *desc,
                           PangoLanguage              *language)
{
  PangoFontset *current_fonts = NULL;
  PangoFontMetrics *metrics;
  const char *sample_str;
  unsigned int text_len;
  GList *items;

  g_return_val_if_fail (PANGO_IS_CONTEXT (context), NULL);

  if (!desc)
    desc = context->font_desc;

  if (!language)
    language = context->language;

  if (desc == context->font_desc &&
      language == context->language &&
      context->metrics != NULL)
    return pango_font_metrics_ref (context->metrics);

  current_fonts = pango_font_map_load_fontset (context->font_map, context, desc, language);
  metrics = get_base_metrics (current_fonts);

  sample_str = pango_language_get_sample_string (language);
  text_len = strlen (sample_str);
  items = pango_itemize_with_font (context, context->base_dir,
                                   sample_str, 0, text_len,
                                   NULL, NULL,
                                   desc);

  update_metrics_from_items (metrics, language, sample_str, text_len, items);

  g_list_foreach (items, (GFunc)pango_item_free, NULL);
  g_list_free (items);

  g_object_unref (current_fonts);

  if (desc == context->font_desc &&
      language == context->language)
    context->metrics = pango_font_metrics_ref (metrics);

  return metrics;
}

static void
context_changed (PangoContext *context)
{
  context->serial++;
  if (context->serial == 0)
    context->serial++;

  g_clear_pointer (&context->metrics, pango_font_metrics_unref);
}

/**
 * pango_context_changed:
 * @context: a `PangoContext`
 *
 * Forces a change in the context, which will cause any `PangoLayout`
 * using this context to re-layout.
 *
 * This function is only useful when implementing a new backend
 * for Pango, something applications won't do. Backends should
 * call this function if they have attached extra data to the context
 * and such data is changed.
 *
 * Since: 1.32.4
 **/
void
pango_context_changed (PangoContext *context)
{
  context_changed (context);
}

static void
check_fontmap_changed (PangoContext *context)
{
  guint old_serial = context->fontmap_serial;

  if (!context->font_map)
    return;

  context->fontmap_serial = pango_font_map_get_serial (context->font_map);

  if (old_serial != context->fontmap_serial)
    context_changed (context);
}

/**
 * pango_context_get_serial:
 * @context: a `PangoContext`
 *
 * Returns the current serial number of @context.
 *
 * The serial number is initialized to an small number larger than zero
 * when a new context is created and is increased whenever the context
 * is changed using any of the setter functions, or the `PangoFontMap` it
 * uses to find fonts has changed. The serial may wrap, but will never
 * have the value 0. Since it can wrap, never compare it with "less than",
 * always use "not equals".
 *
 * This can be used to automatically detect changes to a `PangoContext`,
 * and is only useful when implementing objects that need update when their
 * `PangoContext` changes, like `PangoLayout`.
 *
 * Returns: The current serial number of @context.
 *
 * Since: 1.32.4
 */
guint
pango_context_get_serial (PangoContext *context)
{
  check_fontmap_changed (context);
  return context->serial;
}

/**
 * pango_context_set_round_glyph_positions:
 * @context: a `PangoContext`
 * @round_positions: whether to round glyph positions
 *
 * Sets whether font rendering with this context should
 * round glyph positions and widths to integral positions,
 * in device units.
 *
 * This is useful when the renderer can't handle subpixel
 * positioning of glyphs.
 *
 * The default value is to round glyph positions, to remain
 * compatible with previous Pango behavior.
 *
 * Since: 1.44
 */
void
pango_context_set_round_glyph_positions (PangoContext *context,
                                         gboolean      round_positions)
{
  if (context->round_glyph_positions != round_positions)
    {
      context->round_glyph_positions = round_positions;
      context_changed (context);
    }
}

/**
 * pango_context_get_round_glyph_positions:
 * @context: a `PangoContext`
 *
 * Returns whether font rendering with this context should
 * round glyph positions and widths.
 *
 * Since: 1.44
 */
gboolean
pango_context_get_round_glyph_positions (PangoContext *context)
{
  return context->round_glyph_positions;
}
