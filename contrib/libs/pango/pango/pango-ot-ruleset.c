/* Pango
 * pango-ot-ruleset.c: Shaping using OpenType features
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

#include "config.h"

#include "pango-ot-private.h"

static void pango_ot_ruleset_finalize   (GObject        *object);

/**
 * PangoOTRuleset:
 *
 * The `PangoOTRuleset` structure holds a set of features selected
 * from the tables in an OpenType font.
 *
 * A feature is an operation such as adjusting glyph positioning
 * that should be applied to a text feature such as a certain
 * type of accent.
 *
 * A `PangoOTRuleset` is created with [ctor@PangoOT.Ruleset.new],
 * features are added to it with [method@PangoOT.Ruleset.add_feature],
 * then it is applied to a `PangoGlyphString` with
 * [method@PangoOT.Ruleset.position].
 */
G_DEFINE_TYPE (PangoOTRuleset, pango_ot_ruleset, G_TYPE_OBJECT);

static void
pango_ot_ruleset_class_init (PangoOTRulesetClass *klass)
{
  GObjectClass *object_class = G_OBJECT_CLASS (klass);

  object_class->finalize = pango_ot_ruleset_finalize;
}

static void
pango_ot_ruleset_init (PangoOTRuleset *ruleset)
{
}

static void
pango_ot_ruleset_finalize (GObject *object)
{
  G_OBJECT_CLASS (pango_ot_ruleset_parent_class)->finalize (object);
}

/**
 * pango_ot_ruleset_get_for_description:
 * @info: a `PangoOTInfo`
 * @desc: a `PangoOTRulesetDescription`
 *
 * Returns a ruleset for the given OpenType info and ruleset
 * description.
 *
 * Rulesets are created on demand using
 * [ctor@PangoOT.Ruleset.new_from_description].
 * The returned ruleset should not be modified or destroyed.
 *
 * The static feature map members of @desc should be alive as
 * long as @info is.
 *
 * Return value: the `PangoOTRuleset` for @desc. This object will have
 *   the same lifetime as @info.
 *
 * Since: 1.18
 */
const PangoOTRuleset *
pango_ot_ruleset_get_for_description (PangoOTInfo                     *info,
				      const PangoOTRulesetDescription *desc)
{
  static PangoOTRuleset *ruleset; /* MT-safe */

  if (g_once_init_enter (&ruleset))
    g_once_init_leave (&ruleset, g_object_new (PANGO_TYPE_OT_RULESET, NULL));

  return ruleset;
}

/**
 * pango_ot_ruleset_new:
 * @info: a `PangoOTInfo`
 *
 * Creates a new `PangoOTRuleset` for the given OpenType info.
 *
 * Return value: the newly allocated `PangoOTRuleset`
 */
PangoOTRuleset *
pango_ot_ruleset_new (PangoOTInfo *info)
{
  return g_object_new (PANGO_TYPE_OT_RULESET, NULL);
}

/**
 * pango_ot_ruleset_new_for:
 * @info: a `PangoOTInfo`
 * @script: a `PangoScript`
 * @language: a `PangoLanguage`
 *
 * Creates a new `PangoOTRuleset` for the given OpenType info, script, and
 * language.
 *
 * This function is part of a convenience scheme that highly simplifies
 * using a `PangoOTRuleset` to represent features for a specific pair of script
 * and language.  So one can use this function passing in the script and
 * language of interest, and later try to add features to the ruleset by just
 * specifying the feature name or tag, without having to deal with finding
 * script, language, or feature indices manually.
 *
 * In addition to what [ctor@PangoOT.Ruleset.new] does, this function will:
 *
 * * Find the `PangoOTTag` script and language tags associated with
 *   @script and @language using [func@PangoOT.tag_from_script] and
 *   [func@PangoOT.tag_from_language],
 *
 * * For each of table types %PANGO_OT_TABLE_GSUB and %PANGO_OT_TABLE_GPOS,
 *   find the script index of the script tag found and the language
 *   system index of the language tag found in that script system, using
 *   [method@PangoOT.Info.find_script] and [method@PangoOT.Info.find_language],
 *
 * * For found language-systems, if they have required feature index,
 *   add that feature to the ruleset using [method@PangoOT.Ruleset.add_feature],
 *
 * * Remember found script and language indices for both table types,
 *   and use them in future [method@PangoOT.Ruleset.maybe_add_feature] and
 *   [method@PangoOT.Ruleset.maybe_add_features].
 *
 * Because of the way return values of [method@PangoOT.Info.find_script] and
 * [method@PangoOT.Info.find_language] are ignored, this function automatically
 * finds and uses the 'DFLT' script and the default language-system.
 *
 * Return value: the newly allocated `PangoOTRuleset`
 *
 * Since: 1.18
 */
PangoOTRuleset *
pango_ot_ruleset_new_for (PangoOTInfo       *info,
			  PangoScript        script,
			  PangoLanguage     *language)
{
  return g_object_new (PANGO_TYPE_OT_RULESET, NULL);
}

/**
 * pango_ot_ruleset_new_from_description:
 * @info: a `PangoOTInfo`
 * @desc: a `PangoOTRulesetDescription`
 *
 * Creates a new `PangoOTRuleset` for the given OpenType info and
 * matching the given ruleset description.
 *
 * This is a convenience function that calls [ctor@PangoOT.Ruleset.new_for]
 * and adds the static GSUB/GPOS features to the resulting ruleset,
 * followed by adding other features to both GSUB and GPOS.
 *
 * The static feature map members of @desc should be alive as
 * long as @info is.
 *
 * Return value: the newly allocated `PangoOTRuleset`
 *
 * Since: 1.18
 */
PangoOTRuleset *
pango_ot_ruleset_new_from_description (PangoOTInfo                     *info,
				       const PangoOTRulesetDescription *desc)
{
  return g_object_new (PANGO_TYPE_OT_RULESET, NULL);
}

/**
 * pango_ot_ruleset_add_feature:
 * @ruleset: a `PangoOTRuleset`
 * @table_type: the table type to add a feature to
 * @feature_index: the index of the feature to add
 * @property_bit: the property bit to use for this feature. Used to
 *   identify the glyphs that this feature should be applied to, or
 *   %PANGO_OT_ALL_GLYPHS if it should be applied to all glyphs.
 *
 * Adds a feature to the ruleset.
 */
void
pango_ot_ruleset_add_feature (PangoOTRuleset   *ruleset,
			      PangoOTTableType  table_type,
			      guint             feature_index,
			      gulong            property_bit)
{
}

/**
 * pango_ot_ruleset_maybe_add_feature:
 * @ruleset: a `PangoOTRuleset`
 * @table_type: the table type to add a feature to
 * @feature_tag: the tag of the feature to add
 * @property_bit: the property bit to use for this feature. Used to
 *   identify the glyphs that this feature should be applied to, or
 *   %PANGO_OT_ALL_GLYPHS if it should be applied to all glyphs.
 *
 * This is a convenience function that first tries to find the feature
 * using [method@PangoOT.Info.find_feature] and the ruleset script and
 * language passed to [ctor@PangoOT.Ruleset.new_for] and if the feature
 * is found, adds it to the ruleset.
 *
 * If @ruleset was not created using [ctor@PangoOT.Ruleset.new_for],
 * this function does nothing.
 *
 * Return value: %TRUE if the feature was found and added to ruleset,
 *   %FALSE otherwise
 *
 * Since: 1.18
 */
gboolean
pango_ot_ruleset_maybe_add_feature (PangoOTRuleset          *ruleset,
				    PangoOTTableType         table_type,
				    PangoOTTag               feature_tag,
				    gulong                   property_bit)
{
  return FALSE;
}

/**
 * pango_ot_ruleset_maybe_add_features:
 * @ruleset: a `PangoOTRuleset`
 * @table_type: the table type to add features to
 * @features: array of feature name and property bits to add
 * @n_features: number of feature records in @features array
 *
 * This is a convenience function that for each feature in the feature map
 * array @features converts the feature name to a `PangoOTTag` feature tag
 * using PANGO_OT_TAG_MAKE() and calls [method@PangoOT.Ruleset.maybe_add_feature]
 * on it.
 *
 * Return value: The number of features in @features that were found
 *   and added to @ruleset
 *
 * Since: 1.18
 */
guint
pango_ot_ruleset_maybe_add_features (PangoOTRuleset          *ruleset,
				     PangoOTTableType         table_type,
				     const PangoOTFeatureMap *features,
				     guint                    n_features)
{
  return 0;
}

/**
 * pango_ot_ruleset_get_feature_count:
 * @ruleset: a `PangoOTRuleset`
 * @n_gsub_features: (out) (optional): location to store number of GSUB features
 * @n_gpos_features: (out) (optional): location to store number of GPOS features
 *
 * Gets the number of GSUB and GPOS features in the ruleset.
 *
 * Return value: Total number of features in the @ruleset
 *
 * Since: 1.18
 */
guint
pango_ot_ruleset_get_feature_count (const PangoOTRuleset   *ruleset,
				    guint                  *n_gsub_features,
				    guint                  *n_gpos_features)
{
  return 0;
}

/**
 * pango_ot_ruleset_substitute:
 * @ruleset: a `PangoOTRuleset`
 * @buffer: a `PangoOTBuffer`
 *
 * Performs the OpenType GSUB substitution on @buffer using
 * the features in @ruleset.
 *
 * Since: 1.4
 */
void
pango_ot_ruleset_substitute  (const PangoOTRuleset *ruleset,
			      PangoOTBuffer        *buffer)
{
}

/**
 * pango_ot_ruleset_position:
 * @ruleset: a `PangoOTRuleset`
 * @buffer: a `PangoOTBuffer`
 *
 * Performs the OpenType GPOS positioning on @buffer using
 * the features in @ruleset.
 *
 * Since: 1.4
 */
void
pango_ot_ruleset_position (const PangoOTRuleset *ruleset,
			   PangoOTBuffer        *buffer)
{
}


/* ruleset descriptions */

/**
 * pango_ot_ruleset_description_hash:
 * @desc: a ruleset description
 *
 * Computes a hash of a `PangoOTRulesetDescription` structure suitable
 * to be used, for example, as an argument to g_hash_table_new().
 *
 * Return value: the hash value
 *
 * Since: 1.18
 */
guint
pango_ot_ruleset_description_hash  (const PangoOTRulesetDescription *desc)
{
  return 0;
}

/**
 * pango_ot_ruleset_description_equal:
 * @desc1: a ruleset description
 * @desc2: a ruleset description
 *
 * Compares two ruleset descriptions for equality.
 *
 * Two ruleset descriptions are considered equal if the rulesets
 * they describe are provably identical. This means that their
 * script, language, and all feature sets should be equal.
 *
 * For static feature sets, the array addresses are compared directly,
 * while for other features, the list of features is compared one by
 * one.(Two ruleset descriptions may result in identical rulesets
 * being created, but still compare %FALSE.)
 *
 * Return value: %TRUE if two ruleset descriptions are identical,
 *   %FALSE otherwise
 *
 * Since: 1.18
 **/
gboolean
pango_ot_ruleset_description_equal (const PangoOTRulesetDescription *desc1,
				    const PangoOTRulesetDescription *desc2)
{
  return TRUE;
}

G_DEFINE_BOXED_TYPE (PangoOTRulesetDescription, pango_ot_ruleset_description,
                     pango_ot_ruleset_description_copy,
                     pango_ot_ruleset_description_free)

/**
 * pango_ot_ruleset_description_copy:
 * @desc: ruleset description to copy
 *
 * Creates a copy of @desc, which should be freed with
 * [method@PangoOT.RulesetDescription.free].
 *
 * Primarily used internally by [func@PangoOT.Ruleset.get_for_description]
 * to cache rulesets for ruleset descriptions.
 *
 * Return value: the newly allocated `PangoOTRulesetDescription`
 *
 * Since: 1.18
 */
PangoOTRulesetDescription *
pango_ot_ruleset_description_copy  (const PangoOTRulesetDescription *desc)
{
  PangoOTRulesetDescription *copy;

  g_return_val_if_fail (desc != NULL, NULL);

  copy = g_slice_new (PangoOTRulesetDescription);

  *copy = *desc;

  return copy;
}

/**
 * pango_ot_ruleset_description_free:
 * @desc: an allocated `PangoOTRulesetDescription`
 *
 * Frees a ruleset description allocated by
 * pango_ot_ruleset_description_copy().
 *
 * Since: 1.18
 */
void
pango_ot_ruleset_description_free (PangoOTRulesetDescription *desc)
{
  g_slice_free (PangoOTRulesetDescription, desc);
}
