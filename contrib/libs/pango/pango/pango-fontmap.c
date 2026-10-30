/* Pango
 * pango-fontmap.c: Font handling
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#include "config.h"

#include <gio/gio.h>

#include "pango-fontmap-private.h"
#include "pango-fontset-simple.h"
#include "pango-impl-utils.h"
#include <stdlib.h>
#include <math.h>

static PangoFontset *pango_font_map_real_load_fontset (PangoFontMap               *fontmap,
                                                       PangoContext               *context,
                                                       const PangoFontDescription *desc,
                                                       PangoLanguage              *language);


static PangoFontFamily *pango_font_map_real_get_family (PangoFontMap *fontmap,
                                                        const char   *name);

static void pango_font_map_real_changed (PangoFontMap *fontmap);

static PangoFont *pango_font_map_real_reload_font (PangoFontMap *fontmap,
                                                   PangoFont    *font,
                                                   double        scale,
                                                   PangoContext *context,
                                                   const char   *variations);

static gboolean pango_font_map_real_add_font_file (PangoFontMap  *fontmap,
                                                   const char    *filename,
                                                   GError       **error);

static guint pango_font_map_get_n_items (GListModel *list);

static void pango_font_map_list_model_init (GListModelInterface *iface);

typedef struct {
  guint n_families;
} PangoFontMapPrivate;

enum
{
  PROP_0,
  PROP_ITEM_TYPE,
  PROP_N_ITEMS,
  N_PROPERTIES
};

static GParamSpec *properties[N_PROPERTIES] = { NULL, };

G_DEFINE_ABSTRACT_TYPE_WITH_CODE (PangoFontMap, pango_font_map, G_TYPE_OBJECT,
                                  G_ADD_PRIVATE (PangoFontMap)
                                  g_type_add_class_private (g_define_type_id, sizeof (PangoFontMapClassPrivate));
                                  G_IMPLEMENT_INTERFACE (G_TYPE_LIST_MODEL, pango_font_map_list_model_init))

static void
pango_font_map_get_property (GObject    *object,
                             guint       property_id,
                             GValue     *value,
                             GParamSpec *pspec)
{
  switch (property_id)
    {
    case PROP_ITEM_TYPE:
      g_value_set_gtype (value, PANGO_TYPE_FONT_FAMILY);
      break;

    case PROP_N_ITEMS:
      g_value_set_uint (value, pango_font_map_get_n_items (G_LIST_MODEL (object)));
      break;

    default:
      G_OBJECT_WARN_INVALID_PROPERTY_ID (object, property_id, pspec);
    }
}

static void
pango_font_map_class_init (PangoFontMapClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);
  PangoFontMapClassPrivate *pclass;

  object_class->get_property = pango_font_map_get_property;

  class->load_fontset = pango_font_map_real_load_fontset;
  class->get_family = pango_font_map_real_get_family;
  class->changed = pango_font_map_real_changed;

  pclass = g_type_class_get_private ((GTypeClass *) class, PANGO_TYPE_FONT_MAP);

  pclass->reload_font = pango_font_map_real_reload_font;
  pclass->add_font_file = pango_font_map_real_add_font_file;

  /**
   * PangoFontMap:item-type:
   *
   * The type of items contained in this list.
   */
  properties[PROP_ITEM_TYPE] =
    g_param_spec_gtype ("item-type", "", "", G_TYPE_OBJECT,
                        G_PARAM_READABLE | G_PARAM_STATIC_STRINGS);

  /**
   * PangoFontMap:n-items:
   *
   * The number of items contained in this list.
   */
  properties[PROP_N_ITEMS] =
    g_param_spec_uint ("n-items", "", "", 0, G_MAXUINT, 0,
                       G_PARAM_READABLE | G_PARAM_STATIC_STRINGS);

  g_object_class_install_properties (object_class, N_PROPERTIES, properties);
}

static void
pango_font_map_init (PangoFontMap *fontmap G_GNUC_UNUSED)
{
}

/**
 * pango_font_map_create_context:
 * @fontmap: a `PangoFontMap`
 *
 * Creates a `PangoContext` connected to @fontmap.
 *
 * This is equivalent to [ctor@Pango.Context.new] followed by
 * [method@Pango.Context.set_font_map].
 *
 * If you are using Pango as part of a higher-level system,
 * that system may have it's own way of create a `PangoContext`.
 * For instance, the GTK toolkit has, among others,
 * gtk_widget_get_pango_context(). Use those instead.
 *
 * Return value: (transfer full): the newly allocated `PangoContext`,
 *   which should be freed with g_object_unref().
 *
 * Since: 1.22
 */
PangoContext *
pango_font_map_create_context (PangoFontMap *fontmap)
{
  PangoContext *context;

  g_return_val_if_fail (fontmap != NULL, NULL);

  context = pango_context_new ();
  pango_context_set_font_map (context, fontmap);

  return context;
}

/**
 * pango_font_map_load_font:
 * @fontmap: a `PangoFontMap`
 * @context: the `PangoContext` the font will be used with
 * @desc: a `PangoFontDescription` describing the font to load
 *
 * Load the font in the fontmap that is the closest match for @desc.
 *
 * Returns: (transfer full) (nullable): the newly allocated `PangoFont`
 *   loaded, or %NULL if no font matched.
 */
PangoFont *
pango_font_map_load_font  (PangoFontMap               *fontmap,
                           PangoContext               *context,
                           const PangoFontDescription *desc)
{
  g_return_val_if_fail (fontmap != NULL, NULL);

  return PANGO_FONT_MAP_GET_CLASS (fontmap)->load_font (fontmap, context, desc);
}

/**
 * pango_font_map_list_families:
 * @fontmap: a `PangoFontMap`
 * @families: (out) (array length=n_families) (transfer container): location to
 *   store a pointer to an array of `PangoFontFamily` *.
 *   This array should be freed with g_free().
 * @n_families: (out): location to store the number of elements in @families
 *
 * List all families for a fontmap.
 *
 * Note that the returned families are not in any particular order.
 *
 * `PangoFontMap` also implemented the [iface@Gio.ListModel] interface
 * for enumerating families.
 */
void
pango_font_map_list_families (PangoFontMap      *fontmap,
                              PangoFontFamily ***families,
                              int               *n_families)
{
  PangoFontMapPrivate *priv = pango_font_map_get_instance_private (fontmap);
  g_return_if_fail (fontmap != NULL);

  PANGO_FONT_MAP_GET_CLASS (fontmap)->list_families (fontmap, families, n_families);

  /* keep this value for GListModel::changed */
  priv->n_families = *n_families;
}

/**
 * pango_font_map_load_fontset:
 * @fontmap: a `PangoFontMap`
 * @context: the `PangoContext` the font will be used with
 * @desc: a `PangoFontDescription` describing the font to load
 * @language: a `PangoLanguage` the fonts will be used for
 *
 * Load a set of fonts in the fontmap that can be used to render
 * a font matching @desc.
 *
 * Returns: (transfer full) (nullable): the newly allocated
 *   `PangoFontset` loaded, or %NULL if no font matched.
 */
PangoFontset *
pango_font_map_load_fontset (PangoFontMap               *fontmap,
                             PangoContext               *context,
                             const PangoFontDescription *desc,
                             PangoLanguage              *language)
{
  g_return_val_if_fail (fontmap != NULL, NULL);

  return PANGO_FONT_MAP_GET_CLASS (fontmap)->load_fontset (fontmap, context, desc, language);
}

static void
pango_font_map_fontset_add_fonts (PangoFontMap          *fontmap,
                                  PangoContext          *context,
                                  PangoFontsetSimple    *fonts,
                                  PangoFontDescription  *desc,
                                  const char            *family)
{
  PangoFont *font;

  pango_font_description_set_family_static (desc, family);
  font = pango_font_map_load_font (fontmap, context, desc);
  if (font)
    pango_fontset_simple_append (fonts, font);
}

static PangoFontset *
pango_font_map_real_load_fontset (PangoFontMap               *fontmap,
                                  PangoContext               *context,
                                  const PangoFontDescription *desc,
                                  PangoLanguage              *language)
{
  PangoFontDescription *tmp_desc = pango_font_description_copy_static (desc);
  const char *family;
  char **families;
  int i;
  PangoFontsetSimple *fonts;
  static GHashTable *warned_fonts = NULL; /* MT-safe */
  G_LOCK_DEFINE_STATIC (warned_fonts);

  family = pango_font_description_get_family (desc);
  families = g_strsplit (family ? family : "", ",", -1);

  fonts = pango_fontset_simple_new (language);

  for (i = 0; families[i]; i++)
    pango_font_map_fontset_add_fonts (fontmap,
                                      context,
                                      fonts,
                                      tmp_desc,
                                      families[i]);

  g_strfreev (families);

  /* The font description was completely unloadable, try with
   * family == "Sans"
   */
  if (pango_fontset_simple_size (fonts) == 0)
    {
      char *ctmp1, *ctmp2;

      pango_font_description_set_family_static (tmp_desc,
                                                pango_font_description_get_family (desc));

      ctmp1 = pango_font_description_to_string (desc);
      pango_font_description_set_family_static (tmp_desc, "Sans");

      G_LOCK (warned_fonts);
      if (!warned_fonts || !g_hash_table_lookup (warned_fonts, ctmp1))
        {
          if (!warned_fonts)
            warned_fonts = g_hash_table_new (g_str_hash, g_str_equal);

          g_hash_table_insert (warned_fonts, g_strdup (ctmp1), GINT_TO_POINTER (1));

          ctmp2 = pango_font_description_to_string (tmp_desc);
          g_warning ("couldn't load font \"%s\", falling back to \"%s\", "
                     "expect ugly output.", ctmp1, ctmp2);
          g_free (ctmp2);
        }
      G_UNLOCK (warned_fonts);
      g_free (ctmp1);

      pango_font_map_fontset_add_fonts (fontmap,
                                        context,
                                        fonts,
                                        tmp_desc,
                                        "Sans");
    }

  /* We couldn't try with Sans and the specified style. Try Sans Normal
   */
  if (pango_fontset_simple_size (fonts) == 0)
    {
      char *ctmp1, *ctmp2;

      pango_font_description_set_family_static (tmp_desc, "Sans");
      ctmp1 = pango_font_description_to_string (tmp_desc);
      pango_font_description_set_style (tmp_desc, PANGO_STYLE_NORMAL);
      pango_font_description_set_weight (tmp_desc, PANGO_WEIGHT_NORMAL);
      pango_font_description_set_variant (tmp_desc, PANGO_VARIANT_NORMAL);
      pango_font_description_set_stretch (tmp_desc, PANGO_STRETCH_NORMAL);

      G_LOCK (warned_fonts);
      if (!warned_fonts || !g_hash_table_lookup (warned_fonts, ctmp1))
        {
          g_hash_table_insert (warned_fonts, g_strdup (ctmp1), GINT_TO_POINTER (1));

          ctmp2 = pango_font_description_to_string (tmp_desc);

          g_warning ("couldn't load font \"%s\", falling back to \"%s\", "
                     "expect ugly output.", ctmp1, ctmp2);
          g_free (ctmp2);
        }
      G_UNLOCK (warned_fonts);
      g_free (ctmp1);

      pango_font_map_fontset_add_fonts (fontmap,
                                        context,
                                        fonts,
                                        tmp_desc,
                                        "Sans");
    }

  pango_font_description_free (tmp_desc);

  /* Everything failed, we are screwed, there is no way to continue,
   * but lets just not crash here.
   */
  if (pango_fontset_simple_size (fonts) == 0)
      g_warning ("All font fallbacks failed!!!!");

  return PANGO_FONTSET (fonts);
}

/**
 * pango_font_map_get_shape_engine_type:
 * @fontmap: a `PangoFontMap`
 *
 * Returns the render ID for shape engines for this fontmap.
 * See the `render_type` field of `PangoEngineInfo`.
  *
 * Return value (transfer none): the ID string for shape engines
 *   for this fontmap
 *
 * Since: 1.4
 * Deprecated: 1.38
 */
const char *
pango_font_map_get_shape_engine_type (PangoFontMap *fontmap)
{
  g_return_val_if_fail (PANGO_IS_FONT_MAP (fontmap), NULL);

  return PANGO_FONT_MAP_GET_CLASS (fontmap)->shape_engine_type;
}

/**
 * pango_font_map_get_serial:
 * @fontmap: a `PangoFontMap`
 *
 * Returns the current serial number of @fontmap.
 *
 * The serial number is initialized to an small number larger than zero
 * when a new fontmap is created and is increased whenever the fontmap
 * is changed. It may wrap, but will never have the value 0. Since it can
 * wrap, never compare it with "less than", always use "not equals".
 *
 * The fontmap can only be changed using backend-specific API, like changing
 * fontmap resolution.
 *
 * This can be used to automatically detect changes to a `PangoFontMap`,
 * like in `PangoContext`.
 *
 * Return value: The current serial number of @fontmap.
 *
 * Since: 1.32.4
 */
guint
pango_font_map_get_serial (PangoFontMap *fontmap)
{
  g_return_val_if_fail (PANGO_IS_FONT_MAP (fontmap), 0);

  if (PANGO_FONT_MAP_GET_CLASS (fontmap)->get_serial)
    return PANGO_FONT_MAP_GET_CLASS (fontmap)->get_serial (fontmap);
  else
    return 1;
}

static void
pango_font_map_real_changed (PangoFontMap *fontmap)
{
  PangoFontMapPrivate *priv = pango_font_map_get_instance_private (fontmap);
  guint removed, added;

  removed = priv->n_families;
  added = g_list_model_get_n_items (G_LIST_MODEL (fontmap));

  g_list_model_items_changed (G_LIST_MODEL (fontmap), 0, removed, added);
  if (removed != added)
    g_object_notify_by_pspec (G_OBJECT (fontmap), properties[PROP_N_ITEMS]);
}

/**
 * pango_font_map_changed:
 * @fontmap: a `PangoFontMap`
 *
 * Forces a change in the context, which will cause any `PangoContext`
 * using this fontmap to change.
 *
 * This function is only useful when implementing a new backend
 * for Pango, something applications won't do. Backends should
 * call this function if they have attached extra data to the
 * context and such data is changed.
 *
 * Since: 1.34
 */
void
pango_font_map_changed (PangoFontMap *fontmap)
{
  g_return_if_fail (PANGO_IS_FONT_MAP (fontmap));

  if (PANGO_FONT_MAP_GET_CLASS (fontmap)->changed)
    PANGO_FONT_MAP_GET_CLASS (fontmap)->changed (fontmap);
}

static PangoFontFamily *
pango_font_map_real_get_family (PangoFontMap *fontmap,
                                const char   *name)
{
  PangoFontFamily **families;
  int n_families;
  PangoFontFamily *family;
  int i;

  pango_font_map_list_families (fontmap, &families, &n_families);

  family = NULL;

  for (i = 0; i < n_families; i++)
    {
      if (strcmp (name, pango_font_family_get_name (families[i])) == 0)
        {
          family = families[i];
          break;
        }
    }

  g_free (families);

  return family;
}

/**
 * pango_font_map_get_family:
 * @fontmap: a `PangoFontMap`
 * @name: a family name
 *
 * Gets a font family by name.
 *
 * Returns: (transfer none): the `PangoFontFamily`
 *
 * Since: 1.46
 */
PangoFontFamily *
pango_font_map_get_family (PangoFontMap *fontmap,
                           const char   *name)
{
  g_return_val_if_fail (PANGO_IS_FONT_MAP (fontmap), NULL);

  return PANGO_FONT_MAP_GET_CLASS (fontmap)->get_family (fontmap, name);
}

static PangoFont *
pango_font_map_real_reload_font (PangoFontMap *fontmap,
                                 PangoFont    *font,
                                 double        scale,
                                 PangoContext *context,
                                 const char   *variations)
{
  PangoFontDescription *desc;
  PangoContext *freeme = NULL;
  PangoFont *scaled;

  desc = pango_font_describe_with_absolute_size (font);

  if (scale != 1.0)
    {
      int size = pango_font_description_get_size (desc);

      pango_font_description_set_absolute_size (desc, size * scale);
    }

  if (!context)
    freeme = context = pango_font_map_create_context (fontmap);

  if (variations)
    pango_font_description_set_variations_static (desc, variations);

  scaled = pango_font_map_load_font (fontmap, context, desc);

  g_clear_object (&freeme);

  pango_font_description_free (desc);

  return scaled;
}

/**
 * pango_font_map_reload_font:
 * @fontmap: a `PangoFontMap`
 * @font: a font in @fontmap
 * @scale: the scale factor to apply
 * @context: (nullable): a `PangoContext`
 * @variations: (nullable): font variations to use
 *
 * Returns a new font that is like @font, except that its size
 * is multiplied by @scale, its backend-dependent configuration
 * (e.g. cairo font options) is replaced by the one in @context,
 * and its variations are replaced by @variations.
 *
 * Returns: (transfer full): the modified font
 *
 * Since: 1.52
 */
PangoFont *
pango_font_map_reload_font (PangoFontMap *fontmap,
                            PangoFont    *font,
                            double        scale,
                            PangoContext *context,
                            const char   *variations)
{
  PangoFontMapClassPrivate *pclass;

  g_return_val_if_fail (PANGO_IS_FONT (font), NULL);
  g_return_val_if_fail (fontmap == pango_font_get_font_map (font), NULL);
  g_return_val_if_fail (scale > 0, NULL);
  g_return_val_if_fail (context == NULL || PANGO_IS_CONTEXT (context), NULL);

  pclass = g_type_class_get_private ((GTypeClass *) PANGO_FONT_MAP_GET_CLASS (fontmap),
                                     PANGO_TYPE_FONT_MAP);

  return pclass->reload_font (fontmap, font, scale, context, variations);
}

static gboolean
pango_font_map_real_add_font_file (PangoFontMap  *fontmap,
                                   const char    *filename,
                                   GError       **error)
{
  g_set_error (error, G_IO_ERROR, G_IO_ERROR_NOT_SUPPORTED,
               "Adding font files not supported for %s",
               G_OBJECT_TYPE_NAME (fontmap));

  return FALSE;
}

/**
 * pango_font_map_add_font_file:
 * @fontmap: a `PangoFontMap`
 * @filename: (type filename): Path to the font file
 * @error: return location for an error
 *
 * Loads a font file with one or more fonts into the `PangoFontMap`.
 *
 * The added fonts will take precedence over preexisting
 * fonts with the same name.
 *
 * Return value: True if the font file is successfully loaded
 *     into the fontmap, false if an error occurred.
 *
 * Since: 1.56
 */
gboolean
pango_font_map_add_font_file (PangoFontMap  *fontmap,
                              const char    *filename,
                              GError       **error)
{
  PangoFontMapClassPrivate *pclass;

  g_return_val_if_fail (PANGO_IS_FONT_MAP (fontmap), FALSE);
  g_return_val_if_fail (filename != NULL, FALSE);

  pclass = g_type_class_get_private ((GTypeClass *) PANGO_FONT_MAP_GET_CLASS (fontmap),
                                     PANGO_TYPE_FONT_MAP);

  return pclass->add_font_file (fontmap, filename, error);
}

static GType
pango_font_map_get_item_type (GListModel *list)
{
  return PANGO_TYPE_FONT_FAMILY;
}

static guint
pango_font_map_get_n_items (GListModel *list)
{
  PangoFontMap *fontmap = PANGO_FONT_MAP (list);
  int n_families;

  pango_font_map_list_families (fontmap, NULL, &n_families);

  return (guint)n_families;
}

static gpointer
pango_font_map_get_item (GListModel *list,
                         guint       position)
{
  PangoFontMap *fontmap = PANGO_FONT_MAP (list);
  PangoFontFamily **families;
  int n_families;
  PangoFontFamily *family;

  pango_font_map_list_families (fontmap, &families, &n_families);

  if (position < n_families)
    family = g_object_ref (families[position]);
  else
    family = NULL;

  g_free (families);
  
  return family;
}

static void
pango_font_map_list_model_init (GListModelInterface *iface)
{
  iface->get_item_type = pango_font_map_get_item_type;
  iface->get_n_items = pango_font_map_get_n_items;
  iface->get_item = pango_font_map_get_item;
}
