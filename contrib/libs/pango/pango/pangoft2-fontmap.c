/* Pango
 * pangoft2-fontmap.c:
 *
 * Copyright (C) 2000 Red Hat Software
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

#include <glib.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include <fontconfig/fontconfig.h>

#include "pango-impl-utils.h"
#include "pangoft2-private.h"
#include "pangofc-fontmap.h"

typedef struct _PangoFT2Family       PangoFT2Family;
typedef struct _PangoFT2FontMapClass PangoFT2FontMapClass;

/**
 * PangoFT2FontMap:
 *
 * The `PangoFT2FontMap` is the `PangoFontMap` implementation for FreeType fonts.
 */
struct _PangoFT2FontMap
{
  PangoFcFontMap parent_instance;

  FT_Library library;

  guint serial;
  double dpi_x;
  double dpi_y;

  PangoRenderer *renderer;
};

struct _PangoFT2FontMapClass
{
  PangoFcFontMapClass parent_class;
};

static void          pango_ft2_font_map_finalize            (GObject              *object);
static PangoFcFont * pango_ft2_font_map_new_font            (PangoFcFontMap       *fcfontmap,
							     FcPattern            *pattern);
static double        pango_ft2_font_map_get_resolution      (PangoFcFontMap       *fcfontmap,
							     PangoContext         *context);
static guint         pango_ft2_font_map_get_serial          (PangoFontMap         *fontmap);
static void          pango_ft2_font_map_changed             (PangoFontMap         *fontmap);

static PangoFT2FontMap *pango_ft2_global_fontmap = NULL; /* MT-safe */

G_DEFINE_TYPE (PangoFT2FontMap, pango_ft2_font_map, PANGO_TYPE_FC_FONT_MAP)

static void
pango_ft2_font_map_class_init (PangoFT2FontMapClass *class)
{
  GObjectClass *gobject_class = G_OBJECT_CLASS (class);
  PangoFontMapClass *fontmap_class = PANGO_FONT_MAP_CLASS (class);
  PangoFcFontMapClass *fcfontmap_class = PANGO_FC_FONT_MAP_CLASS (class);

  gobject_class->finalize = pango_ft2_font_map_finalize;
  fontmap_class->get_serial = pango_ft2_font_map_get_serial;
  fontmap_class->changed = pango_ft2_font_map_changed;
  fcfontmap_class->default_substitute = _pango_ft2_font_map_default_substitute;
  fcfontmap_class->new_font = pango_ft2_font_map_new_font;
  fcfontmap_class->get_resolution = pango_ft2_font_map_get_resolution;
}

static void
pango_ft2_font_map_init (PangoFT2FontMap *fontmap)
{
  FT_Error error;

  fontmap->serial = 1;
  fontmap->library = NULL;
  fontmap->dpi_x   = 72.0;
  fontmap->dpi_y   = 72.0;

  error = FT_Init_FreeType (&fontmap->library);
  if (error != FT_Err_Ok)
    g_critical ("pango_ft2_font_map_init: Could not initialize freetype");
}

static void
pango_ft2_font_map_finalize (GObject *object)
{
  PangoFT2FontMap *ft2fontmap = PANGO_FT2_FONT_MAP (object);

  if (ft2fontmap->renderer)
    g_object_unref (ft2fontmap->renderer);

  G_OBJECT_CLASS (pango_ft2_font_map_parent_class)->finalize (object);

  FT_Done_FreeType (ft2fontmap->library);
}

/**
 * pango_ft2_font_map_new:
 *
 * Create a new `PangoFT2FontMap` object.
 *
 * A fontmap is used to cache information about available fonts,
 * and holds certain global parameters such as the resolution and
 * the default substitute function (see
 * [method@PangoFT2.FontMap.set_default_substitute]).
 *
 * Return value: the newly created fontmap object. Unref
 * with g_object_unref() when you are finished with it.
 *
 * Since: 1.2
 **/
PangoFontMap *
pango_ft2_font_map_new (void)
{
  return (PangoFontMap *) g_object_new (PANGO_TYPE_FT2_FONT_MAP, NULL);
}

static guint
pango_ft2_font_map_get_serial (PangoFontMap *fontmap)
{
  PangoFT2FontMap *ft2fontmap = PANGO_FT2_FONT_MAP (fontmap);

  return ft2fontmap->serial;
}

static void
pango_ft2_font_map_changed (PangoFontMap *fontmap)
{
  PangoFT2FontMap *ft2fontmap = PANGO_FT2_FONT_MAP (fontmap);

  ft2fontmap->serial++;
  if (ft2fontmap->serial == 0)
    ft2fontmap->serial++;
}

/**
 * pango_ft2_font_map_set_default_substitute:
 * @fontmap: a `PangoFT2FontMap`
 * @func: function to call to to do final config tweaking
 *        on #FcPattern objects.
 * @data: data to pass to @func
 * @notify: function to call when @data is no longer used.
 *
 * Sets a function that will be called to do final configuration
 * substitution on a `FcPattern` before it is used to load
 * the font.
 *
 * This function can be used to do things like set
 * hinting and antialiasing options.
 *
 * Deprecated: 1.46: Use [method@PangoFc.FontMap.set_default_substitute]
 * instead.
 *
 * Since: 1.2
 **/
void
pango_ft2_font_map_set_default_substitute (PangoFT2FontMap        *fontmap,
					   PangoFT2SubstituteFunc  func,
					   gpointer                data,
					   GDestroyNotify          notify)
{
  PangoFcFontMap *fcfontmap = PANGO_FC_FONT_MAP (fontmap);
  pango_fc_font_map_set_default_substitute(fcfontmap, func, data, notify);
}

/**
 * pango_ft2_font_map_substitute_changed:
 * @fontmap: a `PangoFT2FontMap`
 *
 * Call this function any time the results of the
 * default substitution function set with
 * pango_ft2_font_map_set_default_substitute() change.
 *
 * That is, if your substitution function will return different
 * results for the same input pattern, you must call this function.
 *
 * Deprecated: 1.46: Use [method@PangoFc.FontMap.substitute_changed]
 * instead.
 *
 * Since: 1.2
 **/
void
pango_ft2_font_map_substitute_changed (PangoFT2FontMap *fontmap)
{
  pango_fc_font_map_substitute_changed(PANGO_FC_FONT_MAP (fontmap));
}

/**
 * pango_ft2_font_map_set_resolution:
 * @fontmap: a `PangoFT2FontMap`
 * @dpi_x: dots per inch in the X direction
 * @dpi_y: dots per inch in the Y direction
 *
 * Sets the horizontal and vertical resolutions for the fontmap.
 *
 * Since: 1.2
 **/
void
pango_ft2_font_map_set_resolution (PangoFT2FontMap *fontmap,
				   double           dpi_x,
				   double           dpi_y)
{
  g_return_if_fail (PANGO_FT2_IS_FONT_MAP (fontmap));

  fontmap->dpi_x = dpi_x;
  fontmap->dpi_y = dpi_y;

  pango_ft2_font_map_substitute_changed (fontmap);
}

/**
 * pango_ft2_font_map_create_context: (skip)
 * @fontmap: a `PangoFT2FontMap`
 *
 * Create a `PangoContext` for the given fontmap.
 *
 * Return value: (transfer full): the newly created context; free with
 *     g_object_unref().
 *
 * Since: 1.2
 *
 * Deprecated: 1.22: Use [method@Pango.FontMap.create_context] instead.
 **/
PangoContext *
pango_ft2_font_map_create_context (PangoFT2FontMap *fontmap)
{
  g_return_val_if_fail (PANGO_FT2_IS_FONT_MAP (fontmap), NULL);

  return pango_font_map_create_context (PANGO_FONT_MAP (fontmap));
}

/**
 * pango_ft2_get_context: (skip)
 * @dpi_x:  the horizontal DPI of the target device
 * @dpi_y:  the vertical DPI of the target device
 *
 * Retrieves a `PangoContext` for the default PangoFT2 fontmap
 * (see pango_ft2_font_map_for_display()) and sets the resolution
 * for the default fontmap to @dpi_x by @dpi_y.
 *
 * Return value: (transfer full): the new `PangoContext`
 *
 * Deprecated: 1.22: Use [method@Pango.FontMap.create_context] instead.
 **/
G_GNUC_BEGIN_IGNORE_DEPRECATIONS
PangoContext *
pango_ft2_get_context (double dpi_x, double dpi_y)
{
  PangoFontMap *fontmap;

  fontmap = pango_ft2_font_map_for_display ();
  pango_ft2_font_map_set_resolution (PANGO_FT2_FONT_MAP (fontmap), dpi_x, dpi_y);

  return pango_font_map_create_context (fontmap);
}
G_GNUC_END_IGNORE_DEPRECATIONS

/**
 * pango_ft2_font_map_for_display: (skip)
 *
 * Returns a `PangoFT2FontMap`.
 *
 * This font map is cached and should
 * not be freed. If the font map is no longer needed, it can
 * be released with pango_ft2_shutdown_display(). Use of the
 * global PangoFT2 fontmap is deprecated; use pango_ft2_font_map_new()
 * instead.
 *
 * Return value: (transfer none): a `PangoFT2FontMap`.
 **/
PangoFontMap *
pango_ft2_font_map_for_display (void)
{
  if (g_once_init_enter (&pango_ft2_global_fontmap))
    g_once_init_leave (&pango_ft2_global_fontmap, PANGO_FT2_FONT_MAP (pango_ft2_font_map_new ()));

  return PANGO_FONT_MAP (pango_ft2_global_fontmap);
}

/**
 * pango_ft2_shutdown_display:
 *
 * Free the global fontmap. (See pango_ft2_font_map_for_display())
 * Use of the global PangoFT2 fontmap is deprecated.
 **/
void
pango_ft2_shutdown_display (void)
{
  if (pango_ft2_global_fontmap)
    {
      pango_fc_font_map_cache_clear (PANGO_FC_FONT_MAP (pango_ft2_global_fontmap));

      g_object_unref (pango_ft2_global_fontmap);

      pango_ft2_global_fontmap = NULL;
    }
}

FT_Library
_pango_ft2_font_map_get_library (PangoFontMap *fontmap)
{
  PangoFT2FontMap *ft2fontmap = (PangoFT2FontMap *)fontmap;

  return ft2fontmap->library;
}


/**
 * _pango_ft2_font_map_get_renderer:
 * @fontmap: a `PangoFT2FontMap`
 *
 * Gets the singleton PangoFT2Renderer for this fontmap.
 *
 * Return value: the renderer.
 **/
PangoRenderer *
_pango_ft2_font_map_get_renderer (PangoFT2FontMap *ft2fontmap)
{
  if (!ft2fontmap->renderer)
    ft2fontmap->renderer = g_object_new (PANGO_TYPE_FT2_RENDERER, NULL);

  return ft2fontmap->renderer;
}

void
_pango_ft2_font_map_default_substitute (PangoFcFontMap *fcfontmap,
				       FcPattern      *pattern)
{
  PangoFT2FontMap *ft2fontmap = PANGO_FT2_FONT_MAP (fcfontmap);
  FcValue v;

  FcConfigSubstitute (pango_fc_font_map_get_config (fcfontmap),
                      pattern, FcMatchPattern);

  if (fcfontmap->substitute_func)
    fcfontmap->substitute_func (pattern, fcfontmap->substitute_data);

  if (FcPatternGet (pattern, FC_DPI, 0, &v) == FcResultNoMatch)
    FcPatternAddDouble (pattern, FC_DPI, ft2fontmap->dpi_y);
  FcDefaultSubstitute (pattern);
}

static double
pango_ft2_font_map_get_resolution (PangoFcFontMap       *fcfontmap,
				   PangoContext         *context G_GNUC_UNUSED)
{
  return ((PangoFT2FontMap *)fcfontmap)->dpi_y;
}

static PangoFcFont *
pango_ft2_font_map_new_font (PangoFcFontMap  *fcfontmap,
			     FcPattern       *pattern)
{
  return (PangoFcFont *)_pango_ft2_font_new (PANGO_FT2_FONT_MAP (fcfontmap), pattern);
}
