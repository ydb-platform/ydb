/* Pango
 * pangocairo-fontmap.c: Cairo font handling, fontconfig backend
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

/* FreeType has undefined macros in its headers */
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wundef"
#include <cairo-ft.h>
#pragma GCC diagnostic pop

#include "pangofc-fontmap-private.h"
#include "pangocairo.h"
#include "pangocairo-private.h"
#include "pangocairo-fc-private.h"

typedef struct _PangoCairoFcFontMapClass PangoCairoFcFontMapClass;

struct _PangoCairoFcFontMapClass
{
  PangoFcFontMapClass parent_class;
};

static guint
pango_cairo_fc_font_map_get_serial (PangoFontMap *fontmap)
{
  PangoCairoFcFontMap *cffontmap = (PangoCairoFcFontMap *) (fontmap);

  return cffontmap->serial;
}

static void
pango_cairo_fc_font_map_changed (PangoFontMap *fontmap)
{
  PangoCairoFcFontMap *cffontmap = (PangoCairoFcFontMap *) (fontmap);

  cffontmap->serial++;
  if (cffontmap->serial == 0)
    cffontmap->serial++;
}

static void
pango_cairo_fc_font_map_set_resolution (PangoCairoFontMap *cfontmap,
					double             dpi)
{
  PangoCairoFcFontMap *cffontmap = (PangoCairoFcFontMap *) (cfontmap);

  if (dpi != cffontmap->dpi)
    {
      cffontmap->serial++;
      if (cffontmap->serial == 0)
	cffontmap->serial++;
      cffontmap->dpi = dpi;

      pango_fc_font_map_cache_clear ((PangoFcFontMap *) (cfontmap));
    }
}

static double
pango_cairo_fc_font_map_get_resolution_cairo (PangoCairoFontMap *cfontmap)
{
  PangoCairoFcFontMap *cffontmap = (PangoCairoFcFontMap *) (cfontmap);

  return cffontmap->dpi;
}

static cairo_font_type_t
pango_cairo_fc_font_map_get_font_type (PangoCairoFontMap *cfontmap G_GNUC_UNUSED)
{
  return CAIRO_FONT_TYPE_FT;
}

static void
cairo_font_map_iface_init (PangoCairoFontMapIface *iface)
{
  iface->set_resolution = pango_cairo_fc_font_map_set_resolution;
  iface->get_resolution = pango_cairo_fc_font_map_get_resolution_cairo;
  iface->get_font_type  = pango_cairo_fc_font_map_get_font_type;
}

G_DEFINE_TYPE_WITH_CODE (PangoCairoFcFontMap, pango_cairo_fc_font_map, PANGO_TYPE_FC_FONT_MAP,
    { G_IMPLEMENT_INTERFACE (PANGO_TYPE_CAIRO_FONT_MAP, cairo_font_map_iface_init) })

static void
pango_cairo_fc_font_map_fontset_key_substitute (PangoFcFontMap    *fcfontmap G_GNUC_UNUSED,
						PangoFcFontsetKey *fontkey,
						FcPattern         *pattern)
{
  FcConfigSubstitute (pango_fc_font_map_get_config (fcfontmap), pattern, FcMatchPattern);

  if (fcfontmap->substitute_func)
    fcfontmap->substitute_func (pattern, fcfontmap->substitute_data);
  if (fontkey)
    cairo_ft_font_options_substitute (pango_fc_fontset_key_get_context_key (fontkey),
				      pattern);

  FcDefaultSubstitute (pattern);
}

static double
pango_cairo_fc_font_map_get_resolution_fc (PangoFcFontMap *fcfontmap,
					   PangoContext   *context)
{
  PangoCairoFcFontMap *cffontmap = (PangoCairoFcFontMap *) (fcfontmap);
  double dpi;

  if (context)
    {
      dpi = pango_cairo_context_get_resolution (context);

      if (dpi <= 0)
	dpi = cffontmap->dpi;
    }
  else
    dpi = cffontmap->dpi;

  return dpi;
}

static gconstpointer
pango_cairo_fc_font_map_context_key_get (PangoFcFontMap *fcfontmap G_GNUC_UNUSED,
					 PangoContext   *context)
{
  return _pango_cairo_context_get_merged_font_options (context);
}

static gpointer
pango_cairo_fc_font_map_context_key_copy (PangoFcFontMap *fcfontmap G_GNUC_UNUSED,
					  gconstpointer   key)
{
  return cairo_font_options_copy (key);
}

static void
pango_cairo_fc_font_map_context_key_free (PangoFcFontMap *fcfontmap G_GNUC_UNUSED,
					  gpointer        key)
{
  cairo_font_options_destroy (key);
}


static guint32
pango_cairo_fc_font_map_context_key_hash (PangoFcFontMap *fcfontmap G_GNUC_UNUSED,
					  gconstpointer        key)
{
  return (guint32)cairo_font_options_hash (key);
}

static gboolean
pango_cairo_fc_font_map_context_key_equal (PangoFcFontMap *fcfontmap G_GNUC_UNUSED,
					   gconstpointer   key_a,
					   gconstpointer   key_b)
{
  return cairo_font_options_equal (key_a, key_b);
}

static PangoFcFont *
pango_cairo_fc_font_map_create_font (PangoFcFontMap *fcfontmap,
				     PangoFcFontKey *key)
{
  return _pango_cairo_fc_font_new ((PangoCairoFcFontMap *) (fcfontmap),
				   key);
}

static void
pango_cairo_fc_font_map_class_init (PangoCairoFcFontMapClass *class)
{
  PangoFontMapClass *fontmap_class = PANGO_FONT_MAP_CLASS (class);
  PangoFcFontMapClass *fcfontmap_class = PANGO_FC_FONT_MAP_CLASS (class);

  fontmap_class->get_serial = pango_cairo_fc_font_map_get_serial;
  fontmap_class->changed = pango_cairo_fc_font_map_changed;

  fcfontmap_class->fontset_key_substitute = pango_cairo_fc_font_map_fontset_key_substitute;
  fcfontmap_class->get_resolution = pango_cairo_fc_font_map_get_resolution_fc;

  fcfontmap_class->context_key_get = pango_cairo_fc_font_map_context_key_get;
  fcfontmap_class->context_key_copy = pango_cairo_fc_font_map_context_key_copy;
  fcfontmap_class->context_key_free = pango_cairo_fc_font_map_context_key_free;
  fcfontmap_class->context_key_hash = pango_cairo_fc_font_map_context_key_hash;
  fcfontmap_class->context_key_equal = pango_cairo_fc_font_map_context_key_equal;

  fcfontmap_class->create_font = pango_cairo_fc_font_map_create_font;
}

static void
pango_cairo_fc_font_map_init (PangoCairoFcFontMap *cffontmap)
{
  cffontmap->serial = 1;
  cffontmap->dpi   = 96.0;
}
