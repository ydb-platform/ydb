/* Pango
 * modules.c:
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.	 See the GNU
 * Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this library; if not, write to the
 * Free Software Foundation, Inc., 59 Temple Place - Suite 330,
 * Boston, MA 02111-1307, USA.
 */

#include "config.h"

#include "pango-modules.h"

/**
 * pango_find_map: (skip)
 * @language: the language tag for which to find the map
 * @engine_type_id: the engine type for the map to find
 * @render_type_id: the render type for the map to find
 *
 * Do not use.  Does not do anything.
 *
 * Return value: (transfer none) (nullable): %NULL.
 *
 * Deprecated: 1.38
 **/
PangoMap *
pango_find_map (PangoLanguage *language G_GNUC_UNUSED,
		guint          engine_type_id G_GNUC_UNUSED,
		guint          render_type_id G_GNUC_UNUSED)
{
  return NULL;
}

/**
 * pango_map_get_engine: (skip)
 * @map: a `PangoMap`
 * @script: a `PangoScript`
 *
 * Do not use.  Does not do anything.
 *
 * Return value: (transfer none) (nullable): %NULL.
 *
 * Deprecated: 1.38
 **/
PangoEngine *
pango_map_get_engine (PangoMap   *map G_GNUC_UNUSED,
		      PangoScript script G_GNUC_UNUSED)
{
  return NULL;
}

/**
 * pango_map_get_engines: (skip)
 * @map: a `PangoMap`
 * @script: a `PangoScript`
 * @exact_engines: (nullable): location to store list of engines that exactly
 *  handle this script.
 * @fallback_engines: (nullable): location to store list of engines that
 *  approximately handle this script.
 *
 * Do not use.  Does not do anything.
 *
 * Since: 1.4
 * Deprecated: 1.38
 **/
void
pango_map_get_engines (PangoMap     *map G_GNUC_UNUSED,
		       PangoScript   script G_GNUC_UNUSED,
		       GSList      **exact_engines,
		       GSList      **fallback_engines)
{
  if (exact_engines)
    *exact_engines = NULL;
  if (fallback_engines)
    *fallback_engines = NULL;
}

/**
 * pango_module_register: (skip)
 * @module: a `PangoIncludedModule`
 *
 * Do not use.  Does not do anything.
 *
 * Deprecated: 1.38
 **/
void
pango_module_register (PangoIncludedModule *module G_GNUC_UNUSED)
{
}
