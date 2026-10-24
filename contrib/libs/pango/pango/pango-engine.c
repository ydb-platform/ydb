/* Pango
 * pango-engine.c: Engines for script and language specific processing
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

#include "pango-engine.h"
#include "pango-impl-utils.h"


G_DEFINE_ABSTRACT_TYPE (PangoEngine, pango_engine, G_TYPE_OBJECT);

static void
pango_engine_init (PangoEngine *self)
{
}

static void
pango_engine_class_init (PangoEngineClass *klass)
{
}

G_GNUC_BEGIN_IGNORE_DEPRECATIONS
G_DEFINE_ABSTRACT_TYPE (PangoEngineLang, pango_engine_lang, PANGO_TYPE_ENGINE);
G_GNUC_END_IGNORE_DEPRECATIONS

static void
pango_engine_lang_init (PangoEngineLang *self)
{
}

static void
pango_engine_lang_class_init (PangoEngineLangClass *klass)
{
}


static PangoCoverageLevel
pango_engine_shape_real_covers (PangoEngineShape *engine G_GNUC_UNUSED,
				PangoFont        *font,
				PangoLanguage    *language,
				gunichar          wc)
{
  PangoCoverage *coverage = pango_font_get_coverage (font, language);
  PangoCoverageLevel result = pango_coverage_get (coverage, wc);

  g_object_unref (coverage);

  return result;
}

G_GNUC_BEGIN_IGNORE_DEPRECATIONS
G_DEFINE_ABSTRACT_TYPE (PangoEngineShape, pango_engine_shape, PANGO_TYPE_ENGINE);
G_GNUC_END_IGNORE_DEPRECATIONS

static void
pango_engine_shape_init (PangoEngineShape *klass)
{
}

static void
pango_engine_shape_class_init (PangoEngineShapeClass *class)
{
  class->covers = pango_engine_shape_real_covers;
}
