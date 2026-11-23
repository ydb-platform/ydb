/* Pango
 * pango-fontset-simple.c:
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
#include "pango-fontset-simple-private.h"
#include "pango-impl-utils.h"

/* {{{ PangoFontset implementation */

#define PANGO_FONTSET_SIMPLE_CLASS(klass)      (G_TYPE_CHECK_CLASS_CAST ((klass), PANGO_TYPE_FONTSET_SIMPLE, PangoFontsetSimpleClass))
#define PANGO_IS_FONTSET_SIMPLE_CLASS(klass)   (G_TYPE_CHECK_CLASS_TYPE ((klass), PANGO_TYPE_FONTSET_SIMPLE))
#define PANGO_FONTSET_SIMPLE_GET_CLASS(obj)    (G_TYPE_INSTANCE_GET_CLASS ((obj), PANGO_TYPE_FONTSET_SIMPLE, PangoFontsetSimpleClass))


G_DEFINE_TYPE (PangoFontsetSimple, pango_fontset_simple, PANGO_TYPE_FONTSET);

static void
pango_fontset_simple_init (PangoFontsetSimple *fontset)
{
  fontset->fonts = g_ptr_array_new_with_free_func (g_object_unref);
  fontset->language = NULL;
}

static void
pango_fontset_simple_finalize (GObject *object)
{
  PangoFontsetSimple *fontset = PANGO_FONTSET_SIMPLE (object);

  g_ptr_array_free (fontset->fonts, TRUE);

  G_OBJECT_CLASS (pango_fontset_simple_parent_class)->finalize (object);
}

static PangoFont *
pango_fontset_simple_get_font (PangoFontset *fontset,
                               guint         wc)
{
  PangoFontsetSimple *simple = PANGO_FONTSET_SIMPLE (fontset);
  unsigned int i;

  for (i = 0; i < simple->fonts->len; i++)
    {
      PangoFont *font = g_ptr_array_index (simple->fonts, i);

      if (pango_font_has_char (font, wc))
        return g_object_ref (font);
    }

  return NULL;
}

static PangoFontMetrics *
pango_fontset_simple_get_metrics (PangoFontset *fontset)
{
  PangoFontsetSimple *simple = PANGO_FONTSET_SIMPLE (fontset);

  if (simple->fonts->len == 1)
    {
      PangoFont *font = g_ptr_array_index (simple->fonts, 0);

      return pango_font_get_metrics (font, simple->language);
    }

  return PANGO_FONTSET_CLASS (pango_fontset_simple_parent_class)->get_metrics (fontset);
}

static PangoLanguage *
pango_fontset_simple_get_language (PangoFontset *fontset)
{
  PangoFontsetSimple *simple = PANGO_FONTSET_SIMPLE (fontset);

  return simple->language;
}

static void
pango_fontset_simple_foreach (PangoFontset            *fontset,
                              PangoFontsetForeachFunc  func,
                              gpointer                 data)
{
  PangoFontsetSimple *simple = PANGO_FONTSET_SIMPLE (fontset);
  unsigned int i;

  for (i = 0; i < simple->fonts->len; i++)
    {
      PangoFont *font = g_ptr_array_index (simple->fonts, i);

      if ((*func) (fontset, font, data))
        return;
    }
}

static void
pango_fontset_simple_class_init (PangoFontsetSimpleClass *class)
{
  GObjectClass *object_class = G_OBJECT_CLASS (class);
  PangoFontsetClass *fontset_class = PANGO_FONTSET_CLASS (class);

  object_class->finalize = pango_fontset_simple_finalize;

  fontset_class->get_font = pango_fontset_simple_get_font;
  fontset_class->get_metrics = pango_fontset_simple_get_metrics;
  fontset_class->get_language = pango_fontset_simple_get_language;
  fontset_class->foreach = pango_fontset_simple_foreach;
}

/* }}} */
/* {{{ Public API */

/**
 * pango_fontset_simple_new:
 * @language: a `PangoLanguage` tag
 *
 * Creates a new `PangoFontsetSimple` for the given language.
 *
 * Return value: the newly allocated `PangoFontsetSimple`
 */
PangoFontsetSimple *
pango_fontset_simple_new (PangoLanguage *language)
{
  PangoFontsetSimple *fontset;

  fontset = g_object_new (PANGO_TYPE_FONTSET_SIMPLE, NULL);
  fontset->language = language;

  return fontset;
}

/**
 * pango_fontset_simple_append:
 * @fontset: a `PangoFontsetSimple`.
 * @font: (transfer full): a `PangoFont`.
 *
 * Adds a font to the fontset.
 *
 * The fontset takes ownership of @font.
 */
void
pango_fontset_simple_append (PangoFontsetSimple *fontset,
                             PangoFont          *font)
{
  g_ptr_array_add (fontset->fonts, font);
}

/**
 * pango_fontset_simple_size:
 * @fontset: a `PangoFontsetSimple`.
 *
 * Returns the number of fonts in the fontset.
 *
 * Return value: the size of @fontset
 */
int
pango_fontset_simple_size (PangoFontsetSimple *fontset)
{
  return fontset->fonts->len;
}

/* }}} */

/* vim:set foldmethod=marker expandtab: */
