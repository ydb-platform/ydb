/* Pango
 * pangocairo-fontmap.c: Cairo font handling
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

#include "pangocairo.h"
#include "pangocairo-private.h"
#include "pango-impl-utils.h"

#include <string.h>

typedef struct _PangoCairoContextInfo PangoCairoContextInfo;

struct _PangoCairoContextInfo
{
  double dpi;
  gboolean set_options_explicit;

  cairo_font_options_t *set_options;
  cairo_font_options_t *surface_options;
  cairo_font_options_t *merged_options;

  PangoCairoShapeRendererFunc shape_renderer_func;
  gpointer                    shape_renderer_data;
  GDestroyNotify              shape_renderer_notify;
};

static void
free_context_info (PangoCairoContextInfo *info)
{
  if (info->set_options)
    cairo_font_options_destroy (info->set_options);
  if (info->surface_options)
    cairo_font_options_destroy (info->surface_options);
  if (info->merged_options)
    cairo_font_options_destroy (info->merged_options);

  if (info->shape_renderer_notify)
    info->shape_renderer_notify (info->shape_renderer_data);

  g_slice_free (PangoCairoContextInfo, info);
}

static PangoCairoContextInfo *
get_context_info (PangoContext *context,
		  gboolean      create)
{
  static GQuark context_info_quark; /* MT-safe */
  PangoCairoContextInfo *info;

  if (G_UNLIKELY (!context_info_quark))
    context_info_quark = g_quark_from_static_string ("pango-cairo-context-info");

retry:
  info = g_object_get_qdata (G_OBJECT (context), context_info_quark);

  if (G_UNLIKELY (!info) && create)
    {
      info = g_slice_new0 (PangoCairoContextInfo);
      info->dpi = -1.0;

      if (!g_object_replace_qdata (G_OBJECT (context), context_info_quark, NULL,
                                   info, (GDestroyNotify)free_context_info,
                                   NULL))
        {
          free_context_info (info);
          goto retry;
        }
    }

  return info;
}

static void
_pango_cairo_update_context (cairo_t      *cr,
			     PangoContext *context)
{
  PangoCairoContextInfo *info;
  cairo_matrix_t cairo_matrix;
  cairo_surface_t *target;
  PangoMatrix pango_matrix;
  const PangoMatrix *current_matrix, identity_matrix = PANGO_MATRIX_INIT;
  const cairo_font_options_t *merged_options;
  cairo_font_options_t *old_merged_options;
  gboolean changed = FALSE;

  info = get_context_info (context, TRUE);

  target = cairo_get_target (cr);

  if (!info->surface_options)
    info->surface_options = cairo_font_options_create ();
  cairo_surface_get_font_options (target, info->surface_options);
  if (!info->set_options_explicit)
  {
    if (!info->set_options)
      info->set_options = cairo_font_options_create ();
    cairo_get_font_options (cr, info->set_options);
  }

  old_merged_options = info->merged_options;
  info->merged_options = NULL;

  merged_options = _pango_cairo_context_get_merged_font_options (context);

  if (old_merged_options)
    {
      if (!cairo_font_options_equal (merged_options, old_merged_options))
	changed = TRUE;
      cairo_font_options_destroy (old_merged_options);
      old_merged_options = NULL;
    }
  else
    changed = TRUE;

  cairo_get_matrix (cr, &cairo_matrix);
  pango_matrix.xx = cairo_matrix.xx;
  pango_matrix.yx = cairo_matrix.yx;
  pango_matrix.xy = cairo_matrix.xy;
  pango_matrix.yy = cairo_matrix.yy;
  pango_matrix.x0 = 0;
  pango_matrix.y0 = 0;

  current_matrix = pango_context_get_matrix (context);
  if (!current_matrix)
    current_matrix = &identity_matrix;

  /* layout is matrix-independent if metrics-hinting is off.
   * also ignore matrix translation offsets */
  if ((cairo_font_options_get_hint_metrics (merged_options) != CAIRO_HINT_METRICS_OFF) &&
      (0 != memcmp (&pango_matrix, current_matrix, sizeof (PangoMatrix))))
    changed = TRUE;

  pango_context_set_matrix (context, &pango_matrix);

  if (changed)
    pango_context_changed (context);
}

/**
 * pango_cairo_update_context:
 * @cr: a Cairo context
 * @context: a `PangoContext`, from a pangocairo font map
 *
 * Updates a `PangoContext` previously created for use with Cairo to
 * match the current transformation and target surface of a Cairo
 * context.
 *
 * If any layouts have been created for the context, it's necessary
 * to call [method@Pango.Layout.context_changed] on those layouts.
 *
 * Since: 1.10
 */
void
pango_cairo_update_context (cairo_t      *cr,
			    PangoContext *context)
{
  g_return_if_fail (cr != NULL);
  g_return_if_fail (PANGO_IS_CONTEXT (context));

  _pango_cairo_update_context (cr, context);
}

/**
 * pango_cairo_context_set_resolution:
 * @context: a `PangoContext`, from a pangocairo font map
 * @dpi: the resolution in "dots per inch". (Physical inches aren't actually
 *   involved; the terminology is conventional.) A 0 or negative value
 *   means to use the resolution from the font map.
 *
 * Sets the resolution for the context.
 *
 * This is a scale factor between points specified in a `PangoFontDescription`
 * and Cairo units. The default value is 96, meaning that a 10 point font will
 * be 13 units high. (10 * 96. / 72. = 13.3).
 *
 * Since: 1.10
 */
void
pango_cairo_context_set_resolution (PangoContext *context,
				    double        dpi)
{
  PangoCairoContextInfo *info = get_context_info (context, TRUE);
  info->dpi = dpi;
}

/**
 * pango_cairo_context_get_resolution:
 * @context: a `PangoContext`, from a pangocairo font map
 *
 * Gets the resolution for the context.
 *
 * See [func@PangoCairo.context_set_resolution]
 *
 * Return value: the resolution in "dots per inch". A negative value will
 *   be returned if no resolution has previously been set.
 *
 * Since: 1.10
 */
double
pango_cairo_context_get_resolution (PangoContext *context)
{
  PangoCairoContextInfo *info = get_context_info (context, FALSE);

  if (info)
    return info->dpi;
  else
    return -1.0;
}

/**
 * pango_cairo_context_set_font_options:
 * @context: a `PangoContext`, from a pangocairo font map
 * @options: (nullable): a `cairo_font_options_t`, or %NULL to unset
 *   any previously set options. A copy is made.
 *
 * Sets the font options used when rendering text with this context.
 *
 * These options override any options that [func@update_context]
 * derives from the target surface.
 *
 * Since: 1.10
 */
void
pango_cairo_context_set_font_options (PangoContext               *context,
				      const cairo_font_options_t *options)
{
  PangoCairoContextInfo *info;

  g_return_if_fail (PANGO_IS_CONTEXT (context));

  info = get_context_info (context, TRUE);

  if (!info->set_options && !options)
    return;

  if (info->set_options &&
      options &&
      cairo_font_options_equal (info->set_options, options))
    return;

  if (info->set_options || options)
    pango_context_changed (context);

 if (info->set_options)
    cairo_font_options_destroy (info->set_options);

  if (options)
  {
    info->set_options = cairo_font_options_copy (options);
    info->set_options_explicit = TRUE;
  }
  else
  {
    info->set_options = NULL;
    info->set_options_explicit = FALSE;
  }

  if (info->merged_options)
    {
      cairo_font_options_destroy (info->merged_options);
      info->merged_options = NULL;
    }
}

/**
 * pango_cairo_context_get_font_options:
 * @context: a `PangoContext`, from a pangocairo font map
 *
 * Retrieves any font rendering options previously set with
 * [func@PangoCairo.context_set_font_options].
 *
 * This function does not report options that are derived from
 * the target surface by [func@update_context].
 *
 * Return value: (nullable): the font options previously set on the
 *   context, or %NULL if no options have been set. This value is
 *   owned by the context and must not be modified or freed.
 *
 * Since: 1.10
 */
const cairo_font_options_t *
pango_cairo_context_get_font_options (PangoContext *context)
{
  PangoCairoContextInfo *info;

  g_return_val_if_fail (PANGO_IS_CONTEXT (context), NULL);

  info = get_context_info (context, FALSE);

  if (info)
    return info->set_options;
  else
    return NULL;
}

/**
 * _pango_cairo_context_merge_font_options:
 * @context: a `PangoContext`
 * @options: a `cairo_font_options_t`
 *
 * Merge together options from the target surface and explicitly set
 * on the context.
 *
 * Return value: the combined set of font options. This value is owned
 *   by the context and must not be modified or freed.
 */
const cairo_font_options_t *
_pango_cairo_context_get_merged_font_options (PangoContext *context)
{
  PangoCairoContextInfo *info = get_context_info (context, TRUE);

  if (!info->merged_options)
    {
      info->merged_options = cairo_font_options_create ();

      if (info->surface_options)
	cairo_font_options_merge (info->merged_options, info->surface_options);
      if (info->set_options)
	cairo_font_options_merge (info->merged_options, info->set_options);
    }

  return info->merged_options;
}

/**
 * pango_cairo_context_set_shape_renderer:
 * @context: a `PangoContext`, from a pangocairo font map
 * @func: (nullable): Callback function for rendering attributes of
 *   type %PANGO_ATTR_SHAPE, or %NULL to disable shape rendering.
 * @data: (nullable): User data that will be passed to @func.
 * @dnotify: (nullable): Callback that will be called when the
 *   context is freed to release @data
 *
 * Sets callback function for context to use for rendering attributes
 * of type %PANGO_ATTR_SHAPE.
 *
 * See `PangoCairoShapeRendererFunc` for details.
 *
 * Since: 1.18
 */
void
pango_cairo_context_set_shape_renderer (PangoContext                *context,
					PangoCairoShapeRendererFunc  func,
					gpointer                     data,
					GDestroyNotify               dnotify)
{
  PangoCairoContextInfo *info;

  g_return_if_fail (PANGO_IS_CONTEXT (context));

  info  = get_context_info (context, TRUE);

  if (info->shape_renderer_notify)
    info->shape_renderer_notify (info->shape_renderer_data);

  info->shape_renderer_func   = func;
  info->shape_renderer_data   = data;
  info->shape_renderer_notify = dnotify;
}

/**
 * pango_cairo_context_get_shape_renderer: (skip)
 * @context: a `PangoContext`, from a pangocairo font map
 * @data: Pointer to `gpointer` to return user data
 *
 * Sets callback function for context to use for rendering attributes
 * of type %PANGO_ATTR_SHAPE.
 *
 * See `PangoCairoShapeRendererFunc` for details.
 *
 * Retrieves callback function and associated user data for rendering
 * attributes of type %PANGO_ATTR_SHAPE as set by
 * [func@PangoCairo.context_set_shape_renderer], if any.
 *
 * Return value: (transfer none) (nullable): the shape rendering callback
 *   previously set on the context, or %NULL if no shape rendering callback
 *   have been set.
 *
 * Since: 1.18
 */
PangoCairoShapeRendererFunc
pango_cairo_context_get_shape_renderer (PangoContext *context,
                                        gpointer     *data)
{
  PangoCairoContextInfo *info;

  g_return_val_if_fail (PANGO_IS_CONTEXT (context), NULL);

  info = get_context_info (context, FALSE);

  if (info)
    {
      if (data)
        *data = info->shape_renderer_data;
      return info->shape_renderer_func;
    }
  else
    {
      if (data)
        *data = NULL;
      return NULL;
    }
}

/**
 * pango_cairo_create_context:
 * @cr: a Cairo context
 *
 * Creates a context object set up to match the current transformation
 * and target surface of the Cairo context.
 *
 * This context can then be
 * used to create a layout using [ctor@Pango.Layout.new].
 *
 * This function is a convenience function that creates a context using
 * the default font map, then updates it to @cr. If you just need to
 * create a layout for use with @cr and do not need to access `PangoContext`
 * directly, you can use [func@create_layout] instead.
 *
 * Return value: (transfer full): the newly created `PangoContext`
 *
 * Since: 1.22
 */
PangoContext *
pango_cairo_create_context (cairo_t *cr)
{
  PangoFontMap *fontmap;
  PangoContext *context;

  g_return_val_if_fail (cr != NULL, NULL);

  fontmap = pango_cairo_font_map_get_default ();
  context = pango_font_map_create_context (fontmap);
  pango_cairo_update_context (cr, context);

  return context;
}

/**
 * pango_cairo_create_layout:
 * @cr: a Cairo context
 *
 * Creates a layout object set up to match the current transformation
 * and target surface of the Cairo context.
 *
 * This layout can then be used for text measurement with functions
 * like [method@Pango.Layout.get_size] or drawing with functions like
 * [func@show_layout]. If you change the transformation or target
 * surface for @cr, you need to call [func@update_layout].
 *
 * This function is the most convenient way to use Cairo with Pango,
 * however it is slightly inefficient since it creates a separate
 * `PangoContext` object for each layout. This might matter in an
 * application that was laying out large amounts of text.
 *
 * Return value: (transfer full): the newly created `PangoLayout`
 *
 * Since: 1.10
 */
PangoLayout *
pango_cairo_create_layout (cairo_t *cr)
{
  PangoContext *context;
  PangoLayout *layout;

  g_return_val_if_fail (cr != NULL, NULL);

  context = pango_cairo_create_context (cr);
  layout = pango_layout_new (context);
  g_object_unref (context);

  return layout;
}

/**
 * pango_cairo_update_layout:
 * @cr: a Cairo context
 * @layout: a `PangoLayout`, from [func@create_layout]
 *
 * Updates the private `PangoContext` of a `PangoLayout` created with
 * [func@create_layout] to match the current transformation and target
 * surface of a Cairo context.
 *
 * Since: 1.10
 */
void
pango_cairo_update_layout (cairo_t     *cr,
			   PangoLayout *layout)
{
  g_return_if_fail (cr != NULL);
  g_return_if_fail (PANGO_IS_LAYOUT (layout));

  _pango_cairo_update_context (cr, pango_layout_get_context (layout));
}

