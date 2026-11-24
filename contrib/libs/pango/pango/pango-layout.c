/* Pango
 * pango-layout.c: High-level layout driver
 *
 * Copyright (C) 2000, 2001, 2006 Red Hat Software
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

/**
 * PangoLayout:
 *
 * A `PangoLayout` structure represents an entire paragraph of text.
 *
 * While complete access to the layout capabilities of Pango is provided
 * using the detailed interfaces for itemization and shaping, using
 * that functionality directly involves writing a fairly large amount
 * of code. `PangoLayout` provides a high-level driver for formatting
 * entire paragraphs of text at once. This includes paragraph-level
 * functionality such as line breaking, justification, alignment and
 * ellipsization.
 *
 * A `PangoLayout` is initialized with a `PangoContext`, UTF-8 string
 * and set of attributes for that string. Once that is done, the set of
 * formatted lines can be extracted from the object, the layout can be
 * rendered, and conversion between logical character positions within
 * the layout's text, and the physical position of the resulting glyphs
 * can be made.
 *
 * There are a number of parameters to adjust the formatting of a
 * `PangoLayout`. The following image shows adjustable parameters
 * (on the left) and font metrics (on the right):
 *
 * <picture>
 *   <source srcset="layout-dark.png" media="(prefers-color-scheme: dark)">
 *   <img alt="Pango Layout Parameters" src="layout-light.png">
 * </picture>
 *
 * The following images demonstrate the effect of alignment and
 * justification on the layout of text:
 *
 * | | |
 * | --- | --- |
 * | ![align=left](align-left.png) | ![align=left, justify](align-left-justify.png) |
 * | ![align=center](align-center.png) | ![align=center, justify](align-center-justify.png) |
 * | ![align=right](align-right.png) | ![align=right, justify](align-right-justify.png) |
 *
 *
 * It is possible, as well, to ignore the 2-D setup,
 * and simply treat the results of a `PangoLayout` as a list of lines.
 */

/**
 * PangoLayoutIter:
 *
 * A `PangoLayoutIter` can be used to iterate over the visual
 * extents of a `PangoLayout`.
 *
 * To obtain a `PangoLayoutIter`, use [method@Pango.Layout.get_iter].
 *
 * The `PangoLayoutIter` structure is opaque, and has no user-visible fields.
 */

#include "config.h"
#include "pango-glyph.h"                /* For pango_shape() */
#include "pango-break.h"
#include "pango-item-private.h"
#include "pango-engine.h"
#include "pango-impl-utils.h"
#include "pango-glyph-item.h"
#include <string.h>
#include <math.h>
#include <locale.h>

#include <hb-ot.h>

#include "pango-layout-private.h"
#include "pango-attributes-private.h"
#include "pango-font-private.h"


typedef struct _ItemProperties ItemProperties;
typedef struct _ParaBreakState ParaBreakState;
typedef struct _LastTabState LastTabState;

/* Note that letter_spacing and shape are constant across items,
 * since we pass them into itemization.
 *
 * uline and strikethrough can vary across an item, so we collect
 * all the values that we find.
 *
 * See pango_layout_get_item_properties for details.
 */
struct _ItemProperties
{
  guint uline_single   : 1;
  guint uline_double   : 1;
  guint uline_low      : 1;
  guint uline_error    : 1;
  guint strikethrough  : 1;
  guint oline_single   : 1;
  guint showing_space  : 1;
  gint            letter_spacing;
  gboolean        shape_set;
  PangoRectangle *shape_ink_rect;
  PangoRectangle *shape_logical_rect;
  double line_height;
  int    absolute_line_height;
};

typedef struct _PangoLayoutLinePrivate PangoLayoutLinePrivate;

struct _PangoLayoutLinePrivate
{
  PangoLayoutLine line;
  guint ref_count;

  /* Extents cache status:
   *
   * LEAKED means that the user has access to this line structure or a
   * run included in this line, and so can change the glyphs/glyph-widths.
   * If this is true, extents caching will be disabled.
   */
  enum {
    NOT_CACHED,
    CACHED,
    LEAKED
  } cache_status;
  PangoRectangle ink_rect;
  PangoRectangle logical_rect;
  int height;
};

struct _PangoLayoutClass
{
  GObjectClass parent_class;


};

#define LINE_IS_VALID(line) ((line) && (line)->layout != NULL)

#ifdef G_DISABLE_CHECKS
#define ITER_IS_INVALID(iter) FALSE
#else
#define ITER_IS_INVALID(iter) G_UNLIKELY (check_invalid ((iter), G_STRLOC))
static gboolean
check_invalid (PangoLayoutIter *iter,
               const char      *loc)
{
  if (iter->line->layout == NULL)
    {
      g_warning ("%s: PangoLayout changed since PangoLayoutIter was created, iterator invalid", loc);
      return TRUE;
    }
  else
    {
      return FALSE;
    }
}
#endif

static void check_context_changed  (PangoLayout *layout);
static void layout_changed  (PangoLayout *layout);

static void pango_layout_clear_lines (PangoLayout *layout);
static void pango_layout_check_lines (PangoLayout *layout);

static PangoAttrList *pango_layout_get_effective_attributes (PangoLayout *layout);

static PangoLayoutLine * pango_layout_line_new         (PangoLayout     *layout);
static void              pango_layout_line_postprocess (PangoLayoutLine *line,
                                                        ParaBreakState  *state,
                                                        gboolean         wrapped);

static void pango_layout_line_leaked (PangoLayoutLine *line);

/* doesn't leak line */
static PangoLayoutLine * _pango_layout_iter_get_line (PangoLayoutIter *iter);
static PangoLayoutRun *  _pango_layout_iter_get_run  (PangoLayoutIter *iter);

static void pango_layout_get_item_properties (PangoItem      *item,
                                              ItemProperties *properties);

static void pango_layout_get_empty_extents_and_height_at_index (PangoLayout    *layout,
                                                                int             index,
                                                                PangoRectangle *logical_rect,
                                                                gboolean        apply_line_height,
                                                                int            *height);

static void pango_layout_finalize    (GObject          *object);

G_DEFINE_TYPE (PangoLayout, pango_layout, G_TYPE_OBJECT)

static void
pango_layout_init (PangoLayout *layout)
{
  layout->serial = 1;
  layout->attrs = NULL;
  layout->font_desc = NULL;
  layout->text = NULL;
  layout->length = 0;
  layout->width = -1;
  layout->height = -1;
  layout->indent = 0;
  layout->spacing = 0;
  layout->line_spacing = 0.0;

  layout->alignment = PANGO_ALIGN_LEFT;
  layout->justify = FALSE;
  layout->justify_last_line = FALSE;
  layout->auto_dir = TRUE;
  layout->single_paragraph = FALSE;

  layout->log_attrs = NULL;
  layout->lines = NULL;
  layout->line_count = 0;

  layout->tab_width = -1;
  layout->decimal = 0;
  layout->unknown_glyphs_count = -1;

  layout->wrap = PANGO_WRAP_WORD;
  layout->is_wrapped = FALSE;
  layout->ellipsize = PANGO_ELLIPSIZE_NONE;
  layout->is_ellipsized = FALSE;
}

static void
pango_layout_class_init (PangoLayoutClass *klass)
{
  GObjectClass *object_class = G_OBJECT_CLASS (klass);

  object_class->finalize = pango_layout_finalize;
}

static void
pango_layout_finalize (GObject *object)
{
  PangoLayout *layout;

  layout = PANGO_LAYOUT (object);

  pango_layout_clear_lines (layout);
  g_free (layout->log_attrs);

  if (layout->context)
    g_object_unref (layout->context);

  if (layout->attrs)
    pango_attr_list_unref (layout->attrs);

  g_free (layout->text);

  if (layout->font_desc)
    pango_font_description_free (layout->font_desc);

  if (layout->tabs)
    pango_tab_array_free (layout->tabs);

  G_OBJECT_CLASS (pango_layout_parent_class)->finalize (object);
}

/**
 * pango_layout_new:
 * @context: a `PangoContext`
 *
 * Create a new `PangoLayout` object with attributes initialized to
 * default values for a particular `PangoContext`.
 *
 * Returns: (transfer full): the newly allocated `PangoLayout`
 */
PangoLayout *
pango_layout_new (PangoContext *context)
{
  PangoLayout *layout;

  g_return_val_if_fail (context != NULL, NULL);

  layout = g_object_new (PANGO_TYPE_LAYOUT, NULL);

  layout->context = context;
  layout->context_serial = pango_context_get_serial (context);
  g_object_ref (context);

  return layout;
}

/**
 * pango_layout_copy:
 * @src: a `PangoLayout`
 *
 * Creates a deep copy-by-value of the layout.
 *
 * The attribute list, tab array, and text from the original layout
 * are all copied by value.
 *
 * Returns: (transfer full): the newly allocated `PangoLayout`
 */
PangoLayout*
pango_layout_copy (PangoLayout *src)
{
  PangoLayout *layout;

  g_return_val_if_fail (PANGO_IS_LAYOUT (src), NULL);

  /* Copy referenced members */

  layout = pango_layout_new (src->context);
  if (src->attrs)
    layout->attrs = pango_attr_list_copy (src->attrs);
  if (src->font_desc)
    layout->font_desc = pango_font_description_copy (src->font_desc);
  if (src->tabs)
    layout->tabs = pango_tab_array_copy (src->tabs);

  /* Dupped */
  layout->text = g_strdup (src->text);

  /* Value fields */
  memcpy (&layout->copy_begin, &src->copy_begin,
          G_STRUCT_OFFSET (PangoLayout, copy_end) - G_STRUCT_OFFSET (PangoLayout, copy_begin));

  return layout;
}

/**
 * pango_layout_get_context:
 * @layout: a `PangoLayout`
 *
 * Retrieves the `PangoContext` used for this layout.
 *
 * Returns: (transfer none): the `PangoContext` for the layout
 */
PangoContext *
pango_layout_get_context (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, NULL);

  return layout->context;
}

/**
 * pango_layout_set_width:
 * @layout: a `PangoLayout`.
 * @width: the desired width in Pango units, or -1 to indicate that no
 *   wrapping or ellipsization should be performed.
 *
 * Sets the width to which the lines of the `PangoLayout` should wrap or
 * ellipsized.
 *
 * The default value is -1: no width set.
 */
void
pango_layout_set_width (PangoLayout *layout,
                        int          width)
{
  g_return_if_fail (layout != NULL);

  if (width < 0)
    width = -1;

  if (width != layout->width)
    {
      layout->width = width;
      layout_changed (layout);
    }
}

/**
 * pango_layout_get_width:
 * @layout: a `PangoLayout`
 *
 * Gets the width to which the lines of the `PangoLayout` should wrap.
 *
 * Returns: the width in Pango units, or -1 if no width set.
 */
int
pango_layout_get_width (PangoLayout    *layout)
{
  g_return_val_if_fail (layout != NULL, 0);
  return layout->width;
}

/**
 * pango_layout_set_height:
 * @layout: a `PangoLayout`.
 * @height: the desired height of the layout in Pango units if positive,
 *   or desired number of lines if negative.
 *
 * Sets the height to which the `PangoLayout` should be ellipsized at.
 *
 * There are two different behaviors, based on whether @height is positive
 * or negative.
 *
 * If @height is positive, it will be the maximum height of the layout. Only
 * lines would be shown that would fit, and if there is any text omitted,
 * an ellipsis added. At least one line is included in each paragraph regardless
 * of how small the height value is. A value of zero will render exactly one
 * line for the entire layout.
 *
 * If @height is negative, it will be the (negative of) maximum number of lines
 * per paragraph. That is, the total number of lines shown may well be more than
 * this value if the layout contains multiple paragraphs of text.
 * The default value of -1 means that the first line of each paragraph is ellipsized.
 * This behavior may be changed in the future to act per layout instead of per
 * paragraph. File a bug against pango at
 * [https://gitlab.gnome.org/gnome/pango](https://gitlab.gnome.org/gnome/pango)
 * if your code relies on this behavior.
 *
 * Height setting only has effect if a positive width is set on
 * @layout and ellipsization mode of @layout is not %PANGO_ELLIPSIZE_NONE.
 * The behavior is undefined if a height other than -1 is set and
 * ellipsization mode is set to %PANGO_ELLIPSIZE_NONE, and may change in the
 * future.
 *
 * Since: 1.20
 */
void
pango_layout_set_height (PangoLayout *layout,
                         int          height)
{
  g_return_if_fail (layout != NULL);

  if (height != layout->height)
    {
      layout->height = height;

      /* Do not invalidate if the number of lines requested is
       * larger than the total number of lines in layout.
       * Bug 549003
       */
      if (layout->ellipsize != PANGO_ELLIPSIZE_NONE &&
          !(layout->lines && layout->is_ellipsized == FALSE &&
            height < 0 && layout->line_count <= (guint) -height))
        layout_changed (layout);
    }
}

/**
 * pango_layout_get_height:
 * @layout: a `PangoLayout`
 *
 * Gets the height of layout used for ellipsization.
 *
 * See [method@Pango.Layout.set_height] for details.
 *
 * Returns: the height, in Pango units if positive,
 *   or number of lines if negative.
 *
 * Since: 1.20
 */
int
pango_layout_get_height (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, 0);
  return layout->height;
}

/**
 * pango_layout_set_wrap:
 * @layout: a `PangoLayout`
 * @wrap: the wrap mode
 *
 * Sets the wrap mode.
 *
 * The wrap mode only has effect if a width is set on the layout
 * with [method@Pango.Layout.set_width]. To turn off wrapping,
 * set the width to -1.
 *
 * The default value is %PANGO_WRAP_WORD.
 */
void
pango_layout_set_wrap (PangoLayout   *layout,
                       PangoWrapMode  wrap)
{
  g_return_if_fail (PANGO_IS_LAYOUT (layout));

  if (layout->wrap != wrap)
    {
      layout->wrap = wrap;

      if (layout->width != -1)
        layout_changed (layout);
    }
}

/**
 * pango_layout_get_wrap:
 * @layout: a `PangoLayout`
 *
 * Gets the wrap mode for the layout.
 *
 * Use [method@Pango.Layout.is_wrapped] to query whether
 * any paragraphs were actually wrapped.
 *
 * Returns: active wrap mode.
 */
PangoWrapMode
pango_layout_get_wrap (PangoLayout *layout)
{
  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), 0);

  return layout->wrap;
}

/**
 * pango_layout_is_wrapped:
 * @layout: a `PangoLayout`
 *
 * Queries whether the layout had to wrap any paragraphs.
 *
 * This returns %TRUE if a positive width is set on @layout,
 * ellipsization mode of @layout is set to %PANGO_ELLIPSIZE_NONE,
 * and there are paragraphs exceeding the layout width that have
 * to be wrapped.
 *
 * Returns: %TRUE if any paragraphs had to be wrapped, %FALSE
 *   otherwise
 *
 * Since: 1.16
 */
gboolean
pango_layout_is_wrapped (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, FALSE);

  pango_layout_check_lines (layout);

  return layout->is_wrapped;
}

/**
 * pango_layout_set_indent:
 * @layout: a `PangoLayout`
 * @indent: the amount by which to indent
 *
 * Sets the width in Pango units to indent each paragraph.
 *
 * A negative value of @indent will produce a hanging indentation.
 * That is, the first line will have the full width, and subsequent
 * lines will be indented by the absolute value of @indent.
 *
 * The indent setting is ignored if layout alignment is set to
 * %PANGO_ALIGN_CENTER.
 *
 * The default value is 0.
 */
void
pango_layout_set_indent (PangoLayout *layout,
                         int          indent)
{
  g_return_if_fail (layout != NULL);

  if (indent != layout->indent)
    {
      layout->indent = indent;
      layout_changed (layout);
    }
}

/**
 * pango_layout_get_indent:
 * @layout: a `PangoLayout`
 *
 * Gets the paragraph indent width in Pango units.
 *
 * A negative value indicates a hanging indentation.
 *
 * Returns: the indent in Pango units
 */
int
pango_layout_get_indent (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, 0);
  return layout->indent;
}

/**
 * pango_layout_set_spacing:
 * @layout: a `PangoLayout`
 * @spacing: the amount of spacing
 *
 * Sets the amount of spacing in Pango units between
 * the lines of the layout.
 *
 * When placing lines with spacing, Pango arranges things so that
 *
 *     line2.top = line1.bottom + spacing
 *
 * The default value is 0.
 *
 * Note: Since 1.44, Pango is using the line height (as determined
 * by the font) for placing lines when the line spacing factor is set
 * to a non-zero value with [method@Pango.Layout.set_line_spacing].
 * In that case, the @spacing set with this function is ignored.
 *
 * Note: for semantics that are closer to the CSS line-height
 * property, see [func@Pango.attr_line_height_new].
 */
void
pango_layout_set_spacing (PangoLayout *layout,
                          int          spacing)
{
  g_return_if_fail (layout != NULL);

  if (spacing != layout->spacing)
    {
      layout->spacing = spacing;
      layout_changed (layout);
    }
}

/**
 * pango_layout_get_spacing:
 * @layout: a `PangoLayout`
 *
 * Gets the amount of spacing between the lines of the layout.
 *
 * Returns: the spacing in Pango units
 */
int
pango_layout_get_spacing (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, 0);
  return layout->spacing;
}

/**
 * pango_layout_set_line_spacing:
 * @layout: a `PangoLayout`
 * @factor: the new line spacing factor
 *
 * Sets a factor for line spacing.
 *
 * Typical values are: 0, 1, 1.5, 2. The default values is 0.
 *
 * If @factor is non-zero, lines are placed so that
 *
 *     baseline2 = baseline1 + factor * height2
 *
 * where height2 is the line height of the second line
 * (as determined by the font(s)). In this case, the spacing
 * set with [method@Pango.Layout.set_spacing] is ignored.
 *
 * If @factor is zero (the default), spacing is applied as before.
 *
 * Note: for semantics that are closer to the CSS line-height
 * property, see [func@Pango.attr_line_height_new].
 *
 * Since: 1.44
 */
void
pango_layout_set_line_spacing (PangoLayout *layout,
                               float        factor)
{
  g_return_if_fail (layout != NULL);

  if (layout->line_spacing != factor)
    {
      layout->line_spacing = factor;
      layout_changed (layout);
    }
}

/**
 * pango_layout_get_line_spacing:
 * @layout: a `PangoLayout`
 *
 * Gets the line spacing factor of @layout.
 *
 * See [method@Pango.Layout.set_line_spacing].
 *
 * Since: 1.44
 */
float
pango_layout_get_line_spacing (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, 1.0);
  return layout->line_spacing;
}

/**
 * pango_layout_set_attributes:
 * @layout: a `PangoLayout`
 * @attrs: (nullable) (transfer none): a `PangoAttrList`
 *
 * Sets the text attributes for a layout object.
 *
 * References @attrs, so the caller can unref its reference.
 */
void
pango_layout_set_attributes (PangoLayout   *layout,
                             PangoAttrList *attrs)
{
  PangoAttrList *old_attrs;

  g_return_if_fail (layout != NULL);

  /* Both empty */
  if (!attrs && !layout->attrs)
    return;

  if (layout->attrs &&
      pango_attr_list_equal (layout->attrs, attrs))
    return;

  old_attrs = layout->attrs;

  /* We always clear lines such that this function can be called
   * whenever attrs changes.
   */
  layout->attrs = attrs;
  if (layout->attrs)
    pango_attr_list_ref (layout->attrs);

  g_clear_pointer (&layout->log_attrs, g_free);
  layout_changed (layout);

  if (old_attrs)
    pango_attr_list_unref (old_attrs);
  layout->tab_width = -1;
}

/**
 * pango_layout_get_attributes:
 * @layout: a `PangoLayout`
 *
 * Gets the attribute list for the layout, if any.
 *
 * Returns: (transfer none) (nullable): a `PangoAttrList`
 */
PangoAttrList*
pango_layout_get_attributes (PangoLayout *layout)
{
  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), NULL);

  return layout->attrs;
}

/**
 * pango_layout_set_font_description:
 * @layout: a `PangoLayout`
 * @desc: (nullable): the new `PangoFontDescription`
 *   to unset the current font description
 *
 * Sets the default font description for the layout.
 *
 * If no font description is set on the layout, the
 * font description from the layout's context is used.
 */
void
pango_layout_set_font_description (PangoLayout                *layout,
                                   const PangoFontDescription *desc)
{
  g_return_if_fail (layout != NULL);

  if (desc != layout->font_desc &&
      (!desc || !layout->font_desc || !pango_font_description_equal(desc, layout->font_desc)))
    {
      if (layout->font_desc)
        pango_font_description_free (layout->font_desc);

      layout->font_desc = desc ? pango_font_description_copy (desc) : NULL;

      layout_changed (layout);
      layout->tab_width = -1;
    }
}

/**
 * pango_layout_get_font_description:
 * @layout: a `PangoLayout`
 *
 * Gets the font description for the layout, if any.
 *
 * Returns: (transfer none) (nullable): a pointer to the
 *   layout's font description, or %NULL if the font description
 *   from the layout's context is inherited.
 *
 * Since: 1.8
 */
const PangoFontDescription *
pango_layout_get_font_description (PangoLayout *layout)
{
  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), NULL);

  return layout->font_desc;
}

/**
 * pango_layout_set_justify:
 * @layout: a `PangoLayout`
 * @justify: whether the lines in the layout should be justified
 *
 * Sets whether each complete line should be stretched to fill the
 * entire width of the layout.
 *
 * Stretching is typically done by adding whitespace, but for some scripts
 * (such as Arabic), the justification may be done in more complex ways,
 * like extending the characters.
 *
 * Note that this setting is not implemented and so is ignored in
 * Pango older than 1.18.
 *
 * Note that tabs and justification conflict with each other:
 * Justification will move content away from its tab-aligned
 * positions.
 *
 * The default value is %FALSE.
 *
 * Also see [method@Pango.Layout.set_justify_last_line].
 */
void
pango_layout_set_justify (PangoLayout *layout,
                          gboolean     justify)
{
  g_return_if_fail (layout != NULL);

  if (justify != layout->justify)
    {
      layout->justify = justify;

      if (layout->is_ellipsized ||
          layout->is_wrapped ||
          layout->justify_last_line)
        layout_changed (layout);
    }
}

/**
 * pango_layout_get_justify:
 * @layout: a `PangoLayout`
 *
 * Gets whether each complete line should be stretched to fill the entire
 * width of the layout.
 *
 * Returns: the justify value
 */
gboolean
pango_layout_get_justify (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, FALSE);
  return layout->justify;
}

/**
 * pango_layout_set_justify_last_line:
 * @layout: a `PangoLayout`
 * @justify: whether the last line in the layout should be justified
 *
 * Sets whether the last line should be stretched to fill the
 * entire width of the layout.
 *
 * This only has an effect if [method@Pango.Layout.set_justify] has
 * been called as well.
 *
 * The default value is %FALSE.
 *
 * Since: 1.50
 */
void
pango_layout_set_justify_last_line (PangoLayout *layout,
                                    gboolean     justify)
{
  g_return_if_fail (layout != NULL);

  if (justify != layout->justify_last_line)
    {
      layout->justify_last_line = justify;

      if (layout->justify)
        layout_changed (layout);
    }
}

/**
 * pango_layout_get_justify_last_line:
 * @layout: a `PangoLayout`
 *
 * Gets whether the last line should be stretched
 * to fill the entire width of the layout.
 *
 * Returns: the justify value
 *
 * Since: 1.50
 */
gboolean
pango_layout_get_justify_last_line (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, FALSE);
  return layout->justify_last_line;
}

/**
 * pango_layout_set_auto_dir:
 * @layout: a `PangoLayout`
 * @auto_dir: if %TRUE, compute the bidirectional base direction
 *   from the layout's contents
 *
 * Sets whether to calculate the base direction
 * for the layout according to its contents.
 *
 * When this flag is on (the default), then paragraphs in @layout that
 * begin with strong right-to-left characters (Arabic and Hebrew principally),
 * will have right-to-left layout, paragraphs with letters from other scripts
 * will have left-to-right layout. Paragraphs with only neutral characters
 * get their direction from the surrounding paragraphs.
 *
 * When %FALSE, the choice between left-to-right and right-to-left
 * layout is done according to the base direction of the layout's
 * `PangoContext`. (See [method@Pango.Context.set_base_dir]).
 *
 * When the auto-computed direction of a paragraph differs from the
 * base direction of the context, the interpretation of
 * %PANGO_ALIGN_LEFT and %PANGO_ALIGN_RIGHT are swapped.
 *
 * Since: 1.4
 */
void
pango_layout_set_auto_dir (PangoLayout *layout,
                           gboolean     auto_dir)
{
  g_return_if_fail (PANGO_IS_LAYOUT (layout));

  auto_dir = auto_dir != FALSE;

  if (auto_dir != layout->auto_dir)
    {
      layout->auto_dir = auto_dir;
      layout_changed (layout);
    }
}

/**
 * pango_layout_get_auto_dir:
 * @layout: a `PangoLayout`
 *
 * Gets whether to calculate the base direction for the layout
 * according to its contents.
 *
 * See [method@Pango.Layout.set_auto_dir].
 *
 * Returns: %TRUE if the bidirectional base direction
 *   is computed from the layout's contents, %FALSE otherwise
 *
 * Since: 1.4
 */
gboolean
pango_layout_get_auto_dir (PangoLayout *layout)
{
  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), TRUE);

  return layout->auto_dir;
}

/**
 * pango_layout_set_alignment:
 * @layout: a `PangoLayout`
 * @alignment: the alignment
 *
 * Sets the alignment for the layout: how partial lines are
 * positioned within the horizontal space available.
 *
 * The default alignment is %PANGO_ALIGN_LEFT.
 */
void
pango_layout_set_alignment (PangoLayout   *layout,
                            PangoAlignment alignment)
{
  g_return_if_fail (layout != NULL);

  if (alignment != layout->alignment)
    {
      layout->alignment = alignment;
      layout_changed (layout);
    }
}

/**
 * pango_layout_get_alignment:
 * @layout: a `PangoLayout`
 *
 * Gets the alignment for the layout: how partial lines are
 * positioned within the horizontal space available.
 *
 * Returns: the alignment
 */
PangoAlignment
pango_layout_get_alignment (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, PANGO_ALIGN_LEFT);
  return layout->alignment;
}


/**
 * pango_layout_set_tabs:
 * @layout: a `PangoLayout`
 * @tabs: (nullable): a `PangoTabArray`
 *
 * Sets the tabs to use for @layout, overriding the default tabs.
 *
 * `PangoLayout` will place content at the next tab position
 * whenever it meets a Tab character (U+0009).
 *
 * By default, tabs are every 8 spaces. If @tabs is %NULL, the
 * default tabs are reinstated. @tabs is copied into the layout;
 * you must free your copy of @tabs yourself.
 *
 * Note that tabs and justification conflict with each other:
 * Justification will move content away from its tab-aligned
 * positions. The same is true for alignments other than
 * %PANGO_ALIGN_LEFT.
 */
void
pango_layout_set_tabs (PangoLayout   *layout,
                       PangoTabArray *tabs)
{
  g_return_if_fail (PANGO_IS_LAYOUT (layout));


  if (tabs != layout->tabs)
    {
      g_clear_pointer (&layout->tabs, pango_tab_array_free);

      if (tabs)
        {
          layout->tabs = pango_tab_array_copy (tabs);
          pango_tab_array_sort (layout->tabs);
        }

      layout_changed (layout);
    }
}

/**
 * pango_layout_get_tabs:
 * @layout: a `PangoLayout`
 *
 * Gets the current `PangoTabArray` used by this layout.
 *
 * If no `PangoTabArray` has been set, then the default tabs are
 * in use and %NULL is returned. Default tabs are every 8 spaces.
 *
 * The return value should be freed with [method@Pango.TabArray.free].
 *
 * Returns: (transfer full) (nullable): a copy of the tabs for this layout
 */
PangoTabArray*
pango_layout_get_tabs (PangoLayout *layout)
{
  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), NULL);

  if (layout->tabs)
    return pango_tab_array_copy (layout->tabs);
  else
    return NULL;
}

/**
 * pango_layout_set_single_paragraph_mode:
 * @layout: a `PangoLayout`
 * @setting: new setting
 *
 * Sets the single paragraph mode of @layout.
 *
 * If @setting is %TRUE, do not treat newlines and similar characters
 * as paragraph separators; instead, keep all text in a single paragraph,
 * and display a glyph for paragraph separator characters. Used when
 * you want to allow editing of newlines on a single text line.
 *
 * The default value is %FALSE.
 */
void
pango_layout_set_single_paragraph_mode (PangoLayout *layout,
                                        gboolean     setting)
{
  g_return_if_fail (PANGO_IS_LAYOUT (layout));

  setting = setting != FALSE;

  if (layout->single_paragraph != setting)
    {
      layout->single_paragraph = setting;
      layout_changed (layout);
    }
}

/**
 * pango_layout_get_single_paragraph_mode:
 * @layout: a `PangoLayout`
 *
 * Obtains whether @layout is in single paragraph mode.
 *
 * See [method@Pango.Layout.set_single_paragraph_mode].
 *
 * Returns: %TRUE if the layout does not break paragraphs
 *   at paragraph separator characters, %FALSE otherwise
 */
gboolean
pango_layout_get_single_paragraph_mode (PangoLayout *layout)
{
  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), FALSE);

  return layout->single_paragraph;
}

/**
 * pango_layout_set_ellipsize:
 * @layout: a `PangoLayout`
 * @ellipsize: the new ellipsization mode for @layout
 *
 * Sets the type of ellipsization being performed for @layout.
 *
 * Depending on the ellipsization mode @ellipsize text is
 * removed from the start, middle, or end of text so they
 * fit within the width and height of layout set with
 * [method@Pango.Layout.set_width] and [method@Pango.Layout.set_height].
 *
 * If the layout contains characters such as newlines that
 * force it to be layed out in multiple paragraphs, then whether
 * each paragraph is ellipsized separately or the entire layout
 * is ellipsized as a whole depends on the set height of the layout.
 *
 * The default value is %PANGO_ELLIPSIZE_NONE.
 *
 * See [method@Pango.Layout.set_height] for details.
 *
 * Since: 1.6
 */
void
pango_layout_set_ellipsize (PangoLayout        *layout,
                            PangoEllipsizeMode  ellipsize)
{
  g_return_if_fail (PANGO_IS_LAYOUT (layout));

  if (ellipsize != layout->ellipsize)
    {
      layout->ellipsize = ellipsize;

      if (layout->is_ellipsized || layout->is_wrapped)
        layout_changed (layout);
    }
}

/**
 * pango_layout_get_ellipsize:
 * @layout: a `PangoLayout`
 *
 * Gets the type of ellipsization being performed for @layout.
 *
 * See [method@Pango.Layout.set_ellipsize].
 *
 * Use [method@Pango.Layout.is_ellipsized] to query whether any
 * paragraphs were actually ellipsized.
 *
 * Returns: the current ellipsization mode for @layout
 *
 * Since: 1.6
 */
PangoEllipsizeMode
pango_layout_get_ellipsize (PangoLayout *layout)
{
  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), PANGO_ELLIPSIZE_NONE);

  return layout->ellipsize;
}

/**
 * pango_layout_is_ellipsized:
 * @layout: a `PangoLayout`
 *
 * Queries whether the layout had to ellipsize any paragraphs.
 *
 * This returns %TRUE if the ellipsization mode for @layout
 * is not %PANGO_ELLIPSIZE_NONE, a positive width is set on @layout,
 * and there are paragraphs exceeding that width that have to be
 * ellipsized.
 *
 * Returns: %TRUE if any paragraphs had to be ellipsized,
 *   %FALSE otherwise
 *
 * Since: 1.16
 */
gboolean
pango_layout_is_ellipsized (PangoLayout *layout)
{
  g_return_val_if_fail (layout != NULL, FALSE);

  pango_layout_check_lines (layout);

  return layout->is_ellipsized;
}

/**
 * pango_layout_set_text:
 * @layout: a `PangoLayout`
 * @text: the text
 * @length: maximum length of @text, in bytes. -1 indicates that
 *   the string is nul-terminated and the length should be calculated.
 *   The text will also be truncated on encountering a nul-termination
 *   even when @length is positive.
 *
 * Sets the text of the layout.
 *
 * This function validates @text and renders invalid UTF-8
 * with a placeholder glyph.
 *
 * Note that if you have used [method@Pango.Layout.set_markup] or
 * [method@Pango.Layout.set_markup_with_accel] on @layout before, you
 * may want to call [method@Pango.Layout.set_attributes] to clear the
 * attributes set on the layout from the markup as this function does
 * not clear attributes.
 */
void
pango_layout_set_text (PangoLayout *layout,
                       const char  *text,
                       int          length)
{
  char *old_text, *start, *end;

  g_return_if_fail (layout != NULL);
  g_return_if_fail (length == 0 || text != NULL);

  old_text = layout->text;

  if (length < 0)
    {
      layout->length = strlen (text);
      layout->text = g_strndup (text, layout->length);
    }
  else if (length > 0)
    {
      /* This is not exactly what we want.  We don't need the padding...
       */
      layout->length = length;
      layout->text = g_strndup (text, length);
    }
  else
    {
      layout->length = 0;
      layout->text = g_malloc0 (1);
    }

  /* validate it, and replace invalid bytes with -1 */
  start = layout->text;
  for (;;) {
    gboolean valid;

    valid = g_utf8_validate (start, -1, (const char **)&end);

    if (!*end)
      break;

    /* Replace invalid bytes with -1.  The -1 will be converted to
     * ((gunichar) -1) by glib, and that in turn yields a glyph value of
     * ((PangoGlyph) -1) by PANGO_GET_UNKNOWN_GLYPH(-1),
     * and that's PANGO_GLYPH_INVALID_INPUT.
     */
    if (!valid)
      *end++ = -1;

    start = end;
  }

  if (start != layout->text)
    /* TODO: Write out the beginning excerpt of text? */
    g_warning ("Invalid UTF-8 string passed to pango_layout_set_text()");

  layout->n_chars = pango_utf8_strlen (layout->text, -1);
  layout->length = strlen (layout->text);

  g_clear_pointer (&layout->log_attrs, g_free);
  layout_changed (layout);

  g_free (old_text);
}

/**
 * pango_layout_get_text:
 * @layout: a `PangoLayout`
 *
 * Gets the text in the layout.
 *
 * The returned text should not be freed or modified.
 *
 * Returns: (transfer none): the text in the @layout
 */
const char*
pango_layout_get_text (PangoLayout *layout)
{
  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), NULL);

  /* We don't ever want to return NULL as the text.
   */
  if (G_UNLIKELY (!layout->text))
    return "";

  return layout->text;
}

/**
 * pango_layout_get_character_count:
 * @layout: a `PangoLayout`
 *
 * Returns the number of Unicode characters in the
 * the text of @layout.
 *
 * Returns: the number of Unicode characters
 *   in the text of @layout
 *
 * Since: 1.30
 */
gint
pango_layout_get_character_count (PangoLayout *layout)
{
  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), 0);

  return layout->n_chars;
}

/**
 * pango_layout_set_markup:
 * @layout: a `PangoLayout`
 * @markup: marked-up text
 * @length: length of marked-up text in bytes, or -1 if @markup is
 *   `NUL`-terminated
 *
 * Sets the layout text and attribute list from marked-up text.
 *
 * See [Pango Markup](pango_markup.html)).
 *
 * Replaces the current text and attribute list.
 *
 * This is the same as [method@Pango.Layout.set_markup_with_accel],
 * but the markup text isn't scanned for accelerators.
 */
void
pango_layout_set_markup (PangoLayout *layout,
                         const char  *markup,
                         int          length)
{
  pango_layout_set_markup_with_accel (layout, markup, length, 0, NULL);
}

/**
 * pango_layout_set_markup_with_accel:
 * @layout: a `PangoLayout`
 * @markup: marked-up text (see [Pango Markup](pango_markup.html))
 * @length: length of marked-up text in bytes, or -1 if @markup is
 *   `NUL`-terminated
 * @accel_marker: marker for accelerators in the text
 * @accel_char: (out) (optional): return location
 *   for first located accelerator
 *
 * Sets the layout text and attribute list from marked-up text.
 *
 * See [Pango Markup](pango_markup.html)).
 *
 * Replaces the current text and attribute list.
 *
 * If @accel_marker is nonzero, the given character will mark the
 * character following it as an accelerator. For example, @accel_marker
 * might be an ampersand or underscore. All characters marked
 * as an accelerator will receive a %PANGO_UNDERLINE_LOW attribute,
 * and the first character so marked will be returned in @accel_char.
 * Two @accel_marker characters following each other produce a single
 * literal @accel_marker character.
 */
void
pango_layout_set_markup_with_accel (PangoLayout *layout,
                                    const char  *markup,
                                    int          length,
                                    gunichar     accel_marker,
                                    gunichar    *accel_char)
{
  PangoAttrList *list = NULL;
  char *text = NULL;
  GError *error;

  g_return_if_fail (PANGO_IS_LAYOUT (layout));
  g_return_if_fail (markup != NULL);

  error = NULL;
  if (!pango_parse_markup (markup, length,
                           accel_marker,
                           &list, &text,
                           accel_char,
                           &error))
    {
      g_warning ("pango_layout_set_markup_with_accel: %s", error->message);
      g_error_free (error);
      return;
    }

  pango_layout_set_text (layout, text, -1);
  pango_layout_set_attributes (layout, list);
  pango_attr_list_unref (list);
  g_free (text);
}

/**
 * pango_layout_get_unknown_glyphs_count:
 * @layout: a `PangoLayout`
 *
 * Counts the number of unknown glyphs in @layout.
 *
 * This function can be used to determine if there are any fonts
 * available to render all characters in a certain string, or when
 * used in combination with %PANGO_ATTR_FALLBACK, to check if a
 * certain font supports all the characters in the string.
 *
 * Returns: The number of unknown glyphs in @layout
 *
 * Since: 1.16
 */
int
pango_layout_get_unknown_glyphs_count (PangoLayout *layout)
{
    PangoLayoutLine *line;
    PangoLayoutRun *run;
    GSList *lines_list;
    GSList *runs_list;
    int i, count = 0;

    g_return_val_if_fail (PANGO_IS_LAYOUT (layout), 0);

    pango_layout_check_lines (layout);

    if (layout->unknown_glyphs_count >= 0)
      return layout->unknown_glyphs_count;

    lines_list = layout->lines;
    while (lines_list)
      {
        line = lines_list->data;
        runs_list = line->runs;

        while (runs_list)
          {
            run = runs_list->data;

            for (i = 0; i < run->glyphs->num_glyphs; i++)
              {
                if (run->glyphs->glyphs[i].glyph & PANGO_GLYPH_UNKNOWN_FLAG)
                    count++;
              }

            runs_list = runs_list->next;
          }
        lines_list = lines_list->next;
      }

    layout->unknown_glyphs_count = count;
    return count;
}

static void
check_context_changed (PangoLayout *layout)
{
  guint old_serial = layout->context_serial;

  layout->context_serial = pango_context_get_serial (layout->context);

  if (old_serial != layout->context_serial)
    pango_layout_context_changed (layout);
}

static void
layout_changed (PangoLayout *layout)
{
  layout->serial++;
  if (layout->serial == 0)
    layout->serial++;

  pango_layout_clear_lines (layout);
}

/**
 * pango_layout_context_changed:
 * @layout: a `PangoLayout`
 *
 * Forces recomputation of any state in the `PangoLayout` that
 * might depend on the layout's context.
 *
 * This function should be called if you make changes to the context
 * subsequent to creating the layout.
 */
void
pango_layout_context_changed (PangoLayout *layout)
{
  g_return_if_fail (PANGO_IS_LAYOUT (layout));

  layout_changed (layout);
  layout->tab_width = -1;
}

/**
 * pango_layout_get_serial:
 * @layout: a `PangoLayout`
 *
 * Returns the current serial number of @layout.
 *
 * The serial number is initialized to an small number larger than zero
 * when a new layout is created and is increased whenever the layout is
 * changed using any of the setter functions, or the `PangoContext` it
 * uses has changed. The serial may wrap, but will never have the value 0.
 * Since it can wrap, never compare it with "less than", always use "not equals".
 *
 * This can be used to automatically detect changes to a `PangoLayout`,
 * and is useful for example to decide whether a layout needs redrawing.
 * To force the serial to be increased, use
 * [method@Pango.Layout.context_changed].
 *
 * Returns: The current serial number of @layout.
 *
 * Since: 1.32.4
 */
guint
pango_layout_get_serial (PangoLayout *layout)
{
  check_context_changed (layout);
  return layout->serial;
}

/**
 * pango_layout_get_log_attrs:
 * @layout: a `PangoLayout`
 * @attrs: (out)(array length=n_attrs)(transfer container):
 *   location to store a pointer to an array of logical attributes.
 *   This value must be freed with g_free().
 * @n_attrs: (out): location to store the number of the attributes in the
 *   array. (The stored value will be one more than the total number
 *   of characters in the layout, since there need to be attributes
 *   corresponding to both the position before the first character
 *   and the position after the last character.)
 *
 * Retrieves an array of logical attributes for each character in
 * the @layout.
 */
void
pango_layout_get_log_attrs (PangoLayout   *layout,
                            PangoLogAttr **attrs,
                            gint          *n_attrs)
{
  g_return_if_fail (layout != NULL);

  pango_layout_check_lines (layout);

  if (attrs)
    {
      *attrs = g_new (PangoLogAttr, layout->n_chars + 1);
      memcpy (*attrs, layout->log_attrs, sizeof(PangoLogAttr) * (layout->n_chars + 1));
    }

  if (n_attrs)
    *n_attrs = layout->n_chars + 1;
}

/**
 * pango_layout_get_log_attrs_readonly:
 * @layout: a `PangoLayout`
 * @n_attrs: (out): location to store the number of the attributes in
 *   the array
 *
 * Retrieves an array of logical attributes for each character in
 * the @layout.
 *
 * This is a faster alternative to [method@Pango.Layout.get_log_attrs].
 * The returned array is part of @layout and must not be modified.
 * Modifying the layout will invalidate the returned array.
 *
 * The number of attributes returned in @n_attrs will be one more
 * than the total number of characters in the layout, since there
 * need to be attributes corresponding to both the position before
 * the first character and the position after the last character.
 *
 * Returns: (array length=n_attrs): an array of logical attributes
 *
 * Since: 1.30
 */
const PangoLogAttr *
pango_layout_get_log_attrs_readonly (PangoLayout *layout,
                                     gint        *n_attrs)
{
  if (n_attrs)
    *n_attrs = 0;
  g_return_val_if_fail (layout != NULL, NULL);

  pango_layout_check_lines (layout);

  if (n_attrs)
    *n_attrs = layout->n_chars + 1;

  return layout->log_attrs;
}


/**
 * pango_layout_get_line_count:
 * @layout: `PangoLayout`
 *
 * Retrieves the count of lines for the @layout.
 *
 * Returns: the line count
 */
int
pango_layout_get_line_count (PangoLayout   *layout)
{
  g_return_val_if_fail (layout != NULL, 0);

  pango_layout_check_lines (layout);
  return layout->line_count;
}

/**
 * pango_layout_get_lines:
 * @layout: a `PangoLayout`
 *
 * Returns the lines of the @layout as a list.
 *
 * Use the faster [method@Pango.Layout.get_lines_readonly] if you do not
 * plan to modify the contents of the lines (glyphs, glyph widths, etc.).
 *
 * Returns: (element-type Pango.LayoutLine) (transfer none): a `GSList`
 *   containing the lines in the layout. This points to internal data of the
 *   `PangoLayout` and must be used with care. It will become invalid on any
 *   change to the layout's text or properties.
 */
GSList *
pango_layout_get_lines (PangoLayout *layout)
{
  pango_layout_check_lines (layout);

  if (layout->lines)
    {
      GSList *tmp_list = layout->lines;
      while (tmp_list)
        {
          PangoLayoutLine *line = tmp_list->data;
          tmp_list = tmp_list->next;

          pango_layout_line_leaked (line);
        }
    }

  return layout->lines;
}

/**
 * pango_layout_get_lines_readonly:
 * @layout: a `PangoLayout`
 *
 * Returns the lines of the @layout as a list.
 *
 * This is a faster alternative to [method@Pango.Layout.get_lines],
 * but the user is not expected to modify the contents of the lines
 * (glyphs, glyph widths, etc.).
 *
 * Returns: (element-type Pango.LayoutLine) (transfer none): a `GSList`
 *   containing the lines in the layout. This points to internal data of the
 *   `PangoLayout` and must be used with care. It will become invalid on any
 *   change to the layout's text or properties. No changes should be made to
 *   the lines.
 *
 * Since: 1.16
 */
GSList *
pango_layout_get_lines_readonly (PangoLayout *layout)
{
  pango_layout_check_lines (layout);

  return layout->lines;
}

/**
 * pango_layout_get_line:
 * @layout: a `PangoLayout`
 * @line: the index of a line, which must be between 0 and
 *   `pango_layout_get_line_count(layout) - 1`, inclusive.
 *
 * Retrieves a particular line from a `PangoLayout`.
 *
 * Use the faster [method@Pango.Layout.get_line_readonly] if you do not
 * plan to modify the contents of the line (glyphs, glyph widths, etc.).
 *
 * Returns: (transfer none) (nullable): the requested `PangoLayoutLine`,
 *   or %NULL if the index is out of range. This layout line can be ref'ed
 *   and retained, but will become invalid if changes are made to the
 *   `PangoLayout`.
 */
PangoLayoutLine *
pango_layout_get_line (PangoLayout *layout,
                       int          line)
{
  GSList *list_item;
  g_return_val_if_fail (layout != NULL, NULL);

  if (line < 0)
    return NULL;

  pango_layout_check_lines (layout);

  list_item = g_slist_nth (layout->lines, line);

  if (list_item)
    {
      PangoLayoutLine *line = list_item->data;

      pango_layout_line_leaked (line);
      return line;
    }

  return NULL;
}

/**
 * pango_layout_get_line_readonly:
 * @layout: a `PangoLayout`
 * @line: the index of a line, which must be between 0 and
 *   `pango_layout_get_line_count(layout) - 1`, inclusive.
 *
 * Retrieves a particular line from a `PangoLayout`.
 *
 * This is a faster alternative to [method@Pango.Layout.get_line],
 * but the user is not expected to modify the contents of the line
 * (glyphs, glyph widths, etc.).
 *
 * Returns: (transfer none) (nullable): the requested `PangoLayoutLine`,
 *   or %NULL if the index is out of range. This layout line can be ref'ed
 *   and retained, but will become invalid if changes are made to the
 *   `PangoLayout`. No changes should be made to the line.
 *
 * Since: 1.16
 */
PangoLayoutLine *
pango_layout_get_line_readonly (PangoLayout *layout,
                                int          line)
{
  GSList *list_item;
  g_return_val_if_fail (layout != NULL, NULL);

  if (line < 0)
    return NULL;

  pango_layout_check_lines (layout);

  list_item = g_slist_nth (layout->lines, line);

  if (list_item)
    {
      PangoLayoutLine *line = list_item->data;
      return line;
    }

  return NULL;
}

/**
 * pango_layout_line_index_to_x:
 * @line: a `PangoLayoutLine`
 * @index_: byte offset of a grapheme within the layout
 * @trailing: an integer indicating the edge of the grapheme to retrieve
 *   the position of. If > 0, the trailing edge of the grapheme,
 *   if 0, the leading of the grapheme
 * @x_pos: (out): location to store the x_offset (in Pango units)
 *
 * Converts an index within a line to a X position.
 */
void
pango_layout_line_index_to_x (PangoLayoutLine *line,
                              int              index,
                              int              trailing,
                              int             *x_pos)
{
  PangoLayout *layout = line->layout;
  GSList *run_list = line->runs;
  int width = 0;

  while (run_list)
    {
      PangoLayoutRun *run = run_list->data;

      if (run->item->offset <= index && run->item->offset + run->item->length > index)
        {
          int offset = g_utf8_pointer_to_offset (layout->text, layout->text + index);
          int attr_offset;

          if (trailing)
            {
              while (index < line->start_index + line->length &&
                     offset + 1 < layout->n_chars &&
                     !layout->log_attrs[offset + 1].is_cursor_position)
                {
                  offset++;
                  index = g_utf8_next_char (layout->text + index) - layout->text;
                }
            }
          else
            {
              while (index > line->start_index &&
                     !layout->log_attrs[offset].is_cursor_position)
                {
                  offset--;
                  index = g_utf8_prev_char (layout->text + index) - layout->text;
                }
            }

          /* Note: we simply assert here, since our items are all internally
           * created. If that ever changes, we need to add a fallback here.
           */
          g_assert (run->item->analysis.flags & PANGO_ANALYSIS_FLAG_HAS_CHAR_OFFSET);
          attr_offset = ((PangoItemPrivate *)run->item)->char_offset;

          pango_glyph_string_index_to_x_full (run->glyphs,
                                              layout->text + run->item->offset,
                                              run->item->length,
                                              &run->item->analysis,
                                              layout->log_attrs + attr_offset,
                                              index - run->item->offset, trailing, x_pos);
          if (x_pos)
            *x_pos += width;

          return;
        }

      width += pango_glyph_string_get_width (run->glyphs);

      run_list = run_list->next;
    }

  if (x_pos)
    *x_pos = width;
}

static PangoLayoutLine *
pango_layout_index_to_line (PangoLayout      *layout,
                            int               index,
                            int              *line_nr,
                            PangoLayoutLine **line_before,
                            PangoLayoutLine **line_after)
{
  GSList *tmp_list;
  GSList *line_list;
  PangoLayoutLine *line = NULL;
  PangoLayoutLine *prev_line = NULL;
  int i = -1;

  line_list = tmp_list = layout->lines;
  while (tmp_list)
    {
      PangoLayoutLine *tmp_line = tmp_list->data;

      if (tmp_line->start_index > index)
        break; /* index was in paragraph delimiters */

      prev_line = line;
      line = tmp_line;
      line_list = tmp_list;
      i++;

      if (line->start_index + line->length > index)
        break;

      tmp_list = tmp_list->next;
    }

  if (line_nr)
    *line_nr = i;

  if (line_before)
    *line_before = prev_line;

  if (line_after)
    *line_after = (line_list && line_list->next) ? line_list->next->data : NULL;

  return line;
}

static PangoLayoutLine *
pango_layout_index_to_line_and_extents (PangoLayout     *layout,
                                        int              index,
                                        PangoRectangle  *line_rect,
                                        PangoRectangle  *run_rect)
{
  PangoLayoutIter iter;
  PangoLayoutLine *line = NULL;

  _pango_layout_get_iter (layout, &iter);

  if (!ITER_IS_INVALID (&iter))
    while (TRUE)
      {
        PangoLayoutLine *tmp_line = _pango_layout_iter_get_line (&iter);

        if (tmp_line->start_index > index)
          break; /* index was in paragraph delimiters */

        line = tmp_line;

        pango_layout_iter_get_line_extents (&iter, NULL, line_rect);

        if (!iter.line_list_link->next ||
            ((PangoLayoutLine *)iter.line_list_link->next->data)->start_index > index)
          {
            if (run_rect)
              {
                while (TRUE)
                  {
                    PangoLayoutRun *run = _pango_layout_iter_get_run (&iter);

                    pango_layout_iter_get_run_extents (&iter, NULL, run_rect);

                    if (!run)
                      break;

                    if (run->item->offset <= index && index < run->item->offset + run->item->length)
                      break;

                    if (!pango_layout_iter_next_run (&iter))
                      break;
                  }
              }

            break;
          }

        if (!pango_layout_iter_next_line (&iter))
          break; /* Use end of last line */
      }

  _pango_layout_iter_destroy (&iter);

  return line;
}

/**
 * pango_layout_index_to_line_x:
 * @layout: a `PangoLayout`
 * @index_: the byte index of a grapheme within the layout
 * @trailing: an integer indicating the edge of the grapheme to retrieve the
 *   position of. If > 0, the trailing edge of the grapheme, if 0,
 *   the leading of the grapheme
 * @line: (out) (optional): location to store resulting line index. (which will
 *   between 0 and pango_layout_get_line_count(layout) - 1)
 * @x_pos: (out) (optional): location to store resulting position within line
 *   (%PANGO_SCALE units per device unit)
 *
 * Converts from byte @index_ within the @layout to line and X position.
 *
 * The X position is measured from the left edge of the line.
 */
void
pango_layout_index_to_line_x (PangoLayout *layout,
                              int          index,
                              gboolean     trailing,
                              int         *line,
                              int         *x_pos)
{
  int line_num;
  PangoLayoutLine *layout_line = NULL;

  g_return_if_fail (layout != NULL);
  g_return_if_fail (index >= 0);
  g_return_if_fail (index <= layout->length);

  pango_layout_check_lines (layout);

  layout_line = pango_layout_index_to_line (layout, index,
                                            &line_num, NULL, NULL);

  if (layout_line)
    {
      /* use end of line if index was in the paragraph delimiters */
      if (index > layout_line->start_index + layout_line->length)
        index = layout_line->start_index + layout_line->length;

      if (line)
        *line = line_num;

      pango_layout_line_index_to_x (layout_line, index, trailing, x_pos);
    }
  else
    {
      if (line)
        *line = -1;
      if (x_pos)
        *x_pos = -1;
    }
}

typedef struct {
  int x;
  int pos;
} CursorPos;

static int
compare_cursor (gconstpointer v1,
                gconstpointer v2)
{
  const CursorPos *c1 = v1;
  const CursorPos *c2 = v2;

  return c1->x - c2->x;
}

static void
pango_layout_line_get_cursors (PangoLayoutLine *line,
                               gboolean         strong,
                               GArray          *cursors)
{
  PangoLayout *layout = line->layout;
  int line_no;
  PangoLayoutLine *line2;
  const char *start, *end;
  int start_offset;
  int j;
  const char *p;
  PangoRectangle pos;

  g_assert (g_array_get_element_size (cursors) == sizeof (CursorPos));
  g_assert (cursors->len == 0);

  start = layout->text + line->start_index;
  end = start + line->length;
  start_offset = g_utf8_pointer_to_offset (layout->text, start);

  pango_layout_index_to_line_x (layout, line->start_index + line->length, 0, &line_no, NULL);
  line2 = pango_layout_get_line (layout, line_no);
  if (line2 == line)
    end++;

  for (j = start_offset, p = start; p < end; j++, p = g_utf8_next_char (p))
    {
      if (layout->log_attrs[j].is_cursor_position)
        {
          CursorPos cursor;

          pango_layout_get_cursor_pos (layout, p - layout->text,
                                       strong ? &pos : NULL,
                                       strong ? NULL : &pos);

          cursor.x = pos.x;
          cursor.pos = p - layout->text;
          g_array_append_val (cursors, cursor);
        }
    }

  g_array_sort (cursors, compare_cursor);
}

/**
 * pango_layout_move_cursor_visually:
 * @layout: a `PangoLayout`
 * @strong: whether the moving cursor is the strong cursor or the
 *   weak cursor. The strong cursor is the cursor corresponding
 *   to text insertion in the base direction for the layout.
 * @old_index: the byte index of the current cursor position
 * @old_trailing: if 0, the cursor was at the leading edge of the
 *   grapheme indicated by @old_index, if > 0, the cursor
 *   was at the trailing edge.
 * @direction: direction to move cursor. A negative
 *   value indicates motion to the left
 * @new_index: (out): location to store the new cursor byte index.
 *   A value of -1 indicates that the cursor has been moved off the
 *   beginning of the layout. A value of %G_MAXINT indicates that
 *   the cursor has been moved off the end of the layout.
 * @new_trailing: (out): number of characters to move forward from
 *   the location returned for @new_index to get the position where
 *   the cursor should be displayed. This allows distinguishing the
 *   position at the beginning of one line from the position at the
 *   end of the preceding line. @new_index is always on the line where
 *   the cursor should be displayed.
 *
 * Computes a new cursor position from an old position and a direction.
 *
 * If @direction is positive, then the new position will cause the strong
 * or weak cursor to be displayed one position to right of where it was
 * with the old cursor position. If @direction is negative, it will be
 * moved to the left.
 *
 * In the presence of bidirectional text, the correspondence between
 * logical and visual order will depend on the direction of the current
 * run, and there may be jumps when the cursor is moved off of the end
 * of a run.
 *
 * Motion here is in cursor positions, not in characters, so a single
 * call to this function may move the cursor over multiple characters
 * when multiple characters combine to form a single grapheme.
 */
void
pango_layout_move_cursor_visually (PangoLayout *layout,
                                   gboolean     strong,
                                   int          old_index,
                                   int          old_trailing,
                                   int          direction,
                                   int         *new_index,
                                   int         *new_trailing)
{
  PangoLayoutLine *line = NULL;
  PangoLayoutLine *prev_line;
  PangoLayoutLine *next_line;
  GArray *cursors;
  int n_vis;
  int vis_pos;
  int start_offset;
  gboolean off_start = FALSE;
  gboolean off_end = FALSE;
  PangoRectangle pos;
  int j;

  g_return_if_fail (layout != NULL);
  g_return_if_fail (old_index >= 0 && old_index <= layout->length);
  g_return_if_fail (old_trailing >= 0);
  g_return_if_fail (old_index < layout->length || old_trailing == 0);
  g_return_if_fail (new_index != NULL);
  g_return_if_fail (new_trailing != NULL);

  direction = (direction >= 0 ? 1 : -1);

  pango_layout_check_lines (layout);

  /* Find the line the old cursor is on */
  line = pango_layout_index_to_line (layout, old_index, NULL, &prev_line, &next_line);

  while (old_trailing--)
    old_index = g_utf8_next_char (layout->text + old_index) - layout->text;

  /* Clamp old_index to fit on the line */
  if (old_index > (line->start_index + line->length))
    old_index = line->start_index + line->length;

  cursors = g_array_new (FALSE, FALSE, sizeof (CursorPos));
  pango_layout_line_get_cursors (line, strong, cursors);

  pango_layout_get_cursor_pos (layout, old_index, strong ? &pos : NULL, strong ? NULL : &pos);

  vis_pos = -1;
  for (j = 0; j < cursors->len; j++)
    {
      CursorPos *cursor = &g_array_index (cursors, CursorPos, j);
      if (cursor->x == pos.x)
        {
          vis_pos = j;

          /* If moving left, we pick the leftmost match, otherwise
           * the rightmost one. Without this, we can get stuck
           */
          if (direction < 0)
            break;
        }
    }

  if (vis_pos == -1 &&
      old_index == line->start_index + line->length)
    {
      if (line->resolved_dir == PANGO_DIRECTION_LTR)
        vis_pos = cursors->len;
      else
        vis_pos = 0;
    }

  /* Handling movement between lines */
  if (line->resolved_dir == PANGO_DIRECTION_LTR)
    {
      if (old_index == line->start_index && direction < 0)
        off_start = TRUE;
      if (old_index == line->start_index + line->length && direction > 0)
        off_end = TRUE;
    }
  else
    {
      if (old_index == line->start_index + line->length && direction < 0)
        off_end = TRUE;
      if (old_index == line->start_index && direction > 0)
        off_start = TRUE;
    }

  if (off_start || off_end)
    {
      /* If we move over a paragraph boundary, count that as
       * an extra position in the motion
       */
      gboolean paragraph_boundary;

      if (off_start)
        {
          if (!prev_line)
            {
              *new_index = -1;
              *new_trailing = 0;
              g_array_unref (cursors);
              return;
            }
          line = prev_line;
          paragraph_boundary = (line->start_index + line->length != old_index);
        }
      else
        {
          if (!next_line)
            {
              *new_index = G_MAXINT;
              *new_trailing = 0;
              g_array_unref (cursors);
              return;
            }
          line = next_line;
          paragraph_boundary = (line->start_index != old_index);
        }

      g_array_set_size (cursors, 0);
      pango_layout_line_get_cursors (line, strong, cursors);

      n_vis = cursors->len;

      if (off_start && direction < 0)
        {
          vis_pos = n_vis;
          if (paragraph_boundary)
            vis_pos++;
        }
      else if (off_end && direction > 0)
        {
          vis_pos = 0;
          if (paragraph_boundary)
            vis_pos--;
        }
    }

  if (direction < 0)
    vis_pos--;
  else
    vis_pos++;

  if (0 <= vis_pos && vis_pos < cursors->len)
    *new_index = g_array_index (cursors, CursorPos, vis_pos).pos;
  else if (vis_pos >= cursors->len - 1)
    *new_index = line->start_index + line->length;

  *new_trailing = 0;

  if (*new_index == line->start_index + line->length && line->length > 0)
    {
      int log_pos;

      start_offset = g_utf8_pointer_to_offset (layout->text, layout->text + line->start_index);
      log_pos = start_offset + pango_utf8_strlen (layout->text + line->start_index, line->length);
      do
        {
          log_pos--;
          *new_index = g_utf8_prev_char (layout->text + *new_index) - layout->text;
          (*new_trailing)++;
        }
      while (log_pos > start_offset && !layout->log_attrs[log_pos].is_cursor_position);
    }

  g_array_unref (cursors);
}

/**
 * pango_layout_xy_to_index:
 * @layout: a `PangoLayout`
 * @x: the X offset (in Pango units) from the left edge of the layout
 * @y: the Y offset (in Pango units) from the top edge of the layout
 * @index_: (out): location to store calculated byte index
 * @trailing: (out): location to store a integer indicating where
 *   in the grapheme the user clicked. It will either be zero, or the
 *   number of characters in the grapheme. 0 represents the leading edge
 *   of the grapheme.
 *
 * Converts from X and Y position within a layout to the byte index to the
 * character at that logical position.
 *
 * If the Y position is not inside the layout, the closest position is
 * chosen (the position will be clamped inside the layout). If the X position
 * is not within the layout, then the start or the end of the line is
 * chosen as described for [method@Pango.LayoutLine.x_to_index]. If either
 * the X or Y positions were not inside the layout, then the function returns
 * %FALSE; on an exact hit, it returns %TRUE.
 *
 * Returns: %TRUE if the coordinates were inside text, %FALSE otherwise
 */
gboolean
pango_layout_xy_to_index (PangoLayout *layout,
                          int          x,
                          int          y,
                          int         *index,
                          gint        *trailing)
{
  PangoLayoutIter iter;
  PangoLayoutLine *prev_line = NULL;
  PangoLayoutLine *found = NULL;
  int found_line_x = 0;
  int prev_last = 0;
  int prev_line_x = 0;
  gboolean retval = FALSE;
  gboolean outside = FALSE;

  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), FALSE);

  _pango_layout_get_iter (layout, &iter);

  do
    {
      PangoRectangle line_logical;
      int first_y, last_y;

      g_assert (!ITER_IS_INVALID (&iter));

      pango_layout_iter_get_line_extents (&iter, NULL, &line_logical);
      pango_layout_iter_get_line_yrange (&iter, &first_y, &last_y);

      if (y < first_y)
        {
          if (prev_line && y < (prev_last + (first_y - prev_last) / 2))
            {
              found = prev_line;
              found_line_x = prev_line_x;
            }
          else
            {
              if (prev_line == NULL)
                outside = TRUE; /* off the top */

              found = _pango_layout_iter_get_line (&iter);
              found_line_x = x - line_logical.x;
            }
        }
      else if (y >= first_y &&
               y < last_y)
        {
          found = _pango_layout_iter_get_line (&iter);
          found_line_x = x - line_logical.x;
        }

      prev_line = _pango_layout_iter_get_line (&iter);
      prev_last = last_y;
      prev_line_x = x - line_logical.x;

      if (found != NULL)
        break;
    }
  while (pango_layout_iter_next_line (&iter));

  _pango_layout_iter_destroy (&iter);

  if (found == NULL)
    {
      /* Off the bottom of the layout */
      outside = TRUE;

      found = prev_line;
      found_line_x = prev_line_x;
    }

  retval = pango_layout_line_x_to_index (found,
                                         found_line_x,
                                         index, trailing);

  if (outside)
    retval = FALSE;

  return retval;
}

/**
 * pango_layout_index_to_pos:
 * @layout: a `PangoLayout`
 * @index_: byte index within @layout
 * @pos: (out): rectangle in which to store the position of the grapheme
 *
 * Converts from an index within a `PangoLayout` to the onscreen position
 * corresponding to the grapheme at that index.
 *
 * The returns is represented as rectangle. Note that `pos->x` is
 * always the leading edge of the grapheme and `pos->x + pos->width` the
 * trailing edge of the grapheme. If the directionality of the grapheme
 * is right-to-left, then `pos->width` will be negative.
 */
void
pango_layout_index_to_pos (PangoLayout    *layout,
                           int             index,
                           PangoRectangle *pos)
{
  PangoRectangle line_logical_rect = { 0, };
  PangoRectangle run_logical_rect = { 0, };
  PangoLayoutIter iter;
  PangoLayoutLine *layout_line = NULL;
  int x_pos;

  g_return_if_fail (layout != NULL);
  g_return_if_fail (index >= 0);
  g_return_if_fail (pos != NULL);

  _pango_layout_get_iter (layout, &iter);

  if (!ITER_IS_INVALID (&iter))
    {
      while (TRUE)
        {
          PangoLayoutLine *tmp_line = _pango_layout_iter_get_line (&iter);

          if (tmp_line->start_index > index)
            {
              /* index is in the paragraph delimiters, move to
               * end of previous line
               *
               * This shouldn’t occur in the first loop iteration as the first
               * line’s start_index should always be 0.
               */
              g_assert (layout_line != NULL);
              index = layout_line->start_index + layout_line->length;
              break;
            }

          pango_layout_iter_get_line_extents (&iter, NULL, &line_logical_rect);

          layout_line = tmp_line;

          if (layout_line->start_index + layout_line->length >= index)
            {
              do
                {
                  PangoLayoutRun *run = _pango_layout_iter_get_run (&iter);

                  pango_layout_iter_get_run_extents (&iter, NULL, &run_logical_rect);

                  if (!run)
                    break;

                  if (run->item->offset <= index && index < run->item->offset + run->item->length)
                    break;
                 }
               while (pango_layout_iter_next_run (&iter));

              if (layout_line->start_index + layout_line->length > index)
                break;
            }

          if (!pango_layout_iter_next_line (&iter))
            {
              index = layout_line->start_index + layout_line->length;
              break;
            }
        }

      pos->y = run_logical_rect.y;
      pos->height = run_logical_rect.height;

      pango_layout_line_index_to_x (layout_line, index, 0, &x_pos);
      pos->x = line_logical_rect.x + x_pos;

      if (index < layout_line->start_index + layout_line->length)
        {
          pango_layout_line_index_to_x (layout_line, index, 1, &x_pos);
          pos->width = (line_logical_rect.x + x_pos) - pos->x;
        }
      else
        pos->width = 0;
    }

  _pango_layout_iter_destroy (&iter);
}

static PangoLayoutRun *
pango_layout_line_get_run (PangoLayoutLine *line,
                           int              index)
{
  GSList *run_list;

  run_list = line->runs;
  while (run_list)
    {
      PangoLayoutRun *run = run_list->data;

      if (run->item->offset <= index && run->item->offset + run->item->length > index)
        return run;

      run_list = run_list->next;
    }

  return  NULL;
}

static int
pango_layout_line_get_char_level (PangoLayoutLine *line,
                                  int              index)
{
  PangoLayoutRun *run;

  run = pango_layout_line_get_run (line, index);

  if (run)
    return run->item->analysis.level;

  return 0;
}

static PangoDirection
pango_layout_line_get_char_direction (PangoLayoutLine *layout_line,
                                      int              index)
{
   return pango_layout_line_get_char_level (layout_line, index) % 2
          ? PANGO_DIRECTION_RTL
          : PANGO_DIRECTION_LTR;
}

/**
 * pango_layout_get_direction:
 * @layout: a `PangoLayout`
 * @index: the byte index of the char
 *
 * Gets the text direction at the given character position in @layout.
 *
 * Returns: the text direction at @index
 *
 * Since: 1.46
 */
PangoDirection
pango_layout_get_direction (PangoLayout *layout,
                            int          index)
{
  PangoLayoutLine *line;

  line = pango_layout_index_to_line_and_extents (layout, index, NULL, NULL);

  if (line)
    return pango_layout_line_get_char_direction (line, index);

  return PANGO_DIRECTION_LTR;
}

/**
 * pango_layout_get_cursor_pos:
 * @layout: a `PangoLayout`
 * @index_: the byte index of the cursor
 * @strong_pos: (out) (optional): location to store the strong cursor position
 * @weak_pos: (out) (optional): location to store the weak cursor position
 *
 * Given an index within a layout, determines the positions that of the
 * strong and weak cursors if the insertion point is at that index.
 *
 * The position of each cursor is stored as a zero-width rectangle
 * with the height of the run extents.
 *
 * <picture>
 *   <source srcset="cursor-positions-dark.png" media="(prefers-color-scheme: dark)">
 *   <img alt="Cursor positions" src="cursor-positions-light.png">
 * </picture>
 *
 * The strong cursor location is the location where characters of the
 * directionality equal to the base direction of the layout are inserted.
 * The weak cursor location is the location where characters of the
 * directionality opposite to the base direction of the layout are inserted.
 *
 * The following example shows text with both a strong and a weak cursor.
 *
 * <picture>
 *   <source srcset="split-cursor-dark.png" media="(prefers-color-scheme: dark)">
 *   <img alt="Strong and weak cursors" src="split-cursor-light.png">
 * </picture>
 *
 * The strong cursor has a little arrow pointing to the right, the weak
 * cursor to the left. Typing a 'c' in this situation will insert the
 * character after the 'b', and typing another Hebrew character, like 'ג',
 * will insert it at the end.
 */
void
pango_layout_get_cursor_pos (PangoLayout    *layout,
                             int             index,
                             PangoRectangle *strong_pos,
                             PangoRectangle *weak_pos)
{
  PangoDirection dir1, dir2;
  int level1, level2;
  PangoRectangle line_rect = { 666, };
  PangoRectangle run_rect = { 666, };
  PangoLayoutLine *layout_line = NULL; /* Quiet GCC */
  int x1_trailing;
  int x2;

  g_return_if_fail (layout != NULL);
  g_return_if_fail (index >= 0 && index <= layout->length);

  layout_line = pango_layout_index_to_line_and_extents (layout, index,
                                                        &line_rect, &run_rect);

  g_assert (index >= layout_line->start_index);

  /* Examine the trailing edge of the character before the cursor */
  if (index == layout_line->start_index)
    {
      dir1 = layout_line->resolved_dir;
      level1 = dir1 == PANGO_DIRECTION_LTR ? 0 : 1;
      if (layout_line->resolved_dir == PANGO_DIRECTION_LTR)
        x1_trailing = 0;
      else
        x1_trailing = line_rect.width;
    }
  else
    {
      gint prev_index = g_utf8_prev_char (layout->text + index) - layout->text;
      level1 = pango_layout_line_get_char_level (layout_line, prev_index);
      dir1 = level1 % 2 ? PANGO_DIRECTION_RTL : PANGO_DIRECTION_LTR;
      pango_layout_line_index_to_x (layout_line, prev_index, TRUE, &x1_trailing);
    }

  /* Examine the leading edge of the character after the cursor */
  if (index >= layout_line->start_index + layout_line->length)
    {
      dir2 = layout_line->resolved_dir;
      level2 = dir2 == PANGO_DIRECTION_LTR ? 0 : 1;
      if (layout_line->resolved_dir == PANGO_DIRECTION_LTR)
        x2 = line_rect.width;
      else
        x2 = 0;
    }
  else
    {
      pango_layout_line_index_to_x (layout_line, index, FALSE, &x2);
      level2 = pango_layout_line_get_char_level (layout_line, index);
      dir2 = level2 % 2 ? PANGO_DIRECTION_RTL : PANGO_DIRECTION_LTR;
    }

  if (strong_pos)
    {
      strong_pos->x = line_rect.x;

      if (dir1 == layout_line->resolved_dir &&
          (dir2 != dir1 || level1 < level2))
        strong_pos->x += x1_trailing;
      else
        strong_pos->x += x2;

      strong_pos->y = run_rect.y;
      strong_pos->width = 0;
      strong_pos->height = run_rect.height;
    }

  if (weak_pos)
    {
      weak_pos->x = line_rect.x;

      if (dir1 == layout_line->resolved_dir &&
          (dir2 != dir1 || level1 < level2))
        weak_pos->x += x2;
      else
        weak_pos->x += x1_trailing;

      weak_pos->y = run_rect.y;
      weak_pos->width = 0;
      weak_pos->height = run_rect.height;
    }
}

/**
 * pango_layout_get_caret_pos:
 * @layout: a `PangoLayout`
 * @index_: the byte index of the cursor
 * @strong_pos: (out) (optional): location to store the strong cursor position
 * @weak_pos: (out) (optional): location to store the weak cursor position
 *
 * Given an index within a layout, determines the positions that of the
 * strong and weak cursors if the insertion point is at that index.
 *
 * This is a variant of [method@Pango.Layout.get_cursor_pos] that applies
 * font metric information about caret slope and offset to the positions
 * it returns.
 *
 * <picture>
 *   <source srcset="caret-metrics-dark.png" media="(prefers-color-scheme: dark)">
 *   <img alt="Caret metrics" src="caret-metrics-light.png">
 * </picture>
 *
 * Since: 1.50
 */
void
pango_layout_get_caret_pos (PangoLayout    *layout,
                            int             index,
                            PangoRectangle *strong_pos,
                            PangoRectangle *weak_pos)
{
  PangoLayoutLine *line;
  PangoLayoutRun *run;
  hb_font_t *hb_font;
  hb_position_t caret_offset, caret_run, caret_rise, descender;

  pango_layout_get_cursor_pos (layout, index, strong_pos, weak_pos);

  /* FIXME: not very efficient to re-iterate here */
  line = pango_layout_index_to_line_and_extents (layout, index, NULL, NULL);
  run = pango_layout_line_get_run (line, index);
  if (!run)
    run = pango_layout_line_get_run (line, index - 1);

  if (!run)
    return;

  hb_font = pango_font_get_hb_font (run->item->analysis.font);

  if (hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_HORIZONTAL_CARET_RISE, &caret_rise) &&
      hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_HORIZONTAL_CARET_RUN, &caret_run) &&
      hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_HORIZONTAL_CARET_OFFSET, &caret_offset) &&
      hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_HORIZONTAL_DESCENDER, &descender))
    {
      double slope_inv;
      int x_scale, y_scale;

      if (strong_pos)
        strong_pos->x += caret_offset;

      if (weak_pos)
        weak_pos->x += caret_offset;

      if (caret_rise == 0)
        return;

      hb_font_get_scale (hb_font, &x_scale, &y_scale);
      slope_inv = (caret_run / (double) caret_rise) * (y_scale / (double) x_scale);

      if (strong_pos)
        {
          strong_pos->x += descender * slope_inv;
          strong_pos->width = strong_pos->height * slope_inv;
          if (slope_inv < 0)
            strong_pos->x -= strong_pos->width;
        }

      if (weak_pos)
        {
          weak_pos->x += descender * slope_inv;
          weak_pos->width = weak_pos->height * slope_inv;
          if (slope_inv < 0)
            weak_pos->x -= weak_pos->width;
        }
    }
}

static inline int
direction_simple (PangoDirection d)
{
  switch (d)
    {
    case PANGO_DIRECTION_LTR :
    case PANGO_DIRECTION_WEAK_LTR :
    case PANGO_DIRECTION_TTB_RTL :
      return 1;
    case PANGO_DIRECTION_RTL :
    case PANGO_DIRECTION_WEAK_RTL :
    case PANGO_DIRECTION_TTB_LTR :
      return -1;
    case PANGO_DIRECTION_NEUTRAL :
      return 0;
    default:
      break;
    }
  /* not reached */
  return 0;
}

static PangoAlignment
get_alignment (PangoLayout     *layout,
               PangoLayoutLine *line)
{
  PangoAlignment alignment = layout->alignment;

  if (alignment != PANGO_ALIGN_CENTER && line->layout->auto_dir &&
      direction_simple (line->resolved_dir) ==
      -direction_simple (pango_context_get_base_dir (layout->context)))
    {
      if (alignment == PANGO_ALIGN_LEFT)
        alignment = PANGO_ALIGN_RIGHT;
      else if (alignment == PANGO_ALIGN_RIGHT)
        alignment = PANGO_ALIGN_LEFT;
    }

  return alignment;
}

static void
get_x_offset (PangoLayout     *layout,
              PangoLayoutLine *line,
              int              layout_width,
              int              line_width,
              int             *x_offset)
{
  PangoAlignment alignment = get_alignment (layout, line);

  /* Alignment */
  if (layout_width == 0)
    *x_offset = 0;
  else if (alignment == PANGO_ALIGN_RIGHT)
    *x_offset = layout_width - line_width;
  else if (alignment == PANGO_ALIGN_CENTER) {
    *x_offset = (layout_width - line_width) / 2;
    /* hinting */
    if (((layout_width | line_width) & (PANGO_SCALE - 1)) == 0)
      {
        *x_offset = PANGO_UNITS_ROUND (*x_offset);
      }
  } else
    *x_offset = 0;

  /* Indentation */

  /* For center, we ignore indentation; I think I've seen word
   * processors that still do the indentation here as if it were
   * indented left/right, though we can't sensibly do that without
   * knowing whether left/right is the "normal" thing for this text
   */

  if (alignment == PANGO_ALIGN_CENTER)
    return;

  if (line->is_paragraph_start)
    {
      if (layout->indent > 0)
        {
          if (alignment == PANGO_ALIGN_LEFT)
            *x_offset += layout->indent;
          else
            *x_offset -= layout->indent;
        }
    }
  else
    {
      if (layout->indent < 0)
        {
          if (alignment == PANGO_ALIGN_LEFT)
            *x_offset -= layout->indent;
          else
            *x_offset += layout->indent;
        }
    }
}

static void
pango_layout_line_get_extents_and_height (PangoLayoutLine *line,
                                          PangoRectangle *ink,
                                          PangoRectangle *logical,
                                          int            *height);
static void
get_line_extents_layout_coords (PangoLayout     *layout,
                                PangoLayoutLine *line,
                                int              layout_width,
                                int              y_offset,
                                int             *baseline,
                                PangoRectangle  *line_ink_layout,
                                PangoRectangle  *line_logical_layout)
{
  int x_offset;
  /* Line extents in line coords (origin at line baseline) */
  PangoRectangle line_ink;
  PangoRectangle line_logical;
  gboolean first_line;
  int new_baseline;
  int height;

  if (layout->lines->data == line)
    first_line = TRUE;
  else
    first_line = FALSE;

  pango_layout_line_get_extents_and_height (line, line_ink_layout ? &line_ink : NULL,
                                            &line_logical,
                                            &height);

  get_x_offset (layout, line, layout_width, line_logical.width, &x_offset);

  if (first_line || !baseline || layout->line_spacing == 0.0)
    new_baseline = y_offset - line_logical.y;
  else
    new_baseline = *baseline + layout->line_spacing * height;

  /* Convert the line's extents into layout coordinates */
  if (line_ink_layout)
    {
      *line_ink_layout = line_ink;
      line_ink_layout->x = line_ink.x + x_offset;
      line_ink_layout->y = new_baseline + line_ink.y;
    }

  if (line_logical_layout)
    {
      *line_logical_layout = line_logical;
      line_logical_layout->x = line_logical.x + x_offset;
      line_logical_layout->y = new_baseline + line_logical.y;
    }

  if (baseline)
    *baseline = new_baseline;
}

/* if non-NULL line_extents returns a list of line extents
 * in layout coordinates
 */
static void
pango_layout_get_extents_internal (PangoLayout    *layout,
                                   PangoRectangle *ink_rect,
                                   PangoRectangle *logical_rect,
                                   Extents        **line_extents)
{
  GSList *line_list;
  int y_offset = 0;
  int width;
  gboolean need_width = FALSE;
  int line_index = 0;
  int baseline;

  g_return_if_fail (layout != NULL);

  pango_layout_check_lines (layout);

  if (ink_rect && layout->ink_rect_cached)
    {
      *ink_rect = layout->ink_rect;
      ink_rect = NULL;
    }
  if (logical_rect && layout->logical_rect_cached)
    {
      *logical_rect = layout->logical_rect;
      logical_rect = NULL;
    }
  if (!ink_rect && !logical_rect && !line_extents)
    return;

  /* When we are not wrapping, we need the overall width of the layout to
   * figure out the x_offsets of each line. However, we only need the
   * x_offsets if we are computing the ink_rect or individual line extents.
   */
  width = layout->width;

  if (layout->auto_dir)
    {
      /* If one of the lines of the layout is not left aligned, then we need
       * the width of the layout to calculate line x-offsets; this requires
       * looping through the lines for layout->auto_dir.
       */
      line_list = layout->lines;
      while (line_list && !need_width)
        {
          PangoLayoutLine *line = line_list->data;

          if (get_alignment (layout, line) != PANGO_ALIGN_LEFT)
            need_width = TRUE;

          line_list = line_list->next;
        }
    }
  else if (layout->alignment != PANGO_ALIGN_LEFT)
    need_width = TRUE;

  if (width == -1 && need_width && (ink_rect || line_extents))
    {
      PangoRectangle overall_logical;

      pango_layout_get_extents_internal (layout, NULL, &overall_logical, NULL);
      width = overall_logical.width;
    }

  if (logical_rect)
    {
      logical_rect->x = 0;
      logical_rect->y = 0;
      logical_rect->width = 0;
      logical_rect->height = 0;
    }


  if (line_extents && layout->line_count > 0)
    {
      *line_extents = g_malloc (sizeof (Extents) * layout->line_count);
    }

  baseline = 0;
  line_list = layout->lines;
  while (line_list)
    {
      PangoLayoutLine *line = line_list->data;
      /* Line extents in layout coords (origin at 0,0 of the layout) */
      PangoRectangle line_ink_layout;
      PangoRectangle line_logical_layout;

      int new_pos;

      /* This block gets the line extents in layout coords */
      {
        get_line_extents_layout_coords (layout, line,
                                        width, y_offset,
                                        &baseline,
                                        ink_rect ? &line_ink_layout : NULL,
                                        &line_logical_layout);

        if (line_extents && layout->line_count > 0)
          {
            Extents *ext = &(*line_extents)[line_index];
            ext->baseline = baseline;
            ext->ink_rect = line_ink_layout;
            ext->logical_rect = line_logical_layout;
          }
      }

      if (ink_rect)
        {
          /* Compute the union of the current ink_rect with
           * line_ink_layout
           */

          if (line_list == layout->lines)
            {
              *ink_rect = line_ink_layout;
            }
          else
            {
              new_pos = MIN (ink_rect->x, line_ink_layout.x);
              ink_rect->width =
                MAX (ink_rect->x + ink_rect->width,
                     line_ink_layout.x + line_ink_layout.width) - new_pos;
              ink_rect->x = new_pos;

              new_pos = MIN (ink_rect->y, line_ink_layout.y);
              ink_rect->height =
                MAX (ink_rect->y + ink_rect->height,
                     line_ink_layout.y + line_ink_layout.height) - new_pos;
              ink_rect->y = new_pos;
            }
        }

      if (logical_rect)
        {
          if (layout->width == -1)
            {
              /* When no width is set on layout, we can just compute the max of the
               * line lengths to get the horizontal extents ... logical_rect.x = 0.
               */
              logical_rect->width = MAX (logical_rect->width, line_logical_layout.width);
            }
          else
            {
              /* When a width is set, we have to compute the union of the horizontal
               * extents of all the lines.
               */
              if (line_list == layout->lines)
                {
                  logical_rect->x = line_logical_layout.x;
                  logical_rect->width = line_logical_layout.width;
                }
              else
                {
                  new_pos = MIN (logical_rect->x, line_logical_layout.x);
                  logical_rect->width =
                    MAX (logical_rect->x + logical_rect->width,
                         line_logical_layout.x + line_logical_layout.width) - new_pos;
                  logical_rect->x = new_pos;

                }
            }

          logical_rect->height = line_logical_layout.y + line_logical_layout.height - logical_rect->y;
        }

      y_offset = line_logical_layout.y + line_logical_layout.height + layout->spacing;
      line_list = line_list->next;
      line_index ++;
    }

  if (ink_rect)
    {
      layout->ink_rect = *ink_rect;
      layout->ink_rect_cached = TRUE;
    }
  if (logical_rect)
    {
      layout->logical_rect = *logical_rect;
      layout->logical_rect_cached = TRUE;
    }
}

/**
 * pango_layout_get_extents:
 * @layout: a `PangoLayout`
 * @ink_rect: (out) (optional): rectangle used to store the extents of the
 *   layout as drawn
 * @logical_rect: (out) (optional):rectangle used to store the logical
 *   extents of the layout
 *
 * Computes the logical and ink extents of @layout.
 *
 * Logical extents are usually what you want for positioning things. Note
 * that both extents may have non-zero x and y. You may want to use those
 * to offset where you render the layout. Not doing that is a very typical
 * bug that shows up as right-to-left layouts not being correctly positioned
 * in a layout with a set width.
 *
 * The extents are given in layout coordinates and in Pango units; layout
 * coordinates begin at the top left corner of the layout.
 */
void
pango_layout_get_extents (PangoLayout    *layout,
                          PangoRectangle *ink_rect,
                          PangoRectangle *logical_rect)
{
  g_return_if_fail (layout != NULL);

  pango_layout_get_extents_internal (layout, ink_rect, logical_rect, NULL);
}

/**
 * pango_layout_get_pixel_extents:
 * @layout: a `PangoLayout`
 * @ink_rect: (out) (optional): rectangle used to store the extents of the
 *   layout as drawn
 * @logical_rect: (out) (optional): rectangle used to store the logical
 *   extents of the layout
 *
 * Computes the logical and ink extents of @layout in device units.
 *
 * This function just calls [method@Pango.Layout.get_extents] followed by
 * two [func@extents_to_pixels] calls, rounding @ink_rect and @logical_rect
 * such that the rounded rectangles fully contain the unrounded one (that is,
 * passes them as first argument to [func@Pango.extents_to_pixels]).
 */
void
pango_layout_get_pixel_extents (PangoLayout    *layout,
                                PangoRectangle *ink_rect,
                                PangoRectangle *logical_rect)
{
  g_return_if_fail (PANGO_IS_LAYOUT (layout));

  pango_layout_get_extents (layout, ink_rect, logical_rect);
  pango_extents_to_pixels (ink_rect, NULL);
  pango_extents_to_pixels (logical_rect, NULL);
}

/**
 * pango_layout_get_size:
 * @layout: a `PangoLayout`
 * @width: (out) (optional): location to store the logical width
 * @height: (out) (optional): location to store the logical height
 *
 * Determines the logical width and height of a `PangoLayout` in Pango
 * units.
 *
 * This is simply a convenience function around [method@Pango.Layout.get_extents].
 */
void
pango_layout_get_size (PangoLayout *layout,
                       int         *width,
                       int         *height)
{
  PangoRectangle logical_rect;

  pango_layout_get_extents (layout, NULL, &logical_rect);

  if (width)
    *width = logical_rect.width;
  if (height)
    *height = logical_rect.height;
}

/**
 * pango_layout_get_pixel_size:
 * @layout: a `PangoLayout`
 * @width: (out) (optional): location to store the logical width
 * @height: (out) (optional): location to store the logical height
 *
 * Determines the logical width and height of a `PangoLayout` in device
 * units.
 *
 * [method@Pango.Layout.get_size] returns the width and height
 * scaled by %PANGO_SCALE. This is simply a convenience function
 * around [method@Pango.Layout.get_pixel_extents].
 */
void
pango_layout_get_pixel_size (PangoLayout *layout,
                             int         *width,
                             int         *height)
{
  PangoRectangle logical_rect;

  pango_layout_get_extents_internal (layout, NULL, &logical_rect, NULL);
  pango_extents_to_pixels (&logical_rect, NULL);

  if (width)
    *width = logical_rect.width;
  if (height)
    *height = logical_rect.height;
}

/**
 * pango_layout_get_baseline:
 * @layout: a `PangoLayout`
 *
 * Gets the Y position of baseline of the first line in @layout.
 *
 * Returns: baseline of first line, from top of @layout
 *
 * Since: 1.22
 */
int
pango_layout_get_baseline (PangoLayout *layout)
{
  int baseline;
  Extents *extents = NULL;

  /* XXX this is kinda inefficient */
  pango_layout_get_extents_internal (layout, NULL, NULL, &extents);
  baseline = extents ? extents[0].baseline : 0;

  g_free (extents);

  return baseline;
}

static void
pango_layout_clear_lines (PangoLayout *layout)
{
  if (layout->lines)
    {
      GSList *tmp_list = layout->lines;
      while (tmp_list)
        {
          PangoLayoutLine *line = tmp_list->data;
          tmp_list = tmp_list->next;

          line->layout = NULL;
          pango_layout_line_unref (line);
        }

      g_slist_free (layout->lines);
      layout->lines = NULL;
      layout->line_count = 0;
    }

  layout->unknown_glyphs_count = -1;
  layout->logical_rect_cached = FALSE;
  layout->ink_rect_cached = FALSE;
  layout->is_ellipsized = FALSE;
  layout->is_wrapped = FALSE;
}

static void
pango_layout_line_leaked (PangoLayoutLine *line)
{
  PangoLayoutLinePrivate *private = (PangoLayoutLinePrivate *)line;

  private->cache_status = LEAKED;

  if (line->layout)
    {
      line->layout->logical_rect_cached = FALSE;
      line->layout->ink_rect_cached = FALSE;
    }
}


/*****************
 * Line Breaking *
 *****************/

static void shape_tab (PangoLayoutLine  *line,
                       LastTabState     *tab_state,
                       ItemProperties   *properties,
                       int               current_width,
                       PangoItem        *item,
                       PangoGlyphString *glyphs);

static void
free_run (PangoLayoutRun *run, gpointer data)
{
  gboolean free_item = data != NULL;
  if (free_item)
    pango_item_free (run->item);

  pango_glyph_string_free (run->glyphs);
  g_slice_free (PangoLayoutRun, run);
}

static PangoItem *
uninsert_run (PangoLayoutLine *line)
{
  PangoLayoutRun *run;
  PangoItem *item;

  GSList *tmp_node = line->runs;

  run = tmp_node->data;
  item = run->item;

  line->runs = tmp_node->next;
  line->length -= item->length;

  g_slist_free_1 (tmp_node);
  free_run (run, (gpointer)FALSE);

  return item;
}

static void
ensure_tab_width (PangoLayout *layout)
{
  if (layout->tab_width == -1)
    {
      /* Find out how wide 8 spaces are in the context's default
       * font. Utter performance killer. :-(
       */
      PangoGlyphString *glyphs = pango_glyph_string_new ();
      PangoItem *item;
      GList *items;
      PangoAttribute *attr;
      PangoAttrList *layout_attrs;
      PangoAttrList tmp_attrs;
      PangoFontDescription *font_desc = pango_font_description_copy_static (pango_context_get_font_description (layout->context));
      PangoLanguage *language = NULL;
      PangoShapeFlags shape_flags = PANGO_SHAPE_NONE;

      if (pango_context_get_round_glyph_positions (layout->context))
        shape_flags |= PANGO_SHAPE_ROUND_POSITIONS;

      layout_attrs = pango_layout_get_effective_attributes (layout);
      if (layout_attrs)
        {
          PangoAttrIterator iter;

          _pango_attr_list_get_iterator (layout_attrs, &iter);
          pango_attr_iterator_get_font (&iter, font_desc, &language, NULL);
          _pango_attr_iterator_destroy (&iter);
        }

      _pango_attr_list_init (&tmp_attrs);

      attr = pango_attr_font_desc_new (font_desc);
      pango_font_description_free (font_desc);
      pango_attr_list_insert_before (&tmp_attrs, attr);

      if (language)
        {
          attr = pango_attr_language_new (language);
          pango_attr_list_insert_before (&tmp_attrs, attr);
        }

      items = pango_itemize (layout->context, " ", 0, 1, &tmp_attrs, NULL);

      if (layout_attrs != layout->attrs)
        {
          pango_attr_list_unref (layout_attrs);
          layout_attrs = NULL;
        }
      _pango_attr_list_destroy (&tmp_attrs);

      item = items->data;
      pango_shape_with_flags ("        ", 8, "        ", 8, &item->analysis, glyphs, shape_flags);

      pango_item_free (item);
      g_list_free (items);

      layout->tab_width = pango_glyph_string_get_width (glyphs);

      pango_glyph_string_free (glyphs);

      /* We need to make sure the tab_width is > 0 so finding tab positions
       * terminates. This check should be necessary only under extreme
       * problems with the font.
       */
      if (layout->tab_width <= 0)
        layout->tab_width = 50 * PANGO_SCALE; /* pretty much arbitrary */
    }
}

static void
get_tab_pos (PangoLayoutLine *line,
             int              index,
             int             *tab_pos,
             PangoTabAlign   *alignment,
             gunichar        *decimal,
             gboolean        *is_default)
{
  PangoLayout *layout = line->layout;
  int n_tabs;
  gboolean in_pixels;
  int offset = 0;

  if (layout->alignment != PANGO_ALIGN_CENTER)
    {
      if (line->is_paragraph_start && layout->indent >= 0)
        offset = layout->indent;
      else if (!line->is_paragraph_start && layout->indent < 0)
        offset = - layout->indent;
    }

  if (layout->tabs)
    {
      n_tabs = pango_tab_array_get_size (layout->tabs);
      in_pixels = pango_tab_array_get_positions_in_pixels (layout->tabs);
      *is_default = FALSE;
    }
  else
    {
      n_tabs = 0;
      in_pixels = FALSE;
      *is_default = TRUE;
    }

  if (index < n_tabs)
    {
      pango_tab_array_get_tab (layout->tabs, index, alignment, tab_pos);

      if (in_pixels)
        *tab_pos *= PANGO_SCALE;

      *decimal = pango_tab_array_get_decimal_point (layout->tabs, index);
    }
  else if (n_tabs > 0)
    {
      /* Extrapolate tab position, repeating the last tab gap to infinity. */
      int last_pos = 0;
      int next_to_last_pos = 0;
      int tab_width;

      pango_tab_array_get_tab (layout->tabs, n_tabs - 1, alignment, &last_pos);
      *decimal = pango_tab_array_get_decimal_point (layout->tabs, n_tabs - 1);

      if (n_tabs > 1)
        pango_tab_array_get_tab (layout->tabs, n_tabs - 2, NULL, &next_to_last_pos);
      else
        next_to_last_pos = 0;

      if (in_pixels)
        {
          next_to_last_pos *= PANGO_SCALE;
          last_pos *= PANGO_SCALE;
        }

      if (last_pos > next_to_last_pos)
        tab_width = last_pos - next_to_last_pos;
      else
        tab_width = layout->tab_width;

      *tab_pos = last_pos + tab_width * (index - n_tabs + 1);
    }
  else
    {
      /* No tab array set, so use default tab width */
      *tab_pos = layout->tab_width * index;
      *alignment = PANGO_TAB_LEFT;
      *decimal = 0;
    }

  *tab_pos -= offset;
}

static void
ensure_decimal (PangoLayout *layout)
{
  if (layout->decimal == 0)
    {
#ifndef __BIONIC__
      layout->decimal = g_utf8_get_char (localeconv ()->decimal_point);
#else
      layout->decimal = g_utf8_get_char (".");
#endif
    }
}

struct _LastTabState {
  PangoGlyphString *glyphs;
  int index;
  int width;
  int pos;
  PangoTabAlign align;
  gunichar decimal;
};

static void
shape_tab (PangoLayoutLine  *line,
           LastTabState     *tab_state,
           ItemProperties   *properties,
           int               current_width,
           PangoItem        *item,
           PangoGlyphString *glyphs)
{
  int i, space_width;
  int tab_pos;
  PangoTabAlign tab_align;
  gunichar tab_decimal;

  pango_glyph_string_set_size (glyphs, 1);

  if (properties->showing_space)
    glyphs->glyphs[0].glyph = PANGO_GET_UNKNOWN_GLYPH ('\t');
  else
    glyphs->glyphs[0].glyph = PANGO_GLYPH_EMPTY;

  glyphs->glyphs[0].geometry.x_offset = 0;
  glyphs->glyphs[0].geometry.y_offset = 0;
  glyphs->glyphs[0].attr.is_cluster_start = 1;
  glyphs->glyphs[0].attr.is_color = 0;

  glyphs->log_clusters[0] = 0;

  ensure_tab_width (line->layout);
  space_width = line->layout->tab_width / 8;

  for (i = tab_state->index; ; i++)
    {
      gboolean is_default;

      get_tab_pos (line, i, &tab_pos, &tab_align, &tab_decimal, &is_default);

      /* Make sure there is at least a space-width of space between
       * tab-aligned text and the text before it. However, only do
       * this if no tab array is set on the layout, ie. using default
       * tab positions. If the user has set tab positions, respect it
       * to the pixel.
       */
      if (tab_pos >= current_width + (is_default ? space_width : 1))
        {
          glyphs->glyphs[0].geometry.width = tab_pos - current_width;
          break;
        }
    }

  if (tab_decimal == 0)
    {
      ensure_decimal (line->layout);
      tab_decimal = line->layout->decimal;
    }

  tab_state->glyphs = glyphs;
  tab_state->index = i;
  tab_state->width = current_width;
  tab_state->pos = tab_pos;
  tab_state->align = tab_align;
  tab_state->decimal = tab_decimal;
}

static inline gboolean
can_break_at (PangoLayout   *layout,
              gint           offset,
              PangoWrapMode  wrap)
{
  if (offset == layout->n_chars)
    return TRUE;
  else if (wrap == PANGO_WRAP_CHAR)
    return layout->log_attrs[offset].is_char_break;
  else
    return layout->log_attrs[offset].is_line_break;
}

static inline gboolean
can_break_in (PangoLayout *layout,
              int          start_offset,
              int          num_chars,
              gboolean     allow_break_at_start)
{
  int i;

  for (i = allow_break_at_start ? 0 : 1; i < num_chars; i++)
    if (can_break_at (layout, start_offset + i, layout->wrap))
      return TRUE;

  return FALSE;
}

static inline void
distribute_letter_spacing (int  letter_spacing,
                           int *space_left,
                           int *space_right)
{
  *space_left = letter_spacing / 2;
  /* hinting */
  if ((letter_spacing & (PANGO_SCALE - 1)) == 0)
    {
      *space_left = PANGO_UNITS_ROUND (*space_left);
    }
  *space_right = letter_spacing - *space_left;
}

typedef enum
{
  BREAK_NONE_FIT,
  BREAK_SOME_FIT,
  BREAK_ALL_FIT,
  BREAK_EMPTY_FIT,
  BREAK_LINE_SEPARATOR
} BreakResult;

struct _ParaBreakState
{
  /* maintained per layout */
  int line_height;              /* Estimate of height of current line; < 0 is no estimate */
  int remaining_height;         /* Remaining height of the layout;  only defined if layout->height >= 0 */

  /* maintained per paragraph */
  PangoAttrList *attrs;         /* Attributes being used for itemization */
  GList *items;                 /* This paragraph turned into items */
  PangoDirection base_dir;      /* Current resolved base direction */
  int line_of_par;              /* Line of the paragraph, starting at 1 for first line */

  PangoGlyphString *glyphs;     /* Glyphs for the first item in state->items */
  int start_offset;             /* Character offset of first item in state->items in layout->text */
  ItemProperties properties;    /* Properties for the first item in state->items */
  int *log_widths;              /* Logical widths for first item in state->items.. */
  int num_log_widths;           /* Length of log_widths */
  int log_widths_offset;        /* Offset into log_widths to the point corresponding
                                 * to the remaining portion of the first item */

  int line_start_index;         /* Start index (byte offset) of line in layout->text */
  int line_start_offset;        /* Character offset of line in layout->text */

  /* maintained per line */
  int line_width;               /* Goal width of line currently processing; < 0 is infinite */
  int remaining_width;          /* Amount of space remaining on line; < 0 is infinite */

  int hyphen_width;             /* How much space a hyphen will take */

  GList *baseline_shifts;

  LastTabState last_tab;
};

static gboolean
should_ellipsize_current_line (PangoLayout    *layout,
                               ParaBreakState *state);

static void
get_decimal_prefix_width (PangoItem        *item,
                          PangoGlyphString *glyphs,
                          const char       *text,
                          gunichar          decimal,
                          int              *width,
                          gboolean         *found)
{
  PangoGlyphItem glyph_item = { item, glyphs, 0, 0, 0 };
  int *log_widths;
  int i;
  const char *p;

  log_widths = g_new (int, item->num_chars);

  pango_glyph_item_get_logical_widths (&glyph_item, text, log_widths);

  *width = 0;
  *found = FALSE;

  for (i = 0, p = text + item->offset; i < item->num_chars; i++, p = g_utf8_next_char (p))
    {
      if (g_utf8_get_char (p) == decimal)
        {
          *width += log_widths[i] / 2;
          *found = TRUE;
          break;
        }

      *width += log_widths[i];
    }

  g_free (log_widths);
}

static int
line_width (ParaBreakState  *state,
            PangoLayoutLine *line)
{
  GSList *l;
  int i;
  int width = 0;

  if (state->remaining_width > -1)
    return state->line_width - state->remaining_width;

  /* Compute the width of the line currently - inefficient, but easier
   * than keeping the current width of the line up to date everywhere
   */
  for (l = line->runs; l; l = l->next)
    {
      PangoLayoutRun *run = l->data;

      for (i = 0; i < run->glyphs->num_glyphs; i++)
        width += run->glyphs->glyphs[i].geometry.width;
    }

  return width;
}

static PangoGlyphString *
shape_run (PangoLayoutLine *line,
           ParaBreakState  *state,
           PangoItem       *item)
{
  PangoLayout *layout = line->layout;
  PangoGlyphString *glyphs = pango_glyph_string_new ();

  if (layout->text[item->offset] == '\t')
    shape_tab (line, &state->last_tab, &state->properties, line_width (state, line), item, glyphs);
  else
    {
      PangoShapeFlags shape_flags = PANGO_SHAPE_NONE;

      if (pango_context_get_round_glyph_positions (layout->context))
        shape_flags |= PANGO_SHAPE_ROUND_POSITIONS;

      if (state->properties.shape_set)
        _pango_shape_shape (layout->text + item->offset, item->num_chars,
                            state->properties.shape_ink_rect, state->properties.shape_logical_rect,
                            glyphs);
      else
        pango_shape_item (item,
                          layout->text, layout->length,
                          layout->log_attrs + state->start_offset,
                          glyphs,
                          shape_flags);

      if (state->properties.letter_spacing)
        {
          PangoGlyphItem glyph_item;
          int space_left, space_right;

          glyph_item.item = item;
          glyph_item.glyphs = glyphs;

          pango_glyph_item_letter_space (&glyph_item,
                                         layout->text,
                                         layout->log_attrs + state->start_offset,
                                         state->properties.letter_spacing);

          distribute_letter_spacing (state->properties.letter_spacing, &space_left, &space_right);

          glyphs->glyphs[0].geometry.width += space_left;
          glyphs->glyphs[0].geometry.x_offset += space_left;
          glyphs->glyphs[glyphs->num_glyphs - 1].geometry.width += space_right;
        }

      if (state->last_tab.glyphs != NULL)
        {
          int w;

          g_assert (state->last_tab.glyphs->num_glyphs == 1);

          /* Update the width of the current tab to position this run properly */

          w = state->last_tab.pos - state->last_tab.width;

          if (state->last_tab.align == PANGO_TAB_RIGHT)
            w -= pango_glyph_string_get_width (glyphs);
          else if (state->last_tab.align == PANGO_TAB_CENTER)
            w -= pango_glyph_string_get_width (glyphs) / 2;
          else if (state->last_tab.align == PANGO_TAB_DECIMAL)
            {
              int width;
              gboolean found;

              get_decimal_prefix_width (item, glyphs, layout->text, state->last_tab.decimal, &width, &found);

              w -= width;
            }

          state->last_tab.glyphs->glyphs[0].geometry.width = MAX (w, 0);
        }
    }

  return glyphs;
}

static void
insert_run (PangoLayoutLine  *line,
            ParaBreakState   *state,
            PangoItem        *run_item,
            PangoGlyphString *glyphs,
            gboolean          last_run)
{
  PangoLayoutRun *run = g_slice_new (PangoLayoutRun);

  run->item = run_item;

  if (glyphs)
    run->glyphs = glyphs;
  else if (last_run && state->log_widths_offset == 0 &&
           !(run_item->analysis.flags & PANGO_ANALYSIS_FLAG_NEED_HYPHEN))
    {
      run->glyphs = state->glyphs;
      state->glyphs = NULL;
    }
  else
    run->glyphs = shape_run (line, state, run_item);

  if (last_run && state->glyphs)
    {
      pango_glyph_string_free (state->glyphs);
      state->glyphs = NULL;
    }

  line->runs = g_slist_prepend (line->runs, run);
  line->length += run_item->length;

  if (state->last_tab.glyphs && run->glyphs != state->last_tab.glyphs)
    {
      gboolean found_decimal = FALSE;
      int width;

      /* Adjust the tab position so placing further runs will continue to
       * maintain the tab placement. In the case of decimal tabs, we are
       * done once we've placed the run with the decimal point.
       */

      if (state->last_tab.align == PANGO_TAB_RIGHT)
        state->last_tab.width += pango_glyph_string_get_width (run->glyphs);
      else if (state->last_tab.align == PANGO_TAB_CENTER)
        state->last_tab.width += pango_glyph_string_get_width (run->glyphs) / 2;
      else if (state->last_tab.align == PANGO_TAB_DECIMAL)
        {
          int width;

          get_decimal_prefix_width (run->item, run->glyphs, line->layout->text, state->last_tab.decimal, &width, &found_decimal);

          state->last_tab.width += width;
        }

      width = MAX (state->last_tab.pos - state->last_tab.width, 0);
      state->last_tab.glyphs->glyphs[0].geometry.width = width;

      if (found_decimal || width == 0)
        state->last_tab.glyphs = NULL;
    }
}

static gboolean
break_needs_hyphen (PangoLayout    *layout,
                    ParaBreakState *state,
                    int             pos)
{
  return layout->log_attrs[state->start_offset + pos].break_inserts_hyphen ||
         layout->log_attrs[state->start_offset + pos].break_removes_preceding;
}

static int
find_hyphen_width (PangoItem *item)
{
  hb_font_t *hb_font;
  hb_codepoint_t glyph;

  if (!item->analysis.font)
    return 0;

  /* This is not technically correct, since
   * a) we may end up inserting a different hyphen
   * b) we should reshape the entire run
   * But it is close enough in practice
   */
  hb_font = pango_font_get_hb_font (item->analysis.font);
  if (hb_font_get_nominal_glyph (hb_font, 0x2010, &glyph) ||
      hb_font_get_nominal_glyph (hb_font, '-', &glyph))
    return hb_font_get_glyph_h_advance (hb_font, glyph);

  return 0;
}

static inline void
ensure_hyphen_width (ParaBreakState *state)
{
  if (state->hyphen_width < 0)
    {
      PangoItem *item = state->items->data;
      state->hyphen_width = find_hyphen_width (item);
    }
}

static int
find_break_extra_width (PangoLayout    *layout,
                        ParaBreakState *state,
                        int             pos)
{
  /* Check whether to insert a hyphen,
   * or whether we are breaking after one of those
   * characters that turn into a hyphen,
   * or after a space.
  */
  if (layout->log_attrs[state->start_offset + pos].break_inserts_hyphen)
    {
      ensure_hyphen_width (state);

      if (layout->log_attrs[state->start_offset + pos].break_removes_preceding && pos > 0)
        return state->hyphen_width - state->log_widths[state->log_widths_offset + pos - 1];
      else
        return state->hyphen_width;
    }
  else if (pos > 0 &&
           layout->log_attrs[state->start_offset + pos - 1].is_white)
    {
      return - state->log_widths[state->log_widths_offset + pos - 1];
    }

  return 0;
}

#if 0
# define DEBUG debug
static int pango_layout_line_get_width (PangoLayoutLine *line);
static void
debug (const char *where, PangoLayoutLine *line, ParaBreakState *state)
{
  int line_width = pango_layout_line_get_width (line);

  g_debug ("rem %d + line %d = %d               %s",
           state->remaining_width,
           line_width,
           state->remaining_width + line_width,
           where);
}
# define DEBUG1(...) g_debug (__VA_ARGS__)
#else
# define DEBUG(where, line, state) do { } while (0)
# define DEBUG1(...) do { } while (0)
#endif

static inline void
compute_log_widths (PangoLayout    *layout,
                    ParaBreakState *state)
{
  PangoItem *item = state->items->data;
  PangoGlyphItem glyph_item = { item, state->glyphs };

  if (item->num_chars > state->num_log_widths)
    {
      state->log_widths = g_renew (int, state->log_widths, item->num_chars);
      state->num_log_widths = item->num_chars;
    }

  pango_glyph_item_get_logical_widths (&glyph_item, layout->text, state->log_widths);
}

/* If last_tab is set, we've added a tab and remaining_width has been updated to
 * account for its origin width, which is last_tab_pos - last_tab_width. shape_run
 * updates the tab width, so we need to consider the delta when comparing
 * against remaining_width.
 */
static int
tab_width_change (ParaBreakState *state)
{
  if (state->last_tab.glyphs)
    return state->last_tab.glyphs->glyphs[0].geometry.width - (state->last_tab.pos - state->last_tab.width);

  return 0;
}

/* Tries to insert as much as possible of the item at the head of
 * state->items onto @line. Five results are possible:
 *
 *  %BREAK_NONE_FIT: Couldn't fit anything.
 *  %BREAK_SOME_FIT: The item was broken in the middle.
 *  %BREAK_ALL_FIT: Everything fit.
 *  %BREAK_EMPTY_FIT: Nothing fit, but that was ok, as we can break at the first char.
 *  %BREAK_LINE_SEPARATOR: Item begins with a line separator.
 *
 * If @force_fit is %TRUE, then %BREAK_NONE_FIT will never
 * be returned, a run will be added even if inserting the minimum amount
 * will cause the line to overflow. This is used at the start of a line
 * and until we've found at least some place to break.
 *
 * If @no_break_at_end is %TRUE, then %BREAK_ALL_FIT will never be
 * returned even everything fits; the run will be broken earlier,
 * or %BREAK_NONE_FIT returned. This is used when the end of the
 * run is not a break position.
 *
 * This function is the core of our line-breaking, and it is long and involved.
 * Here is an outline of the algorithm, without all the bookkeeping:
 *
 * if item appears to fit entirely
 *   measure it
 *   if it actually fits
 *     return BREAK_ALL_FIT
 *
 * retry_break:
 *   for each position p in the item
 *     if adding more is 'obviously' not going to help and we have a breakpoint
 *       exit the loop
 *     if p is a possible break position
 *       if p is 'obviously' going to fit
 *         bc = p
 *       else
 *         measure breaking at p (taking extra break width into account
 *         if we don't have a break candidate yet
 *           bc = p
 *         else
 *           if p is better than bc
 *             bc = p
 *
 *   if bc does not fit and we can loosen break conditions
 *     loosen break conditions and retry break
 *
 * return bc
 */
static BreakResult
process_item (PangoLayout     *layout,
              PangoLayoutLine *line,
              ParaBreakState  *state,
              gboolean         force_fit,
              gboolean         no_break_at_end,
              gboolean         is_last_item)
{
  PangoItem *item = state->items->data;
  gboolean shape_set = FALSE;
  int width;
  int extra_width;
  int orig_extra_width;
  int length;
  int i;
  int processing_new_item;
  int num_chars;
  int orig_width;
  PangoWrapMode wrap;
  int break_num_chars;
  int break_width;
  int break_extra_width;
  PangoGlyphString *break_glyphs;
  PangoFontMetrics *metrics;
  int safe_distance;

  DEBUG1 ("process item '%.*s'. Remaining width %d",
          item->length, layout->text + item->offset,
          state->remaining_width);

  /* We don't want to shape more than necessary, so we keep the results
   * of shaping a new item in state->glyphs, state->log_widths. Once
   * we break off initial parts of the item, we update state->log_widths_offset
   * to take that into account. Note that the widths we calculate from the
   * log_widths are an approximation, because a) log_widths are just
   * evenly divided for clusters, and b) clusters may change as we
   * break in the middle (think ff- i).
   *
   * We use state->log_widths_offset != 0 to detect if we are dealing
   * with the original item, or one that has been chopped off.
   */
  if (!state->glyphs)
    {
      pango_layout_get_item_properties (item, &state->properties);
      state->glyphs = shape_run (line, state, item);
      state->log_widths_offset = 0;
      processing_new_item = TRUE;
    }
  else
    processing_new_item = FALSE;

  /* Only one character has type G_UNICODE_LINE_SEPARATOR in Unicode 5.0;
   * update this if that changes.
   */
#define LINE_SEPARATOR 0x2028

  if (!layout->single_paragraph &&
      g_utf8_get_char (layout->text + item->offset) == LINE_SEPARATOR &&
      !should_ellipsize_current_line (layout, state))
    {
      insert_run (line, state, item, NULL, TRUE);
      state->log_widths_offset += item->num_chars;

      return BREAK_LINE_SEPARATOR;
    }

  if (state->remaining_width < 0 && !no_break_at_end)  /* Wrapping off */
    {
      insert_run (line, state, item, NULL, TRUE);
      DEBUG1 ("no wrapping, all-fit");
      return BREAK_ALL_FIT;
    }

  if (processing_new_item)
    {
      width = pango_glyph_string_get_width (state->glyphs);
    }
  else
    {
      width = 0;
      for (i = 0; i < item->num_chars; i++)
        width += state->log_widths[state->log_widths_offset + i];
    }

  if (layout->text[item->offset] == '\t')
    {
      insert_run (line, state, item, NULL, TRUE);
      state->remaining_width -= width;
      state->remaining_width = MAX (state->remaining_width, 0);

      DEBUG1 ("tab run, all-fit");
      return BREAK_ALL_FIT;
    }

  wrap = layout->wrap;

  if (!no_break_at_end &&
      can_break_at (layout, state->start_offset + item->num_chars, wrap))
    {
      if (processing_new_item)
        {
          compute_log_widths (layout, state);
          processing_new_item = FALSE;
        }

      if (!is_last_item)
        extra_width = find_break_extra_width (layout, state, item->num_chars);
      else
        extra_width = 0;
    }
  else
    extra_width = 0;

  if ((width + extra_width <= state->remaining_width || (item->num_chars == 1 && !line->runs) ||
      (state->last_tab.glyphs && state->last_tab.align != PANGO_TAB_LEFT)) &&
      !no_break_at_end)
    {
      PangoGlyphString *glyphs;

      DEBUG1 ("%d + %d <= %d", width, extra_width, state->remaining_width);
      glyphs = shape_run (line, state, item);

      width = pango_glyph_string_get_width (glyphs) + tab_width_change (state);

      if (width + extra_width <= state->remaining_width || (item->num_chars == 1 && !line->runs))
        {
          insert_run (line, state, item, glyphs, TRUE);

          state->remaining_width -= width;
          state->remaining_width = MAX (state->remaining_width, 0);

          DEBUG1 ("early accept '%.*s', all-fit, remaining %d",
                  item->length, layout->text + item->offset,
                  state->remaining_width);
          return BREAK_ALL_FIT;
        }

      /* if it doesn't fit after shaping, discard and proceed to break the item */
      pango_glyph_string_free (glyphs);
    }

  /*** From here on, we look for a way to break item ***/

  orig_width = width;
  orig_extra_width = extra_width;
  break_width = width;
  break_extra_width = extra_width;
  break_num_chars = item->num_chars;
  wrap = layout->wrap;
  break_glyphs = NULL;

  /* Add some safety margin here. If we are farther away from the end of the
   * line than this, we don't look carefully at a break possibility.
   */
  metrics = pango_font_get_metrics (item->analysis.font, item->analysis.language);
  safe_distance = pango_font_metrics_get_approximate_char_width (metrics) * 3;
  pango_font_metrics_unref (metrics);

  if (processing_new_item)
    {
      compute_log_widths (layout, state);
      processing_new_item = FALSE;
    }

retry_break:

  for (num_chars = 0, width = 0; num_chars < (no_break_at_end ? item->num_chars : (item->num_chars + 1)); num_chars++)
    {
      if (!is_last_item || num_chars != item->num_chars)
        extra_width = find_break_extra_width (layout, state, num_chars);
      else
        extra_width = 0;

      /* We don't want to walk the entire item if we can help it, but
       * we need to keep going at least until we've found a breakpoint
       * that 'works' (as in, it doesn't overflow the budget we have,
       * or there is no hope of finding a better one).
       *
       * We rely on the fact that MIN(width + extra_width, width) is
       * monotonically increasing.
       */

      if (MIN (width + extra_width, width) > state->remaining_width + safe_distance &&
          break_num_chars < item->num_chars)
        {
          DEBUG1 ("at %d, MIN(%d, %d + %d) > %d + MARGIN, breaking at %d",
                  num_chars, width, extra_width, width, state->remaining_width, break_num_chars);
          break;
        }

      /* If there are no previous runs we have to take care to grab at least one char. */
      if (can_break_at (layout, state->start_offset + num_chars, wrap) &&
          (num_chars > 0 || line->runs))
        {
          DEBUG1 ("possible breakpoint: %d, extra_width %d", num_chars, extra_width);
          if (num_chars == 0 ||
              width + extra_width < state->remaining_width - safe_distance)
            {
              DEBUG1 ("trivial accept");
              break_num_chars = num_chars;
              break_width = width;
              break_extra_width = extra_width;
            }
          else
            {
              int length;
              int new_break_width;
              PangoItem *new_item;
              PangoGlyphString *glyphs;

              length = g_utf8_offset_to_pointer (layout->text + item->offset, num_chars) - (layout->text + item->offset);

              if (num_chars < item->num_chars)
                {
                  new_item = pango_item_split (item, length, num_chars);

                  if (break_needs_hyphen (layout, state, num_chars))
                    new_item->analysis.flags |= PANGO_ANALYSIS_FLAG_NEED_HYPHEN;
                  else
                    new_item->analysis.flags &= ~PANGO_ANALYSIS_FLAG_NEED_HYPHEN;
                }
              else
                new_item = item;

              glyphs = shape_run (line, state, new_item);

              new_break_width = pango_glyph_string_get_width (glyphs) + tab_width_change (state);

              if (num_chars > 0 &&
                  (item != new_item || !is_last_item) && /* We don't collapse space at the very end */
                  layout->log_attrs[state->start_offset + num_chars - 1].is_white)
                extra_width = - state->log_widths[state->log_widths_offset + num_chars - 1];
              else if (item == new_item && !is_last_item &&
                       break_needs_hyphen (layout, state, num_chars))
                extra_width = state->hyphen_width;
              else
                extra_width = 0;

              DEBUG1 ("measured breakpoint %d: %d, extra %d", num_chars, new_break_width, extra_width);

              if (new_item != item)
                {
                  pango_item_free (new_item);
                  pango_item_unsplit (item, length, num_chars);
                }

              if (break_num_chars == item->num_chars ||
                  new_break_width + extra_width <= state->remaining_width ||
                  new_break_width + extra_width < break_width + break_extra_width)
                {
                  DEBUG1 ("accept breakpoint %d: %d + %d <= %d + %d",
                          num_chars, new_break_width, extra_width, break_width, break_extra_width);
                  DEBUG1 ("replace bp %d by %d", break_num_chars, num_chars);
                  break_num_chars = num_chars;
                  break_width = new_break_width;
                  break_extra_width = extra_width;

                  if (break_glyphs)
                    pango_glyph_string_free (break_glyphs);
                  break_glyphs = glyphs;
                }
              else
                {
                  DEBUG1 ("ignore breakpoint %d", num_chars);
                  pango_glyph_string_free (glyphs);
                }
            }
        }

      DEBUG1 ("bp now %d", break_num_chars);
      if (num_chars < item->num_chars)
        width += state->log_widths[state->log_widths_offset + num_chars];
    }

   if (wrap == PANGO_WRAP_WORD_CHAR && force_fit && break_width + break_extra_width > state->remaining_width)
    {
      /* Try again, with looser conditions */
      DEBUG1 ("does not fit, try again with wrap-char");
      wrap = PANGO_WRAP_CHAR;
      break_num_chars = item->num_chars;
      break_width = orig_width;
      break_extra_width = orig_extra_width;
      if (break_glyphs)
        pango_glyph_string_free (break_glyphs);
      break_glyphs = NULL;
      goto retry_break;
    }

  if (force_fit || break_width + break_extra_width <= state->remaining_width)       /* Successfully broke the item */
    {
      if (state->remaining_width >= 0)
        {
          state->remaining_width -= break_width + break_extra_width;
          state->remaining_width = MAX (state->remaining_width, 0);
        }

      if (break_num_chars == item->num_chars)
        {
          if (can_break_at (layout, state->start_offset + break_num_chars, wrap) &&
              break_needs_hyphen (layout, state, break_num_chars))
            item->analysis.flags |= PANGO_ANALYSIS_FLAG_NEED_HYPHEN;

          insert_run (line, state, item, NULL, TRUE);

          if (break_glyphs)
            pango_glyph_string_free (break_glyphs);

          DEBUG1 ("all-fit '%.*s', remaining %d",
                  item->length, layout->text + item->offset,
                  state->remaining_width);
          return BREAK_ALL_FIT;
        }
      else if (break_num_chars == 0)
        {
          if (break_glyphs)
            pango_glyph_string_free (break_glyphs);

          DEBUG1 ("empty-fit, remaining %d", state->remaining_width);
          return BREAK_EMPTY_FIT;
        }
      else
        {
          PangoItem *new_item;

          length = g_utf8_offset_to_pointer (layout->text + item->offset, break_num_chars) - (layout->text + item->offset);

          new_item = pango_item_split (item, length, break_num_chars);

          insert_run (line, state, new_item, break_glyphs, FALSE);

          state->log_widths_offset += break_num_chars;

          /* Shaped items should never be broken */
          g_assert (!shape_set);

          DEBUG1 ("some-fit '%.*s', remaining %d",
                  new_item->length, layout->text + new_item->offset,
                  state->remaining_width);
          return BREAK_SOME_FIT;
        }
    }
  else
    {
      pango_glyph_string_free (state->glyphs);
      state->glyphs = NULL;

      if (break_glyphs)
        pango_glyph_string_free (break_glyphs);

      DEBUG1 ("none-fit, remaining %d", state->remaining_width);
      return BREAK_NONE_FIT;
    }
}

/* The resolved direction for the line is always one
 * of LTR/RTL; not a week or neutral directions
 */
static void
line_set_resolved_dir (PangoLayoutLine *line,
                       PangoDirection   direction)
{
  switch (direction)
    {
    default:
    case PANGO_DIRECTION_LTR:
    case PANGO_DIRECTION_TTB_RTL:
    case PANGO_DIRECTION_WEAK_LTR:
    case PANGO_DIRECTION_NEUTRAL:
      line->resolved_dir = PANGO_DIRECTION_LTR;
      break;
    case PANGO_DIRECTION_RTL:
    case PANGO_DIRECTION_WEAK_RTL:
    case PANGO_DIRECTION_TTB_LTR:
      line->resolved_dir = PANGO_DIRECTION_RTL;
      break;
    }

  /* The direction vs. gravity dance:
   *    - If gravity is SOUTH, leave direction untouched.
   *    - If gravity is NORTH, switch direction.
   *    - If gravity is EAST, set to LTR, as
   *      it's a clockwise-rotated layout, so the rotated
   *      top is unrotated left.
   *    - If gravity is WEST, set to RTL, as
   *      it's a counter-clockwise-rotated layout, so the rotated
   *      top is unrotated right.
   *
   * A similar dance is performed in pango-context.c:
   * itemize_state_add_character().  Keep in synch.
   */
  switch (pango_context_get_gravity (line->layout->context))
    {
    default:
    case PANGO_GRAVITY_AUTO:
    case PANGO_GRAVITY_SOUTH:
      break;
    case PANGO_GRAVITY_NORTH:
      line->resolved_dir = PANGO_DIRECTION_LTR
                         + PANGO_DIRECTION_RTL
                         - line->resolved_dir;
      break;
    case PANGO_GRAVITY_EAST:
      /* This is in fact why deprecated TTB_RTL is LTR */
      line->resolved_dir = PANGO_DIRECTION_LTR;
      break;
    case PANGO_GRAVITY_WEST:
      /* This is in fact why deprecated TTB_LTR is RTL */
      line->resolved_dir = PANGO_DIRECTION_RTL;
      break;
    }
}

static gboolean
should_ellipsize_current_line (PangoLayout    *layout,
                               ParaBreakState *state)
{
  if (G_LIKELY (layout->ellipsize == PANGO_ELLIPSIZE_NONE || layout->width < 0))
    return FALSE;

  if (layout->height >= 0)
    {
      /* state->remaining_height is height of layout left */

      /* if we can't stuff two more lines at the current guess of line height,
       * the line we are going to produce is going to be the last line
       */
      return state->line_height * 2 > state->remaining_height;
    }
  else
    {
      /* -layout->height is number of lines per paragraph to show */
      return state->line_of_par == - layout->height;
    }
}

static void
add_line (PangoLayoutLine *line,
          ParaBreakState  *state)
{
  PangoLayout *layout = line->layout;

  /* we prepend, then reverse the list later */
  layout->lines = g_slist_prepend (layout->lines, line);
  layout->line_count++;

  if (layout->height >= 0)
    {
      PangoRectangle logical_rect;
      pango_layout_line_get_extents (line, NULL, &logical_rect);
      state->remaining_height -= logical_rect.height;
      state->remaining_height -= layout->spacing;
      state->line_height = logical_rect.height;
    }
}

static void
process_line (PangoLayout    *layout,
              ParaBreakState *state)
{
  PangoLayoutLine *line;

  gboolean have_break = FALSE;      /* If we've seen a possible break yet */
  int break_remaining_width = 0;    /* Remaining width before adding run with break */
  int break_start_offset = 0;       /* Start offset before adding run with break */
  GSList *break_link = NULL;        /* Link holding run before break */
  gboolean wrapped = FALSE;         /* If we had to wrap the line */

  line = pango_layout_line_new (layout);
  line->start_index = state->line_start_index;
  line->is_paragraph_start = state->line_of_par == 1;
  line_set_resolved_dir (line, state->base_dir);

  state->line_width = layout->width;
  if (state->line_width >= 0 && layout->alignment != PANGO_ALIGN_CENTER)
    {
      if (line->is_paragraph_start && layout->indent >= 0)
        state->line_width -= layout->indent;
      else if (!line->is_paragraph_start && layout->indent < 0)
        state->line_width += layout->indent;

      if (state->line_width < 0)
        state->line_width = 0;
    }

  if (G_UNLIKELY (should_ellipsize_current_line (layout, state)))
    state->remaining_width = -1;
  else
    state->remaining_width = state->line_width;

  state->last_tab.glyphs = NULL;
  state->last_tab.index = 0;
  state->last_tab.align = PANGO_TAB_LEFT;

  DEBUG ("starting to fill line", line, state);

  while (state->items)
    {
      PangoItem *item = state->items->data;
      BreakResult result;
      int old_num_chars;
      int old_remaining_width;
      gboolean first_item_in_line;
      gboolean last_item_in_line;

      old_num_chars = item->num_chars;
      old_remaining_width = state->remaining_width;
      first_item_in_line = line->runs == NULL;
      last_item_in_line = state->items->next == NULL;

      result = process_item (layout, line, state, !have_break, FALSE, last_item_in_line);

      switch (result)
        {
        case BREAK_ALL_FIT:
          if (layout->text[item->offset] != '\t' &&
              can_break_in (layout, state->start_offset, old_num_chars, !first_item_in_line))
            {
              have_break = TRUE;
              break_remaining_width = old_remaining_width;
              break_start_offset = state->start_offset;
              break_link = line->runs->next;
              DEBUG1 ("all-fit, have break");
            }

          state->items = g_list_delete_link (state->items, state->items);
          state->start_offset += old_num_chars;

          break;

        case BREAK_EMPTY_FIT:
          wrapped = TRUE;
          goto done;

        case BREAK_SOME_FIT:
          state->start_offset += old_num_chars - item->num_chars;
          wrapped = TRUE;
          goto done;

        case BREAK_NONE_FIT:
          /* Back up over unused runs to run where there is a break */
          while (line->runs && line->runs != break_link)
            {
              PangoLayoutRun *run = line->runs->data;

              /* If we uninsert the current tab run,
               * we need to reset the tab state
               */
              if (run->glyphs == state->last_tab.glyphs)
                {
                  state->last_tab.glyphs = NULL;
                  state->last_tab.index = 0;
                  state->last_tab.align = PANGO_TAB_LEFT;
                }

              state->items = g_list_prepend (state->items, uninsert_run (line));
            }

          state->start_offset = break_start_offset;
          state->remaining_width = break_remaining_width;
          last_item_in_line = state->items->next == NULL;

          /* Reshape run to break */
          item = state->items->data;

          old_num_chars = item->num_chars;
          result = process_item (layout, line, state, TRUE, TRUE, last_item_in_line);
          g_assert (result == BREAK_SOME_FIT || result == BREAK_EMPTY_FIT);

          state->start_offset += old_num_chars - item->num_chars;

          wrapped = TRUE;
          goto done;

        case BREAK_LINE_SEPARATOR:
          state->items = g_list_delete_link (state->items, state->items);
          state->start_offset += old_num_chars;
          /* A line-separate is just a forced break.  Set wrapped, so we do justification */
          wrapped = TRUE;
          goto done;

        default:
          break;
        }
    }

 done:
  pango_layout_line_postprocess (line, state, wrapped);
  DEBUG1 ("line %d done. remaining %d", state->line_of_par, state->remaining_width);
  add_line (line, state);
  state->line_of_par++;
  state->line_start_index += line->length;
  state->line_start_offset = state->start_offset;
}

static void
get_items_log_attrs (const char    *text,
                     int            start,
                     int            length,
                     GList         *items,
                     PangoAttrList *attrs,
                     PangoLogAttr  *log_attrs,
                     int            log_attrs_len)
{
  int offset = 0;
  GList *l;

  pango_default_break (text + start, length, NULL, log_attrs, log_attrs_len);

  for (l = items; l; l = l->next)
    {
      PangoItem *item = l->data;
      g_assert (item->offset <= start + length);
      g_assert (item->length <= (start + length) - item->offset);

      pango_tailor_break (text + item->offset,
                          item->length,
                          &item->analysis,
                          -1,
                          log_attrs + offset,
                          item->num_chars + 1);

      offset += item->num_chars;
    }

  if (attrs && items)
    {
      PangoItem *item = items->data;
      pango_attr_break (text + start, length, attrs, item->offset, log_attrs, log_attrs_len);
    }
}

static PangoAttrList *
pango_layout_get_effective_attributes (PangoLayout *layout)
{
  PangoAttrList *attrs;

  if (layout->attrs)
    attrs = pango_attr_list_copy (layout->attrs);
  else
    attrs = NULL;

  if (layout->font_desc)
    {
      PangoAttribute *attr = pango_attr_font_desc_new (layout->font_desc);

      if (!attrs)
        attrs = pango_attr_list_new ();

      pango_attr_list_insert_before (attrs, attr);
    }

  if (layout->single_paragraph)
    {
      PangoAttribute *attr = pango_attr_show_new (PANGO_SHOW_LINE_BREAKS);

      if (!attrs)
        attrs = pango_attr_list_new ();

      pango_attr_list_insert_before (attrs, attr);
    }

  return attrs;
}

static gboolean
affects_itemization (PangoAttribute *attr,
                     gpointer        data)
{
  switch ((int)attr->klass->type)
    {
    /* These affect font selection */
    case PANGO_ATTR_LANGUAGE:
    case PANGO_ATTR_FAMILY:
    case PANGO_ATTR_STYLE:
    case PANGO_ATTR_WEIGHT:
    case PANGO_ATTR_VARIANT:
    case PANGO_ATTR_STRETCH:
    case PANGO_ATTR_SIZE:
    case PANGO_ATTR_FONT_DESC:
    case PANGO_ATTR_SCALE:
    case PANGO_ATTR_FALLBACK:
    case PANGO_ATTR_ABSOLUTE_SIZE:
    case PANGO_ATTR_GRAVITY:
    case PANGO_ATTR_GRAVITY_HINT:
    case PANGO_ATTR_FONT_SCALE:
    /* These need to be constant across runs */
    case PANGO_ATTR_LETTER_SPACING:
    case PANGO_ATTR_SHAPE:
    case PANGO_ATTR_RISE:
    case PANGO_ATTR_BASELINE_SHIFT:
    case PANGO_ATTR_LINE_HEIGHT:
    case PANGO_ATTR_ABSOLUTE_LINE_HEIGHT:
    case PANGO_ATTR_TEXT_TRANSFORM:
      return TRUE;
    default:
      return FALSE;
    }
}

static gboolean
affects_break_or_shape (PangoAttribute *attr,
                        gpointer        data)
{
  switch ((int)attr->klass->type)
    {
    /* Affects breaks */
    case PANGO_ATTR_ALLOW_BREAKS:
    case PANGO_ATTR_WORD:
    case PANGO_ATTR_SENTENCE:
    /* Affects shaping */
    case PANGO_ATTR_INSERT_HYPHENS:
    case PANGO_ATTR_FONT_FEATURES:
    case PANGO_ATTR_SHOW:
      return TRUE;
    default:
      return FALSE;
    }
}

static void
apply_attributes_to_items (GList         *items,
                           PangoAttrList *attrs)
{
  GList *l;
  PangoAttrIterator iter;

  if (!attrs)
    return;

  _pango_attr_list_get_iterator (attrs, &iter);

  for (l = items; l; l = l->next)
    {
      PangoItem *item = l->data;
      pango_item_apply_attrs (item, &iter);
    }

  _pango_attr_iterator_destroy (&iter);
}

static void
apply_attributes_to_runs (PangoLayout   *layout,
                          PangoAttrList *attrs)
{
  GSList *ll;

  if (!attrs)
    return;

  for (ll = layout->lines; ll; ll = ll->next)
    {
      PangoLayoutLine *line = ll->data;
      GSList *old_runs = g_slist_reverse (line->runs);
      GSList *rl;

      line->runs = NULL;
      for (rl = old_runs; rl; rl = rl->next)
        {
          PangoGlyphItem *glyph_item = rl->data;
          GSList *new_runs;

          new_runs = pango_glyph_item_apply_attrs (glyph_item,
                                                   layout->text,
                                                   attrs);

          line->runs = g_slist_concat (new_runs, line->runs);
        }

      g_slist_free (old_runs);
    }
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

static void
pango_layout_check_lines (PangoLayout *layout)
{
  const char *start;
  gboolean done = FALSE;
  int start_offset;
  PangoAttrList *attrs;
  PangoAttrList *itemize_attrs;
  PangoAttrList *shape_attrs;
  PangoAttrIterator iter;
  PangoDirection prev_base_dir = PANGO_DIRECTION_NEUTRAL;
  PangoDirection base_dir = PANGO_DIRECTION_NEUTRAL;
  ParaBreakState state;
  gboolean need_log_attrs;

  check_context_changed (layout);

  if (G_LIKELY (layout->lines))
    return;

  /* For simplicity, we make sure at this point that layout->text
   * is non-NULL even if it is zero length
   */
  if (G_UNLIKELY (!layout->text))
    pango_layout_set_text (layout, NULL, 0);

  attrs = pango_layout_get_effective_attributes (layout);
  if (attrs)
    {
      shape_attrs = pango_attr_list_filter (attrs, affects_break_or_shape, NULL);
      itemize_attrs = pango_attr_list_filter (attrs, affects_itemization, NULL);

      if (itemize_attrs)
        _pango_attr_list_get_iterator (itemize_attrs, &iter);
    }
  else
    {
      shape_attrs = NULL;
      itemize_attrs = NULL;
    }

  if (!layout->log_attrs)
    {
      layout->log_attrs = g_new0 (PangoLogAttr, layout->n_chars + 1);
      need_log_attrs = TRUE;
    }
  else
    {
      need_log_attrs = FALSE;
    }

  start_offset = 0;
  start = layout->text;

  /* Find the first strong direction of the text */
  if (layout->auto_dir)
    {
      prev_base_dir = pango_find_base_dir (layout->text, layout->length);
      if (prev_base_dir == PANGO_DIRECTION_NEUTRAL)
        prev_base_dir = pango_context_get_base_dir (layout->context);
    }
  else
    base_dir = pango_context_get_base_dir (layout->context);

  /* these are only used if layout->height >= 0 */
  state.remaining_height = layout->height;
  state.line_height = -1;
  if (layout->height >= 0)
    {
      PangoRectangle logical = { 0, };
      int height = 0;
      pango_layout_get_empty_extents_and_height_at_index (layout, 0, &logical, TRUE, &height);
      state.line_height = layout->line_spacing == 0.0 ? logical.height : layout->line_spacing * height;
    }

  state.log_widths = NULL;
  state.num_log_widths = 0;
  state.baseline_shifts = NULL;

  DEBUG1 ("START layout");
  do
    {
      int delim_len;
      const char *end;
      int delimiter_index, next_para_index;

      if (layout->single_paragraph)
        {
          delimiter_index = layout->length;
          next_para_index = layout->length;
        }
      else
        {
          pango_find_paragraph_boundary (start,
                                         (layout->text + layout->length) - start,
                                         &delimiter_index,
                                         &next_para_index);
        }

      g_assert (next_para_index >= delimiter_index);

      if (layout->auto_dir)
        {
          base_dir = pango_find_base_dir (start, delimiter_index);

          /* Propagate the base direction for neutral paragraphs */
          if (base_dir == PANGO_DIRECTION_NEUTRAL)
            base_dir = prev_base_dir;
          else
            prev_base_dir = base_dir;
        }

      end = start + delimiter_index;

      delim_len = next_para_index - delimiter_index;

      if (end == (layout->text + layout->length))
        done = TRUE;

      g_assert (end <= (layout->text + layout->length));
      g_assert (start <= (layout->text + layout->length));
      g_assert (delim_len < 4); /* PS is 3 bytes */
      g_assert (delim_len >= 0);

      state.attrs = itemize_attrs;
      state.items = pango_itemize_with_font (layout->context,
                                             base_dir,
                                             layout->text,
                                             start - layout->text,
                                             end - start,
                                             itemize_attrs,
                                             itemize_attrs ? &iter : NULL,
                                             NULL);

      apply_attributes_to_items (state.items, shape_attrs);

      if (need_log_attrs)
        get_items_log_attrs (layout->text,
                             start - layout->text,
                             delimiter_index + delim_len,
                             state.items,
                             shape_attrs,
                             layout->log_attrs + start_offset,
                             layout->n_chars + 1 - start_offset);

      state.items = pango_itemize_post_process_items (layout->context,
                                                      layout->text,
                                                      layout->log_attrs,
                                                      state.items);

      state.base_dir = base_dir;
      state.line_of_par = 1;
      state.start_offset = start_offset;
      state.line_start_offset = start_offset;
      state.line_start_index = start - layout->text;

      state.glyphs = NULL;

      /* for deterministic bug hunting's sake set everything! */
      state.line_width = -1;
      state.remaining_width = -1;
      state.log_widths_offset = 0;

      state.hyphen_width = -1;

      if (state.items)
        {
          while (state.items)
            process_line (layout, &state);
        }
      else
        {
          PangoLayoutLine *empty_line;

          empty_line = pango_layout_line_new (layout);
          empty_line->start_index = state.line_start_index;
          empty_line->is_paragraph_start = TRUE;
          line_set_resolved_dir (empty_line, base_dir);

          add_line (empty_line, &state);
        }

      if (layout->height >= 0 && state.remaining_height < state.line_height)
        done = TRUE;

      if (!done)
        start_offset += pango_utf8_strlen (start, (end - start) + delim_len);

      start = end + delim_len;
    }
  while (!done);

  g_free (state.log_widths);
  g_list_free_full (state.baseline_shifts, g_free);

  apply_attributes_to_runs (layout, attrs);
  layout->lines = g_slist_reverse (layout->lines);

  if (itemize_attrs)
    {
      pango_attr_list_unref (itemize_attrs);
      _pango_attr_iterator_destroy (&iter);
    }

  pango_attr_list_unref (shape_attrs);
  pango_attr_list_unref (attrs);

  int w, h;
  pango_layout_get_size (layout, &w, &h);
  DEBUG1 ("DONE %d %d", w, h);
}

#pragma GCC diagnostic pop

/**
 * pango_layout_line_ref:
 * @line: (nullable): a `PangoLayoutLine`
 *
 * Increase the reference count of a `PangoLayoutLine` by one.
 *
 * Returns: (transfer full) (nullable): the line passed in.
 *
 * Since: 1.10
 */
PangoLayoutLine *
pango_layout_line_ref (PangoLayoutLine *line)
{
  PangoLayoutLinePrivate *private = (PangoLayoutLinePrivate *)line;

  if (line == NULL)
    return NULL;

  g_atomic_int_inc ((int *) &private->ref_count);

  return line;
}

/**
 * pango_layout_line_unref:
 * @line: a `PangoLayoutLine`
 *
 * Decrease the reference count of a `PangoLayoutLine` by one.
 *
 * If the result is zero, the line and all associated memory
 * will be freed.
 */
void
pango_layout_line_unref (PangoLayoutLine *line)
{
  PangoLayoutLinePrivate *private = (PangoLayoutLinePrivate *)line;

  if (line == NULL)
    return;

  g_return_if_fail (private->ref_count > 0);

  if (g_atomic_int_dec_and_test ((int *) &private->ref_count))
    {
      g_slist_foreach (line->runs, (GFunc)free_run, GINT_TO_POINTER (1));
      g_slist_free (line->runs);
      g_slice_free (PangoLayoutLinePrivate, private);
    }
}

G_DEFINE_BOXED_TYPE (PangoLayoutLine, pango_layout_line,
                     pango_layout_line_ref,
                     pango_layout_line_unref);

/**
 * pango_layout_line_get_start_index:
 * @line: a `PangoLayoutLine`
 *
 * Returns the start index of the line, as byte index
 * into the text of the layout.
 *
 * Returns: the start index of the line
 *
 * Since: 1.50
 */
int
pango_layout_line_get_start_index (PangoLayoutLine *line)
{
  return line->start_index;
}

/**
 * pango_layout_line_get_length:
 * @line: a `PangoLayoutLine`
 *
 * Returns the length of the line, in bytes.
 *
 * Returns: the length of the line
 *
 * Since: 1.50
 */
int
pango_layout_line_get_length (PangoLayoutLine *line)
{
  return line->length;
}

/**
 * pango_layout_line_is_paragraph_start:
 * @line: a `PangoLayoutLine`
 *
 * Returns whether this is the first line of the paragraph.
 *
 * Returns: %TRUE if this is the first line
 *
 * Since: 1.50
 */
gboolean
pango_layout_line_is_paragraph_start (PangoLayoutLine *line)
{
  return line->is_paragraph_start;
}

/**
 * pango_layout_line_get_resolved_direction:
 * @line: a `PangoLayoutLine`
 *
 * Returns the resolved direction of the line.
 *
 * Returns: the resolved direction of the line
 *
 * Since: 1.50
 */
PangoDirection
pango_layout_line_get_resolved_direction (PangoLayoutLine *line)
{
  return (PangoDirection) line->resolved_dir;
}

/**
 * pango_layout_line_x_to_index:
 * @line: a `PangoLayoutLine`
 * @x_pos: the X offset (in Pango units) from the left edge of the line.
 * @index_: (out): location to store calculated byte index for the grapheme
 *   in which the user clicked
 * @trailing: (out): location to store an integer indicating where in the
 *   grapheme the user clicked. It will either be zero, or the number of
 *   characters in the grapheme. 0 represents the leading edge of the grapheme.
 *
 * Converts from x offset to the byte index of the corresponding character
 * within the text of the layout.
 *
 * If @x_pos is outside the line, @index_ and @trailing will point to the very
 * first or very last position in the line. This determination is based on the
 * resolved direction of the paragraph; for example, if the resolved direction
 * is right-to-left, then an X position to the right of the line (after it)
 * results in 0 being stored in @index_ and @trailing. An X position to the
 * left of the line results in @index_ pointing to the (logical) last grapheme
 * in the line and @trailing being set to the number of characters in that
 * grapheme. The reverse is true for a left-to-right line.
 *
 * Returns: %FALSE if @x_pos was outside the line, %TRUE if inside
 */
gboolean
pango_layout_line_x_to_index (PangoLayoutLine *line,
                              int              x_pos,
                              int             *index,
                              int             *trailing)
{
  GSList *tmp_list;
  gint start_pos = 0;
  gint first_index = 0; /* line->start_index */
  gint first_offset;
  gint last_index;      /* start of last grapheme in line */
  gint last_offset;
  gint end_index;       /* end iterator for line */
  gint end_offset;      /* end iterator for line */
  PangoLayout *layout;
  gint last_trailing;
  gboolean suppress_last_trailing;

  g_return_val_if_fail (LINE_IS_VALID (line), FALSE);

  layout = line->layout;

  /* Find the last index in the line
   */
  first_index = line->start_index;

  if (line->length == 0)
    {
      if (index)
        *index = first_index;
      if (trailing)
        *trailing = 0;

      return FALSE;
    }

  g_assert (line->length > 0);

  first_offset = g_utf8_pointer_to_offset (layout->text, layout->text + line->start_index);

  end_index = first_index + line->length;
  end_offset = first_offset + g_utf8_pointer_to_offset (layout->text + first_index, layout->text + end_index);

  last_index = end_index;
  last_offset = end_offset;
  last_trailing = 0;
  do
    {
      last_index = g_utf8_prev_char (layout->text + last_index) - layout->text;
      last_offset--;
      last_trailing++;
    }
  while (last_offset > first_offset && !layout->log_attrs[last_offset].is_cursor_position);

  /* This is a HACK. If a program only keeps track of cursor (etc)
   * indices and not the trailing flag, then the trailing index of the
   * last character on a wrapped line is identical to the leading
   * index of the next line. So, we fake it and set the trailing flag
   * to zero.
   *
   * That is, if the text is "now is the time", and is broken between
   * 'now' and 'is'
   *
   * Then when the cursor is actually at:
   *
   * n|o|w| |i|s|
   *              ^
   * we lie and say it is at:
   *
   * n|o|w| |i|s|
   *            ^
   *
   * So the cursor won't appear on the next line before 'the'.
   *
   * Actually, any program keeping cursor
   * positions with wrapped lines should distinguish leading and
   * trailing cursors.
   */
  tmp_list = layout->lines;
  while (tmp_list->data != line)
    tmp_list = tmp_list->next;

  if (tmp_list->next &&
      line->start_index + line->length == ((PangoLayoutLine *)tmp_list->next->data)->start_index)
    suppress_last_trailing = TRUE;
  else
    suppress_last_trailing = FALSE;

  if (x_pos < 0)
    {
      /* pick the leftmost char */
      if (index)
        *index = (line->resolved_dir == PANGO_DIRECTION_LTR) ? first_index : last_index;
      /* and its leftmost edge */
      if (trailing)
        *trailing = (line->resolved_dir == PANGO_DIRECTION_LTR || suppress_last_trailing) ? 0 : last_trailing;

      return FALSE;
    }

  tmp_list = line->runs;
  while (tmp_list)
    {
      PangoLayoutRun *run = tmp_list->data;
      int logical_width;

      logical_width = pango_glyph_string_get_width (run->glyphs);

      if (x_pos >= start_pos && x_pos < start_pos + logical_width)
        {
          int offset;
          gboolean char_trailing;
          int grapheme_start_index;
          int grapheme_start_offset;
          int grapheme_end_offset;
          int pos;
          int char_index;

          pango_glyph_string_x_to_index (run->glyphs,
                                         layout->text + run->item->offset, run->item->length,
                                         &run->item->analysis,
                                         x_pos - start_pos,
                                         &pos, &char_trailing);

          char_index = run->item->offset + pos;

          /* Convert from characters to graphemes */

          offset = g_utf8_pointer_to_offset (layout->text, layout->text + char_index);

          grapheme_start_offset = offset;
          grapheme_start_index = char_index;
          while (grapheme_start_offset > first_offset &&
                 !layout->log_attrs[grapheme_start_offset].is_cursor_position)
            {
              grapheme_start_index = g_utf8_prev_char (layout->text + grapheme_start_index) - layout->text;
              grapheme_start_offset--;
            }

          grapheme_end_offset = offset;
          do
            {
              grapheme_end_offset++;
            }
          while (grapheme_end_offset < end_offset &&
                 !layout->log_attrs[grapheme_end_offset].is_cursor_position);

          if (index)
            *index = grapheme_start_index;

          if (trailing)
            {
              if ((grapheme_end_offset == end_offset && suppress_last_trailing) ||
                  offset + char_trailing <= (grapheme_start_offset + grapheme_end_offset) / 2)
                *trailing = 0;
              else
                *trailing = grapheme_end_offset - grapheme_start_offset;
            }

          return TRUE;
        }

      start_pos += logical_width;
      tmp_list = tmp_list->next;
    }

  /* pick the rightmost char */
  if (index)
    *index = (line->resolved_dir == PANGO_DIRECTION_LTR) ? last_index : first_index;

  /* and its rightmost edge */
  if (trailing)
    *trailing = (line->resolved_dir == PANGO_DIRECTION_LTR && !suppress_last_trailing) ? last_trailing : 0;

  return FALSE;
}

static int
pango_layout_line_get_width (PangoLayoutLine *line)
{
  int width = 0;
  GSList *tmp_list = line->runs;

  while (tmp_list)
    {
      PangoLayoutRun *run = tmp_list->data;

      width += pango_glyph_string_get_width (run->glyphs);

      tmp_list = tmp_list->next;
    }

  return width;
}

/**
 * pango_layout_line_get_x_ranges:
 * @line: a `PangoLayoutLine`
 * @start_index: Start byte index of the logical range. If this value
 *   is less than the start index for the line, then the first range
 *   will extend all the way to the leading edge of the layout. Otherwise,
 *   it will start at the leading edge of the first character.
 * @end_index: Ending byte index of the logical range. If this value is
 *   greater than the end index for the line, then the last range will
 *   extend all the way to the trailing edge of the layout. Otherwise,
 *   it will end at the trailing edge of the last character.
 * @ranges: (out) (array length=n_ranges) (transfer full): location to
 *   store a pointer to an array of ranges. The array will be of length
 *   `2*n_ranges`, with each range starting at `(*ranges)[2*n]` and of
 *   width `(*ranges)[2*n + 1] - (*ranges)[2*n]`. This array must be freed
 *   with g_free(). The coordinates are relative to the layout and are in
 *   Pango units.
 * @n_ranges: The number of ranges stored in @ranges
 *
 * Gets a list of visual ranges corresponding to a given logical range.
 *
 * This list is not necessarily minimal - there may be consecutive
 * ranges which are adjacent. The ranges will be sorted from left to
 * right. The ranges are with respect to the left edge of the entire
 * layout, not with respect to the line.
 */
void
pango_layout_line_get_x_ranges (PangoLayoutLine  *line,
                                int               start_index,
                                int               end_index,
                                int             **ranges,
                                int              *n_ranges)
{
  gint line_start_index = 0;
  GSList *tmp_list;
  int range_count = 0;
  int accumulated_width = 0;
  int x_offset;
  int width, line_width;
  PangoAlignment alignment;

  g_return_if_fail (line != NULL);
  g_return_if_fail (line->layout != NULL);
  g_return_if_fail (start_index <= end_index);

  alignment = get_alignment (line->layout, line);

  width = line->layout->width;
  if (width == -1 && alignment != PANGO_ALIGN_LEFT)
    {
      PangoRectangle logical_rect;
      pango_layout_get_extents (line->layout, NULL, &logical_rect);
      width = logical_rect.width;
    }

  /* FIXME: The computations here could be optimized, by moving the
   * computations of the x_offset after we go through and figure
   * out where each range is.
   */

  {
    PangoRectangle logical_rect;
    pango_layout_line_get_extents (line, NULL, &logical_rect);
    line_width = logical_rect.width;
  }

  get_x_offset (line->layout, line, width, line_width, &x_offset);

  line_start_index = line->start_index;

  /* Allocate the maximum possible size */
  if (ranges)
    *ranges = g_new (int, 2 * (2 + g_slist_length (line->runs)));

  if (x_offset > 0 &&
      ((line->resolved_dir == PANGO_DIRECTION_LTR && start_index < line_start_index) ||
       (line->resolved_dir == PANGO_DIRECTION_RTL && end_index > line_start_index + line->length)))
    {
      if (ranges)
        {
          (*ranges)[2*range_count] = 0;
          (*ranges)[2*range_count + 1] = x_offset;
        }

      range_count ++;
    }

  tmp_list = line->runs;
  while (tmp_list)
    {
      PangoLayoutRun *run = (PangoLayoutRun *)tmp_list->data;

      if ((start_index < run->item->offset + run->item->length &&
           end_index > run->item->offset))
        {
          if (ranges)
            {
              int run_start_index = MAX (start_index, run->item->offset);
              int run_end_index = MIN (end_index, run->item->offset + run->item->length);
              int run_start_x, run_end_x;
              int attr_offset;

              g_assert (run_end_index > 0);

              /* Back the end_index off one since we want to find the trailing edge of the preceding character */

              run_end_index = g_utf8_prev_char (line->layout->text + run_end_index) - line->layout->text;

              /* Note: we simply assert here, since our items are all internally
               * created. If that ever changes, we need to add a fallback here.
               */
              g_assert (run->item->analysis.flags & PANGO_ANALYSIS_FLAG_HAS_CHAR_OFFSET);
              attr_offset = ((PangoItemPrivate *)run->item)->char_offset;

              pango_glyph_string_index_to_x_full (run->glyphs,
                                                  line->layout->text + run->item->offset,
                                                  run->item->length,
                                                  &run->item->analysis,
                                                  line->layout->log_attrs + attr_offset,
                                                  run_start_index - run->item->offset, FALSE,
                                                  &run_start_x);
              pango_glyph_string_index_to_x_full (run->glyphs,
                                                  line->layout->text + run->item->offset,
                                                  run->item->length,
                                                  &run->item->analysis,
                                                  line->layout->log_attrs + attr_offset,
                                                  run_end_index - run->item->offset, TRUE,
                                                  &run_end_x);

              (*ranges)[2*range_count] = x_offset + accumulated_width + MIN (run_start_x, run_end_x);
              (*ranges)[2*range_count + 1] = x_offset + accumulated_width + MAX (run_start_x, run_end_x);
            }

          range_count++;
        }

      if (tmp_list->next)
        accumulated_width += pango_glyph_string_get_width (run->glyphs);

      tmp_list = tmp_list->next;
    }

  if (x_offset + line_width < line->layout->width &&
      ((line->resolved_dir == PANGO_DIRECTION_LTR && end_index > line_start_index + line->length) ||
       (line->resolved_dir == PANGO_DIRECTION_RTL && start_index < line_start_index)))
    {
      if (ranges)
        {
          (*ranges)[2*range_count] = x_offset + line_width;
          (*ranges)[2*range_count + 1] = line->layout->width;
        }

      range_count ++;
    }

  if (n_ranges)
    *n_ranges = range_count;
}

static void
pango_layout_get_empty_extents_and_height_at_index (PangoLayout    *layout,
                                                    int             index,
                                                    PangoRectangle *logical_rect,
                                                    gboolean        apply_line_height,
                                                    int             *height)
{
  if (logical_rect)
    {
      PangoFont *font;
      PangoFontDescription *font_desc = NULL;
      gboolean free_font_desc = FALSE;
      double line_height_factor = 0.0;
      int absolute_line_height = 0;

      font_desc = pango_context_get_font_description (layout->context);

      if (layout->font_desc)
        {
          font_desc = pango_font_description_copy_static (font_desc);
          pango_font_description_merge (font_desc, layout->font_desc, TRUE);
          free_font_desc = TRUE;
        }

      /* Find the font description for this line
       */
      if (layout->attrs)
        {
          PangoAttrIterator iter;
          int start, end;

          _pango_attr_list_get_iterator (layout->attrs, &iter);

          do
            {
              pango_attr_iterator_range (&iter, &start, &end);

              if (start <= index && index < end)
                {
                  PangoAttribute *attr;

                  if (!free_font_desc)
                    {
                      font_desc = pango_font_description_copy_static (font_desc);
                      free_font_desc = TRUE;
                    }

                  pango_attr_iterator_get_font (&iter, font_desc, NULL, NULL);

                  attr = pango_attr_iterator_get (&iter, PANGO_ATTR_LINE_HEIGHT);
                  if (attr)
                    line_height_factor = ((PangoAttrFloat *)attr)->value;

                  attr = pango_attr_iterator_get (&iter, PANGO_ATTR_ABSOLUTE_LINE_HEIGHT);
                  if (attr)
                    absolute_line_height = ((PangoAttrInt *)attr)->value;

                  break;
                }

            }
          while (pango_attr_iterator_next (&iter));

          _pango_attr_iterator_destroy (&iter);
        }

      font = pango_context_load_font (layout->context, font_desc);
      if (font)
        {
          PangoFontMetrics *metrics;

          metrics = pango_font_get_metrics (font,
                                            pango_context_get_language (layout->context));

          if (metrics)
            {
              logical_rect->y = - pango_font_metrics_get_ascent (metrics);
              logical_rect->height = - logical_rect->y + pango_font_metrics_get_descent (metrics);
              if (height)
                *height = pango_font_metrics_get_height (metrics);

              pango_font_metrics_unref (metrics);

              if (apply_line_height &&
                  (absolute_line_height != 0 || line_height_factor != 0.0))
                {
                  int line_height, leading;

                  line_height = MAX (absolute_line_height, ceilf (line_height_factor * logical_rect->height));

                  leading = line_height - logical_rect->height;
                  logical_rect->y -= leading / 2;
                  logical_rect->height += leading;
                }
            }
          else
            {
              logical_rect->y = 0;
              logical_rect->height = 0;
            }
          g_object_unref (font);
        }
      else
        {
          logical_rect->y = 0;
          logical_rect->height = 0;
        }

      if (free_font_desc)
        pango_font_description_free (font_desc);

      logical_rect->x = 0;
      logical_rect->width = 0;
    }
}

static void
pango_layout_run_get_extents_and_height (PangoLayoutRun *run,
                                         PangoRectangle *run_ink,
                                         PangoRectangle *run_logical,
                                         PangoRectangle *line_logical,
                                         int            *height)
{
  PangoRectangle logical;
  ItemProperties properties;
  PangoFontMetrics *metrics = NULL;
  gboolean has_underline;
  gboolean has_overline;
  int y_offset;

  if (G_UNLIKELY (!run_ink && !run_logical && !line_logical && !height))
    return;

  pango_layout_get_item_properties (run->item, &properties);

  has_underline = properties.uline_single || properties.uline_double ||
                  properties.uline_low || properties.uline_error;
  has_overline = properties.oline_single;

  if (!run_logical && (run->item->analysis.flags & PANGO_ANALYSIS_FLAG_CENTERED_BASELINE))
    run_logical = &logical;

  if (!run_logical && (has_underline || has_overline || properties.strikethrough))
    run_logical = &logical;

  if (!run_logical && line_logical)
    run_logical = &logical;

  if (properties.shape_set)
    _pango_shape_get_extents (run->item->num_chars,
                              properties.shape_ink_rect,
                              properties.shape_logical_rect,
                              run_ink, run_logical);
  else
    pango_glyph_string_extents (run->glyphs, run->item->analysis.font,
                                run_ink, run_logical);

  if (run_ink && (has_underline || has_overline || properties.strikethrough))
    {
      int underline_thickness;
      int underline_position;
      int strikethrough_thickness;
      int strikethrough_position;
      int new_pos;

      if (!metrics)
        metrics = pango_font_get_metrics (run->item->analysis.font,
                                          run->item->analysis.language);

      underline_thickness = pango_font_metrics_get_underline_thickness (metrics);
      underline_position = pango_font_metrics_get_underline_position (metrics);
      strikethrough_thickness = pango_font_metrics_get_strikethrough_thickness (metrics);
      strikethrough_position = pango_font_metrics_get_strikethrough_position (metrics);

      /* the underline/strikethrough takes x,width of logical_rect.  reflect
       * that into ink_rect.
       */
      new_pos = MIN (run_ink->x, run_logical->x);
      run_ink->width = MAX (run_ink->x + run_ink->width, run_logical->x + run_logical->width) - new_pos;
      run_ink->x = new_pos;

      /* We should better handle the case of height==0 in the following cases.
       * If run_ink->height == 0, we should adjust run_ink->y appropriately.
       */

      if (properties.strikethrough)
        {
          if (run_ink->height == 0)
            {
              run_ink->height = strikethrough_thickness;
              run_ink->y = -strikethrough_position;
            }
        }

      if (properties.oline_single)
        {
          run_ink->y -= underline_thickness;
          run_ink->height += underline_thickness;
        }

      if (properties.uline_low)
        run_ink->height += 2 * underline_thickness;
      if (properties.uline_single)
        run_ink->height = MAX (run_ink->height,
                               underline_thickness - underline_position - run_ink->y);
      if (properties.uline_double)
        run_ink->height = MAX (run_ink->height,
                                 3 * underline_thickness - underline_position - run_ink->y);
      if (properties.uline_error)
        run_ink->height = MAX (run_ink->height,
                               3 * underline_thickness - underline_position - run_ink->y);
    }

  if (height)
    {
      if (pango_analysis_get_size_font (&run->item->analysis))
        {
          PangoFontMetrics *height_metrics;
          double xscale, yscale;

          height_metrics = pango_font_get_metrics (pango_analysis_get_size_font (&run->item->analysis),
                                                   run->item->analysis.language);

          pango_font_get_scale_factors (pango_analysis_get_size_font (&run->item->analysis), &xscale, &yscale);
          *height = pango_font_metrics_get_height (height_metrics) * MAX (xscale, yscale);

          pango_font_metrics_unref (height_metrics);
        }
      else
        {
          double xscale, yscale;

          if (!metrics)
            metrics = pango_font_get_metrics (run->item->analysis.font,
                                              run->item->analysis.language);

          pango_font_get_scale_factors (run->item->analysis.font, &xscale, &yscale);
          *height = pango_font_metrics_get_height (metrics) * MAX (xscale, yscale);
        }
    }

  y_offset = run->y_offset;

  if (run->item->analysis.flags & PANGO_ANALYSIS_FLAG_CENTERED_BASELINE)
    {
      gboolean is_hinted = (run_logical->y & run_logical->height & (PANGO_SCALE - 1)) == 0;
      int adjustment = run_logical->y + run_logical->height / 2;

      if (is_hinted)
        adjustment = PANGO_UNITS_ROUND (adjustment);

      y_offset += adjustment;
    }

  if (run_ink)
    run_ink->y -= y_offset;

  if (run_logical)
    run_logical->y -= y_offset;

  if (line_logical)
    {
      *line_logical = *run_logical;

      if (properties.absolute_line_height != 0 || properties.line_height != 0.0)
        {
          int line_height, leading;

          line_height = MAX (properties.absolute_line_height, ceilf (properties.line_height * line_logical->height));

          leading = line_height - line_logical->height;
          line_logical->y -= leading / 2;
          line_logical->height += leading;
        }
    }

  if (metrics)
    pango_font_metrics_unref (metrics);
}

static void
pango_layout_line_get_extents_and_height (PangoLayoutLine *line,
                                          PangoRectangle  *ink_rect,
                                          PangoRectangle  *logical_rect,
                                          int             *height)
{
  PangoLayoutLinePrivate *private = (PangoLayoutLinePrivate *)line;
  GSList *tmp_list;
  int x_pos = 0;
  gboolean caching = FALSE;

  g_return_if_fail (LINE_IS_VALID (line));

  if (G_UNLIKELY (!ink_rect && !logical_rect && !height))
    return;

  switch (private->cache_status)
    {
    case CACHED:
      if (ink_rect)
        *ink_rect = private->ink_rect;
      if (logical_rect)
        *logical_rect = private->logical_rect;
      if (height)
        *height = private->height;
      return;

    case NOT_CACHED:
      caching = TRUE;
      if (!ink_rect)
        ink_rect = &private->ink_rect;
      if (!logical_rect)
        logical_rect = &private->logical_rect;
      if (!height)
        height = &private->height;
      break;

    case LEAKED:
    default:
      break;
    }

  if (ink_rect)
    {
      ink_rect->x = 0;
      ink_rect->y = 0;
      ink_rect->width = 0;
      ink_rect->height = 0;
    }

  if (logical_rect)
    {
      logical_rect->x = 0;
      logical_rect->y = 0;
      logical_rect->width = 0;
      logical_rect->height = 0;
    }

  if (height)
    *height = 0;

  tmp_list = line->runs;
  while (tmp_list)
    {
      PangoLayoutRun *run = tmp_list->data;
      int new_pos;
      PangoRectangle run_ink;
      PangoRectangle run_logical;
      int run_height;

      pango_layout_run_get_extents_and_height (run,
                                               ink_rect ? &run_ink : NULL,
                                               NULL,
                                               &run_logical,
                                               height ? &run_height : NULL);

      if (ink_rect)
        {
          if (ink_rect->width == 0 || ink_rect->height == 0)
            {
              *ink_rect = run_ink;
              ink_rect->x += x_pos;
            }
          else if (run_ink.width != 0 && run_ink.height != 0)
            {
              new_pos = MIN (ink_rect->x, x_pos + run_ink.x);
              ink_rect->width = MAX (ink_rect->x + ink_rect->width,
                                     x_pos + run_ink.x + run_ink.width) - new_pos;
              ink_rect->x = new_pos;

              new_pos = MIN (ink_rect->y, run_ink.y);
              ink_rect->height = MAX (ink_rect->y + ink_rect->height,
                                      run_ink.y + run_ink.height) - new_pos;
              ink_rect->y = new_pos;
            }
        }

      if (logical_rect)
        {
          new_pos = MIN (logical_rect->x, x_pos + run_logical.x);
          logical_rect->width = MAX (logical_rect->x + logical_rect->width,
                                     x_pos + run_logical.x + run_logical.width) - new_pos;
          logical_rect->x = new_pos;

          new_pos = MIN (logical_rect->y, run_logical.y);
          logical_rect->height = MAX (logical_rect->y + logical_rect->height,
                                      run_logical.y + run_logical.height) - new_pos;
          logical_rect->y = new_pos;
        }

      if (height)
        *height = MAX (*height, abs (run_height));

      x_pos += run_logical.width;
      tmp_list = tmp_list->next;
    }

  if (!line->runs)
    {
      PangoRectangle r, *rect;

      rect = logical_rect ? logical_rect : &r;
      pango_layout_get_empty_extents_and_height_at_index (line->layout, line->start_index, rect, TRUE, height);
    }

  if (caching)
    {
      if (&private->ink_rect != ink_rect)
        private->ink_rect = *ink_rect;
      if (&private->logical_rect != logical_rect)
        private->logical_rect = *logical_rect;
      if (&private->height != height)
        private->height = *height;
      private->cache_status = CACHED;
    }
}

/**
 * pango_layout_line_get_extents:
 * @line: a `PangoLayoutLine`
 * @ink_rect: (out) (optional): rectangle used to store the extents of
 *   the glyph string as drawn
 * @logical_rect: (out) (optional): rectangle used to store the logical
 *   extents of the glyph string
 *
 * Computes the logical and ink extents of a layout line.
 *
 * See [method@Pango.Font.get_glyph_extents] for details
 * about the interpretation of the rectangles.
 */
void
pango_layout_line_get_extents (PangoLayoutLine *line,
                               PangoRectangle  *ink_rect,
                               PangoRectangle  *logical_rect)
{
  pango_layout_line_get_extents_and_height (line, ink_rect, logical_rect, NULL);
}

/**
 * pango_layout_line_get_height:
 * @line: a `PangoLayoutLine`
 * @height: (out) (optional): return location for the line height
 *
 * Computes the height of the line, as the maximum of the heights
 * of fonts used in this line.
 *
 * Note that the actual baseline-to-baseline distance between lines
 * of text is influenced by other factors, such as
 * [method@Pango.Layout.set_spacing] and
 * [method@Pango.Layout.set_line_spacing].
 *
 * Since: 1.44
 */
void
pango_layout_line_get_height (PangoLayoutLine *line,
                              int             *height)
{
  pango_layout_line_get_extents_and_height (line, NULL, NULL, height);
}

static PangoLayoutLine *
pango_layout_line_new (PangoLayout *layout)
{
  PangoLayoutLinePrivate *private = g_slice_new (PangoLayoutLinePrivate);

  private->ref_count = 1;
  private->line.layout = layout;
  private->line.runs = NULL;
  private->line.length = 0;
  private->cache_status = NOT_CACHED;

  /* Note that we leave start_index, resolved_dir, and is_paragraph_start
   *  uninitialized */

  return (PangoLayoutLine *) private;
}

/**
 * pango_layout_line_get_pixel_extents:
 * @layout_line: a `PangoLayoutLine`
 * @ink_rect: (out) (optional): rectangle used to store the extents of
 *   the glyph string as drawn
 * @logical_rect: (out) (optional): rectangle used to store the logical
 *   extents of the glyph string
 *
 * Computes the logical and ink extents of @layout_line in device units.
 *
 * This function just calls [method@Pango.LayoutLine.get_extents] followed by
 * two [func@extents_to_pixels] calls, rounding @ink_rect and @logical_rect
 * such that the rounded rectangles fully contain the unrounded one (that is,
 * passes them as first argument to [func@extents_to_pixels]).
 */
void
pango_layout_line_get_pixel_extents (PangoLayoutLine *layout_line,
                                     PangoRectangle  *ink_rect,
                                     PangoRectangle  *logical_rect)
{
  g_return_if_fail (LINE_IS_VALID (layout_line));

  pango_layout_line_get_extents (layout_line, ink_rect, logical_rect);
  pango_extents_to_pixels (ink_rect, NULL);
  pango_extents_to_pixels (logical_rect, NULL);
}

/*
 * NB: This implement the exact same algorithm as
 *     reorder-items.c:pango_reorder_items().
 */
static GSList *
reorder_runs_recurse (GSList *items,
                      int     n_items)
{
  GSList *tmp_list, *level_start_node;
  int i, level_start_i;
  int min_level = G_MAXINT;
  GSList *result = NULL;

  if (n_items == 0)
    return NULL;

  tmp_list = items;
  for (i=0; i<n_items; i++)
    {
      PangoLayoutRun *run = tmp_list->data;

      min_level = MIN (min_level, run->item->analysis.level);

      tmp_list = tmp_list->next;
    }

  level_start_i = 0;
  level_start_node = items;
  tmp_list = items;
  for (i=0; i<n_items; i++)
    {
      PangoLayoutRun *run = tmp_list->data;

      if (run->item->analysis.level == min_level)
        {
          if (min_level % 2)
            {
              if (i > level_start_i)
                result = g_slist_concat (reorder_runs_recurse (level_start_node, i - level_start_i), result);
              result = g_slist_prepend (result, run);
            }
          else
            {
              if (i > level_start_i)
                result = g_slist_concat (result, reorder_runs_recurse (level_start_node, i - level_start_i));
              result = g_slist_append (result, run);
            }

          level_start_i = i + 1;
          level_start_node = tmp_list->next;
        }

      tmp_list = tmp_list->next;
    }

  if (min_level % 2)
    {
      if (i > level_start_i)
        result = g_slist_concat (reorder_runs_recurse (level_start_node, i - level_start_i), result);
    }
  else
    {
      if (i > level_start_i)
        result = g_slist_concat (result, reorder_runs_recurse (level_start_node, i - level_start_i));
    }

  return result;
}

static void
pango_layout_line_reorder (PangoLayoutLine *line)
{
  GSList *logical_runs = line->runs;
  GSList *tmp_list;
  gboolean all_even, all_odd;
  guint8 level_or = 0, level_and = 1;
  int length = 0;

  /* Check if all items are in the same direction, in that case, the
   * line does not need modification and we can avoid the expensive
   * reorder runs recurse procedure.
   */
  for (tmp_list = logical_runs; tmp_list != NULL; tmp_list = tmp_list->next)
    {
      PangoLayoutRun *run = tmp_list->data;

      level_or |= run->item->analysis.level;
      level_and &= run->item->analysis.level;

      length++;
    }

  /* If none of the levels had the LSB set, all numbers were even. */
  all_even = (level_or & 0x1) == 0;

  /* If all of the levels had the LSB set, all numbers were odd. */
  all_odd = (level_and & 0x1) == 1;

  if (!all_even && !all_odd)
    {
      line->runs = reorder_runs_recurse (logical_runs, length);
      g_slist_free (logical_runs);
    }
  else if (all_odd)
      line->runs = g_slist_reverse (logical_runs);
}

static int
get_item_letter_spacing (PangoItem *item)
{
  ItemProperties properties;

  pango_layout_get_item_properties (item, &properties);

  return properties.letter_spacing;
}

static void
pad_glyphstring_right (PangoGlyphString *glyphs,
                       ParaBreakState   *state,
                       int               adjustment)
{
  int glyph = glyphs->num_glyphs - 1;

  while (glyph >= 0 && glyphs->glyphs[glyph].geometry.width == 0)
    glyph--;

  if (glyph < 0)
    return;

  state->remaining_width -= adjustment;
  glyphs->glyphs[glyph].geometry.width += adjustment;
  if (glyphs->glyphs[glyph].geometry.width < 0)
    {
      state->remaining_width += glyphs->glyphs[glyph].geometry.width;
      glyphs->glyphs[glyph].geometry.width = 0;
    }
}

static void
pad_glyphstring_left (PangoGlyphString *glyphs,
                      ParaBreakState   *state,
                      int               adjustment)
{
  int glyph = 0;

  while (glyph < glyphs->num_glyphs && glyphs->glyphs[glyph].geometry.width == 0)
    glyph++;

  if (glyph == glyphs->num_glyphs)
    return;

  state->remaining_width -= adjustment;
  glyphs->glyphs[glyph].geometry.width += adjustment;
  glyphs->glyphs[glyph].geometry.x_offset += adjustment;
}

static gboolean
is_tab_run (PangoLayout    *layout,
            PangoLayoutRun *run)
{
  return (layout->text[run->item->offset] == '\t');
}

static void
add_missing_hyphen (PangoLayoutLine *line,
                    ParaBreakState  *state,
                    PangoLayoutRun  *run)
{
  PangoLayout *layout = line->layout;
  PangoItem *item = run->item;
  int line_chars;

  line_chars = 0;
  for (GSList *l = line->runs; l; l = l->next)
    {
      PangoLayoutRun *r = l->data;

      if (r)
        line_chars += r->item->num_chars;
    }

  if (layout->log_attrs[state->line_start_offset + line_chars].break_inserts_hyphen &&
      !(item->analysis.flags & PANGO_ANALYSIS_FLAG_NEED_HYPHEN))
    {
      int width;
      int start_offset;

      DEBUG1("add a missing hyphen");
      /* The last run fit onto the line without breaking it, but it still needs a hyphen */

      width = pango_glyph_string_get_width (run->glyphs);

      /* Ugly, shape_run uses state->start_offset, so temporarily rewind things
       * to the state before the run was inserted. Otherwise, we end up passing
       * the wrong log attrs to the shaping machinery.
       */
      start_offset = state->start_offset;
      state->start_offset = state->line_start_offset + line_chars - item->num_chars;

      pango_glyph_string_free (run->glyphs);
      item->analysis.flags |= PANGO_ANALYSIS_FLAG_NEED_HYPHEN;
      run->glyphs = shape_run (line, state, item);

      state->start_offset = start_offset;

      state->remaining_width += pango_glyph_string_get_width (run->glyphs) - width;
    }
}

static PangoShowFlags
find_show_flags (const PangoAnalysis *analysis)
{
  GSList *l;
  PangoShowFlags flags = 0;

  for (l = analysis->extra_attrs; l; l = l->next)
    {
      PangoAttribute *attr = l->data;

      if (attr->klass->type == PANGO_ATTR_SHOW)
        flags |= ((PangoAttrInt*)attr)->value;
    }

  return flags;
}

static void
zero_line_final_space (PangoLayoutLine *line,
                       ParaBreakState  *state,
                       PangoLayoutRun  *run)
{
  PangoLayout *layout = line->layout;
  PangoItem *item = run->item;
  PangoGlyphString *glyphs;
  int glyph;

  glyphs = run->glyphs;
  glyph = item->analysis.level % 2 ? 0 : glyphs->num_glyphs - 1;

  if (glyphs->glyphs[glyph].glyph == PANGO_GET_UNKNOWN_GLYPH (0x2028))
    {
      PangoShowFlags show_flags;

      show_flags = find_show_flags (&item->analysis);

      if ((show_flags & PANGO_SHOW_LINE_BREAKS) != 0)
        {
          DEBUG1 ("zero final space: visible space");
          return; /* this LS is visible */
        }
    }

  /* if the final char of line forms a cluster, and it's
   * a whitespace char, zero its glyph's width as it's been wrapped
   */
  if (glyphs->num_glyphs < 1 || state->start_offset == 0 ||
      !layout->log_attrs[state->start_offset - 1].is_white)
    {
      DEBUG1 ("zero final space: not whitespace");
      return;
    }

  if (glyphs->num_glyphs >= 2 &&
      glyphs->log_clusters[glyph] == glyphs->log_clusters[glyph + (item->analysis.level % 2 ? 1 : -1)])
    {

      DEBUG1 ("zero final space: its a cluster");
      return;
    }

  DEBUG1 ("zero line final space: collapsing the space");
  glyphs->glyphs[glyph].geometry.width = 0;
  glyphs->glyphs[glyph].glyph = PANGO_GLYPH_EMPTY;
}

/* When doing shaping, we add the letter spacing value for a
 * run after every grapheme in the run. This produces ugly
 * asymmetrical results, so what this routine is redistributes
 * that space to the beginning and the end of the run.
 *
 * We also trim the letter spacing from runs adjacent to
 * tabs and from the outside runs of the lines so that things
 * line up properly. The line breaking and tab positioning
 * were computed without this trimming so they are no longer
 * exactly correct, but this won't be very noticeable in most
 * cases.
 */
static void
adjust_line_letter_spacing (PangoLayoutLine *line,
                            ParaBreakState  *state)
{
  PangoLayout *layout = line->layout;
  gboolean reversed;
  PangoLayoutRun *last_run;
  int tab_adjustment;
  GSList *l;

  /* If we have tab stops and the resolved direction of the
   * line is RTL, then we need to walk through the line
   * in reverse direction to figure out the corrections for
   * tab stops.
   */
  reversed = FALSE;
  if (line->resolved_dir == PANGO_DIRECTION_RTL)
    {
      for (l = line->runs; l; l = l->next)
        if (is_tab_run (layout, l->data))
          {
            line->runs = g_slist_reverse (line->runs);
            reversed = TRUE;
            break;
          }
    }

  /* Walk over the runs in the line, redistributing letter
   * spacing from the end of the run to the start of the
   * run and trimming letter spacing from the ends of the
   * runs adjacent to the ends of the line or tab stops.
   *
   * We accumulate a correction factor from this trimming
   * which we add onto the next tab stop space to keep the
   * things properly aligned.
   */
  last_run = NULL;
  tab_adjustment = 0;
  for (l = line->runs; l; l = l->next)
    {
      PangoLayoutRun *run = l->data;
      PangoLayoutRun *next_run = l->next ? l->next->data : NULL;

      if (is_tab_run (layout, run))
        {
          pad_glyphstring_right (run->glyphs, state, tab_adjustment);
          tab_adjustment = 0;
        }
      else
        {
          PangoLayoutRun *visual_next_run = reversed ? last_run : next_run;
          PangoLayoutRun *visual_last_run = reversed ? next_run : last_run;
          int run_spacing = get_item_letter_spacing (run->item);
          int space_left, space_right;

          distribute_letter_spacing (run_spacing, &space_left, &space_right);

          if (run->glyphs->glyphs[0].geometry.width == 0)
            {
              /* we've zeroed this space glyph at the end of line, now remove
               * the letter spacing added to its adjacent glyph */
              pad_glyphstring_left (run->glyphs, state, - space_left);
            }
          else if (!visual_last_run || is_tab_run (layout, visual_last_run))
            {
              pad_glyphstring_left (run->glyphs, state, - space_left);
              tab_adjustment += space_left;
            }

          if (run->glyphs->glyphs[run->glyphs->num_glyphs - 1].geometry.width == 0)
            {
              /* we've zeroed this space glyph at the end of line, now remove
               * the letter spacing added to its adjacent glyph */
              pad_glyphstring_right (run->glyphs, state, - space_right);
            }
          else if (!visual_next_run || is_tab_run (layout, visual_next_run))
            {
              pad_glyphstring_right (run->glyphs, state, - space_right);
              tab_adjustment += space_right;
            }
        }

      last_run = run;
    }

  if (reversed)
    line->runs = g_slist_reverse (line->runs);
}

static void
justify_clusters (PangoLayoutLine *line,
                  ParaBreakState  *state)
{
  const gchar *text = line->layout->text;
  const PangoLogAttr *log_attrs = line->layout->log_attrs;

  int total_remaining_width, total_gaps = 0;
  int added_so_far, gaps_so_far;
  gboolean is_hinted;
  GSList *run_iter;
  enum {
    MEASURE,
    ADJUST
  } mode;

  total_remaining_width = state->remaining_width;
  if (total_remaining_width <= 0)
    return;

  /* hint to full pixel if total remaining width was so */
  is_hinted = (total_remaining_width & (PANGO_SCALE - 1)) == 0;

  for (mode = MEASURE; mode <= ADJUST; mode++)
    {
      gboolean leftedge = TRUE;
      PangoGlyphString *rightmost_glyphs = NULL;
      int rightmost_space = 0;
      int residual = 0;

      added_so_far = 0;
      gaps_so_far = 0;

      for (run_iter = line->runs; run_iter; run_iter = run_iter->next)
        {
          PangoLayoutRun *run = run_iter->data;
          PangoGlyphString *glyphs = run->glyphs;
          PangoGlyphItemIter cluster_iter;
          gboolean have_cluster;
          int dir;
          int offset;

          dir = run->item->analysis.level % 2 == 0 ? +1 : -1;

          /* Note: we simply assert here, since our items are all internally
           * created. If that ever changes, we need to add a fallback here.
           */
          g_assert (run->item->analysis.flags & PANGO_ANALYSIS_FLAG_HAS_CHAR_OFFSET);
          offset = ((PangoItemPrivate *)run->item)->char_offset;

          for (have_cluster = dir > 0 ?
                 pango_glyph_item_iter_init_start (&cluster_iter, run, text) :
                 pango_glyph_item_iter_init_end   (&cluster_iter, run, text);
               have_cluster;
               have_cluster = dir > 0 ?
                 pango_glyph_item_iter_next_cluster (&cluster_iter) :
                 pango_glyph_item_iter_prev_cluster (&cluster_iter))
            {
              int i;
              int width = 0;

              /* don't expand in the middle of graphemes */
              if (!log_attrs[offset + cluster_iter.start_char].is_cursor_position)
                continue;

              for (i = cluster_iter.start_glyph; i != cluster_iter.end_glyph; i += dir)
                width += glyphs->glyphs[i].geometry.width;

              /* also don't expand zero-width clusters. */
              if (width == 0)
                continue;

              gaps_so_far++;

              if (mode == ADJUST)
                {
                  int leftmost, rightmost;
                  int adjustment, space_left, space_right;

                  adjustment = total_remaining_width / total_gaps + residual;
                  if (is_hinted)
                  {
                    int old_adjustment = adjustment;
                    adjustment = PANGO_UNITS_ROUND (adjustment);
                    residual = old_adjustment - adjustment;
                  }
                  /* distribute to before/after */
                  distribute_letter_spacing (adjustment, &space_left, &space_right);

                  if (cluster_iter.start_glyph < cluster_iter.end_glyph)
                  {
                    /* LTR */
                    leftmost  = cluster_iter.start_glyph;
                    rightmost = cluster_iter.end_glyph - 1;
                  }
                  else
                  {
                    /* RTL */
                    leftmost  = cluster_iter.end_glyph + 1;
                    rightmost = cluster_iter.start_glyph;
                  }
                  /* Don't add to left-side of left-most glyph of left-most non-zero run. */
                  if (leftedge)
                    leftedge = FALSE;
                  else
                  {
                    glyphs->glyphs[leftmost].geometry.width    += space_left ;
                    glyphs->glyphs[leftmost].geometry.x_offset += space_left ;
                    added_so_far += space_left;
                  }
                  /* Don't add to right-side of right-most glyph of right-most non-zero run. */
                  {
                    /* Save so we can undo later. */
                    rightmost_glyphs = glyphs;
                    rightmost_space = space_right;

                    glyphs->glyphs[rightmost].geometry.width  += space_right;
                    added_so_far += space_right;
                  }
                }
            }
        }

      if (mode == MEASURE)
        {
          total_gaps = gaps_so_far - 1;

          if (total_gaps == 0)
            {
              /* a single cluster, can't really justify it */
              return;
            }
        }
      else /* mode == ADJUST */
        {
          if (rightmost_glyphs)
           {
             rightmost_glyphs->glyphs[rightmost_glyphs->num_glyphs - 1].geometry.width -= rightmost_space;
             added_so_far -= rightmost_space;
           }
        }
    }

  state->remaining_width -= added_so_far;
}

static void
justify_words (PangoLayoutLine *line,
               ParaBreakState  *state)
{
  const gchar *text = line->layout->text;
  const PangoLogAttr *log_attrs = line->layout->log_attrs;

  int total_remaining_width, total_space_width = 0;
  int added_so_far, spaces_so_far;
  gboolean is_hinted;
  GSList *run_iter;
  enum {
    MEASURE,
    ADJUST
  } mode;

  total_remaining_width = state->remaining_width;
  if (total_remaining_width <= 0)
    return;

  /* hint to full pixel if total remaining width was so */
  is_hinted = (total_remaining_width & (PANGO_SCALE - 1)) == 0;

  for (mode = MEASURE; mode <= ADJUST; mode++)
    {
      added_so_far = 0;
      spaces_so_far = 0;

      for (run_iter = line->runs; run_iter; run_iter = run_iter->next)
        {
          PangoLayoutRun *run = run_iter->data;
          PangoGlyphString *glyphs = run->glyphs;
          PangoGlyphItemIter cluster_iter;
          gboolean have_cluster;
          int offset;

          /* Note: we simply assert here, since our items are all internally
           * created. If that ever changes, we need to add a fallback here.
           */
          g_assert (run->item->analysis.flags & PANGO_ANALYSIS_FLAG_HAS_CHAR_OFFSET);
          offset = ((PangoItemPrivate *)run->item)->char_offset;

          for (have_cluster = pango_glyph_item_iter_init_start (&cluster_iter, run, text);
               have_cluster;
               have_cluster = pango_glyph_item_iter_next_cluster (&cluster_iter))
            {
              int i;
              int dir;

              if (!log_attrs[offset + cluster_iter.start_char].is_expandable_space)
                continue;

              dir = (cluster_iter.start_glyph < cluster_iter.end_glyph) ? 1 : -1;
              for (i = cluster_iter.start_glyph; i != cluster_iter.end_glyph; i += dir)
                {
                  int glyph_width = glyphs->glyphs[i].geometry.width;

                  if (glyph_width == 0)
                    continue;

                  spaces_so_far += glyph_width;

                  if (mode == ADJUST)
                    {
                      int adjustment;

                      adjustment = ((guint64) spaces_so_far * total_remaining_width) / total_space_width - added_so_far;
                      if (is_hinted)
                        adjustment = PANGO_UNITS_ROUND (adjustment);

                      glyphs->glyphs[i].geometry.width += adjustment;
                      added_so_far += adjustment;
                    }
                }
            }
        }

      if (mode == MEASURE)
        {
          total_space_width = spaces_so_far;

          if (total_space_width == 0)
            {
              justify_clusters (line, state);
              return;
            }
        }
    }

  state->remaining_width -= added_so_far;
}

typedef struct {
  PangoAttribute *attr;
  int x_offset;
  int y_offset;
} BaselineItem;

static void
collect_baseline_shift (ParaBreakState *state,
                        PangoItem      *item,
                        PangoItem      *prev,
                        int            *start_x_offset,
                        int            *start_y_offset,
                        int            *end_x_offset,
                        int            *end_y_offset)
{
  *start_x_offset = 0;
  *start_y_offset = 0;
  *end_x_offset = 0;
  *end_y_offset = 0;

  for (GSList *l = item->analysis.extra_attrs; l; l = l->next)
    {
      PangoAttribute *attr = l->data;

      if (attr->klass->type == PANGO_ATTR_RISE)
        {
          int value = ((PangoAttrInt *)attr)->value;

          *start_y_offset += value;
          *end_y_offset -= value;
        }
      else if (attr->klass->type == PANGO_ATTR_BASELINE_SHIFT)
        {
          if (attr->start_index == item->offset)
            {
              BaselineItem *entry;
              int value;

              entry = g_new0 (BaselineItem, 1);
              entry->attr = attr;
              state->baseline_shifts = g_list_prepend (state->baseline_shifts, entry);

              value = ((PangoAttrInt *)attr)->value;

              if (value > 1024 || value < -1024)
                {
                  entry->y_offset = value;
                  /* FIXME: compute an x_offset from value to italic angle */
                }
              else
                {
                  int superscript_x_offset = 0;
                  int superscript_y_offset = 0;
                  int subscript_x_offset = 0;
                  int subscript_y_offset = 0;


                  if (prev && prev->analysis.font)
                    {
                      hb_font_t *hb_font = pango_font_get_hb_font (prev->analysis.font);
                      hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_SUPERSCRIPT_EM_Y_OFFSET, &superscript_y_offset);
                      hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_SUPERSCRIPT_EM_X_OFFSET, &superscript_x_offset);
                      hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_SUBSCRIPT_EM_Y_OFFSET, &subscript_y_offset);
                      hb_ot_metrics_get_position (hb_font, HB_OT_METRICS_TAG_SUBSCRIPT_EM_X_OFFSET, &subscript_x_offset);
                    }

                  if (superscript_y_offset == 0)
                    superscript_y_offset = 5000;
                  if (subscript_y_offset == 0)
                    subscript_y_offset = 5000;

                  switch (value)
                    {
                    case PANGO_BASELINE_SHIFT_NONE:
                      entry->x_offset = 0;
                      entry->y_offset = 0;
                      break;
                    case PANGO_BASELINE_SHIFT_SUPERSCRIPT:
                      entry->x_offset = superscript_x_offset;
                      entry->y_offset = superscript_y_offset;
                      break;
                    case PANGO_BASELINE_SHIFT_SUBSCRIPT:
                      entry->x_offset = subscript_x_offset;
                      entry->y_offset = -subscript_y_offset;
                      break;
                    default:
                      g_assert_not_reached ();
                    }
                }

              *start_x_offset += entry->x_offset;
              *start_y_offset += entry->y_offset;
            }

          if (attr->end_index == item->offset + item->length)
            {
              GList *t;

              for (t = state->baseline_shifts; t; t = t->next)
                {
                  BaselineItem *entry = t->data;

                  if (attr->start_index == entry->attr->start_index &&
                      attr->end_index == entry->attr->end_index &&
                      ((PangoAttrInt *)attr)->value == ((PangoAttrInt *)entry->attr)->value)
                    {
                      *end_x_offset -= entry->x_offset;
                      *end_y_offset -= entry->y_offset;
                    }

                  state->baseline_shifts = g_list_remove (state->baseline_shifts, entry);
                  g_free (entry);
                  break;
                }
              if (t == NULL)
                g_warning ("Baseline attributes mismatch\n");
            }
        }
    }
}

static void
apply_baseline_shift (PangoLayoutLine *line,
                       ParaBreakState  *state)
{
  int y_offset = 0;
  PangoItem *prev = NULL;
  hb_position_t baseline_adjustment = 0;
#if HB_VERSION_ATLEAST(4,0,0)
  hb_ot_layout_baseline_tag_t baseline_tag = 0;
  hb_position_t baseline;
  hb_position_t run_baseline;
#endif

  for (GSList *l = line->runs; l; l = l->next)
    {
      PangoLayoutRun *run = l->data;
      PangoItem *item = run->item;
      int start_x_offset, end_x_offset;
      int start_y_offset, end_y_offset;
#if HB_VERSION_ATLEAST(4,0,0)
      hb_font_t *hb_font;
      hb_script_t script;
      hb_language_t language;
      hb_direction_t direction;
      hb_tag_t script_tags[HB_OT_MAX_TAGS_PER_SCRIPT];
      hb_tag_t lang_tags[HB_OT_MAX_TAGS_PER_LANGUAGE];
      unsigned int script_count = HB_OT_MAX_TAGS_PER_SCRIPT;
      unsigned int lang_count = HB_OT_MAX_TAGS_PER_LANGUAGE;
#endif

      if (item->analysis.font == NULL)
        continue;

#if HB_VERSION_ATLEAST(4,0,0)
      hb_font = pango_font_get_hb_font (item->analysis.font);

      script = (hb_script_t) g_unicode_script_to_iso15924 (item->analysis.script);
      language = hb_language_from_string (pango_language_to_string (item->analysis.language), -1);
      hb_ot_tags_from_script_and_language (script, language,
                                           &script_count, script_tags,
                                           &lang_count, lang_tags);

      if (item->analysis.flags & PANGO_ANALYSIS_FLAG_CENTERED_BASELINE)
        direction = HB_DIRECTION_TTB;
      else
        direction = HB_DIRECTION_LTR;

      if (baseline_tag == 0)
        {
          if (item->analysis.flags & PANGO_ANALYSIS_FLAG_CENTERED_BASELINE)
            baseline_tag = HB_OT_LAYOUT_BASELINE_TAG_IDEO_EMBOX_CENTRAL;
          else
            baseline_tag = hb_ot_layout_get_horizontal_baseline_tag_for_script (script);
          hb_ot_layout_get_baseline_with_fallback (hb_font,
                                                   baseline_tag,
                                                   direction,
                                                   script_tags[script_count - 1],
                                                   lang_count ? lang_tags[lang_count - 1] : HB_TAG_NONE,
                                                   &run_baseline);
          baseline = run_baseline;
        }
      else
        {
          hb_ot_layout_get_baseline_with_fallback (hb_font,
                                                   baseline_tag,
                                                   direction,
                                                   script_tags[script_count - 1],
                                                   lang_count ? lang_tags[lang_count - 1] : HB_TAG_NONE,
                                                   &run_baseline);
        }

      /* Don't do baseline adjustment in vertical, since the renderer
       * is still doing its own baseline shifting there
       */
      if (item->analysis.flags & PANGO_ANALYSIS_FLAG_CENTERED_BASELINE)
        baseline_adjustment = 0;
      else
        baseline_adjustment = baseline - run_baseline;
#endif

      collect_baseline_shift (state, item, prev, &start_x_offset, &start_y_offset, &end_x_offset, &end_y_offset);

      y_offset += start_y_offset + baseline_adjustment;

      run->y_offset = y_offset;
      run->start_x_offset = start_x_offset;
      run->end_x_offset = end_x_offset;

      y_offset += end_y_offset - baseline_adjustment;

      prev = item;
    }
}

static void
pango_layout_line_postprocess (PangoLayoutLine *line,
                               ParaBreakState  *state,
                               gboolean         wrapped)
{
  gboolean ellipsized = FALSE;
  
  DEBUG1 ("postprocessing line, %s", wrapped ? "wrapped" : "not wrapped");

  add_missing_hyphen (line, state, line->runs->data);
  DEBUG ("after hyphen addition", line, state);

  /* Truncate the logical-final whitespace in the line if we broke the line at it */
  if (wrapped)
    zero_line_final_space (line, state, line->runs->data);

  DEBUG ("after removing final space", line, state);

  /* Reverse the runs */
  line->runs = g_slist_reverse (line->runs);

  apply_baseline_shift (line, state);

  /* Ellipsize the line if necessary */
  if (G_UNLIKELY (state->line_width >= 0 &&
                  should_ellipsize_current_line (line->layout, state)))
    {
      PangoShapeFlags shape_flags = PANGO_SHAPE_NONE;

      if (pango_context_get_round_glyph_positions (line->layout->context))
        shape_flags |= PANGO_SHAPE_ROUND_POSITIONS;

      ellipsized = _pango_layout_line_ellipsize (line, state->attrs, shape_flags, state->line_width);
    }

  /* Now convert logical to visual order */
  pango_layout_line_reorder (line);

  DEBUG ("after reordering", line, state);

  /* Fixup letter spacing between runs */
  adjust_line_letter_spacing (line, state);

  DEBUG ("after letter spacing", line, state);

  /* Distribute extra space between words if justifying and line was wrapped */
  if (line->layout->justify && (wrapped || ellipsized || line->layout->justify_last_line))
    {
      /* if we ellipsized, we don't have remaining_width set */
      if (state->remaining_width < 0)
        state->remaining_width = state->line_width - pango_layout_line_get_width (line);

      justify_words (line, state);
    }

  DEBUG ("after justification", line, state);

  line->layout->is_wrapped |= wrapped;
  line->layout->is_ellipsized |= ellipsized;
}

static void
pango_layout_get_item_properties (PangoItem      *item,
                                  ItemProperties *properties)
{
  GSList *tmp_list = item->analysis.extra_attrs;

  properties->uline_single = FALSE;
  properties->uline_double = FALSE;
  properties->uline_low = FALSE;
  properties->uline_error = FALSE;
  properties->oline_single = FALSE;
  properties->strikethrough = FALSE;
  properties->showing_space = FALSE;
  properties->letter_spacing = 0;
  properties->shape_set = FALSE;
  properties->shape_ink_rect = NULL;
  properties->shape_logical_rect = NULL;
  properties->line_height = 0.0;
  properties->absolute_line_height = 0;

  while (tmp_list)
    {
      PangoAttribute *attr = tmp_list->data;

      switch ((int) attr->klass->type)
        {
        case PANGO_ATTR_UNDERLINE:
          switch ((PangoUnderline)(((PangoAttrInt *)attr)->value))
            {
            case PANGO_UNDERLINE_NONE:
              break;
            case PANGO_UNDERLINE_SINGLE:
            case PANGO_UNDERLINE_SINGLE_LINE:
              properties->uline_single = TRUE;
              break;
            case PANGO_UNDERLINE_DOUBLE:
            case PANGO_UNDERLINE_DOUBLE_LINE:
              properties->uline_double = TRUE;
              break;
            case PANGO_UNDERLINE_LOW:
              properties->uline_low = TRUE;
              break;
            case PANGO_UNDERLINE_ERROR:
            case PANGO_UNDERLINE_ERROR_LINE:
              properties->uline_error = TRUE;
              break;
            default:
              g_assert_not_reached ();
              break;
            }
          break;

        case PANGO_ATTR_OVERLINE:
          switch ((PangoOverline)(((PangoAttrInt *)attr)->value))
            {
            case PANGO_OVERLINE_NONE:
              break;
            case PANGO_OVERLINE_SINGLE:
              properties->oline_single = TRUE;
              break;
            default:
              g_assert_not_reached ();
              break;
            }
          break;

        case PANGO_ATTR_STRIKETHROUGH:
          properties->strikethrough = ((PangoAttrInt *)attr)->value;
          break;

        case PANGO_ATTR_LETTER_SPACING:
          properties->letter_spacing = ((PangoAttrInt *)attr)->value;
          break;

        case PANGO_ATTR_SHAPE:
          properties->shape_set = TRUE;
          properties->shape_logical_rect = &((PangoAttrShape *)attr)->logical_rect;
          properties->shape_ink_rect = &((PangoAttrShape *)attr)->ink_rect;
          break;

        case PANGO_ATTR_LINE_HEIGHT:
          properties->line_height = ((PangoAttrFloat *)attr)->value;
          break;

        case PANGO_ATTR_ABSOLUTE_LINE_HEIGHT:
          properties->absolute_line_height = ((PangoAttrInt *)attr)->value;
          break;

        case PANGO_ATTR_SHOW:
          properties->showing_space = (((PangoAttrInt *)attr)->value & PANGO_SHOW_SPACES) != 0;
          break;

        default:
          break;
        }
      tmp_list = tmp_list->next;
    }
}

static int
next_cluster_start (PangoGlyphString *gs,
                    int               cluster_start)
{
  int i;

  i = cluster_start + 1;
  while (i < gs->num_glyphs)
    {
      if (gs->glyphs[i].attr.is_cluster_start)
        return i;

      i++;
    }

  return gs->num_glyphs;
}

static int
cluster_width (PangoGlyphString *gs,
               int               cluster_start)
{
  int i;
  int width;

  width = gs->glyphs[cluster_start].geometry.width;
  i = cluster_start + 1;
  while (i < gs->num_glyphs)
    {
      if (gs->glyphs[i].attr.is_cluster_start)
        break;

      width += gs->glyphs[i].geometry.width;
      i++;
    }

  return width;
}

static inline void
offset_y (PangoLayoutIter *iter,
          int             *y)
{
  *y += iter->line_extents[iter->line_index].baseline;
}

/* Sets up the iter for the start of a new cluster. cluster_start_index
 * is the byte index of the cluster start relative to the run.
 */
static void
update_cluster (PangoLayoutIter *iter,
                int              cluster_start_index)
{
  char             *cluster_text;
  PangoGlyphString *gs;
  int               cluster_length;

  iter->character_position = 0;

  gs = iter->run->glyphs;
  iter->cluster_width = cluster_width (gs, iter->cluster_start);
  iter->next_cluster_glyph = next_cluster_start (gs, iter->cluster_start);

  if (iter->ltr)
    {
      /* For LTR text, finding the length of the cluster is easy
       * since logical and visual runs are in the same direction.
       */
      if (iter->next_cluster_glyph < gs->num_glyphs)
        cluster_length = gs->log_clusters[iter->next_cluster_glyph] - cluster_start_index;
      else
        cluster_length = iter->run->item->length - cluster_start_index;
    }
  else
    {
      /* For RTL text, we have to scan backwards to find the previous
       * visual cluster which is the next logical cluster.
       */
      int i = iter->cluster_start;
      while (i > 0 && gs->log_clusters[i - 1] == cluster_start_index)
        i--;

      if (i == 0)
        cluster_length = iter->run->item->length - cluster_start_index;
      else
        cluster_length = gs->log_clusters[i - 1] - cluster_start_index;
    }

  cluster_text = iter->layout->text + iter->run->item->offset + cluster_start_index;
  iter->cluster_num_chars = pango_utf8_strlen (cluster_text, cluster_length);

  if (iter->ltr)
    iter->index = cluster_text - iter->layout->text;
  else
    iter->index = g_utf8_prev_char (cluster_text + cluster_length) - iter->layout->text;
}

static void
update_run (PangoLayoutIter *iter,
            int              run_start_index)
{
  const Extents *line_ext = &iter->line_extents[iter->line_index];

  /* Note that in iter_new() the iter->run_width
   * is garbage but we don't use it since we're on the first run of
   * a line.
   */
  if (iter->run_list_link == iter->line->runs)
    iter->run_x = line_ext->logical_rect.x;
  else
    {
      iter->run_x += iter->end_x_offset + iter->run_width;
      if (iter->run)
        iter->run_x += iter->run->start_x_offset;
    }

  if (iter->run)
    {
      iter->run_width = pango_glyph_string_get_width (iter->run->glyphs);
      iter->end_x_offset = iter->run->end_x_offset;
    }
  else
    {
      /* The empty run at the end of a line */
      iter->run_width = 0;
      iter->end_x_offset = 0;
    }

  if (iter->run)
    iter->ltr = (iter->run->item->analysis.level % 2) == 0;
  else
    iter->ltr = TRUE;

  iter->cluster_start = 0;
  iter->cluster_x = iter->run_x;

  if (iter->run)
    {
      update_cluster (iter, iter->run->glyphs->log_clusters[0]);
    }
  else
    {
      iter->cluster_width = 0;
      iter->character_position = 0;
      iter->cluster_num_chars = 0;
      iter->index = run_start_index;
    }
}

/**
 * pango_layout_iter_copy:
 * @iter: (nullable): a `PangoLayoutIter`
 *
 * Copies a `PangoLayoutIter`.
 *
 * Returns: (transfer full) (nullable): the newly allocated `PangoLayoutIter`
 *
 * Since: 1.20
 */
PangoLayoutIter *
pango_layout_iter_copy (PangoLayoutIter *iter)
{
  PangoLayoutIter *new;

  if (iter == NULL)
    return NULL;

  new = g_slice_new (PangoLayoutIter);

  new->layout = g_object_ref (iter->layout);
  new->line_list_link = iter->line_list_link;
  new->line = iter->line;
  pango_layout_line_ref (new->line);

  new->run_list_link = iter->run_list_link;
  new->run = iter->run;
  new->index = iter->index;

  new->line_extents = NULL;
  if (iter->line_extents != NULL)
    {
      new->line_extents = g_memdup2 (iter->line_extents,
                                     iter->layout->line_count * sizeof (Extents));

    }
  new->line_index = iter->line_index;

  new->run_x = iter->run_x;
  new->run_width = iter->run_width;
  new->ltr = iter->ltr;

  new->cluster_x = iter->cluster_x;
  new->cluster_width = iter->cluster_width;

  new->cluster_start = iter->cluster_start;
  new->next_cluster_glyph = iter->next_cluster_glyph;

  new->cluster_num_chars = iter->cluster_num_chars;
  new->character_position = iter->character_position;

  new->layout_width = iter->layout_width;

  return new;
}

G_DEFINE_BOXED_TYPE (PangoLayoutIter, pango_layout_iter,
                     pango_layout_iter_copy,
                     pango_layout_iter_free);

/**
 * pango_layout_get_iter:
 * @layout: a `PangoLayout`
 *
 * Returns an iterator to iterate over the visual extents of the layout.
 *
 * Returns: (transfer full): the new `PangoLayoutIter`
 */
PangoLayoutIter*
pango_layout_get_iter (PangoLayout *layout)
{
  PangoLayoutIter *iter;

  g_return_val_if_fail (PANGO_IS_LAYOUT (layout), NULL);

  iter = g_slice_new (PangoLayoutIter);

  _pango_layout_get_iter (layout, iter);

  return iter;
}

void
_pango_layout_get_iter (PangoLayout    *layout,
                        PangoLayoutIter*iter)
{
  int run_start_index;

  g_return_if_fail (PANGO_IS_LAYOUT (layout));

  iter->layout = g_object_ref (layout);

  pango_layout_check_lines (layout);

  iter->line_list_link = layout->lines;
  iter->line = iter->line_list_link->data;
  pango_layout_line_ref (iter->line);

  run_start_index = iter->line->start_index;
  iter->run_list_link = iter->line->runs;

  if (iter->run_list_link)
    {
      iter->run = iter->run_list_link->data;
      run_start_index = iter->run->item->offset;
    }
  else
    iter->run = NULL;

  iter->line_extents = NULL;

  if (layout->width == -1)
    {
      PangoRectangle logical_rect;

      pango_layout_get_extents_internal (layout,
                                         NULL,
                                         &logical_rect,
                                         &iter->line_extents);
      iter->layout_width = logical_rect.width;
    }
  else
    {
      pango_layout_get_extents_internal (layout,
                                         NULL,
                                         NULL,
                                         &iter->line_extents);
      iter->layout_width = layout->width;
    }
  iter->line_index = 0;

  update_run (iter, run_start_index);
}

void
_pango_layout_iter_destroy (PangoLayoutIter *iter)
{
  if (iter == NULL)
    return;

  g_free (iter->line_extents);
  pango_layout_line_unref (iter->line);
  g_object_unref (iter->layout);
}

/**
 * pango_layout_iter_free:
 * @iter: (nullable): a `PangoLayoutIter`, may be %NULL
 *
 * Frees an iterator that's no longer in use.
 **/
void
pango_layout_iter_free (PangoLayoutIter *iter)
{
  if (iter == NULL)
    return;

  _pango_layout_iter_destroy (iter);
  g_slice_free (PangoLayoutIter, iter);
}

/**
 * pango_layout_iter_get_index:
 * @iter: a `PangoLayoutIter`
 *
 * Gets the current byte index.
 *
 * Note that iterating forward by char moves in visual order,
 * not logical order, so indexes may not be sequential. Also,
 * the index may be equal to the length of the text in the
 * layout, if on the %NULL run (see [method@Pango.LayoutIter.get_run]).
 *
 * Returns: current byte index
 */
int
pango_layout_iter_get_index (PangoLayoutIter *iter)
{
  if (ITER_IS_INVALID (iter))
    return 0;

  return iter->index;
}

/**
 * pango_layout_iter_get_run:
 * @iter: a `PangoLayoutIter`
 *
 * Gets the current run.
 *
 * When iterating by run, at the end of each line, there's a position
 * with a %NULL run, so this function can return %NULL. The %NULL run
 * at the end of each line ensures that all lines have at least one run,
 * even lines consisting of only a newline.
 *
 * Use the faster [method@Pango.LayoutIter.get_run_readonly] if you do not
 * plan to modify the contents of the run (glyphs, glyph widths, etc.).
 *
 * Returns: (transfer none) (nullable): the current run
 */
PangoLayoutRun*
pango_layout_iter_get_run (PangoLayoutIter *iter)
{
  if (ITER_IS_INVALID (iter))
    return NULL;

  pango_layout_line_leaked (iter->line);

  return iter->run;
}

/**
 * pango_layout_iter_get_run_readonly:
 * @iter: a `PangoLayoutIter`
 *
 * Gets the current run for read-only access.
 *
 * When iterating by run, at the end of each line, there's a position
 * with a %NULL run, so this function can return %NULL. The %NULL run
 * at the end of each line ensures that all lines have at least one run,
 * even lines consisting of only a newline.
 *
 * This is a faster alternative to [method@Pango.LayoutIter.get_run],
 * but the user is not expected to modify the contents of the run (glyphs,
 * glyph widths, etc.).
 *
 * Returns: (transfer none) (nullable): the current run, that
 *   should not be modified
 *
 * Since: 1.16
 */
PangoLayoutRun*
pango_layout_iter_get_run_readonly (PangoLayoutIter *iter)
{
  if (ITER_IS_INVALID (iter))
    return NULL;

  pango_layout_line_leaked (iter->line);

  return iter->run;
}

/* an inline-able version for local use */
static PangoLayoutLine*
_pango_layout_iter_get_line (PangoLayoutIter *iter)
{
  return iter->line;
}

static PangoLayoutRun *
_pango_layout_iter_get_run (PangoLayoutIter *iter)
{
  return iter->run;
}

/**
 * pango_layout_iter_get_line:
 * @iter: a `PangoLayoutIter`
 *
 * Gets the current line.
 *
 * Use the faster [method@Pango.LayoutIter.get_line_readonly] if
 * you do not plan to modify the contents of the line (glyphs,
 * glyph widths, etc.).
 *
 * Returns: (transfer none) (nullable): the current line
 */
PangoLayoutLine*
pango_layout_iter_get_line (PangoLayoutIter *iter)
{
  if (ITER_IS_INVALID (iter))
    return NULL;

  pango_layout_line_leaked (iter->line);

  return iter->line;
}

/**
 * pango_layout_iter_get_line_readonly:
 * @iter: a `PangoLayoutIter`
 *
 * Gets the current line for read-only access.
 *
 * This is a faster alternative to [method@Pango.LayoutIter.get_line],
 * but the user is not expected to modify the contents of the line
 * (glyphs, glyph widths, etc.).
 *
 * Returns: (transfer none) (nullable): the current line, that should not be
 *   modified
 *
 * Since: 1.16
 */
PangoLayoutLine*
pango_layout_iter_get_line_readonly (PangoLayoutIter *iter)
{
  if (ITER_IS_INVALID (iter))
    return NULL;

  return iter->line;
}

/**
 * pango_layout_iter_at_last_line:
 * @iter: a `PangoLayoutIter`
 *
 * Determines whether @iter is on the last line of the layout.
 *
 * Returns: %TRUE if @iter is on the last line
 */
gboolean
pango_layout_iter_at_last_line (PangoLayoutIter *iter)
{
  if (ITER_IS_INVALID (iter))
    return FALSE;

  return iter->line_index == iter->layout->line_count - 1;
}

/**
 * pango_layout_iter_get_layout:
 * @iter: a `PangoLayoutIter`
 *
 * Gets the layout associated with a `PangoLayoutIter`.
 *
 * Returns: (transfer none) (nullable): the layout associated with @iter
 *
 * Since: 1.20
 */
PangoLayout*
pango_layout_iter_get_layout (PangoLayoutIter *iter)
{
  /* check is redundant as it simply checks that iter->layout is not NULL */
  if (ITER_IS_INVALID (iter))
    return NULL;

  return iter->layout;
}

static gboolean
line_is_terminated (PangoLayoutIter *iter)
{
  /* There is a real terminator at the end of each paragraph other
   * than the last.
   */
  if (iter->line_list_link->next)
    {
      PangoLayoutLine *next_line = iter->line_list_link->next->data;
      if (next_line->is_paragraph_start)
        return TRUE;
    }

  return FALSE;
}

/* Moves to the next non-empty line. If @include_terminators
 * is set, a line with just an explicit paragraph separator
 * is considered non-empty.
 */
static gboolean
next_nonempty_line (PangoLayoutIter *iter,
                    gboolean         include_terminators)
{
  gboolean result;

  while (TRUE)
    {
      result = pango_layout_iter_next_line (iter);
      if (!result)
        break;

      if (iter->line->runs)
        break;

      if (include_terminators && line_is_terminated (iter))
        break;
    }

  return result;
}

/* Moves to the next non-empty run. If @include_terminators
 * is set, the trailing run at the end of a line with an explicit
 * paragraph separator is considered non-empty.
 */
static gboolean
next_nonempty_run (PangoLayoutIter *iter,
                    gboolean         include_terminators)
{
  gboolean result;

  while (TRUE)
    {
      result = pango_layout_iter_next_run (iter);
      if (!result)
        break;

      if (iter->run)
        break;

      if (include_terminators && line_is_terminated (iter))
        break;
    }

  return result;
}

/* Like pango_layout_next_cluster(), but if @include_terminators
 * is set, includes the fake runs/clusters for empty lines.
 * (But not positions introduced by line wrapping).
 */
static gboolean
next_cluster_internal (PangoLayoutIter *iter,
                       gboolean         include_terminators)
{
  PangoGlyphString *gs;
  int               next_start;

  if (ITER_IS_INVALID (iter))
    return FALSE;

  if (iter->run == NULL)
    return next_nonempty_line (iter, include_terminators);

  gs = iter->run->glyphs;

  next_start = iter->next_cluster_glyph;
  if (next_start == gs->num_glyphs)
    {
      return next_nonempty_run (iter, include_terminators);
    }
  else
    {
      iter->cluster_start = next_start;
      iter->cluster_x += iter->cluster_width;
      update_cluster(iter, gs->log_clusters[iter->cluster_start]);

      return TRUE;
    }
}

/**
 * pango_layout_iter_next_char:
 * @iter: a `PangoLayoutIter`
 *
 * Moves @iter forward to the next character in visual order.
 *
 * If @iter was already at the end of the layout, returns %FALSE.
 *
 * Returns: whether motion was possible
 */
gboolean
pango_layout_iter_next_char (PangoLayoutIter *iter)
{
  const char *text;

  if (ITER_IS_INVALID (iter))
    return FALSE;

  if (iter->run == NULL)
    {
      /* We need to fake an iterator position in the middle of a \r\n line terminator */
      if (line_is_terminated (iter) &&
          strncmp (iter->layout->text + iter->line->start_index + iter->line->length, "\r\n", 2) == 0 &&
          iter->character_position == 0)
        {
          iter->character_position++;
          return TRUE;
        }

      return next_nonempty_line (iter, TRUE);
    }

  iter->character_position++;
  if (iter->character_position >= iter->cluster_num_chars)
    return next_cluster_internal (iter, TRUE);

  text = iter->layout->text;
  if (iter->ltr)
    iter->index = g_utf8_next_char (text + iter->index) - text;
  else
    iter->index = g_utf8_prev_char (text + iter->index) - text;

  return TRUE;
}

/**
 * pango_layout_iter_next_cluster:
 * @iter: a `PangoLayoutIter`
 *
 * Moves @iter forward to the next cluster in visual order.
 *
 * If @iter was already at the end of the layout, returns %FALSE.
 *
 * Returns: whether motion was possible
 */
gboolean
pango_layout_iter_next_cluster (PangoLayoutIter *iter)
{
  return next_cluster_internal (iter, FALSE);
}

/**
 * pango_layout_iter_next_run:
 * @iter: a `PangoLayoutIter`
 *
 * Moves @iter forward to the next run in visual order.
 *
 * If @iter was already at the end of the layout, returns %FALSE.
 *
 * Returns: whether motion was possible
 */
gboolean
pango_layout_iter_next_run (PangoLayoutIter *iter)
{
  int next_run_start; /* byte index */
  GSList *next_link;

  if (ITER_IS_INVALID (iter))
    return FALSE;

  if (iter->run == NULL)
    return pango_layout_iter_next_line (iter);

  next_link = iter->run_list_link->next;

  if (next_link == NULL)
    {
      /* Moving on to the zero-width "virtual run" at the end of each
       * line
       */
      next_run_start = iter->run->item->offset + iter->run->item->length;
      iter->run = NULL;
      iter->run_list_link = NULL;
    }
  else
    {
      iter->run_list_link = next_link;
      iter->run = iter->run_list_link->data;
      next_run_start = iter->run->item->offset;
    }

  update_run (iter, next_run_start);

  return TRUE;
}

/**
 * pango_layout_iter_next_line:
 * @iter: a `PangoLayoutIter`
 *
 * Moves @iter forward to the start of the next line.
 *
 * If @iter is already on the last line, returns %FALSE.
 *
 * Returns: whether motion was possible
 */
gboolean
pango_layout_iter_next_line (PangoLayoutIter *iter)
{
  GSList *next_link;

  if (ITER_IS_INVALID (iter))
    return FALSE;

  next_link = iter->line_list_link->next;

  if (next_link == NULL)
    return FALSE;

  iter->line_list_link = next_link;

  pango_layout_line_unref (iter->line);

  iter->line = iter->line_list_link->data;

  pango_layout_line_ref (iter->line);

  iter->run_list_link = iter->line->runs;

  if (iter->run_list_link)
    iter->run = iter->run_list_link->data;
  else
    iter->run = NULL;

  iter->line_index ++;

  update_run (iter, iter->line->start_index);

  return TRUE;
}

/**
 * pango_layout_iter_get_char_extents:
 * @iter: a `PangoLayoutIter`
 * @logical_rect: (out caller-allocates): rectangle to fill with
 *   logical extents
 *
 * Gets the extents of the current character, in layout coordinates.
 *
 * Layout coordinates have the origin at the top left of the entire layout.
 *
 * Only logical extents can sensibly be obtained for characters;
 * ink extents make sense only down to the level of clusters.
 */
void
pango_layout_iter_get_char_extents (PangoLayoutIter *iter,
                                    PangoRectangle  *logical_rect)
{
  PangoRectangle cluster_rect;
  int            x0, x1;

  if (ITER_IS_INVALID (iter))
    return;

  if (logical_rect == NULL)
    return;

  pango_layout_iter_get_cluster_extents (iter, NULL, &cluster_rect);

  if (iter->run == NULL)
    {
      /* When on the NULL run, cluster, char, and run all have the
       * same extents
       */
      *logical_rect = cluster_rect;
      return;
    }

  if (iter->cluster_num_chars)
  {
    x0 = (iter->character_position * cluster_rect.width) / iter->cluster_num_chars;
    x1 = ((iter->character_position + 1) * cluster_rect.width) / iter->cluster_num_chars;
  }
  else
  {
    x0 = x1 = 0;
  }

  logical_rect->width = x1 - x0;
  logical_rect->height = cluster_rect.height;
  logical_rect->y = cluster_rect.y;
  logical_rect->x = cluster_rect.x + x0;
}

/**
 * pango_layout_iter_get_cluster_extents:
 * @iter: a `PangoLayoutIter`
 * @ink_rect: (out) (optional): rectangle to fill with ink extents
 * @logical_rect: (out) (optional): rectangle to fill with logical extents
 *
 * Gets the extents of the current cluster, in layout coordinates.
 *
 * Layout coordinates have the origin at the top left of the entire layout.
 */
void
pango_layout_iter_get_cluster_extents (PangoLayoutIter *iter,
                                       PangoRectangle  *ink_rect,
                                       PangoRectangle  *logical_rect)
{
  if (ITER_IS_INVALID (iter))
    return;

  if (iter->run == NULL)
    {
      /* When on the NULL run, cluster, char, and run all have the
       * same extents
       */
      pango_layout_iter_get_run_extents (iter, ink_rect, logical_rect);
      return;
    }

  pango_glyph_string_extents_range (iter->run->glyphs,
                                    iter->cluster_start,
                                    iter->next_cluster_glyph,
                                    iter->run->item->analysis.font,
                                    ink_rect,
                                    logical_rect);

  if (ink_rect)
    {
      ink_rect->x += iter->cluster_x + iter->run->start_x_offset;
      ink_rect->y -= iter->run->y_offset;
      offset_y (iter, &ink_rect->y);
    }

  if (logical_rect)
    {
      g_assert (logical_rect->width == iter->cluster_width);
      logical_rect->x += iter->cluster_x + iter->run->start_x_offset;
      logical_rect->y -= iter->run->y_offset;
      offset_y (iter, &logical_rect->y);
    }
}

/**
 * pango_layout_iter_get_run_extents:
 * @iter: a `PangoLayoutIter`
 * @ink_rect: (out) (optional): rectangle to fill with ink extents
 * @logical_rect: (out) (optional): rectangle to fill with logical extents
 *
 * Gets the extents of the current run in layout coordinates.
 *
 * Layout coordinates have the origin at the top left of the entire layout.
 */
void
pango_layout_iter_get_run_extents (PangoLayoutIter *iter,
                                   PangoRectangle  *ink_rect,
                                   PangoRectangle  *logical_rect)
{
  if (G_UNLIKELY (!ink_rect && !logical_rect))
    return;

  if (ITER_IS_INVALID (iter))
    return;

  if (iter->run)
    {
      pango_layout_run_get_extents_and_height (iter->run, ink_rect, logical_rect, NULL, NULL);

      if (ink_rect)
        {
          offset_y (iter, &ink_rect->y);
          ink_rect->x += iter->run_x;
        }

      if (logical_rect)
        {
          offset_y (iter, &logical_rect->y);
          logical_rect->x += iter->run_x;
        }
    }
  else
    {
      if (iter->line->runs)
        {
          /* The empty run at the end of a non-empty line */
          PangoLayoutRun *run = g_slist_last (iter->line->runs)->data;
          pango_layout_run_get_extents_and_height (run, ink_rect, logical_rect, NULL, NULL);
        }
      else
        {
          PangoRectangle r;

          pango_layout_get_empty_extents_and_height_at_index (iter->layout, 0, &r, FALSE, NULL);

          if (ink_rect)
            *ink_rect = r;

          if (logical_rect)
            *logical_rect = r;
        }

      if (ink_rect)
        {
          offset_y (iter, &ink_rect->y);
          ink_rect->x = iter->run_x;
          ink_rect->width = 0;
        }

      if (logical_rect)
        {
          offset_y (iter, &logical_rect->y);
          logical_rect->x = iter->run_x;
          logical_rect->width = 0;
        }
    }
}

/**
 * pango_layout_iter_get_line_extents:
 * @iter: a `PangoLayoutIter`
 * @ink_rect: (out) (optional): rectangle to fill with ink extents
 * @logical_rect: (out) (optional): rectangle to fill with logical extents
 *
 * Obtains the extents of the current line.
 *
 * Extents are in layout coordinates (origin is the top-left corner
 * of the entire `PangoLayout`). Thus the extents returned by this
 * function will be the same width/height but not at the same x/y
 * as the extents returned from [method@Pango.LayoutLine.get_extents].
 */
void
pango_layout_iter_get_line_extents (PangoLayoutIter *iter,
                                    PangoRectangle  *ink_rect,
                                    PangoRectangle  *logical_rect)
{
  const Extents *ext;

  if (ITER_IS_INVALID (iter))
    return;

  ext = &iter->line_extents[iter->line_index];

  if (ink_rect)
    {
      get_line_extents_layout_coords (iter->layout, iter->line,
                                      iter->layout_width,
                                      ext->logical_rect.y,
                                      NULL,
                                      ink_rect,
                                      NULL);
    }

  if (logical_rect)
    *logical_rect = ext->logical_rect;
}

/**
 * pango_layout_iter_get_line_yrange:
 * @iter: a `PangoLayoutIter`
 * @y0_: (out) (optional): start of line
 * @y1_: (out) (optional): end of line
 *
 * Divides the vertical space in the `PangoLayout` being iterated over
 * between the lines in the layout, and returns the space belonging to
 * the current line.
 *
 * A line's range includes the line's logical extents. plus half of the
 * spacing above and below the line, if [method@Pango.Layout.set_spacing]
 * has been called to set layout spacing. The Y positions are in layout
 * coordinates (origin at top left of the entire layout).
 *
 * Note: Since 1.44, Pango uses line heights for placing lines, and there
 * may be gaps between the ranges returned by this function.
 */
void
pango_layout_iter_get_line_yrange (PangoLayoutIter *iter,
                                   int             *y0,
                                   int             *y1)
{
  const Extents *ext;
  int half_spacing;

  if (ITER_IS_INVALID (iter))
    return;

  ext = &iter->line_extents[iter->line_index];

  half_spacing = iter->layout->spacing / 2;

  /* Note that if layout->spacing is odd, the remainder spacing goes
   * above the line (this is pretty arbitrary of course)
   */

  if (y0)
    {
      /* No spacing above the first line */

      if (iter->line_index == 0)
        *y0 = ext->logical_rect.y;
      else
        *y0 = ext->logical_rect.y - (iter->layout->spacing - half_spacing);
    }

  if (y1)
    {
      /* No spacing below the last line */
      if (iter->line_index == iter->layout->line_count - 1)
        *y1 = ext->logical_rect.y + ext->logical_rect.height;
      else
        *y1 = ext->logical_rect.y + ext->logical_rect.height + half_spacing;
    }
}

/**
 * pango_layout_iter_get_baseline:
 * @iter: a `PangoLayoutIter`
 *
 * Gets the Y position of the current line's baseline, in layout
 * coordinates.
 *
 * Layout coordinates have the origin at the top left of the entire layout.
 *
 * Returns: baseline of current line
 */
int
pango_layout_iter_get_baseline (PangoLayoutIter *iter)
{
  if (ITER_IS_INVALID (iter))
    return 0;

  return iter->line_extents[iter->line_index].baseline;
}

/**
 * pango_layout_iter_get_run_baseline:
 * @iter: a `PangoLayoutIter`
 * 
 * Gets the Y position of the current run's baseline, in layout
 * coordinates.
 *
 * Layout coordinates have the origin at the top left of the entire layout.
 *
 * The run baseline can be different from the line baseline, for
 * example due to superscript or subscript positioning.
 *
 * Since: 1.50
 */
int
pango_layout_iter_get_run_baseline (PangoLayoutIter *iter)
{
  if (ITER_IS_INVALID (iter))
    return 0;

  if (!iter->run)
    return iter->line_extents[iter->line_index].baseline;

  return iter->line_extents[iter->line_index].baseline - iter->run->y_offset;
}

/**
 * pango_layout_iter_get_layout_extents:
 * @iter: a `PangoLayoutIter`
 * @ink_rect: (out) (optional): rectangle to fill with ink extents
 * @logical_rect: (out) (optional): rectangle to fill with logical extents
 *
 * Obtains the extents of the `PangoLayout` being iterated over.
 */
void
pango_layout_iter_get_layout_extents (PangoLayoutIter *iter,
                                      PangoRectangle  *ink_rect,
                                      PangoRectangle  *logical_rect)
{
  if (ITER_IS_INVALID (iter))
    return;

  pango_layout_get_extents (iter->layout, ink_rect, logical_rect);
}
