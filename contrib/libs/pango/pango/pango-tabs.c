/* Pango
 * pango-tabs.c: Tab-related stuff
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
#include "pango-tabs.h"
#include "pango-impl-utils.h"
#include <stdlib.h>
#include <string.h>

typedef struct _PangoTab PangoTab;

struct _PangoTab
{
  int location;
  PangoTabAlign alignment;
  gunichar decimal_point;
};

/**
 * PangoTabArray:
 *
 * A `PangoTabArray` contains an array of tab stops.
 *
 * `PangoTabArray` can be used to set tab stops in a `PangoLayout`.
 * Each tab stop has an alignment, a position, and optionally
 * a character to use as decimal point.
 */
struct _PangoTabArray
{
  gint size;
  gint allocated;
  gboolean positions_in_pixels;
  PangoTab *tabs;
};

static void
init_tabs (PangoTabArray *array, gint start, gint end)
{
  while (start < end)
    {
      array->tabs[start].location = 0;
      array->tabs[start].alignment = PANGO_TAB_LEFT;
      array->tabs[start].decimal_point = 0;
      ++start;
    }
}

/**
 * pango_tab_array_new:
 * @initial_size: Initial number of tab stops to allocate, can be 0
 * @positions_in_pixels: whether positions are in pixel units
 *
 * Creates an array of @initial_size tab stops.
 *
 * Tab stops are specified in pixel units if @positions_in_pixels is %TRUE,
 * otherwise in Pango units. All stops are initially at position 0.
 *
 * Return value: the newly allocated `PangoTabArray`, which should
 *   be freed with [method@Pango.TabArray.free].
 */
PangoTabArray*
pango_tab_array_new (gint     initial_size,
                     gboolean positions_in_pixels)
{
  PangoTabArray *array;

  g_return_val_if_fail (initial_size >= 0, NULL);

  /* alloc enough to treat array->tabs as an array of length
   * size, though it's declared as an array of length 1.
   * If we allowed tab array resizing we'd need to drop this
   * optimization.
   */
  array = g_slice_new (PangoTabArray);
  array->size = initial_size; array->allocated = initial_size;

  if (array->allocated > 0)
    {
      array->tabs = g_new (PangoTab, array->allocated);
      init_tabs (array, 0, array->allocated);
    }
  else
    array->tabs = NULL;

  array->positions_in_pixels = positions_in_pixels;

  return array;
}

/**
 * pango_tab_array_new_with_positions:
 * @size: number of tab stops in the array
 * @positions_in_pixels: whether positions are in pixel units
 * @first_alignment: alignment of first tab stop
 * @first_position: position of first tab stop
 * @...: additional alignment/position pairs
 *
 * Creates a `PangoTabArray` and allows you to specify the alignment
 * and position of each tab stop.
 *
 * You **must** provide an alignment and position for @size tab stops.
 *
 * Return value: the newly allocated `PangoTabArray`, which should
 *   be freed with [method@Pango.TabArray.free].
 */
PangoTabArray  *
pango_tab_array_new_with_positions (gint           size,
                                    gboolean       positions_in_pixels,
                                    PangoTabAlign  first_alignment,
                                    gint           first_position,
                                    ...)
{
  PangoTabArray *array;
  va_list args;
  int i;

  g_return_val_if_fail (size >= 0, NULL);

  array = pango_tab_array_new (size, positions_in_pixels);

  if (size == 0)
    return array;

  array->tabs[0].alignment = first_alignment;
  array->tabs[0].location = first_position;
  array->tabs[0].decimal_point = 0;

  if (size == 1)
    return array;

  va_start (args, first_position);

  i = 1;
  while (i < size)
    {
      PangoTabAlign align = va_arg (args, PangoTabAlign);
      int pos = va_arg (args, int);

      array->tabs[i].alignment = align;
      array->tabs[i].location = pos;
      array->tabs[i].decimal_point = 0;

      ++i;
    }

  va_end (args);

  return array;
}

G_DEFINE_BOXED_TYPE (PangoTabArray, pango_tab_array,
                     pango_tab_array_copy,
                     pango_tab_array_free);

/**
 * pango_tab_array_copy:
 * @src: `PangoTabArray` to copy
 *
 * Copies a `PangoTabArray`.
 *
 * Return value: the newly allocated `PangoTabArray`, which should
 *   be freed with [method@Pango.TabArray.free].
 */
PangoTabArray*
pango_tab_array_copy (PangoTabArray *src)
{
  PangoTabArray *copy;

  g_return_val_if_fail (src != NULL, NULL);

  copy = pango_tab_array_new (src->size, src->positions_in_pixels);

  if (copy->tabs)
    memcpy (copy->tabs, src->tabs, sizeof(PangoTab) * src->size);

  return copy;
}

/**
 * pango_tab_array_free:
 * @tab_array: a `PangoTabArray`
 *
 * Frees a tab array and associated resources.
 */
void
pango_tab_array_free (PangoTabArray *tab_array)
{
  g_return_if_fail (tab_array != NULL);

  g_free (tab_array->tabs);

  g_slice_free (PangoTabArray, tab_array);
}

/**
 * pango_tab_array_get_size:
 * @tab_array: a `PangoTabArray`
 *
 * Gets the number of tab stops in @tab_array.
 *
 * Return value: the number of tab stops in the array.
 */
gint
pango_tab_array_get_size (PangoTabArray *tab_array)
{
  g_return_val_if_fail (tab_array != NULL, 0);

  return tab_array->size;
}

/**
 * pango_tab_array_resize:
 * @tab_array: a `PangoTabArray`
 * @new_size: new size of the array
 *
 * Resizes a tab array.
 *
 * You must subsequently initialize any tabs
 * that were added as a result of growing the array.
 */
void
pango_tab_array_resize (PangoTabArray *tab_array,
                        gint           new_size)
{
  if (new_size > tab_array->allocated)
    {
      gint current_end = tab_array->allocated;

      /* Ratchet allocated size up above the index. */
      if (tab_array->allocated == 0)
        tab_array->allocated = 2;

      while (new_size > tab_array->allocated)
        tab_array->allocated = tab_array->allocated * 2;

      tab_array->tabs = g_renew (PangoTab, tab_array->tabs,
                                 tab_array->allocated);

      init_tabs (tab_array, current_end, tab_array->allocated);
    }

  tab_array->size = new_size;
}

/**
 * pango_tab_array_set_tab:
 * @tab_array: a `PangoTabArray`
 * @tab_index: the index of a tab stop
 * @alignment: tab alignment
 * @location: tab location in Pango units
 *
 * Sets the alignment and location of a tab stop.
 */
void
pango_tab_array_set_tab (PangoTabArray *tab_array,
                         gint           tab_index,
                         PangoTabAlign  alignment,
                         gint           location)
{
  g_return_if_fail (tab_array != NULL);
  g_return_if_fail (tab_index >= 0);
  g_return_if_fail (location >= 0);

  if (tab_index >= tab_array->size)
    pango_tab_array_resize (tab_array, tab_index + 1);

  tab_array->tabs[tab_index].alignment = alignment;
  tab_array->tabs[tab_index].location = location;
}

/**
 * pango_tab_array_get_tab:
 * @tab_array: a `PangoTabArray`
 * @tab_index: tab stop index
 * @alignment: (out) (optional): location to store alignment
 * @location: (out) (optional): location to store tab position
 *
 * Gets the alignment and position of a tab stop.
 */
void
pango_tab_array_get_tab (PangoTabArray *tab_array,
                         gint           tab_index,
                         PangoTabAlign *alignment,
                         gint          *location)
{
  g_return_if_fail (tab_array != NULL);
  g_return_if_fail (tab_index < tab_array->size);
  g_return_if_fail (tab_index >= 0);

  if (alignment)
    *alignment = tab_array->tabs[tab_index].alignment;

  if (location)
    *location = tab_array->tabs[tab_index].location;
}

/**
 * pango_tab_array_get_tabs:
 * @tab_array: a `PangoTabArray`
 * @alignments: (out) (optional): location to store an array of tab
 *   stop alignments
 * @locations: (out) (optional) (array): location to store an array
 *   of tab positions
 *
 * If non-%NULL, @alignments and @locations are filled with allocated
 * arrays.
 *
 * The arrays are of length [method@Pango.TabArray.get_size].
 * You must free the returned array.
 */
void
pango_tab_array_get_tabs (PangoTabArray *tab_array,
                          PangoTabAlign **alignments,
                          gint          **locations)
{
  gint i;

  g_return_if_fail (tab_array != NULL);

  if (alignments)
    *alignments = g_new (PangoTabAlign, tab_array->size);

  if (locations)
    *locations = g_new (gint, tab_array->size);

  i = 0;
  while (i < tab_array->size)
    {
      if (alignments)
        (*alignments)[i] = tab_array->tabs[i].alignment;
      if (locations)
        (*locations)[i] = tab_array->tabs[i].location;

      ++i;
    }
}

/**
 * pango_tab_array_get_positions_in_pixels:
 * @tab_array: a `PangoTabArray`
 *
 * Returns %TRUE if the tab positions are in pixels,
 * %FALSE if they are in Pango units.
 *
 * Return value: whether positions are in pixels.
 */
gboolean
pango_tab_array_get_positions_in_pixels (PangoTabArray *tab_array)
{
  g_return_val_if_fail (tab_array != NULL, FALSE);

  return tab_array->positions_in_pixels;
}

/**
 * pango_tab_array_set_positions_in_pixels:
 * @tab_array: a `PangoTabArray`
 * @positions_in_pixels: whether positions are in pixels
 *
 * Sets whether positions in this array are specified in
 * pixels.
 *
 * Since: 1.50
 */
void
pango_tab_array_set_positions_in_pixels (PangoTabArray *tab_array,
                                         gboolean       positions_in_pixels)
{
  g_return_if_fail (tab_array != NULL);

  tab_array->positions_in_pixels = positions_in_pixels;
}

/**
 * pango_tab_array_to_string:
 * @tab_array: a `PangoTabArray`
 *
 * Serializes a `PangoTabArray` to a string.
 *
 * No guarantees are made about the format of the string,
 * it may change between Pango versions.
 *
 * The intended use of this function is testing and
 * debugging. The format is not meant as a permanent
 * storage format.
 *
 * Returns: (transfer full): a newly allocated string
 * Since: 1.50
 */
char *
pango_tab_array_to_string (PangoTabArray *tab_array)
{
  GString *s;

  s = g_string_new ("");

  for (int i = 0; i < tab_array->size; i++)
    {
      if (i > 0)
        g_string_append_c (s, '\n');

      if (tab_array->tabs[i].alignment == PANGO_TAB_RIGHT)
        g_string_append (s, "right:");
      else if (tab_array->tabs[i].alignment == PANGO_TAB_CENTER)
        g_string_append (s, "center:");
      else if (tab_array->tabs[i].alignment == PANGO_TAB_DECIMAL)
        g_string_append (s, "decimal:");

      g_string_append_printf (s, "%d", tab_array->tabs[i].location);
      if (tab_array->positions_in_pixels)
        g_string_append (s, "px");

      if (tab_array->tabs[i].decimal_point != 0)
        g_string_append_printf (s, ":%d", tab_array->tabs[i].decimal_point);
    }

  return g_string_free (s, FALSE);
}

static const char *
skip_whitespace (const char *p)
{
  while (g_ascii_isspace (*p))
    p++;
  return p;
}

/**
 * pango_tab_array_from_string:
 * @text: a string
 *
 * Deserializes a `PangoTabArray` from a string.
 *
 * This is the counterpart to [method@Pango.TabArray.to_string].
 * See that functions for details about the format.
 *
 * Returns: (transfer full) (nullable): a new `PangoTabArray`
 * Since: 1.50
 */
PangoTabArray *
pango_tab_array_from_string (const char *text)
{
  PangoTabArray *array;
  gboolean pixels;
  const char *p;
  int i;

  pixels = strstr (text, "px") != NULL;

  array = pango_tab_array_new (0, pixels);

  p = skip_whitespace (text);

  i = 0;
  while (*p)
    {
      char *endp;
      gint64 pos;
      PangoTabAlign align;

      if (g_str_has_prefix (p, "left:"))
        {
          align = PANGO_TAB_LEFT;
          p += strlen ("left:");
        }
      else if (g_str_has_prefix (p, "right:"))
        {
          align = PANGO_TAB_RIGHT;
          p += strlen ("right:");
        }
      else if (g_str_has_prefix (p, "center:"))
        {
          align = PANGO_TAB_CENTER;
          p += strlen ("center:");
        }
      else if (g_str_has_prefix (p, "decimal:"))
        {
          align = PANGO_TAB_DECIMAL;
          p += strlen ("decimal:");
        }
      else
        {
          align = PANGO_TAB_LEFT;
        }

      pos = g_ascii_strtoll (p, &endp, 10);
      if (pos < 0 ||
          (pixels && *endp != 'p') ||
          (!pixels && !g_ascii_isspace (*endp) && *endp != ':' && *endp != '\0')) goto fail;

      pango_tab_array_set_tab (array, i, align, pos);

      p = (const char *)endp;
      if (pixels)
        {
          if (p[0] != 'p' || p[1] != 'x') goto fail;
          p += 2;
        }

      if (p[0] == ':')
        {
          gunichar ch;

          p++;
          ch = g_ascii_strtoll (p, &endp, 10);
          if (!g_ascii_isspace (*endp) && *endp != '\0') goto fail;

          pango_tab_array_set_decimal_point (array, i, ch);

          p = (const char *)endp;
        }

      p = skip_whitespace (p);

      i++;
    }

  goto success;

fail:
  pango_tab_array_free (array);
  array = NULL;

success:
  return array;
}

/**
 * pango_tab_array_set_decimal_point:
 * @tab_array: a `PangoTabArray`
 * @tab_index: the index of a tab stop
 * @decimal_point: the decimal point to use
 *
 * Sets the Unicode character to use as decimal point.
 *
 * This is only relevant for tabs with %PANGO_TAB_DECIMAL alignment,
 * which align content at the first occurrence of the decimal point
 * character.
 *
 * By default, Pango uses the decimal point according
 * to the current locale.
 *
 * Since: 1.50
 */
void
pango_tab_array_set_decimal_point (PangoTabArray *tab_array,
                                   int            tab_index,
                                   gunichar       decimal_point)
{
  g_return_if_fail (tab_array != NULL);
  g_return_if_fail (tab_index >= 0);

  if (tab_index >= tab_array->size)
    pango_tab_array_resize (tab_array, tab_index + 1);

  tab_array->tabs[tab_index].decimal_point = decimal_point;
}

/**
 * pango_tab_array_get_decimal_point:
 * @tab_array: a `PangoTabArray`
 * @tab_index: the index of a tab stop
 *
 * Gets the Unicode character to use as decimal point.
 *
 * This is only relevant for tabs with %PANGO_TAB_DECIMAL alignment,
 * which align content at the first occurrence of the decimal point
 * character.
 *
 * The default value of 0 means that Pango will use the
 * decimal point according to the current locale.
 *
 * Since: 1.50
 */
gunichar
pango_tab_array_get_decimal_point (PangoTabArray *tab_array,
                                   int            tab_index)
{
  g_return_val_if_fail (tab_array != NULL, 0);
  g_return_val_if_fail (tab_index < tab_array->size, 0);
  g_return_val_if_fail (tab_index >= 0, 0);

  return tab_array->tabs[tab_index].decimal_point;
}

static int
compare_tabs (const void *p1, const void *p2)
{
  const PangoTab *t1 = p1;
  const PangoTab *t2 = p2;

  return t1->location - t2->location;
}

/**
 * pango_tab_array_sort:
 * @tab_array: a `PangoTabArray`
 *
 * Utility function to ensure that the tab stops are in increasing order.
 *
 * Since: 1.50
 */
void
pango_tab_array_sort (PangoTabArray *tab_array)
{
  g_return_if_fail (tab_array != NULL);

  qsort (tab_array->tabs, tab_array->size, sizeof (PangoTab), compare_tabs);
}
