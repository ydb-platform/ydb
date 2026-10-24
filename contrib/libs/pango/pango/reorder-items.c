/* Pango
 * reorder-items.c:
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
#include "pango-item.h"

/*
 * NB: The contents of the file implement the exact same algorithm
 *     pango-layout.c:pango_layout_line_reorder().
 */

static GList *reorder_items_recurse (GList *items, int n_items);

/**
 * pango_reorder_items:
 * @items: (element-type Pango.Item): a `GList` of `PangoItem`
 *   in logical order.
 *
 * Reorder items from logical order to visual order.
 *
 * The visual order is determined from the associated directional
 * levels of the items. The original list is unmodified.
 *
 * (Please open a bug if you use this function.
 *  It is not a particularly convenient interface, and the code
 *  is duplicated elsewhere in Pango for that reason.)
 *
 * Returns: (transfer full) (element-type Pango.Item): a `GList`
 *   of `PangoItem` structures in visual order.
 */
GList *
pango_reorder_items (GList *items)
{
  return reorder_items_recurse (items, g_list_length (items));
}

static GList *
reorder_items_recurse (GList *items,
                       int    n_items)
{
  GList *tmp_list, *level_start_node;
  int i, level_start_i;
  int min_level = G_MAXINT;
  GList *result = NULL;

  if (n_items == 0)
    return NULL;

  tmp_list = items;
  for (i=0; i<n_items; i++)
    {
      PangoItem *item = tmp_list->data;

      min_level = MIN (min_level, item->analysis.level);

      tmp_list = tmp_list->next;
    }

  level_start_i = 0;
  level_start_node = items;
  tmp_list = items;
  for (i=0; i<n_items; i++)
    {
      PangoItem *item = tmp_list->data;

      if (item->analysis.level == min_level)
	{
	  if (min_level % 2)
	    {
	      if (i > level_start_i)
		result = g_list_concat (reorder_items_recurse (level_start_node, i - level_start_i), result);
	      result = g_list_prepend (result, item);
	    }
	  else
	    {
	      if (i > level_start_i)
		result = g_list_concat (result, reorder_items_recurse (level_start_node, i - level_start_i));
	      result = g_list_append (result, item);
	    }

	  level_start_i = i + 1;
	  level_start_node = tmp_list->next;
	}

      tmp_list = tmp_list->next;
    }

  if (min_level % 2)
    {
      if (i > level_start_i)
	result = g_list_concat (reorder_items_recurse (level_start_node, i - level_start_i), result);
    }
  else
    {
      if (i > level_start_i)
	result = g_list_concat (result, reorder_items_recurse (level_start_node, i - level_start_i));
    }

  return result;
}
