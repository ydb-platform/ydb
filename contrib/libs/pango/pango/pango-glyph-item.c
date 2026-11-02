/* Pango
 * pango-glyph-item.c: Pair of PangoItem and a glyph string
 *
 * Copyright (C) 2002 Red Hat Software
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
#include <string.h>

#include "pango-glyph-item.h"
#include "pango-impl-utils.h"
#include "pango-attributes-private.h"

#define LTR(glyph_item) (((glyph_item)->item->analysis.level % 2) == 0)

/**
 * pango_glyph_item_split:
 * @orig: a `PangoItem`
 * @text: text to which positions in @orig apply
 * @split_index: byte index of position to split item, relative to the
 *   start of the item
 *
 * Modifies @orig to cover only the text after @split_index, and
 * returns a new item that covers the text before @split_index that
 * used to be in @orig.
 *
 * You can think of @split_index as the length of the returned item.
 * @split_index may not be 0, and it may not be greater than or equal
 * to the length of @orig (that is, there must be at least one byte
 * assigned to each item, you can't create a zero-length item).
 *
 * This function is similar in function to pango_item_split() (and uses
 * it internally.)
 *
 * Returns: (transfer full) (nullable): the newly allocated item
 *   representing text before @split_index, which should be freed
 *   with pango_glyph_item_free().
 *
 * Since: 1.2
 */
PangoGlyphItem *
pango_glyph_item_split (PangoGlyphItem *orig,
			const char     *text,
			int             split_index)
{
  PangoGlyphItem *new;
  int i;
  int num_glyphs;
  int num_remaining;
  int split_offset;

  g_return_val_if_fail (orig != NULL, NULL);
  g_return_val_if_fail (orig->item->length > 0, NULL);
  g_return_val_if_fail (split_index > 0, NULL);
  g_return_val_if_fail (split_index < orig->item->length, NULL);

  if (LTR (orig))
    {
      for (i = 0; i < orig->glyphs->num_glyphs; i++)
	{
	  if (orig->glyphs->log_clusters[i] >= split_index)
	    break;
	}

      if (i == orig->glyphs->num_glyphs) /* No splitting necessary */
	return NULL;

      split_index = orig->glyphs->log_clusters[i];
      num_glyphs = i;
    }
  else
    {
      for (i = orig->glyphs->num_glyphs - 1; i >= 0; i--)
	{
	  if (orig->glyphs->log_clusters[i] >= split_index)
	    break;
	}

      if (i < 0) /* No splitting necessary */
	return NULL;

      split_index = orig->glyphs->log_clusters[i];
      num_glyphs = orig->glyphs->num_glyphs - 1 - i;
    }

  num_remaining = orig->glyphs->num_glyphs - num_glyphs;

  new = g_slice_new (PangoGlyphItem);
  split_offset = g_utf8_pointer_to_offset (text + orig->item->offset,
					   text + orig->item->offset + split_index);
  new->item = pango_item_split (orig->item, split_index, split_offset);

  new->glyphs = pango_glyph_string_new ();
  pango_glyph_string_set_size (new->glyphs, num_glyphs);

  if (LTR (orig))
    {
      memcpy (new->glyphs->glyphs, orig->glyphs->glyphs, num_glyphs * sizeof (PangoGlyphInfo));
      memcpy (new->glyphs->log_clusters, orig->glyphs->log_clusters, num_glyphs * sizeof (int));

      memmove (orig->glyphs->glyphs, orig->glyphs->glyphs + num_glyphs,
	       num_remaining * sizeof (PangoGlyphInfo));
      for (i = num_glyphs; i < orig->glyphs->num_glyphs; i++)
	orig->glyphs->log_clusters[i - num_glyphs] = orig->glyphs->log_clusters[i] - split_index;
    }
  else
    {
      memcpy (new->glyphs->glyphs, orig->glyphs->glyphs + num_remaining, num_glyphs * sizeof (PangoGlyphInfo));
      memcpy (new->glyphs->log_clusters, orig->glyphs->log_clusters + num_remaining, num_glyphs * sizeof (int));

      for (i = 0; i < num_remaining; i++)
	orig->glyphs->log_clusters[i] = orig->glyphs->log_clusters[i] - split_index;
    }

  pango_glyph_string_set_size (orig->glyphs, orig->glyphs->num_glyphs - num_glyphs);

  new->y_offset = orig->y_offset;
  new->start_x_offset = orig->start_x_offset;
  new->end_x_offset = -orig->start_x_offset;

  return new;
}

/**
 * pango_glyph_item_copy:
 * @orig: (nullable): a `PangoGlyphItem`
 *
 * Make a deep copy of an existing `PangoGlyphItem` structure.
 *
 * Returns: (transfer full) (nullable): the newly allocated `PangoGlyphItem`
 *
 * Since: 1.20
 */
PangoGlyphItem *
pango_glyph_item_copy  (PangoGlyphItem *orig)
{
  PangoGlyphItem *result;

  if (orig == NULL)
    return NULL;

  result = g_slice_new (PangoGlyphItem);

  result->item = pango_item_copy (orig->item);
  result->glyphs = pango_glyph_string_copy (orig->glyphs);
  result->y_offset = orig->y_offset;
  result->start_x_offset = orig->start_x_offset;
  result->end_x_offset = orig->end_x_offset;

  return result;
}

/**
 * pango_glyph_item_free:
 * @glyph_item: (nullable): a `PangoGlyphItem`
 *
 * Frees a `PangoGlyphItem` and resources to which it points.
 *
 * Since: 1.6
 */
void
pango_glyph_item_free  (PangoGlyphItem *glyph_item)
{
  if (glyph_item == NULL)
    return;

  if (glyph_item->item)
    pango_item_free (glyph_item->item);
  if (glyph_item->glyphs)
    pango_glyph_string_free (glyph_item->glyphs);

  g_slice_free (PangoGlyphItem, glyph_item);
}

G_DEFINE_BOXED_TYPE (PangoGlyphItem, pango_glyph_item,
                     pango_glyph_item_copy,
                     pango_glyph_item_free);


/**
 * pango_glyph_item_iter_copy:
 * @orig: (nullable): a `PangoGlyphItem`Iter
 *
 * Make a shallow copy of an existing `PangoGlyphItemIter` structure.
 *
 * Returns: (transfer full) (nullable): the newly allocated `PangoGlyphItemIter`
 *
 * Since: 1.22
 */
PangoGlyphItemIter *
pango_glyph_item_iter_copy (PangoGlyphItemIter *orig)
{
  PangoGlyphItemIter *result;

  if (orig == NULL)
    return NULL;

  result = g_slice_new (PangoGlyphItemIter);

  *result = *orig;

  return result;
}

/**
 * pango_glyph_item_iter_free:
 * @iter: (nullable): a `PangoGlyphItemIter`
 *
 * Frees a `PangoGlyphItem`Iter.
 *
 * Since: 1.22
 */
void
pango_glyph_item_iter_free  (PangoGlyphItemIter *iter)
{
  if (iter == NULL)
    return;

  g_slice_free (PangoGlyphItemIter, iter);
}

G_DEFINE_BOXED_TYPE (PangoGlyphItemIter, pango_glyph_item_iter,
                     pango_glyph_item_iter_copy,
                     pango_glyph_item_iter_free)

/**
 * pango_glyph_item_iter_next_cluster:
 * @iter: a `PangoGlyphItemIter`
 *
 * Advances the iterator to the next cluster in the glyph item.
 *
 * See `PangoGlyphItemIter` for details of cluster orders.
 *
 * Returns: %TRUE if the iterator was advanced,
 *   %FALSE if we were already on the  last cluster.
 *
 * Since: 1.22
 */
gboolean
pango_glyph_item_iter_next_cluster (PangoGlyphItemIter *iter)
{
  int glyph_index = iter->end_glyph;
  PangoGlyphString *glyphs = iter->glyph_item->glyphs;
  int cluster;
  PangoItem *item = iter->glyph_item->item;

  if (LTR (iter->glyph_item))
    {
      if (glyph_index == glyphs->num_glyphs)
	return FALSE;
    }
  else
    {
      if (glyph_index < 0)
	return FALSE;
    }

  iter->start_glyph = iter->end_glyph;
  iter->start_index = iter->end_index;
  iter->start_char = iter->end_char;

  if (LTR (iter->glyph_item))
    {
      cluster = glyphs->log_clusters[glyph_index];
      while (TRUE)
	{
	  glyph_index++;

	  if (glyph_index == glyphs->num_glyphs)
	    {
	      iter->end_index = item->offset + item->length;
	      iter->end_char = item->num_chars;
	      break;
	    }

	  if (glyphs->log_clusters[glyph_index] > cluster)
	    {
	      iter->end_index = item->offset + glyphs->log_clusters[glyph_index];
	      iter->end_char += pango_utf8_strlen (iter->text + iter->start_index,
					       iter->end_index - iter->start_index);
	      break;
	    }
	}
    }
  else			/* RTL */
    {
      cluster = glyphs->log_clusters[glyph_index];
      while (TRUE)
	{
	  glyph_index--;

	  if (glyph_index < 0)
	    {
	      iter->end_index = item->offset + item->length;
	      iter->end_char = item->num_chars;
	      break;
	    }

	  if (glyphs->log_clusters[glyph_index] > cluster)
	    {
	      iter->end_index = item->offset + glyphs->log_clusters[glyph_index];
	      iter->end_char += pango_utf8_strlen (iter->text + iter->start_index,
					       iter->end_index - iter->start_index);
	      break;
	    }
	}
    }

  iter->end_glyph = glyph_index;

  g_assert (iter->start_char <= iter->end_char);
  g_assert (iter->end_char <= item->num_chars);

  return TRUE;
}

/**
 * pango_glyph_item_iter_prev_cluster:
 * @iter: a `PangoGlyphItemIter`
 *
 * Moves the iterator to the preceding cluster in the glyph item.
 * See `PangoGlyphItemIter` for details of cluster orders.
 *
 * Returns: %TRUE if the iterator was moved,
 *   %FALSE if we were already on the first cluster.
 *
 * Since: 1.22
 */
gboolean
pango_glyph_item_iter_prev_cluster (PangoGlyphItemIter *iter)
{
  int glyph_index = iter->start_glyph;
  PangoGlyphString *glyphs = iter->glyph_item->glyphs;
  int cluster;
  PangoItem *item = iter->glyph_item->item;

  if (LTR (iter->glyph_item))
    {
      if (glyph_index == 0)
	return FALSE;
    }
  else
    {
      if (glyph_index == glyphs->num_glyphs - 1)
	return FALSE;

    }

  iter->end_glyph = iter->start_glyph;
  iter->end_index = iter->start_index;
  iter->end_char = iter->start_char;

  if (LTR (iter->glyph_item))
    {
      cluster = glyphs->log_clusters[glyph_index - 1];
      while (TRUE)
	{
	  if (glyph_index == 0)
	    {
	      iter->start_index = item->offset;
	      iter->start_char = 0;
	      break;
	    }

	  glyph_index--;

	  if (glyphs->log_clusters[glyph_index] < cluster)
	    {
	      glyph_index++;
	      iter->start_index = item->offset + glyphs->log_clusters[glyph_index];
	      iter->start_char -= pango_utf8_strlen (iter->text + iter->start_index,
						 iter->end_index - iter->start_index);
	      break;
	    }
	}
    }
  else			/* RTL */
    {
      cluster = glyphs->log_clusters[glyph_index + 1];
      while (TRUE)
	{
	  if (glyph_index == glyphs->num_glyphs - 1)
	    {
	      iter->start_index = item->offset;
	      iter->start_char = 0;
	      break;
	    }

	  glyph_index++;

	  if (glyphs->log_clusters[glyph_index] < cluster)
	    {
	      glyph_index--;
	      iter->start_index = item->offset + glyphs->log_clusters[glyph_index];
	      iter->start_char -= pango_utf8_strlen (iter->text + iter->start_index,
						 iter->end_index - iter->start_index);
	      break;
	    }
	}
    }

  iter->start_glyph = glyph_index;

  g_assert (iter->start_char <= iter->end_char);
  g_assert (0 <= iter->start_char);

  return TRUE;
}

/**
 * pango_glyph_item_iter_init_start:
 * @iter: a `PangoGlyphItemIter`
 * @glyph_item: the glyph item to iterate over
 * @text: text corresponding to the glyph item
 *
 * Initializes a `PangoGlyphItemIter` structure to point to the
 * first cluster in a glyph item.
 *
 * See `PangoGlyphItemIter` for details of cluster orders.
 *
 * Returns: %FALSE if there are no clusters in the glyph item
 *
 * Since: 1.22
 */
gboolean
pango_glyph_item_iter_init_start (PangoGlyphItemIter  *iter,
				  PangoGlyphItem      *glyph_item,
				  const char          *text)
{
  iter->glyph_item = glyph_item;
  iter->text = text;

  if (LTR (glyph_item))
    iter->end_glyph = 0;
  else
    iter->end_glyph = glyph_item->glyphs->num_glyphs - 1;

  iter->end_index = glyph_item->item->offset;
  iter->end_char = 0;

  iter->start_glyph = iter->end_glyph;
  iter->start_index = iter->end_index;
  iter->start_char = iter->end_char;

  /* Advance onto the first cluster of the glyph item */
  return pango_glyph_item_iter_next_cluster (iter);
}

/**
 * pango_glyph_item_iter_init_end:
 * @iter: a `PangoGlyphItemIter`
 * @glyph_item: the glyph item to iterate over
 * @text: text corresponding to the glyph item
 *
 * Initializes a `PangoGlyphItemIter` structure to point to the
 * last cluster in a glyph item.
 *
 * See `PangoGlyphItemIter` for details of cluster orders.
 *
 * Returns: %FALSE if there are no clusters in the glyph item
 *
 * Since: 1.22
 */
gboolean
pango_glyph_item_iter_init_end (PangoGlyphItemIter  *iter,
				PangoGlyphItem      *glyph_item,
				const char          *text)
{
  iter->glyph_item = glyph_item;
  iter->text = text;

  if (LTR (glyph_item))
    iter->start_glyph = glyph_item->glyphs->num_glyphs;
  else
    iter->start_glyph = -1;

  iter->start_index = glyph_item->item->offset + glyph_item->item->length;
  iter->start_char = glyph_item->item->num_chars;

  iter->end_glyph = iter->start_glyph;
  iter->end_index = iter->start_index;
  iter->end_char = iter->start_char;

  /* Advance onto the first cluster of the glyph item */
  return pango_glyph_item_iter_prev_cluster (iter);
}

typedef struct
{
  PangoGlyphItemIter iter;

  GSList *segment_attrs;
} ApplyAttrsState;

/* Tack @attrs onto the attributes of glyph_item
 */
static void
append_attrs (PangoGlyphItem *glyph_item,
	      GSList         *attrs)
{
  glyph_item->item->analysis.extra_attrs =
    g_slist_concat (glyph_item->item->analysis.extra_attrs, attrs);
}

/* Make a deep copy of a GSList of PangoAttribute
 */
static GSList *
attr_slist_copy (GSList *attrs)
{
  GSList *tmp_list;
  GSList *new_attrs;

  new_attrs = g_slist_copy (attrs);

  for (tmp_list = new_attrs; tmp_list; tmp_list = tmp_list->next)
    tmp_list->data = pango_attribute_copy (tmp_list->data);

  return new_attrs;
}

/* Split the glyph item at the start of the current cluster
 */
static PangoGlyphItem *
split_before_cluster_start (ApplyAttrsState *state)
{
  PangoGlyphItem *split_item;
  int split_len = state->iter.start_index - state->iter.glyph_item->item->offset;

  split_item = pango_glyph_item_split (state->iter.glyph_item, state->iter.text, split_len);
  append_attrs (split_item, state->segment_attrs);

  /* Adjust iteration to account for the split
   */
  if (LTR (state->iter.glyph_item))
    {
      state->iter.start_glyph -= split_item->glyphs->num_glyphs;
      state->iter.end_glyph -= split_item->glyphs->num_glyphs;
    }

  state->iter.start_char -= split_item->item->num_chars;
  state->iter.end_char -= split_item->item->num_chars;

  return split_item;
}

/**
 * pango_glyph_item_apply_attrs:
 * @glyph_item: a shaped item
 * @text: text that @list applies to
 * @list: a `PangoAttrList`
 *
 * Splits a shaped item (`PangoGlyphItem`) into multiple items based
 * on an attribute list.
 *
 * The idea is that if you have attributes that don't affect shaping,
 * such as color or underline, to avoid affecting shaping, you filter
 * them out ([method@Pango.AttrList.filter]), apply the shaping process
 * and then reapply them to the result using this function.
 *
 * All attributes that start or end inside a cluster are applied
 * to that cluster; for instance, if half of a cluster is underlined
 * and the other-half strikethrough, then the cluster will end
 * up with both underline and strikethrough attributes. In these
 * cases, it may happen that @item->extra_attrs for some of the
 * result items can have multiple attributes of the same type.
 *
 * This function takes ownership of @glyph_item; it will be reused
 * as one of the elements in the list.
 *
 * Returns: (transfer full) (element-type Pango.GlyphItem): a
 *   list of glyph items resulting from splitting @glyph_item. Free
 *   the elements using [method@Pango.GlyphItem.free], the list using
 *   g_slist_free().
 *
 * Since: 1.2
 */
GSList *
pango_glyph_item_apply_attrs (PangoGlyphItem   *glyph_item,
			      const char       *text,
			      PangoAttrList    *list)
{
  PangoAttrIterator iter;
  GSList *result = NULL;
  ApplyAttrsState state;
  gboolean start_new_segment = FALSE;
  gboolean have_cluster;
  int range_start, range_end;
  gboolean is_ellipsis;

  /* This routine works by iterating through the item cluster by
   * cluster; we accumulate the attributes that we need to
   * add to the next output item, and decide when to split
   * off an output item based on two criteria:
   *
   * A) If start_index < attribute_start < end_index
   *    (attribute starts within cluster) then we need
   *    to split between the last cluster and this cluster.
   * B) If start_index < attribute_end <= end_index,
   *    (attribute ends within cluster) then we need to
   *    split between this cluster and the next one.
   */

  /* Advance the attr iterator to the start of the item
   */
  _pango_attr_list_get_iterator (list, &iter);
  do
    {
      pango_attr_iterator_range (&iter, &range_start, &range_end);
      if (range_end > glyph_item->item->offset)
	break;
    }
  while (pango_attr_iterator_next (&iter));

  state.segment_attrs = pango_attr_iterator_get_attrs (&iter);

  is_ellipsis = (glyph_item->item->analysis.flags & PANGO_ANALYSIS_FLAG_IS_ELLIPSIS) != 0;

  /* Short circuit the case when we don't actually need to
   * split the item
   */
  if (is_ellipsis ||
      (range_start <= glyph_item->item->offset &&
       range_end >= glyph_item->item->offset + glyph_item->item->length))
    goto out;

  for (have_cluster = pango_glyph_item_iter_init_start (&state.iter, glyph_item, text);
       have_cluster;
       have_cluster = pango_glyph_item_iter_next_cluster (&state.iter))
    {
      gboolean have_next;

      /* [range_start,range_end] is the first range that intersects
       * the current cluster.
       */

      /* Split item into two, if this cluster isn't a continuation
       * of the last cluster
       */
      if (start_new_segment)
	{
	  result = g_slist_prepend (result,
				    split_before_cluster_start (&state));
	  state.segment_attrs = pango_attr_iterator_get_attrs (&iter);
	}

      start_new_segment = FALSE;

      /* Loop over all ranges that intersect this cluster; exiting
       * leaving [range_start,range_end] being the first range that
       * intersects the next cluster.
       */
      do
	{
	  if (range_end > state.iter.end_index) /* Range intersects next cluster */
	    break;

	  /* Since ranges end in this cluster, the next cluster goes into a
	   * separate segment
	   */
	  start_new_segment = TRUE;

	  have_next = pango_attr_iterator_next (&iter);
	  pango_attr_iterator_range (&iter, &range_start, &range_end);

	  if (range_start >= state.iter.end_index) /* New range doesn't intersect this cluster */
	    {
	      /* No gap between ranges, so previous range must of ended
	       * at cluster boundary.
	       */
	      g_assert (range_start == state.iter.end_index && start_new_segment);
	      break;
	    }

	  /* If any ranges start *inside* this cluster, then we need
	   * to split the previous cluster into a separate segment
	   */
	  if (range_start > state.iter.start_index &&
	      state.iter.start_index != glyph_item->item->offset)
	    {
	      GSList *new_attrs = attr_slist_copy (state.segment_attrs);
	      result = g_slist_prepend (result,
					split_before_cluster_start (&state));
	      state.segment_attrs = new_attrs;
	    }

	  state.segment_attrs = g_slist_concat (state.segment_attrs,
						pango_attr_iterator_get_attrs (&iter));
	}
      while (have_next);
    }

 out:
  /* What's left in glyph_item is the remaining portion
   */
  append_attrs (glyph_item, state.segment_attrs);
  result = g_slist_prepend (result, glyph_item);

  if (LTR (glyph_item))
    result = g_slist_reverse (result);

  _pango_attr_iterator_destroy (&iter);

  return result;
}

/**
 * pango_glyph_item_letter_space:
 * @glyph_item: a `PangoGlyphItem`
 * @text: text that @glyph_item corresponds to
 *   (glyph_item->item->offset is an offset from the
 *   start of @text)
 * @log_attrs: (array): logical attributes for the item
 *   (the first logical attribute refers to the position
 *   before the first character in the item)
 * @letter_spacing: amount of letter spacing to add
 *   in Pango units. May be negative, though too large
 *   negative values will give ugly results.
 *
 * Adds spacing between the graphemes of @glyph_item to
 * give the effect of typographic letter spacing.
 *
 * Since: 1.6
 */
void
pango_glyph_item_letter_space (PangoGlyphItem *glyph_item,
			       const char     *text,
			       PangoLogAttr   *log_attrs,
			       int             letter_spacing)
{
  PangoGlyphItemIter iter;
  PangoGlyphInfo *glyphs = glyph_item->glyphs->glyphs;
  gboolean have_cluster;
  int space_left, space_right;

  space_left = letter_spacing / 2;

  /* hinting */
  if ((letter_spacing & (PANGO_SCALE - 1)) == 0)
    {
      space_left = PANGO_UNITS_ROUND (space_left);
    }

  space_right = letter_spacing - space_left;

  for (have_cluster = pango_glyph_item_iter_init_start (&iter, glyph_item, text);
       have_cluster;
       have_cluster = pango_glyph_item_iter_next_cluster (&iter))
    {
      if (!log_attrs[iter.start_char].is_cursor_position)
        {
          if (glyphs[iter.start_glyph].geometry.width == 0)
            {
              if (iter.start_glyph < iter.end_glyph) /* LTR */
                glyphs[iter.start_glyph].geometry.x_offset -= space_right;
              else
                glyphs[iter.start_glyph].geometry.x_offset += space_left;
            }
          continue;
        }

      if (iter.start_glyph < iter.end_glyph) /* LTR */
	{
	  if (iter.start_char > 0)
	    {
	      glyphs[iter.start_glyph].geometry.width    += space_left ;
	      glyphs[iter.start_glyph].geometry.x_offset += space_left ;
	    }
	  if (iter.end_char < glyph_item->item->num_chars)
	    {
	      glyphs[iter.end_glyph-1].geometry.width    += space_right;
	    }
	}
      else			                 /* RTL */
	{
	  if (iter.start_char > 0)
	    {
	      glyphs[iter.start_glyph].geometry.width    += space_right;
	    }
	  if (iter.end_char < glyph_item->item->num_chars)
	    {
	      glyphs[iter.end_glyph+1].geometry.x_offset += space_left ;
	      glyphs[iter.end_glyph+1].geometry.width    += space_left ;
	    }
	}
    }
}

/**
 * pango_glyph_item_get_logical_widths:
 * @glyph_item: a `PangoGlyphItem`
 * @text: text that @glyph_item corresponds to
 *   (glyph_item->item->offset is an offset from the
 *   start of @text)
 * @logical_widths: (array): an array whose length is the number of
 *   characters in glyph_item (equal to glyph_item->item->num_chars)
 *   to be filled in with the resulting character widths.
 *
 * Given a `PangoGlyphItem` and the corresponding text, determine the
 * width corresponding to each character.
 *
 * When multiple characters compose a single cluster, the width of the
 * entire cluster is divided equally among the characters.
 *
 * See also [method@Pango.GlyphString.get_logical_widths].
 *
 * Since: 1.26
 */
void
pango_glyph_item_get_logical_widths (PangoGlyphItem *glyph_item,
				     const char     *text,
				     int            *logical_widths)
{
  PangoGlyphItemIter iter;
  gboolean has_cluster;
  int dir;

  dir = glyph_item->item->analysis.level % 2 == 0 ? +1 : -1;
  for (has_cluster = pango_glyph_item_iter_init_start (&iter, glyph_item, text);
       has_cluster;
       has_cluster = pango_glyph_item_iter_next_cluster (&iter))
    {
      int glyph_index, char_index, num_chars, cluster_width = 0, char_width;

      for (glyph_index  = iter.start_glyph;
	   glyph_index != iter.end_glyph;
	   glyph_index += dir)
        {
	  cluster_width += glyph_item->glyphs->glyphs[glyph_index].geometry.width;
	}

      num_chars = iter.end_char - iter.start_char;
      if (num_chars) /* pedantic */
        {
	  char_width = cluster_width / num_chars;

	  for (char_index = iter.start_char;
	       char_index < iter.end_char;
	       char_index++)
	    {
	      logical_widths[char_index] = char_width;
	    }

	  /* add any residues to the first char */
	  logical_widths[iter.start_char] += cluster_width - (char_width * num_chars);
	}
    }
}
