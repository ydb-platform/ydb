/* Pango
 * ellipsize.c: Routine to ellipsize layout lines
 *
 * Copyright (C) 2004 Red Hat Software
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
#include "pango-layout-private.h"
#include "pango-font-private.h"
#include "pango-attributes-private.h"
#include "pango-impl-utils.h"

typedef struct _EllipsizeState EllipsizeState;
typedef struct _RunInfo        RunInfo;
typedef struct _LineIter       LineIter;


/* Overall, the way we ellipsize is we grow a "gap" out from an original
 * gap center position until:
 *
 *  line_width - gap_width + ellipsize_width <= goal_width
 *
 * Line:  [-------------------------------------------]
 * Runs:  [------)[---------------)[------------------]
 * Gap center:                 *
 * Gap:             [----------------------]
 *
 * The gap center may be at the start or end in which case the gap grows
 * in only one direction.
 *
 * Note the line and last run are logically closed at the end; this allows
 * us to use a gap position at x=line_width and still have it be part of
 * of a run.
 *
 * We grow the gap out one "span" at a time, where a span is simply a
 * consecutive run of clusters that we can't interrupt with an ellipsis.
 *
 * When choosing whether to grow the gap at the start or the end, we
 * calculate the next span to remove in both directions and see which
 * causes the smaller increase in:
 *
 *  MAX (gap_end - gap_center, gap_start - gap_center)
 *
 * All computations are done using logical order; the ellipsization
 * process occurs before the runs are ordered into visual order.
 */

/* Keeps information about a single run */
struct _RunInfo
{
  PangoGlyphItem *run;
  int start_offset;		/* Character offset of run start */
  int width;			/* Width of run in Pango units */
};

/* Iterator to a position within the ellipsized line */
struct _LineIter
{
  PangoGlyphItemIter run_iter;
  int run_index;
};

/* State of ellipsization process */
struct _EllipsizeState
{
  PangoLayout *layout;		/* Layout being ellipsized */
  PangoAttrList *attrs;		/* Attributes used for itemization/shaping */

  RunInfo *run_info;		/* Array of information about each run */
  int n_runs;

  int total_width;		/* Original width of line in Pango units */
  int gap_center;		/* Goal for center of gap */

  PangoGlyphItem *ellipsis_run;	/* Run created to hold ellipsis */
  int ellipsis_width;		/* Width of ellipsis, in Pango units */
  int ellipsis_is_cjk;		/* Whether the first character in the ellipsized
				 * is wide; this triggers us to try to use a
				 * mid-line ellipsis instead of a baseline
				 */

  PangoAttrIterator *line_start_attr; /* Cached PangoAttrIterator for the start of the run */

  LineIter gap_start_iter;	/* Iteratator pointig to the first cluster in gap */
  int gap_start_x;		/* x position of start of gap, in Pango units */
  PangoAttrIterator *gap_start_attr; /* Attribute iterator pointing to a range containing
				      * the first character in gap */

  LineIter gap_end_iter;	/* Iterator pointing to last cluster in gap */
  int gap_end_x;		/* x position of end of gap, in Pango units */

  PangoShapeFlags shape_flags;
};

/* Compute global information needed for the itemization process
 */
static void
init_state (EllipsizeState  *state,
            PangoLayoutLine *line,
            PangoAttrList   *attrs,
            PangoShapeFlags  shape_flags)
{
  GSList *l;
  int i;
  int start_offset;

  state->layout = line->layout;
  if (attrs)
    state->attrs = pango_attr_list_ref (attrs);
  else
    state->attrs = pango_attr_list_new ();

  state->shape_flags = shape_flags;

  state->n_runs = g_slist_length (line->runs);
  state->run_info = g_new (RunInfo, state->n_runs);

  start_offset = pango_utf8_strlen (line->layout->text,
				line->start_index);

  state->total_width = 0;
  for (l = line->runs, i = 0; l; l = l->next, i++)
    {
      PangoGlyphItem *run = l->data;
      int width = pango_glyph_string_get_width (run->glyphs);
      state->run_info[i].run = run;
      state->run_info[i].width = width;
      state->run_info[i].start_offset = start_offset;
      state->total_width += width;

      start_offset += run->item->num_chars;
    }

  state->ellipsis_run = NULL;
  state->ellipsis_is_cjk = FALSE;
  state->line_start_attr = NULL;
  state->gap_start_attr = NULL;
}

/* Cleanup memory allocation
 */
static void
free_state (EllipsizeState *state)
{
  pango_attr_list_unref (state->attrs);
  if (state->line_start_attr)
    pango_attr_iterator_destroy (state->line_start_attr);
  if (state->gap_start_attr)
    pango_attr_iterator_destroy (state->gap_start_attr);
  g_free (state->run_info);
}

/* Computes the width of a single cluster
 */
static int
get_cluster_width (LineIter *iter)
{
  PangoGlyphItemIter *run_iter = &iter->run_iter;
  PangoGlyphString *glyphs = run_iter->glyph_item->glyphs;
  int width = 0;
  int i;

  if (run_iter->start_glyph < run_iter->end_glyph) /* LTR */
    {
      for (i = run_iter->start_glyph; i < run_iter->end_glyph; i++)
	width += glyphs->glyphs[i].geometry.width;
    }
  else			                 /* RTL */
    {
      for (i = run_iter->start_glyph; i > run_iter->end_glyph; i--)
	width += glyphs->glyphs[i].geometry.width;
    }

  return width;
}

/* Move forward one cluster. Returns %FALSE if we were already at the end
 */
static gboolean
line_iter_next_cluster (EllipsizeState *state,
			LineIter       *iter)
{
  if (!pango_glyph_item_iter_next_cluster (&iter->run_iter))
    {
      if (iter->run_index == state->n_runs - 1)
	return FALSE;
      else
	{
	  iter->run_index++;
	  pango_glyph_item_iter_init_start (&iter->run_iter,
					    state->run_info[iter->run_index].run,
					    state->layout->text);
	}
    }

  return TRUE;
}

/* Move backward one cluster. Returns %FALSE if we were already at the end
 */
static gboolean
line_iter_prev_cluster (EllipsizeState *state,
			LineIter       *iter)
{
  if (!pango_glyph_item_iter_prev_cluster (&iter->run_iter))
    {
      if (iter->run_index == 0)
	return FALSE;
      else
	{
	  iter->run_index--;
	  pango_glyph_item_iter_init_end (&iter->run_iter,
					  state->run_info[iter->run_index].run,
					  state->layout->text);
	}
    }

  return TRUE;
}

/*
 * An ellipsization boundary is defined by two things
 *
 * - Starts a cluster - forced by structure of code
 * - Starts a grapheme - checked here
 *
 * In the future we'd also like to add a check for cursive connectivity here.
 * This should be an addition to `PangoGlyphVisAttr`
 *
 */

/* Checks if there is a ellipsization boundary before the cluster @iter points to
 */
static gboolean
starts_at_ellipsization_boundary (EllipsizeState *state,
				  LineIter       *iter)
{
  RunInfo *run_info = &state->run_info[iter->run_index];

  if (iter->run_iter.start_char == 0 && iter->run_index == 0)
    return TRUE;

  return state->layout->log_attrs[run_info->start_offset + iter->run_iter.start_char].is_cursor_position;
}

/* Checks if there is a ellipsization boundary after the cluster @iter points to
 */
static gboolean
ends_at_ellipsization_boundary (EllipsizeState *state,
				LineIter       *iter)
{
  RunInfo *run_info = &state->run_info[iter->run_index];

  if (iter->run_iter.end_char == run_info->run->item->num_chars && iter->run_index == state->n_runs - 1)
    return TRUE;

  return state->layout->log_attrs[run_info->start_offset + iter->run_iter.end_char + 1].is_cursor_position;
}

/* Helper function to re-itemize a string of text
 */
static PangoItem *
itemize_text (EllipsizeState *state,
	      const char     *text,
	      PangoAttrList  *attrs)
{
  GList *items;
  PangoItem *item;

  items = pango_itemize (state->layout->context, text, 0, strlen (text), attrs, NULL);
  g_assert (g_list_length (items) == 1);

  item = items->data;
  g_list_free (items);

  return item;
}

/* Shapes the ellipsis using the font and is_cjk information computed by
 * update_ellipsis_shape() from the first character in the gap.
 */
static void
shape_ellipsis (EllipsizeState *state)
{
  PangoAttrList attrs;
  GSList *run_attrs;
  PangoItem *item;
  PangoGlyphString *glyphs;
  GSList *l;
  PangoAttribute *fallback;
  const char *ellipsis_text;
  int len;
  int i;

  _pango_attr_list_init (&attrs);

  /* Create/reset state->ellipsis_run
   */
  if (!state->ellipsis_run)
    {
      state->ellipsis_run = g_slice_new0 (PangoGlyphItem);
      state->ellipsis_run->glyphs = pango_glyph_string_new ();
    }

  if (state->ellipsis_run->item)
    {
      pango_item_free (state->ellipsis_run->item);
      state->ellipsis_run->item = NULL;
    }

  /* Create an attribute list
   */
  run_attrs = pango_attr_iterator_get_attrs (state->gap_start_attr);
  for (l = run_attrs; l; l = l->next)
    {
      PangoAttribute *attr = l->data;
      attr->start_index = 0;
      attr->end_index = G_MAXINT;

      pango_attr_list_insert (&attrs, attr);
    }

  g_slist_free (run_attrs);

  fallback = pango_attr_fallback_new (FALSE);
  fallback->start_index = 0;
  fallback->end_index = G_MAXINT;
  pango_attr_list_insert (&attrs, fallback);

  /* First try using a specific ellipsis character in the best matching font
   */
  if (state->ellipsis_is_cjk)
    ellipsis_text = "\342\213\257";	/* U+22EF: MIDLINE HORIZONTAL ELLIPSIS, used for CJK */
  else
    ellipsis_text = "\342\200\246";	/* U+2026: HORIZONTAL ELLIPSIS */

  item = itemize_text (state, ellipsis_text, &attrs);

  /* If that fails we use "..." in the first matching font
   */
  if (!item->analysis.font ||
      !pango_font_has_char (item->analysis.font,
                            g_utf8_get_char (ellipsis_text)))
    {
      pango_item_free (item);

      /* Modify the fallback iter while it is inside the PangoAttrList; Don't try this at home
       */
      ((PangoAttrInt *)fallback)->value = TRUE;

      ellipsis_text = "...";
      item = itemize_text (state, ellipsis_text, &attrs);
    }

  _pango_attr_list_destroy (&attrs);

  state->ellipsis_run->item = item;

  /* Now shape
   */
  glyphs = state->ellipsis_run->glyphs;

  len = strlen (ellipsis_text);
  pango_shape_with_flags (ellipsis_text, len,
                          ellipsis_text, len,
	                  &item->analysis, glyphs,
                          state->shape_flags);

  state->ellipsis_width = 0;
  for (i = 0; i < glyphs->num_glyphs; i++)
    state->ellipsis_width += glyphs->glyphs[i].geometry.width;
}

/* Helper function to advance a PangoAttrIterator to a particular
 * byte index.
 */
static void
advance_iterator_to (PangoAttrIterator *iter,
		     int                new_index)
{
  int start, end;

  do
    {
      pango_attr_iterator_range (iter, &start, &end);
      if (end > new_index)
	break;
    }
  while (pango_attr_iterator_next (iter));
}

/* Updates the shaping of the ellipsis if necessary when we move the
 * position of the start of the gap.
 *
 * The shaping of the ellipsis is determined by two things:
 *
 * - The font attributes applied to the first character in the gap
 * - Whether the first character in the gap is wide or not. If the
 *   first character is wide, then we assume that we are ellipsizing
 *   East-Asian text, so prefer a mid-line ellipsizes to a baseline
 *   ellipsis, since that's typical practice for Chinese/Japanese/Korean.
 */
static void
update_ellipsis_shape (EllipsizeState *state)
{
  gboolean recompute = FALSE;
  gunichar start_wc;
  gboolean is_cjk;

  /* Unfortunately, we can only advance PangoAttrIterator forward; so each
   * time we back up we need to go forward to find the new position. To make
   * this not utterly slow, we cache an iterator at the start of the line
   */
  if (!state->line_start_attr)
    {
      state->line_start_attr = pango_attr_list_get_iterator (state->attrs);
      advance_iterator_to (state->line_start_attr, state->run_info[0].run->item->offset);
    }

  if (state->gap_start_attr)
    {
      /* See if the current attribute range contains the new start position
       */
      int start, end;

      pango_attr_iterator_range (state->gap_start_attr, &start, &end);

      if (state->gap_start_iter.run_iter.start_index < start)
	{
	  pango_attr_iterator_destroy (state->gap_start_attr);
	  state->gap_start_attr = NULL;
	}
    }

  /* Check whether we need to recompute the ellipsis because of new font attributes
   */
  if (!state->gap_start_attr)
    {
      state->gap_start_attr = pango_attr_iterator_copy (state->line_start_attr);
      advance_iterator_to (state->gap_start_attr,
			   state->run_info[state->gap_start_iter.run_index].run->item->offset);

      recompute = TRUE;
    }

  /* Check whether we need to recompute the ellipsis because we switch from CJK to not
   * or vice-versa
   */
  start_wc = g_utf8_get_char (state->layout->text + state->gap_start_iter.run_iter.start_index);
  is_cjk = g_unichar_iswide (start_wc);

  if (is_cjk != state->ellipsis_is_cjk)
    {
      state->ellipsis_is_cjk = is_cjk;
      recompute = TRUE;
    }

  if (recompute)
    shape_ellipsis (state);
}

/* Computes the position of the gap center and finds the smallest span containing it
 */
static void
find_initial_span (EllipsizeState *state)
{
  PangoGlyphItem *glyph_item;
  PangoGlyphItemIter *run_iter;
  gboolean have_cluster;
  int i;
  int x;
  int cluster_width;

  switch (state->layout->ellipsize)
    {
    case PANGO_ELLIPSIZE_NONE:
    default:
      g_assert_not_reached ();
    case PANGO_ELLIPSIZE_START:
      state->gap_center = 0;
      break;
    case PANGO_ELLIPSIZE_MIDDLE:
      state->gap_center = state->total_width / 2;
      break;
    case PANGO_ELLIPSIZE_END:
      state->gap_center = state->total_width;
      break;
    }

  /* Find the run containing the gap center
   */
  x = 0;
  for (i = 0; i < state->n_runs; i++)
    {
      if (x + state->run_info[i].width > state->gap_center)
	break;

      x += state->run_info[i].width;
    }

  if (i == state->n_runs)	/* Last run is a closed interval, so back off one run */
    {
      i--;
      x -= state->run_info[i].width;
    }

  /* Find the cluster containing the gap center
   */
  state->gap_start_iter.run_index = i;
  run_iter = &state->gap_start_iter.run_iter;
  glyph_item = state->run_info[i].run;

  cluster_width = 0;		/* Quiet GCC, the line must have at least one cluster */
  for (have_cluster = pango_glyph_item_iter_init_start (run_iter, glyph_item, state->layout->text);
       have_cluster;
       have_cluster = pango_glyph_item_iter_next_cluster (run_iter))
    {
      cluster_width = get_cluster_width (&state->gap_start_iter);

      if (x + cluster_width > state->gap_center)
	break;

      x += cluster_width;
    }

  if (!have_cluster)	/* Last cluster is a closed interval, so back off one cluster */
    x -= cluster_width;

  state->gap_end_iter = state->gap_start_iter;

  state->gap_start_x = x;
  state->gap_end_x = x + cluster_width;

  /* Expand the gap to a full span
   */
  while (!starts_at_ellipsization_boundary (state, &state->gap_start_iter))
    {
      line_iter_prev_cluster (state, &state->gap_start_iter);
      state->gap_start_x -= get_cluster_width (&state->gap_start_iter);
    }

  while (!ends_at_ellipsization_boundary (state, &state->gap_end_iter))
    {
      line_iter_next_cluster (state, &state->gap_end_iter);
      state->gap_end_x += get_cluster_width (&state->gap_end_iter);
    }

  update_ellipsis_shape (state);
}

/* Removes one run from the start or end of the gap. Returns FALSE
 * if there's nothing left to remove in either direction.
 */
static gboolean
remove_one_span (EllipsizeState *state)
{
  LineIter new_gap_start_iter;
  LineIter new_gap_end_iter;
  int new_gap_start_x;
  int new_gap_end_x;
  int width;

  /* Find one span backwards and forward from the gap
   */
  new_gap_start_iter = state->gap_start_iter;
  new_gap_start_x = state->gap_start_x;
  do
    {
      if (!line_iter_prev_cluster (state, &new_gap_start_iter))
	break;
      width = get_cluster_width (&new_gap_start_iter);
      new_gap_start_x -= width;
    }
  while (!starts_at_ellipsization_boundary (state, &new_gap_start_iter) ||
	 width == 0);

  new_gap_end_iter = state->gap_end_iter;
  new_gap_end_x = state->gap_end_x;
  do
    {
      if (!line_iter_next_cluster (state, &new_gap_end_iter))
	break;
      width = get_cluster_width (&new_gap_end_iter);
      new_gap_end_x += width;
    }
  while (!ends_at_ellipsization_boundary (state, &new_gap_end_iter) ||
	 width == 0);

  if (state->gap_end_x == new_gap_end_x && state->gap_start_x == new_gap_start_x)
    return FALSE;

  /* In the case where we could remove a span from either end of the
   * gap, we look at which causes the smaller increase in the
   * MAX (gap_end - gap_center, gap_start - gap_center)
   */
  if (state->gap_end_x == new_gap_end_x ||
      (state->gap_start_x != new_gap_start_x &&
       state->gap_center - new_gap_start_x < new_gap_end_x - state->gap_center))
    {
      state->gap_start_iter = new_gap_start_iter;
      state->gap_start_x = new_gap_start_x;

      update_ellipsis_shape (state);
    }
  else
    {
      state->gap_end_iter = new_gap_end_iter;
      state->gap_end_x = new_gap_end_x;
    }

  return TRUE;
}

/* Fixes up the properties of the ellipsis run once we've determined the final extents
 * of the gap
 */
static void
fixup_ellipsis_run (EllipsizeState *state,
                    int             extra_width)
{
  PangoGlyphString *glyphs = state->ellipsis_run->glyphs;
  PangoItem *item = state->ellipsis_run->item;
  int level;
  int i;

  /* Make the entire glyphstring into a single logical cluster */
  for (i = 0; i < glyphs->num_glyphs; i++)
    {
      glyphs->log_clusters[i] = 0;
      glyphs->glyphs[i].attr.is_cluster_start = FALSE;
    }

  glyphs->glyphs[0].attr.is_cluster_start = TRUE;

  glyphs->glyphs[glyphs->num_glyphs - 1].geometry.width += extra_width;

  /* Fix up the item to point to the entire elided text */
  item->offset = state->gap_start_iter.run_iter.start_index;
  item->length = state->gap_end_iter.run_iter.end_index - item->offset;
  item->num_chars = pango_utf8_strlen (state->layout->text + item->offset, item->length);

  /* The level for the item is the minimum level of the elided text */
  level = G_MAXINT;
  for (i = state->gap_start_iter.run_index; i <= state->gap_end_iter.run_index; i++)
    level = MIN (level, state->run_info[i].run->item->analysis.level);

  item->analysis.level = level;

  item->analysis.flags |= PANGO_ANALYSIS_FLAG_IS_ELLIPSIS;
}

/* Computes the new list of runs for the line
 */
static GSList *
get_run_list (EllipsizeState *state)
{
  PangoGlyphItem *partial_start_run = NULL;
  PangoGlyphItem *partial_end_run = NULL;
  GSList *result = NULL;
  RunInfo *run_info;
  PangoGlyphItemIter *run_iter;
  int i;

  /* We first cut out the pieces of the starting and ending runs we want to
   * preserve; we do the end first in case the end and the start are
   * the same. Doing the start first would disturb the indices for the end.
   */
  run_info = &state->run_info[state->gap_end_iter.run_index];
  run_iter = &state->gap_end_iter.run_iter;
  if (run_iter->end_char != run_info->run->item->num_chars)
    {
      partial_end_run = run_info->run;
      run_info->run = pango_glyph_item_split (run_info->run, state->layout->text,
					      run_iter->end_index - run_info->run->item->offset);
    }

  run_info = &state->run_info[state->gap_start_iter.run_index];
  run_iter = &state->gap_start_iter.run_iter;
  if (run_iter->start_char != 0)
    {
      partial_start_run = pango_glyph_item_split (run_info->run, state->layout->text,
						  run_iter->start_index - run_info->run->item->offset);
    }

  /* Now assemble the new list of runs
   */
  for (i = 0; i < state->gap_start_iter.run_index; i++)
    result = g_slist_prepend (result, state->run_info[i].run);

  if (partial_start_run)
    result = g_slist_prepend (result, partial_start_run);

  result = g_slist_prepend (result, state->ellipsis_run);

  if (partial_end_run)
    result = g_slist_prepend (result, partial_end_run);

  for (i = state->gap_end_iter.run_index + 1; i < state->n_runs; i++)
    result = g_slist_prepend (result, state->run_info[i].run);

  /* And free the ones we didn't use
   */
  for (i = state->gap_start_iter.run_index; i <= state->gap_end_iter.run_index; i++)
    pango_glyph_item_free (state->run_info[i].run);

  return g_slist_reverse (result);
}

/* Computes the width of the line as currently ellipsized
 */
static int
current_width (EllipsizeState *state)
{
  return state->total_width - (state->gap_end_x - state->gap_start_x) + state->ellipsis_width;
}

/**
 * _pango_layout_line_ellipsize:
 * @line: a `PangoLayoutLine`
 * @attrs: Attributes being used for itemization/shaping
 * @shape_flags: Flags to use when shaping
 *
 * Given a `PangoLayoutLine` with the runs still in logical order, ellipsize
 * it according the layout's policy to fit within the set width of the layout.
 *
 * Return value: whether the line had to be ellipsized
 **/
gboolean
_pango_layout_line_ellipsize (PangoLayoutLine *line,
			      PangoAttrList   *attrs,
                              PangoShapeFlags  shape_flags,
			      int              goal_width)
{
  EllipsizeState state;
  gboolean is_ellipsized = FALSE;

  g_return_val_if_fail (line->layout->ellipsize != PANGO_ELLIPSIZE_NONE && goal_width >= 0, is_ellipsized);

  init_state (&state, line, attrs, shape_flags);

  if (state.total_width <= goal_width)
    goto out;

  find_initial_span (&state);

  while (current_width (&state) > goal_width)
    {
      if (!remove_one_span (&state))
	break;
    }

  fixup_ellipsis_run (&state, MAX (goal_width - current_width (&state), 0));

  g_slist_free (line->runs);
  line->runs = get_run_list (&state);
  is_ellipsized = TRUE;

 out:
  free_state (&state);

  return is_ellipsized;
}
