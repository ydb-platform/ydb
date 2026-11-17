/* Pango
 * pango-layout-private.h: Internal structures of PangoLayout
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

#ifndef __PANGO_LAYOUT_PRIVATE_H__
#define __PANGO_LAYOUT_PRIVATE_H__

#include <pango/pango-layout.h>

G_BEGIN_DECLS

struct _PangoLayout
{
  GObject parent_instance;

  /* If you add fields to PangoLayout be sure to update _copy()
   * unless you add a value between copy_begin and copy_end.
   */

  /* Referenced items */
  PangoContext *context;
  PangoAttrList *attrs;
  PangoFontDescription *font_desc;
  PangoTabArray *tabs;

  /* Dupped */
  gchar *text;

  /* Value fields.  These will be memcpy'd in _copy() */
  int copy_begin;

  guint serial;
  guint context_serial;

  int length;			/* length of text in bytes */
  int n_chars;		        /* number of characters in layout */
  int width;			/* wrap/ellipsize width, in device units, or -1 if not set */
  int height;			/* ellipsize width, in device units if positive, number of lines if negative */
  int indent;			/* amount by which first line should be shorter */
  int spacing;			/* spacing between lines */
  float line_spacing;           /* factor to apply to line height */

  guint justify : 1;
  guint justify_last_line : 1;
  guint alignment : 2;
  guint single_paragraph : 1;
  guint auto_dir : 1;
  guint wrap : 2;		/* PangoWrapMode */
  guint is_wrapped : 1;		/* Whether the layout has any wrapped lines */
  guint ellipsize : 2;		/* PangoEllipsizeMode */
  guint is_ellipsized : 1;	/* Whether the layout has any ellipsized lines */
  int unknown_glyphs_count;	/* number of unknown glyphs */

  /* some caching */
  guint logical_rect_cached : 1;
  guint ink_rect_cached : 1;
  PangoRectangle logical_rect;
  PangoRectangle ink_rect;
  int tab_width;		/* Cached width of a tab. -1 == not yet calculated */
  gunichar decimal;

  int copy_end;

  /* Not copied during _copy() */

  PangoLogAttr *log_attrs;	/* Logical attributes for layout's text */
  GSList *lines;
  guint line_count;		/* Number of lines in @lines. 0 if lines is %NULL */
};

typedef struct _Extents Extents;
struct _Extents
{
  /* Vertical position of the line's baseline in layout coords */
  int baseline;

  /* Line extents in layout coords */
  PangoRectangle ink_rect;
  PangoRectangle logical_rect;
};

struct _PangoLayoutIter
{
  PangoLayout *layout;
  GSList *line_list_link;
  PangoLayoutLine *line;

  /* If run is NULL, it means we're on a "virtual run"
   * at the end of the line with 0 width
   */
  GSList *run_list_link;
  PangoLayoutRun *run; /* FIXME nuke this, just keep the link */
  int index;

  /* list of Extents for each line in layout coordinates */
  Extents *line_extents;
  int line_index;

  /* Position of the current run */
  int run_x;

  /* Width and end offset of the current run */
  int run_width;
  int end_x_offset;

  /* this run is left-to-right */
  gboolean ltr;

  /* X position of the left side of the current cluster */
  int cluster_x;

  /* The width of the current cluster */
  int cluster_width;

  /* glyph offset to the current cluster start */
  int cluster_start;

  /* first glyph in the next cluster */
  int next_cluster_glyph;

  /* number of Unicode chars in current cluster */
  int cluster_num_chars;

  /* visual position of current character within the cluster */
  int character_position;

  /* the real width of layout */
  int layout_width;
};

gboolean _pango_layout_line_ellipsize (PangoLayoutLine *line,
				       PangoAttrList   *attrs,
                                       PangoShapeFlags  shape_flags,
				       int              goal_width);

void     _pango_layout_get_iter (PangoLayout     *layout,
                                 PangoLayoutIter *iter);

void     _pango_layout_iter_destroy (PangoLayoutIter *iter);

G_END_DECLS

#endif /* __PANGO_LAYOUT_PRIVATE_H__ */
