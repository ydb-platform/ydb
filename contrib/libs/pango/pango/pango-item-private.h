/* Pango
 *
 * Copyright (C) 2021 Matthias Clasen
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

#ifndef __PANGO_ITEM_PRIVATE_H__
#define __PANGO_ITEM_PRIVATE_H__

#include <pango/pango-item.h>
#include <pango/pango-break.h>

G_BEGIN_DECLS

/**
 * We have to do some extra work for adding the char_offset field
 * to PangoItem to preserve ABI in the face of pango's open-coded
 * structs.
 *
 * Internally, pango uses the PangoItemPrivate type, and we use
 * a bit in the PangoAnalysis flags to indicate whether we are
 * dealing with a PangoItemPrivate struct or not.
 */

#define PANGO_ANALYSIS_FLAG_HAS_CHAR_OFFSET (1 << 7)

typedef struct _PangoAnalysisPrivate PangoAnalysisPrivate;

struct _PangoAnalysisPrivate
{
  gpointer reserved;
  PangoFont *size_font;
  PangoFont *font;

  guint8 level;
  guint8 gravity;
  guint8 flags;

  guint8 script;
  PangoLanguage *language;

  GSList *extra_attrs;
};

typedef struct _PangoItemPrivate PangoItemPrivate;

#if defined(__x86_64__) && !defined(__ILP32__)

struct _PangoItemPrivate
{
  int offset;
  int length;
  int num_chars;
  int char_offset;
  PangoAnalysis analysis;
};

#else

struct _PangoItemPrivate
{
  int offset;
  int length;
  int num_chars;
  PangoAnalysis analysis;
  int char_offset;
};

#endif

G_STATIC_ASSERT (offsetof (PangoItem, offset) == offsetof (PangoItemPrivate, offset));
G_STATIC_ASSERT (offsetof (PangoItem, length) == offsetof (PangoItemPrivate, length));
G_STATIC_ASSERT (offsetof (PangoItem, num_chars) == offsetof (PangoItemPrivate, num_chars));
G_STATIC_ASSERT (offsetof (PangoItem, analysis) == offsetof (PangoItemPrivate, analysis));

void               pango_analysis_collect_features    (const PangoAnalysis        *analysis,
                                                       hb_feature_t               *features,
                                                       guint                       length,
                                                       guint                      *num_features);

void               pango_analysis_set_size_font       (PangoAnalysis              *analysis,
                                                       PangoFont                  *font);
PangoFont *        pango_analysis_get_size_font       (const PangoAnalysis        *analysis);

GList *            pango_itemize_with_font            (PangoContext               *context,
                                                       PangoDirection              base_dir,
                                                       const char                 *text,
                                                       int                         start_index,
                                                       int                         length,
                                                       PangoAttrList              *attrs,
                                                       PangoAttrIterator          *cached_iter,
                                                       const PangoFontDescription *desc);

GList *            pango_itemize_post_process_items   (PangoContext               *context,
                                                       const char                 *text,
                                                       PangoLogAttr               *log_attrs,
                                                       GList                      *items);

void               pango_item_unsplit                 (PangoItem *orig,
                                                       int        split_index,
                                                       int        split_offset);


G_END_DECLS

#endif /* __PANGO_ITEM_PRIVATE_H__ */
