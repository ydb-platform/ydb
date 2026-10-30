/* Pango
 * pango-emoji-private.h: Emoji handling, private definitions
 *
 * Copyright (C) 2017 Google, Inc.
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

#ifndef __PANGO_EMOJI_PRIVATE_H__
#define __PANGO_EMOJI_PRIVATE_H__

#include <glib.h>

gboolean
_pango_Is_Emoji_Base_Character (gunichar ch);

gboolean
_pango_Is_Emoji_Extended_Pictographic (gunichar ch);

typedef struct _PangoEmojiIter PangoEmojiIter;

struct _PangoEmojiIter
{
  const gchar *text_start;
  const gchar *text_end;
  const gchar *start;
  const gchar *end;
  gboolean is_emoji;

  unsigned char *types;
  unsigned char types_[64];
  unsigned int n_chars;
  unsigned int cursor;
};

PangoEmojiIter *
_pango_emoji_iter_init (PangoEmojiIter *iter,
                        const char     *text,
                        int             length,
                        unsigned int    n_chars);

gboolean
_pango_emoji_iter_next (PangoEmojiIter *iter);

void
_pango_emoji_iter_fini (PangoEmojiIter *iter);

#endif /* __PANGO_EMOJI_PRIVATE_H__ */
