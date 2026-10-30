/* Pango
 * pango-emoji.c: Emoji handling
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
 *
 * Implementation of pango_emoji_iter is based on Chromium's Ragel-based
 * parser:
 *
 * https://chromium-review.googlesource.com/c/chromium/src/+/1264577
 *
 * The grammar file emoji_presentation_scanner.rl was just modified to
 * adapt the function signature and variables to our usecase.  The
 * grammar itself was NOT modified:
 *
 * https://chromium-review.googlesource.com/c/chromium/src/+/1264577/3/third_party/blink/renderer/platform/fonts/emoji_presentation_scanner.rl
 *
 * The emoji_presentation_scanner.c is generated from .rl file by
 * running ragel on it.
 *
 * The categorization is also based on:
 *
 * https://chromium-review.googlesource.com/c/chromium/src/+/1264577/3/third_party/blink/renderer/platform/fonts/utf16_ragel_iterator.h
 *
 * The iterator next() is based on:
 *
 * https://chromium-review.googlesource.com/c/chromium/src/+/1264577/3/third_party/blink/renderer/platform/fonts/symbols_iterator.cc
 *
 * // Copyright 2015 The Chromium Authors. All rights reserved.
 * // Use of this source code is governed by a BSD-style license that can be
 * // found in the LICENSE file.
 */

#include "config.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "pango-emoji-private.h"
#include "pango-emoji-table.h"

static inline gboolean
bsearch_interval (gunichar              c,
                  const struct Interval table[],
                  guint                 n)
{
  guint lower = 0;
  guint upper = n - 1;

  while (lower <= upper)
    {
      int mid = (lower + upper) / 2;

      if (c < table[mid].start)
        upper = mid - 1;
      else if (c > table[mid].end)
        lower = mid + 1;
      else
        return TRUE;
    }

  return FALSE;
}

#define DEFINE_pango_Is_(name) \
static inline gboolean \
_pango_Is_##name (gunichar ch) \
{ \
  return ch >= _pango_##name##_table[0].start && \
         bsearch_interval (ch, \
                           _pango_##name##_table, \
                           G_N_ELEMENTS (_pango_##name##_table)); \
}

DEFINE_pango_Is_(Emoji)
DEFINE_pango_Is_(Emoji_Presentation)
DEFINE_pango_Is_(Emoji_Modifier)
DEFINE_pango_Is_(Emoji_Modifier_Base)
DEFINE_pango_Is_(Extended_Pictographic)

gboolean
_pango_Is_Emoji_Base_Character (gunichar ch)
{
	return _pango_Is_Emoji (ch);
}

gboolean
_pango_Is_Emoji_Extended_Pictographic (gunichar ch)
{
	return _pango_Is_Extended_Pictographic (ch);
}

static inline gboolean
_pango_Is_Emoji_Emoji_Default (gunichar ch)
{
  return _pango_Is_Emoji_Presentation (ch);
}

static inline gboolean
_pango_Is_Emoji_Keycap_Base (gunichar ch)
{
  return (ch >= '0' && ch <= '9') || ch == '#' || ch == '*';
}

static inline gboolean
_pango_Is_Regional_Indicator (gunichar ch)
{
  return (ch >= 0x1F1E6 && ch <= 0x1F1FF);
}


#define kCombiningEnclosingCircleBackslashCharacter 0x20E0
#define kCombiningEnclosingKeycapCharacter 0x20E3
#define kVariationSelector15Character 0xFE0E
#define kVariationSelector16Character 0xFE0F
#define kZeroWidthJoinerCharacter 0x200D

enum PangoEmojiScannerCategory {
  EMOJI = 0,
  EMOJI_TEXT_PRESENTATION = 1,
  EMOJI_EMOJI_PRESENTATION = 2,
  EMOJI_MODIFIER_BASE = 3,
  EMOJI_MODIFIER = 4,
  EMOJI_VS_BASE = 5,
  REGIONAL_INDICATOR = 6,
  KEYCAP_BASE = 7,
  COMBINING_ENCLOSING_KEYCAP = 8,
  COMBINING_ENCLOSING_CIRCLE_BACKSLASH = 9,
  ZWJ = 10,
  VS15 = 11,
  VS16 = 12,
  TAG_BASE = 13,
  TAG_SEQUENCE = 14,
  TAG_TERM = 15,
  kMaxEmojiScannerCategory = 16
};

static inline unsigned char
_pango_EmojiSegmentationCategory (gunichar codepoint)
{
  /* Specific ones first. */
  if (('a' <= codepoint && codepoint <= 'z') ||
      ('A' <= codepoint && codepoint <= 'Z') ||
      codepoint == ' ')
    return kMaxEmojiScannerCategory;

  if ('0' <= codepoint && codepoint <= '9')
    return KEYCAP_BASE;

  switch (codepoint)
    {
    case kCombiningEnclosingKeycapCharacter:
      return COMBINING_ENCLOSING_KEYCAP;
    case kCombiningEnclosingCircleBackslashCharacter:
      return COMBINING_ENCLOSING_CIRCLE_BACKSLASH;
    case kZeroWidthJoinerCharacter:
      return ZWJ;
    case kVariationSelector15Character:
      return VS15;
    case kVariationSelector16Character:
      return VS16;
    case 0x1F3F4:
      return TAG_BASE;
    case 0xE007F:
      return TAG_TERM;
    default: ;
    }

  if ((0xE0030 <= codepoint && codepoint <= 0xE0039) ||
      (0xE0061 <= codepoint && codepoint <= 0xE007A))
    return TAG_SEQUENCE;

  if (_pango_Is_Emoji_Modifier_Base (codepoint))
    return EMOJI_MODIFIER_BASE;
  if (_pango_Is_Emoji_Modifier (codepoint))
    return EMOJI_MODIFIER;
  if (_pango_Is_Regional_Indicator (codepoint))
    return REGIONAL_INDICATOR;
  if (_pango_Is_Emoji_Keycap_Base (codepoint))
    return KEYCAP_BASE;
  if (_pango_Is_Emoji_Emoji_Default (codepoint))
    return EMOJI_EMOJI_PRESENTATION;
  if (_pango_Is_Emoji (codepoint))
    return EMOJI_TEXT_PRESENTATION;

  /* Ragel state machine will interpret unknown category as "any". */
  return kMaxEmojiScannerCategory;
}


typedef unsigned char *emoji_text_iter_t;

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wswitch-default"
#include "emoji_presentation_scanner.c"
#pragma GCC diagnostic pop


PangoEmojiIter *
_pango_emoji_iter_init (PangoEmojiIter *iter,
                        const char     *text,
                        int             length,
                        unsigned int    n_chars)
{
  unsigned char *types;
  unsigned int i;
  const char *p;

  if (n_chars < 64)
    types = iter->types_;
  else
    types = g_malloc (n_chars);

  p = text;
  for (i = 0; i < n_chars; i++)
  {
    types[i] = _pango_EmojiSegmentationCategory (g_utf8_get_char (p));
    p = g_utf8_next_char (p);
  }

  iter->text_start = iter->start = iter->end = text;
  if (length >= 0)
    iter->text_end = text + length;
  else
    iter->text_end = text + strlen (text);
  iter->is_emoji = FALSE;

  iter->types = types;
  iter->n_chars = n_chars;
  iter->cursor = 0;

  _pango_emoji_iter_next (iter);

  return iter;
}

void
_pango_emoji_iter_fini (PangoEmojiIter *iter)
{
  if (iter->types != iter->types_)
    g_free (iter->types);
}

gboolean
_pango_emoji_iter_next (PangoEmojiIter *iter)
{
  unsigned int old_cursor, cursor;
  bool is_emoji;

  if (iter->end >= iter->text_end)
    return FALSE;

  iter->start = iter->end;

  old_cursor = cursor = iter->cursor;
  cursor = scan_emoji_presentation (iter->types + cursor,
				    iter->types + iter->n_chars,
				    &is_emoji) - iter->types;
  do
  {
    iter->cursor = cursor;
    iter->is_emoji = is_emoji;

    if (cursor == iter->n_chars)
      break;

    cursor = scan_emoji_presentation (iter->types + cursor,
				      iter->types + iter->n_chars,
				      &is_emoji) - iter->types;
  }
  while (iter->is_emoji == is_emoji);

  iter->end = g_utf8_offset_to_pointer (iter->start, iter->cursor - old_cursor);

  return TRUE;
}


/**********************************************************
 * End of code from Chromium
 **********************************************************/
