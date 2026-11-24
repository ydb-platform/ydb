/* Pango
 * break-thai.c:
 *
 * Copyright (C) 2003 Theppitak Karoonboonyanan <thep@linux.thai.net>
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
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 */

#include "config.h"

#include "pango-break.h"
#include "pango-impl-utils.h"

#ifdef HAVE_LIBTHAI
#error #include <thai/thwchar.h>
#error #include <thai/thbrk.h>

G_LOCK_DEFINE_STATIC (thai_brk);

#ifdef HAVE_TH_BRK_FIND_BREAKS
static ThBrk *thai_brk = NULL;
#endif

/*
 * tis_text is assumed to be large enough to hold the converted string,
 * i.e. it must be at least pango_utf8_strlen(text, len)+1 bytes.
 */
static thchar_t *
utf8_to_tis (const char *text, int len, thchar_t *tis_text, int *tis_cnt)
{
  thchar_t   *tis_p;
  const char *text_p;

  tis_p = tis_text;
  for (text_p = text; text_p < text + len; text_p = g_utf8_next_char (text_p))
    *tis_p++ = th_uni2tis (g_utf8_get_char (text_p));
  *tis_p++ = '\0';

  *tis_cnt = tis_p - tis_text;
  return tis_text;
}

#endif
static void
break_thai (const char          *text,
	    int                  len,
	    const PangoAnalysis *analysis G_GNUC_UNUSED,
	    PangoLogAttr        *attrs,
	    int                  attrs_len G_GNUC_UNUSED)
{
#ifdef HAVE_LIBTHAI
  thchar_t tis_stack[512];
  int brk_stack[512];
  thchar_t *tis_text;
  int *brk_pnts;
  int cnt;

  cnt = pango_utf8_strlen (text, len) + 1;

  tis_text = tis_stack;
  if (cnt > (int) G_N_ELEMENTS (tis_stack))
    tis_text = g_new (thchar_t, cnt);

  utf8_to_tis (text, len, tis_text, &cnt);

  brk_pnts = brk_stack;
  if (cnt > (int) G_N_ELEMENTS (brk_stack))
    brk_pnts = g_new (int, cnt);

  /* find line break positions */

  G_LOCK (thai_brk);
#ifdef HAVE_TH_BRK_FIND_BREAKS
  if (thai_brk == NULL)
    thai_brk = th_brk_new (NULL);
  len = th_brk_find_breaks (thai_brk, tis_text, brk_pnts, cnt);
#else
  len = th_brk (tis_text, brk_pnts, cnt);
#endif
  G_UNLOCK (thai_brk);

  for (cnt = 0; cnt < len; cnt++)
    {
      if (!attrs[brk_pnts[cnt]].is_line_break)
        {
          /* Insert line breaks where there wasn't one.
           * Satisfy invariants by marking it as char break too.
           */
          attrs[brk_pnts[cnt]].is_char_break = TRUE;
          attrs[brk_pnts[cnt]].is_line_break = TRUE;
        }
      if (!(attrs[brk_pnts[cnt]].is_word_start ||
            attrs[brk_pnts[cnt]].is_word_end))
        {
          /* If we find a break in the middle of a sequence
           * of characters, end and start a word. We must
           * be careful only to do that if default_break
           * did not already find a word start or end,
           * otherwise we mess up the sequence.
           */
          attrs[brk_pnts[cnt]].is_word_start = TRUE;
          attrs[brk_pnts[cnt]].is_word_end = TRUE;
        }
    }

  if (brk_pnts != brk_stack)
    g_free (brk_pnts);

  if (tis_text != tis_stack)
    g_free (tis_text);
#endif
}
