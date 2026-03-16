/* Pango
 * break-latin.c:
 *
 * Copyright (C) 2021 Jordi Mas i Hernàndez <jmas@softcatala.org>
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

static void
break_latin (const char          *text,
             int                  length,
             const PangoAnalysis *analysis,
             PangoLogAttr        *attrs,
             int                  attrs_len G_GNUC_UNUSED)
{
  int i;
  const char *p, *next;
  gunichar wc, prev_wc;

  if (!analysis || !analysis->language ||
      g_ascii_strncasecmp (pango_language_to_string (analysis->language), "ca-", 3) != 0)
    return;

  for (p = text, i = 0, prev_wc = 0;
       p < text + length;
       p = next, i++, prev_wc = wc)
    {
      wc = g_utf8_get_char (p);
      next = g_utf8_next_char (p);

      /* Catalan middle dot does not break words */
      if (wc == 0x00b7)
        {
          gunichar middle_next = g_utf8_get_char (next);
          if (g_unichar_tolower (middle_next) == 'l' && g_unichar_tolower (prev_wc) == 'l')
            {
              attrs[i].is_word_end = FALSE;
              attrs[i+1].is_word_start = FALSE;
            }
        }
    }
}
