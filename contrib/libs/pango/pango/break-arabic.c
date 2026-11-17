/* Pango
 * break-arabic.c:
 *
 * Copyright (C) 2006 Red Hat Software
 * Copyright (C) 2006 Sharif FarsiWeb, Inc.
 * Authors: Behdad Esfahbod <besfahbo@redhat.com>
 *          Roozbeh Pournader <roozbeh@farsiweb.info>
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

#include "pango-break.h"

#define ALEF_WITH_MADDA_ABOVE	0x0622
#define YEH_WITH_HAMZA_ABOVE	0x0626
#define ALEF			0x0627
#define WAW			0x0648
#define YEH			0x064A

#define MADDAH_ABOVE		0x0653
#define HAMZA_ABOVE		0x0654
#define HAMZA_BELOW		0x0655

/*
 * Arabic characters with canonical decompositions that are not just
 * ligatures.  The characters U+06C0, U+06C2, and U+06D3 are intentionally
 * excluded as they are marked as "not an independent letter" in Unicode
 * Character Database's NamesList.txt
 */
#define IS_COMPOSITE(c) (ALEF_WITH_MADDA_ABOVE <= (c) && (c) <= YEH_WITH_HAMZA_ABOVE)

/* If a character is the second part of a composite Arabic character with an Alef */
#define IS_COMPOSITE_WITH_ALEF(c) (MADDAH_ABOVE <= (c) && (c) <= HAMZA_BELOW)

static void
break_arabic (const char          *text,
	      int                  length,
	      const PangoAnalysis *analysis G_GNUC_UNUSED,
	      PangoLogAttr        *attrs,
	      int                  attrs_len G_GNUC_UNUSED)
{
  int i;
  const char *p;
  gunichar prev_wc, this_wc;

  /* See http://bugzilla.gnome.org/show_bug.cgi?id=350132 for issues this
   * module tries to solve.
   */

  for (p = text, i = 0, prev_wc = 0;
       p < text + length;
       p = g_utf8_next_char (p), i++, prev_wc = this_wc)
    {
      this_wc = g_utf8_get_char (p);

      /*
       * Unset backspace_deletes_character for various combinations.
       *
       * A few more combinations may need to be handled here, but are not
       * handled yet, as expectations of users is not known or may differ
       * among different languages or users:
       * some letters combined with U+0658 ARABIC MARK NOON GHUNNA;
       * combinations considered one letter in Azerbaijani (WAW+SUKUN and
       * FARSI_YEH+HAMZA_ABOVE); combinations of YEH and ALEF_MAKSURA with
       * HAMZA_BELOW (Qur'anic); TATWEEL+HAMZA_ABOVE (Qur'anic).
       *
       * FIXME: Ordering these in some other way may lower the time spent here, or not.
       */
      if (G_UNLIKELY (
	   IS_COMPOSITE (this_wc) ||
	  (prev_wc == ALEF && IS_COMPOSITE_WITH_ALEF (this_wc)) ||
	  (this_wc == HAMZA_ABOVE && (prev_wc == WAW || prev_wc == YEH))
	 ))
	attrs[i+1].backspace_deletes_character = FALSE;
    }
}
