/* Pango
 * break-indic.c:
 *
 * Copyright (C) 2006 Red Hat Software
 * Author: Akira TAGOH <tagoh@redhat.com>
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

#define DEV_RRA 0x0931 /* 0930 + 093c */
#define DEV_QA 0x0958 /* 0915 + 093c */
#define DEV_YA 0x095F /* 092f + 003c */
#define DEV_KHHA 0x0959
#define DEV_GHHA 0x095A
#define DEV_ZA 0x095B
#define DEV_DDDHA 0x095C
#define DEV_RHA 0x095D
#define DEV_FA 0x095E
#define DEV_YYA 0x095F

/* Bengali */
/* for split matras in all brahmi based script */
#define BENGALI_SIGN_O 0x09CB  /* 09c7 + 09be */
#define BENGALI_SIGN_AU 0x09CC /* 09c7 + 09d7 */
#define BENGALI_RRA 0x09DC
#define BENGALI_RHA 0x09DD
#define BENGALI_YYA 0x09DF

/* Gurumukhi */
#define GURUMUKHI_LLA 0x0A33
#define GURUMUKHI_SHA 0x0A36
#define GURUMUKHI_KHHA 0x0A59
#define GURUMUKHI_GHHA 0x0A5A
#define GURUMUKHI_ZA 0x0A5B
#define GURUMUKHI_RRA 0x0A5C
#define GURUMUKHI_FA 0x0A5E

/* Oriya */
#define ORIYA_AI 0x0B48
#define ORIYA_O 0x0B4B
#define ORIYA_AU 0x0B4C

/* Telugu */
#define TELUGU_EE 0x0C47
#define TELUGU_AI 0x0C48

/* Tamil */
#define TAMIL_O 0x0BCA
#define TAMIL_OO 0x0BCB
#define TAMIL_AU 0x0BCC

/* Kannada */
#define KNDA_EE 0x0CC7
#define KNDA_AI 0x0CC8
#define KNDA_O 0x0CCA
#define KNDA_OO 0x0CCB

/* Malayalam */
#define MLYM_O 0x0D4A
#define MLYM_OO 0x0D4B
#define MLYM_AU 0x0D4C

#define IS_COMPOSITE_WITH_BRAHMI_NUKTA(c) ( \
	(c >= BENGALI_RRA  && c <= BENGALI_YYA) || \
	(c >= DEV_QA  && c <= DEV_YA) || (c == DEV_RRA) || (c >= DEV_KHHA  && c <= DEV_YYA) || \
	(c >= KNDA_EE  && c <= KNDA_AI) ||(c >= KNDA_O  && c <= KNDA_OO) || \
	(c == TAMIL_O) || (c == TAMIL_OO) || (c == TAMIL_AU) || \
	(c == TELUGU_EE) || (c == TELUGU_AI) || \
	(c == ORIYA_AI) || (c == ORIYA_O) || (c == ORIYA_AU) || \
	(c >= GURUMUKHI_KHHA  && c <= GURUMUKHI_RRA) || (c == GURUMUKHI_FA)|| (c == GURUMUKHI_LLA)|| (c == GURUMUKHI_SHA) || \
	FALSE)
#define IS_SPLIT_MATRA_BRAHMI(c) ( \
	(c == BENGALI_SIGN_O) || (c == BENGALI_SIGN_AU) || \
	(c >= MLYM_O  && c <= MLYM_AU) || \
	FALSE)

static void
not_cursor_position (PangoLogAttr *attr)
{
  if (!attr->is_mandatory_break)
    {
      attr->is_cursor_position = FALSE;
      attr->is_char_break = FALSE;
      attr->is_line_break = FALSE;
      attr->is_mandatory_break = FALSE;
    }
}

static void
break_indic (const char          *text,
	     int                  length,
	     const PangoAnalysis *analysis,
	     PangoLogAttr        *attrs,
	     int                  attrs_len G_GNUC_UNUSED)
{
  const gchar *p, *next = NULL, *next_next;
  gunichar prev_wc, this_wc, next_wc, next_next_wc;
  gboolean is_conjunct = FALSE;
  int i;

  for (p = text, prev_wc = 0, i = 0;
       p != NULL && p < (text + length);
       p = next, prev_wc = this_wc, i++)
    {
      this_wc = g_utf8_get_char (p);
      next = g_utf8_next_char (p);

    if (G_UNLIKELY (
               IS_COMPOSITE_WITH_BRAHMI_NUKTA(this_wc) || IS_SPLIT_MATRA_BRAHMI(this_wc))) {
         attrs[i+1].backspace_deletes_character = FALSE;
      }

      if (next != NULL && next < (text + length))
	{
	  next_wc = g_utf8_get_char (next);
	  next_next = g_utf8_next_char (next);
	}
      else
	{
	  next_wc = 0;
	  next_next = NULL;
	}
      if (next_next != NULL && next_next < (text + length))
	next_next_wc = g_utf8_get_char (next_next);
      else
	next_next_wc = 0;

      switch (analysis->script)
      {
        case PANGO_SCRIPT_SINHALA:
	  /*
	   * TODO: The cursor position should be based on the state table.
	   *       This is the wrong place to be doing this.
	   */

	  /*
	   * The cursor should treat as a single glyph:
	   * SINHALA CONS + 0x0DCA + 0x200D + SINHALA CONS
	   * SINHALA CONS + 0x200D + 0x0DCA + SINHALA CONS
	   */
	  if ((this_wc == 0x0DCA && next_wc == 0x200D)
	      || (this_wc == 0x200D && next_wc == 0x0DCA))
	    {
	      not_cursor_position(&attrs[i]);
	      not_cursor_position(&attrs[i + 1]);
	      is_conjunct = TRUE;
	    }
	  else if (is_conjunct
		   && (prev_wc == 0x200D || prev_wc == 0x0DCA)
		   && this_wc >= 0x0D9A
		   && this_wc <= 0x0DC6)
	    {
	      not_cursor_position(&attrs[i]);
	      is_conjunct = FALSE;
	    }
	  /*
	   * Consonant clusters do NOT result in implicit conjuncts
	   * in SINHALA orthography.
	   */
	  else if (!is_conjunct && prev_wc == 0x0DCA && this_wc != 0x200D)
	    {
	      attrs[i].is_cursor_position = TRUE;
	    }

	  break;

	default:

	  if (prev_wc != 0 && (this_wc == 0x200D || this_wc == 0x200C))
	    {
	      not_cursor_position(&attrs[i]);
	      if (next_wc != 0)
		{
		  not_cursor_position(&attrs[i+1]);
		  if ((next_next_wc != 0) &&
		       (next_wc == 0x09CD ||	/* Bengali */
			next_wc == 0x0ACD ||	/* Gujarati */
			next_wc == 0x094D ||	/* Hindi */
			next_wc == 0x0CCD ||	/* Kannada */
			next_wc == 0x0D4D ||	/* Malayalam */
			next_wc == 0x0B4D ||	/* Oriya */
			next_wc == 0x0A4D ||	/* Punjabi */
			next_wc == 0x0BCD ||	/* Tamil */
			next_wc == 0x0C4D))	/* Telugu */
		    {
		      not_cursor_position(&attrs[i+2]);
		    }
		}
	    }

	  break;
      }
    }
}
