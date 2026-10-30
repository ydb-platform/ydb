/* Pango
 * break.c:
 *
 * Copyright (C) 1999 Red Hat Software
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
#include "pango-script-private.h"
#include "pango-emoji-private.h"
#include "pango-attributes-private.h"
#include "pango-break-table.h"
#include "pango-impl-utils.h"
#include <string.h>

/* {{{ Unicode line breaking and segmentation */

#define PARAGRAPH_SEPARATOR 0x2029

/* See http://www.unicode.org/unicode/reports/tr14/ if you hope
 * to understand the line breaking code.
 */

typedef enum
{
  BREAK_ALREADY_HANDLED,   /* didn't use the table */
  BREAK_PROHIBITED, /* no break, even if spaces intervene */
  BREAK_IF_SPACES,  /* "indirect break" (only if there are spaces) */
  BREAK_ALLOWED     /* "direct break" (can always break here) */
  /* TR 14 has two more break-opportunity classes,
   * "indirect break opportunity for combining marks following a space"
   * and "prohibited break for combining marks"
   * but we handle that inline in the code.
   */
} BreakOpportunity;

/* need to sync the break range to glib/gunicode.h . */
#if GLIB_CHECK_VERSION(2, 80, 0)
#define LAST_BREAK_TYPE G_UNICODE_BREAK_VIRAMA
#else
#define LAST_BREAK_TYPE G_UNICODE_BREAK_ZERO_WIDTH_JOINER
#endif
#define BREAK_TYPE_SAFE(btype)            \
	 ((btype) <= LAST_BREAK_TYPE ? (btype) : G_UNICODE_BREAK_UNKNOWN)


/*
 * Hangul Conjoining Jamo handling.
 *
 * The way we implement it is just a bit different from TR14,
 * but produces the same results.
 * The same algorithm is also used in TR29 for cluster boundaries.
 *
 */


/* An enum that works as the states of the Hangul syllables system.
 **/
typedef enum
{
  JAMO_L,	/* G_UNICODE_BREAK_HANGUL_L_JAMO */
  JAMO_V,	/* G_UNICODE_BREAK_HANGUL_V_JAMO */
  JAMO_T,	/* G_UNICODE_BREAK_HANGUL_T_JAMO */
  JAMO_LV,	/* G_UNICODE_BREAK_HANGUL_LV_SYLLABLE */
  JAMO_LVT,	/* G_UNICODE_BREAK_HANGUL_LVT_SYLLABLE */
  NO_JAMO	/* Other */
} JamoType;

/* There are Hangul syllables encoded as characters, that act like a
 * sequence of Jamos. For each character we define a JamoType
 * that the character starts with, and one that it ends with.  This
 * decomposes JAMO_LV and JAMO_LVT to simple other JAMOs.  So for
 * example, a character with LineBreak type
 * G_UNICODE_BREAK_HANGUL_LV_SYLLABLE has start=JAMO_L and end=JAMO_V.
 */
typedef struct _CharJamoProps
{
  JamoType start, end;
} CharJamoProps;

/* Map from JamoType to CharJamoProps that hold only simple
 * JamoTypes (no LV or LVT) or none.
 */
static const CharJamoProps HangulJamoProps[] = {
  {JAMO_L, JAMO_L},	/* JAMO_L */
  {JAMO_V, JAMO_V},	/* JAMO_V */
  {JAMO_T, JAMO_T},	/* JAMO_T */
  {JAMO_L, JAMO_V},	/* JAMO_LV */
  {JAMO_L, JAMO_T},	/* JAMO_LVT */
  {NO_JAMO, NO_JAMO}	/* NO_JAMO */
};

/* A character forms a syllable with the previous character if and only if:
 * JamoType(this) is not NO_JAMO and:
 *
 * HangulJamoProps[JamoType(prev)].end and
 * HangulJamoProps[JamoType(this)].start are equal,
 * or the former is one less than the latter.
 */

#define IS_JAMO(btype)              \
	((btype >= G_UNICODE_BREAK_HANGUL_L_JAMO) && \
	 (btype <= G_UNICODE_BREAK_HANGUL_LVT_SYLLABLE))
#define JAMO_TYPE(btype)      \
	(IS_JAMO(btype) ? (btype - G_UNICODE_BREAK_HANGUL_L_JAMO) : NO_JAMO)

/* Types of Japanese characters */
#define JAPANESE(wc) ((wc) >= 0x2F00 && (wc) <= 0x30FF)
#define KANJI(wc)    ((wc) >= 0x2F00 && (wc) <= 0x2FDF)
#define HIRAGANA(wc) ((wc) >= 0x3040 && (wc) <= 0x309F)
#define KATAKANA(wc) ((wc) >= 0x30A0 && (wc) <= 0x30FF)

#define LATIN(wc) (((wc) >= 0x0020 && (wc) <= 0x02AF) || ((wc) >= 0x1E00 && (wc) <= 0x1EFF))
#define CYRILLIC(wc) (((wc) >= 0x0400 && (wc) <= 0x052F))
#define GREEK(wc) (((wc) >= 0x0370 && (wc) <= 0x3FF) || ((wc) >= 0x1F00 && (wc) <= 0x1FFF))
#define KANA(wc) ((wc) >= 0x3040 && (wc) <= 0x30FF)
#define HANGUL(wc) ((wc) >= 0xAC00 && (wc) <= 0xD7A3)
#define EMOJI(wc) (_pango_Is_Emoji_Base_Character (wc))
#define MATH(wc) ((wc) >= 0x2200 && (wc) <= 0x22FF)
#define BACKSPACE_DELETES_CHARACTER(wc) (!LATIN (wc) && !CYRILLIC (wc) && !GREEK (wc) && !KANA (wc) && !HANGUL (wc) && !EMOJI (wc) && !MATH (wc))

/* Previously "123foo" was two words. But in UAX 29 of Unicode, 
 * we know don't break words between consecutive letters and numbers
 */
typedef enum
{
  WordNone,
  WordLetters,
  WordNumbers
} WordType;

static void
default_break (const char    *text,
               int            length,
               PangoAnalysis *analysis G_GNUC_UNUSED,
               PangoLogAttr  *attrs,
               int            attrs_len G_GNUC_UNUSED)
{
  /* The rationale for all this is in section 5.15 of the Unicode 3.0 book,
   * the line breaking stuff is also in TR14 on unicode.org
   */

  /* This is a default break implementation that should work for nearly all
   * languages. Language engines can override it optionally.
   */

  /* FIXME one cheesy optimization here would be to memset attrs to 0
   * before we start, and then never assign %FALSE to anything
   */

  const gchar *next;
  const gchar *next_next;
  gint i;

  gunichar prev_wc;
  gunichar prev_prev_wc;
  gunichar next_wc;
  gunichar next_next_wc;

  JamoType prev_jamo;

  GUnicodeBreakType next_break_type;
  GUnicodeBreakType next_next_break_type;
  GUnicodeBreakType prev_break_type;
  GUnicodeBreakType prev_prev_break_type;

  PangoScript prev_script;

  /* See Grapheme_Cluster_Break Property Values table of UAX#29 */
  typedef enum
  {
    GB_Other,
    GB_ControlCRLF,
    GB_Extend,
    GB_ZWJ,
    GB_Prepend,
    GB_SpacingMark,
    GB_InHangulSyllable, /* Handles all of L, V, T, LV, LVT rules */
    /* Use state machine to handle emoji sequence */
    /* Rule GB12 and GB13 */
    GB_RI_Odd, /* Meets odd number of RI */
    GB_RI_Even, /* Meets even number of RI */
  } GraphemeBreakType;
  GraphemeBreakType prev_GB_type = GB_Other;
  gboolean met_Extended_Pictographic = FALSE;

  /* Rule GB9c */
  typedef enum
  {
    InCB_None,
    InCB_Consonant,
    InCB_Consonant_Linker,
  } Indic_Conjunct_Break;
  Indic_Conjunct_Break InCB_state = InCB_None;

  /* See Word_Break Property Values table of UAX#29 */
  typedef enum
  {
    WB_Other,
    WB_NewlineCRLF,
    WB_ExtendFormat,
    WB_Katakana,
    WB_Hebrew_Letter,
    WB_ALetter,
    WB_MidNumLet,
    WB_MidLetter,
    WB_MidNum,
    WB_Numeric,
    WB_ExtendNumLet,
    WB_RI_Odd,
    WB_RI_Even,
    WB_WSegSpace,
  } WordBreakType;
  WordBreakType prev_prev_WB_type = WB_Other, prev_WB_type = WB_Other;
  gint prev_WB_i = -1;

  /* See Sentence_Break Property Values table of UAX#29 */
  typedef enum
  {
    SB_Other,
    SB_ExtendFormat,
    SB_ParaSep,
    SB_Sp,
    SB_Lower,
    SB_Upper,
    SB_OLetter,
    SB_Numeric,
    SB_ATerm,
    SB_SContinue,
    SB_STerm,
    SB_Close,
    /* Rules SB8 and SB8a */
    SB_ATerm_Close_Sp,
    SB_STerm_Close_Sp,
  } SentenceBreakType;
  SentenceBreakType prev_prev_SB_type = SB_Other, prev_SB_type = SB_Other;
  gint prev_SB_i = -1;

  /* Rule LB25 with Example 7 of Customization */
  typedef enum
  {
    LB_Other,
    LB_Numeric,
    LB_Numeric_Close,
    LB_RI_Odd,
    LB_RI_Even,
    LB_Dotted_Circle,
    LB_Hyphen,
  } LineBreakType;
  LineBreakType prev_prev_LB_type G_GNUC_UNUSED = LB_Other;
  LineBreakType prev_LB_type = LB_Other;
  gboolean met_LB15a = FALSE;
  gint prev_LB_i = -1;

  WordType current_word_type = WordNone;
  gunichar last_word_letter = 0;
  gunichar base_character = 0;

  gint last_sentence_start = -1;
  gint last_non_space = -1;

  gboolean prev_space_or_hyphen;

  gboolean almost_done = FALSE;
  gboolean done = FALSE;

  g_return_if_fail (length == 0 || text != NULL);
  g_return_if_fail (attrs != NULL);

  next = text;
  next_next = NULL;

  prev_break_type = G_UNICODE_BREAK_UNKNOWN;
  prev_prev_break_type = G_UNICODE_BREAK_UNKNOWN;
  prev_wc = 0;
  prev_prev_wc = 0;
  prev_script = PANGO_SCRIPT_COMMON;
  prev_jamo = NO_JAMO;
  prev_space_or_hyphen = FALSE;

  if (length == 0 || *text == '\0')
    {
      next_wc = PARAGRAPH_SEPARATOR;
      almost_done = TRUE;
    }
  else
    next_wc = g_utf8_get_char (next);

  next_break_type = g_unichar_break_type (next_wc);
  next_break_type = BREAK_TYPE_SAFE (next_break_type);

  for (i = 0; !done ; i++)
    {
      GUnicodeType type;
      gunichar wc;
      GUnicodeBreakType break_type;
      GUnicodeBreakType row_break_type;
      BreakOpportunity break_op;
      JamoType jamo;
      gboolean makes_hangul_syllable;

      /* UAX#29 boundaries */
      gboolean is_grapheme_boundary;
      gboolean is_word_boundary;
      gboolean is_sentence_boundary;

      /* Emoji extended pictographics */
      gboolean is_Extended_Pictographic;

      PangoScript script;

      wc = next_wc;
      break_type = next_break_type;

      if (almost_done)
	{
	  /*
	   * If we have already reached the end of @text g_utf8_next_char()
	   * may not increment next
	   */
	  next_wc = 0;
	  next_break_type = G_UNICODE_BREAK_UNKNOWN;
	  done = TRUE;
	}
      else
	{
	  next = g_utf8_next_char (next);

	  if ((length >= 0 && next >= text + length) || *next == '\0')
	    {
	      /* This is how we fill in the last element (end position) of the
	       * attr array - assume there's a paragraph separators off the end
	       * of @text.
	       */
	      next_wc = PARAGRAPH_SEPARATOR;
	      next_next_wc = PARAGRAPH_SEPARATOR;
	      almost_done = TRUE;
	    }
	  else
	    {
	      next_wc = g_utf8_get_char (next);
	      next_next = g_utf8_next_char (next);

	      if ((length >= 0 && next_next >= text + length) || *next_next == '\0')
	        next_next_wc = PARAGRAPH_SEPARATOR;
	      else
	        next_next_wc = g_utf8_get_char (next_next);
	    }

	  next_break_type = g_unichar_break_type (next_wc);
	  next_break_type = BREAK_TYPE_SAFE (next_break_type);

	  next_next_break_type = g_unichar_break_type (next_next_wc);
	  next_next_break_type = BREAK_TYPE_SAFE (next_next_break_type);
	}

      type = g_unichar_type (wc);
      jamo = JAMO_TYPE (break_type);

      /* Determine wheter this forms a Hangul syllable with prev. */
      if (jamo == NO_JAMO)
	makes_hangul_syllable = FALSE;
      else
	{
	  JamoType prev_end   = HangulJamoProps[prev_jamo].end  ;
	  JamoType this_start = HangulJamoProps[     jamo].start;

	  /* See comments before IS_JAMO */
	  makes_hangul_syllable = (prev_end == this_start) || (prev_end + 1 == this_start);
	}

      switch ((int)type)
        {
        case G_UNICODE_SPACE_SEPARATOR:
        case G_UNICODE_LINE_SEPARATOR:
        case G_UNICODE_PARAGRAPH_SEPARATOR:
          attrs[i].is_white = TRUE;
          break;
        case G_UNICODE_CONTROL:
          if (wc == '\t' || wc == '\n' || wc == '\r' || wc == '\f')
            attrs[i].is_white = TRUE;
          else
            attrs[i].is_white = FALSE;
          break;
        default:
          attrs[i].is_white = FALSE;
          break;
        }

      /* Just few spaces have variable width. So explicitly mark them.
       */
      attrs[i].is_expandable_space = (0x0020 == wc || 0x00A0 == wc);
      is_Extended_Pictographic =
	_pango_Is_Emoji_Extended_Pictographic (wc);


      /* ---- UAX#29 Grapheme Boundaries ---- */
      {
	GraphemeBreakType GB_type;

        /* Find the GraphemeBreakType of wc */
	GB_type = GB_Other;
	switch ((int)type)
	  {
	  case G_UNICODE_FORMAT:
	    if (G_UNLIKELY (wc == 0x200C))
	      {
		GB_type = GB_Extend;
		break;
	      }
	    if (G_UNLIKELY (wc == 0x200D))
	      {
		GB_type = GB_ZWJ;
		break;
	      }
            if (G_UNLIKELY((wc >= 0x600 && wc <= 0x605) ||
                            wc == 0x6DD ||
                            wc == 0x70F ||
                            wc == 0x8E2 ||
                            wc == 0x110BD ||
                            wc == 0x110CD))
              {
                GB_type = GB_Prepend;
                break;
              }
            /* Tag chars */
            if (wc >= 0xE0020 && wc <= 0xE00FF)
              {
                GB_type = GB_Extend;
                break;
              }
            G_GNUC_FALLTHROUGH;
	  case G_UNICODE_CONTROL:
	  case G_UNICODE_LINE_SEPARATOR:
	  case G_UNICODE_PARAGRAPH_SEPARATOR:
	  case G_UNICODE_SURROGATE:
	    GB_type = GB_ControlCRLF;
	    break;

	  case G_UNICODE_UNASSIGNED:
	    /* Unassigned default ignorables */
	    if ((wc >= 0xFFF0 && wc <= 0xFFF8) ||
		(wc >= 0xE0000 && wc <= 0xE0FFF))
	      {
		GB_type = GB_ControlCRLF;
		break;
	      }
            G_GNUC_FALLTHROUGH;

	  case G_UNICODE_OTHER_LETTER:
	    if (makes_hangul_syllable)
	      GB_type = GB_InHangulSyllable;

	    if (_pango_is_Consonant_Preceding_Repha (wc) ||
		_pango_is_Consonant_Prefixed (wc))
	      GB_type = GB_Prepend;
	    break;

	  case G_UNICODE_MODIFIER_LETTER:
	    if (wc >= 0xFF9E && wc <= 0xFF9F)
	      GB_type = GB_Extend; /* Other_Grapheme_Extend */
	    break;

	  case G_UNICODE_SPACING_MARK:
	    GB_type = GB_SpacingMark; /* SpacingMark */
	    if (wc >= 0x0900)
	      {
		if (wc == 0x09BE || wc == 0x09D7 ||
		    wc == 0x0B3E || wc == 0x0B57 || wc == 0x0BBE || wc == 0x0BD7 ||
		    wc == 0x0CC2 || wc == 0x0CD5 || wc == 0x0CD6 ||
		    wc == 0x0D3E || wc == 0x0D57 || wc == 0x0DCF || wc == 0x0DDF ||
		    wc == 0x1D165 || (wc >= 0x1D16E && wc <= 0x1D172))
		  GB_type = GB_Extend; /* Other_Grapheme_Extend */
	      }
	    break;

	  case G_UNICODE_ENCLOSING_MARK:
	  case G_UNICODE_NON_SPACING_MARK:
	    GB_type = GB_Extend; /* Grapheme_Extend */
	    break;

          case G_UNICODE_OTHER_SYMBOL:
            if (G_UNLIKELY(wc >=0x1F1E6 && wc <=0x1F1FF))
              {
                if (prev_GB_type == GB_RI_Odd)
                  GB_type = GB_RI_Even;
                else
                  GB_type = GB_RI_Odd;
                break;
              }
            break;

          case G_UNICODE_MODIFIER_SYMBOL:
            /* Fitzpatrick modifiers */
            if (wc >= 0x1F3FB && wc <= 0x1F3FF)
              GB_type = GB_Extend;
            break;

          default:
            break;
	  }

	/* Rule GB11 */
	if (met_Extended_Pictographic)
	  {
	    if (GB_type == GB_Extend)
	      met_Extended_Pictographic = TRUE;
	    else if (_pango_Is_Emoji_Extended_Pictographic (prev_wc) &&
		     GB_type == GB_ZWJ)
	      met_Extended_Pictographic = TRUE;
	    else if (prev_GB_type == GB_Extend && GB_type == GB_ZWJ)
	      met_Extended_Pictographic = TRUE;
	    else if (prev_GB_type == GB_ZWJ && is_Extended_Pictographic)
	      met_Extended_Pictographic = TRUE;
	    else
	      met_Extended_Pictographic = FALSE;
	  }

	/* Grapheme Cluster Boundary Rules */
	is_grapheme_boundary = TRUE; /* Rule GB999 */

	if (prev_GB_type == GB_RI_Odd && GB_type == GB_RI_Even)
	  is_grapheme_boundary = FALSE; /* Rule GB12 and GB13 */

	if (is_Extended_Pictographic)
	  { /* Rule GB11 */
	    if (prev_GB_type == GB_ZWJ && met_Extended_Pictographic)
	      is_grapheme_boundary = FALSE;
	  }

	if (InCB_state == InCB_Consonant ||
	    InCB_state == InCB_Consonant_Linker)
	  { /* Rule GB9c */
	    if (InCB_state == InCB_Consonant_Linker &&
		_pango_is_Indic_Conjunct_Break_Consonant(wc))
	      {
		InCB_state = InCB_Consonant;
		is_grapheme_boundary = FALSE;
	      }

	    if (_pango_is_Indic_Conjunct_Break_Extend(wc) &&
		!_pango_is_Indic_Conjunct_Break_Linker(next_wc))
	      InCB_state = InCB_None;
	  }

	if (prev_GB_type == GB_Prepend)
	  is_grapheme_boundary = FALSE; /* Rule GB9b */

	if (GB_type == GB_SpacingMark)
	  is_grapheme_boundary = FALSE; /* Rule GB9a */

	if (GB_type == GB_Extend || GB_type == GB_ZWJ)
	  is_grapheme_boundary = FALSE; /* Rule GB9 */

	if (GB_type == GB_InHangulSyllable)
	  is_grapheme_boundary = FALSE; /* Rules GB6, GB7, GB8 */

	if (prev_GB_type == GB_ControlCRLF || GB_type == GB_ControlCRLF)
	  is_grapheme_boundary = TRUE; /* Rules GB4 and GB5 */

	if (wc == '\n' && prev_wc == '\r')
          is_grapheme_boundary = FALSE; /* Rule GB3 */

	/* We apply Rules GB1 and GB2 at the end of the function */

	if (is_Extended_Pictographic)
	  met_Extended_Pictographic = TRUE;

	if (InCB_state == InCB_Consonant &&
	    !_pango_is_Indic_Conjunct_Break_Extend(prev_wc) &&
	    _pango_is_Indic_Conjunct_Break_Linker(wc))
	      InCB_state = InCB_Consonant_Linker; /* Rule GB9c */

	if (InCB_state == InCB_None &&
	    _pango_is_Indic_Conjunct_Break_Consonant(wc))
	      InCB_state = InCB_Consonant; /* Rule GB9c */

	attrs[i].is_cursor_position = is_grapheme_boundary;
	/* If this is a grapheme boundary, we have to decide if backspace
	 * deletes a character or the whole grapheme cluster */
	if (is_grapheme_boundary)
          {
	    attrs[i].backspace_deletes_character = BACKSPACE_DELETES_CHARACTER (base_character);

	    /* Dependent Vowels for Indic language */
	    if (_pango_is_Virama (prev_wc) ||
		_pango_is_Vowel_Dependent (prev_wc))
	      attrs[i].backspace_deletes_character = TRUE;
          }
	else
	  attrs[i].backspace_deletes_character = FALSE;

	prev_GB_type = GB_type;
      }

      script = (PangoScript)g_unichar_get_script (wc);
      /* ---- UAX#29 Word Boundaries ---- */
      {
	is_word_boundary = FALSE;
	if (is_grapheme_boundary ||
	    G_UNLIKELY(wc >=0x1F1E6 && wc <=0x1F1FF)) /* Rules WB3 and WB4 */
	  {
	    WordBreakType WB_type;

	    /* Find the WordBreakType of wc */
	    WB_type = WB_Other;

	    if (script == PANGO_SCRIPT_KATAKANA)
	      WB_type = WB_Katakana;

	    if (script == PANGO_SCRIPT_HEBREW && type == G_UNICODE_OTHER_LETTER)
	      WB_type = WB_Hebrew_Letter;

#if !GLIB_CHECK_VERSION(2, 80, 0)
	    /* The line break property of character 0x06DD is changed
	       from G_UNICODE_BREAK_ALPHABETIC to G_UNICODE_BREAK_NUMERIC
	       in Unicode 15.1.0.

	       After the line break property is updated in glib,
	       we will remove the following code. */
	    if (wc == 0x06DD)
	      WB_type = WB_Numeric;
#endif

	    if (WB_type == WB_Other)
	      switch (wc >> 8)
	        {
		case 0x30:
		  if (wc == 0x3031 || wc == 0x3032 || wc == 0x3033 || wc == 0x3034 || wc == 0x3035 ||
		      wc == 0x309b || wc == 0x309c || wc == 0x30a0 || wc == 0x30fc)
		    WB_type = WB_Katakana; /* Katakana exceptions */
		  break;
		case 0xFF:
		  if (wc == 0xFF70)
		    WB_type = WB_Katakana; /* Katakana exceptions */
		  else if (wc >= 0xFF9E && wc <= 0xFF9F)
		    WB_type = WB_ExtendFormat; /* Other_Grapheme_Extend */
		  break;
		case 0x05:
		  if (wc == 0x058A)
		    WB_type = WB_ALetter; /* ALetter exceptions */
		  break;
		case 0x07:
		  if (wc == 0x070F)
		    WB_type = WB_ALetter; /* ALetter exceptions */
		  break;
                default:
                  break;
		}

	    if (WB_type == WB_Other)
	      switch ((int) break_type)
	        {
		case G_UNICODE_BREAK_NUMERIC:
		  if (wc != 0x066C)
		    WB_type = WB_Numeric; /* Numeric */
		  break;
		case G_UNICODE_BREAK_INFIX_SEPARATOR:
		  if (wc != 0x003A && wc != 0xFE13 && wc != 0x002E)
		    WB_type = WB_MidNum; /* MidNum */
		  break;
                default:
                  break;
		}

	    if (WB_type == WB_Other)
	      switch ((int) type)
		{
		case G_UNICODE_CONTROL:
		  if (wc != 0x000D && wc != 0x000A && wc != 0x000B && wc != 0x000C && wc != 0x0085)
		    break;
                  G_GNUC_FALLTHROUGH;
		case G_UNICODE_LINE_SEPARATOR:
		case G_UNICODE_PARAGRAPH_SEPARATOR:
		  WB_type = WB_NewlineCRLF; /* CR, LF, Newline */
		  break;

		case G_UNICODE_FORMAT:
		case G_UNICODE_SPACING_MARK:
		case G_UNICODE_ENCLOSING_MARK:
		case G_UNICODE_NON_SPACING_MARK:
		  WB_type = WB_ExtendFormat; /* Extend, Format */
		  break;

		case G_UNICODE_CONNECT_PUNCTUATION:
		  WB_type = WB_ExtendNumLet; /* ExtendNumLet */
		  break;

		case G_UNICODE_INITIAL_PUNCTUATION:
		case G_UNICODE_FINAL_PUNCTUATION:
		  if (wc == 0x2018 || wc == 0x2019)
		    WB_type = WB_MidNumLet; /* MidNumLet */
		  break;
		case G_UNICODE_OTHER_PUNCTUATION:
		  if ((wc >= 0x055a && wc <= 0x055c) ||
		      wc == 0x055e || wc == 0x05f3)
		    WB_type = WB_ALetter; /* ALetter */
		  else if (wc == 0x0027 || wc == 0x002e || wc == 0x2024 ||
		      wc == 0xfe52 || wc == 0xff07 || wc == 0xff0e)
		    WB_type = WB_MidNumLet; /* MidNumLet */
		  else if (wc == 0x00b7 || wc == 0x05f4 || wc == 0x2027 ||
			   wc == 0x003a || wc == 0x0387 || wc == 0x055f ||
			   wc == 0xfe13 || wc == 0xfe55 || wc == 0xff1a)
		    WB_type = WB_MidLetter; /* MidLetter */
		  else if (wc == 0x066c ||
			   wc == 0xfe50 || wc == 0xfe54 || wc == 0xff0c || wc == 0xff1b)
		    WB_type = WB_MidNum; /* MidNum */
		  break;

		case G_UNICODE_OTHER_SYMBOL:
		  if (wc >= 0x24B6 && wc <= 0x24E9) /* Other_Alphabetic */
		    goto Alphabetic;

		  if (G_UNLIKELY(wc >= 0x1F1E6 && wc <= 0x1F1FF))
		    {
                      if (prev_WB_type == WB_RI_Odd)
                        WB_type = WB_RI_Even;
                      else
                        WB_type = WB_RI_Odd;
		    }

		  break;

		case G_UNICODE_OTHER_LETTER:
		case G_UNICODE_LETTER_NUMBER:
		  if (wc == 0x3006 || wc == 0x3007 ||
		      (wc >= 0x3021 && wc <= 0x3029) ||
		      (wc >= 0x3038 && wc <= 0x303A) ||
		      (wc >= 0x3400 && wc <= 0x4DB5) ||
		      (wc >= 0x4E00 && wc <= 0x9FC3) ||
		      (wc >= 0xF900 && wc <= 0xFA2D) ||
		      (wc >= 0xFA30 && wc <= 0xFA6A) ||
		      (wc >= 0xFA70 && wc <= 0xFAD9) ||
		      (wc >= 0x20000 && wc <= 0x2A6D6) ||
		      (wc >= 0x2F800 && wc <= 0x2FA1D))
		    break; /* ALetter exceptions: Ideographic */
		  goto Alphabetic;

		case G_UNICODE_LOWERCASE_LETTER:
		case G_UNICODE_MODIFIER_LETTER:
		case G_UNICODE_TITLECASE_LETTER:
		case G_UNICODE_UPPERCASE_LETTER:
		Alphabetic:
		  if (break_type != G_UNICODE_BREAK_COMPLEX_CONTEXT && script != PANGO_SCRIPT_HIRAGANA)
		    WB_type = WB_ALetter; /* ALetter */
		  break;
                default:
                  break;
		}

	    if (WB_type == WB_Other)
	      {
		if (type == G_UNICODE_SPACE_SEPARATOR &&
		    break_type != G_UNICODE_BREAK_NON_BREAKING_GLUE)
		  WB_type = WB_WSegSpace;
	      }

	    /* Word Cluster Boundary Rules */

	    /* We apply Rules WB1 and WB2 at the end of the function */

	    if (prev_wc == 0x3031 && wc == 0x41)
	      g_debug ("Y %d %d", prev_WB_type, WB_type);
	    if (prev_WB_type == WB_NewlineCRLF && prev_WB_i + 1 == i)
	      {
	        /* The extra check for prev_WB_i is to correctly handle sequences like
		 * Newline ÷ Extend × Extend
		 * since we have not skipped ExtendFormat yet.
		 */
	        is_word_boundary = TRUE; /* Rule WB3a */
	      }
	    else if (WB_type == WB_NewlineCRLF)
	      is_word_boundary = TRUE; /* Rule WB3b */
	    else if (prev_wc == 0x200D && is_Extended_Pictographic)
	      is_word_boundary = FALSE; /* Rule WB3c */
	    else if (prev_WB_type == WB_WSegSpace &&
		     WB_type == WB_WSegSpace && prev_WB_i + 1 == i)
	      is_word_boundary = FALSE; /* Rule WB3d */
	    else if (WB_type == WB_ExtendFormat)
	      is_word_boundary = FALSE; /* Rules WB4? */
	    else if ((prev_WB_type == WB_ALetter  ||
                  prev_WB_type == WB_Hebrew_Letter ||
                  prev_WB_type == WB_Numeric) &&
                 (WB_type == WB_ALetter  ||
                  WB_type == WB_Hebrew_Letter ||
                  WB_type == WB_Numeric))
	      is_word_boundary = FALSE; /* Rules WB5, WB8, WB9, WB10 */
	    else if (prev_WB_type == WB_Katakana && WB_type == WB_Katakana)
	      is_word_boundary = FALSE; /* Rule WB13 */
	    else if ((prev_WB_type == WB_ALetter ||
                  prev_WB_type == WB_Hebrew_Letter ||
                  prev_WB_type == WB_Numeric ||
                  prev_WB_type == WB_Katakana ||
                  prev_WB_type == WB_ExtendNumLet) &&
                 WB_type == WB_ExtendNumLet)
	      is_word_boundary = FALSE; /* Rule WB13a */
	    else if (prev_WB_type == WB_ExtendNumLet &&
                 (WB_type == WB_ALetter ||
                  WB_type == WB_Hebrew_Letter ||
                  WB_type == WB_Numeric ||
                  WB_type == WB_Katakana))
	      is_word_boundary = FALSE; /* Rule WB13b */
	    else if (((prev_prev_WB_type == WB_ALetter ||
                   prev_prev_WB_type == WB_Hebrew_Letter) &&
                  (WB_type == WB_ALetter ||
                   WB_type == WB_Hebrew_Letter)) &&
		     (prev_WB_type == WB_MidLetter ||
              prev_WB_type == WB_MidNumLet ||
              prev_wc == 0x0027))
	      {
		attrs[prev_WB_i].is_word_boundary = FALSE; /* Rule WB6 */
		is_word_boundary = FALSE; /* Rule WB7 */
	      }
	    else if (prev_WB_type == WB_Hebrew_Letter && wc == 0x0027)
          is_word_boundary = FALSE; /* Rule WB7a */
	    else if (prev_prev_WB_type == WB_Hebrew_Letter && prev_wc == 0x0022 &&
                 WB_type == WB_Hebrew_Letter) {
          attrs[prev_WB_i].is_word_boundary = FALSE; /* Rule WB7b */
          is_word_boundary = FALSE; /* Rule WB7c */
        }
	    else if ((prev_prev_WB_type == WB_Numeric && WB_type == WB_Numeric) &&
                 (prev_WB_type == WB_MidNum || prev_WB_type == WB_MidNumLet ||
                  prev_wc == 0x0027))
	      {
		is_word_boundary = FALSE; /* Rule WB11 */
		attrs[prev_WB_i].is_word_boundary = FALSE; /* Rule WB12 */
	      }
	    else if (prev_WB_type == WB_RI_Odd && WB_type == WB_RI_Even)
	      is_word_boundary = FALSE; /* Rule WB15 and WB16 */
	    else
	      is_word_boundary = TRUE; /* Rule WB999 */

	    if (WB_type != WB_ExtendFormat)
	      {
		prev_prev_WB_type = prev_WB_type;
		prev_WB_type = WB_type;
		prev_WB_i = i;
	      }
	  }

	attrs[i].is_word_boundary = is_word_boundary;
      }

      /* ---- UAX#29 Sentence Boundaries ---- */
      {
	is_sentence_boundary = FALSE;
	if (is_word_boundary ||
	    wc == '\r' || wc == '\n') /* Rules SB3 and SB5 */
	  {
	    SentenceBreakType SB_type;

	    /* Find the SentenceBreakType of wc */
	    SB_type = SB_Other;

	    if (break_type == G_UNICODE_BREAK_NUMERIC)
	      SB_type = SB_Numeric; /* Numeric */

	    if (SB_type == SB_Other)
	      switch ((int) type)
		{
		case G_UNICODE_CONTROL:
		  if (wc == '\r' || wc == '\n')
		    SB_type = SB_ParaSep;
		  else if (wc == 0x0009 || wc == 0x000B || wc == 0x000C)
		    SB_type = SB_Sp;
		  else if (wc == 0x0085)
		    SB_type = SB_ParaSep;
		  break;

		case G_UNICODE_SPACE_SEPARATOR:
		  if (wc == 0x0020 || wc == 0x00A0 || wc == 0x1680 ||
		      (wc >= 0x2000 && wc <= 0x200A) ||
		      wc == 0x202F || wc == 0x205F || wc == 0x3000)
		    SB_type = SB_Sp;
		  break;

		case G_UNICODE_LINE_SEPARATOR:
		case G_UNICODE_PARAGRAPH_SEPARATOR:
		  SB_type = SB_ParaSep;
		  break;

		case G_UNICODE_FORMAT:
		case G_UNICODE_SPACING_MARK:
		case G_UNICODE_ENCLOSING_MARK:
		case G_UNICODE_NON_SPACING_MARK:
		  SB_type = SB_ExtendFormat; /* Extend, Format */
		  break;

		case G_UNICODE_MODIFIER_LETTER:
		  if (wc >= 0xFF9E && wc <= 0xFF9F)
		    SB_type = SB_ExtendFormat; /* Other_Grapheme_Extend */
		  break;

		case G_UNICODE_TITLECASE_LETTER:
		  SB_type = SB_Upper;
		  break;

		case G_UNICODE_DASH_PUNCTUATION:
		  if (wc == 0x002D ||
		      wc == 0x003B ||
		      wc == 0x037E ||
		      (wc >= 0x2013 && wc <= 0x2014) ||
		      wc == 0xFE14 ||
		      (wc >= 0xFE31 && wc <= 0xFE32) ||
		      wc == 0xFE54 ||
		      wc == 0xFE58 ||
		      wc == 0xFE63 ||
		      wc == 0xFF0D ||
		      wc == 0xFF1A ||
		      wc == 0xFF1B ||
		      wc == 0xFF64)
		    SB_type = SB_SContinue;
		  break;

		case G_UNICODE_OTHER_PUNCTUATION:
		  if (wc == 0x05F3)
		    SB_type = SB_OLetter;
		  else if (wc == 0x002E || wc == 0x2024 ||
		      wc == 0xFE52 || wc == 0xFF0E)
		    SB_type = SB_ATerm;

		  if (wc == 0x002C ||
		      wc == 0x003A ||
		      wc == 0x055D ||
		      (wc >= 0x060C && wc <= 0x060D) ||
		      wc == 0x07F8 ||
		      wc == 0x1802 ||
		      wc == 0x1808 ||
		      wc == 0x3001 ||
		      (wc >= 0xFE10 && wc <= 0xFE11) ||
		      wc == 0xFE13 ||
		      (wc >= 0xFE50 && wc <= 0xFE51) ||
		      wc == 0xFE55 ||
		      wc == 0xFF0C ||
		      wc == 0xFF1A ||
		      wc == 0xFF64)
		    SB_type = SB_SContinue;

		  if (_pango_is_STerm(wc))
		    SB_type = SB_STerm;

		  break;

                default:
                  break;
		}

	    if (SB_type == SB_Other)
	      {
                if (type == G_UNICODE_LOWERCASE_LETTER)
		  SB_type = SB_Lower;
                else if (type == G_UNICODE_UPPERCASE_LETTER)
		  SB_type = SB_Upper;
                else if (type == G_UNICODE_TITLECASE_LETTER ||
                         type == G_UNICODE_MODIFIER_LETTER ||
                         type == G_UNICODE_OTHER_LETTER)
		  SB_type = SB_OLetter;

		if (type == G_UNICODE_OPEN_PUNCTUATION ||
		    type == G_UNICODE_CLOSE_PUNCTUATION ||
		    break_type == G_UNICODE_BREAK_QUOTATION)
		  SB_type = SB_Close;
	      }

	    /* Sentence Boundary Rules */

	    /* We apply Rules SB1 and SB2 at the end of the function */

#define IS_OTHER_TERM(SB_type)						\
	    /* not in (OLetter | Upper | Lower | ParaSep | SATerm) */	\
	      !(SB_type == SB_OLetter ||				\
		SB_type == SB_Upper || SB_type == SB_Lower ||		\
		SB_type == SB_ParaSep ||				\
		SB_type == SB_ATerm || SB_type == SB_STerm ||		\
		SB_type == SB_ATerm_Close_Sp ||				\
		SB_type == SB_STerm_Close_Sp)


	    if (wc == '\n' && prev_wc == '\r')
	      is_sentence_boundary = FALSE; /* Rule SB3 */
	    else if (prev_SB_type == SB_ParaSep && prev_SB_i + 1 == i)
	      {
		/* The extra check for prev_SB_i is to correctly handle sequences like
		 * ParaSep ÷ Extend × Extend
		 * since we have not skipped ExtendFormat yet.
		 */

		is_sentence_boundary = TRUE; /* Rule SB4 */
	      }
	    else if (SB_type == SB_ExtendFormat)
	      is_sentence_boundary = FALSE; /* Rule SB5? */
	    else if (prev_SB_type == SB_ATerm && SB_type == SB_Numeric)
	      is_sentence_boundary = FALSE; /* Rule SB6 */
	    else if ((prev_prev_SB_type == SB_Upper ||
		      prev_prev_SB_type == SB_Lower) &&
		     prev_SB_type == SB_ATerm &&
		     SB_type == SB_Upper)
	      is_sentence_boundary = FALSE; /* Rule SB7 */
	    else if (prev_SB_type == SB_ATerm && SB_type == SB_Close)
		SB_type = SB_ATerm;
	    else if (prev_SB_type == SB_STerm && SB_type == SB_Close)
	      SB_type = SB_STerm;
	    else if (prev_SB_type == SB_ATerm && SB_type == SB_Sp)
	      SB_type = SB_ATerm_Close_Sp;
	    else if (prev_SB_type == SB_STerm && SB_type == SB_Sp)
	      SB_type = SB_STerm_Close_Sp;
	    /* Rule SB8 */
	    else if ((prev_SB_type == SB_ATerm ||
		      prev_SB_type == SB_ATerm_Close_Sp) &&
		     SB_type == SB_Lower)
	      is_sentence_boundary = FALSE;
	    else if ((prev_prev_SB_type == SB_ATerm ||
		      prev_prev_SB_type == SB_ATerm_Close_Sp) &&
		     IS_OTHER_TERM(prev_SB_type) &&
		     SB_type == SB_Lower)
              {
	        attrs[prev_SB_i].is_sentence_boundary = FALSE;
	        attrs[prev_SB_i].is_sentence_end = FALSE;
                last_sentence_start = -1;
                for (int j = prev_SB_i - 1; j >= 0; j--)
                  {
                    attrs[j].is_sentence_end = FALSE;
                    if (attrs[j].is_sentence_boundary)
                      {
                        last_sentence_start = j;
                        break;
                      }
                  }
              }
	    else if ((prev_SB_type == SB_ATerm ||
		      prev_SB_type == SB_ATerm_Close_Sp ||
		      prev_SB_type == SB_STerm ||
		      prev_SB_type == SB_STerm_Close_Sp) &&
		     (SB_type == SB_SContinue ||
		      SB_type == SB_ATerm || SB_type == SB_STerm))
	      is_sentence_boundary = FALSE; /* Rule SB8a */
	    else if ((prev_SB_type == SB_ATerm ||
		      prev_SB_type == SB_STerm) &&
		     (SB_type == SB_Close || SB_type == SB_Sp ||
		      SB_type == SB_ParaSep))
	      is_sentence_boundary = FALSE; /* Rule SB9 */
	    else if ((prev_SB_type == SB_ATerm ||
		      prev_SB_type == SB_ATerm_Close_Sp ||
		      prev_SB_type == SB_STerm ||
		      prev_SB_type == SB_STerm_Close_Sp) &&
		     (SB_type == SB_Sp || SB_type == SB_ParaSep))
	      is_sentence_boundary = FALSE; /* Rule SB10 */
	    else if ((prev_SB_type == SB_ATerm ||
		      prev_SB_type == SB_ATerm_Close_Sp ||
		      prev_SB_type == SB_STerm ||
		      prev_SB_type == SB_STerm_Close_Sp) &&
		     SB_type != SB_ParaSep)
	      is_sentence_boundary = TRUE; /* Rule SB11 */
	    else
	      is_sentence_boundary = FALSE; /* Rule SB998 */

	    if (SB_type != SB_ExtendFormat &&
		!((prev_prev_SB_type == SB_ATerm ||
		   prev_prev_SB_type == SB_ATerm_Close_Sp) &&
		  IS_OTHER_TERM(prev_SB_type) &&
		  IS_OTHER_TERM(SB_type)))
              {
                prev_prev_SB_type = prev_SB_type;
                prev_SB_type = SB_type;
                prev_SB_i = i;
              }

#undef IS_OTHER_TERM

	  }

	if (i == 0 || done)
	  is_sentence_boundary = TRUE; /* Rules SB1 and SB2 */

	attrs[i].is_sentence_boundary = is_sentence_boundary;
      }

      /* ---- Line breaking ---- */

      break_op = BREAK_ALREADY_HANDLED;

      row_break_type = prev_break_type == G_UNICODE_BREAK_SPACE ?
	prev_prev_break_type : prev_break_type;
      g_assert (row_break_type != G_UNICODE_BREAK_SPACE);

      attrs[i].is_char_break = FALSE;
      attrs[i].is_line_break = FALSE;
      attrs[i].is_mandatory_break = FALSE;

      /* Rule LB1:
	 assign a line breaking class to each code point of the input. */
      switch ((int)break_type)
	{
	case G_UNICODE_BREAK_AMBIGUOUS:
	case G_UNICODE_BREAK_SURROGATE:
	case G_UNICODE_BREAK_UNKNOWN:
	  break_type = G_UNICODE_BREAK_ALPHABETIC;
	  break;

	case G_UNICODE_BREAK_COMPLEX_CONTEXT:
	  if (type == G_UNICODE_NON_SPACING_MARK ||
	      type == G_UNICODE_SPACING_MARK)
	    break_type = G_UNICODE_BREAK_COMBINING_MARK;
	  else
	    break_type = G_UNICODE_BREAK_ALPHABETIC;
	  break;

	case G_UNICODE_BREAK_CONDITIONAL_JAPANESE_STARTER:
	  break_type = G_UNICODE_BREAK_NON_STARTER;
	  break;

	default:
          break;
	}

      /* If it's not a grapheme boundary, it's not a line break either */
      if (attrs[i].is_cursor_position ||
	  prev_break_type == G_UNICODE_BREAK_SPACE ||
	  break_type == G_UNICODE_BREAK_COMBINING_MARK ||
	  break_type == G_UNICODE_BREAK_ZERO_WIDTH_JOINER ||
	  break_type == G_UNICODE_BREAK_NON_BREAKING_GLUE ||
	  break_type == G_UNICODE_BREAK_HANGUL_L_JAMO ||
	  break_type == G_UNICODE_BREAK_HANGUL_V_JAMO ||
	  break_type == G_UNICODE_BREAK_HANGUL_T_JAMO ||
	  break_type == G_UNICODE_BREAK_HANGUL_LV_SYLLABLE ||
	  break_type == G_UNICODE_BREAK_HANGUL_LVT_SYLLABLE ||
	  break_type == G_UNICODE_BREAK_EMOJI_MODIFIER ||
	  break_type == G_UNICODE_BREAK_REGIONAL_INDICATOR ||
#if GLIB_CHECK_VERSION(2, 80, 0)
	  break_type == G_UNICODE_BREAK_VIRAMA ||
	  break_type == G_UNICODE_BREAK_VIRAMA_FINAL ||
#endif
	  FALSE)
	{
	  LineBreakType LB_type;

	  /* Find the LineBreakType of wc */
	  LB_type = LB_Other;

	  /* Rule LB20a */
	  if (wc == 0x2010)
	    LB_type = LB_Hyphen;

	  /* Rule LB28a */
	  if (wc == 0x25CC)
	    LB_type = LB_Dotted_Circle;

	  if (break_type == G_UNICODE_BREAK_NUMERIC)
	    LB_type = LB_Numeric;

	  if (break_type == G_UNICODE_BREAK_SYMBOL ||
	      break_type == G_UNICODE_BREAK_INFIX_SEPARATOR)
	    {
	      if (!(prev_LB_type == LB_Numeric))
		LB_type = LB_Other;
	    }

	  if (break_type == G_UNICODE_BREAK_CLOSE_PUNCTUATION ||
	      break_type == G_UNICODE_BREAK_CLOSE_PARANTHESIS)
	    {
	      if (prev_LB_type == LB_Numeric)
		LB_type = LB_Numeric_Close;
	      else
		LB_type = LB_Other;
	    }

	  if (break_type == G_UNICODE_BREAK_REGIONAL_INDICATOR)
	    {
	      if (prev_LB_type == LB_RI_Odd)
		LB_type = LB_RI_Even;
	      else
		LB_type = LB_RI_Odd;
	    }

	  attrs[i].is_line_break = TRUE; /* Rule LB31 */
	  /* Unicode doesn't specify char wrap;
	     we wrap around all chars currently. */
	  if (attrs[i].is_cursor_position)
	    attrs[i].is_char_break = TRUE;

	  /* Make any necessary replacements first */
	  if (row_break_type == G_UNICODE_BREAK_UNKNOWN)
	    row_break_type = G_UNICODE_BREAK_ALPHABETIC;

	  /* add the line break rules in reverse order to override
	     the lower priority rules. */

	  /* Rule LB30b */
	  if (prev_break_type == G_UNICODE_BREAK_EMOJI_BASE &&
	      break_type == G_UNICODE_BREAK_EMOJI_MODIFIER)
	    break_op = BREAK_PROHIBITED;

	  if ((_pango_Is_Emoji_Extended_Pictographic (prev_wc) &&
	       g_unichar_type (prev_wc) == G_UNICODE_UNASSIGNED) &&
	      break_type == G_UNICODE_BREAK_EMOJI_MODIFIER)
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB30a */
	  if (prev_LB_type == LB_RI_Odd && LB_type == LB_RI_Even)
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB30 */
	  if ((prev_break_type == G_UNICODE_BREAK_ALPHABETIC ||
	       prev_break_type == G_UNICODE_BREAK_HEBREW_LETTER ||
	       prev_break_type == G_UNICODE_BREAK_NUMERIC) &&
	      break_type == G_UNICODE_BREAK_OPEN_PUNCTUATION &&
	      !_pango_is_EastAsianWide (wc))
	    break_op = BREAK_PROHIBITED;

	  if (prev_break_type == G_UNICODE_BREAK_CLOSE_PARANTHESIS &&
	      !_pango_is_EastAsianWide (prev_wc)&&
	      (break_type == G_UNICODE_BREAK_ALPHABETIC ||
	       break_type == G_UNICODE_BREAK_HEBREW_LETTER ||
	       break_type == G_UNICODE_BREAK_NUMERIC))
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB29 */
	  if (prev_break_type == G_UNICODE_BREAK_INFIX_SEPARATOR &&
	      (break_type == G_UNICODE_BREAK_ALPHABETIC ||
	       break_type == G_UNICODE_BREAK_HEBREW_LETTER))
	    break_op = BREAK_PROHIBITED;

#if GLIB_CHECK_VERSION(2, 80, 0)
	  /* Rule LB28a */
	  if (prev_break_type == G_UNICODE_BREAK_AKSARA_PRE_BASE &&
	      (break_type == G_UNICODE_BREAK_AKSARA ||
	       LB_type == LB_Dotted_Circle ||
	       break_type == G_UNICODE_BREAK_AKSARA_START))
	    break_op = BREAK_PROHIBITED;

	  if ((prev_break_type == G_UNICODE_BREAK_AKSARA ||
	       prev_LB_type == LB_Dotted_Circle ||
	       prev_break_type == G_UNICODE_BREAK_AKSARA_START) &&
	      (break_type == G_UNICODE_BREAK_VIRAMA_FINAL ||
	       break_type == G_UNICODE_BREAK_VIRAMA))
	    break_op = BREAK_PROHIBITED;

	  if ((prev_prev_break_type == G_UNICODE_BREAK_AKSARA ||
	       prev_prev_LB_type == LB_Dotted_Circle ||
	       prev_prev_break_type == G_UNICODE_BREAK_AKSARA_START) &&
	      prev_break_type == G_UNICODE_BREAK_VIRAMA &&
	      (break_type == G_UNICODE_BREAK_AKSARA ||
	       LB_type == LB_Dotted_Circle))
	    break_op = BREAK_PROHIBITED;

	  if ((prev_break_type == G_UNICODE_BREAK_AKSARA ||
	       prev_LB_type == LB_Dotted_Circle ||
	       prev_break_type == G_UNICODE_BREAK_AKSARA_START) &&
	      (break_type == G_UNICODE_BREAK_AKSARA ||
	       LB_type == LB_Dotted_Circle ||
	       break_type == G_UNICODE_BREAK_AKSARA_START) &&
	      next_break_type == G_UNICODE_BREAK_VIRAMA_FINAL)
	    break_op = BREAK_PROHIBITED;
#endif

	  /* Rule LB28 */
	  if ((prev_break_type == G_UNICODE_BREAK_ALPHABETIC ||
	       prev_break_type == G_UNICODE_BREAK_HEBREW_LETTER) &&
	      (break_type == G_UNICODE_BREAK_ALPHABETIC ||
	       break_type == G_UNICODE_BREAK_HEBREW_LETTER))
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB27 */
	  if ((prev_break_type == G_UNICODE_BREAK_HANGUL_L_JAMO ||
	       prev_break_type == G_UNICODE_BREAK_HANGUL_V_JAMO ||
	       prev_break_type == G_UNICODE_BREAK_HANGUL_T_JAMO ||
	       prev_break_type == G_UNICODE_BREAK_HANGUL_LV_SYLLABLE ||
	       prev_break_type == G_UNICODE_BREAK_HANGUL_LVT_SYLLABLE) &&
	      break_type == G_UNICODE_BREAK_POSTFIX)
	    break_op = BREAK_PROHIBITED;

	  if (prev_break_type == G_UNICODE_BREAK_PREFIX &&
	      (break_type == G_UNICODE_BREAK_HANGUL_L_JAMO ||
	       break_type == G_UNICODE_BREAK_HANGUL_V_JAMO ||
	       break_type == G_UNICODE_BREAK_HANGUL_T_JAMO ||
	       break_type == G_UNICODE_BREAK_HANGUL_LV_SYLLABLE ||
	       break_type == G_UNICODE_BREAK_HANGUL_LVT_SYLLABLE))
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB26 */
	  if (prev_break_type == G_UNICODE_BREAK_HANGUL_L_JAMO &&
	      (break_type == G_UNICODE_BREAK_HANGUL_L_JAMO ||
	       break_type == G_UNICODE_BREAK_HANGUL_V_JAMO ||
	       break_type == G_UNICODE_BREAK_HANGUL_LV_SYLLABLE ||
	       break_type == G_UNICODE_BREAK_HANGUL_LVT_SYLLABLE))
	    break_op = BREAK_PROHIBITED;

	  if ((prev_break_type == G_UNICODE_BREAK_HANGUL_V_JAMO ||
	       prev_break_type == G_UNICODE_BREAK_HANGUL_LV_SYLLABLE) &&
	      (break_type == G_UNICODE_BREAK_HANGUL_V_JAMO ||
	       break_type == G_UNICODE_BREAK_HANGUL_T_JAMO))
	    break_op = BREAK_PROHIBITED;

	  if ((prev_break_type == G_UNICODE_BREAK_HANGUL_T_JAMO ||
	       prev_break_type == G_UNICODE_BREAK_HANGUL_LVT_SYLLABLE) &&
	      break_type == G_UNICODE_BREAK_HANGUL_T_JAMO)
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB25 */
	  if (prev_prev_LB_type == LB_Numeric &&
	      prev_break_type == G_UNICODE_BREAK_CLOSE_PUNCTUATION &&
	      break_type == G_UNICODE_BREAK_POSTFIX)
	    break_op = BREAK_PROHIBITED; /* NU ( SY | IS )* CL × PO */

	  if (prev_prev_LB_type == LB_Numeric &&
	      prev_break_type == G_UNICODE_BREAK_CLOSE_PARANTHESIS &&
	      break_type == G_UNICODE_BREAK_POSTFIX)
	    break_op = BREAK_PROHIBITED; /* NU ( SY | IS )* CP × PO */

	  if (prev_prev_LB_type == LB_Numeric &&
	      prev_break_type == G_UNICODE_BREAK_CLOSE_PUNCTUATION &&
	      break_type == G_UNICODE_BREAK_PREFIX)
	    break_op = BREAK_PROHIBITED; /* NU ( SY | IS )* CL × PR */

	  if (prev_prev_LB_type == LB_Numeric &&
	      prev_break_type == G_UNICODE_BREAK_CLOSE_PARANTHESIS &&
	      break_type == G_UNICODE_BREAK_PREFIX)
	    break_op = BREAK_PROHIBITED; /* NU ( SY | IS )* CP × PR */

	  if (prev_LB_type == LB_Numeric &&
	      break_type == G_UNICODE_BREAK_POSTFIX)
	    break_op = BREAK_PROHIBITED; /* NU ( SY | IS )* × PO */

	  if (prev_LB_type == LB_Numeric &&
	      break_type == G_UNICODE_BREAK_PREFIX)
	    break_op = BREAK_PROHIBITED; /* NU ( SY | IS )* × PR */

	  if (prev_break_type == G_UNICODE_BREAK_POSTFIX &&
	      break_type == G_UNICODE_BREAK_OPEN_PUNCTUATION &&
	      next_break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED; /* PO × OP NU */

	  if (prev_break_type == G_UNICODE_BREAK_POSTFIX &&
	      break_type == G_UNICODE_BREAK_OPEN_PUNCTUATION &&
	      next_break_type == G_UNICODE_BREAK_INFIX_SEPARATOR &&
	      next_next_break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED; /* PO × OP IS NU */

	  if (prev_break_type == G_UNICODE_BREAK_POSTFIX &&
	      break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED; /* PO × NU */

	  if (prev_break_type == G_UNICODE_BREAK_PREFIX &&
	      break_type == G_UNICODE_BREAK_OPEN_PUNCTUATION &&
	      next_break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED; /* PR × OP NU */

	  if (prev_break_type == G_UNICODE_BREAK_PREFIX &&
	      break_type == G_UNICODE_BREAK_OPEN_PUNCTUATION &&
	      next_break_type == G_UNICODE_BREAK_INFIX_SEPARATOR &&
	      next_next_break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED; /* PR × OP IS NU */

	  if (prev_break_type == G_UNICODE_BREAK_PREFIX &&
	      break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED; /* PR × NU */

	  if (prev_break_type == G_UNICODE_BREAK_HYPHEN &&
	      break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED; /* HY × NU */

	  if (prev_break_type == G_UNICODE_BREAK_INFIX_SEPARATOR &&
	      break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED; /* IS × NU */

	  if (prev_LB_type == LB_Numeric &&
	      break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED; /* NU ( SY | IS )* × NU */

	  /* Rule LB24 */
	  if ((prev_break_type == G_UNICODE_BREAK_PREFIX ||
	       prev_break_type == G_UNICODE_BREAK_POSTFIX) &&
	      (break_type == G_UNICODE_BREAK_ALPHABETIC ||
	       break_type == G_UNICODE_BREAK_HEBREW_LETTER))
	    break_op = BREAK_PROHIBITED;

	  if ((prev_break_type == G_UNICODE_BREAK_ALPHABETIC ||
	       prev_break_type == G_UNICODE_BREAK_HEBREW_LETTER) &&
	      (break_type == G_UNICODE_BREAK_PREFIX ||
	       break_type == G_UNICODE_BREAK_POSTFIX))
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB23a */
	  if (prev_break_type == G_UNICODE_BREAK_PREFIX &&
	      (break_type == G_UNICODE_BREAK_IDEOGRAPHIC ||
	       break_type == G_UNICODE_BREAK_EMOJI_BASE ||
	       break_type == G_UNICODE_BREAK_EMOJI_MODIFIER))
	    break_op = BREAK_PROHIBITED;

	  if ((prev_break_type == G_UNICODE_BREAK_IDEOGRAPHIC ||
	       prev_break_type == G_UNICODE_BREAK_EMOJI_BASE ||
	       prev_break_type == G_UNICODE_BREAK_EMOJI_MODIFIER) &&
	      break_type == G_UNICODE_BREAK_POSTFIX)
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB23 */
	  if ((prev_break_type == G_UNICODE_BREAK_ALPHABETIC ||
	       prev_break_type == G_UNICODE_BREAK_HEBREW_LETTER) &&
	      break_type == G_UNICODE_BREAK_NUMERIC)
	    break_op = BREAK_PROHIBITED;

	  if (prev_break_type == G_UNICODE_BREAK_NUMERIC &&
	      (break_type == G_UNICODE_BREAK_ALPHABETIC ||
	       break_type == G_UNICODE_BREAK_HEBREW_LETTER))
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB22 */
	  if (break_type == G_UNICODE_BREAK_INSEPARABLE)
	    break_op = BREAK_PROHIBITED;

	  if (prev_break_type == G_UNICODE_BREAK_SYMBOL &&
	      break_type == G_UNICODE_BREAK_HEBREW_LETTER)
	    break_op = BREAK_PROHIBITED; /* Rule LB21b */

	  if (prev_prev_break_type == G_UNICODE_BREAK_HEBREW_LETTER &&
	      (prev_break_type == G_UNICODE_BREAK_HYPHEN ||
	       (prev_break_type == G_UNICODE_BREAK_AFTER &&
		!_pango_is_EastAsianWide(prev_wc))) &&
	      break_type != G_UNICODE_BREAK_HEBREW_LETTER)
	    break_op = BREAK_PROHIBITED; /* Rule LB21a */

	  if (break_type == G_UNICODE_BREAK_AFTER ||
	      break_type == G_UNICODE_BREAK_HYPHEN ||
	      break_type == G_UNICODE_BREAK_NON_STARTER ||
	      prev_break_type == G_UNICODE_BREAK_BEFORE)
	    break_op = BREAK_PROHIBITED; /* Rule LB21 */

	  /* Rule LB20a */
	  if ((prev_LB_i == 0 ||
	       prev_prev_break_type == G_UNICODE_BREAK_MANDATORY ||
	       prev_prev_break_type == G_UNICODE_BREAK_CARRIAGE_RETURN ||
	       prev_prev_break_type == G_UNICODE_BREAK_LINE_FEED ||
	       prev_prev_break_type == G_UNICODE_BREAK_NEXT_LINE ||
	       prev_prev_break_type == G_UNICODE_BREAK_SPACE ||
	       prev_prev_break_type == G_UNICODE_BREAK_ZERO_WIDTH_SPACE ||
	       prev_prev_break_type == G_UNICODE_BREAK_CONTINGENT ||
	       prev_prev_break_type == G_UNICODE_BREAK_NON_BREAKING_GLUE) &&
	      (prev_break_type == G_UNICODE_BREAK_HYPHEN ||
	       prev_LB_type == LB_Hyphen) &&
	       break_type == G_UNICODE_BREAK_ALPHABETIC)
	     break_op = BREAK_PROHIBITED;

	  if (prev_break_type == G_UNICODE_BREAK_CONTINGENT ||
	      break_type == G_UNICODE_BREAK_CONTINGENT)
	    break_op = BREAK_ALLOWED; /* Rule LB20 */

	  /* Rule LB19a */
	  if (!_pango_is_EastAsianWide(prev_wc) &&
	      break_type == G_UNICODE_BREAK_QUOTATION)
	    break_op = BREAK_PROHIBITED;

	  if (break_type == G_UNICODE_BREAK_QUOTATION &&
	      (!_pango_is_EastAsianWide(next_wc) || done))
	    break_op = BREAK_PROHIBITED;

	  if (prev_break_type == G_UNICODE_BREAK_QUOTATION &&
	      !_pango_is_EastAsianWide(wc))
	    break_op = BREAK_PROHIBITED;

	  if ((prev_LB_i == 0 || !_pango_is_EastAsianWide(prev_prev_wc)) &&
	      prev_break_type == G_UNICODE_BREAK_QUOTATION)
	    break_op = BREAK_PROHIBITED;

	  /* Rule LB19 */
	  if (break_type == G_UNICODE_BREAK_QUOTATION &&
	      type != G_UNICODE_INITIAL_PUNCTUATION)
	    break_op = BREAK_PROHIBITED;

	  if (prev_break_type == G_UNICODE_BREAK_QUOTATION &&
	      g_unichar_type(prev_wc) != G_UNICODE_FINAL_PUNCTUATION)
	    break_op = BREAK_PROHIBITED;

	  /* handle related rules for Space as state machine here,
	     and override the pair table result. */
	  if (prev_break_type == G_UNICODE_BREAK_SPACE) /* Rule LB18 */
	    break_op = BREAK_ALLOWED;

	  if (row_break_type == G_UNICODE_BREAK_BEFORE_AND_AFTER &&
	      break_type == G_UNICODE_BREAK_BEFORE_AND_AFTER)
	    break_op = BREAK_PROHIBITED; /* Rule LB17 */

	  if ((row_break_type == G_UNICODE_BREAK_CLOSE_PUNCTUATION ||
	       row_break_type == G_UNICODE_BREAK_CLOSE_PARANTHESIS) &&
	      break_type == G_UNICODE_BREAK_NON_STARTER)
	    break_op = BREAK_PROHIBITED; /* Rule LB16 */

	  /* Rule LB15d */
	  if (break_type == G_UNICODE_BREAK_INFIX_SEPARATOR)
	      break_op = BREAK_PROHIBITED;

	  /* Rule LB15c */
	  if (prev_break_type == G_UNICODE_BREAK_SPACE &&
	      break_type == G_UNICODE_BREAK_INFIX_SEPARATOR &&
	      next_break_type == G_UNICODE_BREAK_NUMERIC)
	      break_op = BREAK_ALLOWED;

	  /* Rule LB15b */
	  if (type == G_UNICODE_FINAL_PUNCTUATION &&
	      break_type == G_UNICODE_BREAK_QUOTATION)
	    {
	      if (next_wc == 0 ||
		  next_break_type == G_UNICODE_BREAK_SPACE ||
		  next_break_type == G_UNICODE_BREAK_NON_BREAKING_GLUE ||
		  next_break_type == G_UNICODE_BREAK_WORD_JOINER ||
		  next_break_type == G_UNICODE_BREAK_CLOSE_PUNCTUATION ||
		  next_break_type == G_UNICODE_BREAK_QUOTATION ||
		  next_break_type == G_UNICODE_BREAK_CLOSE_PARANTHESIS ||
		  next_break_type == G_UNICODE_BREAK_EXCLAMATION ||
		  next_break_type == G_UNICODE_BREAK_INFIX_SEPARATOR ||
		  next_break_type == G_UNICODE_BREAK_SYMBOL ||
		  next_break_type == G_UNICODE_BREAK_MANDATORY ||
		  next_break_type == G_UNICODE_BREAK_CARRIAGE_RETURN ||
		  next_break_type == G_UNICODE_BREAK_LINE_FEED ||
		  next_break_type == G_UNICODE_BREAK_NEXT_LINE ||
		  next_break_type == G_UNICODE_BREAK_ZERO_WIDTH_SPACE)
		break_op = BREAK_PROHIBITED;
	    }

	  /* Rule LB15a */
	  if (met_LB15a &&
	      break_type != G_UNICODE_BREAK_SPACE &&
	      /* Rule LB9 */
	      break_type != G_UNICODE_BREAK_COMBINING_MARK &&
	      break_type != G_UNICODE_BREAK_ZERO_WIDTH_JOINER)
	    {
	      met_LB15a = FALSE;
	      break_op = BREAK_PROHIBITED;
	    }
	  else if (type == G_UNICODE_INITIAL_PUNCTUATION &&
	      break_type == G_UNICODE_BREAK_QUOTATION)
	    {
	      if (i == 0 ||
		  prev_break_type == G_UNICODE_BREAK_MANDATORY ||
		  prev_break_type == G_UNICODE_BREAK_CARRIAGE_RETURN ||
		  prev_break_type == G_UNICODE_BREAK_LINE_FEED ||
		  prev_break_type == G_UNICODE_BREAK_NEXT_LINE ||
		  prev_break_type == G_UNICODE_BREAK_OPEN_PUNCTUATION ||
		  prev_break_type == G_UNICODE_BREAK_QUOTATION ||
		  prev_break_type == G_UNICODE_BREAK_NON_BREAKING_GLUE ||
		  prev_break_type == G_UNICODE_BREAK_SPACE ||
		  prev_break_type == G_UNICODE_BREAK_ZERO_WIDTH_SPACE)
		met_LB15a = TRUE;
	    }

	  if (row_break_type == G_UNICODE_BREAK_OPEN_PUNCTUATION)
	    break_op = BREAK_PROHIBITED; /* Rule LB14 */

	  if (break_type == G_UNICODE_BREAK_CLOSE_PUNCTUATION ||
	      break_type == G_UNICODE_BREAK_CLOSE_PARANTHESIS ||
	      break_type == G_UNICODE_BREAK_EXCLAMATION ||
	      break_type == G_UNICODE_BREAK_SYMBOL)
	    break_op = BREAK_PROHIBITED; /* Rule LB13 */

	  if (break_type == G_UNICODE_BREAK_NON_BREAKING_GLUE &&
	      (prev_break_type != G_UNICODE_BREAK_SPACE &&
	       prev_break_type != G_UNICODE_BREAK_AFTER &&
	       prev_break_type != G_UNICODE_BREAK_HYPHEN))
	    break_op = BREAK_PROHIBITED; /* Rule LB12a */

	  if (prev_break_type == G_UNICODE_BREAK_NON_BREAKING_GLUE)
	    break_op = BREAK_PROHIBITED; /* Rule LB12 */

	  if (prev_break_type == G_UNICODE_BREAK_WORD_JOINER ||
	      break_type == G_UNICODE_BREAK_WORD_JOINER)
	    break_op = BREAK_PROHIBITED; /* Rule LB11 */

	  /* Rule LB9 */
	  if (break_type == G_UNICODE_BREAK_COMBINING_MARK ||
              break_type == G_UNICODE_BREAK_ZERO_WIDTH_JOINER)
	    {
	      if (!(prev_break_type == G_UNICODE_BREAK_MANDATORY ||
		    prev_break_type == G_UNICODE_BREAK_CARRIAGE_RETURN ||
		    prev_break_type == G_UNICODE_BREAK_LINE_FEED ||
		    prev_break_type == G_UNICODE_BREAK_NEXT_LINE ||
		    prev_break_type == G_UNICODE_BREAK_SPACE ||
		    prev_break_type == G_UNICODE_BREAK_ZERO_WIDTH_SPACE))
		break_op = BREAK_PROHIBITED;

	      if (met_LB15a)
		break_op = BREAK_PROHIBITED;
	    }

	  if (prev_wc == 0x200D)
	    break_op = BREAK_PROHIBITED; /* Rule LB8a */

	  if (row_break_type == G_UNICODE_BREAK_ZERO_WIDTH_SPACE)
	    break_op = BREAK_ALLOWED; /* Rule LB8 */

	  if (break_type == G_UNICODE_BREAK_SPACE ||
	      break_type == G_UNICODE_BREAK_ZERO_WIDTH_SPACE)
	    break_op = BREAK_PROHIBITED; /* Rule LB7 */

	  /* Rule LB6 */
	  if (break_type == G_UNICODE_BREAK_MANDATORY ||
	      break_type == G_UNICODE_BREAK_CARRIAGE_RETURN ||
	      break_type == G_UNICODE_BREAK_LINE_FEED ||
	      break_type == G_UNICODE_BREAK_NEXT_LINE)
	    break_op = BREAK_PROHIBITED;

	  /* Rules LB4 and LB5 */
	  if (prev_break_type == G_UNICODE_BREAK_MANDATORY ||
	      (prev_break_type == G_UNICODE_BREAK_CARRIAGE_RETURN &&
	       wc != '\n') ||
	      prev_break_type == G_UNICODE_BREAK_LINE_FEED ||
	      prev_break_type == G_UNICODE_BREAK_NEXT_LINE)
	    {
	      attrs[i].is_mandatory_break = TRUE;
	      break_op = BREAK_ALLOWED;
	    }

	  switch (break_op)
	    {
	    case BREAK_PROHIBITED:
	      /* can't break here */
	      attrs[i].is_line_break = FALSE;
	      break;

	    case BREAK_IF_SPACES:
	      /* break if prev char was space */
	      if (prev_break_type != G_UNICODE_BREAK_SPACE)
		attrs[i].is_line_break = FALSE;
	      break;

	    case BREAK_ALLOWED:
	      attrs[i].is_line_break = TRUE;
	      break;

	    case BREAK_ALREADY_HANDLED:
	      break;

	    default:
	      g_assert_not_reached ();
	      break;
	    }

	  /* Rule LB9 */
	  if (!(break_type == G_UNICODE_BREAK_COMBINING_MARK ||
		break_type == G_UNICODE_BREAK_ZERO_WIDTH_JOINER))
	    {
	      /* Rule LB25 without Example 7 of Customization */
	      if (break_type == G_UNICODE_BREAK_SYMBOL ||
		    break_type == G_UNICODE_BREAK_INFIX_SEPARATOR)
		{
		  if (prev_LB_type != LB_Numeric)
		    {
		      prev_prev_LB_type = prev_LB_type;
		      prev_LB_type = LB_type;
		    }
		  /* else don't change the prev_LB_type */
		}
	      else
		{
		  prev_prev_LB_type = prev_LB_type;
		  prev_LB_type = LB_type;
		}
	    }
	  /* else don't change the prev_LB_type for Rule LB9 */
	}

      if (break_type != G_UNICODE_BREAK_SPACE)
	{
	  /* Rule LB9 */
	  if (break_type == G_UNICODE_BREAK_COMBINING_MARK ||
	      break_type == G_UNICODE_BREAK_ZERO_WIDTH_JOINER)
	    {
	      if (i == 0 /* start of text */ ||
		  prev_break_type == G_UNICODE_BREAK_MANDATORY ||
		  prev_break_type == G_UNICODE_BREAK_CARRIAGE_RETURN ||
		  prev_break_type == G_UNICODE_BREAK_LINE_FEED ||
		  prev_break_type == G_UNICODE_BREAK_NEXT_LINE ||
		  prev_break_type == G_UNICODE_BREAK_SPACE ||
		  prev_break_type == G_UNICODE_BREAK_ZERO_WIDTH_SPACE)
		prev_break_type = G_UNICODE_BREAK_ALPHABETIC; /* Rule LB10 */
	      /* else don't change the prev_break_type for Rule LB9 */
	    }
	  else
	    {
	      prev_prev_break_type = prev_break_type;
	      prev_break_type = break_type;
	      prev_LB_i = i;
	    }

	  prev_jamo = jamo;
	}
      else
	{
	  if (prev_break_type != G_UNICODE_BREAK_SPACE)
	    {
	      prev_prev_break_type = prev_break_type;
	      prev_break_type = break_type;
	      prev_LB_i = i;
	    }
	  /* else don't change the prev_break_type */
	}

      /* ---- Word breaks ---- */

      /* default to not a word start/end */
      attrs[i].is_word_start = FALSE;
      attrs[i].is_word_end = FALSE;

      if (current_word_type != WordNone)
	{
	  /* Check for a word end */
	  switch ((int) type)
	    {
	    case G_UNICODE_SPACING_MARK:
	    case G_UNICODE_ENCLOSING_MARK:
	    case G_UNICODE_NON_SPACING_MARK:
	    case G_UNICODE_FORMAT:
	      /* nothing, we just eat these up as part of the word */
	      break;

	    case G_UNICODE_LOWERCASE_LETTER:
	    case G_UNICODE_MODIFIER_LETTER:
	    case G_UNICODE_OTHER_LETTER:
	    case G_UNICODE_TITLECASE_LETTER:
	    case G_UNICODE_UPPERCASE_LETTER:
	      if (current_word_type == WordLetters)
		{
		  /* Japanese special cases for ending the word */
		  if (JAPANESE (last_word_letter) ||
		      JAPANESE (wc))
		    {
		      if ((HIRAGANA (last_word_letter) &&
			   !HIRAGANA (wc)) ||
			  (KATAKANA (last_word_letter) &&
			   !(KATAKANA (wc) || HIRAGANA (wc))) ||
			  (KANJI (last_word_letter) &&
			   !(HIRAGANA (wc) || KANJI (wc))) ||
			  (JAPANESE (last_word_letter) &&
			   !JAPANESE (wc)) ||
			  (!JAPANESE (last_word_letter) &&
			   JAPANESE (wc)))
			{
			  attrs[i].is_word_start = TRUE;
			  attrs[i].is_word_end = TRUE;
			}
		    }
		}
	      last_word_letter = wc;
	      break;

	    case G_UNICODE_DECIMAL_NUMBER:
	    case G_UNICODE_LETTER_NUMBER:
	    case G_UNICODE_OTHER_NUMBER:
	      last_word_letter = wc;
	      break;

	    default:
	      /* Punctuation, control/format chars, etc. all end a word. */
	      attrs[i].is_word_end = TRUE;
	      current_word_type = WordNone;
	      break;
	    }
	}
      else
	{
	  /* Check for a word start */
	  switch ((int) type)
	    {
	    case G_UNICODE_LOWERCASE_LETTER:
	    case G_UNICODE_MODIFIER_LETTER:
	    case G_UNICODE_OTHER_LETTER:
	    case G_UNICODE_TITLECASE_LETTER:
	    case G_UNICODE_UPPERCASE_LETTER:
	      current_word_type = WordLetters;
	      last_word_letter = wc;
	      attrs[i].is_word_start = TRUE;
	      break;

	    case G_UNICODE_DECIMAL_NUMBER:
	    case G_UNICODE_LETTER_NUMBER:
	    case G_UNICODE_OTHER_NUMBER:
	      current_word_type = WordNumbers;
	      last_word_letter = wc;
	      attrs[i].is_word_start = TRUE;
	      break;

	    default:
	      /* No word here */
	      break;
	    }
	}

      /* ---- Sentence breaks ---- */
      {

	/* default to not a sentence start/end */
	attrs[i].is_sentence_start = FALSE;
	attrs[i].is_sentence_end = FALSE;

	/* maybe start sentence */
	if (last_sentence_start == -1 && !is_sentence_boundary)
	  last_sentence_start = i - 1;

	/* remember last non space character position */
	if (i > 0 && !attrs[i - 1].is_white)
	  last_non_space = i;

	/* meets sentence end, mark both sentence start and end */
	if (last_sentence_start != -1 && is_sentence_boundary) {
	  if (last_non_space >= last_sentence_start) {
	    attrs[last_sentence_start].is_sentence_start = TRUE;
	    attrs[last_non_space].is_sentence_end = TRUE;
	  }

	  last_sentence_start = -1;
	  last_non_space = -1;
	}

	/* meets space character, move sentence start */
	if (last_sentence_start != -1 &&
	    last_sentence_start == i - 1 &&
	    attrs[i - 1].is_white) {
	    last_sentence_start++;
          }
      }

      /* --- Hyphens --- */

      {
        gboolean insert_hyphens;
        gboolean space_or_hyphen = FALSE;

        attrs[i].break_inserts_hyphen = FALSE;
        attrs[i].break_removes_preceding = FALSE;

        switch ((int)prev_script)
          {
          case PANGO_SCRIPT_COMMON:
            insert_hyphens = prev_wc == 0x00ad;
            break;
          case PANGO_SCRIPT_HAN:
          case PANGO_SCRIPT_HANGUL:
          case PANGO_SCRIPT_HIRAGANA:
          case PANGO_SCRIPT_KATAKANA:
            insert_hyphens = FALSE;
            break;
          default:
            insert_hyphens = TRUE;
            break;
          }

        switch ((int)type)
          {
          case G_UNICODE_SPACE_SEPARATOR:
          case G_UNICODE_LINE_SEPARATOR:
          case G_UNICODE_PARAGRAPH_SEPARATOR:
            space_or_hyphen = TRUE;
            break;
          case G_UNICODE_CONTROL:
            if (wc == '\t' || wc == '\n' || wc == '\r' || wc == '\f')
              space_or_hyphen = TRUE;
            break;
          default:
            break;
          }

        if (!space_or_hyphen)
          {
            if (wc == '-'    || /* Hyphen-minus */
                wc == 0x058a || /* Armenian hyphen */
                wc == 0x1400 || /* Canadian syllabics hyphen */
                wc == 0x1806 || /* Mongolian todo hyphen */
                wc == 0x2010 || /* Hyphen */
                wc == 0x2e17 || /* Double oblique hyphen */
                wc == 0x2e40 || /* Double hyphen */
                wc == 0x30a0 || /* Katakana-Hiragana double hyphen */
                wc == 0xfe63 || /* Small hyphen-minus */
                wc == 0xff0d)   /* Fullwidth hyphen-minus */
              space_or_hyphen = TRUE;
          }

        if (attrs[i].is_word_boundary)
          attrs[i].break_inserts_hyphen = FALSE;
        else if (prev_space_or_hyphen)
          attrs[i].break_inserts_hyphen = FALSE;
        else if (space_or_hyphen)
          attrs[i].break_inserts_hyphen = FALSE;
        else
          attrs[i].break_inserts_hyphen = insert_hyphens;

        if (prev_wc == 0x2027)     /* Hyphenation point */
          {
            attrs[i].break_inserts_hyphen = TRUE;
            attrs[i].break_removes_preceding = TRUE;
          }

        prev_space_or_hyphen = space_or_hyphen;
      }

      prev_prev_wc = prev_wc;
      prev_wc = wc;
      prev_script = script;

      /* wc might not be a valid Unicode base character, but really all we
       * need to know is the last non-combining character */
      if (type != G_UNICODE_SPACING_MARK &&
	  type != G_UNICODE_ENCLOSING_MARK &&
	  type != G_UNICODE_NON_SPACING_MARK)
	base_character = wc;
    }

  i--;

  attrs[0].is_cursor_position = TRUE;  /* Rule GB1 */
  attrs[i].is_cursor_position = TRUE;  /* Rule GB2 */

  attrs[0].is_word_boundary = TRUE;  /* Rule WB1 */
  attrs[i].is_word_boundary = TRUE;  /* Rule WB2 */

  attrs[0].is_line_break = FALSE; /* Rule LB2 */
  attrs[i].is_line_break = TRUE;  /* Rule LB3 */
  attrs[i].is_mandatory_break = TRUE;  /* Rule LB3 */
}

/* }}} */
/* {{{ Tailoring */
/* {{{ Script-specific tailoring */

#include "break-arabic.c"
#include "break-indic.c"
#include "break-thai.c"
#include "break-latin.c"

static gboolean
break_script (const char          *item_text,
	      unsigned int         item_length,
	      const PangoAnalysis *analysis,
	      PangoLogAttr        *attrs,
	      int                  attrs_len)
{
  switch (analysis->script)
    {
    case PANGO_SCRIPT_ARABIC:
      break_arabic (item_text, item_length, analysis, attrs, attrs_len);
      break;

    case PANGO_SCRIPT_DEVANAGARI:
    case PANGO_SCRIPT_BENGALI:
    case PANGO_SCRIPT_GURMUKHI:
    case PANGO_SCRIPT_GUJARATI:
    case PANGO_SCRIPT_ORIYA:
    case PANGO_SCRIPT_TAMIL:
    case PANGO_SCRIPT_TELUGU:
    case PANGO_SCRIPT_KANNADA:
    case PANGO_SCRIPT_MALAYALAM:
    case PANGO_SCRIPT_SINHALA:
      break_indic (item_text, item_length, analysis, attrs, attrs_len);
      break;

    case PANGO_SCRIPT_THAI:
      break_thai (item_text, item_length, analysis, attrs, attrs_len);
      break;

    case PANGO_SCRIPT_LATIN:
      break_latin (item_text, item_length, analysis, attrs, attrs_len);
      break;

    default:
      return FALSE;
    }

  return TRUE;
}

/* }}} */
/* {{{ Attribute-based customization */

/* We allow customizing log attrs in two ways:
 *
 * - You can directly remove breaks from a range, using allow_breaks=false.
 *   We preserve the non-tailorable rules from UAX #14, so mandatory breaks
 *   and breaks after ZWS remain. We also preserve break opportunities after
 *   hyphens and visible word dividers.
 *
 * - You can tweak the segmentation by marking ranges as word or sentence.
 *   When doing so, we split adjacent segments to preserve alternating
 *   starts and ends. We add a line break opportunity before each word that
 *   is created in this way, and we remove line break opportunities inside
 *   the word in the same way as for a range marked as allow_breaks=false,
 *   except that we don't remove char break opportunities.
 *
 *   Note that UAX #14 does not guarantee that words fall neatly into
 *   sentences, so we don't do extra work to enforce that.
 */

static void
remove_breaks_from_range (const char   *text,
                          int           start,
                          PangoLogAttr *log_attrs,
                          int           start_pos,
                          int           end_pos)
{
  int pos;
  const char *p;
  gunichar ch;
  int bt;
  gboolean after_zws;
  gboolean after_hyphen;

  /* Assume our range doesn't start after a hyphen or in a zws sequence */
  after_zws = FALSE;
  after_hyphen = FALSE;
  for (pos = start_pos + 1, p = g_utf8_next_char (text + start);
       pos < end_pos;
       pos++, p = g_utf8_next_char (p))
    {
      /* Mandatory breaks aren't tailorable */
      if (!log_attrs[pos].is_mandatory_break)
        log_attrs[pos].is_line_break = FALSE;

      ch = g_utf8_get_char (p);
      bt = g_unichar_break_type (ch);

      /* Hyphens and visible word dividers */
      if (after_hyphen)
        log_attrs[pos].is_line_break = TRUE;

      after_hyphen = ch == 0x00ad || /* Soft Hyphen */
         ch == 0x05A0 || ch == 0x2010 || /* Breaking Hyphens */
         ch == 0x2012 || ch == 0x2013 ||
         ch == 0x05BE || ch == 0x0F0B || /* Visible word dividers */
         ch == 0x1361 || ch == 0x17D8 ||
         ch == 0x17DA || ch == 0x2027 ||
         ch == 0x007C;

      /* ZWS sequence */
      if (after_zws && bt != G_UNICODE_BREAK_SPACE)
        log_attrs[pos].is_line_break = TRUE;

      after_zws = bt == G_UNICODE_BREAK_ZERO_WIDTH_SPACE ||
                  (bt == G_UNICODE_BREAK_SPACE && after_zws);
    }
}

static gboolean
handle_allow_breaks (const char    *text,
                     int            length,
                     PangoAttrList *attrs,
                     int            offset,
                     PangoLogAttr  *log_attrs,
                     int            log_attrs_len)
{
  PangoAttrIterator iter;
  gboolean tailored = FALSE;

  _pango_attr_list_get_iterator (attrs, &iter);

  do
    {
      const PangoAttribute *attr = pango_attr_iterator_get (&iter, PANGO_ATTR_ALLOW_BREAKS);

      if (!attr)
        continue;

      if (!((PangoAttrInt*)attr)->value)
        {
          int start, end;
          int start_pos, end_pos;
          int pos;

          start = attr->start_index;
          end = attr->end_index;
          if (start < offset)
            start_pos = 0;
          else
            start_pos = g_utf8_pointer_to_offset (text, text + start - offset);
          if (end >= offset + length)
            end_pos = log_attrs_len;
          else
            end_pos = g_utf8_pointer_to_offset (text, text + end - offset);

          for (pos = start_pos + 1; pos < end_pos; pos++)
            log_attrs[pos].is_char_break = FALSE;

          remove_breaks_from_range (text, MAX (start - offset, 0), log_attrs, start_pos, end_pos);

          tailored = TRUE;
        }
    }
  while (pango_attr_iterator_next (&iter));

  _pango_attr_iterator_destroy (&iter);

  return tailored;
}


static gboolean
handle_words (const char    *text,
              int            length,
              PangoAttrList *attrs,
              int            offset,
              PangoLogAttr  *log_attrs,
              int            log_attrs_len)
{
  PangoAttrIterator iter;
  gboolean tailored = FALSE;

  _pango_attr_list_get_iterator (attrs, &iter);

  do
    {
      const PangoAttribute *attr = pango_attr_iterator_get (&iter, PANGO_ATTR_WORD);
      int start, end;
      int start_pos, end_pos;
      int pos;

      if (!attr)
        continue;

      start = attr->start_index;
      end = attr->end_index;
      if (start < offset)
        start_pos = 0;
      else
        start_pos = g_utf8_pointer_to_offset (text, text + start - offset);
      if (end >= offset + length)
        end_pos = log_attrs_len;
      else
        end_pos = g_utf8_pointer_to_offset (text, text + end - offset);

      for (pos = start_pos + 1; pos < end_pos; pos++)
        {
          log_attrs[pos].is_word_start = FALSE;
          log_attrs[pos].is_word_end = FALSE;
          log_attrs[pos].is_word_boundary = FALSE;
        }

      remove_breaks_from_range (text, MAX (start - offset, 0), log_attrs,
                                start_pos, end_pos);

      if (start >= offset)
        {
          gboolean in_word = FALSE;
          for (pos = start_pos; pos >= 0; pos--)
            {
              if (log_attrs[pos].is_word_end)
                {
                  in_word = pos == start_pos;
                  break;
                }
              if (pos < start_pos && log_attrs[pos].is_word_start)
                {
                  in_word = TRUE;
                  break;
                }
            }
          log_attrs[start_pos].is_word_start = TRUE;
          log_attrs[start_pos].is_word_end = in_word;
          log_attrs[start_pos].is_word_boundary = TRUE;

          /* Allow line breaks before words */
          if (start_pos > 0)
            log_attrs[start_pos].is_line_break = TRUE;

          tailored = TRUE;
        }

      if (end < offset + length)
        {
          gboolean in_word = FALSE;
          for (pos = end_pos; pos < log_attrs_len; pos++)
            {
              if (log_attrs[pos].is_word_start)
                {
                  in_word = pos == end_pos;
                  break;
                }
              if (pos > end_pos && log_attrs[pos].is_word_end)
                {
                  in_word = TRUE;
                  break;
                }
            }
          log_attrs[end_pos].is_word_start = in_word;
          log_attrs[end_pos].is_word_end = TRUE;
          log_attrs[end_pos].is_word_boundary = TRUE;

          /* Allow line breaks before words */
          if (in_word)
            log_attrs[end_pos].is_line_break = TRUE;

          tailored = TRUE;
        }
    }
  while (pango_attr_iterator_next (&iter));

  _pango_attr_iterator_destroy (&iter);

  return tailored;
}

static gboolean
handle_sentences (const char    *text,
                  int            length,
                  PangoAttrList *attrs,
                  int            offset,
                  PangoLogAttr  *log_attrs,
                  int            log_attrs_len)
{
  PangoAttrIterator iter;
  gboolean tailored = FALSE;

  _pango_attr_list_get_iterator (attrs, &iter);

  do
    {
      const PangoAttribute *attr = pango_attr_iterator_get (&iter, PANGO_ATTR_SENTENCE);
      int start, end;
      int start_pos, end_pos;
      int pos;

      if (!attr)
        continue;

      start = attr->start_index;
      end = attr->end_index;
      if (start < offset)
        start_pos = 0;
      else
        start_pos = g_utf8_pointer_to_offset (text, text + start - offset);
      if (end >= offset + length)
        end_pos = log_attrs_len;
      else
        end_pos = g_utf8_pointer_to_offset (text, text + end - offset);

      for (pos = start_pos + 1; pos < end_pos; pos++)
        {
          log_attrs[pos].is_sentence_start = FALSE;
          log_attrs[pos].is_sentence_end = FALSE;
          log_attrs[pos].is_sentence_boundary = FALSE;

          tailored = TRUE;
        }
      if (start >= offset)
        {
          gboolean in_sentence = FALSE;
          for (pos = start_pos - 1; pos >= 0; pos--)
            {
              if (log_attrs[pos].is_sentence_end)
                break;
              if (log_attrs[pos].is_sentence_start)
                {
                  in_sentence = TRUE;
                  break;
                }
            }
          log_attrs[start_pos].is_sentence_start = TRUE;
          log_attrs[start_pos].is_sentence_end = in_sentence;
          log_attrs[start_pos].is_sentence_boundary = TRUE;

          tailored = TRUE;
        }
      if (end < offset + length)
        {
          gboolean in_sentence = FALSE;
          for (pos = end_pos + 1; end_pos < log_attrs_len; pos++)
            {
              if (log_attrs[pos].is_sentence_start)
                break;
              if (log_attrs[pos].is_sentence_end)
                {
                  in_sentence = TRUE;
                  break;
                }
            }
          log_attrs[end_pos].is_sentence_start = in_sentence;
          log_attrs[end_pos].is_sentence_end = TRUE;
          log_attrs[end_pos].is_sentence_boundary = TRUE;

          tailored = TRUE;
        }
    }
  while (pango_attr_iterator_next (&iter));

  _pango_attr_iterator_destroy (&iter);

  return tailored;
}

static gboolean
handle_hyphens (const char    *text,
                int            length,
                PangoAttrList *attrs,
                int            offset,
                PangoLogAttr  *log_attrs,
                int            log_attrs_len)
{
  PangoAttrIterator iter;
  gboolean tailored = FALSE;

  _pango_attr_list_get_iterator (attrs, &iter);

  do {
    const PangoAttribute *attr = pango_attr_iterator_get (&iter, PANGO_ATTR_INSERT_HYPHENS);

    if (attr && ((PangoAttrInt*)attr)->value == 0)
      {
        int start, end;
        int start_pos, end_pos;
        int pos;

        pango_attr_iterator_range (&iter, &start, &end);
        if (start < offset)
          start_pos = 0;
        else
          start_pos = g_utf8_pointer_to_offset (text, text + start - offset);
        if (end >= offset + length)
          end_pos = log_attrs_len;
        else
          end_pos = g_utf8_pointer_to_offset (text, text + end - offset);

        for (pos = start_pos + 1; pos < end_pos; pos++)
          {
            if (!log_attrs[pos].break_removes_preceding)
              {
                log_attrs[pos].break_inserts_hyphen = FALSE;

                tailored = TRUE;
              }
          }
      }
  } while (pango_attr_iterator_next (&iter));

  _pango_attr_iterator_destroy (&iter);

  return tailored;
}

static gboolean
break_attrs (const char   *text,
             int           length,
             GSList       *attributes,
             int           offset,
             PangoLogAttr *log_attrs,
             int           log_attrs_len)
{
  PangoAttrList allow_breaks;
  PangoAttrList words;
  PangoAttrList sentences;
  PangoAttrList hyphens;
  GSList *l;
  gboolean tailored = FALSE;

  _pango_attr_list_init (&allow_breaks);
  _pango_attr_list_init (&words);
  _pango_attr_list_init (&sentences);
  _pango_attr_list_init (&hyphens);

  for (l = attributes; l; l = l->next)
    {
      PangoAttribute *attr = l->data;

      if (attr->klass->type == PANGO_ATTR_ALLOW_BREAKS)
        pango_attr_list_insert (&allow_breaks, pango_attribute_copy (attr));
      else if (attr->klass->type == PANGO_ATTR_WORD)
        pango_attr_list_insert (&words, pango_attribute_copy (attr));
      else if (attr->klass->type == PANGO_ATTR_SENTENCE)
        pango_attr_list_insert (&sentences, pango_attribute_copy (attr));
      else if (attr->klass->type == PANGO_ATTR_INSERT_HYPHENS)
        pango_attr_list_insert (&hyphens, pango_attribute_copy (attr));
    }

  tailored |= handle_words (text, length, &words, offset,
                            log_attrs, log_attrs_len);

  tailored |= handle_sentences (text, length, &words, offset,
                                log_attrs, log_attrs_len);

  tailored |= handle_hyphens (text, length, &hyphens, offset,
                              log_attrs, log_attrs_len);

  tailored |= handle_allow_breaks (text, length, &allow_breaks, offset,
                                   log_attrs, log_attrs_len);

  _pango_attr_list_destroy (&allow_breaks);
  _pango_attr_list_destroy (&words);
  _pango_attr_list_destroy (&sentences);
  _pango_attr_list_destroy (&hyphens);

  return tailored;
}

/* }}} */

static gboolean
tailor_break (const char    *text,
              int            length,
              PangoAnalysis *analysis,
              int            item_offset,
              PangoLogAttr  *attrs,
              int            attrs_len)
{
  gboolean res;

  if (length < 0)
    length = strlen (text);
  else if (text == NULL)
    text = "";

  res = break_script (text, length, analysis, attrs, attrs_len);

  if (item_offset >= 0 && analysis->extra_attrs)
    res |= break_attrs (text, length, analysis->extra_attrs, item_offset, attrs, attrs_len);

  return res;
}

/* }}} */
/* {{{ Public API */

/**
 * pango_default_break:
 * @text: text to break. Must be valid UTF-8
 * @length: length of text in bytes (may be -1 if @text is nul-terminated)
 * @analysis: (nullable): a `PangoAnalysis` structure for the @text
 * @attrs: logical attributes to fill in
 * @attrs_len: size of the array passed as @attrs
 *
 * This is the default break algorithm.
 *
 * It applies rules from the [Unicode Line Breaking Algorithm](http://www.unicode.org/unicode/reports/tr14/)
 * without language-specific tailoring, therefore the @analyis argument is unused
 * and can be %NULL.
 *
 * See [func@Pango.tailor_break] for language-specific breaks.
 *
 * See [func@Pango.attr_break] for attribute-based customization.
 */
void
pango_default_break (const char    *text,
                     int            length,
                     PangoAnalysis *analysis G_GNUC_UNUSED,
                     PangoLogAttr  *attrs,
                     int            attrs_len G_GNUC_UNUSED)
{
  PangoLogAttr before = *attrs;

  default_break (text, length, analysis, attrs, attrs_len);

  attrs->is_line_break      |= before.is_line_break;
  attrs->is_mandatory_break |= before.is_mandatory_break;
  attrs->is_cursor_position |= before.is_cursor_position;
}

/**
 * pango_break:
 * @text: the text to process. Must be valid UTF-8
 * @length: length of @text in bytes (may be -1 if @text is nul-terminated)
 * @analysis: `PangoAnalysis` structure for @text
 * @attrs: (array length=attrs_len): an array to store character information in
 * @attrs_len: size of the array passed as @attrs
 *
 * Determines possible line, word, and character breaks
 * for a string of Unicode text with a single analysis.
 *
 * For most purposes you may want to use [func@Pango.get_log_attrs].
 *
 * Deprecated: 1.44: Use [func@Pango.default_break],
 *   [func@Pango.tailor_break] and [func@Pango.attr_break].
 */
void
pango_break (const char    *text,
             gint           length,
             PangoAnalysis *analysis,
             PangoLogAttr  *attrs,
             int            attrs_len)
{
  g_return_if_fail (analysis != NULL);
  g_return_if_fail (attrs != NULL);

  default_break (text, length, analysis, attrs, attrs_len);
  tailor_break (text, length, analysis, -1, attrs, attrs_len);
}

/**
 * pango_tailor_break:
 * @text: text to process. Must be valid UTF-8
 * @length: length in bytes of @text
 * @analysis: `PangoAnalysis` for @text
 * @offset: Byte offset of @text from the beginning of the
 *   paragraph, or -1 to ignore attributes from @analysis
 * @attrs: (array length=attrs_len): array with one `PangoLogAttr`
 *   per character in @text, plus one extra, to be filled in
 * @attrs_len: length of @attrs array
 *
 * Apply language-specific tailoring to the breaks in @attrs.
 *
 * The line breaks are assumed to have been produced by [func@Pango.default_break].
 *
 * If @offset is not -1, it is used to apply attributes from @analysis that are
 * relevant to line breaking.
 *
 * Note that it is better to pass -1 for @offset and use [func@Pango.attr_break]
 * to apply attributes to the whole paragraph.
 *
 * Since: 1.44
 */
void
pango_tailor_break (const char    *text,
                    int            length,
                    PangoAnalysis *analysis,
                    int            offset,
                    PangoLogAttr  *attrs,
                    int            attrs_len)
{
  PangoLogAttr *start = attrs;
  PangoLogAttr attr_before = *start;

  if (tailor_break (text, length, analysis, offset, attrs, attrs_len))
    {
      /* if tailored, we enforce some of the attrs from before
       * tailoring at the boundary
       */

     start->backspace_deletes_character  = attr_before.backspace_deletes_character;

     start->is_line_break      |= attr_before.is_line_break;
     start->is_mandatory_break |= attr_before.is_mandatory_break;
     start->is_cursor_position |= attr_before.is_cursor_position;
    }
}

/**
 * pango_attr_break:
 * @text: text to break. Must be valid UTF-8
 * @length: length of text in bytes (may be -1 if @text is nul-terminated)
 * @attr_list: `PangoAttrList` to apply
 * @offset: Byte offset of @text from the beginning of the paragraph
 * @attrs: (array length=attrs_len): array with one `PangoLogAttr`
 *   per character in @text, plus one extra, to be filled in
 * @attrs_len: length of @attrs array
 *
 * Apply customization from attributes to the breaks in @attrs.
 *
 * The line breaks are assumed to have been produced
 * by [func@Pango.default_break] and [func@Pango.tailor_break].
 *
 * Since: 1.50
 */
void
pango_attr_break (const char    *text,
                  int            length,
                  PangoAttrList *attr_list,
                  int            offset,
                  PangoLogAttr  *attrs,
                  int            attrs_len)
{
  PangoLogAttr *start = attrs;
  PangoLogAttr attr_before = *start;
  GSList *attributes;

  attributes = pango_attr_list_get_attributes (attr_list);
  if (break_attrs (text, length, attributes, offset, attrs, attrs_len))
    {
      /* if tailored, we enforce some of the attrs from before
       * tailoring at the boundary
       */

      start->backspace_deletes_character  = attr_before.backspace_deletes_character;

      start->is_line_break      |= attr_before.is_line_break;
      start->is_mandatory_break |= attr_before.is_mandatory_break;
      start->is_cursor_position |= attr_before.is_cursor_position;
    }

  g_slist_free_full (attributes, (GDestroyNotify)pango_attribute_destroy);
}

/**
 * pango_get_log_attrs:
 * @text: text to process. Must be valid UTF-8
 * @length: length in bytes of @text
 * @level: embedding level, or -1 if unknown
 * @language: language tag
 * @attrs: (array length=attrs_len): array with one `PangoLogAttr`
 *   per character in @text, plus one extra, to be filled in
 * @attrs_len: length of @attrs array
 *
 * Computes a `PangoLogAttr` for each character in @text.
 *
 * The @attrs array must have one `PangoLogAttr` for
 * each position in @text; if @text contains N characters,
 * it has N+1 positions, including the last position at the
 * end of the text. @text should be an entire paragraph;
 * logical attributes can't be computed without context
 * (for example you need to see spaces on either side of
 * a word to know the word is a word).
 */
void
pango_get_log_attrs (const char    *text,
                     int            length,
                     int            level,
                     PangoLanguage *language,
                     PangoLogAttr  *attrs,
                     int            attrs_len)
{
  int chars_broken;
  PangoAnalysis analysis = { NULL };
  PangoScriptIter iter;

  g_return_if_fail (length == 0 || text != NULL);
  g_return_if_fail (attrs != NULL);

  analysis.level = level;
  analysis.language = language;

  pango_default_break (text, length, &analysis, attrs, attrs_len);

  chars_broken = 0;

  _pango_script_iter_init (&iter, text, length);
  do
    {
      const char *run_start, *run_end;
      PangoScript script;
      int chars_in_range;

      pango_script_iter_get_range (&iter, &run_start, &run_end, &script);
      analysis.script = script;

      chars_in_range = pango_utf8_strlen (run_start, run_end - run_start);

      pango_tailor_break (run_start,
                          run_end - run_start,
                          &analysis,
                          -1,
                          attrs + chars_broken,
                          chars_in_range + 1);

      chars_broken += chars_in_range;
    }
  while (pango_script_iter_next (&iter));
  _pango_script_iter_fini (&iter);

  if (chars_broken + 1 > attrs_len)
    g_warning ("pango_get_log_attrs: attrs_len should have been at least %d, but was %d.  Expect corrupted memory.",
               chars_broken + 1,
               attrs_len);
}

/* }}} */

/* vim:set foldmethod=marker expandtab: */
