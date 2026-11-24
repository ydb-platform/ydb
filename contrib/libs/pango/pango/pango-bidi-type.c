/* Pango
 * pango-bidi-type.c: Bidirectional Character Types
 *
 * Copyright (C) 2008 Jürg Billeter <j@bitron.ch>
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

#include <fribidi.h>

#undef PANGO_DISABLE_DEPRECATED

#include "pango-bidi-type.h"
#include "pango-utils.h"
#include "pango-utils-private.h"

/**
 * pango_bidi_type_for_unichar:
 * @ch: a Unicode character
 *
 * Determines the bidirectional type of a character.
 *
 * The bidirectional type is specified in the Unicode Character Database.
 *
 * A simplified version of this function is available as [func@unichar_direction].
 *
 * Return value: the bidirectional character type, as used in the
 * Unicode bidirectional algorithm.
 *
 * Since: 1.22
 */
PangoBidiType
pango_bidi_type_for_unichar (gunichar ch)
{
  FriBidiCharType fribidi_ch_type;

  G_STATIC_ASSERT (sizeof (FriBidiChar) == sizeof (gunichar));

  fribidi_ch_type = fribidi_get_bidi_type (ch);

  switch (fribidi_ch_type)
    {
    case FRIBIDI_TYPE_LTR:  return PANGO_BIDI_TYPE_L;
    case FRIBIDI_TYPE_LRE:  return PANGO_BIDI_TYPE_LRE;
    case FRIBIDI_TYPE_LRO:  return PANGO_BIDI_TYPE_LRO;
    case FRIBIDI_TYPE_RTL:  return PANGO_BIDI_TYPE_R;
    case FRIBIDI_TYPE_AL:   return PANGO_BIDI_TYPE_AL;
    case FRIBIDI_TYPE_RLE:  return PANGO_BIDI_TYPE_RLE;
    case FRIBIDI_TYPE_RLO:  return PANGO_BIDI_TYPE_RLO;
    case FRIBIDI_TYPE_PDF:  return PANGO_BIDI_TYPE_PDF;
    case FRIBIDI_TYPE_EN:   return PANGO_BIDI_TYPE_EN;
    case FRIBIDI_TYPE_ES:   return PANGO_BIDI_TYPE_ES;
    case FRIBIDI_TYPE_ET:   return PANGO_BIDI_TYPE_ET;
    case FRIBIDI_TYPE_AN:   return PANGO_BIDI_TYPE_AN;
    case FRIBIDI_TYPE_CS:   return PANGO_BIDI_TYPE_CS;
    case FRIBIDI_TYPE_NSM:  return PANGO_BIDI_TYPE_NSM;
    case FRIBIDI_TYPE_BN:   return PANGO_BIDI_TYPE_BN;
    case FRIBIDI_TYPE_BS:   return PANGO_BIDI_TYPE_B;
    case FRIBIDI_TYPE_SS:   return PANGO_BIDI_TYPE_S;
    case FRIBIDI_TYPE_WS:   return PANGO_BIDI_TYPE_WS;
    case FRIBIDI_TYPE_ON:   return PANGO_BIDI_TYPE_ON;
    case FRIBIDI_TYPE_LRI:  return PANGO_BIDI_TYPE_LRI;
    case FRIBIDI_TYPE_RLI:  return PANGO_BIDI_TYPE_RLI;
    case FRIBIDI_TYPE_FSI:  return PANGO_BIDI_TYPE_FSI;
    case FRIBIDI_TYPE_PDI:  return PANGO_BIDI_TYPE_PDI;
    case _FRIBIDI_TYPE_SENTINEL:
    default:
      return PANGO_BIDI_TYPE_ON;
    }
}

/* Some bidi-related functions */

/**
 * pango_log2vis_get_embedding_levels:
 * @text: the text to itemize.
 * @length: the number of bytes (not characters) to process, or -1
 *   if @text is nul-terminated and the length should be calculated.
 * @pbase_dir: input base direction, and output resolved direction.
 *
 * Return the bidirectional embedding levels of the input paragraph.
 *
 * The bidirectional embedding levels are defined by the [Unicode Bidirectional
 * Algorithm](http://www.unicode.org/reports/tr9/).
 *
 * If the input base direction is a weak direction, the direction of the
 * characters in the text will determine the final resolved direction.
 *
 * Return value: a newly allocated array of embedding levels, one item per
 *   character (not byte), that should be freed using [func@GLib.free].
 *
 * Since: 1.4
 */
guint8 *
pango_log2vis_get_embedding_levels (const gchar    *text,
                                    int             length,
                                    PangoDirection *pbase_dir)
{
  unsigned int n_chars;
  guint8 *embedding_levels;

  if (length < 0)
    length = strlen (text);

  n_chars = g_utf8_strlen (text, length);
  embedding_levels = g_new (guint8, n_chars);

  pango_log2vis_fill_embedding_levels (text, length, n_chars, embedding_levels, pbase_dir);

  return embedding_levels;
}

void
pango_log2vis_fill_embedding_levels (const gchar    *text,
                                    int             length,
                                    unsigned int    n_chars,
                                    guint8         *embedding_levels_list,
                                    PangoDirection *pbase_dir)
{
  glong i;
  const gchar *p;
  FriBidiParType fribidi_base_dir;
  FriBidiCharType *bidi_types;
  FriBidiCharType bidi_types_[64];
  FriBidiBracketType *bracket_types;
  FriBidiBracketType bracket_types_[64];
  FriBidiLevel max_level;
  FriBidiCharType ored_types = 0;
  FriBidiCharType anded_strongs = FRIBIDI_TYPE_RLE;

  G_STATIC_ASSERT (sizeof (FriBidiLevel) == sizeof (guint8));
  G_STATIC_ASSERT (sizeof (FriBidiChar) == sizeof (gunichar));

  switch (*pbase_dir)
    {
    case PANGO_DIRECTION_LTR:
    case PANGO_DIRECTION_TTB_RTL:
      fribidi_base_dir = FRIBIDI_PAR_LTR;
      break;
    case PANGO_DIRECTION_RTL:
    case PANGO_DIRECTION_TTB_LTR:
      fribidi_base_dir = FRIBIDI_PAR_RTL;
      break;
    case PANGO_DIRECTION_WEAK_RTL:
      fribidi_base_dir = FRIBIDI_PAR_WRTL;
      break;
    case PANGO_DIRECTION_WEAK_LTR:
    case PANGO_DIRECTION_NEUTRAL:
    default:
      fribidi_base_dir = FRIBIDI_PAR_WLTR;
      break;
    }

  if (n_chars < 64)
    {
      bidi_types = bidi_types_;
      bracket_types = bracket_types_;
    }
  else
    {
      bidi_types = g_new (FriBidiCharType, n_chars);
      bracket_types = g_new (FriBidiBracketType, n_chars);
    }

  for (i = 0, p = text; p < text + length; p = g_utf8_next_char(p), i++)
    {
      gunichar ch = g_utf8_get_char (p);
      FriBidiCharType char_type = fribidi_get_bidi_type (ch);

      if (i == n_chars)
        break;

      bidi_types[i] = char_type;
      ored_types |= char_type;
      if (FRIBIDI_IS_STRONG (char_type))
        anded_strongs &= char_type;
      if (G_UNLIKELY(bidi_types[i] == FRIBIDI_TYPE_ON))
        bracket_types[i] = fribidi_get_bracket (ch);
      else
        bracket_types[i] = FRIBIDI_NO_BRACKET;
    }

    /* Short-circuit (malloc-expensive) FriBidi call for unidirectional
     * text.
     *
     * For details see:
     * https://bugzilla.gnome.org/show_bug.cgi?id=590183
     */

    /* The case that all resolved levels will be ltr.
     * No isolates, all strongs be LTR, there should be no Arabic numbers
     * (or letters for that matter), and one of the following:
     *
     * o base_dir doesn't have an RTL taste.
     * o there are letters, and base_dir is weak.
     */
    if (!FRIBIDI_IS_ISOLATE (ored_types) &&
	!FRIBIDI_IS_RTL (ored_types) &&
	!FRIBIDI_IS_ARABIC (ored_types) &&
	(!FRIBIDI_IS_RTL (fribidi_base_dir) ||
	  (FRIBIDI_IS_WEAK (fribidi_base_dir) &&
	   FRIBIDI_IS_LETTER (ored_types))
	))
      {
        /* all LTR */
	fribidi_base_dir = FRIBIDI_PAR_LTR;
	memset (embedding_levels_list, 0, n_chars);
	goto resolved;
      }
    /* The case that all resolved levels will be RTL is much more complex.
     * No isolates, no numbers, all strongs are RTL, and one of
     * the following:
     *
     * o base_dir has an RTL taste (may be weak).
     * o there are letters, and base_dir is weak.
     */
    else if (!FRIBIDI_IS_ISOLATE (ored_types) &&
	     !FRIBIDI_IS_NUMBER (ored_types) &&
	     FRIBIDI_IS_RTL (anded_strongs) &&
	     (FRIBIDI_IS_RTL (fribidi_base_dir) ||
	       (FRIBIDI_IS_WEAK (fribidi_base_dir) &&
		FRIBIDI_IS_LETTER (ored_types))
	     ))
      {
        /* all RTL */
	fribidi_base_dir = FRIBIDI_PAR_RTL;
	memset (embedding_levels_list, 1, n_chars);
	goto resolved;
      }


  max_level = fribidi_get_par_embedding_levels_ex (bidi_types, bracket_types, n_chars,
						   &fribidi_base_dir,
						   (FriBidiLevel*)embedding_levels_list);

  if (G_UNLIKELY(max_level == 0))
    {
      /* fribidi_get_par_embedding_levels() failed. */
      memset (embedding_levels_list, 0, length);
    }

resolved:
  if (n_chars >= 64)
    {
      g_free (bidi_types);
      g_free (bracket_types);
    }

  *pbase_dir = (fribidi_base_dir == FRIBIDI_PAR_LTR) ?  PANGO_DIRECTION_LTR : PANGO_DIRECTION_RTL;
}

/**
 * pango_unichar_direction:
 * @ch: a Unicode character
 *
 * Determines the inherent direction of a character.
 *
 * The inherent direction is either `PANGO_DIRECTION_LTR`, `PANGO_DIRECTION_RTL`,
 * or `PANGO_DIRECTION_NEUTRAL`.
 *
 * This function is useful to categorize characters into left-to-right
 * letters, right-to-left letters, and everything else. If full Unicode
 * bidirectional type of a character is needed, [func@Pango.BidiType.for_unichar]
 * can be used instead.
 *
 * Return value: the direction of the character.
 */
PangoDirection
pango_unichar_direction (gunichar ch)
{
  FriBidiCharType fribidi_ch_type;

  G_STATIC_ASSERT (sizeof (FriBidiChar) == sizeof (gunichar));

  fribidi_ch_type = fribidi_get_bidi_type (ch);

  if (!FRIBIDI_IS_STRONG (fribidi_ch_type))
    return PANGO_DIRECTION_NEUTRAL;
  else if (FRIBIDI_IS_RTL (fribidi_ch_type))
    return PANGO_DIRECTION_RTL;
  else
    return PANGO_DIRECTION_LTR;
}


/**
 * pango_get_mirror_char:
 * @ch: a Unicode character
 * @mirrored_ch: location to store the mirrored character
 *
 * Returns the mirrored character of a Unicode character.
 *
 * Mirror characters are determined by the Unicode mirrored property.
 *
 * Return value: %TRUE if @ch has a mirrored character and @mirrored_ch is
 * filled in, %FALSE otherwise
 *
 * Deprecated: 1.30: Use [func@GLib.unichar_get_mirror_char] instead;
 *   the docs for that function provide full details.
 */
gboolean
pango_get_mirror_char (gunichar  ch,
		       gunichar *mirrored_ch)
{
  return g_unichar_get_mirror_char (ch, mirrored_ch);
}

