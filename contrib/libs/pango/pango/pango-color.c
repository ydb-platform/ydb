/* pango
 * pango-color.c: Color handling
 *
 * Copyright (C) 2000 Red Hat Software
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "pango-attributes.h"
#include "pango-impl-utils.h"
#include "pango-utils-internal.h"

G_DEFINE_BOXED_TYPE (PangoColor, pango_color,
                     pango_color_copy,
                     pango_color_free);

/**
 * pango_color_copy:
 * @src: (nullable): color to copy
 *
 * Creates a copy of @src.
 *
 * The copy should be freed with [method@Pango.Color.free].
 * Primarily used by language bindings, not that useful
 * otherwise (since colors can just be copied by assignment
 * in C).
 *
 * Return value: (nullable): the newly allocated `PangoColor`,
 *   which should be freed with [method@Pango.Color.free]
 */
PangoColor*
pango_color_copy (const PangoColor *src)
{
  PangoColor *ret;

  if (src == NULL)
    return NULL;

  ret = g_slice_new (PangoColor);

  *ret = *src;

  return ret;
}

/**
 * pango_color_free:
 * @color: (nullable): an allocated `PangoColor`
 *
 * Frees a color allocated by [method@Pango.Color.copy].
 */
void
pango_color_free (PangoColor *color)
{
  if (color == NULL)
    return;

  g_slice_free (PangoColor, color);
}

/**
 * pango_color_to_string:
 * @color: a `PangoColor`
 *
 * Returns a textual specification of @color.
 *
 * The string is in the hexadecimal form `#rrrrggggbbbb`,
 * where `r`, `g` and `b` are hex digits representing the
 * red, green, and blue components respectively.
 *
 * Return value: a newly-allocated text string that must
 *   be freed with g_free().
 *
 * Since: 1.16
 */
gchar *
pango_color_to_string (const PangoColor *color)
{
  g_return_val_if_fail (color != NULL, NULL);

  return g_strdup_printf ("#%04x%04x%04x", color->red, color->green, color->blue);
}

/* Color parsing
 */

/* The following 2 routines (parse_color, find_color) come from Tk, via the Win32
 * port of GDK. The licensing terms on these (longer than the functions) is:
 *
 * This software is copyrighted by the Regents of the University of
 * California, Sun Microsystems, Inc., and other parties.  The following
 * terms apply to all files associated with the software unless explicitly
 * disclaimed in individual files.
 *
 * The authors hereby grant permission to use, copy, modify, distribute,
 * and license this software and its documentation for any purpose, provided
 * that existing copyright notices are retained in all copies and that this
 * notice is included verbatim in any distributions. No written agreement,
 * license, or royalty fee is required for any of the authorized uses.
 * Modifications to this software may be copyrighted by their authors
 * and need not follow the licensing terms described here, provided that
 * the new terms are clearly indicated on the first page of each file where
 * they apply.
 *
 * IN NO EVENT SHALL THE AUTHORS OR DISTRIBUTORS BE LIABLE TO ANY PARTY
 * FOR DIRECT, INDIRECT, SPECIAL, INCIDENTAL, OR CONSEQUENTIAL DAMAGES
 * ARISING OUT OF THE USE OF THIS SOFTWARE, ITS DOCUMENTATION, OR ANY
 * DERIVATIVES THEREOF, EVEN IF THE AUTHORS HAVE BEEN ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * THE AUTHORS AND DISTRIBUTORS SPECIFICALLY DISCLAIM ANY WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, AND NON-INFRINGEMENT.  THIS SOFTWARE
 * IS PROVIDED ON AN "AS IS" BASIS, AND THE AUTHORS AND DISTRIBUTORS HAVE
 * NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES, ENHANCEMENTS, OR
 * MODIFICATIONS.
 *
 * GOVERNMENT USE: If you are acquiring this software on behalf of the
 * U.S. government, the Government shall have only "Restricted Rights"
 * in the software and related documentation as defined in the Federal
 * Acquisition Regulations (FARs) in Clause 52.227.19 (c) (2).  If you
 * are acquiring the software on behalf of the Department of Defense, the
 * software shall be classified as "Commercial Computer Software" and the
 * Government shall have only "Restricted Rights" as defined in Clause
 * 252.227-7013 (c) (1) of DFARs.  Notwithstanding the foregoing, the
 * authors grant the U.S. Government and others acting in its behalf
 * permission to use and distribute the software in accordance with the
 * terms specified in this license.
 */

#include "pango-color-table.h"

#define ISUPPER(c)              ((c) >= 'A' && (c) <= 'Z')
#define TOLOWER(c)              (ISUPPER (c) ? (c) - 'A' + 'a' : (c))

static int
compare_xcolor_entries (const void *a, const void *b)
{
  const guchar *s1 = (const guchar *) a;
  const guchar *s2 = (const guchar *) (color_names + ((const ColorEntry *) b)->name_offset);

  while (*s1 && *s2)
    {
      int c1, c2;
      while (*s1 == ' ') s1++;
      while (*s2 == ' ') s1++;
      c1 = (gint)(guchar) TOLOWER (*s1);
      c2 = (gint)(guchar) TOLOWER (*s2);
      if (c1 != c2)
        return (c1 - c2);
      s1++; s2++;
    }

  return ((gint) *s1) - ((gint) *s2);
}

static gboolean
find_color(const char *name,
	   PangoColor *color)
{
  ColorEntry *found;

  found = bsearch (name, color_entries, G_N_ELEMENTS (color_entries),
		   sizeof (ColorEntry),
		   compare_xcolor_entries);
  if (found == NULL)
    return FALSE;

  if (color)
    {
      color->red = (found->red * 65535) / 255;
      color->green = (found->green * 65535) / 255;
      color->blue = (found->blue * 65535) / 255;
    }

  return TRUE;
}

static gboolean
hex (const char *spec,
    int len,
    unsigned int *c)
{
  const char *end;
  *c = 0;
  for (end = spec + len; spec != end; spec++)
    if (g_ascii_isxdigit (*spec))
      *c = (*c << 4) | g_ascii_xdigit_value (*spec);
    else
      return FALSE;
  return TRUE;
}


/**
 * pango_color_parse_with_alpha:
 * @color: (nullable): a `PangoColor` structure in which
 *   to store the result
 * @alpha: (out) (optional): return location for alpha
 * @spec: a string specifying the new color
 *
 * Fill in the fields of a color from a string specification.
 *
 * The string can either one of a large set of standard names.
 * (Taken from the CSS Color [specification](https://www.w3.org/TR/css-color-4/#named-colors),
 * or it can be a hexadecimal value in the form `#rgb`,
 * `#rrggbb`, `#rrrgggbbb` or `#rrrrggggbbbb` where `r`, `g`
 * and `b` are hex digits of the red, green, and blue components
 * of the color, respectively. (White in the four forms is
 * `#fff`, `#ffffff`, `#fffffffff` and `#ffffffffffff`.)
 *
 * Additionally, parse strings of the form `#rgba`, `#rrggbbaa`,
 * `#rrrrggggbbbbaaaa`, if @alpha is not %NULL, and set @alpha
 * to the value specified by the hex digits for `a`. If no alpha
 * component is found in @spec, @alpha is set to 0xffff (for a
 * solid color).
 *
 * Return value: %TRUE if parsing of the specifier succeeded,
 *   otherwise %FALSE
 *
 * Since: 1.46
 */
gboolean
pango_color_parse_with_alpha (PangoColor *color,
                              guint16    *alpha,
                              const char *spec)
{
  g_return_val_if_fail (spec != NULL, FALSE);

  if (alpha)
    *alpha = 0xffff;

  if (spec[0] == '#')
    {
      size_t len;
      unsigned int r, g, b, a;
      gboolean has_alpha;

      spec++;
      len = strlen (spec);
      switch (len)
        {
        case 3:
        case 6:
        case 9:
        case 12:
          len /= 3;
          has_alpha = FALSE;
          break;
        case 4:
        case 8:
        case 16:
          if (!alpha)
            return FALSE;
          len /= 4;
          has_alpha = TRUE;
          break;
        default:
          return FALSE;
        }

      if (!hex (spec, len, &r) ||
          !hex (spec + len, len, &g) ||
          !hex (spec + len * 2, len, &b) ||
          (has_alpha && !hex (spec + len * 3, len, &a)))
        return FALSE;

      if (color)
        {
          int bits = len * 4;
          r <<= 16 - bits;
          g <<= 16 - bits;
          b <<= 16 - bits;
          while (bits < 16)
            {
              r |= (r >> bits);
              g |= (g >> bits);
              b |= (b >> bits);
              bits *= 2;
            }
          color->red   = r;
          color->green = g;
          color->blue  = b;
        }

      if (alpha && has_alpha)
        {
          int bits = len * 4;
          a <<= 16 - bits;
          while (bits < 16)
            {
              a |= (a >> bits);
              bits *= 2;
            }
          *alpha = a;
        }
    }
  else
    {
      if (!find_color (spec, color))
        return FALSE;
    }
  return TRUE;
}

/**
 * pango_color_parse:
 * @color: (nullable): a `PangoColor` structure in which
 *   to store the result
 * @spec: a string specifying the new color
 *
 * Fill in the fields of a color from a string specification.
 *
 * The string can either one of a large set of standard names.
 * (Taken from the CSS Color [specification](https://www.w3.org/TR/css-color-4/#named-colors),
 * or it can be a value in the form `#rgb`, `#rrggbb`,
 * `#rrrgggbbb` or `#rrrrggggbbbb`, where `r`, `g` and `b`
 * are hex digits of the red, green, and blue components
 * of the color, respectively. (White in the four forms is
 * `#fff`, `#ffffff`, `#fffffffff` and `#ffffffffffff`.)
 *
 * Return value: %TRUE if parsing of the specifier succeeded,
 *   otherwise %FALSE
 */
gboolean
pango_color_parse (PangoColor *color,
                   const char *spec)
{
  return pango_color_parse_with_alpha (color, NULL, spec);
}
