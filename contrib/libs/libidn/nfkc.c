/* nfkc.c --- Unicode normalization utilities. 
 * Copyright (C) 2002, 2003, 2004, 2006, 2007  Simon Josefsson 
 * 
 * This file is part of GNU Libidn. 
 * 
 * GNU Libidn is free software; you can redistribute it and/or 
 * modify it under the terms of the GNU Lesser General Public 
 * License as published by the Free Software Foundation; either 
 * version 2.1 of the License, or (at your option) any later version. 
 * 
 * GNU Libidn is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of 
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
 * Lesser General Public License for more details. 
 * 
 * You should have received a copy of the GNU Lesser General Public 
 * License along with GNU Libidn; if not, write to the Free Software 
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA 
 * 
 */ 
 
#ifdef HAVE_CONFIG_H 
# include "idn_config.h"
#endif 
 
#include <stdlib.h> 
#include <string.h> 
 
#include "stringprep.h" 
 
/* This file contains functions from GLIB, including gutf8.c and 
 * gunidecomp.c, all licensed under LGPL and copyright hold by: 
 * 
 *  Copyright (C) 1999, 2000 Tom Tromey 
 *  Copyright 2000 Red Hat, Inc. 
 */ 
 
/* Hacks to make syncing with GLIB code easier. */ 
#define gboolean int 
#define gchar char 
#define guchar unsigned char 
#define glong long 
#define gint int 
#define guint unsigned int 
#define gushort unsigned short 
#define gint16 int16_t 
#define guint16 uint16_t 
#define gunichar uint32_t 
#define gsize size_t 
#define gssize ssize_t 
#define g_malloc malloc 
#define g_free free 
#define GError void 
#define g_set_error(a,b,c,d) ((void) 0) 
#define g_new(struct_type, n_structs)					\ 
  ((struct_type *) g_malloc (((gsize) sizeof (struct_type)) * ((gsize) (n_structs)))) 
#  if defined (__GNUC__) && !defined (__STRICT_ANSI__) && !defined (__cplusplus) 
#    define G_STMT_START	(void)( 
#    define G_STMT_END		) 
#  else 
#    if (defined (sun) || defined (__sun__)) 
#      define G_STMT_START	if (1) 
#      define G_STMT_END	else (void)0 
#    else 
#      define G_STMT_START	do 
#      define G_STMT_END	while (0) 
#    endif 
#  endif 
#define g_return_val_if_fail(expr,val)		G_STMT_START{ (void)0; }G_STMT_END 
#define G_N_ELEMENTS(arr)		(sizeof (arr) / sizeof ((arr)[0])) 
#define TRUE 1 
#define FALSE 0 
 
/* Code from GLIB gunicode.h starts here. */ 
 
typedef enum 
{ 
  G_NORMALIZE_DEFAULT, 
  G_NORMALIZE_NFD = G_NORMALIZE_DEFAULT, 
  G_NORMALIZE_DEFAULT_COMPOSE, 
  G_NORMALIZE_NFC = G_NORMALIZE_DEFAULT_COMPOSE, 
  G_NORMALIZE_ALL, 
  G_NORMALIZE_NFKD = G_NORMALIZE_ALL, 
  G_NORMALIZE_ALL_COMPOSE, 
  G_NORMALIZE_NFKC = G_NORMALIZE_ALL_COMPOSE 
} 
GNormalizeMode; 
 
/* Code from GLIB gutf8.c starts here. */ 
 
#define UTF8_COMPUTE(Char, Mask, Len)		\ 
  if (Char < 128)				\ 
    {						\ 
      Len = 1;					\ 
      Mask = 0x7f;				\ 
    }						\ 
  else if ((Char & 0xe0) == 0xc0)		\ 
    {						\ 
      Len = 2;					\ 
      Mask = 0x1f;				\ 
    }						\ 
  else if ((Char & 0xf0) == 0xe0)		\ 
    {						\ 
      Len = 3;					\ 
      Mask = 0x0f;				\ 
    }						\ 
  else if ((Char & 0xf8) == 0xf0)		\ 
    {						\ 
      Len = 4;					\ 
      Mask = 0x07;				\ 
    }						\ 
  else if ((Char & 0xfc) == 0xf8)		\ 
    {						\ 
      Len = 5;					\ 
      Mask = 0x03;				\ 
    }						\ 
  else if ((Char & 0xfe) == 0xfc)		\ 
    {						\ 
      Len = 6;					\ 
      Mask = 0x01;				\ 
    }						\ 
  else						\ 
    Len = -1; 
 
#define UTF8_LENGTH(Char)			\ 
  ((Char) < 0x80 ? 1 :				\ 
   ((Char) < 0x800 ? 2 :			\ 
    ((Char) < 0x10000 ? 3 :			\ 
     ((Char) < 0x200000 ? 4 :			\ 
      ((Char) < 0x4000000 ? 5 : 6))))) 
 
 
#define UTF8_GET(Result, Chars, Count, Mask, Len)	\ 
  (Result) = (Chars)[0] & (Mask);			\ 
  for ((Count) = 1; (Count) < (Len); ++(Count))		\ 
    {							\ 
      if (((Chars)[(Count)] & 0xc0) != 0x80)		\ 
	{						\ 
	  (Result) = -1;				\ 
	  break;					\ 
	}						\ 
      (Result) <<= 6;					\ 
      (Result) |= ((Chars)[(Count)] & 0x3f);		\ 
    } 
 
#define UNICODE_VALID(Char)			\ 
  ((Char) < 0x110000 &&				\ 
   (((Char) & 0xFFFFF800) != 0xD800) &&		\ 
   ((Char) < 0xFDD0 || (Char) > 0xFDEF) &&	\ 
   ((Char) & 0xFFFE) != 0xFFFE) 
 
 
static const gchar utf8_skip_data[256] = { 
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 
  1, 1, 1, 1, 1, 1, 1, 
  2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 
  2, 2, 2, 2, 2, 2, 2, 
  3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 3, 4, 4, 4, 4, 4, 4, 4, 4, 5, 
  5, 5, 5, 6, 6, 1, 1 
}; 
 
static const gchar *const g_utf8_skip = utf8_skip_data; 
 
#define g_utf8_next_char(p) (char *)((p) + g_utf8_skip[*(guchar *)(p)]) 
 
/* 
 * g_utf8_strlen: 
 * @p: pointer to the start of a UTF-8 encoded string. 
 * @max: the maximum number of bytes to examine. If @max 
 *       is less than 0, then the string is assumed to be 
 *       nul-terminated. If @max is 0, @p will not be examined and 
 *       may be %NULL. 
 * 
 * Returns the length of the string in characters. 
 * 
 * Return value: the length of the string in characters 
 **/ 
static glong 
g_utf8_strlen (const gchar * p, gssize max) 
{ 
  glong len = 0; 
  const gchar *start = p; 
  g_return_val_if_fail (p != NULL || max == 0, 0); 
 
  if (max < 0) 
    { 
      while (*p) 
	{ 
	  p = g_utf8_next_char (p); 
	  ++len; 
	} 
    } 
  else 
    { 
      if (max == 0 || !*p) 
	return 0; 
 
      p = g_utf8_next_char (p); 
 
      while (p - start < max && *p) 
	{ 
	  ++len; 
	  p = g_utf8_next_char (p); 
	} 
 
      /* only do the last len increment if we got a complete 
       * char (don't count partial chars) 
       */ 
      if (p - start == max) 
	++len; 
    } 
 
  return len; 
} 
 
/* 
 * g_utf8_get_char: 
 * @p: a pointer to Unicode character encoded as UTF-8 
 * 
 * Converts a sequence of bytes encoded as UTF-8 to a Unicode character. 
 * If @p does not point to a valid UTF-8 encoded character, results are 
 * undefined. If you are not sure that the bytes are complete 
 * valid Unicode characters, you should use g_utf8_get_char_validated() 
 * instead. 
 * 
 * Return value: the resulting character 
 **/ 
static gunichar 
g_utf8_get_char (const gchar * p) 
{ 
  int i, mask = 0, len; 
  gunichar result; 
  unsigned char c = (unsigned char) *p; 
 
  UTF8_COMPUTE (c, mask, len); 
  if (len == -1) 
    return (gunichar) - 1; 
  UTF8_GET (result, p, i, mask, len); 
 
  return result; 
} 
 
/* 
 * g_unichar_to_utf8: 
 * @c: a ISO10646 character code 
 * @outbuf: output buffer, must have at least 6 bytes of space. 
 *       If %NULL, the length will be computed and returned 
 *       and nothing will be written to @outbuf. 
 * 
 * Converts a single character to UTF-8. 
 * 
 * Return value: number of bytes written 
 **/ 
static int 
g_unichar_to_utf8 (gunichar c, gchar * outbuf) 
{ 
  guint len = 0; 
  int first; 
  int i; 
 
  if (c < 0x80) 
    { 
      first = 0; 
      len = 1; 
    } 
  else if (c < 0x800) 
    { 
      first = 0xc0; 
      len = 2; 
    } 
  else if (c < 0x10000) 
    { 
      first = 0xe0; 
      len = 3; 
    } 
  else if (c < 0x200000) 
    { 
      first = 0xf0; 
      len = 4; 
    } 
  else if (c < 0x4000000) 
    { 
      first = 0xf8; 
      len = 5; 
    } 
  else 
    { 
      first = 0xfc; 
      len = 6; 
    } 
 
  if (outbuf) 
    { 
      for (i = len - 1; i > 0; --i) 
	{ 
	  outbuf[i] = (c & 0x3f) | 0x80; 
	  c >>= 6; 
	} 
      outbuf[0] = c | first; 
    } 
 
  return len; 
} 
 
/* 
 * g_utf8_to_ucs4_fast: 
 * @str: a UTF-8 encoded string 
 * @len: the maximum length of @str to use. If @len < 0, then 
 *       the string is nul-terminated. 
 * @items_written: location to store the number of characters in the 
 *                 result, or %NULL. 
 * 
 * Convert a string from UTF-8 to a 32-bit fixed width 
 * representation as UCS-4, assuming valid UTF-8 input. 
 * This function is roughly twice as fast as g_utf8_to_ucs4() 
 * but does no error checking on the input. 
 * 
 * Return value: a pointer to a newly allocated UCS-4 string. 
 *               This value must be freed with g_free(). 
 **/ 
static gunichar * 
g_utf8_to_ucs4_fast (const gchar * str, glong len, size_t * items_written)
{ 
  gint j, charlen; 
  gunichar *result; 
  gint n_chars, i;
  const gchar *p = str;
 
  g_return_val_if_fail (str != NULL, NULL); 
 
  n_chars = 0; 
  if (len < 0) 
    { 
      while (*p) 
	{ 
	  p = g_utf8_next_char (p); 
	  ++n_chars; 
	} 
    } 
  else 
    { 
      while (p < str + len && *p) 
	{ 
	  p = g_utf8_next_char (p); 
	  ++n_chars; 
	} 
    } 
 
  result = g_new (gunichar, n_chars + 1); 
  if (!result) 
    return NULL; 
 
  p = str; 
  for (i = 0; i < n_chars; i++) 
    { 
      gunichar wc = ((unsigned char *) p)[0]; 
 
      if (wc < 0x80) 
	{ 
	  result[i] = wc; 
	  p++; 
	} 
      else 
	{ 
	  if (wc < 0xe0) 
	    { 
	      charlen = 2; 
	      wc &= 0x1f; 
	    } 
	  else if (wc < 0xf0) 
	    { 
	      charlen = 3; 
	      wc &= 0x0f; 
	    } 
	  else if (wc < 0xf8) 
	    { 
	      charlen = 4; 
	      wc &= 0x07; 
	    } 
	  else if (wc < 0xfc) 
	    { 
	      charlen = 5; 
	      wc &= 0x03; 
	    } 
	  else 
	    { 
	      charlen = 6; 
	      wc &= 0x01; 
	    } 
 
	  for (j = 1; j < charlen; j++) 
	    { 
	      wc <<= 6; 
	      wc |= ((unsigned char *) p)[j] & 0x3f; 
	    } 
 
	  result[i] = wc; 
	  p += charlen; 
	} 
    } 
  result[i] = 0; 
 
  if (items_written) 
    *items_written = i; 
 
  return result; 
} 
 
/* 
 * g_ucs4_to_utf8: 
 * @str: a UCS-4 encoded string 
 * @len: the maximum length of @str to use. If @len < 0, then 
 *       the string is terminated with a 0 character. 
 * @items_read: location to store number of characters read read, or %NULL. 
 * @items_written: location to store number of bytes written or %NULL. 
 *                 The value here stored does not include the trailing 0 
 *                 byte. 
 * @error: location to store the error occuring, or %NULL to ignore 
 *         errors. Any of the errors in #GConvertError other than 
 *         %G_CONVERT_ERROR_NO_CONVERSION may occur. 
 * 
 * Convert a string from a 32-bit fixed width representation as UCS-4. 
 * to UTF-8. The result will be terminated with a 0 byte. 
 * 
 * Return value: a pointer to a newly allocated UTF-8 string. 
 *               This value must be freed with g_free(). If an 
 *               error occurs, %NULL will be returned and 
 *               @error set. 
 **/ 
static gchar * 
g_ucs4_to_utf8 (const gunichar * str, 
		glong len, 
		size_t * items_read, size_t * items_written, GError ** error)
{ 
  gint result_length; 
  gchar *result = NULL; 
  gchar *p; 
  gint i; 
 
  result_length = 0; 
  for (i = 0; len < 0 || i < len; i++) 
    { 
      if (!str[i]) 
	break; 
 
      if (str[i] >= 0x80000000) 
	{ 
	  if (items_read) 
	    *items_read = i; 
 
	  g_set_error (error, G_CONVERT_ERROR, 
		       G_CONVERT_ERROR_ILLEGAL_SEQUENCE, 
		       _("Character out of range for UTF-8")); 
	  goto err_out; 
	} 
 
      result_length += UTF8_LENGTH (str[i]); 
    } 
 
  result = g_malloc (result_length + 1); 
  if (!result) 
    return NULL; 
  p = result; 
 
  i = 0; 
  while (p < result + result_length) 
    p += g_unichar_to_utf8 (str[i++], p); 
 
  *p = '\0'; 
 
  if (items_written) 
    *items_written = p - result; 
 
err_out: 
  if (items_read) 
    *items_read = i; 
 
  return result; 
} 
 
/* Code from GLIB gunidecomp.c starts here. */ 
 
#include "gunidecomp.h" 
#include "gunicomp.h" 
 
#define CC_PART1(Page, Char) \ 
  ((combining_class_table_part1[Page] >= G_UNICODE_MAX_TABLE_INDEX) \ 
   ? (combining_class_table_part1[Page] - G_UNICODE_MAX_TABLE_INDEX) \ 
   : (cclass_data[combining_class_table_part1[Page]][Char])) 
 
#define CC_PART2(Page, Char) \ 
  ((combining_class_table_part2[Page] >= G_UNICODE_MAX_TABLE_INDEX) \ 
   ? (combining_class_table_part2[Page] - G_UNICODE_MAX_TABLE_INDEX) \ 
   : (cclass_data[combining_class_table_part2[Page]][Char])) 
 
#define COMBINING_CLASS(Char) \ 
  (((Char) <= G_UNICODE_LAST_CHAR_PART1) \ 
   ? CC_PART1 ((Char) >> 8, (Char) & 0xff) \ 
   : (((Char) >= 0xe0000 && (Char) <= G_UNICODE_LAST_CHAR) \ 
      ? CC_PART2 (((Char) - 0xe0000) >> 8, (Char) & 0xff) \ 
      : 0)) 
 
/* constants for hangul syllable [de]composition */ 
#define SBase 0xAC00 
#define LBase 0x1100 
#define VBase 0x1161 
#define TBase 0x11A7 
#define LCount 19 
#define VCount 21 
#define TCount 28 
#define NCount (VCount * TCount) 
#define SCount (LCount * NCount) 
 
/* 
 * g_unicode_canonical_ordering: 
 * @string: a UCS-4 encoded string. 
 * @len: the maximum length of @string to use. 
 * 
 * Computes the canonical ordering of a string in-place. 
 * This rearranges decomposed characters in the string 
 * according to their combining classes.  See the Unicode 
 * manual for more information. 
 **/ 
static void 
g_unicode_canonical_ordering (gunichar * string, gsize len) 
{ 
  gsize i; 
  int swap = 1; 
 
  while (swap) 
    { 
      int last; 
      swap = 0; 
      last = COMBINING_CLASS (string[0]); 
      for (i = 0; i < len - 1; ++i) 
	{ 
	  int next = COMBINING_CLASS (string[i + 1]); 
	  if (next != 0 && last > next) 
	    { 
	      gsize j; 
	      /* Percolate item leftward through string.  */ 
	      for (j = i + 1; j > 0; --j) 
		{ 
		  gunichar t; 
		  if (COMBINING_CLASS (string[j - 1]) <= next) 
		    break; 
		  t = string[j]; 
		  string[j] = string[j - 1]; 
		  string[j - 1] = t; 
		  swap = 1; 
		} 
	      /* We're re-entering the loop looking at the old 
	         character again.  */ 
	      next = last; 
	    } 
	  last = next; 
	} 
    } 
} 
 
/* http://www.unicode.org/unicode/reports/tr15/#Hangul 
 * r should be null or have sufficient space. Calling with r == NULL will 
 * only calculate the result_len; however, a buffer with space for three 
 * characters will always be big enough. */ 
static void 
decompose_hangul (gunichar s, gunichar * r, gsize * result_len) 
{ 
  gint SIndex = s - SBase; 
 
  /* not a hangul syllable */ 
  if (SIndex < 0 || SIndex >= SCount) 
    { 
      if (r) 
	r[0] = s; 
      *result_len = 1; 
    } 
  else 
    { 
      gunichar L = LBase + SIndex / NCount; 
      gunichar V = VBase + (SIndex % NCount) / TCount; 
      gunichar T = TBase + SIndex % TCount; 
 
      if (r) 
	{ 
	  r[0] = L; 
	  r[1] = V; 
	} 
 
      if (T != TBase) 
	{ 
	  if (r) 
	    r[2] = T; 
	  *result_len = 3; 
	} 
      else 
	*result_len = 2; 
    } 
} 
 
/* returns a pointer to a null-terminated UTF-8 string */ 
static const gchar * 
find_decomposition (gunichar ch, gboolean compat) 
{ 
  int start = 0; 
  int end = G_N_ELEMENTS (decomp_table); 
 
  if (ch >= decomp_table[start].ch && ch <= decomp_table[end - 1].ch) 
    { 
      while (TRUE) 
	{ 
	  int half = (start + end) / 2; 
	  if (ch == decomp_table[half].ch) 
	    { 
	      int offset; 
 
	      if (compat) 
		{ 
		  offset = decomp_table[half].compat_offset; 
		  if (offset == G_UNICODE_NOT_PRESENT_OFFSET) 
		    offset = decomp_table[half].canon_offset; 
		} 
	      else 
		{ 
		  offset = decomp_table[half].canon_offset; 
		  if (offset == G_UNICODE_NOT_PRESENT_OFFSET) 
		    return NULL; 
		} 
 
	      return &(decomp_expansion_string[offset]); 
	    } 
	  else if (half == start) 
	    break; 
	  else if (ch > decomp_table[half].ch) 
	    start = half; 
	  else 
	    end = half; 
	} 
    } 
 
  return NULL; 
} 
 
/* L,V => LV and LV,T => LVT  */ 
static gboolean 
combine_hangul (gunichar a, gunichar b, gunichar * result) 
{ 
  gint LIndex = a - LBase; 
  gint SIndex = a - SBase; 
 
  gint VIndex = b - VBase; 
  gint TIndex = b - TBase; 
 
  if (0 <= LIndex && LIndex < LCount && 0 <= VIndex && VIndex < VCount) 
    { 
      *result = SBase + (LIndex * VCount + VIndex) * TCount; 
      return TRUE; 
    } 
  else if (0 <= SIndex && SIndex < SCount && (SIndex % TCount) == 0 
	   && 0 <= TIndex && TIndex <= TCount) 
    { 
      *result = a + TIndex; 
      return TRUE; 
    } 
 
  return FALSE; 
} 
 
#define CI(Page, Char) \ 
  ((compose_table[Page] >= G_UNICODE_MAX_TABLE_INDEX) \ 
   ? (compose_table[Page] - G_UNICODE_MAX_TABLE_INDEX) \ 
   : (compose_data[compose_table[Page]][Char])) 
 
#define COMPOSE_INDEX(Char) \ 
     ((((Char) >> 8) > (COMPOSE_TABLE_LAST)) ? 0 : CI((Char) >> 8, (Char) & 0xff)) 
 
static gboolean 
combine (gunichar a, gunichar b, gunichar * result) 
{ 
  gushort index_a, index_b; 
 
  if (combine_hangul (a, b, result)) 
    return TRUE; 
 
  index_a = COMPOSE_INDEX (a); 
 
  if (index_a >= COMPOSE_FIRST_SINGLE_START && index_a < COMPOSE_SECOND_START) 
    { 
      if (b == compose_first_single[index_a - COMPOSE_FIRST_SINGLE_START][0]) 
	{ 
	  *result = 
	    compose_first_single[index_a - COMPOSE_FIRST_SINGLE_START][1]; 
	  return TRUE; 
	} 
      else 
	return FALSE; 
    } 
 
  index_b = COMPOSE_INDEX (b); 
 
  if (index_b >= COMPOSE_SECOND_SINGLE_START) 
    { 
      if (a == 
	  compose_second_single[index_b - COMPOSE_SECOND_SINGLE_START][0]) 
	{ 
	  *result = 
	    compose_second_single[index_b - COMPOSE_SECOND_SINGLE_START][1]; 
	  return TRUE; 
	} 
      else 
	return FALSE; 
    } 
 
  if (index_a >= COMPOSE_FIRST_START && index_a < COMPOSE_FIRST_SINGLE_START 
      && index_b >= COMPOSE_SECOND_START 
      && index_b < COMPOSE_SECOND_SINGLE_START) 
    { 
      gunichar res = 
	compose_array[index_a - COMPOSE_FIRST_START][index_b - 
						     COMPOSE_SECOND_START]; 
 
      if (res) 
	{ 
	  *result = res; 
	  return TRUE; 
	} 
    } 
 
  return FALSE; 
} 
 
static gunichar * 
_g_utf8_normalize_wc (const gchar * str, gssize max_len, GNormalizeMode mode) 
{ 
  gsize n_wc; 
  gunichar *wc_buffer; 
  const char *p; 
  gsize last_start; 
  gboolean do_compat = (mode == G_NORMALIZE_NFKC || mode == G_NORMALIZE_NFKD); 
  gboolean do_compose = (mode == G_NORMALIZE_NFC || mode == G_NORMALIZE_NFKC); 
 
  n_wc = 0; 
  p = str; 
  while ((max_len < 0 || p < str + max_len) && *p) 
    { 
      const gchar *decomp; 
      gunichar wc = g_utf8_get_char (p); 
 
      if (wc >= 0xac00 && wc <= 0xd7af) 
	{ 
	  gsize result_len; 
	  decompose_hangul (wc, NULL, &result_len); 
	  n_wc += result_len; 
	} 
      else 
	{ 
	  decomp = find_decomposition (wc, do_compat); 
 
	  if (decomp) 
	    n_wc += g_utf8_strlen (decomp, -1); 
	  else 
	    n_wc++; 
	} 
 
      p = g_utf8_next_char (p); 
    } 
 
  wc_buffer = g_new (gunichar, n_wc + 1); 
  if (!wc_buffer) 
    return NULL; 
 
  last_start = 0; 
  n_wc = 0; 
  p = str; 
  while ((max_len < 0 || p < str + max_len) && *p) 
    { 
      gunichar wc = g_utf8_get_char (p); 
      const gchar *decomp; 
      int cc; 
      gsize old_n_wc = n_wc; 
 
      if (wc >= 0xac00 && wc <= 0xd7af) 
	{ 
	  gsize result_len; 
	  decompose_hangul (wc, wc_buffer + n_wc, &result_len); 
	  n_wc += result_len; 
	} 
      else 
	{ 
	  decomp = find_decomposition (wc, do_compat); 
 
	  if (decomp) 
	    { 
	      const char *pd; 
	      for (pd = decomp; *pd != '\0'; pd = g_utf8_next_char (pd)) 
		wc_buffer[n_wc++] = g_utf8_get_char (pd); 
	    } 
	  else 
	    wc_buffer[n_wc++] = wc; 
	} 
 
      if (n_wc > 0) 
	{ 
	  cc = COMBINING_CLASS (wc_buffer[old_n_wc]); 
 
	  if (cc == 0) 
	    { 
	      g_unicode_canonical_ordering (wc_buffer + last_start, 
					    n_wc - last_start); 
	      last_start = old_n_wc; 
	    } 
	} 
 
      p = g_utf8_next_char (p); 
    } 
 
  if (n_wc > 0) 
    { 
      g_unicode_canonical_ordering (wc_buffer + last_start, 
				    n_wc - last_start); 
      last_start = n_wc; 
    } 
 
  wc_buffer[n_wc] = 0; 
 
  /* All decomposed and reordered */ 
 
  if (do_compose && n_wc > 0) 
    { 
      gsize i, j; 
      int last_cc = 0; 
      last_start = 0; 
 
      for (i = 0; i < n_wc; i++) 
	{ 
	  int cc = COMBINING_CLASS (wc_buffer[i]); 
 
	  if (i > 0 && 
	      (last_cc == 0 || last_cc != cc) && 
	      combine (wc_buffer[last_start], wc_buffer[i], 
		       &wc_buffer[last_start])) 
	    { 
	      for (j = i + 1; j < n_wc; j++) 
		wc_buffer[j - 1] = wc_buffer[j]; 
	      n_wc--; 
	      i--; 
 
	      if (i == last_start) 
		last_cc = 0; 
	      else 
		last_cc = COMBINING_CLASS (wc_buffer[i - 1]); 
 
	      continue; 
	    } 
 
	  if (cc == 0) 
	    last_start = i; 
 
	  last_cc = cc; 
	} 
    } 
 
  wc_buffer[n_wc] = 0; 
 
  return wc_buffer; 
} 
 
/* 
 * g_utf8_normalize: 
 * @str: a UTF-8 encoded string. 
 * @len: length of @str, in bytes, or -1 if @str is nul-terminated. 
 * @mode: the type of normalization to perform. 
 * 
 * Converts a string into canonical form, standardizing 
 * such issues as whether a character with an accent 
 * is represented as a base character and combining 
 * accent or as a single precomposed character. You 
 * should generally call g_utf8_normalize() before 
 * comparing two Unicode strings. 
 * 
 * The normalization mode %G_NORMALIZE_DEFAULT only 
 * standardizes differences that do not affect the 
 * text content, such as the above-mentioned accent 
 * representation. %G_NORMALIZE_ALL also standardizes 
 * the "compatibility" characters in Unicode, such 
 * as SUPERSCRIPT THREE to the standard forms 
 * (in this case DIGIT THREE). Formatting information 
 * may be lost but for most text operations such 
 * characters should be considered the same. 
 * For example, g_utf8_collate() normalizes 
 * with %G_NORMALIZE_ALL as its first step. 
 * 
 * %G_NORMALIZE_DEFAULT_COMPOSE and %G_NORMALIZE_ALL_COMPOSE 
 * are like %G_NORMALIZE_DEFAULT and %G_NORMALIZE_ALL, 
 * but returned a result with composed forms rather 
 * than a maximally decomposed form. This is often 
 * useful if you intend to convert the string to 
 * a legacy encoding or pass it to a system with 
 * less capable Unicode handling. 
 * 
 * Return value: a newly allocated string, that is the 
 *   normalized form of @str. 
 **/ 
static gchar * 
g_utf8_normalize (const gchar * str, gssize len, GNormalizeMode mode) 
{ 
  gunichar *result_wc = _g_utf8_normalize_wc (str, len, mode); 
  gchar *result; 
 
  result = g_ucs4_to_utf8 (result_wc, -1, NULL, NULL, NULL); 
  g_free (result_wc); 
 
  return result; 
} 
 
/* Public Libidn API starts here. */ 
 
/** 
 * stringprep_utf8_to_unichar - convert UTF-8 to Unicode code point 
 * @p: a pointer to Unicode character encoded as UTF-8 
 * 
 * Converts a sequence of bytes encoded as UTF-8 to a Unicode character. 
 * If @p does not point to a valid UTF-8 encoded character, results are 
 * undefined. 
 * 
 * Return value: the resulting character. 
 **/ 
uint32_t 
stringprep_utf8_to_unichar (const char *p) 
{ 
  return g_utf8_get_char (p); 
} 
 
/** 
 * stringprep_unichar_to_utf8 - convert Unicode code point to UTF-8 
 * @c: a ISO10646 character code 
 * @outbuf: output buffer, must have at least 6 bytes of space. 
 *       If %NULL, the length will be computed and returned 
 *       and nothing will be written to @outbuf. 
 * 
 * Converts a single character to UTF-8. 
 * 
 * Return value: number of bytes written. 
 **/ 
int 
stringprep_unichar_to_utf8 (uint32_t c, char *outbuf) 
{ 
  return g_unichar_to_utf8 (c, outbuf); 
} 
 
/** 
 * stringprep_utf8_to_ucs4 - convert UTF-8 string to UCS-4 
 * @str: a UTF-8 encoded string 
 * @len: the maximum length of @str to use. If @len < 0, then 
 *       the string is nul-terminated. 
 * @items_written: location to store the number of characters in the 
 *                 result, or %NULL. 
 * 
 * Convert a string from UTF-8 to a 32-bit fixed width 
 * representation as UCS-4, assuming valid UTF-8 input. 
 * This function does no error checking on the input. 
 * 
 * Return value: a pointer to a newly allocated UCS-4 string. 
 *               This value must be freed with free(). 
 **/ 
uint32_t * 
stringprep_utf8_to_ucs4 (const char *str, ssize_t len, size_t * items_written) 
{ 
  return g_utf8_to_ucs4_fast (str, (glong) len, items_written);
} 
 
/** 
 * stringprep_ucs4_to_utf8 - convert UCS-4 string to UTF-8 
 * @str: a UCS-4 encoded string 
 * @len: the maximum length of @str to use. If @len < 0, then 
 *       the string is terminated with a 0 character. 
 * @items_read: location to store number of characters read read, or %NULL. 
 * @items_written: location to store number of bytes written or %NULL. 
 *                 The value here stored does not include the trailing 0 
 *                 byte. 
 * 
 * Convert a string from a 32-bit fixed width representation as UCS-4. 
 * to UTF-8. The result will be terminated with a 0 byte. 
 * 
 * Return value: a pointer to a newly allocated UTF-8 string. 
 *               This value must be freed with free(). If an 
 *               error occurs, %NULL will be returned and 
 *               @error set. 
 **/ 
char * 
stringprep_ucs4_to_utf8 (const uint32_t * str, ssize_t len, 
			 size_t * items_read, size_t * items_written) 
{ 
  return g_ucs4_to_utf8 (str, len, items_read, items_written, NULL);
} 
 
/** 
 * stringprep_utf8_nfkc_normalize - normalize Unicode string 
 * @str: a UTF-8 encoded string. 
 * @len: length of @str, in bytes, or -1 if @str is nul-terminated. 
 * 
 * Converts a string into canonical form, standardizing 
 * such issues as whether a character with an accent 
 * is represented as a base character and combining 
 * accent or as a single precomposed character. 
 * 
 * The normalization mode is NFKC (ALL COMPOSE).  It standardizes 
 * differences that do not affect the text content, such as the 
 * above-mentioned accent representation. It standardizes the 
 * "compatibility" characters in Unicode, such as SUPERSCRIPT THREE to 
 * the standard forms (in this case DIGIT THREE). Formatting 
 * information may be lost but for most text operations such 
 * characters should be considered the same. It returns a result with 
 * composed forms rather than a maximally decomposed form. 
 * 
 * Return value: a newly allocated string, that is the 
 *   NFKC normalized form of @str. 
 **/ 
char * 
stringprep_utf8_nfkc_normalize (const char *str, ssize_t len) 
{ 
  return g_utf8_normalize (str, len, G_NORMALIZE_NFKC); 
} 
 
/** 
 * stringprep_ucs4_nfkc_normalize - normalize Unicode string 
 * @str: a Unicode string. 
 * @len: length of @str array, or -1 if @str is nul-terminated. 
 * 
 * Converts UCS4 string into UTF-8 and runs 
 * stringprep_utf8_nfkc_normalize(). 
 * 
 * Return value: a newly allocated Unicode string, that is the NFKC 
 *   normalized form of @str. 
 **/ 
uint32_t * 
stringprep_ucs4_nfkc_normalize (uint32_t * str, ssize_t len) 
{ 
  char *p; 
  uint32_t *result_wc; 
 
  p = stringprep_ucs4_to_utf8 (str, len, 0, 0); 
  result_wc = _g_utf8_normalize_wc (p, -1, G_NORMALIZE_NFKC); 
  free (p); 
 
  return result_wc; 
} 
