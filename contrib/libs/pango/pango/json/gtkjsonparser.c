/*
 * Copyright © 2021 Benjamin Otte
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
 *
 * Authors: Benjamin Otte <otte@gnome.org>
 */


#include "config.h"

#include "gtkjsonparserprivate.h"
#include <stdlib.h>
#include <errno.h>

typedef struct _GtkJsonBlock GtkJsonBlock;

typedef enum {
  GTK_JSON_BLOCK_TOPLEVEL,
  GTK_JSON_BLOCK_OBJECT,
  GTK_JSON_BLOCK_ARRAY,
} GtkJsonBlockType;

struct _GtkJsonBlock
{
  GtkJsonBlockType type;
  const guchar *value; /* start of current value to be consumed by external code */
  const guchar *member_name; /* name of current value, only used for object types */
  gsize index; /* index of the current element */
};

struct _GtkJsonParser
{
  GBytes *bytes;
  const guchar *reader; /* current read head, pointing as far as we've read */
  const guchar *start; /* pointer at start of data, after optional BOM */
  const guchar *end; /* pointer after end of data we're reading */

  GError *error; /* if an error has happened, it's stored here. Errors aren't recoverable. */
  const guchar *error_start; /* start of error location */
  const guchar *error_end; /* end of error location */

  GtkJsonBlock *block; /* current block */
  GtkJsonBlock *blocks; /* blocks array */
  GtkJsonBlock *blocks_end; /* blocks array */
  GtkJsonBlock blocks_preallocated[128]; /* preallocated */
};

typedef enum {
  WHITESPACE     = (1 << 4),
  NEWLINE        = (1 << 5),
  STRING_ELEMENT = (1 << 6),
  STRING_MARKER  = (1 << 7),
} JsonCharacterType;

#define JSON_CHARACTER_NODE_MASK ((1 << 4) - 1)

static const guchar json_character_table[256] = {
  ['\t'] = WHITESPACE,
  ['\r'] = WHITESPACE | NEWLINE,
  ['\n'] = WHITESPACE | NEWLINE,
  [' ']  = WHITESPACE | STRING_ELEMENT,
  ['!']  = STRING_ELEMENT,
  ['"']  = GTK_JSON_STRING | STRING_MARKER,
  ['#']  = STRING_ELEMENT,
  ['$']  = STRING_ELEMENT,
  ['%']  = STRING_ELEMENT,
  ['&']  = STRING_ELEMENT,
  ['\''] = STRING_ELEMENT,
  ['(']  = STRING_ELEMENT,
  [')']  = STRING_ELEMENT,
  ['*']  = STRING_ELEMENT,
  ['+']  = STRING_ELEMENT,
  [',']  = STRING_ELEMENT,
  ['-']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['.']  = STRING_ELEMENT,
  ['/']  = STRING_ELEMENT,
  ['0']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['1']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['2']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['3']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['4']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['5']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['6']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['7']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['8']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  ['9']  = GTK_JSON_NUMBER | STRING_ELEMENT,
  [':']  = STRING_ELEMENT,
  [';']  = STRING_ELEMENT,
  ['<']  = STRING_ELEMENT,
  ['=']  = STRING_ELEMENT,
  ['>']  = STRING_ELEMENT,
  ['?']  = STRING_ELEMENT,
  ['@']  = STRING_ELEMENT,
  ['A']  = STRING_ELEMENT,
  ['B']  = STRING_ELEMENT,
  ['C']  = STRING_ELEMENT,
  ['D']  = STRING_ELEMENT,
  ['E']  = STRING_ELEMENT,
  ['F']  = STRING_ELEMENT,
  ['G']  = STRING_ELEMENT,
  ['H']  = STRING_ELEMENT,
  ['I']  = STRING_ELEMENT,
  ['J']  = STRING_ELEMENT,
  ['K']  = STRING_ELEMENT,
  ['L']  = STRING_ELEMENT,
  ['M']  = STRING_ELEMENT,
  ['N']  = STRING_ELEMENT,
  ['O']  = STRING_ELEMENT,
  ['P']  = STRING_ELEMENT,
  ['Q']  = STRING_ELEMENT,
  ['R']  = STRING_ELEMENT,
  ['S']  = STRING_ELEMENT,
  ['T']  = STRING_ELEMENT,
  ['U']  = STRING_ELEMENT,
  ['V']  = STRING_ELEMENT,
  ['W']  = STRING_ELEMENT,
  ['X']  = STRING_ELEMENT,
  ['Y']  = STRING_ELEMENT,
  ['Z']  = STRING_ELEMENT,
  ['[']  = GTK_JSON_ARRAY | STRING_ELEMENT,
  ['\\'] = STRING_MARKER,
  [']']  = STRING_ELEMENT,
  ['^']  = STRING_ELEMENT,
  ['_']  = STRING_ELEMENT,
  ['`']  = STRING_ELEMENT,
  ['a']  = STRING_ELEMENT,
  ['b']  = STRING_ELEMENT,
  ['c']  = STRING_ELEMENT,
  ['d']  = STRING_ELEMENT,
  ['e']  = STRING_ELEMENT,
  ['f']  = GTK_JSON_BOOLEAN | STRING_ELEMENT,
  ['g']  = STRING_ELEMENT,
  ['h']  = STRING_ELEMENT,
  ['i']  = STRING_ELEMENT,
  ['j']  = STRING_ELEMENT,
  ['k']  = STRING_ELEMENT,
  ['l']  = STRING_ELEMENT,
  ['m']  = STRING_ELEMENT,
  ['n']  = GTK_JSON_NULL | STRING_ELEMENT,
  ['o']  = STRING_ELEMENT,
  ['p']  = STRING_ELEMENT,
  ['q']  = STRING_ELEMENT,
  ['r']  = STRING_ELEMENT,
  ['s']  = STRING_ELEMENT,
  ['t']  = GTK_JSON_BOOLEAN | STRING_ELEMENT,
  ['u']  = STRING_ELEMENT,
  ['v']  = STRING_ELEMENT,
  ['w']  = STRING_ELEMENT,
  ['x']  = STRING_ELEMENT,
  ['y']  = STRING_ELEMENT,
  ['z']  = STRING_ELEMENT,
  ['{']  = GTK_JSON_OBJECT | STRING_ELEMENT,
  ['|']  = STRING_ELEMENT,
  ['}']  = STRING_ELEMENT,
  ['~']  = STRING_ELEMENT,
  [127]  = STRING_ELEMENT,
};

static const guchar *
json_skip_characters (const guchar      *start,
                      const guchar      *end,
                      JsonCharacterType  type)
{
  const guchar *s;

  for (s = start; s < end; s++)
    {
      if (!(json_character_table[*s] & type))
        break;
    }
  return s;
}

static const guchar *
json_skip_characters_until (const guchar      *start,
                            const guchar      *end,
                            JsonCharacterType  type)
{
  const guchar *s;

  for (s = start; s < end; s++)
    {
      if (json_character_table[*s] & type)
        break;
    }
  return s;
}

static const guchar *
json_find_character (const guchar      *start,
                     JsonCharacterType  type)
{
  const guchar *s;

  for (s = start; ; s++)
    {
      if ((json_character_table[*s] & type))
        break;
    }
  return s;
}

GQuark
gtk_json_error_quark (void)
{
  return g_quark_from_static_string ("gtk-json-error-quark");
}

static void
gtk_json_parser_take_error (GtkJsonParser *self,
                            const guchar  *start_location,
                            const guchar  *end_location,
                            GError        *error)
{
  g_assert (start_location <= end_location);
  g_assert (self->start <= start_location);
  g_assert (end_location <= self->end);

  if (self->error)
    {
      g_error_free (error);
      return;
    }

  self->error = error;
  self->error_start = start_location;
  self->error_end = end_location;
}

static void
gtk_json_parser_syntax_error_at (GtkJsonParser *self,
                                 const guchar  *error_start,
                                 const guchar  *error_end,
                                 const char    *format,
                                 ...) G_GNUC_PRINTF(4, 5);
static void
gtk_json_parser_syntax_error_at (GtkJsonParser *self,
                                 const guchar  *error_start,
                                 const guchar  *error_end,
                                 const char    *format,
                                 ...)
{
  va_list args;

  if (self->error)
    return;

  va_start (args, format);
  gtk_json_parser_take_error (self,
                              error_start,
                              error_end,
                              g_error_new_valist (GTK_JSON_ERROR,
                                                  GTK_JSON_ERROR_SYNTAX,
                                                  format, args));
  va_end (args);
}

static void
gtk_json_parser_syntax_error (GtkJsonParser *self,
                              const char    *format,
                              ...) G_GNUC_PRINTF(2, 3);
static void
gtk_json_parser_syntax_error (GtkJsonParser *self,
                              const char    *format,
                              ...)
{
  va_list args;
  const guchar *error_end;

  if (self->error)
    return;

  va_start (args, format);
  for (error_end = self->reader;
       error_end < self->end && g_ascii_isalnum (*error_end);
       error_end++)
    ;
  if (error_end == self->reader &&
      g_utf8_get_char_validated ((const char *) error_end, self->end - error_end) < (gunichar) -2)
    {
      error_end = (const guchar *) g_utf8_next_char (error_end);
    }

  gtk_json_parser_take_error (self,
                              self->reader,
                              error_end,
                              g_error_new_valist (GTK_JSON_ERROR,
                                                  GTK_JSON_ERROR_SYNTAX,
                                                  format, args));
  va_end (args);
}

static void
gtk_json_parser_type_error (GtkJsonParser *self,
                            const char    *format,
                            ...) G_GNUC_PRINTF(2, 3);
static void
gtk_json_parser_type_error (GtkJsonParser *self,
                            const char    *format,
                            ...)
{
  const guchar *start_location;
  va_list args;

  if (self->error)
    return;

  if (self->block->value)
    start_location = self->block->value;
  else if (self->block != self->blocks)
    start_location = self->block[-1].value;
  else
    start_location = self->start;

  va_start (args, format);
  gtk_json_parser_take_error (self,
                              start_location,
                              self->reader,
                              g_error_new_valist (GTK_JSON_ERROR,
                                                  GTK_JSON_ERROR_TYPE,
                                                  format, args));
  va_end (args);
}

void
gtk_json_parser_value_error (GtkJsonParser *self,
                             const char    *format,
                             ...)
{
  const guchar *start_location;
  va_list args;

  if (self->error)
    return;

  if (self->block->value)
    start_location = self->block->value;
  else if (self->block != self->blocks)
    start_location = self->block[-1].value;
  else
    start_location = self->start;

  va_start (args, format);
  gtk_json_parser_take_error (self,
                              start_location,
                              self->reader,
                              g_error_new_valist (GTK_JSON_ERROR,
                                                  GTK_JSON_ERROR_VALUE,
                                                  format, args));
  va_end (args);
}

void
gtk_json_parser_schema_error (GtkJsonParser *self,
                              const char    *format,
                              ...)
{
  const guchar *start_location;
  va_list args;

  if (self->error)
    return;

  if (self->block->member_name)
    start_location = self->block->member_name;
  if (self->block->value)
    start_location = self->block->value;
  else if (self->block != self->blocks)
    start_location = self->block[-1].value;
  else
    start_location = self->start;

  va_start (args, format);
  gtk_json_parser_take_error (self,
                              start_location,
                              self->reader,
                              g_error_new_valist (GTK_JSON_ERROR,
                                                  GTK_JSON_ERROR_SCHEMA,
                                                  format, args));
  va_end (args);
}

static gboolean
gtk_json_parser_is_eof (GtkJsonParser *self)
{
  return self->reader >= self->end;
}

static gsize
gtk_json_parser_remaining (GtkJsonParser *self)
{
  g_return_val_if_fail (self->reader <= self->end, 0);

  return self->end - self->reader;
}

static void
gtk_json_parser_skip_bom (GtkJsonParser *self)
{
  if (gtk_json_parser_remaining (self) < 3)
    return;

  if (self->reader[0] == 0xEF &&
      self->reader[1] == 0xBB &&
      self->reader[2] == 0xBF)
    self->reader += 3;
}

static void
gtk_json_parser_skip_whitespace (GtkJsonParser *self)
{
  self->reader = json_skip_characters (self->reader, self->end, WHITESPACE);
}

static gboolean
gtk_json_parser_has_char (GtkJsonParser *self,
                          char           c)
{
  return gtk_json_parser_remaining (self) && *self->reader == c;
}

static gboolean
gtk_json_parser_try_char (GtkJsonParser *self,
                          char           c)
{
  if (!gtk_json_parser_has_char (self, c))
    return FALSE;

  self->reader++;
  return TRUE;
}

static gboolean
gtk_json_parser_try_identifier_len (GtkJsonParser *self,
                                    const char    *ident,
                                    gsize          len)
{
  if (gtk_json_parser_remaining (self) < len)
    return FALSE;

  if (memcmp (self->reader, ident, len) != 0)
    return FALSE;

  self->reader += len;
  return TRUE;
}

#define gtk_json_parser_try_identifier(parser, ident) gtk_json_parser_try_identifier_len(parser, ident, strlen(ident))

/*
 * decode_utf16_surrogate_pair:
 * @first: the first UTF-16 code point
 * @second: the second UTF-16 code point
 *
 * Decodes a surrogate pair of UTF-16 code points into the equivalent
 * Unicode code point.
 *
 * If the code points are not valid, 0 is returned.
 *
 * Returns: the Unicode code point equivalent to the surrogate pair
 */
static inline gunichar
decode_utf16_surrogate_pair (gunichar first,
                             gunichar second)
{
  if (0xd800 > first || first > 0xdbff ||
      0xdc00 > second || second > 0xdfff)
    return 0;

  return 0x10000
       | (first & 0x3ff) << 10
       | (second & 0x3ff);
}

static gsize
gtk_json_unescape_char (const guchar *json_escape,
                        char          out_data[6],
                        gsize        *out_len)
{
  switch (json_escape[1])
    {
    case '"':
    case '\\':
    case '/':
      out_data[0] = json_escape[1];
      *out_len = 1;
      return 2;
    case 'b':
      out_data[0] = '\b';
      *out_len = 1;
      return 2;
    case 'f':
      out_data[0] = '\f';
      *out_len = 1;
      return 2;
    case 'n':
      out_data[0] = '\n';
      *out_len = 1;
      return 2;
    case 'r':
      out_data[0] = '\r';
      *out_len = 1;
      return 2;
    case 't':
      out_data[0] = '\t';
      *out_len = 1;
      return 2;
    case 'u':
      {
        gunichar unichar = (g_ascii_xdigit_value (json_escape[2]) << 12) |
                           (g_ascii_xdigit_value (json_escape[3]) <<  8) |
                           (g_ascii_xdigit_value (json_escape[4]) <<  4) |
                           (g_ascii_xdigit_value (json_escape[5]));
        gsize result = 6;

        /* resolve UTF-16 surrogates for Unicode characters not in the BMP,
         * as per ECMA 404, § 9, "String"
         */
        if (g_unichar_type (unichar) == G_UNICODE_SURROGATE)
          {
            unichar = decode_utf16_surrogate_pair (unichar,
                                                   (g_ascii_xdigit_value (json_escape[8])  << 12) |
                                                   (g_ascii_xdigit_value (json_escape[9])  <<  8) |
                                                   (g_ascii_xdigit_value (json_escape[10]) <<  4) |
                                                   (g_ascii_xdigit_value (json_escape[11])));
            result += 6;
          }
        *out_len = g_unichar_to_utf8 (unichar, out_data);
        return result;
        }
    default:
      g_assert_not_reached ();
      return 0;
    }
}
                   
typedef struct _JsonStringIter JsonStringIter;
struct _JsonStringIter
{
  char buf[6];
  const guchar *s;
  const guchar *next;
};

static gsize
json_string_iter_next (JsonStringIter *iter)
{
  gsize len;

  iter->s = iter->next;
  iter->next = json_find_character (iter->s, STRING_MARKER);
  if (iter->next != iter->s)
    return iter->next - iter->s;
  if (*iter->next == '"')
    return 0;
  iter->next += gtk_json_unescape_char (iter->next, iter->buf, &len);
  iter->s = (const guchar *) iter->buf;
  return len;
}

/* The escaped string MUST be valid json, so it must begin
 * with " and end with " and must not contain any invalid
 * escape codes.
 * This function is meant to be fast
 */
static gsize
json_string_iter_init (JsonStringIter *iter,
                       const guchar   *string)
{
  g_assert (*string == '"');

  iter->next = string + 1;

  return json_string_iter_next (iter);
}

static gboolean
json_string_iter_has_next (JsonStringIter *iter)
{
  return *iter->next != '"';
}

static const char *
json_string_iter_get (JsonStringIter *iter)
{
  return (const char *) iter->s;
}

/* The escaped string MUST be valid json, so it must begin
 * with " and end with " and must not contain any invalid
 * escape codes.
 * This function is meant to be fast
 */
static char *
gtk_json_unescape_string (const guchar *escaped)
{
  JsonStringIter iter;
  GString *string;
  gsize len;

  len = json_string_iter_init (&iter, escaped);
  string = NULL;

  if (!json_string_iter_has_next (&iter))
    return g_strndup (json_string_iter_get (&iter), len);

  string = g_string_new (NULL);

  do
    {
      g_string_append_len (string, json_string_iter_get (&iter), len);
    }
  while ((len = json_string_iter_next (&iter)));
  
  return g_string_free (string, FALSE);
}

static gboolean
gtk_json_parser_parse_string (GtkJsonParser *self)
{
  const guchar *start;

  start = self->reader;

  if (!gtk_json_parser_try_char (self, '"'))
    {
      gtk_json_parser_type_error (self, "Not a string");
      return FALSE;
    }

  self->reader = json_skip_characters (self->reader, self->end, STRING_ELEMENT);

  while (gtk_json_parser_remaining (self))
    {
      if (*self->reader < 0x20)
        {
          if (*self->reader == '\r' || *self->reader == '\n')
            gtk_json_parser_syntax_error (self, "Newlines in strings are not allowed");
          else if (*self->reader == '\t')
            gtk_json_parser_syntax_error (self, "Tabs not allowed in strings");
          else
            gtk_json_parser_syntax_error (self, "Disallowed control character in string literal");
          return FALSE;
        }
      else if (*self->reader > 127)
        {
          gunichar c = g_utf8_get_char_validated ((const char *) self->reader, gtk_json_parser_remaining (self));
          if (c == (gunichar) -2 || c == (gunichar) -1)
            {
              gtk_json_parser_syntax_error (self, "Invalid UTF-8");
              return FALSE;
            }
          self->reader = (const guchar *) g_utf8_next_char ((const char *) self->reader);
        }
      else if (*self->reader == '"')
        {
          self->reader++;
          return TRUE;
        }
      else if (*self->reader == '\\')
        {
          if (gtk_json_parser_remaining (self) < 2)
            {
              self->reader = self->end;
              goto end;
            }
          switch (self->reader[1])
            {
            case '"':
            case '\\':
            case '/':
            case 'b':
            case 'f':
            case 'n':
            case 'r':
            case 't':
              break;

            case 'u':
              /* lots of work necessary to validate the unicode escapes here */
              if (gtk_json_parser_remaining (self) < 6 ||
                  !g_ascii_isxdigit (self->reader[2]) ||
                  !g_ascii_isxdigit (self->reader[3]) ||
                  !g_ascii_isxdigit (self->reader[4]) ||
                  !g_ascii_isxdigit (self->reader[5]))
                {
                  const guchar *end;
                  for (end = self->reader + 2;
                       end < self->reader + 6 && end < self->end;
                       end++)
                    {
                      if (!g_ascii_isxdigit (*end))
                        break;
                    }
                  gtk_json_parser_syntax_error_at (self, self->reader, end, "Invalid Unicode escape sequence");
                  return FALSE;
                }
              else
                {
                  gsize escape_size = 6;
                  gunichar unichar = (g_ascii_xdigit_value (self->reader[2]) << 12) |
                                     (g_ascii_xdigit_value (self->reader[3]) <<  8) |
                                     (g_ascii_xdigit_value (self->reader[4]) <<  4) |
                                     (g_ascii_xdigit_value (self->reader[5]));

                  /* resolve UTF-16 surrogates for Unicode characters not in the BMP,
                   * as per ECMA 404, § 9, "String"
                   */
                  if (g_unichar_type (unichar) == G_UNICODE_SURROGATE)
                    {
                      if (gtk_json_parser_remaining (self) >= 12 &&
                          self->reader[6] == '\\' &&
                          self->reader[7] == 'u' &&
                          g_ascii_isxdigit (self->reader[8]) &&
                          g_ascii_isxdigit (self->reader[9]) &&
                          g_ascii_isxdigit (self->reader[10]) &&
                          g_ascii_isxdigit (self->reader[11]))
                        {
                          unichar = decode_utf16_surrogate_pair (unichar,
                                                                 (g_ascii_xdigit_value (self->reader[8]) << 12) |
                                                                 (g_ascii_xdigit_value (self->reader[9]) <<  8) |
                                                                 (g_ascii_xdigit_value (self->reader[10]) <<  4) |
                                                                 (g_ascii_xdigit_value (self->reader[11])));
                          escape_size += 6;
                        }
                      else
                        {
                          unichar = 0;
                        }

                      if (unichar == 0)
                        {
                          gtk_json_parser_syntax_error_at (self, self->reader, self->reader + escape_size, "Invalid UTF-16 surrogate pair");
                          return FALSE;
                        }

                      self->reader += escape_size - 2;
                    }
                }
              break;
            default:
              if (g_utf8_get_char_validated ((const char *) self->reader + 1, self->end - self->reader - 1) < (gunichar) -2)
                gtk_json_parser_syntax_error_at (self, self->reader, (const guchar *) g_utf8_next_char (self->reader + 1), "Unknown escape sequence");
              else
                gtk_json_parser_syntax_error_at (self, self->reader, self->reader + 1, "Unknown escape sequence");
              return FALSE;
            }
          self->reader += 2;
        }

      self->reader = json_skip_characters (self->reader, self->end, STRING_ELEMENT);
    }

end:
  gtk_json_parser_syntax_error_at (self, start, self->reader, "Unterminated string literal");
  return FALSE;
}

static gboolean
gtk_json_parser_parse_number (GtkJsonParser *self)
{
  const guchar *start = self->reader;
  gboolean have_sign;

  /* sign */
  have_sign = gtk_json_parser_try_char (self, '-');

  /* integer part */
  if (gtk_json_parser_try_char (self, '0'))
    {
      /* Technically, "01" in the JSON grammar would be 2 numbers:
       * "0" followed by "1".
       * Practically, nobody understands that it's 2 numbers, so we
       * special-purpose an error message for it, because 2 numbers
       * can never follow each other.
       */
      if (!gtk_json_parser_is_eof (self) &&
          g_ascii_isdigit (*self->reader))
        {
          do
            {
              self->reader++;
            }
          while (!gtk_json_parser_is_eof (self) &&
                 g_ascii_isdigit (*self->reader));
          
          gtk_json_parser_syntax_error_at (self, start, self->reader, "Numbers may not start with leading 0s");
          return FALSE;
        }
    }
  else
    {
      if (gtk_json_parser_is_eof (self) ||
          !g_ascii_isdigit (*self->reader))
        {
          if (have_sign)
            gtk_json_parser_syntax_error_at (self, start, self->reader, "Expected a number after '-' character");
          else
            gtk_json_parser_type_error (self, "Not a number");
          return FALSE;
        }

      self->reader++;

      while (!gtk_json_parser_is_eof (self) && g_ascii_isdigit (*self->reader))
        self->reader++;
    }

  /* fractional part */
  if (gtk_json_parser_try_char (self, '.'))
    {
      if (!g_ascii_isdigit (*self->reader))
        {
          gtk_json_parser_syntax_error_at (self, start, self->reader, "Expected a digit after '.'");
          return FALSE;
        }

      do
        {
          self->reader++;
        }
      while (!gtk_json_parser_is_eof (self) && g_ascii_isdigit (*self->reader));
    }

  /* exponent */
  if (gtk_json_parser_try_char (self, 'e') ||
      gtk_json_parser_try_char (self, 'E'))
    {
      if (!gtk_json_parser_try_char (self, '-'))
        gtk_json_parser_try_char (self, '+');

      if (!g_ascii_isdigit (*self->reader))
        {
          gtk_json_parser_syntax_error_at (self, start, self->reader, "Expected a digit in exponent");
          return FALSE;
        }

      do
        {
          self->reader++;
        }
      while (!gtk_json_parser_is_eof (self) && g_ascii_isdigit (*self->reader));
    }
  return TRUE;
}

static gboolean
gtk_json_parser_parse_value (GtkJsonParser *self)
{
  if (gtk_json_parser_is_eof (self))
    {
      gtk_json_parser_syntax_error (self, "Unexpected end of document");
      return FALSE;
    }

  switch (json_character_table[*self->block->value] & JSON_CHARACTER_NODE_MASK)
  {
    case GTK_JSON_STRING:
      return gtk_json_parser_parse_string (self);
    
    case GTK_JSON_NUMBER:
      return gtk_json_parser_parse_number (self);

    case GTK_JSON_NULL:
      if (gtk_json_parser_try_identifier (self, "null"))
        return TRUE;
      break;

    case GTK_JSON_BOOLEAN:
      if (gtk_json_parser_try_identifier (self, "true") ||
          gtk_json_parser_try_identifier (self, "false"))
        return TRUE;
      break;

    case GTK_JSON_OBJECT:
    case GTK_JSON_ARRAY:
      /* don't preparse objects */
      return TRUE;

    default:
      break;
  }

  if (gtk_json_parser_remaining (self) >= 2 &&
      (self->block->value[0] == '.' || self->block->value[0] == '+') &&
      g_ascii_isdigit (self->block->value[1]))
    {
      const guchar *end = self->block->value + 2;
      while (end < self->end && g_ascii_isalnum (*end))
        end++;
      gtk_json_parser_syntax_error_at (self, self->block->value, end, "Numbers may not start with '%c'", *self->block->value);
    }
  else if (*self->reader == 0)
    gtk_json_parser_syntax_error (self, "Unexpected nul byte in document");
  else
    gtk_json_parser_syntax_error (self, "Expected a value");
  return FALSE;
}

static void
gtk_json_parser_push_block (GtkJsonParser    *self,
                            GtkJsonBlockType  type)
{
  self->block++;
  if (self->block == self->blocks_end)
    {
      gsize old_size = self->blocks_end - self->blocks;
      gsize new_size = old_size + 128;

      if (self->blocks == self->blocks_preallocated)
        {
          self->blocks = g_new (GtkJsonBlock, new_size);
          memcpy (self->blocks, self->blocks_preallocated, sizeof (GtkJsonBlock) * G_N_ELEMENTS (self->blocks_preallocated));
        }
      else
        {
          self->blocks = g_renew (GtkJsonBlock, self->blocks, new_size);
        }
      self->blocks_end = self->blocks + new_size;
      self->block = self->blocks + old_size;
    }

  self->block->type = type;
  self->block->member_name = 0;
  self->block->value = 0;
  self->block->index = 0;
}
                     
static void
gtk_json_parser_pop_block (GtkJsonParser *self)
{
  g_assert (self->block > self->blocks);
  self->block--;
}

GtkJsonParser *
gtk_json_parser_new_for_string (const char *string,
                                gssize      size)
{
  GtkJsonParser *self;
  GBytes *bytes;

  bytes = g_bytes_new (string, size >= 0 ? size : strlen (string));

  self = gtk_json_parser_new_for_bytes (bytes);

  g_bytes_unref (bytes);

  return self;
}

GtkJsonParser *
gtk_json_parser_new_for_bytes (GBytes *bytes)
{
  GtkJsonParser *self;
  gsize size;

  g_return_val_if_fail (bytes != NULL, NULL);

  self = g_slice_new0 (GtkJsonParser);

  self->bytes = g_bytes_ref (bytes);
  self->reader = g_bytes_get_data (bytes, &size);
  self->end = self->reader + size;

  self->blocks = self->blocks_preallocated;
  self->blocks_end = self->blocks + G_N_ELEMENTS (self->blocks_preallocated);
  self->block = self->blocks;
  self->block->type = GTK_JSON_BLOCK_TOPLEVEL;

  gtk_json_parser_skip_bom (self);
  self->start = self->reader;
  gtk_json_parser_rewind (self);

  return self;
}

void
gtk_json_parser_free (GtkJsonParser *self)
{
  if (self == NULL)
    return;

  g_bytes_unref (self->bytes);

  if (self->blocks != self->blocks_preallocated)
    g_free (self->blocks);

  if (self->error)
    g_error_free (self->error);

  g_slice_free (GtkJsonParser, self);
}

static gboolean
gtk_json_parser_skip_block (GtkJsonParser *self)
{
  gsize depth;

  if (self->reader != self->block->value)
    return TRUE;

  depth = gtk_json_parser_get_depth (self);
  while (TRUE)
    {
      if (*self->reader == '{')
        {
          if (!gtk_json_parser_start_object (self))
            return FALSE;
        }
      else if (*self->reader == '[')
        {
          if (!gtk_json_parser_start_array (self))
            return FALSE;
        }
    
      while (self->reader != self->block->value)
        {
          /* This should never be reentrant to this function or we might
           * loop causing stack overflow */
          if (!gtk_json_parser_next (self))
            {
              if (!gtk_json_parser_end (self))
                return FALSE;
              if (depth >= gtk_json_parser_get_depth (self))
                return TRUE;
            }
        }
    }

  return TRUE;
}

gboolean
gtk_json_parser_next (GtkJsonParser *self)
{
  if (self->error)
    return FALSE;

  if (self->block->value == NULL)
    return FALSE;

  if (!gtk_json_parser_skip_block (self))
    {
      g_assert (self->error);
      return FALSE;
    }

  switch (self->block->type)
    {
    case GTK_JSON_BLOCK_TOPLEVEL:
      gtk_json_parser_skip_whitespace (self);
      if (gtk_json_parser_is_eof (self))
        {
          self->block->value = NULL;
        }
      else if (*self->reader == 0)
        {
          gtk_json_parser_syntax_error (self, "Unexpected nul byte in document");
        }
      else
        {
          gtk_json_parser_syntax_error_at (self, self->reader, self->end, "Data at end of document");
        }
      return FALSE;

    case GTK_JSON_BLOCK_OBJECT:
      gtk_json_parser_skip_whitespace (self);
      if (gtk_json_parser_is_eof (self))
        {
          gtk_json_parser_syntax_error_at (self,
                                           self->block[-1].value,
                                           self->reader,
                                           "Unterminated object");
          self->block->member_name = NULL;
          self->block->value = NULL;
        }
      if (gtk_json_parser_has_char (self, '}'))
        {
          self->block->member_name = NULL;
          self->block->value = NULL;
          return FALSE;
        }
      if (!gtk_json_parser_try_char (self, ','))
        {
          gtk_json_parser_syntax_error (self, "Expected a ',' to separate object members");
          return FALSE;
        }
      gtk_json_parser_skip_whitespace (self);
      if (!gtk_json_parser_has_char (self, '"'))
        {
          gtk_json_parser_syntax_error (self, "Expected a string for object member name");
          return FALSE;
        }
      self->block->member_name = self->reader;

      if (!gtk_json_parser_parse_string (self))
        return FALSE;
      gtk_json_parser_skip_whitespace (self);
      if (!gtk_json_parser_try_char (self, ':'))
        {
          gtk_json_parser_syntax_error (self, "Missing ':' after member name");
          return FALSE;
        }

      gtk_json_parser_skip_whitespace (self);
      self->block->value = self->reader;
      if (!gtk_json_parser_parse_value (self))
        return FALSE;
      break;

    case GTK_JSON_BLOCK_ARRAY:
      gtk_json_parser_skip_whitespace (self);
      if (gtk_json_parser_is_eof (self))
        {
          gtk_json_parser_syntax_error_at (self,
                                           self->block[-1].value,
                                           self->reader,
                                           "Unterminated array");
          self->block->member_name = NULL;
          self->block->value = NULL;
        }
      if (gtk_json_parser_has_char (self, ']'))
        {
          self->block->value = NULL;
          return FALSE;
        }

      if (!gtk_json_parser_try_char (self, ','))
        {
          gtk_json_parser_syntax_error (self, "Expected a ',' to separate array members");
          return FALSE;
        }

      gtk_json_parser_skip_whitespace (self);
      self->block->value = self->reader;
      if (!gtk_json_parser_parse_value (self))
        return FALSE;
      break;

    default:
      g_assert_not_reached ();
      break;
    }

  return TRUE;
}

void
gtk_json_parser_rewind (GtkJsonParser *self)
{
  if (self->error)
    return;

  switch (self->block->type)
    {
    case GTK_JSON_BLOCK_OBJECT:
      gtk_json_parser_pop_block (self);
      self->reader = self->block->value;
      gtk_json_parser_start_object (self);
      break;

    case GTK_JSON_BLOCK_ARRAY:
      gtk_json_parser_pop_block (self);
      self->reader = self->block->value;
      gtk_json_parser_start_array (self);
      break;

    case GTK_JSON_BLOCK_TOPLEVEL:
      self->reader = self->start;
      gtk_json_parser_skip_whitespace (self);
      if (gtk_json_parser_is_eof (self))
        {
          gtk_json_parser_syntax_error_at (self, self->start, self->reader, "Empty document");
        }
      else
        {
          self->block->value = self->reader;
          gtk_json_parser_parse_value (self);
        }
      break;

    default:
      g_assert_not_reached ();
      return;
    }
}

gsize
gtk_json_parser_get_depth (GtkJsonParser *self)
{
  return self->block - self->blocks;
}

GtkJsonNode
gtk_json_parser_get_node (GtkJsonParser *self)
{
  if (self->error)
    return GTK_JSON_NONE;

  if (self->block->value == NULL)
    return GTK_JSON_NONE;

  return (json_character_table[*self->block->value] & JSON_CHARACTER_NODE_MASK);
}

const GError *
gtk_json_parser_get_error (GtkJsonParser *self)
{
  return self->error;
}

void
gtk_json_parser_get_error_offset (GtkJsonParser *self,
                                  gsize         *start,
                                  gsize         *end)
{
  const guchar *data;

  if (self->error == NULL)
    {
      if (start)
        *start = 0;
      if (end)
        *end = 0;
      return;
    }

  data = g_bytes_get_data (self->bytes, NULL);
  if (start)
    *start = self->error_start - data;
  if (end)
    *end = self->error_end - data;
}

void
gtk_json_parser_get_error_location (GtkJsonParser *self,
                                    gsize         *start_line,
                                    gsize         *start_line_bytes,
                                    gsize         *end_line,
                                    gsize         *end_line_bytes)
{
  const guchar *s, *line_start;
  gsize lines;

  if (self->error == NULL)
    {
      if (start_line)
        *start_line = 0;
      if (start_line_bytes)
        *start_line_bytes = 0;
      if (end_line)
        *end_line = 0;
      if (end_line_bytes)
        *end_line_bytes = 0;
      return;
    }

  line_start = self->start;
  lines = 0;

  for (s = json_skip_characters_until (line_start, self->error_start, NEWLINE);
       s < self->error_start;
       s = json_skip_characters_until  (line_start, self->error_start, NEWLINE))
    {
      if (s[0] == '\r' && s + 1 < self->error_start && s[1] == '\n')
        s++;
      lines++;
      line_start = s + 1;
    }

  if (start_line)
    *start_line = lines;
  if (start_line_bytes)
    *start_line_bytes = s - line_start;

  if (end_line == NULL && end_line_bytes == NULL)
    return;

  for (s = json_skip_characters_until (s, self->error_end, NEWLINE);
       s < self->error_end;
       s = json_skip_characters_until (line_start, self->error_end, NEWLINE))
    {
      if (s[0] == '\r' && s + 1 < self->error_start && s[1] == '\n')
        s++;
      lines++;
      line_start = s + 1;
    }

  if (end_line)
    *end_line = lines;
  if (end_line_bytes)
    *end_line_bytes = s - line_start;
}

static gboolean
gtk_json_parser_supports_member (GtkJsonParser *self)
{
  if (self->error)
    return FALSE;

  if (self->block->type != GTK_JSON_BLOCK_OBJECT)
    return FALSE;

  if (self->block->member_name == NULL)
    return FALSE;

  return TRUE;
}

char *
gtk_json_parser_get_member_name (GtkJsonParser *self)
{
  if (!gtk_json_parser_supports_member (self))
    return NULL;

  return gtk_json_unescape_string (self->block->member_name);
}

gboolean
gtk_json_parser_has_member (GtkJsonParser *self,
                            const char    *name)
{
  JsonStringIter iter;
  gsize found, len;

  if (!gtk_json_parser_supports_member (self))
    return FALSE;

  found = 0;

  for (len = json_string_iter_init (&iter, self->block->member_name);
       len > 0;
       len = json_string_iter_next (&iter))
    {
      const char *s = json_string_iter_get (&iter);

      if (strncmp (name + found, s, len) != 0)
        return FALSE;

      found += len;
    }

  return TRUE;
}

gboolean
gtk_json_parser_find_member (GtkJsonParser *self,
                             const char    *name)
{
  if (!gtk_json_parser_supports_member (self))
    {
      while (gtk_json_parser_next (self));
      return FALSE;
    }

  gtk_json_parser_rewind (self);

  do
    {
      if (gtk_json_parser_has_member (self, name))
        return TRUE;
    }
  while (gtk_json_parser_next (self));

  return FALSE;
}

static gssize
json_string_iter_run_select (const guchar       *string_data,
                             const char * const *options)
{
  JsonStringIter iter;
  gssize i, j;
  gsize found, len;

  if (options == NULL || options[0] == NULL)
    return -1;

  found = 0;
  i = 0;

  for (len = json_string_iter_init (&iter, string_data);
       len > 0;
       len = json_string_iter_next (&iter))
    {
      const char *s = json_string_iter_get (&iter);

      if (strncmp (options[i] + found, s, len) != 0)
        {
          for (j = i + 1; options[j]; j++)
            {
              if (strncmp (options[j], options[i], found) == 0 &&
                  strncmp (options[j] + found, s, len) == 0)
                {
                  i = j;
                  break;
                }
            }
          if (j != i)
            return -1;
        }
      found += len;
    }

  if (options[i][found] == 0)
    return i;

  for (j = i + 1; options[j]; i++)
    {
      if (strncmp (options[j], options[i], found) != 0)
        continue;
      if (options[j][found] == 0)
        return j;
    }

  return -1;
}
                     
gssize
gtk_json_parser_select_member (GtkJsonParser      *self,
                               const char * const *options)
{
  if (!gtk_json_parser_supports_member (self))
    return -1;

  return json_string_iter_run_select (self->block->member_name, options);
}

gboolean
gtk_json_parser_get_boolean (GtkJsonParser *self)
{
  if (self->error)
    return FALSE;

  if (self->block->value == NULL)
    return FALSE;

  if (*self->block->value == 't')
    return TRUE;
  else if (*self->block->value == 'f')
    return FALSE;

  gtk_json_parser_type_error (self, "Expected a boolean value");
  return FALSE;
}

double
gtk_json_parser_get_number (GtkJsonParser *self)
{
  double result;

  if (self->error)
    return 0;

  if (self->block->value == NULL)
    return 0;

  if (!strchr ("-0123456789", *self->block->value))
    {
      gtk_json_parser_type_error (self, "Expected a number");
      return 0;
    }

  errno = 0;
  result = g_ascii_strtod ((const char *) self->block->value, NULL);

  if (errno)
    {
      if (errno == ERANGE)
        gtk_json_parser_value_error (self, "Number out of range");
      else
        gtk_json_parser_value_error (self, "%s", g_strerror (errno));

      return 0;
    }

  return result;
}

int
gtk_json_parser_get_int (GtkJsonParser *self)
{
  long result;
  char *end;

  if (self->error)
    return 0;

  if (self->block->value == NULL)
    return 0;

  if (!strchr ("-0123456789", *self->block->value))
    {
      gtk_json_parser_type_error (self, "Expected an intereger");
      return 0;
    }

  errno = 0;
  result = strtol ((const char *) self->block->value, &end, 10);
  if (*end == '.' || *end == 'e' || *end == 'E')
    {
      gtk_json_parser_type_error (self, "Expected an intereger");
      return 0;
    }

  if (errno)
    {
      if (errno == ERANGE)
        gtk_json_parser_value_error (self, "Number out of integer range");
      else
        gtk_json_parser_value_error (self, "%s", g_strerror (errno));

      return 0;
    }
  else if (result > G_MAXINT || result < G_MININT)
    {
      gtk_json_parser_value_error (self, "Number out of integer range");
      return 0;
    }

  return result;
}

guint
gtk_json_parser_get_uint (GtkJsonParser *self)
{
  gulong result;
  char *end;

  if (self->error)
    return 0;

  if (self->block->value == NULL)
    return 0;

  if (!strchr ("0123456789", *self->block->value))
    {
      gtk_json_parser_type_error (self, "Expected an unsigned intereger");
      return 0;
    }

  errno = 0;
  result = strtoul ((const char *) self->block->value, &end, 10);
  if (*end == '.' || *end == 'e' || *end == 'E')
    {
      gtk_json_parser_type_error (self, "Expected an unsigned intereger");
      return 0;
    }

  if (errno)
    {
      if (errno == ERANGE)
        gtk_json_parser_value_error (self, "Number out of unsignedinteger range");
      else
        gtk_json_parser_value_error (self, "%s", g_strerror (errno));

      return 0;
    }
  else if (result > G_MAXUINT)
    {
      gtk_json_parser_value_error (self, "Number out of unsigned integer range");
      return 0;
    }

  return result;
}

char *
gtk_json_parser_get_string (GtkJsonParser *self)
{
  if (self->error)
    return g_strdup ("");

  if (self->block->value == NULL)
    return g_strdup ("");

  if (*self->block->value != '"')
    {
      gtk_json_parser_type_error (self, "Expected a string");
      return g_strdup ("");
    }

  return gtk_json_unescape_string (self->block->value);
}

gssize
gtk_json_parser_select_string (GtkJsonParser      *self,
                               const char * const *options)
{
  if (self->error)
    return -1;

  if (self->block->value == NULL)
    return -1;

  if (*self->block->value != '"')
    {
      gtk_json_parser_type_error (self, "Expected a string");
      return -1;
    }

  return json_string_iter_run_select (self->block->value, options);
}

gboolean
gtk_json_parser_start_object (GtkJsonParser *self)
{
  if (self->error)
    return FALSE;

  if (!gtk_json_parser_try_char (self, '{'))
    {
      gtk_json_parser_type_error (self, "Expected an object");
      return FALSE;
    }

  gtk_json_parser_push_block (self, GTK_JSON_BLOCK_OBJECT);

  gtk_json_parser_skip_whitespace (self);
  if (gtk_json_parser_is_eof (self))
    {
      gtk_json_parser_syntax_error_at (self,
                                       self->block[-1].value,
                                       self->reader,
                                       "Unterminated object");
      return FALSE;
    }
  if (gtk_json_parser_has_char (self, '}'))
    return TRUE;

  if (!gtk_json_parser_has_char (self, '"'))
    {
      gtk_json_parser_syntax_error (self, "Expected a string for object member name");
      return FALSE;
    }
  self->block->member_name = self->reader;

  if (!gtk_json_parser_parse_string (self))
    return FALSE;
  gtk_json_parser_skip_whitespace (self);
  if (!gtk_json_parser_try_char (self, ':'))
    {
      gtk_json_parser_syntax_error (self, "Missing ':' after member name");
      return FALSE;
    }

  gtk_json_parser_skip_whitespace (self);
  self->block->value = self->reader;
  if (!gtk_json_parser_parse_value (self))
    return FALSE;

  return TRUE;
}

gboolean
gtk_json_parser_start_array (GtkJsonParser *self)
{
  if (self->error)
    return FALSE;

  if (!gtk_json_parser_try_char (self, '['))
    {
      gtk_json_parser_type_error (self, "Expected an array");
      return FALSE;
    }

  gtk_json_parser_push_block (self, GTK_JSON_BLOCK_ARRAY);
  gtk_json_parser_skip_whitespace (self);
  if (gtk_json_parser_is_eof (self))
    {
      gtk_json_parser_syntax_error_at (self,
                                       self->block[-1].value,
                                       self->reader,
                                       "Unterminated array");
      return FALSE;
    }
  if (gtk_json_parser_has_char (self, ']'))
    {
      self->block->value = NULL;
      return TRUE;
    }
  self->block->value = self->reader;
  if (!gtk_json_parser_parse_value (self))
    return FALSE;

  return TRUE;
}

gboolean
gtk_json_parser_end (GtkJsonParser *self)
{
  char bracket;

  g_return_val_if_fail (self != NULL, FALSE);

  while (gtk_json_parser_next (self));

  if (self->error)
    return FALSE;

  switch (self->block->type)
    {
    case GTK_JSON_BLOCK_OBJECT:
      bracket = '}';
      break;
    case GTK_JSON_BLOCK_ARRAY:
      bracket = ']';
      break;
    case GTK_JSON_BLOCK_TOPLEVEL:
    default:
      g_return_val_if_reached (FALSE);
    }

  if (!gtk_json_parser_try_char (self, bracket))
    {
      gtk_json_parser_syntax_error (self, "No terminating '%c'", bracket);
      return FALSE;
    }

  gtk_json_parser_pop_block (self);

  return TRUE;
}

