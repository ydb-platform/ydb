/* Pango
 * pango-utils.c: Utilities for internal functions and modules
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
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <locale.h>

#include "pango-font.h"
#include "pango-features.h"
#include "pango-impl-utils.h"
#include "pango-utils-internal.h"
#include "pango-utils-private.h"

#include <glib/gstdio.h>

#ifdef G_OS_WIN32

#define flockfile(f) (void)1
#define funlockfile(f) (void)1
#define getc_unlocked(f) getc(f)

#include <sys/types.h>

#define STRICT
#include <windows.h>

#endif

/**
 * pango_version:
 *
 * Returns the encoded version of Pango available at run-time.
 *
 * This is similar to the macro %PANGO_VERSION except that the macro
 * returns the encoded version available at compile-time. A version
 * number can be encoded into an integer using PANGO_VERSION_ENCODE().
 *
 * Returns: The encoded version of Pango library available at run time.
 *
 * Since: 1.16
 */
int
pango_version (void)
{
  return PANGO_VERSION;
}

/**
 * pango_version_string:
 *
 * Returns the version of Pango available at run-time.
 *
 * This is similar to the macro %PANGO_VERSION_STRING except that the
 * macro returns the version available at compile-time.
 *
 * Returns: (transfer none): A string containing the version of Pango library available
 *   at run time. The returned string is owned by Pango and should not
 *   be modified or freed.
 *
 * Since: 1.16
 */
const char *
pango_version_string (void)
{
  return PANGO_VERSION_STRING;
}

/**
 * pango_version_check:
 * @required_major: the required major version
 * @required_minor: the required minor version
 * @required_micro: the required major version
 *
 * Checks that the Pango library in use is compatible with the
 * given version.
 *
 * Generally you would pass in the constants %PANGO_VERSION_MAJOR,
 * %PANGO_VERSION_MINOR, %PANGO_VERSION_MICRO as the three arguments
 * to this function; that produces a check that the library in use at
 * run-time is compatible with the version of Pango the application or
 * module was compiled against.
 *
 * Compatibility is defined by two things: first the version
 * of the running library is newer than the version
 * @required_major.required_minor.@required_micro. Second
 * the running library must be binary compatible with the
 * version @required_major.required_minor.@required_micro
 * (same major version.)
 *
 * For compile-time version checking use PANGO_VERSION_CHECK().
 *
 * Returns: (transfer none) (nullable): %NULL if the Pango library is compatible
 *   with the given version, or a string describing the version
 *   mismatch.  The returned string is owned by Pango and should not
 *   be modified or freed.
 *
 * Since: 1.16
 */
const gchar*
pango_version_check (int required_major,
		     int required_minor,
		     int required_micro)
{
  gint pango_effective_micro = 100 * PANGO_VERSION_MINOR + PANGO_VERSION_MICRO;
  gint required_effective_micro = 100 * required_minor + required_micro;

  if (required_major > PANGO_VERSION_MAJOR)
    return "Pango version too old (major mismatch)";
  if (required_major < PANGO_VERSION_MAJOR)
    return "Pango version too new (major mismatch)";
  if (required_effective_micro < pango_effective_micro - PANGO_BINARY_AGE)
    return "Pango version too new (micro mismatch)";
  if (required_effective_micro > pango_effective_micro)
    return "Pango version too old (micro mismatch)";
  return NULL;
}

/**
 * pango_trim_string:
 * @str: a string
 *
 * Trims leading and trailing whitespace from a string.
 *
 * Returns: (transfer full): A newly-allocated string that must be freed with g_free()
 *
 * Deprecated: 1.38
 */
char *
pango_trim_string (const char *str)
{
  return _pango_trim_string (str);
}

char *
_pango_trim_string (const char *str)
{
  int len;

  g_return_val_if_fail (str != NULL, NULL);

  while (*str && g_ascii_isspace (*str))
    str++;

  len = strlen (str);
  while (len > 0 && g_ascii_isspace (str[len-1]))
    len--;

  return g_strndup (str, len);
}

/**
 * pango_split_file_list:
 * @str: a %G_SEARCHPATH_SEPARATOR separated list of filenames
 *
 * Splits a %G_SEARCHPATH_SEPARATOR-separated list of files, stripping
 * white space and substituting ~/ with $HOME/.
 *
 * Returns: (transfer full) (array zero-terminated=1): a list of
 *   strings to be freed with g_strfreev()
 *
 * Deprecated: 1.38
 */
char **
pango_split_file_list (const char *str)
{
  int i = 0;
  int j;
  char **files;

  files = g_strsplit (str, G_SEARCHPATH_SEPARATOR_S, -1);

  while (files[i])
    {
      char *file = _pango_trim_string (files[i]);

      /* If the resulting file is empty, skip it */
      if (file[0] == '\0')
	{
	  g_free(file);
	  g_free (files[i]);

	  for (j = i + 1; files[j]; j++)
	    files[j - 1] = files[j];

	  files[j - 1] = NULL;

	  continue;
	}
#ifndef G_OS_WIN32
      /* '~' is a quite normal and common character in file names on
       * Windows, especially in the 8.3 versions of long file names, which
       * still occur now and then. Also, few Windows user are aware of the
       * Unix shell convention that '~' stands for the home directory,
       * even if they happen to have a home directory.
       */
      if (file[0] == '~' && file[1] == G_DIR_SEPARATOR)
	{
	  char *tmp = g_strconcat (g_get_home_dir(), file + 1, NULL);
	  g_free (file);
	  file = tmp;
	}
      else if (file[0] == '~' && file[1] == '\0')
	{
	  g_free (file);
	  file = g_strdup (g_get_home_dir());
	}
#endif
      g_free (files[i]);
      files[i] = file;

      i++;
    }

  return files;
}

/**
 * pango_read_line:
 * @stream: a stdio stream
 * @str: `GString` buffer into which to write the result
 *
 * Reads an entire line from a file into a buffer.
 *
 * Lines may be delimited with '\n', '\r', '\n\r', or '\r\n'. The delimiter
 * is not written into the buffer. Text after a '#' character is treated as
 * a comment and skipped. '\' can be used to escape a # character.
 * '\' proceeding a line delimiter combines adjacent lines. A '\' proceeding
 * any other character is ignored and written into the output buffer
 * unmodified.
 *
 * Returns: 0 if the stream was already at an %EOF character,
 *   otherwise the number of lines read (this is useful for maintaining
 *   a line number counter which doesn't combine lines with '\')
 *
 * Deprecated: 1.38
 */
gint
pango_read_line (FILE *stream, GString *str)
{
  gboolean quoted = FALSE;
  gboolean comment = FALSE;
  int n_read = 0;
  int lines = 1;

  flockfile (stream);

  g_string_truncate (str, 0);

  while (1)
    {
      int c;

      c = getc_unlocked (stream);

      if (c == EOF)
	{
	  if (quoted)
	    g_string_append_c (str, '\\');

	  goto done;
	}
      else
	n_read++;

      if (quoted)
	{
	  quoted = FALSE;

	  switch (c)
	    {
	    case '#':
	      g_string_append_c (str, '#');
	      break;
	    case '\r':
	    case '\n':
	      {
		int next_c = getc_unlocked (stream);

		if (!(next_c == EOF ||
		      (c == '\r' && next_c == '\n') ||
		      (c == '\n' && next_c == '\r')))
		  ungetc (next_c, stream);

		lines++;

		break;
	      }
	    default:
	      g_string_append_c (str, '\\');
	      g_string_append_c (str, c);
	    }
	}
      else
	{
	  switch (c)
	    {
	    case '#':
	      comment = TRUE;
	      break;
	    case '\\':
	      if (!comment)
		quoted = TRUE;
	      break;
	    case '\r':
	    case '\n':
	      {
		int next_c = getc_unlocked (stream);

		if (!(next_c == EOF ||
		      (c == '\r' && next_c == '\n') ||
		      (c == '\n' && next_c == '\r')))
		  ungetc (next_c, stream);

		goto done;
	      }
	    default:
	      if (!comment)
		g_string_append_c (str, c);
	    }
	}
    }

 done:

  funlockfile (stream);

  return (n_read > 0) ? lines : 0;
}

/**
 * pango_skip_space:
 * @pos: (inout): in/out string position
 *
 * Skips 0 or more characters of white space.
 *
 * Returns: %FALSE if skipping the white space leaves
 *   the position at a '\0' character.
 *
 * Deprecated: 1.38
 */
gboolean
pango_skip_space (const char **pos)
{
  const char *p = *pos;

  while (g_ascii_isspace (*p))
    p++;

  *pos = p;

  return !(*p == '\0');
}

/**
 * pango_scan_word:
 * @pos: (inout): in/out string position
 * @out: a `GString` into which to write the result
 *
 * Scans a word into a `GString` buffer.
 *
 * A word consists of [A-Za-z_] followed by zero or more
 * [A-Za-z_0-9]. Leading white space is skipped.
 *
 * Returns: %FALSE if a parse error occurred
 *
 * Deprecated: 1.38
 */
gboolean
pango_scan_word (const char **pos, GString *out)
{
  const char *p = *pos;

  while (g_ascii_isspace (*p))
    p++;

  if (!((*p >= 'A' && *p <= 'Z') ||
	(*p >= 'a' && *p <= 'z') ||
	*p == '_'))
    return FALSE;

  g_string_truncate (out, 0);
  g_string_append_c (out, *p);
  p++;

  while ((*p >= 'A' && *p <= 'Z') ||
	 (*p >= 'a' && *p <= 'z') ||
	 (*p >= '0' && *p <= '9') ||
	 *p == '_')
    {
      g_string_append_c (out, *p);
      p++;
    }

  *pos = p;

  return TRUE;
}

/**
 * pango_scan_string:
 * @pos: (inout): in/out string position
 * @out: a `GString` into which to write the result
 *
 * Scans a string into a `GString` buffer.
 *
 * The string may either be a sequence of non-white-space characters,
 * or a quoted string with '"'. Instead a quoted string, '\"' represents
 * a literal quote. Leading white space outside of quotes is skipped.
 *
 * Returns: %FALSE if a parse error occurred
 *
 * Deprecated: 1.38
 */
gboolean
pango_scan_string (const char **pos, GString *out)
{
  const char *p = *pos;

  while (g_ascii_isspace (*p))
    p++;

  if (G_UNLIKELY (!*p))
    return FALSE;
  else if (*p == '"')
    {
      gboolean quoted = FALSE;
      g_string_truncate (out, 0);

      p++;

      while (TRUE)
	{
	  if (quoted)
	    {
	      int c = *p;

	      switch (c)
		{
		case '\0':
		  return FALSE;
		case 'n':
		  c = '\n';
		  break;
		case 't':
		  c = '\t';
		  break;
		default:
		  break;
		}

	      quoted = FALSE;
	      g_string_append_c (out, c);
	    }
	  else
	    {
	      switch (*p)
		{
		case '\0':
		  return FALSE;
		case '\\':
		  quoted = TRUE;
		  break;
		case '"':
		  p++;
		  goto done;
		default:
		  g_string_append_c (out, *p);
		  break;
		}
	    }
	  p++;
	}
    done:
      ;
    }
  else
    {
      g_string_truncate (out, 0);

      while (*p && !g_ascii_isspace (*p))
	{
	  g_string_append_c (out, *p);
	  p++;
	}
    }

  *pos = p;

  return TRUE;
}

/**
 * pango_scan_int:
 * @pos: (inout): in/out string position
 * @out: (out): an int into which to write the result
 *
 * Scans an integer.
 *
 * Leading white space is skipped.
 *
 * Returns: %FALSE if a parse error occurred
 *
 * Deprecated: 1.38
 */
gboolean
pango_scan_int (const char **pos, int *out)
{
  return _pango_scan_int (pos, out);
}

gboolean
_pango_scan_int (const char **pos, int *out)
{
  char *end;
  long temp;

  errno = 0;
  temp = strtol (*pos, &end, 10);
  if (errno == ERANGE)
    {
      errno = 0;
      return FALSE;
    }

  *out = (int)temp;
  if ((long)(*out) != temp)
    {
      return FALSE;
    }

  *pos = end;

  return TRUE;
}

/**
 * pango_config_key_get_system:
 * @key: Key to look up, in the form "SECTION/KEY"
 *
 * Do not use.  Does not do anything.
 *
 * Returns: (nullable): %NULL
 *
 * Deprecated: 1.38
 */
char *
pango_config_key_get_system (const char *key)
{
  return NULL;
}

/**
 * pango_config_key_get:
 * @key: Key to look up, in the form "SECTION/KEY"
 *
 * Do not use.  Does not do anything.
 *
 * Returns: (nullable): %NULL
 *
 * Deprecated: 1.38
 */
char *
pango_config_key_get (const char *key)
{
  return NULL;
}

/**
 * pango_get_sysconf_subdirectory:
 *
 * Returns the name of the "pango" subdirectory of SYSCONFDIR
 * (which is set at compile time).
 *
 * Returns: (transfer none): the Pango sysconf directory. The returned string should
 * not be freed.
 *
 * Deprecated: 1.38
 */
const char *
pango_get_sysconf_subdirectory (void)
{
  static const gchar *result = NULL; /* MT-safe */

  if (g_once_init_enter (&result))
    {
      const char *tmp_result = NULL;
      const char *sysconfdir = g_getenv ("PANGO_SYSCONFDIR");
      if (sysconfdir != NULL)
	tmp_result = g_build_filename (sysconfdir, "pango", NULL);
      else
	tmp_result = SYSCONFDIR "/pango";
      g_once_init_leave(&result, tmp_result);
    }
  return result;
}

/**
 * pango_get_lib_subdirectory:
 *
 * Returns the name of the "pango" subdirectory of LIBDIR
 * (which is set at compile time).
 *
 * Returns: (transfer none): the Pango lib directory. The returned string should
 * not be freed.
 *
 * Deprecated: 1.38
 */
const char *
pango_get_lib_subdirectory (void)
{
  static const gchar *result = NULL; /* MT-safe */

  if (g_once_init_enter (&result))
    {
      const gchar *tmp_result = NULL;
      const char *libdir = g_getenv ("PANGO_LIBDIR");
      if (libdir != NULL)
	tmp_result = g_build_filename (libdir, "pango", NULL);
      else
	tmp_result = LIBDIR "/pango";
      g_once_init_leave(&result, tmp_result);
    }
  return result;
}


static gboolean
parse_int (const char *word,
	   int        *out)
{
  char *end;
  long val;
  int i;

  if (word == NULL)
    return FALSE;

  val = strtol (word, &end, 10);
  i = val;

  if (end != word && *end == '\0' && val >= 0 && val == i)
    {
      if (out)
        *out = i;

      return TRUE;
    }

  return FALSE;
}

/**
 * pango_parse_enum:
 * @type: enum type to parse, eg. %PANGO_TYPE_ELLIPSIZE_MODE
 * @str: (nullable): string to parse
 * @value: (out) (optional): integer to store the result in
 * @warn: if %TRUE, issue a g_warning() on bad input
 * @possible_values: (out) (optional): place to store list of possible
 *   values on failure
 *
 * Parses an enum type and stores the result in @value.
 *
 * If @str does not match the nick name of any of the possible values
 * for the enum and is not an integer, %FALSE is returned, a warning
 * is issued if @warn is %TRUE, and a string representing the list of
 * possible values is stored in @possible_values. The list is
 * slash-separated, eg. "none/start/middle/end".
 *
 * If failed and @possible_values is not %NULL, returned string should
 * be freed using g_free().
 *
 * Returns: %TRUE if @str was successfully parsed
 *
 * Deprecated: 1.38
 *
 * Since: 1.16
 */
gboolean
pango_parse_enum (GType       type,
		  const char *str,
		  int        *value,
		  gboolean    warn,
		  char      **possible_values)
{
  return _pango_parse_enum (type, str, value, warn, possible_values);
}

gboolean
_pango_parse_enum (GType       type,
		   const char *str,
 		   int        *value,
		   gboolean    warn,
		   char      **possible_values)
{
  GEnumClass *class = NULL;
  gboolean ret = TRUE;
  GEnumValue *v = NULL;

  class = g_type_class_ref (type);

  if (G_LIKELY (str))
    v = g_enum_get_value_by_nick (class, str);

  if (v)
    {
      if (G_LIKELY (value))
	*value = v->value;
    }
  else if (!parse_int (str, value))
    {
      ret = FALSE;
      if (G_LIKELY (warn || possible_values))
	{
	  int i;
	  GString *s = g_string_new (NULL);

	  for (i = 0, v = g_enum_get_value (class, i); v;
	       i++  , v = g_enum_get_value (class, i))
	    {
	      if (i)
		g_string_append_c (s, '/');
	      g_string_append (s, v->value_nick);
	    }

	  if (warn)
	    g_warning ("%s must be one of %s",
		       G_ENUM_CLASS_TYPE_NAME(class),
		       s->str);

	  if (possible_values)
	    *possible_values = g_string_free (s, FALSE);
          else
	    g_string_free (s, TRUE);
	}
    }

  g_type_class_unref (class);

  return ret;
}

gboolean
pango_parse_flags (GType        type,
                   const char  *str,
                   int         *value,
                   char       **possible_values)
{
  GFlagsClass *class = NULL;
  gboolean ret = TRUE;
  GFlagsValue *v = NULL;

  class = g_type_class_ref (type);

  v = g_flags_get_value_by_nick (class, str);

  if (v)
    {
      *value = v->value;
    }
  else if (!parse_int (str, value))
    {
      char **strv = g_strsplit (str, "|", 0);
      int i;

      *value = 0;

      for (i = 0; strv[i]; i++)
        {
          strv[i] = g_strstrip (strv[i]);
          v = g_flags_get_value_by_nick (class, strv[i]);
          if (!v)
            {
              ret = FALSE;
              break;
            }
          *value |= v->value;
        }
      g_strfreev (strv);

      if (!ret && possible_values)
	{
	  int i;
	  GString *s = g_string_new (NULL);

          for (i = 0; i < class->n_values; i++)
            {
              v = &class->values[i];
              if (i)
                g_string_append_c (s, '/');
              g_string_append (s, v->value_nick);
            }

          *possible_values = g_string_free (s, FALSE);
	}
    }

  g_type_class_unref (class);

  return ret;
}

/**
 * pango_lookup_aliases:
 * @fontname: an ASCII string
 * @families: (out) (array length=n_families): will be set to an array of
 *   font family names. This array is owned by Pango and should not be freed
 * @n_families: (out): will be set to the length of the @families array
 *
 * Look up all user defined aliases for the alias @fontname.
 *
 * The resulting font family names will be stored in @families,
 * and the number of families in @n_families.
 *
 * Deprecated: 1.32: This function is not thread-safe.
 */
void
pango_lookup_aliases (const char   *fontname,
		      char       ***families,
		      int          *n_families)
{
  *families = NULL;
  *n_families = 0;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"

/**
 * pango_find_base_dir:
 * @text: the text to process. Must be valid UTF-8
 * @length: length of @text in bytes (may be -1 if @text is nul-terminated)
 *
 * Searches a string the first character that has a strong
 * direction, according to the Unicode bidirectional algorithm.
 *
 * Returns: The direction corresponding to the first strong character.
 *   If no such character is found, then %PANGO_DIRECTION_NEUTRAL is returned.
 *
 * Since: 1.4
 */
PangoDirection
pango_find_base_dir (const gchar *text,
		     gint         length)
{
  PangoDirection dir = PANGO_DIRECTION_NEUTRAL;
  const gchar *p;

  g_return_val_if_fail (text != NULL || length == 0, PANGO_DIRECTION_NEUTRAL);

  p = text;
  while ((length < 0 || p < text + length) && *p)
    {
      gunichar wc = g_utf8_get_char (p);

      dir = pango_unichar_direction (wc);

      if (dir != PANGO_DIRECTION_NEUTRAL)
	break;

      p = g_utf8_next_char (p);
    }

  return dir;
}

#pragma GCC diagnostic pop

/**
 * pango_is_zero_width:
 * @ch: a Unicode character
 *
 * Checks if a character that should not be normally rendered.
 *
 * This includes all Unicode characters with "ZERO WIDTH" in their name,
 * as well as *bidi* formatting characters, and a few other ones.
 *
 * This is totally different from [func@GLib.unichar_iszerowidth] and is at best misnamed.
 *
 * Returns: %TRUE if @ch is a zero-width character, %FALSE otherwise
 *
 * Since: 1.10
 */
gboolean
pango_is_zero_width (gunichar ch)
{
/* Zero Width characters:
 *
 *  00AD  SOFT HYPHEN
 *  034F  COMBINING GRAPHEME JOINER
 *
 *  200B  ZERO WIDTH SPACE
 *  200C  ZERO WIDTH NON-JOINER
 *  200D  ZERO WIDTH JOINER
 *  200E  LEFT-TO-RIGHT MARK
 *  200F  RIGHT-TO-LEFT MARK
 *
 *  2028  LINE SEPARATOR
 *
 *  2060  WORD JOINER
 *  2061  FUNCTION APPLICATION
 *  2062  INVISIBLE TIMES
 *  2063  INVISIBLE SEPARATOR
 *
 *  2066  LEFT-TO-RIGHT ISOLATE
 *  2067  RIGHT-TO-LEFT ISOLATE
 *  2068  FIRST STRONG ISOLATE
 *  2069  POP DIRECTIONAL ISOLATE
 *
 *  202A  LEFT-TO-RIGHT EMBEDDING
 *  202B  RIGHT-TO-LEFT EMBEDDING
 *  202C  POP DIRECTIONAL FORMATTING
 *  202D  LEFT-TO-RIGHT OVERRIDE
 *  202E  RIGHT-TO-LEFT OVERRIDE
 *
 *  FEFF  ZERO WIDTH NO-BREAK SPACE
 */
  return ((ch & ~(gunichar)0x007F) == 0x2000 && (
		(ch >= 0x200B && ch <= 0x200F) ||
		(ch >= 0x202A && ch <= 0x202E) ||
		(ch >= 0x2060 && ch <= 0x2063) ||
                (ch >= 0x2066 && ch <= 0x2069) ||
		(ch == 0x2028)
	 )) || G_UNLIKELY (ch == 0x00AD
			|| ch == 0x034F
			|| ch == 0xFEFF);
}

/**
 * pango_quantize_line_geometry:
 * @thickness: (inout): pointer to the thickness of a line, in Pango units
 * @position: (inout): corresponding position
 *
 * Quantizes the thickness and position of a line to whole device pixels.
 *
 * This is typically used for underline or strikethrough. The purpose of
 * this function is to avoid such lines looking blurry.
 *
 * Care is taken to make sure @thickness is at least one pixel when this
 * function returns, but returned @position may become zero as a result
 * of rounding.
 *
 * Since: 1.12
 */
void
pango_quantize_line_geometry (int *thickness,
			      int *position)
{
  int thickness_pixels = (*thickness + PANGO_SCALE / 2) / PANGO_SCALE;
  if (thickness_pixels == 0)
    thickness_pixels = 1;

  if (thickness_pixels & 1)
    {
      int new_center = ((*position - *thickness / 2) & ~(PANGO_SCALE - 1)) + PANGO_SCALE / 2;
      *position = new_center + (PANGO_SCALE * thickness_pixels) / 2;
    }
  else
    {
      int new_center = ((*position - *thickness / 2 + PANGO_SCALE / 2) & ~(PANGO_SCALE - 1));
      *position = new_center + (PANGO_SCALE * thickness_pixels) / 2;
    }

  *thickness = thickness_pixels * PANGO_SCALE;
}

/**
 * pango_units_from_double:
 * @d: double floating-point value
 *
 * Converts a floating-point number to Pango units.
 *
 * The conversion is done by multiplying @d by %PANGO_SCALE and
 * rounding the result to nearest integer.
 *
 * Returns: the value in Pango units.
 *
 * Since: 1.16
 */
int
pango_units_from_double (double d)
{
  return (int)floor (d * PANGO_SCALE + 0.5);
}

/**
 * pango_units_to_double:
 * @i: value in Pango units
 *
 * Converts a number in Pango units to floating-point.
 *
 * The conversion is done by dividing @i by %PANGO_SCALE.
 *
 * Returns: the double value.
 *
 * Since: 1.16
 */
double
pango_units_to_double (int i)
{
  return (double)i / PANGO_SCALE;
}

/**
 * pango_extents_to_pixels:
 * @inclusive: (nullable): rectangle to round to pixels inclusively
 * @nearest: (nullable): rectangle to round to nearest pixels
 *
 * Converts extents from Pango units to device units.
 *
 * The conversion is done by dividing by the %PANGO_SCALE factor and
 * performing rounding.
 *
 * The @inclusive rectangle is converted by flooring the x/y coordinates
 * and extending width/height, such that the final rectangle completely
 * includes the original rectangle.
 *
 * The @nearest rectangle is converted by rounding the coordinates
 * of the rectangle to the nearest device unit (pixel).
 *
 * The rule to which argument to use is: if you want the resulting device-space
 * rectangle to completely contain the original rectangle, pass it in as
 * @inclusive. If you want two touching-but-not-overlapping rectangles stay
 * touching-but-not-overlapping after rounding to device units, pass them in
 * as @nearest.
 *
 * Since: 1.16
 */
void
pango_extents_to_pixels (PangoRectangle *inclusive,
			 PangoRectangle *nearest)
{
  if (inclusive)
    {
      int orig_x = inclusive->x;
      int orig_y = inclusive->y;

      inclusive->x = PANGO_PIXELS_FLOOR (inclusive->x);
      inclusive->y = PANGO_PIXELS_FLOOR (inclusive->y);

      inclusive->width  = PANGO_PIXELS_CEIL (orig_x + inclusive->width ) - inclusive->x;
      inclusive->height = PANGO_PIXELS_CEIL (orig_y + inclusive->height) - inclusive->y;
    }

  if (nearest)
    {
      int orig_x = nearest->x;
      int orig_y = nearest->y;

      nearest->x = PANGO_PIXELS (nearest->x);
      nearest->y = PANGO_PIXELS (nearest->y);

      nearest->width  = PANGO_PIXELS (orig_x + nearest->width ) - nearest->x;
      nearest->height = PANGO_PIXELS (orig_y + nearest->height) - nearest->y;
    }
}





/*********************************************************
 * Some internal functions for handling PANGO_ATTR_SHAPE *
 ********************************************************/

void
_pango_shape_shape (const char       *text,
		    unsigned int      n_chars,
		    PangoRectangle   *shape_ink G_GNUC_UNUSED,
		    PangoRectangle   *shape_logical,
		    PangoGlyphString *glyphs)
{
  unsigned int i;
  const char *p;

  pango_glyph_string_set_size (glyphs, n_chars);

  for (i=0, p = text; i < n_chars; i++, p = g_utf8_next_char (p))
    {
      glyphs->glyphs[i].glyph = PANGO_GLYPH_EMPTY;
      glyphs->glyphs[i].geometry.x_offset = 0;
      glyphs->glyphs[i].geometry.y_offset = 0;
      glyphs->glyphs[i].geometry.width = shape_logical->width;
      glyphs->glyphs[i].attr.is_cluster_start = 1;

      glyphs->log_clusters[i] = p - text;
    }
}

void
_pango_shape_get_extents (gint              n_chars,
			  PangoRectangle   *shape_ink,
			  PangoRectangle   *shape_logical,
			  PangoRectangle   *ink_rect,
			  PangoRectangle   *logical_rect)
{
  if (n_chars > 0)
    {
      if (ink_rect)
	{
	  ink_rect->x = MIN (shape_ink->x, shape_ink->x + shape_logical->width * (n_chars - 1));
	  ink_rect->width = MAX (shape_ink->width, shape_ink->width + shape_logical->width * (n_chars - 1));
	  ink_rect->y = shape_ink->y;
	  ink_rect->height = shape_ink->height;
	}
      if (logical_rect)
	{
	  logical_rect->x = MIN (shape_logical->x, shape_logical->x + shape_logical->width * (n_chars - 1));
	  logical_rect->width = MAX (shape_logical->width, shape_logical->width + shape_logical->width * (n_chars - 1));
	  logical_rect->y = shape_logical->y;
	  logical_rect->height = shape_logical->height;
	}
    }
  else
    {
      if (ink_rect)
	{
	  ink_rect->x = 0;
	  ink_rect->y = 0;
	  ink_rect->width = 0;
	  ink_rect->height = 0;
	}

      if (logical_rect)
	{
	  logical_rect->x = 0;
	  logical_rect->y = 0;
	  logical_rect->width = 0;
	  logical_rect->height = 0;
	}
    }
}

/**
 * pango_find_paragraph_boundary:
 * @text: UTF-8 text
 * @length: length of @text in bytes, or -1 if nul-terminated
 * @paragraph_delimiter_index: (out): return location for index of
 *   delimiter
 * @next_paragraph_start: (out): return location for start of next
 *   paragraph
 *
 * Locates a paragraph boundary in @text.
 *
 * A boundary is caused by delimiter characters, such as
 * a newline, carriage return, carriage return-newline pair,
 * or Unicode paragraph separator character.
 *
 * The index of the run of delimiters is returned in
 * @paragraph_delimiter_index. The index of the start of the
 * next paragraph (index after all delimiters) is stored n
 * @next_paragraph_start.
 *
 * If no delimiters are found, both @paragraph_delimiter_index
 * and @next_paragraph_start are filled with the length of @text
 * (an index one off the end).
 */
void
pango_find_paragraph_boundary (const char *text,
                               int         length,
                               int        *paragraph_delimiter_index,
                               int        *next_paragraph_start)
{
  const char *p = text;
  const char *end;
  const char *start = NULL;
  const char *delimiter = NULL;

  /* Only one character has type G_UNICODE_PARAGRAPH_SEPARATOR in
   * Unicode 5.0; update the following code if that changes.
   */

  /* prev_sep is the first byte of the previous separator.  Since
   * the valid separators are \r, \n, and PARAGRAPH_SEPARATOR, the
   * first byte is enough to identify it.
   */
  char prev_sep;

#define PARAGRAPH_SEPARATOR_STRING "\xE2\x80\xA9"

  if (length < 0)
    length = strlen (text);

  end = text + length;

  if (paragraph_delimiter_index)
    *paragraph_delimiter_index = length;

  if (next_paragraph_start)
    *next_paragraph_start = length;

  if (length == 0)
    return;

  prev_sep = 0;
  while (p < end)
    {
      if (prev_sep == '\n' ||
          prev_sep == PARAGRAPH_SEPARATOR_STRING[0])
        {
          g_assert (delimiter);
          start = p;
          break;
        }
      else if (prev_sep == '\r')
        {
          /* don't break between \r and \n */
          if (*p != '\n')
            {
              g_assert (delimiter);
              start = p;
              break;
            }
        }

      if (*p == '\n' ||
           *p == '\r' ||
           !strncmp(p, PARAGRAPH_SEPARATOR_STRING, strlen (PARAGRAPH_SEPARATOR_STRING)))
        {
          if (delimiter == NULL)
            delimiter = p;
          prev_sep = *p;
        }
      else
        prev_sep = 0;

      p = g_utf8_next_char (p);
    }

  if (delimiter && paragraph_delimiter_index)
    *paragraph_delimiter_index = delimiter - text;

  if (start && next_paragraph_start)
    *next_paragraph_start = start - text;
}
