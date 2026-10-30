/* GLIB - Library of useful routines for C programming
 * Copyright © 2020 Red Hat, Inc.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General
 * Public License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

#include <contrib/restricted/glib/config.h>

#include <stdlib.h>
#include <string.h>

#include "glib.h"
#include "glibintl.h"
#include "guriprivate.h"

/**
 * SECTION:guri
 * @short_description: URI-handling utilities
 * @include: glib.h
 *
 * The #GUri type and related functions can be used to parse URIs into
 * their components, and build valid URIs from individual components.
 *
 * Note that #GUri scope is to help manipulate URIs in various applications,
 * following [RFC 3986](https://tools.ietf.org/html/rfc3986). In particular,
 * it doesn't intend to cover web browser needs, and doesn't implement the
 * [WHATWG URL](https://url.spec.whatwg.org/) standard. No APIs are provided to
 * help prevent
 * [homograph attacks](https://en.wikipedia.org/wiki/IDN_homograph_attack), so
 * #GUri is not suitable for formatting URIs for display to the user for making
 * security-sensitive decisions.
 *
 * ## Relative and absolute URIs # {#relative-absolute-uris}
 *
 * As defined in [RFC 3986](https://tools.ietf.org/html/rfc3986#section-4), the
 * hierarchical nature of URIs means that they can either be ‘relative
 * references’ (sometimes referred to as ‘relative URIs’) or ‘URIs’ (for
 * clarity, ‘URIs’ are referred to in this documentation as
 * ‘absolute URIs’ — although
 * [in constrast to RFC 3986](https://tools.ietf.org/html/rfc3986#section-4.3),
 * fragment identifiers are always allowed).
 *
 * Relative references have one or more components of the URI missing. In
 * particular, they have no scheme. Any other component, such as hostname,
 * query, etc. may be missing, apart from a path, which has to be specified (but
 * may be empty). The path may be relative, starting with `./` rather than `/`.
 *
 * For example, a valid relative reference is `./path?query`,
 * `/?query#fragment` or `//example.com`.
 *
 * Absolute URIs have a scheme specified. Any other components of the URI which
 * are missing are specified as explicitly unset in the URI, rather than being
 * resolved relative to a base URI using g_uri_parse_relative().
 *
 * For example, a valid absolute URI is `file:///home/bob` or
 * `https://search.com?query=string`.
 *
 * A #GUri instance is always an absolute URI. A string may be an absolute URI
 * or a relative reference; see the documentation for individual functions as to
 * what forms they accept.
 *
 * ## Parsing URIs
 *
 * The most minimalist APIs for parsing URIs are g_uri_split() and
 * g_uri_split_with_user(). These split a URI into its component
 * parts, and return the parts; the difference between the two is that
 * g_uri_split() treats the ‘userinfo’ component of the URI as a
 * single element, while g_uri_split_with_user() can (depending on the
 * #GUriFlags you pass) treat it as containing a username, password,
 * and authentication parameters. Alternatively, g_uri_split_network()
 * can be used when you are only interested in the components that are
 * needed to initiate a network connection to the service (scheme,
 * host, and port).
 *
 * g_uri_parse() is similar to g_uri_split(), but instead of returning
 * individual strings, it returns a #GUri structure (and it requires
 * that the URI be an absolute URI).
 *
 * g_uri_resolve_relative() and g_uri_parse_relative() allow you to
 * resolve a relative URI relative to a base URI.
 * g_uri_resolve_relative() takes two strings and returns a string,
 * and g_uri_parse_relative() takes a #GUri and a string and returns a
 * #GUri.
 *
 * All of the parsing functions take a #GUriFlags argument describing
 * exactly how to parse the URI; see the documentation for that type
 * for more details on the specific flags that you can pass. If you
 * need to choose different flags based on the type of URI, you can
 * use g_uri_peek_scheme() on the URI string to check the scheme
 * first, and use that to decide what flags to parse it with.
 *
 * For example, you might want to use %G_URI_PARAMS_WWW_FORM when parsing the
 * params for a web URI, so compare the result of g_uri_peek_scheme() against
 * `http` and `https`.
 *
 * ## Building URIs
 *
 * g_uri_join() and g_uri_join_with_user() can be used to construct
 * valid URI strings from a set of component strings. They are the
 * inverse of g_uri_split() and g_uri_split_with_user().
 *
 * Similarly, g_uri_build() and g_uri_build_with_user() can be used to
 * construct a #GUri from a set of component strings.
 *
 * As with the parsing functions, the building functions take a
 * #GUriFlags argument. In particular, it is important to keep in mind
 * whether the URI components you are using are already `%`-encoded. If so,
 * you must pass the %G_URI_FLAGS_ENCODED flag.
 *
 * ## `file://` URIs
 *
 * Note that Windows and Unix both define special rules for parsing
 * `file://` URIs (involving non-UTF-8 character sets on Unix, and the
 * interpretation of path separators on Windows). #GUri does not
 * implement these rules. Use g_filename_from_uri() and
 * g_filename_to_uri() if you want to properly convert between
 * `file://` URIs and local filenames.
 *
 * ## URI Equality
 *
 * Note that there is no `g_uri_equal ()` function, because comparing
 * URIs usefully requires scheme-specific knowledge that #GUri does
 * not have. #GUri can help with normalization if you use the various
 * encoded #GUriFlags as well as %G_URI_FLAGS_SCHEME_NORMALIZE however
 * it is not comprehensive.
 * For example, `data:,foo` and `data:;base64,Zm9v` resolve to the same
 * thing according to the `data:` URI specification which GLib does not
 * handle.
 *
 * Since: 2.66
 */

/**
 * GUri:
 *
 * A parsed absolute URI.
 *
 * Since #GUri only represents absolute URIs, all #GUris will have a
 * URI scheme, so g_uri_get_scheme() will always return a non-%NULL
 * answer. Likewise, by definition, all URIs have a path component, so
 * g_uri_get_path() will always return a non-%NULL string (which may be empty).
 *
 * If the URI string has an
 * [‘authority’ component](https://tools.ietf.org/html/rfc3986#section-3) (that
 * is, if the scheme is followed by `://` rather than just `:`), then the
 * #GUri will contain a hostname, and possibly a port and ‘userinfo’.
 * Additionally, depending on how the #GUri was constructed/parsed (for example,
 * using the %G_URI_FLAGS_HAS_PASSWORD and %G_URI_FLAGS_HAS_AUTH_PARAMS flags),
 * the userinfo may be split out into a username, password, and
 * additional authorization-related parameters.
 *
 * Normally, the components of a #GUri will have all `%`-encoded
 * characters decoded. However, if you construct/parse a #GUri with
 * %G_URI_FLAGS_ENCODED, then the `%`-encoding will be preserved instead in
 * the userinfo, path, and query fields (and in the host field if also
 * created with %G_URI_FLAGS_NON_DNS). In particular, this is necessary if
 * the URI may contain binary data or non-UTF-8 text, or if decoding
 * the components might change the interpretation of the URI.
 *
 * For example, with the encoded flag:
 *
 * |[<!-- language="C" -->
 *   g_autoptr(GUri) uri = g_uri_parse ("http://host/path?query=http%3A%2F%2Fhost%2Fpath%3Fparam%3Dvalue", G_URI_FLAGS_ENCODED, &err);
 *   g_assert_cmpstr (g_uri_get_query (uri), ==, "query=http%3A%2F%2Fhost%2Fpath%3Fparam%3Dvalue");
 * ]|
 *
 * While the default `%`-decoding behaviour would give:
 *
 * |[<!-- language="C" -->
 *   g_autoptr(GUri) uri = g_uri_parse ("http://host/path?query=http%3A%2F%2Fhost%2Fpath%3Fparam%3Dvalue", G_URI_FLAGS_NONE, &err);
 *   g_assert_cmpstr (g_uri_get_query (uri), ==, "query=http://host/path?param=value");
 * ]|
 *
 * During decoding, if an invalid UTF-8 string is encountered, parsing will fail
 * with an error indicating the bad string location:
 *
 * |[<!-- language="C" -->
 *   g_autoptr(GUri) uri = g_uri_parse ("http://host/path?query=http%3A%2F%2Fhost%2Fpath%3Fbad%3D%00alue", G_URI_FLAGS_NONE, &err);
 *   g_assert_error (err, G_URI_ERROR, G_URI_ERROR_BAD_QUERY);
 * ]|
 *
 * You should pass %G_URI_FLAGS_ENCODED or %G_URI_FLAGS_ENCODED_QUERY if you
 * need to handle that case manually. In particular, if the query string
 * contains `=` characters that are `%`-encoded, you should let
 * g_uri_parse_params() do the decoding once of the query.
 *
 * #GUri is immutable once constructed, and can safely be accessed from
 * multiple threads. Its reference counting is atomic.
 *
 * Since: 2.66
 */
struct _GUri {
  gchar     *scheme;
  gchar     *userinfo;
  gchar     *host;
  gint       port;
  gchar     *path;
  gchar     *query;
  gchar     *fragment;

  gchar     *user;
  gchar     *password;
  gchar     *auth_params;

  GUriFlags  flags;
};

/**
 * g_uri_ref: (skip)
 * @uri: a #GUri
 *
 * Increments the reference count of @uri by one.
 *
 * Returns: @uri
 *
 * Since: 2.66
 */
GUri *
g_uri_ref (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return g_atomic_rc_box_acquire (uri);
}

static void
g_uri_clear (GUri *uri)
{
  g_free (uri->scheme);
  g_free (uri->userinfo);
  g_free (uri->host);
  g_free (uri->path);
  g_free (uri->query);
  g_free (uri->fragment);
  g_free (uri->user);
  g_free (uri->password);
  g_free (uri->auth_params);
}

/**
 * g_uri_unref: (skip)
 * @uri: a #GUri
 *
 * Atomically decrements the reference count of @uri by one.
 *
 * When the reference count reaches zero, the resources allocated by
 * @uri are freed
 *
 * Since: 2.66
 */
void
g_uri_unref (GUri *uri)
{
  g_return_if_fail (uri != NULL);

  g_atomic_rc_box_release_full (uri, (GDestroyNotify)g_uri_clear);
}

static gboolean
g_uri_char_is_unreserved (gchar ch)
{
  if (g_ascii_isalnum (ch))
    return TRUE;
  return ch == '-' || ch == '.' || ch == '_' || ch == '~';
}

#define XDIGIT(c) ((c) <= '9' ? (c) - '0' : ((c) & 0x4F) - 'A' + 10)
#define HEXCHAR(s) ((XDIGIT (s[1]) << 4) + XDIGIT (s[2]))

static gssize
uri_decoder (gchar       **out,
             const gchar  *illegal_chars,
             const gchar  *start,
             gsize         length,
             gboolean      just_normalize,
             gboolean      www_form,
             GUriFlags     flags,
             GUriError     parse_error,
             GError      **error)
{
  gchar c;
  GString *decoded;
  const gchar *invalid, *s, *end;
  gssize len;

  if (!(flags & G_URI_FLAGS_ENCODED))
    just_normalize = FALSE;

  decoded = g_string_sized_new (length + 1);
  for (s = start, end = s + length; s < end; s++)
    {
      if (*s == '%')
        {
          if (s + 2 >= end ||
              !g_ascii_isxdigit (s[1]) ||
              !g_ascii_isxdigit (s[2]))
            {
              /* % followed by non-hex or the end of the string; this is an error */
              if (!(flags & G_URI_FLAGS_PARSE_RELAXED))
                {
                  g_set_error_literal (error, G_URI_ERROR, parse_error,
                                       /* xgettext: no-c-format */
                                       _("Invalid %-encoding in URI"));
                  g_string_free (decoded, TRUE);
                  return -1;
                }

              /* In non-strict mode, just let it through; we *don't*
               * fix it to "%25", since that might change the way that
               * the URI's owner would interpret it.
               */
              g_string_append_c (decoded, *s);
              continue;
            }

          c = HEXCHAR (s);
          if (illegal_chars && strchr (illegal_chars, c))
            {
              g_set_error_literal (error, G_URI_ERROR, parse_error,
                                   _("Illegal character in URI"));
              g_string_free (decoded, TRUE);
              return -1;
            }
          if (just_normalize && !g_uri_char_is_unreserved (c))
            {
              /* Leave the % sequence there but normalize it. */
              g_string_append_c (decoded, *s);
              g_string_append_c (decoded, g_ascii_toupper (s[1]));
              g_string_append_c (decoded, g_ascii_toupper (s[2]));
              s += 2;
            }
          else
            {
              g_string_append_c (decoded, c);
              s += 2;
            }
        }
      else if (www_form && *s == '+')
        g_string_append_c (decoded, ' ');
      /* Normalize any illegal characters. */
      else if (just_normalize && (!g_ascii_isgraph (*s)))
        g_string_append_printf (decoded, "%%%02X", (guchar)*s);
      else
        g_string_append_c (decoded, *s);
    }

  len = decoded->len;
  g_assert (len >= 0);

  if (!(flags & G_URI_FLAGS_ENCODED) &&
      !g_utf8_validate (decoded->str, len, &invalid))
    {
      g_set_error_literal (error, G_URI_ERROR, parse_error,
                           _("Non-UTF-8 characters in URI"));
      g_string_free (decoded, TRUE);
      return -1;
    }

  if (out)
    *out = g_string_free (decoded, FALSE);
  else
    g_string_free (decoded, TRUE);

  return len;
}

static gboolean
uri_decode (gchar       **out,
            const gchar  *illegal_chars,
            const gchar  *start,
            gsize         length,
            gboolean      www_form,
            GUriFlags     flags,
            GUriError     parse_error,
            GError      **error)
{
  return uri_decoder (out, illegal_chars, start, length, FALSE, www_form, flags,
                      parse_error, error) != -1;
}

static gboolean
uri_normalize (gchar       **out,
               const gchar  *start,
               gsize         length,
               GUriFlags     flags,
               GUriError     parse_error,
               GError      **error)
{
  return uri_decoder (out, NULL, start, length, TRUE, FALSE, flags,
                      parse_error, error) != -1;
}

static gboolean
is_valid (guchar       c,
          const gchar *reserved_chars_allowed)
{
  if (g_uri_char_is_unreserved (c))
    return TRUE;

  if (reserved_chars_allowed && strchr (reserved_chars_allowed, c))
    return TRUE;

  return FALSE;
}

void
_uri_encoder (GString      *out,
              const guchar *start,
              gsize         length,
              const gchar  *reserved_chars_allowed,
              gboolean      allow_utf8)
{
  static const gchar hex[] = "0123456789ABCDEF";
  const guchar *p = start;
  const guchar *end = p + length;

  while (p < end)
    {
      gunichar multibyte_utf8_char = 0;

      if (allow_utf8 && *p >= 0x80)
        multibyte_utf8_char = g_utf8_get_char_validated ((gchar *)p, end - p);

      if (multibyte_utf8_char > 0 &&
          multibyte_utf8_char != (gunichar) -1 && multibyte_utf8_char != (gunichar) -2)
        {
          gint len = g_utf8_skip [*p];
          g_string_append_len (out, (gchar *)p, len);
          p += len;
        }
      else if (is_valid (*p, reserved_chars_allowed))
        {
          g_string_append_c (out, *p);
          p++;
        }
      else
        {
          g_string_append_c (out, '%');
          g_string_append_c (out, hex[*p >> 4]);
          g_string_append_c (out, hex[*p & 0xf]);
          p++;
        }
    }
}

/* Parse the IP-literal construction from RFC 6874 (which extends RFC 3986 to
 * support IPv6 zone identifiers.
 *
 * Currently, IP versions beyond 6 (i.e. the IPvFuture rule) are unsupported.
 * There’s no point supporting them until (a) they exist and (b) the rest of the
 * stack (notably, sockets) supports them.
 *
 * Rules:
 *
 * IP-literal = "[" ( IPv6address / IPv6addrz / IPvFuture  ) "]"
 *
 * ZoneID = 1*( unreserved / pct-encoded )
 *
 * IPv6addrz = IPv6address "%25" ZoneID
 *
 * If %G_URI_FLAGS_PARSE_RELAXED is specified, this function also accepts:
 *
 * IPv6addrz = IPv6address "%" ZoneID
 */
static gboolean
parse_ip_literal (const gchar  *start,
                  gsize         length,
                  GUriFlags     flags,
                  gchar       **out,
                  GError      **error)
{
  gchar *pct, *zone_id = NULL;
  gchar *addr = NULL;
  gsize addr_length = 0;
  gsize zone_id_length = 0;
  gchar *decoded_zone_id = NULL;

  if (start[length - 1] != ']')
    goto bad_ipv6_literal;

  /* Drop the square brackets */
  addr = g_strndup (start + 1, length - 2);
  addr_length = length - 2;

  /* If there's an IPv6 scope ID, split out the zone. */
  pct = strchr (addr, '%');
  if (pct != NULL)
    {
      *pct = '\0';

      if (addr_length - (pct - addr) >= 4 &&
          *(pct + 1) == '2' && *(pct + 2) == '5')
        {
          zone_id = pct + 3;
          zone_id_length = addr_length - (zone_id - addr);
        }
      else if (flags & G_URI_FLAGS_PARSE_RELAXED &&
               addr_length - (pct - addr) >= 2)
        {
          zone_id = pct + 1;
          zone_id_length = addr_length - (zone_id - addr);
        }
      else
        goto bad_ipv6_literal;

      g_assert (zone_id_length >= 1);
    }

  /* addr must be an IPv6 address */
  if (!g_hostname_is_ip_address (addr) || !strchr (addr, ':'))
    goto bad_ipv6_literal;

  /* Zone ID must be valid. It can contain %-encoded characters. */
  if (zone_id != NULL &&
      !uri_decode (&decoded_zone_id, NULL, zone_id, zone_id_length, FALSE,
                   flags, G_URI_ERROR_BAD_HOST, NULL))
    goto bad_ipv6_literal;

  /* Success */
  if (out != NULL && decoded_zone_id != NULL)
    *out = g_strconcat (addr, "%", decoded_zone_id, NULL);
  else if (out != NULL)
    *out = g_steal_pointer (&addr);

  g_free (addr);
  g_free (decoded_zone_id);

  return TRUE;

bad_ipv6_literal:
  g_free (addr);
  g_free (decoded_zone_id);
  g_set_error (error, G_URI_ERROR, G_URI_ERROR_BAD_HOST,
               _("Invalid IPv6 address ‘%.*s’ in URI"),
               (gint)length, start);

  return FALSE;
}

static gboolean
parse_host (const gchar  *start,
            gsize         length,
            GUriFlags     flags,
            gchar       **out,
            GError      **error)
{
  gchar *decoded = NULL, *host;
  gchar *addr = NULL;

  if (*start == '[')
    {
      if (!parse_ip_literal (start, length, flags, &host, error))
        return FALSE;
      goto ok;
    }

  if (g_ascii_isdigit (*start))
    {
      addr = g_strndup (start, length);
      if (g_hostname_is_ip_address (addr))
        {
          host = addr;
          goto ok;
        }
      g_free (addr);
    }

  if (flags & G_URI_FLAGS_NON_DNS)
    {
      if (!uri_normalize (&decoded, start, length, flags,
                          G_URI_ERROR_BAD_HOST, error))
        return FALSE;
      host = g_steal_pointer (&decoded);
      goto ok;
    }

  flags &= ~G_URI_FLAGS_ENCODED;
  if (!uri_decode (&decoded, NULL, start, length, FALSE, flags,
                   G_URI_ERROR_BAD_HOST, error))
    return FALSE;

  /* You're not allowed to %-encode an IP address, so if it wasn't
   * one before, it better not be one now.
   */
  if (g_hostname_is_ip_address (decoded))
    {
      g_free (decoded);
      g_set_error (error, G_URI_ERROR, G_URI_ERROR_BAD_HOST,
                   _("Illegal encoded IP address ‘%.*s’ in URI"),
                   (gint)length, start);
      return FALSE;
    }

  if (g_hostname_is_non_ascii (decoded))
    {
      host = g_hostname_to_ascii (decoded);
      if (host == NULL)
        {
          g_free (decoded);
          g_set_error (error, G_URI_ERROR, G_URI_ERROR_BAD_HOST,
                       _("Illegal internationalized hostname ‘%.*s’ in URI"),
                       (gint) length, start);
          return FALSE;
        }
    }
  else
    {
      host = g_steal_pointer (&decoded);
    }

 ok:
  if (out)
    *out = g_steal_pointer (&host);
  g_free (host);
  g_free (decoded);

  return TRUE;
}

static gboolean
parse_port (const gchar  *start,
            gsize         length,
            gint         *out,
            GError      **error)
{
  gchar *end;
  gulong parsed_port;

  /* strtoul() allows leading + or -, so we have to check this first. */
  if (!g_ascii_isdigit (*start))
    {
      g_set_error (error, G_URI_ERROR, G_URI_ERROR_BAD_PORT,
                   _("Could not parse port ‘%.*s’ in URI"),
                   (gint)length, start);
      return FALSE;
    }

  /* We know that *(start + length) is either '\0' or a non-numeric
   * character, so strtoul() won't scan beyond it.
   */
  parsed_port = strtoul (start, &end, 10);
  if (end != start + length)
    {
      g_set_error (error, G_URI_ERROR, G_URI_ERROR_BAD_PORT,
                   _("Could not parse port ‘%.*s’ in URI"),
                   (gint)length, start);
      return FALSE;
    }
  else if (parsed_port > 65535)
    {
      g_set_error (error, G_URI_ERROR, G_URI_ERROR_BAD_PORT,
                   _("Port ‘%.*s’ in URI is out of range"),
                   (gint)length, start);
      return FALSE;
    }

  if (out)
    *out = parsed_port;
  return TRUE;
}

static gboolean
parse_userinfo (const gchar  *start,
                gsize         length,
                GUriFlags     flags,
                gchar       **user,
                gchar       **password,
                gchar       **auth_params,
                GError      **error)
{
  const gchar *user_end = NULL, *password_end = NULL, *auth_params_end;

  auth_params_end = start + length;
  if (flags & G_URI_FLAGS_HAS_AUTH_PARAMS)
    password_end = memchr (start, ';', auth_params_end - start);
  if (!password_end)
    password_end = auth_params_end;
  if (flags & G_URI_FLAGS_HAS_PASSWORD)
    user_end = memchr (start, ':', password_end - start);
  if (!user_end)
    user_end = password_end;

  if (!uri_normalize (user, start, user_end - start, flags,
                      G_URI_ERROR_BAD_USER, error))
    return FALSE;

  if (*user_end == ':')
    {
      start = user_end + 1;
      if (!uri_normalize (password, start, password_end - start, flags,
                          G_URI_ERROR_BAD_PASSWORD, error))
        {
          if (user)
            g_clear_pointer (user, g_free);
          return FALSE;
        }
    }
  else if (password)
    *password = NULL;

  if (*password_end == ';')
    {
      start = password_end + 1;
      if (!uri_normalize (auth_params, start, auth_params_end - start, flags,
                          G_URI_ERROR_BAD_AUTH_PARAMS, error))
        {
          if (user)
            g_clear_pointer (user, g_free);
          if (password)
            g_clear_pointer (password, g_free);
          return FALSE;
        }
    }
  else if (auth_params)
    *auth_params = NULL;

  return TRUE;
}

static gchar *
uri_cleanup (const gchar *uri_string)
{
  GString *copy;
  const gchar *end;

  /* Skip leading whitespace */
  while (g_ascii_isspace (*uri_string))
    uri_string++;

  /* Ignore trailing whitespace */
  end = uri_string + strlen (uri_string);
  while (end > uri_string && g_ascii_isspace (*(end - 1)))
    end--;

  /* Copy the rest, encoding unencoded spaces and stripping other whitespace */
  copy = g_string_sized_new (end - uri_string);
  while (uri_string < end)
    {
      if (*uri_string == ' ')
        g_string_append (copy, "%20");
      else if (g_ascii_isspace (*uri_string))
        ;
      else
        g_string_append_c (copy, *uri_string);
      uri_string++;
    }

  return g_string_free (copy, FALSE);
}

static gboolean
should_normalize_empty_path (const char *scheme)
{
  const char * const schemes[] = { "https", "http", "wss", "ws" };
  gsize i;
  for (i = 0; i < G_N_ELEMENTS (schemes); ++i)
    {
      if (!strcmp (schemes[i], scheme))
        return TRUE;
    }
  return FALSE;
}

static int
normalize_port (const char *scheme,
                int         port)
{
  const char *default_schemes[3] = { NULL };
  int i;

  switch (port)
    {
    case 21:
      default_schemes[0] = "ftp";
      break;
    case 80:
      default_schemes[0] = "http";
      default_schemes[1] = "ws";
      break;
    case 443:
      default_schemes[0] = "https";
      default_schemes[1] = "wss";
      break;
    default:
      break;
    }

  for (i = 0; default_schemes[i]; ++i)
    {
      if (!strcmp (scheme, default_schemes[i]))
        return -1;
    }

  return port;
}

static int
default_scheme_port (const char *scheme)
{
  if (strcmp (scheme, "http") == 0 || strcmp (scheme, "ws") == 0)
    return 80;

  if (strcmp (scheme, "https") == 0 || strcmp (scheme, "wss") == 0)
    return 443;

  if (strcmp (scheme, "ftp") == 0)
    return 21;

  return -1;
}

static gboolean
g_uri_split_internal (const gchar  *uri_string,
                      GUriFlags     flags,
                      gchar       **scheme,
                      gchar       **userinfo,
                      gchar       **user,
                      gchar       **password,
                      gchar       **auth_params,
                      gchar       **host,
                      gint         *port,
                      gchar       **path,
                      gchar       **query,
                      gchar       **fragment,
                      GError      **error)
{
  const gchar *end, *colon, *at, *path_start, *semi, *question;
  const gchar *p, *bracket, *hostend;
  gchar *cleaned_uri_string = NULL;
  gchar *normalized_scheme = NULL;

  if (scheme)
    *scheme = NULL;
  if (userinfo)
    *userinfo = NULL;
  if (user)
    *user = NULL;
  if (password)
    *password = NULL;
  if (auth_params)
    *auth_params = NULL;
  if (host)
    *host = NULL;
  if (port)
    *port = -1;
  if (path)
    *path = NULL;
  if (query)
    *query = NULL;
  if (fragment)
    *fragment = NULL;

  if ((flags & G_URI_FLAGS_PARSE_RELAXED) && strpbrk (uri_string, " \t\n\r"))
    {
      cleaned_uri_string = uri_cleanup (uri_string);
      uri_string = cleaned_uri_string;
    }

  /* Find scheme */
  p = uri_string;
  while (*p && (g_ascii_isalpha (*p) ||
               (p > uri_string && (g_ascii_isdigit (*p) ||
                                   *p == '.' || *p == '+' || *p == '-'))))
    p++;

  if (p > uri_string && *p == ':')
    {
      normalized_scheme = g_ascii_strdown (uri_string, p - uri_string);
      if (scheme)
        *scheme = g_steal_pointer (&normalized_scheme);
      p++;
    }
  else
    {
      if (scheme)
        *scheme = NULL;
      p = uri_string;
    }

  /* Check for authority */
  if (strncmp (p, "//", 2) == 0)
    {
      p += 2;

      path_start = p + strcspn (p, "/?#");
      at = memchr (p, '@', path_start - p);
      if (at)
        {
          if (flags & G_URI_FLAGS_PARSE_RELAXED)
            {
              gchar *next_at;

              /* Any "@"s in the userinfo must be %-encoded, but
               * people get this wrong sometimes. Since "@"s in the
               * hostname are unlikely (and also wrong anyway), assume
               * that if there are extra "@"s, they belong in the
               * userinfo.
               */
              do
                {
                  next_at = memchr (at + 1, '@', path_start - (at + 1));
                  if (next_at)
                    at = next_at;
                }
              while (next_at);
            }

          if (user || password || auth_params ||
              (flags & (G_URI_FLAGS_HAS_PASSWORD|G_URI_FLAGS_HAS_AUTH_PARAMS)))
            {
              if (!parse_userinfo (p, at - p, flags,
                                   user, password, auth_params,
                                   error))
                goto fail;
            }

          if (!uri_normalize (userinfo, p, at - p, flags,
                              G_URI_ERROR_BAD_USER, error))
            goto fail;

          p = at + 1;
        }

      if (flags & G_URI_FLAGS_PARSE_RELAXED)
        {
          semi = strchr (p, ';');
          if (semi && semi < path_start)
            {
              /* Technically, semicolons are allowed in the "host"
               * production, but no one ever does this, and some
               * schemes mistakenly use semicolon as a delimiter
               * marking the start of the path. We have to check this
               * after checking for userinfo though, because a
               * semicolon before the "@" must be part of the
               * userinfo.
               */
              path_start = semi;
            }
        }

      /* Find host and port. The host may be a bracket-delimited IPv6
       * address, in which case the colon delimiting the port must come
       * (immediately) after the close bracket.
       */
      if (*p == '[')
        {
          bracket = memchr (p, ']', path_start - p);
          if (bracket && *(bracket + 1) == ':')
            colon = bracket + 1;
          else
            colon = NULL;
        }
      else
        colon = memchr (p, ':', path_start - p);

      hostend = colon ? colon : path_start;
      if (!parse_host (p, hostend - p, flags, host, error))
        goto fail;

      if (colon && colon != path_start - 1)
        {
          p = colon + 1;
          if (!parse_port (p, path_start - p, port, error))
            goto fail;
        }

      p = path_start;
    }

  /* Find fragment. */
  end = p + strcspn (p, "#");
  if (*end == '#')
    {
      if (!uri_normalize (fragment, end + 1, strlen (end + 1),
                          flags | (flags & G_URI_FLAGS_ENCODED_FRAGMENT ? G_URI_FLAGS_ENCODED : 0),
                          G_URI_ERROR_BAD_FRAGMENT, error))
        goto fail;
    }

  /* Find query */
  question = memchr (p, '?', end - p);
  if (question)
    {
      if (!uri_normalize (query, question + 1, end - (question + 1),
                          flags | (flags & G_URI_FLAGS_ENCODED_QUERY ? G_URI_FLAGS_ENCODED : 0),
                          G_URI_ERROR_BAD_QUERY, error))
        goto fail;
      end = question;
    }

  if (!uri_normalize (path, p, end - p,
                      flags | (flags & G_URI_FLAGS_ENCODED_PATH ? G_URI_FLAGS_ENCODED : 0),
                      G_URI_ERROR_BAD_PATH, error))
    goto fail;

  /* Scheme-based normalization */
  if (flags & G_URI_FLAGS_SCHEME_NORMALIZE && ((scheme && *scheme) || normalized_scheme))
    {
      const char *scheme_str = scheme && *scheme ? *scheme : normalized_scheme;

      if (should_normalize_empty_path (scheme_str) && path && !**path)
        {
          g_free (*path);
          *path = g_strdup ("/");
        }

      if (port && *port == -1)
        *port = default_scheme_port (scheme_str);
    }

  g_free (normalized_scheme);
  g_free (cleaned_uri_string);
  return TRUE;

 fail:
  if (scheme)
    g_clear_pointer (scheme, g_free);
  if (userinfo)
    g_clear_pointer (userinfo, g_free);
  if (host)
    g_clear_pointer (host, g_free);
  if (port)
    *port = -1;
  if (path)
    g_clear_pointer (path, g_free);
  if (query)
    g_clear_pointer (query, g_free);
  if (fragment)
    g_clear_pointer (fragment, g_free);

  g_free (normalized_scheme);
  g_free (cleaned_uri_string);
  return FALSE;
}

/**
 * g_uri_split:
 * @uri_ref: a string containing a relative or absolute URI
 * @flags: flags for parsing @uri_ref
 * @scheme: (out) (nullable) (optional) (transfer full): on return, contains
 *    the scheme (converted to lowercase), or %NULL
 * @userinfo: (out) (nullable) (optional) (transfer full): on return, contains
 *    the userinfo, or %NULL
 * @host: (out) (nullable) (optional) (transfer full): on return, contains the
 *    host, or %NULL
 * @port: (out) (optional) (transfer full): on return, contains the
 *    port, or `-1`
 * @path: (out) (not nullable) (optional) (transfer full): on return, contains the
 *    path
 * @query: (out) (nullable) (optional) (transfer full): on return, contains the
 *    query, or %NULL
 * @fragment: (out) (nullable) (optional) (transfer full): on return, contains
 *    the fragment, or %NULL
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Parses @uri_ref (which can be an
 * [absolute or relative URI][relative-absolute-uris]) according to @flags, and
 * returns the pieces. Any component that doesn't appear in @uri_ref will be
 * returned as %NULL (but note that all URIs always have a path component,
 * though it may be the empty string).
 *
 * If @flags contains %G_URI_FLAGS_ENCODED, then `%`-encoded characters in
 * @uri_ref will remain encoded in the output strings. (If not,
 * then all such characters will be decoded.) Note that decoding will
 * only work if the URI components are ASCII or UTF-8, so you will
 * need to use %G_URI_FLAGS_ENCODED if they are not.
 *
 * Note that the %G_URI_FLAGS_HAS_PASSWORD and
 * %G_URI_FLAGS_HAS_AUTH_PARAMS @flags are ignored by g_uri_split(),
 * since it always returns only the full userinfo; use
 * g_uri_split_with_user() if you want it split up.
 *
 * Returns: (skip): %TRUE if @uri_ref parsed successfully, %FALSE
 *   on error.
 *
 * Since: 2.66
 */
gboolean
g_uri_split (const gchar  *uri_ref,
             GUriFlags     flags,
             gchar       **scheme,
             gchar       **userinfo,
             gchar       **host,
             gint         *port,
             gchar       **path,
             gchar       **query,
             gchar       **fragment,
             GError      **error)
{
  g_return_val_if_fail (uri_ref != NULL, FALSE);
  g_return_val_if_fail (error == NULL || *error == NULL, FALSE);

  return g_uri_split_internal (uri_ref, flags,
                               scheme, userinfo, NULL, NULL, NULL,
                               host, port, path, query, fragment,
                               error);
}

/**
 * g_uri_split_with_user:
 * @uri_ref: a string containing a relative or absolute URI
 * @flags: flags for parsing @uri_ref
 * @scheme: (out) (nullable) (optional) (transfer full): on return, contains
 *    the scheme (converted to lowercase), or %NULL
 * @user: (out) (nullable) (optional) (transfer full): on return, contains
 *    the user, or %NULL
 * @password: (out) (nullable) (optional) (transfer full): on return, contains
 *    the password, or %NULL
 * @auth_params: (out) (nullable) (optional) (transfer full): on return, contains
 *    the auth_params, or %NULL
 * @host: (out) (nullable) (optional) (transfer full): on return, contains the
 *    host, or %NULL
 * @port: (out) (optional) (transfer full): on return, contains the
 *    port, or `-1`
 * @path: (out) (not nullable) (optional) (transfer full): on return, contains the
 *    path
 * @query: (out) (nullable) (optional) (transfer full): on return, contains the
 *    query, or %NULL
 * @fragment: (out) (nullable) (optional) (transfer full): on return, contains
 *    the fragment, or %NULL
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Parses @uri_ref (which can be an
 * [absolute or relative URI][relative-absolute-uris]) according to @flags, and
 * returns the pieces. Any component that doesn't appear in @uri_ref will be
 * returned as %NULL (but note that all URIs always have a path component,
 * though it may be the empty string).
 *
 * See g_uri_split(), and the definition of #GUriFlags, for more
 * information on the effect of @flags. Note that @password will only
 * be parsed out if @flags contains %G_URI_FLAGS_HAS_PASSWORD, and
 * @auth_params will only be parsed out if @flags contains
 * %G_URI_FLAGS_HAS_AUTH_PARAMS.
 *
 * Returns: (skip): %TRUE if @uri_ref parsed successfully, %FALSE
 *   on error.
 *
 * Since: 2.66
 */
gboolean
g_uri_split_with_user (const gchar  *uri_ref,
                       GUriFlags     flags,
                       gchar       **scheme,
                       gchar       **user,
                       gchar       **password,
                       gchar       **auth_params,
                       gchar       **host,
                       gint         *port,
                       gchar       **path,
                       gchar       **query,
                       gchar       **fragment,
                       GError      **error)
{
  g_return_val_if_fail (uri_ref != NULL, FALSE);
  g_return_val_if_fail (error == NULL || *error == NULL, FALSE);

  return g_uri_split_internal (uri_ref, flags,
                               scheme, NULL, user, password, auth_params,
                               host, port, path, query, fragment,
                               error);
}


/**
 * g_uri_split_network:
 * @uri_string: a string containing an absolute URI
 * @flags: flags for parsing @uri_string
 * @scheme: (out) (nullable) (optional) (transfer full): on return, contains
 *    the scheme (converted to lowercase), or %NULL
 * @host: (out) (nullable) (optional) (transfer full): on return, contains the
 *    host, or %NULL
 * @port: (out) (optional) (transfer full): on return, contains the
 *    port, or `-1`
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Parses @uri_string (which must be an [absolute URI][relative-absolute-uris])
 * according to @flags, and returns the pieces relevant to connecting to a host.
 * See the documentation for g_uri_split() for more details; this is
 * mostly a wrapper around that function with simpler arguments.
 * However, it will return an error if @uri_string is a relative URI,
 * or does not contain a hostname component.
 *
 * Returns: (skip): %TRUE if @uri_string parsed successfully,
 *   %FALSE on error.
 *
 * Since: 2.66
 */
gboolean
g_uri_split_network (const gchar  *uri_string,
                     GUriFlags     flags,
                     gchar       **scheme,
                     gchar       **host,
                     gint         *port,
                     GError      **error)
{
  gchar *my_scheme = NULL, *my_host = NULL;

  g_return_val_if_fail (uri_string != NULL, FALSE);
  g_return_val_if_fail (error == NULL || *error == NULL, FALSE);

  if (!g_uri_split_internal (uri_string, flags,
                             &my_scheme, NULL, NULL, NULL, NULL,
                             &my_host, port, NULL, NULL, NULL,
                             error))
    return FALSE;

  if (!my_scheme || !my_host)
    {
      if (!my_scheme)
        {
          g_set_error (error, G_URI_ERROR, G_URI_ERROR_BAD_SCHEME,
                       _("URI ‘%s’ is not an absolute URI"),
                       uri_string);
        }
      else
        {
          g_set_error (error, G_URI_ERROR, G_URI_ERROR_BAD_HOST,
                       _("URI ‘%s’ has no host component"),
                       uri_string);
        }
      g_free (my_scheme);
      g_free (my_host);

      return FALSE;
    }

  if (scheme)
    *scheme = g_steal_pointer (&my_scheme);
  if (host)
    *host = g_steal_pointer (&my_host);

  g_free (my_scheme);
  g_free (my_host);

  return TRUE;
}

/**
 * g_uri_is_valid:
 * @uri_string: a string containing an absolute URI
 * @flags: flags for parsing @uri_string
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Parses @uri_string according to @flags, to determine whether it is a valid
 * [absolute URI][relative-absolute-uris], i.e. it does not need to be resolved
 * relative to another URI using g_uri_parse_relative().
 *
 * If it’s not a valid URI, an error is returned explaining how it’s invalid.
 *
 * See g_uri_split(), and the definition of #GUriFlags, for more
 * information on the effect of @flags.
 *
 * Returns: %TRUE if @uri_string is a valid absolute URI, %FALSE on error.
 *
 * Since: 2.66
 */
gboolean
g_uri_is_valid (const gchar  *uri_string,
                GUriFlags     flags,
                GError      **error)
{
  gchar *my_scheme = NULL;

  g_return_val_if_fail (uri_string != NULL, FALSE);
  g_return_val_if_fail (error == NULL || *error == NULL, FALSE);

  if (!g_uri_split_internal (uri_string, flags,
                             &my_scheme, NULL, NULL, NULL, NULL,
                             NULL, NULL, NULL, NULL, NULL,
                             error))
    return FALSE;

  if (!my_scheme)
    {
      g_set_error (error, G_URI_ERROR, G_URI_ERROR_BAD_SCHEME,
                   _("URI ‘%s’ is not an absolute URI"),
                   uri_string);
      return FALSE;
    }

  g_free (my_scheme);

  return TRUE;
}


/* Implements the "Remove Dot Segments" algorithm from section 5.2.4 of
 * RFC 3986.
 *
 * See https://tools.ietf.org/html/rfc3986#section-5.2.4
 */
static void
remove_dot_segments (gchar *path)
{
  /* The output can be written to the same buffer that the input
   * is read from, as the output pointer is only ever increased
   * when the input pointer is increased as well, and the input
   * pointer is never decreased. */
  gchar *input = path;
  gchar *output = path;

  if (!*path)
    return;

  while (*input)
    {
      /*  A.  If the input buffer begins with a prefix of "../" or "./",
       *      then remove that prefix from the input buffer; otherwise,
       */
      if (strncmp (input, "../", 3) == 0)
        input += 3;
      else if (strncmp (input, "./", 2) == 0)
        input += 2;

      /*  B.  if the input buffer begins with a prefix of "/./" or "/.",
       *      where "." is a complete path segment, then replace that
       *      prefix with "/" in the input buffer; otherwise,
       */
      else if (strncmp (input, "/./", 3) == 0)
        input += 2;
      else if (strcmp (input, "/.") == 0)
        input[1] = '\0';

      /*  C.  if the input buffer begins with a prefix of "/../" or "/..",
       *      where ".." is a complete path segment, then replace that
       *      prefix with "/" in the input buffer and remove the last
       *      segment and its preceding "/" (if any) from the output
       *      buffer; otherwise,
       */
      else if (strncmp (input, "/../", 4) == 0)
        {
          input += 3;
          if (output > path)
            {
              do
                {
                  output--;
                }
              while (*output != '/' && output > path);
            }
        }
      else if (strcmp (input, "/..") == 0)
        {
          input[1] = '\0';
          if (output > path)
            {
              do
                 {
                   output--;
                 }
              while (*output != '/' && output > path);
            }
        }

      /*  D.  if the input buffer consists only of "." or "..", then remove
       *      that from the input buffer; otherwise,
       */
      else if (strcmp (input, "..") == 0 || strcmp (input, ".") == 0)
        input[0] = '\0';

      /*  E.  move the first path segment in the input buffer to the end of
       *      the output buffer, including the initial "/" character (if
       *      any) and any subsequent characters up to, but not including,
       *      the next "/" character or the end of the input buffer.
       */
      else
        {
          *output++ = *input++;
          while (*input && *input != '/')
            *output++ = *input++;
        }
    }
  *output = '\0';
}

/**
 * g_uri_parse:
 * @uri_string: a string representing an absolute URI
 * @flags: flags describing how to parse @uri_string
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Parses @uri_string according to @flags. If the result is not a
 * valid [absolute URI][relative-absolute-uris], it will be discarded, and an
 * error returned.
 *
 * Return value: (transfer full): a new #GUri, or NULL on error.
 *
 * Since: 2.66
 */
GUri *
g_uri_parse (const gchar  *uri_string,
             GUriFlags     flags,
             GError      **error)
{
  g_return_val_if_fail (uri_string != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  return g_uri_parse_relative (NULL, uri_string, flags, error);
}

/**
 * g_uri_parse_relative:
 * @base_uri: (nullable) (transfer none): a base absolute URI
 * @uri_ref: a string representing a relative or absolute URI
 * @flags: flags describing how to parse @uri_ref
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Parses @uri_ref according to @flags and, if it is a
 * [relative URI][relative-absolute-uris], resolves it relative to @base_uri.
 * If the result is not a valid absolute URI, it will be discarded, and an error
 * returned.
 *
 * Return value: (transfer full): a new #GUri, or NULL on error.
 *
 * Since: 2.66
 */
GUri *
g_uri_parse_relative (GUri         *base_uri,
                      const gchar  *uri_ref,
                      GUriFlags     flags,
                      GError      **error)
{
  GUri *uri = NULL;

  g_return_val_if_fail (uri_ref != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);
  g_return_val_if_fail (base_uri == NULL || base_uri->scheme != NULL, NULL);

  /* Use GUri struct to construct the return value: there is no guarantee it is
   * actually correct within the function body. */
  uri = g_atomic_rc_box_new0 (GUri);
  uri->flags = flags;

  if (!g_uri_split_internal (uri_ref, flags,
                             &uri->scheme, &uri->userinfo,
                             &uri->user, &uri->password, &uri->auth_params,
                             &uri->host, &uri->port,
                             &uri->path, &uri->query, &uri->fragment,
                             error))
    {
      g_uri_unref (uri);
      return NULL;
    }

  if (!uri->scheme && !base_uri)
    {
      g_set_error_literal (error, G_URI_ERROR, G_URI_ERROR_FAILED,
                           _("URI is not absolute, and no base URI was provided"));
      g_uri_unref (uri);
      return NULL;
    }

  if (base_uri)
    {
      /* This is section 5.2.2 of RFC 3986, except that we're doing
       * it in place in @uri rather than copying from R to T.
       *
       * See https://tools.ietf.org/html/rfc3986#section-5.2.2
       */
      if (uri->scheme)
        remove_dot_segments (uri->path);
      else
        {
          uri->scheme = g_strdup (base_uri->scheme);
          if (uri->host)
            remove_dot_segments (uri->path);
          else
            {
              if (!*uri->path)
                {
                  g_free (uri->path);
                  uri->path = g_strdup (base_uri->path);
                  if (!uri->query)
                    uri->query = g_strdup (base_uri->query);
                }
              else
                {
                  if (*uri->path == '/')
                    remove_dot_segments (uri->path);
                  else
                    {
                      gchar *newpath, *last;

                      last = strrchr (base_uri->path, '/');
                      if (last)
                        {
                          newpath = g_strdup_printf ("%.*s/%s",
                                                     (gint)(last - base_uri->path),
                                                     base_uri->path,
                                                     uri->path);
                        }
                      else
                        newpath = g_strdup_printf ("/%s", uri->path);

                      g_free (uri->path);
                      uri->path = g_steal_pointer (&newpath);

                      remove_dot_segments (uri->path);
                    }
                }

              uri->userinfo = g_strdup (base_uri->userinfo);
              uri->user = g_strdup (base_uri->user);
              uri->password = g_strdup (base_uri->password);
              uri->auth_params = g_strdup (base_uri->auth_params);
              uri->host = g_strdup (base_uri->host);
              uri->port = base_uri->port;
            }
        }

      /* Scheme normalization couldn't have been done earlier
       * as the relative URI may not have had a scheme */
      if (flags & G_URI_FLAGS_SCHEME_NORMALIZE)
        {
          if (should_normalize_empty_path (uri->scheme) && !*uri->path)
            {
              g_free (uri->path);
              uri->path = g_strdup ("/");
            }

          uri->port = normalize_port (uri->scheme, uri->port);
        }
    }
  else
    {
      remove_dot_segments (uri->path);
    }

  return g_steal_pointer (&uri);
}

/**
 * g_uri_resolve_relative:
 * @base_uri_string: (nullable): a string representing a base URI
 * @uri_ref: a string representing a relative or absolute URI
 * @flags: flags describing how to parse @uri_ref
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Parses @uri_ref according to @flags and, if it is a
 * [relative URI][relative-absolute-uris], resolves it relative to
 * @base_uri_string. If the result is not a valid absolute URI, it will be
 * discarded, and an error returned.
 *
 * (If @base_uri_string is %NULL, this just returns @uri_ref, or
 * %NULL if @uri_ref is invalid or not absolute.)
 *
 * Return value: (transfer full): the resolved URI string,
 * or NULL on error.
 *
 * Since: 2.66
 */
gchar *
g_uri_resolve_relative (const gchar  *base_uri_string,
                        const gchar  *uri_ref,
                        GUriFlags     flags,
                        GError      **error)
{
  GUri *base_uri, *resolved_uri;
  gchar *resolved_uri_string;

  g_return_val_if_fail (uri_ref != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  flags |= G_URI_FLAGS_ENCODED;

  if (base_uri_string)
    {
      base_uri = g_uri_parse (base_uri_string, flags, error);
      if (!base_uri)
        return NULL;
    }
  else
    base_uri = NULL;

  resolved_uri = g_uri_parse_relative (base_uri, uri_ref, flags, error);
  if (base_uri)
    g_uri_unref (base_uri);
  if (!resolved_uri)
    return NULL;

  resolved_uri_string = g_uri_to_string (resolved_uri);
  g_uri_unref (resolved_uri);
  return g_steal_pointer (&resolved_uri_string);
}

/* userinfo as a whole can contain sub-delims + ":", but split-out
 * user can't contain ":" or ";", and split-out password can't contain
 * ";".
 */
#define USERINFO_ALLOWED_CHARS G_URI_RESERVED_CHARS_ALLOWED_IN_USERINFO
#define USER_ALLOWED_CHARS "!$&'()*+,="
#define PASSWORD_ALLOWED_CHARS "!$&'()*+,=:"
#define AUTH_PARAMS_ALLOWED_CHARS USERINFO_ALLOWED_CHARS
#define IP_ADDR_ALLOWED_CHARS ":"
#define HOST_ALLOWED_CHARS G_URI_RESERVED_CHARS_SUBCOMPONENT_DELIMITERS
#define PATH_ALLOWED_CHARS G_URI_RESERVED_CHARS_ALLOWED_IN_PATH
#define QUERY_ALLOWED_CHARS G_URI_RESERVED_CHARS_ALLOWED_IN_PATH "?"
#define FRAGMENT_ALLOWED_CHARS G_URI_RESERVED_CHARS_ALLOWED_IN_PATH "?"

static gchar *
g_uri_join_internal (GUriFlags    flags,
                     const gchar *scheme,
                     gboolean     userinfo,
                     const gchar *user,
                     const gchar *password,
                     const gchar *auth_params,
                     const gchar *host,
                     gint         port,
                     const gchar *path,
                     const gchar *query,
                     const gchar *fragment)
{
  gboolean encoded = (flags & G_URI_FLAGS_ENCODED);
  GString *str;
  char *normalized_scheme = NULL;

  /* Restrictions on path prefixes. See:
   * https://tools.ietf.org/html/rfc3986#section-3
   */
  g_return_val_if_fail (path != NULL, NULL);
  g_return_val_if_fail (host == NULL || (path[0] == '\0' || path[0] == '/'), NULL);
  g_return_val_if_fail (host != NULL || (path[0] != '/' || path[1] != '/'), NULL);

  str = g_string_new (scheme);
  if (scheme)
    g_string_append_c (str, ':');

  if (flags & G_URI_FLAGS_SCHEME_NORMALIZE && scheme && ((host && port != -1) || path[0] == '\0'))
    normalized_scheme = g_ascii_strdown (scheme, -1);

  if (host)
    {
      g_string_append (str, "//");

      if (user)
        {
          if (encoded)
            g_string_append (str, user);
          else
            {
              if (userinfo)
                g_string_append_uri_escaped (str, user, USERINFO_ALLOWED_CHARS, TRUE);
              else
                /* Encode ':' and ';' regardless of whether we have a
                 * password or auth params, since it may be parsed later
                 * under the assumption that it does.
                 */
                g_string_append_uri_escaped (str, user, USER_ALLOWED_CHARS, TRUE);
            }

          if (password)
            {
              g_string_append_c (str, ':');
              if (encoded)
                g_string_append (str, password);
              else
                g_string_append_uri_escaped (str, password,
                                             PASSWORD_ALLOWED_CHARS, TRUE);
            }

          if (auth_params)
            {
              g_string_append_c (str, ';');
              if (encoded)
                g_string_append (str, auth_params);
              else
                g_string_append_uri_escaped (str, auth_params,
                                             AUTH_PARAMS_ALLOWED_CHARS, TRUE);
            }

          g_string_append_c (str, '@');
        }

      if (strchr (host, ':') && g_hostname_is_ip_address (host))
        {
          g_string_append_c (str, '[');
          if (encoded)
            g_string_append (str, host);
          else
            g_string_append_uri_escaped (str, host, IP_ADDR_ALLOWED_CHARS, TRUE);
          g_string_append_c (str, ']');
        }
      else
        {
          if (encoded)
            g_string_append (str, host);
          else
            g_string_append_uri_escaped (str, host, HOST_ALLOWED_CHARS, TRUE);
        }

      if (port != -1 && (!normalized_scheme || normalize_port (normalized_scheme, port) != -1))
        g_string_append_printf (str, ":%d", port);
    }

  if (path[0] == '\0' && normalized_scheme && should_normalize_empty_path (normalized_scheme))
    g_string_append (str, "/");
  else if (encoded || flags & G_URI_FLAGS_ENCODED_PATH)
    g_string_append (str, path);
  else
    g_string_append_uri_escaped (str, path, PATH_ALLOWED_CHARS, TRUE);

  g_free (normalized_scheme);

  if (query)
    {
      g_string_append_c (str, '?');
      if (encoded || flags & G_URI_FLAGS_ENCODED_QUERY)
        g_string_append (str, query);
      else
        g_string_append_uri_escaped (str, query, QUERY_ALLOWED_CHARS, TRUE);
    }
  if (fragment)
    {
      g_string_append_c (str, '#');
      if (encoded || flags & G_URI_FLAGS_ENCODED_FRAGMENT)
        g_string_append (str, fragment);
      else
        g_string_append_uri_escaped (str, fragment, FRAGMENT_ALLOWED_CHARS, TRUE);
    }

  return g_string_free (str, FALSE);
}

/**
 * g_uri_join:
 * @flags: flags describing how to build the URI string
 * @scheme: (nullable): the URI scheme, or %NULL
 * @userinfo: (nullable): the userinfo component, or %NULL
 * @host: (nullable): the host component, or %NULL
 * @port: the port, or `-1`
 * @path: (not nullable): the path component
 * @query: (nullable): the query component, or %NULL
 * @fragment: (nullable): the fragment, or %NULL
 *
 * Joins the given components together according to @flags to create
 * an absolute URI string. @path may not be %NULL (though it may be the empty
 * string).
 *
 * When @host is present, @path must either be empty or begin with a slash (`/`)
 * character. When @host is not present, @path cannot begin with two slash
   characters (`//`). See
 * [RFC 3986, section 3](https://tools.ietf.org/html/rfc3986#section-3).
 *
 * See also g_uri_join_with_user(), which allows specifying the
 * components of the ‘userinfo’ separately.
 *
 * %G_URI_FLAGS_HAS_PASSWORD and %G_URI_FLAGS_HAS_AUTH_PARAMS are ignored if set
 * in @flags.
 *
 * Return value: (not nullable) (transfer full): an absolute URI string
 *
 * Since: 2.66
 */
gchar *
g_uri_join (GUriFlags    flags,
            const gchar *scheme,
            const gchar *userinfo,
            const gchar *host,
            gint         port,
            const gchar *path,
            const gchar *query,
            const gchar *fragment)
{
  g_return_val_if_fail (port >= -1 && port <= 65535, NULL);
  g_return_val_if_fail (path != NULL, NULL);

  return g_uri_join_internal (flags,
                              scheme,
                              TRUE, userinfo, NULL, NULL,
                              host,
                              port,
                              path,
                              query,
                              fragment);
}

/**
 * g_uri_join_with_user:
 * @flags: flags describing how to build the URI string
 * @scheme: (nullable): the URI scheme, or %NULL
 * @user: (nullable): the user component of the userinfo, or %NULL
 * @password: (nullable): the password component of the userinfo, or
 *   %NULL
 * @auth_params: (nullable): the auth params of the userinfo, or
 *   %NULL
 * @host: (nullable): the host component, or %NULL
 * @port: the port, or `-1`
 * @path: (not nullable): the path component
 * @query: (nullable): the query component, or %NULL
 * @fragment: (nullable): the fragment, or %NULL
 *
 * Joins the given components together according to @flags to create
 * an absolute URI string. @path may not be %NULL (though it may be the empty
 * string).
 *
 * In contrast to g_uri_join(), this allows specifying the components
 * of the ‘userinfo’ separately. It otherwise behaves the same.
 *
 * %G_URI_FLAGS_HAS_PASSWORD and %G_URI_FLAGS_HAS_AUTH_PARAMS are ignored if set
 * in @flags.
 *
 * Return value: (not nullable) (transfer full): an absolute URI string
 *
 * Since: 2.66
 */
gchar *
g_uri_join_with_user (GUriFlags    flags,
                      const gchar *scheme,
                      const gchar *user,
                      const gchar *password,
                      const gchar *auth_params,
                      const gchar *host,
                      gint         port,
                      const gchar *path,
                      const gchar *query,
                      const gchar *fragment)
{
  g_return_val_if_fail (port >= -1 && port <= 65535, NULL);
  g_return_val_if_fail (path != NULL, NULL);

  return g_uri_join_internal (flags,
                              scheme,
                              FALSE, user, password, auth_params,
                              host,
                              port,
                              path,
                              query,
                              fragment);
}

/**
 * g_uri_build:
 * @flags: flags describing how to build the #GUri
 * @scheme: (not nullable): the URI scheme
 * @userinfo: (nullable): the userinfo component, or %NULL
 * @host: (nullable): the host component, or %NULL
 * @port: the port, or `-1`
 * @path: (not nullable): the path component
 * @query: (nullable): the query component, or %NULL
 * @fragment: (nullable): the fragment, or %NULL
 *
 * Creates a new #GUri from the given components according to @flags.
 *
 * See also g_uri_build_with_user(), which allows specifying the
 * components of the "userinfo" separately.
 *
 * Return value: (not nullable) (transfer full): a new #GUri
 *
 * Since: 2.66
 */
GUri *
g_uri_build (GUriFlags    flags,
             const gchar *scheme,
             const gchar *userinfo,
             const gchar *host,
             gint         port,
             const gchar *path,
             const gchar *query,
             const gchar *fragment)
{
  GUri *uri;

  g_return_val_if_fail (scheme != NULL, NULL);
  g_return_val_if_fail (port >= -1 && port <= 65535, NULL);
  g_return_val_if_fail (path != NULL, NULL);

  uri = g_atomic_rc_box_new0 (GUri);
  uri->flags = flags;
  uri->scheme = g_ascii_strdown (scheme, -1);
  uri->userinfo = g_strdup (userinfo);
  uri->host = g_strdup (host);
  uri->port = port;
  uri->path = g_strdup (path);
  uri->query = g_strdup (query);
  uri->fragment = g_strdup (fragment);

  return g_steal_pointer (&uri);
}

/**
 * g_uri_build_with_user:
 * @flags: flags describing how to build the #GUri
 * @scheme: (not nullable): the URI scheme
 * @user: (nullable): the user component of the userinfo, or %NULL
 * @password: (nullable): the password component of the userinfo, or %NULL
 * @auth_params: (nullable): the auth params of the userinfo, or %NULL
 * @host: (nullable): the host component, or %NULL
 * @port: the port, or `-1`
 * @path: (not nullable): the path component
 * @query: (nullable): the query component, or %NULL
 * @fragment: (nullable): the fragment, or %NULL
 *
 * Creates a new #GUri from the given components according to @flags
 * (%G_URI_FLAGS_HAS_PASSWORD is added unconditionally). The @flags must be
 * coherent with the passed values, in particular use `%`-encoded values with
 * %G_URI_FLAGS_ENCODED.
 *
 * In contrast to g_uri_build(), this allows specifying the components
 * of the ‘userinfo’ field separately. Note that @user must be non-%NULL
 * if either @password or @auth_params is non-%NULL.
 *
 * Return value: (not nullable) (transfer full): a new #GUri
 *
 * Since: 2.66
 */
GUri *
g_uri_build_with_user (GUriFlags    flags,
                       const gchar *scheme,
                       const gchar *user,
                       const gchar *password,
                       const gchar *auth_params,
                       const gchar *host,
                       gint         port,
                       const gchar *path,
                       const gchar *query,
                       const gchar *fragment)
{
  GUri *uri;
  GString *userinfo;

  g_return_val_if_fail (scheme != NULL, NULL);
  g_return_val_if_fail (password == NULL || user != NULL, NULL);
  g_return_val_if_fail (auth_params == NULL || user != NULL, NULL);
  g_return_val_if_fail (port >= -1 && port <= 65535, NULL);
  g_return_val_if_fail (path != NULL, NULL);

  uri = g_atomic_rc_box_new0 (GUri);
  uri->flags = flags | G_URI_FLAGS_HAS_PASSWORD;
  uri->scheme = g_ascii_strdown (scheme, -1);
  uri->user = g_strdup (user);
  uri->password = g_strdup (password);
  uri->auth_params = g_strdup (auth_params);
  uri->host = g_strdup (host);
  uri->port = port;
  uri->path = g_strdup (path);
  uri->query = g_strdup (query);
  uri->fragment = g_strdup (fragment);

  if (user)
    {
      userinfo = g_string_new (user);
      if (password)
        {
          g_string_append_c (userinfo, ':');
          g_string_append (userinfo, uri->password);
        }
      if (auth_params)
        {
          g_string_append_c (userinfo, ';');
          g_string_append (userinfo, uri->auth_params);
        }
      uri->userinfo = g_string_free (userinfo, FALSE);
    }

  return g_steal_pointer (&uri);
}

/**
 * g_uri_to_string:
 * @uri: a #GUri
 *
 * Returns a string representing @uri.
 *
 * This is not guaranteed to return a string which is identical to the
 * string that @uri was parsed from. However, if the source URI was
 * syntactically correct (according to RFC 3986), and it was parsed
 * with %G_URI_FLAGS_ENCODED, then g_uri_to_string() is guaranteed to return
 * a string which is at least semantically equivalent to the source
 * URI (according to RFC 3986).
 *
 * If @uri might contain sensitive details, such as authentication parameters,
 * or private data in its query string, and the returned string is going to be
 * logged, then consider using g_uri_to_string_partial() to redact parts.
 *
 * Return value: (not nullable) (transfer full): a string representing @uri,
 *     which the caller must free.
 *
 * Since: 2.66
 */
gchar *
g_uri_to_string (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return g_uri_to_string_partial (uri, G_URI_HIDE_NONE);
}

/**
 * g_uri_to_string_partial:
 * @uri: a #GUri
 * @flags: flags describing what parts of @uri to hide
 *
 * Returns a string representing @uri, subject to the options in
 * @flags. See g_uri_to_string() and #GUriHideFlags for more details.
 *
 * Return value: (not nullable) (transfer full): a string representing
 *     @uri, which the caller must free.
 *
 * Since: 2.66
 */
gchar *
g_uri_to_string_partial (GUri          *uri,
                         GUriHideFlags  flags)
{
  gboolean hide_user = (flags & G_URI_HIDE_USERINFO);
  gboolean hide_password = (flags & (G_URI_HIDE_USERINFO | G_URI_HIDE_PASSWORD));
  gboolean hide_auth_params = (flags & (G_URI_HIDE_USERINFO | G_URI_HIDE_AUTH_PARAMS));
  gboolean hide_query = (flags & G_URI_HIDE_QUERY);
  gboolean hide_fragment = (flags & G_URI_HIDE_FRAGMENT);

  g_return_val_if_fail (uri != NULL, NULL);

  if (uri->flags & (G_URI_FLAGS_HAS_PASSWORD | G_URI_FLAGS_HAS_AUTH_PARAMS))
    {
      return g_uri_join_with_user (uri->flags,
                                   uri->scheme,
                                   hide_user ? NULL : uri->user,
                                   hide_password ? NULL : uri->password,
                                   hide_auth_params ? NULL : uri->auth_params,
                                   uri->host,
                                   uri->port,
                                   uri->path,
                                   hide_query ? NULL : uri->query,
                                   hide_fragment ? NULL : uri->fragment);
    }

  return g_uri_join (uri->flags,
                     uri->scheme,
                     hide_user ? NULL : uri->userinfo,
                     uri->host,
                     uri->port,
                     uri->path,
                     hide_query ? NULL : uri->query,
                     hide_fragment ? NULL : uri->fragment);
}

/* This is just a copy of g_str_hash() with g_ascii_toupper() added */
static guint
str_ascii_case_hash (gconstpointer v)
{
  const signed char *p;
  guint32 h = 5381;

  for (p = v; *p != '\0'; p++)
    h = (h << 5) + h + g_ascii_toupper (*p);

  return h;
}

static gboolean
str_ascii_case_equal (gconstpointer v1,
                      gconstpointer v2)
{
  const gchar *string1 = v1;
  const gchar *string2 = v2;

  return g_ascii_strcasecmp (string1, string2) == 0;
}

/**
 * GUriParamsIter:
 *
 * Many URI schemes include one or more attribute/value pairs as part of the URI
 * value. For example `scheme://server/path?query=string&is=there` has two
 * attributes – `query=string` and `is=there` – in its query part.
 *
 * A #GUriParamsIter structure represents an iterator that can be used to
 * iterate over the attribute/value pairs of a URI query string. #GUriParamsIter
 * structures are typically allocated on the stack and then initialized with
 * g_uri_params_iter_init(). See the documentation for g_uri_params_iter_init()
 * for a usage example.
 *
 * Since: 2.66
 */
typedef struct
{
  GUriParamsFlags flags;
  const gchar    *attr;
  const gchar    *end;
  guint8          sep_table[256]; /* 1 = index is a separator; 0 otherwise */
} RealIter;

G_STATIC_ASSERT (sizeof (GUriParamsIter) == sizeof (RealIter));
G_STATIC_ASSERT (G_ALIGNOF (GUriParamsIter) >= G_ALIGNOF (RealIter));

/**
 * g_uri_params_iter_init:
 * @iter: an uninitialized #GUriParamsIter
 * @params: a `%`-encoded string containing `attribute=value`
 *   parameters
 * @length: the length of @params, or `-1` if it is nul-terminated
 * @separators: the separator byte character set between parameters. (usually
 *   `&`, but sometimes `;` or both `&;`). Note that this function works on
 *   bytes not characters, so it can't be used to delimit UTF-8 strings for
 *   anything but ASCII characters. You may pass an empty set, in which case
 *   no splitting will occur.
 * @flags: flags to modify the way the parameters are handled.
 *
 * Initializes an attribute/value pair iterator.
 *
 * The iterator keeps pointers to the @params and @separators arguments, those
 * variables must thus outlive the iterator and not be modified during the
 * iteration.
 *
 * If %G_URI_PARAMS_WWW_FORM is passed in @flags, `+` characters in the param
 * string will be replaced with spaces in the output. For example, `foo=bar+baz`
 * will give attribute `foo` with value `bar baz`. This is commonly used on the
 * web (the `https` and `http` schemes only), but is deprecated in favour of
 * the equivalent of encoding spaces as `%20`.
 *
 * Unlike with g_uri_parse_params(), %G_URI_PARAMS_CASE_INSENSITIVE has no
 * effect if passed to @flags for g_uri_params_iter_init(). The caller is
 * responsible for doing their own case-insensitive comparisons.
 *
 * |[<!-- language="C" -->
 * GUriParamsIter iter;
 * GError *error = NULL;
 * gchar *unowned_attr, *unowned_value;
 *
 * g_uri_params_iter_init (&iter, "foo=bar&baz=bar&Foo=frob&baz=bar2", -1, "&", G_URI_PARAMS_NONE);
 * while (g_uri_params_iter_next (&iter, &unowned_attr, &unowned_value, &error))
 *   {
 *     g_autofree gchar *attr = g_steal_pointer (&unowned_attr);
 *     g_autofree gchar *value = g_steal_pointer (&unowned_value);
 *     // do something with attr and value; this code will be called 4 times
 *     // for the params string in this example: once with attr=foo and value=bar,
 *     // then with baz/bar, then Foo/frob, then baz/bar2.
 *   }
 * if (error)
 *   // handle parsing error
 * ]|
 *
 * Since: 2.66
 */
void
g_uri_params_iter_init (GUriParamsIter *iter,
                        const gchar    *params,
                        gssize          length,
                        const gchar    *separators,
                        GUriParamsFlags flags)
{
  RealIter *ri = (RealIter *)iter;
  const gchar *s;

  g_return_if_fail (iter != NULL);
  g_return_if_fail (length == 0 || params != NULL);
  g_return_if_fail (length >= -1);
  g_return_if_fail (separators != NULL);

  ri->flags = flags;

  if (length == -1)
    ri->end = params + strlen (params);
  else
    ri->end = params + length;

  memset (ri->sep_table, FALSE, sizeof (ri->sep_table));
  for (s = separators; *s != '\0'; ++s)
    ri->sep_table[*(guchar *)s] = TRUE;

  ri->attr = params;
}

/**
 * g_uri_params_iter_next:
 * @iter: an initialized #GUriParamsIter
 * @attribute: (out) (nullable) (optional) (transfer full): on return, contains
 *     the attribute, or %NULL.
 * @value: (out) (nullable) (optional) (transfer full): on return, contains
 *     the value, or %NULL.
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Advances @iter and retrieves the next attribute/value. %FALSE is returned if
 * an error has occurred (in which case @error is set), or if the end of the
 * iteration is reached (in which case @attribute and @value are set to %NULL
 * and the iterator becomes invalid). If %TRUE is returned,
 * g_uri_params_iter_next() may be called again to receive another
 * attribute/value pair.
 *
 * Note that the same @attribute may be returned multiple times, since URIs
 * allow repeated attributes.
 *
 * Returns: %FALSE if the end of the parameters has been reached or an error was
 *     encountered. %TRUE otherwise.
 *
 * Since: 2.66
 */
gboolean
g_uri_params_iter_next (GUriParamsIter *iter,
                        gchar         **attribute,
                        gchar         **value,
                        GError        **error)
{
  RealIter *ri = (RealIter *)iter;
  const gchar *attr_end, *val, *val_end;
  gchar *decoded_attr, *decoded_value;
  gboolean www_form = ri->flags & G_URI_PARAMS_WWW_FORM;
  GUriFlags decode_flags = G_URI_FLAGS_NONE;

  g_return_val_if_fail (iter != NULL, FALSE);
  g_return_val_if_fail (error == NULL || *error == NULL, FALSE);

  /* Pre-clear these in case of failure or finishing. */
  if (attribute)
    *attribute = NULL;
  if (value)
    *value = NULL;

  if (ri->attr >= ri->end)
    return FALSE;

  if (ri->flags & G_URI_PARAMS_PARSE_RELAXED)
    decode_flags |= G_URI_FLAGS_PARSE_RELAXED;

  /* Check if each character in @attr is a separator, by indexing by the
   * character value into the @sep_table, which has value 1 stored at an
   * index if that index is a separator. */
  for (val_end = ri->attr; val_end < ri->end; val_end++)
    if (ri->sep_table[*(guchar *)val_end])
      break;

  attr_end = memchr (ri->attr, '=', val_end - ri->attr);
  if (!attr_end)
    {
      g_set_error_literal (error, G_URI_ERROR, G_URI_ERROR_FAILED,
                           _("Missing ‘=’ and parameter value"));
      return FALSE;
    }
  if (!uri_decode (&decoded_attr, NULL, ri->attr, attr_end - ri->attr,
                   www_form, decode_flags, G_URI_ERROR_FAILED, error))
    {
      return FALSE;
    }

  val = attr_end + 1;
  if (!uri_decode (&decoded_value, NULL, val, val_end - val,
                   www_form, decode_flags, G_URI_ERROR_FAILED, error))
    {
      g_free (decoded_attr);
      return FALSE;
    }

  if (attribute)
    *attribute = g_steal_pointer (&decoded_attr);
  if (value)
    *value = g_steal_pointer (&decoded_value);

  g_free (decoded_attr);
  g_free (decoded_value);

  ri->attr = val_end + 1;
  return TRUE;
}

/**
 * g_uri_parse_params:
 * @params: a `%`-encoded string containing `attribute=value`
 *   parameters
 * @length: the length of @params, or `-1` if it is nul-terminated
 * @separators: the separator byte character set between parameters. (usually
 *   `&`, but sometimes `;` or both `&;`). Note that this function works on
 *   bytes not characters, so it can't be used to delimit UTF-8 strings for
 *   anything but ASCII characters. You may pass an empty set, in which case
 *   no splitting will occur.
 * @flags: flags to modify the way the parameters are handled.
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Many URI schemes include one or more attribute/value pairs as part of the URI
 * value. This method can be used to parse them into a hash table. When an
 * attribute has multiple occurrences, the last value is the final returned
 * value. If you need to handle repeated attributes differently, use
 * #GUriParamsIter.
 *
 * The @params string is assumed to still be `%`-encoded, but the returned
 * values will be fully decoded. (Thus it is possible that the returned values
 * may contain `=` or @separators, if the value was encoded in the input.)
 * Invalid `%`-encoding is treated as with the %G_URI_FLAGS_PARSE_RELAXED
 * rules for g_uri_parse(). (However, if @params is the path or query string
 * from a #GUri that was parsed without %G_URI_FLAGS_PARSE_RELAXED and
 * %G_URI_FLAGS_ENCODED, then you already know that it does not contain any
 * invalid encoding.)
 *
 * %G_URI_PARAMS_WWW_FORM is handled as documented for g_uri_params_iter_init().
 *
 * If %G_URI_PARAMS_CASE_INSENSITIVE is passed to @flags, attributes will be
 * compared case-insensitively, so a params string `attr=123&Attr=456` will only
 * return a single attribute–value pair, `Attr=456`. Case will be preserved in
 * the returned attributes.
 *
 * If @params cannot be parsed (for example, it contains two @separators
 * characters in a row), then @error is set and %NULL is returned.
 *
 * Return value: (transfer full) (element-type utf8 utf8):
 *     A hash table of attribute/value pairs, with both names and values
 *     fully-decoded; or %NULL on error.
 *
 * Since: 2.66
 */
GHashTable *
g_uri_parse_params (const gchar     *params,
                    gssize           length,
                    const gchar     *separators,
                    GUriParamsFlags  flags,
                    GError         **error)
{
  GHashTable *hash;
  GUriParamsIter iter;
  gchar *attribute, *value;
  GError *err = NULL;

  g_return_val_if_fail (length == 0 || params != NULL, NULL);
  g_return_val_if_fail (length >= -1, NULL);
  g_return_val_if_fail (separators != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, FALSE);

  if (flags & G_URI_PARAMS_CASE_INSENSITIVE)
    {
      hash = g_hash_table_new_full (str_ascii_case_hash,
                                    str_ascii_case_equal,
                                    g_free, g_free);
    }
  else
    {
      hash = g_hash_table_new_full (g_str_hash, g_str_equal,
                                    g_free, g_free);
    }

  g_uri_params_iter_init (&iter, params, length, separators, flags);

  while (g_uri_params_iter_next (&iter, &attribute, &value, &err))
    g_hash_table_insert (hash, attribute, value);

  if (err)
    {
      g_propagate_error (error, g_steal_pointer (&err));
      g_hash_table_destroy (hash);
      return NULL;
    }

  return g_steal_pointer (&hash);
}

/**
 * g_uri_get_scheme:
 * @uri: a #GUri
 *
 * Gets @uri's scheme. Note that this will always be all-lowercase,
 * regardless of the string or strings that @uri was created from.
 *
 * Return value: (not nullable): @uri's scheme.
 *
 * Since: 2.66
 */
const gchar *
g_uri_get_scheme (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return uri->scheme;
}

/**
 * g_uri_get_userinfo:
 * @uri: a #GUri
 *
 * Gets @uri's userinfo, which may contain `%`-encoding, depending on
 * the flags with which @uri was created.
 *
 * Return value: (nullable): @uri's userinfo.
 *
 * Since: 2.66
 */
const gchar *
g_uri_get_userinfo (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return uri->userinfo;
}

/**
 * g_uri_get_user:
 * @uri: a #GUri
 *
 * Gets the ‘username’ component of @uri's userinfo, which may contain
 * `%`-encoding, depending on the flags with which @uri was created.
 * If @uri was not created with %G_URI_FLAGS_HAS_PASSWORD or
 * %G_URI_FLAGS_HAS_AUTH_PARAMS, this is the same as g_uri_get_userinfo().
 *
 * Return value: (nullable): @uri's user.
 *
 * Since: 2.66
 */
const gchar *
g_uri_get_user (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return uri->user;
}

/**
 * g_uri_get_password:
 * @uri: a #GUri
 *
 * Gets @uri's password, which may contain `%`-encoding, depending on
 * the flags with which @uri was created. (If @uri was not created
 * with %G_URI_FLAGS_HAS_PASSWORD then this will be %NULL.)
 *
 * Return value: (nullable): @uri's password.
 *
 * Since: 2.66
 */
const gchar *
g_uri_get_password (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return uri->password;
}

/**
 * g_uri_get_auth_params:
 * @uri: a #GUri
 *
 * Gets @uri's authentication parameters, which may contain
 * `%`-encoding, depending on the flags with which @uri was created.
 * (If @uri was not created with %G_URI_FLAGS_HAS_AUTH_PARAMS then this will
 * be %NULL.)
 *
 * Depending on the URI scheme, g_uri_parse_params() may be useful for
 * further parsing this information.
 *
 * Return value: (nullable): @uri's authentication parameters.
 *
 * Since: 2.66
 */
const gchar *
g_uri_get_auth_params (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return uri->auth_params;
}

/**
 * g_uri_get_host:
 * @uri: a #GUri
 *
 * Gets @uri's host. This will never have `%`-encoded characters,
 * unless it is non-UTF-8 (which can only be the case if @uri was
 * created with %G_URI_FLAGS_NON_DNS).
 *
 * If @uri contained an IPv6 address literal, this value will be just
 * that address, without the brackets around it that are necessary in
 * the string form of the URI. Note that in this case there may also
 * be a scope ID attached to the address. Eg, `fe80::1234%``em1` (or
 * `fe80::1234%``25em1` if the string is still encoded).
 *
 * Return value: (nullable): @uri's host.
 *
 * Since: 2.66
 */
const gchar *
g_uri_get_host (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return uri->host;
}

/**
 * g_uri_get_port:
 * @uri: a #GUri
 *
 * Gets @uri's port.
 *
 * Return value: @uri's port, or `-1` if no port was specified.
 *
 * Since: 2.66
 */
gint
g_uri_get_port (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, -1);

  if (uri->port == -1 && uri->flags & G_URI_FLAGS_SCHEME_NORMALIZE)
    return default_scheme_port (uri->scheme);

  return uri->port;
}

/**
 * g_uri_get_path:
 * @uri: a #GUri
 *
 * Gets @uri's path, which may contain `%`-encoding, depending on the
 * flags with which @uri was created.
 *
 * Return value: (not nullable): @uri's path.
 *
 * Since: 2.66
 */
const gchar *
g_uri_get_path (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return uri->path;
}

/**
 * g_uri_get_query:
 * @uri: a #GUri
 *
 * Gets @uri's query, which may contain `%`-encoding, depending on the
 * flags with which @uri was created.
 *
 * For queries consisting of a series of `name=value` parameters,
 * #GUriParamsIter or g_uri_parse_params() may be useful.
 *
 * Return value: (nullable): @uri's query.
 *
 * Since: 2.66
 */
const gchar *
g_uri_get_query (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return uri->query;
}

/**
 * g_uri_get_fragment:
 * @uri: a #GUri
 *
 * Gets @uri's fragment, which may contain `%`-encoding, depending on
 * the flags with which @uri was created.
 *
 * Return value: (nullable): @uri's fragment.
 *
 * Since: 2.66
 */
const gchar *
g_uri_get_fragment (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, NULL);

  return uri->fragment;
}


/**
 * g_uri_get_flags:
 * @uri: a #GUri
 *
 * Gets @uri's flags set upon construction.
 *
 * Return value: @uri's flags.
 *
 * Since: 2.66
 **/
GUriFlags
g_uri_get_flags (GUri *uri)
{
  g_return_val_if_fail (uri != NULL, G_URI_FLAGS_NONE);

  return uri->flags;
}

/**
 * g_uri_unescape_segment:
 * @escaped_string: (nullable): A string, may be %NULL
 * @escaped_string_end: (nullable): Pointer to end of @escaped_string,
 *   may be %NULL
 * @illegal_characters: (nullable): An optional string of illegal
 *   characters not to be allowed, may be %NULL
 *
 * Unescapes a segment of an escaped string.
 *
 * If any of the characters in @illegal_characters or the NUL
 * character appears as an escaped character in @escaped_string, then
 * that is an error and %NULL will be returned. This is useful if you
 * want to avoid for instance having a slash being expanded in an
 * escaped path element, which might confuse pathname handling.
 *
 * Note: `NUL` byte is not accepted in the output, in contrast to
 * g_uri_unescape_bytes().
 *
 * Returns: (nullable): an unescaped version of @escaped_string,
 * or %NULL on error. The returned string should be freed when no longer
 * needed.  As a special case if %NULL is given for @escaped_string, this
 * function will return %NULL.
 *
 * Since: 2.16
 **/
gchar *
g_uri_unescape_segment (const gchar *escaped_string,
                        const gchar *escaped_string_end,
                        const gchar *illegal_characters)
{
  gchar *unescaped;
  gsize length;
  gssize decoded_len;

  if (!escaped_string)
    return NULL;

  if (escaped_string_end)
    length = escaped_string_end - escaped_string;
  else
    length = strlen (escaped_string);

  decoded_len = uri_decoder (&unescaped,
                             illegal_characters,
                             escaped_string, length,
                             FALSE, FALSE,
                             G_URI_FLAGS_ENCODED,
                             0, NULL);
  if (decoded_len < 0)
    return NULL;

  if (memchr (unescaped, '\0', decoded_len))
    {
      g_free (unescaped);
      return NULL;
    }

  return unescaped;
}

/**
 * g_uri_unescape_string:
 * @escaped_string: an escaped string to be unescaped.
 * @illegal_characters: (nullable): a string of illegal characters
 *   not to be allowed, or %NULL.
 *
 * Unescapes a whole escaped string.
 *
 * If any of the characters in @illegal_characters or the NUL
 * character appears as an escaped character in @escaped_string, then
 * that is an error and %NULL will be returned. This is useful if you
 * want to avoid for instance having a slash being expanded in an
 * escaped path element, which might confuse pathname handling.
 *
 * Returns: (nullable): an unescaped version of @escaped_string.
 * The returned string should be freed when no longer needed.
 *
 * Since: 2.16
 **/
gchar *
g_uri_unescape_string (const gchar *escaped_string,
                       const gchar *illegal_characters)
{
  return g_uri_unescape_segment (escaped_string, NULL, illegal_characters);
}

/**
 * g_uri_escape_string:
 * @unescaped: the unescaped input string.
 * @reserved_chars_allowed: (nullable): a string of reserved
 *   characters that are allowed to be used, or %NULL.
 * @allow_utf8: %TRUE if the result can include UTF-8 characters.
 *
 * Escapes a string for use in a URI.
 *
 * Normally all characters that are not "unreserved" (i.e. ASCII
 * alphanumerical characters plus dash, dot, underscore and tilde) are
 * escaped. But if you specify characters in @reserved_chars_allowed
 * they are not escaped. This is useful for the "reserved" characters
 * in the URI specification, since those are allowed unescaped in some
 * portions of a URI.
 *
 * Returns: (not nullable): an escaped version of @unescaped. The
 * returned string should be freed when no longer needed.
 *
 * Since: 2.16
 **/
gchar *
g_uri_escape_string (const gchar *unescaped,
                     const gchar *reserved_chars_allowed,
                     gboolean     allow_utf8)
{
  GString *s;

  g_return_val_if_fail (unescaped != NULL, NULL);

  s = g_string_sized_new (strlen (unescaped) * 1.25);

  g_string_append_uri_escaped (s, unescaped, reserved_chars_allowed, allow_utf8);

  return g_string_free (s, FALSE);
}

/**
 * g_uri_unescape_bytes:
 * @escaped_string: A URI-escaped string
 * @length: the length (in bytes) of @escaped_string to escape, or `-1` if it
 *   is nul-terminated.
 * @illegal_characters: (nullable): a string of illegal characters
 *   not to be allowed, or %NULL.
 * @error: #GError for error reporting, or %NULL to ignore.
 *
 * Unescapes a segment of an escaped string as binary data.
 *
 * Note that in contrast to g_uri_unescape_string(), this does allow
 * nul bytes to appear in the output.
 *
 * If any of the characters in @illegal_characters appears as an escaped
 * character in @escaped_string, then that is an error and %NULL will be
 * returned. This is useful if you want to avoid for instance having a slash
 * being expanded in an escaped path element, which might confuse pathname
 * handling.
 *
 * Returns: (transfer full): an unescaped version of @escaped_string
 *     or %NULL on error (if decoding failed, using %G_URI_ERROR_FAILED error
 *     code). The returned #GBytes should be unreffed when no longer needed.
 *
 * Since: 2.66
 **/
GBytes *
g_uri_unescape_bytes (const gchar *escaped_string,
                      gssize       length,
                      const char *illegal_characters,
                      GError     **error)
{
  gchar *buf;
  gssize unescaped_length;

  g_return_val_if_fail (escaped_string != NULL, NULL);
  g_return_val_if_fail (error == NULL || *error == NULL, NULL);

  if (length == -1)
    length = strlen (escaped_string);

  unescaped_length = uri_decoder (&buf,
                                  illegal_characters,
                                  escaped_string, length,
                                  FALSE,
                                  FALSE,
                                  G_URI_FLAGS_ENCODED,
                                  G_URI_ERROR_FAILED, error);
  if (unescaped_length == -1)
    return NULL;

  return g_bytes_new_take (buf, unescaped_length);
}

/**
 * g_uri_escape_bytes:
 * @unescaped: (array length=length): the unescaped input data.
 * @length: the length of @unescaped
 * @reserved_chars_allowed: (nullable): a string of reserved
 *   characters that are allowed to be used, or %NULL.
 *
 * Escapes arbitrary data for use in a URI.
 *
 * Normally all characters that are not ‘unreserved’ (i.e. ASCII
 * alphanumerical characters plus dash, dot, underscore and tilde) are
 * escaped. But if you specify characters in @reserved_chars_allowed
 * they are not escaped. This is useful for the ‘reserved’ characters
 * in the URI specification, since those are allowed unescaped in some
 * portions of a URI.
 *
 * Though technically incorrect, this will also allow escaping nul
 * bytes as `%``00`.
 *
 * Returns: (not nullable) (transfer full): an escaped version of @unescaped.
 *     The returned string should be freed when no longer needed.
 *
 * Since: 2.66
 */
gchar *
g_uri_escape_bytes (const guint8 *unescaped,
                    gsize         length,
                    const gchar  *reserved_chars_allowed)
{
  GString *string;

  g_return_val_if_fail (unescaped != NULL, NULL);

  string = g_string_sized_new (length * 1.25);

  _uri_encoder (string, unescaped, length,
               reserved_chars_allowed, FALSE);

  return g_string_free (string, FALSE);
}

static gssize
g_uri_scheme_length (const gchar *uri)
{
  const gchar *p;

  p = uri;
  if (!g_ascii_isalpha (*p))
    return -1;
  p++;
  while (g_ascii_isalnum (*p) || *p == '.' || *p == '+' || *p == '-')
    p++;

  if (p > uri && *p == ':')
    return p - uri;

  return -1;
}

/**
 * g_uri_parse_scheme:
 * @uri: a valid URI.
 *
 * Gets the scheme portion of a URI string.
 * [RFC 3986](https://tools.ietf.org/html/rfc3986#section-3) decodes the scheme
 * as:
 * |[
 * URI = scheme ":" hier-part [ "?" query ] [ "#" fragment ]
 * ]|
 * Common schemes include `file`, `https`, `svn+ssh`, etc.
 *
 * Returns: (transfer full) (nullable): The ‘scheme’ component of the URI, or
 *     %NULL on error. The returned string should be freed when no longer needed.
 *
 * Since: 2.16
 **/
gchar *
g_uri_parse_scheme (const gchar *uri)
{
  gssize len;

  g_return_val_if_fail (uri != NULL, NULL);

  len = g_uri_scheme_length (uri);
  return len == -1 ? NULL : g_strndup (uri, len);
}

/**
 * g_uri_peek_scheme:
 * @uri: a valid URI.
 *
 * Gets the scheme portion of a URI string.
 * [RFC 3986](https://tools.ietf.org/html/rfc3986#section-3) decodes the scheme
 * as:
 * |[
 * URI = scheme ":" hier-part [ "?" query ] [ "#" fragment ]
 * ]|
 * Common schemes include `file`, `https`, `svn+ssh`, etc.
 *
 * Unlike g_uri_parse_scheme(), the returned scheme is normalized to
 * all-lowercase and does not need to be freed.
 *
 * Returns: (transfer none) (nullable): The ‘scheme’ component of the URI, or
 *     %NULL on error. The returned string is normalized to all-lowercase, and
 *     interned via g_intern_string(), so it does not need to be freed.
 *
 * Since: 2.66
 **/
const gchar *
g_uri_peek_scheme (const gchar *uri)
{
  gssize len;
  gchar *lower_scheme;
  const gchar *scheme;

  g_return_val_if_fail (uri != NULL, NULL);

  len = g_uri_scheme_length (uri);
  if (len == -1)
    return NULL;

  lower_scheme = g_ascii_strdown (uri, len);
  scheme = g_intern_string (lower_scheme);
  g_free (lower_scheme);

  return scheme;
}

G_DEFINE_QUARK (g-uri-quark, g_uri_error)
