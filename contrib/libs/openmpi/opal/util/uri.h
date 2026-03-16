/*
 * Copyright (c) 2012      Los Alamos National Security, LLC.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 * @file
 */

/*
  Per RFC3986:
  The generic URI syntax consists of a hierarchical sequence of
   components referred to as the scheme, authority, path, query, and
   fragment.

      URI         = scheme ":" hier-part [ "?" query ] [ "#" fragment ]

      hier-part   = "//" authority path-abempty
                  / path-absolute
                  / path-rootless
                  / path-empty

   The scheme and path components are required, though the path may be
   empty (no characters).  When authority is present, the path must
   either be empty or begin with a slash ("/") character.  When
   authority is not present, the path cannot begin with two slash
   characters ("//").  These restrictions result in five different ABNF
   rules for a path (Section 3.3), only one of which will match any
   given URI reference.

   The following are two example URIs and their component parts:

         foo://example.com:8042/over/there?name=ferret#nose
         \_/   \______________/\_________/ \_________/ \__/
          |           |            |            |        |
       scheme     authority       path        query   fragment
          |   _____________________|__
         / \ /                        \
         urn:example:animal:ferret:nose

   Initially, this code supports only part of the first example - a scheme
   followed by an authority and a path. Queries and fragments are
   not supported. The APIs are modeled on the Gnome equivalent functions,
   though the code is NOT in any way based on thoat code.
*/

#ifndef OPAL_URI_H
#define OPAL_URI_H

#include "opal_config.h"

#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

BEGIN_C_DECLS

/**
 * Parse a uri to retrieve the scheme
 *
 * The caller is responsible for freeing the returned string.
 */
OPAL_DECLSPEC char *opal_uri_get_scheme(const char *uri) __opal_attribute_malloc__ __opal_attribute_warn_unused_result__;

/**
 *  Create a uri from a hostname and filename
 *
 * The caller is responsible for freeing the returned string.
 */
OPAL_DECLSPEC char *opal_filename_to_uri(const char *filename,
                                         const char *hostname) __opal_attribute_malloc__ __opal_attribute_warn_unused_result__;
/**
 * Extract the filename (and hostname) from a uri
 *
 * @param uri : a uri describing a filename (escaped, encoded in ASCII).
 * @param hostname : Location to store hostname for the URI, or NULL.
 *                   If there is no hostname in the URI, NULL will be
 *                   stored in this location.
 * @retval a newly-allocated string holding the resulting filename, or NULL on an error.
 *
 * The caller is responsible for freeing the returned string.
 */
OPAL_DECLSPEC char *opal_filename_from_uri(const char *uri,
                                           char **hostname) __opal_attribute_malloc__ __opal_attribute_warn_unused_result__;

END_C_DECLS
#endif /* OPAL_URI_H */
