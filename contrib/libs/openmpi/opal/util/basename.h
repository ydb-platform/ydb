/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/**
 * @file
 *
 * Returns an OS-independant basename() of a given filename.
 */

#ifndef OPAL_BASENAME_H
#define OPAL_BASENAME_H

#include "opal_config.h"

BEGIN_C_DECLS

/**
 * Return the basename of a filename.
 *
 * @param filename The filename to examine
 *
 * @returns A string containing the basename, or NULL if there is an error
 *
 * The contents of the \em filename parameter are unchanged.  This
 * function returns a new string containing the basename of the
 * filename (which must be eventually freed by the caller), or
 * NULL if there is an error.  Trailing "/" characters in the
 * filename do not count, unless it is in the only part of the
 * filename (e.g., "/" or "C:\").
 *
 * This function will do the Right Things on POSIX and
 * Windows-based operating systems.  For example:
 *
 * foo.txt returns "foo.txt"
 *
 * /foo/bar/baz returns "baz"
 *
 * /yow.c returns "yow.c"
 *
 * / returns "/"
 *
 * C:\foo\bar\baz returns "baz"
 *
 * D:foo.txt returns "foo.txt"
 *
 * E:\yow.c returns "yow.c"
 *
 * F: returns "F:"
 *
 * G:\ returns "G:"
 *
 * The caller is responsible for freeing the returned string.
 */
OPAL_DECLSPEC char *opal_basename(const char* filename) __opal_attribute_malloc__ __opal_attribute_warn_unused_result__;

/**
 * Return the dirname of a filename.
 *
 * @param filename The filename to examine
 *
 * @returns A string containing the dirname, or NULL if there is an error
 *
 * The contents of the \em filename parameter are unchanged.  This
 * function returns a new string containing the dirname of the
 * filename (which must be eventually freed by the caller), or
 * NULL if there is an error.  Trailing "/" characters in the
 * filename do not count, unless it is in the only part of the
 * filename (e.g., "/" or "C:\").
 *
 * This function will do the Right Things on POSIX and
 * Windows-based operating systems.  For example:
 *
 * foo.txt returns "foo.txt"
 *
 * /foo/bar/baz returns "/foo/bar"
 *
 * /yow.c returns "/"
 *
 * / returns ""
 *
 * C:\foo\bar\baz returns "C:\foo\bar"
 *
 * D:foo.txt returns "D:"
 *
 * E:\yow.c returns "E:"
 *
 * F: returns ""
 *
 * G:\ returns ""
 *
 * The caller is responsible for freeing the returned string.
 */
OPAL_DECLSPEC char *opal_dirname(const char* filename) __opal_attribute_malloc__ __opal_attribute_warn_unused_result__;

END_C_DECLS

#endif /* OPAL_BASENAME_H */
