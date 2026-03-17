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
 *
 * This file is only here because some platforms have a broken strncpy
 * (e.g., Itanium with RedHat Advanced Server glibc).
 */

#include "opal_config.h"

#include <string.h>

#include "opal/util/strncpy.h"


/**
 * Provide a portable, working strncpy() for platforms that have
 * broken implementations.
 *
 * @param dest Destination string.
 * @param src Source string.
 * @param len Length of the string to copy.
 *
 * @return The value dest.
 *
 * This function is identical in behavior to strncpy(), but is not
 * optimized at all (we're not concerned with high-performance
 * strncpy!).  It is only here because some platforms have broken
 * implementations of strncpy() that cause segfaults (cough cough Red
 * Hat Advanced Server glibc cough cough).
 */
char *
opal_strncpy(char *dest, const char *src, size_t len)
{
  size_t i;
  int pad = 0;
  char *new_dest = dest;

  for (i = 0; i < len; ++i, ++src, ++new_dest) {
    if (pad != 0)
      *new_dest = '\0';
    else {
      *new_dest = *src;
      if ('\0' == *src)
        pad = 1;
    }
  }

  return dest;
}
