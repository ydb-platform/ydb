/* xmalloc.c -- malloc with out of memory checking

   Copyright (C) 1990-2000, 2002-2006, 2008-2020 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

#include <config.h>

#define XALLOC_INLINE _GL_EXTERN_INLINE

#include "xalloc.h"

#include <stdlib.h>
#include <string.h>

/* 1 if calloc, malloc and realloc are known to be compatible with GNU.
   This matters if we are not also using the calloc-gnu, malloc-gnu
   and realloc-gnu modules, which define HAVE_CALLOC_GNU,
   HAVE_MALLOC_GNU and HAVE_REALLOC_GNU and support the GNU API even
   on non-GNU platforms.  */
#if defined HAVE_CALLOC_GNU || (defined __GLIBC__ && !defined __UCLIBC__)
enum { HAVE_GNU_CALLOC = 1 };
#else
enum { HAVE_GNU_CALLOC = 0 };
#endif
#if defined HAVE_MALLOC_GNU || (defined __GLIBC__ && !defined __UCLIBC__)
enum { HAVE_GNU_MALLOC = 1 };
#else
enum { HAVE_GNU_MALLOC = 0 };
#endif
#if defined HAVE_REALLOC_GNU || (defined __GLIBC__ && !defined __UCLIBC__)
enum { HAVE_GNU_REALLOC = 1 };
#else
enum { HAVE_GNU_REALLOC = 0 };
#endif

/* Allocate N bytes of memory dynamically, with error checking.  */

void *
xmalloc (size_t n)
{
  void *p = malloc (n);
  if (!p && (HAVE_GNU_MALLOC || n))
    xalloc_die ();
  return p;
}

/* Change the size of an allocated block of memory P to N bytes,
   with error checking.  */

void *
xrealloc (void *p, size_t n)
{
  if (!HAVE_GNU_REALLOC && !n && p)
    {
      /* The GNU and C99 realloc behaviors disagree here.  Act like GNU.  */
      free (p);
      return NULL;
    }

  void *r = realloc (p, n);
  if (!r && (n || (HAVE_GNU_REALLOC && !p)))
    xalloc_die ();
  return r;
}

/* If P is null, allocate a block of at least *PN bytes; otherwise,
   reallocate P so that it contains more than *PN bytes.  *PN must be
   nonzero unless P is null.  Set *PN to the new block's size, and
   return the pointer to the new block.  *PN is never set to zero, and
   the returned pointer is never null.  */

void *
x2realloc (void *p, size_t *pn)
{
  return x2nrealloc (p, pn, 1);
}

/* Allocate N bytes of zeroed memory dynamically, with error checking.
   There's no need for xnzalloc (N, S), since it would be equivalent
   to xcalloc (N, S).  */

void *
xzalloc (size_t n)
{
  return xcalloc (n, 1);
}

/* Allocate zeroed memory for N elements of S bytes, with error
   checking.  S must be nonzero.  */

void *
xcalloc (size_t n, size_t s)
{
  void *p;
  /* Test for overflow, since objects with size greater than
     PTRDIFF_MAX cause pointer subtraction to go awry.  Omit size-zero
     tests if HAVE_GNU_CALLOC, since GNU calloc never returns NULL if
     successful.  */
  if (xalloc_oversized (n, s)
      || (! (p = calloc (n, s)) && (HAVE_GNU_CALLOC || n != 0)))
    xalloc_die ();
  return p;
}

/* Clone an object P of size S, with error checking.  There's no need
   for xnmemdup (P, N, S), since xmemdup (P, N * S) works without any
   need for an arithmetic overflow check.  */

void *
xmemdup (void const *p, size_t s)
{
  return memcpy (xmalloc (s), p, s);
}

/* Clone STRING.  */

char *
xstrdup (char const *string)
{
  return xmemdup (string, strlen (string) + 1);
}
