/* FriBidi
 * fribidi-deprecated.c - deprecated interfaces.
 *
 * Authors:
 *   Behdad Esfahbod, 2001, 2002, 2004
 *   Dov Grobgeld, 1999, 2000
 *
 * Copyright (C) 2004 Sharif FarsiWeb, Inc
 * Copyright (C) 2001,2002 Behdad Esfahbod
 * Copyright (C) 1999,2000 Dov Grobgeld
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
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library, in a file named COPYING; if not, write to the
 * Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA 02110-1301, USA
 * 
 * For licensing issues, contact <fribidi.license@gmail.com>.
 */

#include "common.h"

#undef FRIBIDI_NO_DEPRECATED

#include <fribidi-deprecated.h>
#include <fribidi.h>

#ifdef FRIBIDI_NO_DEPRECATED
#else

static FriBidiFlags flags = FRIBIDI_FLAGS_DEFAULT | FRIBIDI_FLAGS_ARABIC;

FRIBIDI_ENTRY fribidi_boolean
fribidi_set_mirroring (
  /* input */
  fribidi_boolean state
)
{
  return FRIBIDI_ADJUST_AND_TEST_BITS (flags, FRIBIDI_FLAG_SHAPE_MIRRORING, state);
}

FRIBIDI_ENTRY fribidi_boolean
fribidi_mirroring_status (
  void
)
{
  return FRIBIDI_TEST_BITS (flags, FRIBIDI_FLAG_SHAPE_MIRRORING);
}

FRIBIDI_ENTRY fribidi_boolean
fribidi_set_reorder_nsm (
  /* input */
  fribidi_boolean state
)
{
  return FRIBIDI_ADJUST_AND_TEST_BITS (flags, FRIBIDI_FLAG_REORDER_NSM, state);
}

fribidi_boolean
fribidi_reorder_nsm_status (
  void
)
{
  return FRIBIDI_TEST_BITS (flags, FRIBIDI_FLAG_REORDER_NSM);
}




FRIBIDI_ENTRY FriBidiLevel
fribidi_log2vis_get_embedding_levels (
  const FriBidiCharType *bidi_types,	/* input list of bidi types as returned by
					   fribidi_get_bidi_types() */
  const FriBidiStrIndex len,	/* input string length of the paragraph */
  FriBidiParType *pbase_dir,	/* requested and resolved paragraph
				 * base direction */
  FriBidiLevel *embedding_levels	/* output list of embedding levels */
)
{
  return fribidi_get_par_embedding_levels_ex (bidi_types, NULL, len, pbase_dir, embedding_levels);
}

FRIBIDI_ENTRY FriBidiCharType
fribidi_get_type (
  FriBidiChar ch		/* input character */
)
{
  return fribidi_get_bidi_type (ch);
}

FRIBIDI_ENTRY FriBidiCharType
fribidi_get_type_internal (
  FriBidiChar ch		/* input character */
)
{
  return fribidi_get_bidi_type (ch);
}


FRIBIDI_ENTRY FriBidiLevel
fribidi_get_par_embedding_levels (
  /* input */
  const FriBidiCharType *bidi_types,
  const FriBidiStrIndex len,
  /* input and output */
  FriBidiParType *pbase_dir,
  /* output */
  FriBidiLevel *embedding_levels
)
{
  return fribidi_get_par_embedding_levels_ex (/* input */
                                              bidi_types,
                                              NULL, /* No bracket_types */
                                              len,
                                              /* input and output */
                                              pbase_dir,
                                              /* output */
                                              embedding_levels);
}

#endif /* !FRIBIDI_NO_DEPRECATED */

/* Editor directions:
 * vim:textwidth=78:tabstop=8:shiftwidth=2:autoindent:cindent
 */
