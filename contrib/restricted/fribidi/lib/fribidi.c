/* FriBidi
 * fribidi.c - Unicode bidirectional and Arabic joining/shaping algorithms
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

#include <fribidi.h>

#ifdef DEBUG
static int flag_debug = false;
#endif

FRIBIDI_ENTRY fribidi_boolean
fribidi_debug_status (
  void
)
{
#ifdef DEBUG
  return flag_debug;
#else
  return false;
#endif
}

FRIBIDI_ENTRY fribidi_boolean
fribidi_set_debug (
  /* input */
  fribidi_boolean state
)
{
#ifdef DEBUG
  return flag_debug = state;
#else
  return false;
#endif
}

FRIBIDI_ENTRY FriBidiStrIndex
fribidi_remove_bidi_marks (
  FriBidiChar *str,
  const FriBidiStrIndex len,
  FriBidiStrIndex *positions_to_this,
  FriBidiStrIndex *position_from_this_list,
  FriBidiLevel *embedding_levels
)
{
  register FriBidiStrIndex i, j = 0;
  fribidi_boolean private_from_this = false;
  fribidi_boolean status = false;

  if UNLIKELY
    (len == 0 || str == NULL)
    {
      status = true;
      goto out;
    }

  DBG ("in fribidi_remove_bidi_marks");

  fribidi_assert (str);

  /* If to_this is not NULL, we must have from_this as well. If it is
     not given by the caller, we have to make a private instance of it. */
  if (positions_to_this && !position_from_this_list)
    {
      position_from_this_list = fribidi_malloc (sizeof
						(position_from_this_list[0]) *
						len);
      if UNLIKELY
	(!position_from_this_list) goto out;
      private_from_this = true;
      for (i = 0; i < len; i++)
	position_from_this_list[positions_to_this[i]] = i;
    }

  for (i = 0; i < len; i++)
    if (!FRIBIDI_IS_EXPLICIT_OR_BN (fribidi_get_bidi_type (str[i]))
        && !FRIBIDI_IS_ISOLATE (fribidi_get_bidi_type (str[i]))
	&& str[i] != FRIBIDI_CHAR_LRM && str[i] != FRIBIDI_CHAR_RLM)
      {
	str[j] = str[i];
	if (embedding_levels)
	  embedding_levels[j] = embedding_levels[i];
	if (position_from_this_list)
	  position_from_this_list[j] = position_from_this_list[i];
	j++;
      }

  /* Convert the from_this list to to_this */
  if (positions_to_this)
    {
      for (i = 0; i < len; i++)
	positions_to_this[i] = -1;
      for (i = 0; i < len; i++)
	positions_to_this[position_from_this_list[i]] = i;
    }

  status = true;

out:

  if (private_from_this)
    fribidi_free (position_from_this_list);

  return status ? j : -1;
}

/* Local array size, used for stack-based local arrays */
#define LOCAL_LIST_SIZE 128
static FriBidiFlags flags = FRIBIDI_FLAGS_DEFAULT | FRIBIDI_FLAGS_ARABIC;


FRIBIDI_ENTRY FriBidiLevel
fribidi_log2vis (
  /* input */
  const FriBidiChar *str,
  const FriBidiStrIndex len,
  /* input and output */
  FriBidiParType *pbase_dir,
  /* output */
  FriBidiChar *visual_str,
  FriBidiStrIndex *positions_L_to_V,
  FriBidiStrIndex *positions_V_to_L,
  FriBidiLevel *embedding_levels
)
{
  register FriBidiStrIndex i;
  FriBidiLevel max_level = 0;
  fribidi_boolean private_V_to_L = false;
  fribidi_boolean private_embedding_levels = false;
  fribidi_boolean status = false;
  FriBidiArabicProp local_ar_props[LOCAL_LIST_SIZE];
  FriBidiArabicProp *ar_props = NULL;
  FriBidiLevel local_embedding_levels[LOCAL_LIST_SIZE];
  FriBidiCharType local_bidi_types[LOCAL_LIST_SIZE];
  FriBidiCharType *bidi_types = NULL;
  FriBidiBracketType local_bracket_types[LOCAL_LIST_SIZE];
  FriBidiBracketType *bracket_types = NULL;
  FriBidiStrIndex local_positions_V_to_L[LOCAL_LIST_SIZE];

  if UNLIKELY
    (len == 0)
    {
      status = true;
      goto out;
    }

  DBG ("in fribidi_log2vis");

  fribidi_assert (str);
  fribidi_assert (pbase_dir);

  if (len < LOCAL_LIST_SIZE)
    bidi_types = local_bidi_types;
  else
    bidi_types = fribidi_malloc (len * sizeof bidi_types[0]);
  if (!bidi_types)
    goto out;

  fribidi_get_bidi_types (str, len, bidi_types);

  if (len < LOCAL_LIST_SIZE)
    bracket_types = local_bracket_types;
  else
    bracket_types = fribidi_malloc (len * sizeof bracket_types[0]);
    
  if (!bracket_types)
    goto out;

  fribidi_get_bracket_types (str, len, bidi_types,
                             /* output */
                             bracket_types);
  if (!embedding_levels)
    {
      if (len < LOCAL_LIST_SIZE)
        embedding_levels = local_embedding_levels;
      else
        embedding_levels = fribidi_malloc (len * sizeof embedding_levels[0]);
      if (!embedding_levels)
	goto out;
      private_embedding_levels = true;
    }

  max_level = fribidi_get_par_embedding_levels_ex (bidi_types,
                                                   bracket_types,
                                                   len,
                                                   pbase_dir,
                                                   embedding_levels) - 1;
  if UNLIKELY
    (max_level < 0) goto out;

  /* If l2v is to be calculated we must have v2l as well. If it is not
     given by the caller, we have to make a private instance of it. */
  if (positions_L_to_V && !positions_V_to_L)
    {
      if (len < LOCAL_LIST_SIZE)
        positions_V_to_L = local_positions_V_to_L;
      else
        positions_V_to_L =
	(FriBidiStrIndex *) fribidi_malloc (sizeof (FriBidiStrIndex) * len);
      if (!positions_V_to_L)
	goto out;
      private_V_to_L = true;
    }

  /* Set up the ordering array to identity order */
  if (positions_V_to_L)
    {
      for (i = 0; i < len; i++)
	positions_V_to_L[i] = i;
    }


  if (visual_str)
    {
      /* Using memcpy instead
      for (i = len - 1; i >= 0; i--)
	visual_str[i] = str[i];
      */
      memcpy (visual_str, str, len * sizeof (*visual_str));

      /* Arabic joining */
      if (len < LOCAL_LIST_SIZE)
        ar_props = local_ar_props;
      else
        ar_props = fribidi_malloc (len * sizeof ar_props[0]);
      fribidi_get_joining_types (str, len, ar_props);
      fribidi_join_arabic (bidi_types, len, embedding_levels, ar_props);

      fribidi_shape (flags, embedding_levels, len, ar_props, visual_str);
    }

  /* line breaking goes here, but we assume one line in this function */

  /* and this should be called once per line, but again, we assume one
   * line in this deprecated function */
  status =
    fribidi_reorder_line (flags, bidi_types, len, 0, *pbase_dir,
			  embedding_levels, visual_str,
			  positions_V_to_L);

  /* Convert the v2l list to l2v */
  if (positions_L_to_V)
    {
      for (i = 0; i < len; i++)
	positions_L_to_V[i] = -1;
      for (i = 0; i < len; i++)
	positions_L_to_V[positions_V_to_L[i]] = i;
    }

out:

  if (private_V_to_L && positions_V_to_L != local_positions_V_to_L)
    fribidi_free (positions_V_to_L);

  if (private_embedding_levels && embedding_levels != local_embedding_levels)
    fribidi_free (embedding_levels);

  if (ar_props && ar_props != local_ar_props)
    fribidi_free (ar_props);

  if (bidi_types && bidi_types != local_bidi_types)
    fribidi_free (bidi_types);

  if (bracket_types && bracket_types != local_bracket_types)
    fribidi_free (bracket_types);

  return status ? max_level + 1 : 0;
}

const char *fribidi_unicode_version = FRIBIDI_UNICODE_VERSION;

const char *fribidi_version_info =
  "(" FRIBIDI_NAME ") " FRIBIDI_VERSION "\n"
  "interface version " FRIBIDI_INTERFACE_VERSION_STRING ",\n"
  "Unicode Character Database version " FRIBIDI_UNICODE_VERSION ",\n"
  "Configure options"
#ifdef DEBUG
  " --enable-debug"
#endif /* DEBUG */
  ".\n\n"
  "Copyright (C) 2004  Sharif FarsiWeb, Inc.\n"
  "Copyright (C) 2001, 2002, 2004, 2005  Behdad Esfahbod\n"
  "Copyright (C) 1999, 2000, 2017, 2018, 2019  Dov Grobgeld\n"
  FRIBIDI_NAME " comes with NO WARRANTY, to the extent permitted by law.\n"
  "You may redistribute copies of " FRIBIDI_NAME " under\n"
  "the terms of the GNU Lesser General Public License.\n"
  "For more information about these matters, see the file named COPYING.\n\n"
  "Written by Behdad Esfahbod and Dov Grobgeld.\n";

/* Editor directions:
 * vim:textwidth=78:tabstop=8:shiftwidth=2:autoindent:cindent
 */
