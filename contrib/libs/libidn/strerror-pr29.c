/* strerror-pr29.c --- Convert PR29 errors into text.
 * Copyright (C) 2004, 2005, 2006, 2007, 2008  Simon Josefsson
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

#include "pr29.h"

#include "gettext.h"
#define _(String) dgettext (PACKAGE, String)

/**
 * pr29_strerror - return string describing pr29 error code
 * @rc: an #Pr29_rc return code.
 *
 * Convert a return code integer to a text string.  This string can be
 * used to output a diagnostic message to the user.
 *
 * PR29_SUCCESS: Successful operation.  This value is guaranteed to
 *   always be zero, the remaining ones are only guaranteed to hold
 *   non-zero values, for logical comparison purposes.
 * PR29_PROBLEM: A problem sequence was encountered.
 * PR29_STRINGPREP_ERROR: The character set conversion failed (only
 *   for pr29_8() and pr29_8z()).
 *
 * Return value: Returns a pointer to a statically allocated string
 *   containing a description of the error with the return code @rc.
 **/
const char *
pr29_strerror (Pr29_rc rc)
{
  const char *p;

  bindtextdomain (PACKAGE, LOCALEDIR);

  switch (rc)
    {
    case PR29_SUCCESS:
      p = _("Success");
      break;

    case PR29_PROBLEM:
      p = _("String not idempotent under Unicode NFKC normalization");
      break;

    case PR29_STRINGPREP_ERROR:
      p = _("String preparation failed");
      break;

    default:
      p = _("Unknown error");
      break;
    }

  return p;
}
