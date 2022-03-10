/* version.c --- Version handling.
 * Copyright (C) 2002, 2003, 2004, 2006, 2007  Simon Josefsson
 *
 * This file is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This file is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this file; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301, USA
 *
 */

#ifdef HAVE_CONFIG_H
# include "idn_config.h"
#endif

#include "stringprep.h"

#include <strverscmp.h>

/**
 * stringprep_check_version - check for library version
 * @req_version: Required version number, or NULL.
 *
 * Check that the the version of the library is at minimum the requested one
 * and return the version string; return NULL if the condition is not
 * satisfied.  If a NULL is passed to this function, no check is done,
 * but the version string is simply returned.
 *
 * See %STRINGPREP_VERSION for a suitable @req_version string.
 *
 * Return value: Version string of run-time library, or NULL if the
 * run-time library does not meet the required version number.
 */
const char *
stringprep_check_version (const char *req_version)
{
  if (!req_version || strverscmp (req_version, PACKAGE_VERSION) <= 0)
    return PACKAGE_VERSION;

  return NULL;
}
