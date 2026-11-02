/* FriBidi
 * joining-types.h - define internal joining types
 *
 * Author:
 *   Behdad Esfahbod, 2004
 *
 * Copyright (C) 2004 Sharif FarsiWeb, Inc.
 * Copyright (C) 2004 Behdad Esfahbod
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
#ifndef _JOINING_TYPES_H
#define _JOINING_TYPES_H

#include "fribidi-common.h"

#include <fribidi-types.h>
#include <fribidi-joining-types.h>

#include <fribidi-begindecls.h>

#ifdef DEBUG

char
fribidi_char_from_joining_type (
  FriBidiJoiningType j,		/* input joining type */
  fribidi_boolean visual	/* in visual context or logical? */
) FRIBIDI_GNUC_HIDDEN;

#endif /* DEBUG */

#include <fribidi-enddecls.h>

#endif /* !_JOINING_TYPES_H */
/* Editor directions:
 * vim:textwidth=78:tabstop=8:shiftwidth=2:autoindent:cindent
 */
