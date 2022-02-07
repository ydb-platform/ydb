/* idn-free.h --- Invoke the `free' function releasing memory
 *                allocated by libidn functions.
 * Copyright (C) 2004, 2005, 2006, 2007 Simon Josefsson
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

/* I don't recommend using this interface in general.  Use `free'.
 *
 * I'm told Microsoft Windows may use one set of `malloc' and `free'
 * in a library, and another incompatible set in a statically compiled
 * application that link to the library, thus creating problems if the
 * application would invoke `free' on a pointer pointing to memory
 * allocated by the library.  This motivated adding this function.
 *
 * The theory of isolating all memory allocations and de-allocations
 * within a code package (library) sounds good, to simplify hunting
 * down memory allocation related problems, but I'm not sure if it is
 * worth enough to motivate recommending this interface over calling
 * `free' directly, though.
 *
 * If you have any thoughts or comments on this, please let me know.
 */

void idn_free (void *ptr);
