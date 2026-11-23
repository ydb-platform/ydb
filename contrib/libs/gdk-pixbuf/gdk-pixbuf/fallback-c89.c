/* GTK - The GIMP Toolkit
 * Copyright (C) 2011 Chun-wei Fan <fanc999@yahoo.com.tw>
 *
 * Author: Chun-wei Fan <fanc999@yahoo.com.tw>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
 */

#include <math.h>
 
/* Workaround for round() for non-GCC/non-C99 compilers */
#ifndef HAVE_ROUND
static inline double
round (double x)
{
  if (x >= 0)
    return floor (x + 0.5);
  else
    return ceil (x - 0.5);
}
#endif

/* Workaround for lrint() for non-GCC/non-C99 compilers */
#ifndef HAVE_LRINT
static inline long
lrint (double x)
{
  if (ceil (x + 0.5) == floor (x + 0.5))
    {
      if (x < 1 && x > -1)
        return 0;

      return (int) ceil (x) % 2 == 0 ? ceil (x) : floor (x);
    }
  else
    return x >= 0 ? floor (x + 0.5) : ceil (x - 0.5);
}
#endif