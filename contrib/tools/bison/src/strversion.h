/* Convert version string to int.

   Copyright (C) 2020-2021 Free Software Foundation, Inc.

   This file is part of Bison, the GNU Compiler Compiler.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <https://www.gnu.org/licenses/>.  */

#ifndef STRVERSION_H_
# define STRVERSION_H_

/* Convert VERSION into an int (MAJOR * 10000 + MINOR * 100 + MICRO).
   E.g., "3.7.4" => 30704, "3.8" => 30800.
   Return -1 on errors. */
int strversion_to_int (char const *version);

#endif
