/* Scan Bison Skeletons.

   Copyright (C) 2005-2007, 2009-2015, 2018-2021 Free Software
   Foundation, Inc.

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

void scan_skel (FILE *);

/* Pacify "make syntax-check".  */
extern FILE *skel_in;
extern FILE *skel_out;
extern int skel__flex_debug;
extern int skel_lineno;
void skel_scanner_free (void);
