## Makefile.am - template for generating Makefile via Automake
##
## Copyright (C) 2006-2013 Free Software Foundation, Inc.
##
## This file is part of GNU M4.
##
## GNU M4 is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.
##
## GNU M4 is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.
##
## You should have received a copy of the GNU General Public License
## along with this program.  If not, see <http://www.gnu.org/licenses/>.
##
## This file written by Eric Blake <ebb9@byu.net>

AUTOMAKE_OPTIONS = nostdinc
AM_CPPFLAGS = -I$(top_srcdir)/lib -I../lib
AM_CFLAGS = $(WARN_CFLAGS) $(WERROR_CFLAGS)
bin_PROGRAMS = m4
noinst_HEADERS = m4.h
m4_SOURCES = m4.c builtin.c debug.c eval.c format.c freeze.c input.c \
macro.c output.c path.c symtab.c
m4_LDADD = ../lib/libm4.a $(LIBM4_LIBDEPS) $(LIBCSTACK) $(LIBTHREAD)
