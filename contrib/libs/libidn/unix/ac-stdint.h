/* Copyright (C) 2008 Free Software Foundation, Inc. 
   Written by Adam Strzelecki <ono@java.pl> 
 
   This program is free software; you can redistribute it and/or 
   modify it under the terms of the GNU Lesser General Public License 
   as published by the Free Software Foundation; either version 2.1, 
   or (at your option) any later version. 
 
   This program is distributed in the hope that it will be useful, but 
   WITHOUT ANY WARRANTY; without even the implied warranty of 
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU 
   Lesser General Public License for more details. 
 
   You should have received a copy of the GNU Lesser General Public 
   License along with this program; if not, write to the Free Software 
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 
   02110-1301, USA.  */ 
 
#ifndef _AC_STDINT_H 
#define _AC_STDINT_H 1 
#ifndef _GENERATED_STDINT_H 
#define _GENERATED_STDINT_H 
 
#if defined(__GNUC__) && (__GNUC__ < 4) 
#   define uint8_t		unsigned char 
#   define uint16_t	unsigned short 
#   define uint32_t	unsigned int 
#   define int8_t		signed char 
#   define int16_t		signed short 
#   define int32_t		signed int 
#   define gint16		int16_t 
#else 
#   include <stdint.h> 
#endif 
/* 
#ifdef  _WIN64 
typedef __int64		ssize_t; 
#else 
typedef _W64 int	ssize_t; 
#endif 
*/ 
#endif 
#endif 
