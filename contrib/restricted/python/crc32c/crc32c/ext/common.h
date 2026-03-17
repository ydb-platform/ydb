/*
 *  Common macro definitions
 *
 *  ICRAR - International Centre for Radio Astronomy Research
 *  (c) UWA - The University of Western Australia, 2014
 *  Copyright by UWA (in the framework of the ICRAR)
 *  All rights reserved
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2.1 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 *  MA 02111-1307  USA
 *
 */

#ifndef _COMMON_H_
#define _COMMON_H_

#if defined(__x86_64__) || defined(_M_X64) || defined(i386) || defined(__i386__) || defined(__i386) || defined(_M_IX86)
# define IS_INTEL
#elif defined(__aarch64__) || defined(_M_ARM64)
# define IS_ARM
#endif

#if defined(IS_INTEL) || (defined(IS_ARM) && (defined(__linux__) || defined(linux)))
# define CRC32C_CAN_PROBE_HW
#endif

/* inline support (it's not a keyword in MSVC for C code) */
#if defined(_MSC_VER)
# define CRC32C_INLINE __inline
#else
# define CRC32C_INLINE inline
#endif

/* switch case fallthrough annotation support */
#if defined(__STDC_VERSION__) && defined(__has_c_attribute)
# if __has_c_attribute(fallthrough) && __STDC_VERSION__ >= __has_c_attribute(fallthrough)
#   define CRC32C_FALLTHROUGH [[fallthrough]]
# endif
#endif
#if !defined(CRC32C_FALLTHROUGH) && defined(__has_attribute)
# if __has_attribute(__fallthrough__)
#   define CRC32C_FALLTHROUGH __attribute__((__fallthrough__))
# endif
#endif
#if !defined(CRC32C_FALLTHROUGH)
# define CRC32C_FALLTHROUGH (void)0
#endif

/* 32/64 bit detection */
#if defined(__amd64__) || defined(__amd64) || defined(__x86_64__) || defined(__x86_64) ||\
    (defined(_MSC_VER) && defined(_M_AMD64))
# define CRC32C_IS_64_BITS
#else
# undef CRC32C_IS_64_BITS
#endif

/* uint64_t / uint32_t / uint16_t definitions */
#if !defined(_MSC_VER) || (MSC_VER >= 1800)
# include <stdint.h>
#else
# include <limits.h>
  typedef unsigned __int64 uint64_t;

# if ULONG_MAX == (0xffffffffUL)
   typedef unsigned long uint32_t;
# elif UINT_MAX == (0xffffffffUL)
   typedef unsigned int uint32_t;
# else
#  error "Unsupported platform"
# endif

# if UINT_MAX == (0xffffUL)
   typedef unsigned int uint16_t;
# elif USHRT_MAX == (0xffffUL)
   typedef unsigned short uint16_t;
# else
#  error "Unsupported platform"
# endif

#endif

/* size_t definition */
#include <stdlib.h>

/* crc32c function signature */
typedef uint32_t (* crc_function)(uint32_t crc, unsigned const char *data, unsigned long length);

/* Are we big endian? */
extern int is_big_endian;

#endif // _COMMON_H_
