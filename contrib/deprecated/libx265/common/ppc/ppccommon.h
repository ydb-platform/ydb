/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Min Chen <min.chen@multicorewareinc.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02111, USA.
 *
 * This program is also available under a commercial proprietary license.
 * For more information, contact us at license @ x265.com.
 *****************************************************************************/

#ifndef X265_PPCCOMMON_H
#define X265_PPCCOMMON_H


#if HAVE_ALTIVEC
#include <altivec.h>

#define vec_u8_t  vector unsigned char
#define vec_s8_t  vector signed char
#define vec_u16_t vector unsigned short
#define vec_s16_t vector signed short
#define vec_u32_t vector unsigned int
#define vec_s32_t vector signed int

//copy from x264
#define LOAD_ZERO const vec_u8_t zerov = vec_splat_u8( 0 )

#define zero_u8v  (vec_u8_t)  zerov
#define zero_s8v  (vec_s8_t)  zerov
#define zero_u16v (vec_u16_t) zerov
#define zero_s16v (vec_s16_t) zerov
#define zero_u32v (vec_u32_t) zerov
#define zero_s32v (vec_s32_t) zerov

/***********************************************************************
 * 8 <-> 16 bits conversions
 **********************************************************************/
#ifdef WORDS_BIGENDIAN
#define vec_u8_to_u16_h(v) (vec_u16_t) vec_mergeh( zero_u8v, (vec_u8_t) v )
#define vec_u8_to_u16_l(v) (vec_u16_t) vec_mergel( zero_u8v, (vec_u8_t) v )
#define vec_u8_to_s16_h(v) (vec_s16_t) vec_mergeh( zero_u8v, (vec_u8_t) v )
#define vec_u8_to_s16_l(v) (vec_s16_t) vec_mergel( zero_u8v, (vec_u8_t) v )
#else
#define vec_u8_to_u16_h(v) (vec_u16_t) vec_mergeh( (vec_u8_t) v, zero_u8v )
#define vec_u8_to_u16_l(v) (vec_u16_t) vec_mergel( (vec_u8_t) v, zero_u8v )
#define vec_u8_to_s16_h(v) (vec_s16_t) vec_mergeh( (vec_u8_t) v, zero_u8v )
#define vec_u8_to_s16_l(v) (vec_s16_t) vec_mergel( (vec_u8_t) v, zero_u8v )
#endif

#define vec_u8_to_u16(v) vec_u8_to_u16_h(v)
#define vec_u8_to_s16(v) vec_u8_to_s16_h(v)

#ifdef WORDS_BIGENDIAN
#define vec_u16_to_u32_h(v) (vec_u32_t) vec_mergeh( zero_u16v, (vec_u16_t) v )
#define vec_u16_to_u32_l(v) (vec_u32_t) vec_mergel( zero_u16v, (vec_u16_t) v )
#define vec_u16_to_s32_h(v) (vec_s32_t) vec_mergeh( zero_u16v, (vec_u16_t) v )
#define vec_u16_to_s32_l(v) (vec_s32_t) vec_mergel( zero_u16v, (vec_u16_t) v )
#else
#define vec_u16_to_u32_h(v) (vec_u32_t) vec_mergeh( (vec_u16_t) v, zero_u16v )
#define vec_u16_to_u32_l(v) (vec_u32_t) vec_mergel( (vec_u16_t) v, zero_u16v )
#define vec_u16_to_s32_h(v) (vec_s32_t) vec_mergeh( (vec_u16_t) v, zero_u16v )
#define vec_u16_to_s32_l(v) (vec_s32_t) vec_mergel( (vec_u16_t) v, zero_u16v )
#endif

#define vec_u16_to_u32(v) vec_u16_to_u32_h(v)
#define vec_u16_to_s32(v) vec_u16_to_s32_h(v)

#define vec_u32_to_u16(v) vec_pack( v, zero_u32v )
#define vec_s32_to_u16(v) vec_packsu( v, zero_s32v )

#define BITS_PER_SUM (8 * sizeof(sum_t))

#endif /* HAVE_ALTIVEC */

#endif /* X265_PPCCOMMON_H */



