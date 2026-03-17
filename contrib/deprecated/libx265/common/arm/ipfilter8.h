/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Dnyaneshwar Gorade <dnyaneshwar@multicorewareinc.com>
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

#ifndef X265_IPFILTER8_ARM_H
#define X265_IPFILTER8_ARM_H

void x265_filterPixelToShort_4x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_4x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_4x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_8x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_8x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_8x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_8x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_12x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_16x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_16x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_16x12_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_16x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_16x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_16x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_24x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_32x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_32x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_32x24_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_32x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_32x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_48x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_64x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_64x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_64x48_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);
void x265_filterPixelToShort_64x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride);

void x265_interp_8tap_vert_pp_4x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_4x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_4x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_8x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_8x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_8x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_8x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_16x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_16x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_16x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_16x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_16x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_16x12_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_32x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_32x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_32x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_32x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_32x24_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_64x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_64x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_64x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_64x48_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_24x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_48x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_pp_12x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);

void x265_interp_8tap_vert_sp_4x4_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_4x8_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_4x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_8x4_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_8x8_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_8x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_8x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_16x4_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_16x8_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_16x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_16x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_16x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_16x12_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_32x8_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_32x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_32x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_32x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_32x24_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_64x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_64x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_64x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_64x48_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_24x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_48x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_sp_12x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);

void x265_interp_8tap_vert_ps_4x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_4x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_4x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_8x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_8x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_8x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_8x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_16x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_16x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_16x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_16x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_16x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_16x12_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_32x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_32x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_32x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_32x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_32x24_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_64x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_64x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_64x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_64x48_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_24x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_48x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_8tap_vert_ps_12x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);

void x265_interp_4tap_vert_pp_8x2_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_8x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_8x6_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_8x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_8x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_8x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_8x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_8x12_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_16x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_16x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_16x12_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_16x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_16x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_16x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_16x24_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_32x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_32x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_32x24_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_32x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_32x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_32x48_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_24x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_24x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_48x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_64x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_64x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_64x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_pp_64x48_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);

void x265_interp_4tap_vert_ps_8x2_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_8x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_8x6_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_8x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_8x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_8x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_8x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_8x12_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_16x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_16x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_16x12_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_16x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_16x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_16x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_16x24_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_32x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_32x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_32x24_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_32x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_32x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_32x48_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_24x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_24x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_48x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_64x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_64x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_64x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_ps_64x48_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx);

void x265_interp_4tap_vert_sp_8x2_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_8x4_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_8x6_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_8x8_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_8x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_8x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_8x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_8x12_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_16x4_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_16x8_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_16x12_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_16x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_16x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_16x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_16x24_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_32x8_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_32x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_32x24_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_32x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_32x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_32x48_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_24x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_24x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_48x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_64x16_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_64x32_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_64x64_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_vert_sp_64x48_neon(const int16_t* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);

void x265_interp_horiz_pp_4x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_4x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_4x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_8x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_8x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_8x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_8x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_12x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_16x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_16x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_16x12_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_16x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_16x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_16x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_24x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_32x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_32x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_32x24_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_32x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_32x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_48x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_64x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_64x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_64x48_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_horiz_pp_64x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);

void x265_interp_horiz_ps_4x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_4x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_4x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_8x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_8x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_8x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_8x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_12x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_16x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_16x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_16x12_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_16x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_16x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_16x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_24x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_32x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_32x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_32x24_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_32x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_32x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_48x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_64x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_64x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_64x48_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_horiz_ps_64x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);

void x265_interp_4tap_horiz_pp_4x2_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_4x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_4x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_4x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_4x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_8x2_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_8x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_8x6_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_8x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_8x12_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_8x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_8x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_8x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_12x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_12x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_16x4_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_16x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_16x12_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_16x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_16x24_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_16x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_16x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_24x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_24x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_32x8_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_32x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_32x24_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_32x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_32x48_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_32x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_48x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_64x16_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_64x32_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_64x48_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);
void x265_interp_4tap_horiz_pp_64x64_neon(const pixel* src, intptr_t srcStride, pixel* dst, intptr_t dstStride, int coeffIdx);

void x265_interp_4tap_horiz_ps_4x2_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_4x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_4x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_4x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_4x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_8x2_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_8x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_8x6_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_8x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_8x12_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_8x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_8x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_8x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_12x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_12x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_16x4_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_16x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_16x12_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_16x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_16x24_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_16x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_16x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_24x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_24x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_32x8_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_32x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_32x24_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_32x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_32x48_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_32x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_48x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_64x16_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_64x32_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_64x48_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
void x265_interp_4tap_horiz_ps_64x64_neon(const pixel* src, intptr_t srcStride, int16_t* dst, intptr_t dstStride, int coeffIdx, int isRowExt);
#endif // ifndef X265_IPFILTER8_ARM_H
