/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Min Chen <chenm003@163.com>
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

#ifndef X265_DCT8_ARM_H
#define X265_DCT8_ARM_H

void PFX(dct_4x4_neon)(const int16_t* src, int16_t* dst, intptr_t srcStride);
void PFX(dct_8x8_neon)(const int16_t* src, int16_t* dst, intptr_t srcStride);
void PFX(dct_16x16_neon)(const int16_t* src, int16_t* dst, intptr_t srcStride);

#endif // ifndef X265_DCT8_ARM_H
