/*****************************************************************************
* Copyright (C) 2013-2017 MulticoreWare, Inc
*
* Authors: Vignesh V Menon <vignesh@multicorewareinc.com>
*          Jayashri Murugan <jayashri@multicorewareinc.com>
*          Praveen Tiwari <praveen@multicorewareinc.com>
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

#ifndef X265_SEAINTEGRAL_H
#define X265_SEAINTEGRAL_H

void PFX(integral4v_avx2)(uint32_t *sum, intptr_t stride);
void PFX(integral8v_avx2)(uint32_t *sum, intptr_t stride);
void PFX(integral12v_avx2)(uint32_t *sum, intptr_t stride);
void PFX(integral16v_avx2)(uint32_t *sum, intptr_t stride);
void PFX(integral24v_avx2)(uint32_t *sum, intptr_t stride);
void PFX(integral32v_avx2)(uint32_t *sum, intptr_t stride);
void PFX(integral4h_avx2)(uint32_t *sum, pixel *pix, intptr_t stride);
void PFX(integral8h_avx2)(uint32_t *sum, pixel *pix, intptr_t stride);
void PFX(integral12h_avx2)(uint32_t *sum, pixel *pix, intptr_t stride);
void PFX(integral16h_avx2)(uint32_t *sum, pixel *pix, intptr_t stride);
void PFX(integral24h_avx2)(uint32_t *sum, pixel *pix, intptr_t stride);
void PFX(integral32h_avx2)(uint32_t *sum, pixel *pix, intptr_t stride);

#endif //X265_SEAINTEGRAL_H
