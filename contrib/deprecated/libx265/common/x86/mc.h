/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
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

#ifndef X265_MC_H
#define X265_MC_H

#define LOWRES(cpu) \
    void PFX(frame_init_lowres_core_ ## cpu)(const pixel* src0, pixel* dst0, pixel* dsth, pixel* dstv, pixel* dstc, \
                                             intptr_t src_stride, intptr_t dst_stride, int width, int height);
LOWRES(mmx2)
LOWRES(sse2)
LOWRES(ssse3)
LOWRES(avx)
LOWRES(avx2)
LOWRES(xop)

#undef LOWRES

#define PROPAGATE_COST(cpu) \
    void PFX(mbtree_propagate_cost_ ## cpu)(int* dst, const uint16_t* propagateIn, const int32_t* intraCosts, \
                                              const uint16_t* interCosts, const int32_t* invQscales, const double* fpsFactor, int len);

PROPAGATE_COST(sse2)
PROPAGATE_COST(avx)
PROPAGATE_COST(avx2)

#undef PROPAGATE_COST

#define FIX8UNPACK(cpu) \
    void PFX(cutree_fix8_unpack_ ## cpu)(double *dst, uint16_t *src, int count);

FIX8UNPACK(ssse3)
FIX8UNPACK(avx2)

#undef FIX8UNPACK

#define FIX8PACK(cpu) \
    void PFX(cutree_fix8_pack_## cpu)(uint16_t *dst, double *src, int count);

FIX8PACK(ssse3)
FIX8PACK(avx2)

#undef FIX8PACK

#endif // ifndef X265_MC_H
