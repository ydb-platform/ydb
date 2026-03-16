/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Steve Borho <steve@borho.org>
 *          Praveen Kumar Tiwari <praveen@multicorewareinc.com>
 *          Min Chen <chenm003@163.com> <min.chen@multicorewareinc.com>
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

#include "common.h"
#include "primitives.h"
#include "x265.h"
#include "cpu.h"

extern "C" {
#include "blockcopy8.h"
#include "pixel.h"
#include "pixel-util.h"
#include "ipfilter8.h"
#include "dct8.h"
}

namespace X265_NS {
// private x265 namespace

void setupAssemblyPrimitives(EncoderPrimitives &p, int cpuMask)
{
    if (cpuMask & X265_CPU_NEON)
    {
        // ssim_4x4x2_core
        p.ssim_4x4x2_core = PFX(ssim_4x4x2_core_neon);

        // addAvg
         p.pu[LUMA_4x4].addAvg   = PFX(addAvg_4x4_neon);
         p.pu[LUMA_4x8].addAvg   = PFX(addAvg_4x8_neon);
         p.pu[LUMA_4x16].addAvg  = PFX(addAvg_4x16_neon);
         p.pu[LUMA_8x4].addAvg   = PFX(addAvg_8x4_neon);
         p.pu[LUMA_8x8].addAvg   = PFX(addAvg_8x8_neon);
         p.pu[LUMA_8x16].addAvg  = PFX(addAvg_8x16_neon);
         p.pu[LUMA_8x32].addAvg  = PFX(addAvg_8x32_neon);
         p.pu[LUMA_12x16].addAvg = PFX(addAvg_12x16_neon);
         p.pu[LUMA_16x4].addAvg  = PFX(addAvg_16x4_neon);
         p.pu[LUMA_16x8].addAvg  = PFX(addAvg_16x8_neon);
         p.pu[LUMA_16x12].addAvg = PFX(addAvg_16x12_neon);
         p.pu[LUMA_16x16].addAvg = PFX(addAvg_16x16_neon);
         p.pu[LUMA_16x32].addAvg = PFX(addAvg_16x32_neon);
         p.pu[LUMA_16x64].addAvg = PFX(addAvg_16x64_neon);
         p.pu[LUMA_24x32].addAvg = PFX(addAvg_24x32_neon);
         p.pu[LUMA_32x8].addAvg  = PFX(addAvg_32x8_neon);
         p.pu[LUMA_32x16].addAvg = PFX(addAvg_32x16_neon);
         p.pu[LUMA_32x24].addAvg = PFX(addAvg_32x24_neon);
         p.pu[LUMA_32x32].addAvg = PFX(addAvg_32x32_neon);
         p.pu[LUMA_32x64].addAvg = PFX(addAvg_32x64_neon);
         p.pu[LUMA_48x64].addAvg = PFX(addAvg_48x64_neon);
         p.pu[LUMA_64x16].addAvg = PFX(addAvg_64x16_neon);
         p.pu[LUMA_64x32].addAvg = PFX(addAvg_64x32_neon);
         p.pu[LUMA_64x48].addAvg = PFX(addAvg_64x48_neon);
         p.pu[LUMA_64x64].addAvg = PFX(addAvg_64x64_neon);

        // chroma addAvg
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].addAvg   = PFX(addAvg_4x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].addAvg   = PFX(addAvg_4x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].addAvg   = PFX(addAvg_4x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].addAvg  = PFX(addAvg_4x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].addAvg   = PFX(addAvg_6x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].addAvg   = PFX(addAvg_8x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].addAvg   = PFX(addAvg_8x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].addAvg   = PFX(addAvg_8x6_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].addAvg   = PFX(addAvg_8x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].addAvg  = PFX(addAvg_8x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].addAvg  = PFX(addAvg_8x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].addAvg = PFX(addAvg_12x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].addAvg  = PFX(addAvg_16x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].addAvg  = PFX(addAvg_16x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].addAvg = PFX(addAvg_16x12_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].addAvg = PFX(addAvg_16x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].addAvg = PFX(addAvg_16x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].addAvg = PFX(addAvg_24x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].addAvg  = PFX(addAvg_32x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].addAvg = PFX(addAvg_32x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].addAvg = PFX(addAvg_32x24_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].addAvg = PFX(addAvg_32x32_neon);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].addAvg   = PFX(addAvg_4x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].addAvg  = PFX(addAvg_4x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].addAvg  = PFX(addAvg_4x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].addAvg  = PFX(addAvg_6x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].addAvg   = PFX(addAvg_8x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].addAvg   = PFX(addAvg_8x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].addAvg  = PFX(addAvg_8x12_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].addAvg  = PFX(addAvg_8x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].addAvg  = PFX(addAvg_8x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].addAvg  = PFX(addAvg_8x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].addAvg = PFX(addAvg_12x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].addAvg  = PFX(addAvg_16x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].addAvg = PFX(addAvg_16x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].addAvg = PFX(addAvg_16x24_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].addAvg = PFX(addAvg_16x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].addAvg = PFX(addAvg_16x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].addAvg = PFX(addAvg_24x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].addAvg = PFX(addAvg_32x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].addAvg = PFX(addAvg_32x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].addAvg = PFX(addAvg_32x48_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].addAvg = PFX(addAvg_32x64_neon);

        // quant
         p.quant = PFX(quant_neon);
         p.nquant = PFX(nquant_neon);

        // dequant_scaling
         p.dequant_scaling = PFX(dequant_scaling_neon);
         p.dequant_normal  = PFX(dequant_normal_neon);

        // luma satd
         p.pu[LUMA_4x4].satd   = PFX(pixel_satd_4x4_neon);
         p.pu[LUMA_4x8].satd   = PFX(pixel_satd_4x8_neon);
         p.pu[LUMA_4x16].satd  = PFX(pixel_satd_4x16_neon);
         p.pu[LUMA_8x4].satd   = PFX(pixel_satd_8x4_neon);
         p.pu[LUMA_8x8].satd   = PFX(pixel_satd_8x8_neon);
         p.pu[LUMA_8x16].satd  = PFX(pixel_satd_8x16_neon);
         p.pu[LUMA_8x32].satd  = PFX(pixel_satd_8x32_neon);
         p.pu[LUMA_12x16].satd = PFX(pixel_satd_12x16_neon);
         p.pu[LUMA_16x4].satd  = PFX(pixel_satd_16x4_neon);
         p.pu[LUMA_16x8].satd  = PFX(pixel_satd_16x8_neon);
         p.pu[LUMA_16x16].satd = PFX(pixel_satd_16x16_neon);
         p.pu[LUMA_16x32].satd = PFX(pixel_satd_16x32_neon);
         p.pu[LUMA_16x64].satd = PFX(pixel_satd_16x64_neon);
         p.pu[LUMA_24x32].satd = PFX(pixel_satd_24x32_neon);
         p.pu[LUMA_32x8].satd  = PFX(pixel_satd_32x8_neon);
         p.pu[LUMA_32x16].satd = PFX(pixel_satd_32x16_neon);
         p.pu[LUMA_32x24].satd = PFX(pixel_satd_32x24_neon);
         p.pu[LUMA_32x32].satd = PFX(pixel_satd_32x32_neon);
         p.pu[LUMA_32x64].satd = PFX(pixel_satd_32x64_neon);
         p.pu[LUMA_48x64].satd = PFX(pixel_satd_48x64_neon);
         p.pu[LUMA_64x16].satd = PFX(pixel_satd_64x16_neon);
         p.pu[LUMA_64x32].satd = PFX(pixel_satd_64x32_neon);
         p.pu[LUMA_64x48].satd = PFX(pixel_satd_64x48_neon);
         p.pu[LUMA_64x64].satd = PFX(pixel_satd_64x64_neon);

        // chroma satd
         p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].satd    = PFX(pixel_satd_4x4_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].satd    = PFX(pixel_satd_4x8_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].satd   = PFX(pixel_satd_4x16_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].satd    = PFX(pixel_satd_8x4_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].satd    = PFX(pixel_satd_8x8_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].satd   = PFX(pixel_satd_8x16_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].satd   = PFX(pixel_satd_8x32_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].satd  = PFX(pixel_satd_12x16_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].satd   = PFX(pixel_satd_16x4_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].satd   = PFX(pixel_satd_16x8_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].satd  = PFX(pixel_satd_16x12_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].satd  = PFX(pixel_satd_16x16_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].satd  = PFX(pixel_satd_16x32_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].satd  = PFX(pixel_satd_24x32_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].satd   = PFX(pixel_satd_32x8_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].satd  = PFX(pixel_satd_32x16_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].satd  = PFX(pixel_satd_32x24_neon);
         p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].satd  = PFX(pixel_satd_32x32_neon);

         p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].satd    = PFX(pixel_satd_4x4_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].satd    = PFX(pixel_satd_4x8_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].satd   = PFX(pixel_satd_4x16_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].satd   = PFX(pixel_satd_4x32_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].satd    = PFX(pixel_satd_8x4_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].satd    = PFX(pixel_satd_8x8_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].satd   = PFX(pixel_satd_8x12_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].satd   = PFX(pixel_satd_8x16_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].satd   = PFX(pixel_satd_8x32_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].satd   = PFX(pixel_satd_8x64_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].satd  = PFX(pixel_satd_12x32_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].satd   = PFX(pixel_satd_16x8_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].satd  = PFX(pixel_satd_16x16_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].satd  = PFX(pixel_satd_16x24_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].satd  = PFX(pixel_satd_16x32_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].satd  = PFX(pixel_satd_16x64_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].satd  = PFX(pixel_satd_24x64_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].satd  = PFX(pixel_satd_32x16_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].satd  = PFX(pixel_satd_32x32_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].satd  = PFX(pixel_satd_32x48_neon);
         p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].satd  = PFX(pixel_satd_32x64_neon);

        // chroma_hpp
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_hpp   = PFX(interp_4tap_horiz_pp_4x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_hpp   = PFX(interp_4tap_horiz_pp_4x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_hpp   = PFX(interp_4tap_horiz_pp_4x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_hpp  = PFX(interp_4tap_horiz_pp_4x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_hpp   = PFX(interp_4tap_horiz_pp_8x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_hpp   = PFX(interp_4tap_horiz_pp_8x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_hpp   = PFX(interp_4tap_horiz_pp_8x6_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_hpp   = PFX(interp_4tap_horiz_pp_8x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_hpp  = PFX(interp_4tap_horiz_pp_8x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_hpp  = PFX(interp_4tap_horiz_pp_8x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_hpp = PFX(interp_4tap_horiz_pp_12x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_hpp  = PFX(interp_4tap_horiz_pp_16x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_hpp  = PFX(interp_4tap_horiz_pp_16x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_hpp = PFX(interp_4tap_horiz_pp_16x12_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_hpp = PFX(interp_4tap_horiz_pp_16x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_hpp = PFX(interp_4tap_horiz_pp_16x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_hpp = PFX(interp_4tap_horiz_pp_24x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_hpp  = PFX(interp_4tap_horiz_pp_32x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_hpp = PFX(interp_4tap_horiz_pp_32x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_hpp = PFX(interp_4tap_horiz_pp_32x24_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_hpp = PFX(interp_4tap_horiz_pp_32x32_neon);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_hpp   = PFX(interp_4tap_horiz_pp_4x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_hpp   = PFX(interp_4tap_horiz_pp_4x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_hpp  = PFX(interp_4tap_horiz_pp_4x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_hpp  = PFX(interp_4tap_horiz_pp_4x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_hpp   = PFX(interp_4tap_horiz_pp_8x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_hpp   = PFX(interp_4tap_horiz_pp_8x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_hpp  = PFX(interp_4tap_horiz_pp_8x12_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_hpp  = PFX(interp_4tap_horiz_pp_8x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_hpp  = PFX(interp_4tap_horiz_pp_8x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_hpp  = PFX(interp_4tap_horiz_pp_8x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_hpp = PFX(interp_4tap_horiz_pp_12x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_hpp  = PFX(interp_4tap_horiz_pp_16x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_hpp = PFX(interp_4tap_horiz_pp_16x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_hpp = PFX(interp_4tap_horiz_pp_16x24_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_hpp = PFX(interp_4tap_horiz_pp_16x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_hpp = PFX(interp_4tap_horiz_pp_16x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_hpp = PFX(interp_4tap_horiz_pp_24x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_hpp = PFX(interp_4tap_horiz_pp_32x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_hpp = PFX(interp_4tap_horiz_pp_32x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_hpp = PFX(interp_4tap_horiz_pp_32x48_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_hpp = PFX(interp_4tap_horiz_pp_32x64_neon);

        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_hpp   = PFX(interp_4tap_horiz_pp_4x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_hpp   = PFX(interp_4tap_horiz_pp_4x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_hpp  = PFX(interp_4tap_horiz_pp_4x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_hpp   = PFX(interp_4tap_horiz_pp_8x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_hpp   = PFX(interp_4tap_horiz_pp_8x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_hpp  = PFX(interp_4tap_horiz_pp_8x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_hpp  = PFX(interp_4tap_horiz_pp_8x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_hpp = PFX(interp_4tap_horiz_pp_12x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_hpp  = PFX(interp_4tap_horiz_pp_16x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_hpp  = PFX(interp_4tap_horiz_pp_16x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_hpp = PFX(interp_4tap_horiz_pp_16x12_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_hpp = PFX(interp_4tap_horiz_pp_16x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_hpp = PFX(interp_4tap_horiz_pp_16x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_hpp = PFX(interp_4tap_horiz_pp_16x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_hpp = PFX(interp_4tap_horiz_pp_24x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_hpp  = PFX(interp_4tap_horiz_pp_32x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_hpp = PFX(interp_4tap_horiz_pp_32x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_hpp = PFX(interp_4tap_horiz_pp_32x24_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_hpp = PFX(interp_4tap_horiz_pp_32x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_hpp = PFX(interp_4tap_horiz_pp_32x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_hpp = PFX(interp_4tap_horiz_pp_48x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_hpp = PFX(interp_4tap_horiz_pp_64x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_hpp = PFX(interp_4tap_horiz_pp_64x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_hpp = PFX(interp_4tap_horiz_pp_64x48_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_hpp = PFX(interp_4tap_horiz_pp_64x64_neon);

        // chroma_hps
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].filter_hps   = PFX(interp_4tap_horiz_ps_4x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].filter_hps   = PFX(interp_4tap_horiz_ps_4x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].filter_hps   = PFX(interp_4tap_horiz_ps_4x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].filter_hps  = PFX(interp_4tap_horiz_ps_4x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_hps   = PFX(interp_4tap_horiz_ps_8x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_hps   = PFX(interp_4tap_horiz_ps_8x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_hps   = PFX(interp_4tap_horiz_ps_8x6_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_hps   = PFX(interp_4tap_horiz_ps_8x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_hps  = PFX(interp_4tap_horiz_ps_8x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_hps  = PFX(interp_4tap_horiz_ps_8x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].filter_hps = PFX(interp_4tap_horiz_ps_12x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_hps  = PFX(interp_4tap_horiz_ps_16x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_hps  = PFX(interp_4tap_horiz_ps_16x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_hps = PFX(interp_4tap_horiz_ps_16x12_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_hps = PFX(interp_4tap_horiz_ps_16x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_hps = PFX(interp_4tap_horiz_ps_16x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_hps = PFX(interp_4tap_horiz_ps_24x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_hps  = PFX(interp_4tap_horiz_ps_32x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_hps = PFX(interp_4tap_horiz_ps_32x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_hps = PFX(interp_4tap_horiz_ps_32x24_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_hps = PFX(interp_4tap_horiz_ps_32x32_neon);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].filter_hps   = PFX(interp_4tap_horiz_ps_4x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].filter_hps   = PFX(interp_4tap_horiz_ps_4x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].filter_hps  = PFX(interp_4tap_horiz_ps_4x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].filter_hps  = PFX(interp_4tap_horiz_ps_4x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_hps   = PFX(interp_4tap_horiz_ps_8x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_hps   = PFX(interp_4tap_horiz_ps_8x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_hps  = PFX(interp_4tap_horiz_ps_8x12_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_hps  = PFX(interp_4tap_horiz_ps_8x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_hps  = PFX(interp_4tap_horiz_ps_8x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_hps  = PFX(interp_4tap_horiz_ps_8x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].filter_hps = PFX(interp_4tap_horiz_ps_12x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_hps  = PFX(interp_4tap_horiz_ps_16x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_hps = PFX(interp_4tap_horiz_ps_16x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_hps = PFX(interp_4tap_horiz_ps_16x24_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_hps = PFX(interp_4tap_horiz_ps_16x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_hps = PFX(interp_4tap_horiz_ps_16x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_hps = PFX(interp_4tap_horiz_ps_24x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_hps = PFX(interp_4tap_horiz_ps_32x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_hps = PFX(interp_4tap_horiz_ps_32x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_hps = PFX(interp_4tap_horiz_ps_32x48_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_hps = PFX(interp_4tap_horiz_ps_32x64_neon);

        p.chroma[X265_CSP_I444].pu[LUMA_4x4].filter_hps   = PFX(interp_4tap_horiz_ps_4x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_4x8].filter_hps   = PFX(interp_4tap_horiz_ps_4x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_4x16].filter_hps  = PFX(interp_4tap_horiz_ps_4x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_hps   = PFX(interp_4tap_horiz_ps_8x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_hps   = PFX(interp_4tap_horiz_ps_8x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_hps  = PFX(interp_4tap_horiz_ps_8x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_hps  = PFX(interp_4tap_horiz_ps_8x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_12x16].filter_hps = PFX(interp_4tap_horiz_ps_12x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_hps  = PFX(interp_4tap_horiz_ps_16x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_hps  = PFX(interp_4tap_horiz_ps_16x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_hps = PFX(interp_4tap_horiz_ps_16x12_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_hps = PFX(interp_4tap_horiz_ps_16x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_hps = PFX(interp_4tap_horiz_ps_16x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_hps = PFX(interp_4tap_horiz_ps_16x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_hps = PFX(interp_4tap_horiz_ps_24x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_hps  = PFX(interp_4tap_horiz_ps_32x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_hps = PFX(interp_4tap_horiz_ps_32x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x24].filter_hps = PFX(interp_4tap_horiz_ps_32x24_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_hps = PFX(interp_4tap_horiz_ps_32x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_hps = PFX(interp_4tap_horiz_ps_32x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_hps = PFX(interp_4tap_horiz_ps_48x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_hps = PFX(interp_4tap_horiz_ps_64x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_hps = PFX(interp_4tap_horiz_ps_64x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_hps = PFX(interp_4tap_horiz_ps_64x48_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_hps = PFX(interp_4tap_horiz_ps_64x64_neon);

        // luma_hpp
        p.pu[LUMA_4x4].luma_hpp   = PFX(interp_horiz_pp_4x4_neon);
        p.pu[LUMA_4x8].luma_hpp   = PFX(interp_horiz_pp_4x8_neon);
        p.pu[LUMA_4x16].luma_hpp  = PFX(interp_horiz_pp_4x16_neon);
        p.pu[LUMA_8x4].luma_hpp   = PFX(interp_horiz_pp_8x4_neon);
        p.pu[LUMA_8x8].luma_hpp   = PFX(interp_horiz_pp_8x8_neon);
        p.pu[LUMA_8x16].luma_hpp  = PFX(interp_horiz_pp_8x16_neon);
        p.pu[LUMA_8x32].luma_hpp  = PFX(interp_horiz_pp_8x32_neon);
        p.pu[LUMA_12x16].luma_hpp = PFX(interp_horiz_pp_12x16_neon);
        p.pu[LUMA_16x4].luma_hpp  = PFX(interp_horiz_pp_16x4_neon);
        p.pu[LUMA_16x8].luma_hpp  = PFX(interp_horiz_pp_16x8_neon);
        p.pu[LUMA_16x12].luma_hpp = PFX(interp_horiz_pp_16x12_neon);
        p.pu[LUMA_16x16].luma_hpp = PFX(interp_horiz_pp_16x16_neon);
        p.pu[LUMA_16x32].luma_hpp = PFX(interp_horiz_pp_16x32_neon);
        p.pu[LUMA_16x64].luma_hpp = PFX(interp_horiz_pp_16x64_neon);
        p.pu[LUMA_24x32].luma_hpp = PFX(interp_horiz_pp_24x32_neon);
        p.pu[LUMA_32x8].luma_hpp  = PFX(interp_horiz_pp_32x8_neon);
        p.pu[LUMA_32x16].luma_hpp = PFX(interp_horiz_pp_32x16_neon);
        p.pu[LUMA_32x24].luma_hpp = PFX(interp_horiz_pp_32x24_neon);
        p.pu[LUMA_32x32].luma_hpp = PFX(interp_horiz_pp_32x32_neon);
        p.pu[LUMA_32x64].luma_hpp = PFX(interp_horiz_pp_32x64_neon);
        p.pu[LUMA_48x64].luma_hpp = PFX(interp_horiz_pp_48x64_neon);
        p.pu[LUMA_64x16].luma_hpp = PFX(interp_horiz_pp_64x16_neon);
        p.pu[LUMA_64x32].luma_hpp = PFX(interp_horiz_pp_64x32_neon);
        p.pu[LUMA_64x48].luma_hpp = PFX(interp_horiz_pp_64x48_neon);
        p.pu[LUMA_64x64].luma_hpp = PFX(interp_horiz_pp_64x64_neon);

        // luma_hps
        p.pu[LUMA_4x4].luma_hps   = PFX(interp_horiz_ps_4x4_neon);
        p.pu[LUMA_4x8].luma_hps   = PFX(interp_horiz_ps_4x8_neon);
        p.pu[LUMA_4x16].luma_hps  = PFX(interp_horiz_ps_4x16_neon);
        p.pu[LUMA_8x4].luma_hps   = PFX(interp_horiz_ps_8x4_neon);
        p.pu[LUMA_8x8].luma_hps   = PFX(interp_horiz_ps_8x8_neon);
        p.pu[LUMA_8x16].luma_hps  = PFX(interp_horiz_ps_8x16_neon);
        p.pu[LUMA_8x32].luma_hps  = PFX(interp_horiz_ps_8x32_neon);
        p.pu[LUMA_12x16].luma_hps = PFX(interp_horiz_ps_12x16_neon);
        p.pu[LUMA_16x4].luma_hps  = PFX(interp_horiz_ps_16x4_neon);
        p.pu[LUMA_16x8].luma_hps  = PFX(interp_horiz_ps_16x8_neon);
        p.pu[LUMA_16x12].luma_hps = PFX(interp_horiz_ps_16x12_neon);
        p.pu[LUMA_16x16].luma_hps = PFX(interp_horiz_ps_16x16_neon);
        p.pu[LUMA_16x32].luma_hps = PFX(interp_horiz_ps_16x32_neon);
        p.pu[LUMA_16x64].luma_hps = PFX(interp_horiz_ps_16x64_neon);
        p.pu[LUMA_24x32].luma_hps = PFX(interp_horiz_ps_24x32_neon);
        p.pu[LUMA_32x8].luma_hps  = PFX(interp_horiz_ps_32x8_neon);
        p.pu[LUMA_32x16].luma_hps = PFX(interp_horiz_ps_32x16_neon);
        p.pu[LUMA_32x24].luma_hps = PFX(interp_horiz_ps_32x24_neon);
        p.pu[LUMA_32x32].luma_hps = PFX(interp_horiz_ps_32x32_neon);
        p.pu[LUMA_32x64].luma_hps = PFX(interp_horiz_ps_32x64_neon);
        p.pu[LUMA_48x64].luma_hps = PFX(interp_horiz_ps_48x64_neon);
        p.pu[LUMA_64x16].luma_hps = PFX(interp_horiz_ps_64x16_neon);
        p.pu[LUMA_64x32].luma_hps = PFX(interp_horiz_ps_64x32_neon);
        p.pu[LUMA_64x48].luma_hps = PFX(interp_horiz_ps_64x48_neon);
        p.pu[LUMA_64x64].luma_hps = PFX(interp_horiz_ps_64x64_neon);

        // count nonzero
        p.cu[BLOCK_4x4].count_nonzero     = PFX(count_nonzero_4_neon);
        p.cu[BLOCK_8x8].count_nonzero     = PFX(count_nonzero_8_neon);
        p.cu[BLOCK_16x16].count_nonzero   = PFX(count_nonzero_16_neon);
        p.cu[BLOCK_32x32].count_nonzero   = PFX(count_nonzero_32_neon);

        //scale2D_64to32
        p.scale2D_64to32  = PFX(scale2D_64to32_neon);

        // scale1D_128to64
        p.scale1D_128to64 = PFX(scale1D_128to64_neon);

        // copy_count
        p.cu[BLOCK_4x4].copy_cnt     = PFX(copy_cnt_4_neon);
        p.cu[BLOCK_8x8].copy_cnt     = PFX(copy_cnt_8_neon);
        p.cu[BLOCK_16x16].copy_cnt   = PFX(copy_cnt_16_neon);
        p.cu[BLOCK_32x32].copy_cnt   = PFX(copy_cnt_32_neon);

        // filterPixelToShort
        p.pu[LUMA_4x4].convert_p2s   = PFX(filterPixelToShort_4x4_neon);
        p.pu[LUMA_4x8].convert_p2s   = PFX(filterPixelToShort_4x8_neon);
        p.pu[LUMA_4x16].convert_p2s  = PFX(filterPixelToShort_4x16_neon);
        p.pu[LUMA_8x4].convert_p2s   = PFX(filterPixelToShort_8x4_neon);
        p.pu[LUMA_8x8].convert_p2s   = PFX(filterPixelToShort_8x8_neon);
        p.pu[LUMA_8x16].convert_p2s  = PFX(filterPixelToShort_8x16_neon);
        p.pu[LUMA_8x32].convert_p2s  = PFX(filterPixelToShort_8x32_neon);
        p.pu[LUMA_12x16].convert_p2s = PFX(filterPixelToShort_12x16_neon);
        p.pu[LUMA_16x4].convert_p2s  = PFX(filterPixelToShort_16x4_neon);
        p.pu[LUMA_16x8].convert_p2s  = PFX(filterPixelToShort_16x8_neon);
        p.pu[LUMA_16x12].convert_p2s = PFX(filterPixelToShort_16x12_neon);
        p.pu[LUMA_16x16].convert_p2s = PFX(filterPixelToShort_16x16_neon);
        p.pu[LUMA_16x32].convert_p2s = PFX(filterPixelToShort_16x32_neon);
        p.pu[LUMA_16x64].convert_p2s = PFX(filterPixelToShort_16x64_neon);
        p.pu[LUMA_24x32].convert_p2s = PFX(filterPixelToShort_24x32_neon);
        p.pu[LUMA_32x8].convert_p2s  = PFX(filterPixelToShort_32x8_neon);
        p.pu[LUMA_32x16].convert_p2s = PFX(filterPixelToShort_32x16_neon);
        p.pu[LUMA_32x24].convert_p2s = PFX(filterPixelToShort_32x24_neon);
        p.pu[LUMA_32x32].convert_p2s = PFX(filterPixelToShort_32x32_neon);
        p.pu[LUMA_32x64].convert_p2s = PFX(filterPixelToShort_32x64_neon);
        p.pu[LUMA_48x64].convert_p2s = PFX(filterPixelToShort_48x64_neon);
        p.pu[LUMA_64x16].convert_p2s = PFX(filterPixelToShort_64x16_neon);
        p.pu[LUMA_64x32].convert_p2s = PFX(filterPixelToShort_64x32_neon);
        p.pu[LUMA_64x48].convert_p2s = PFX(filterPixelToShort_64x48_neon);
        p.pu[LUMA_64x64].convert_p2s = PFX(filterPixelToShort_64x64_neon);

        // Block_fill
        p.cu[BLOCK_4x4].blockfill_s   = PFX(blockfill_s_4x4_neon);
        p.cu[BLOCK_8x8].blockfill_s   = PFX(blockfill_s_8x8_neon);
        p.cu[BLOCK_16x16].blockfill_s = PFX(blockfill_s_16x16_neon);
        p.cu[BLOCK_32x32].blockfill_s = PFX(blockfill_s_32x32_neon);

        // Blockcopy_ss
        p.cu[BLOCK_4x4].copy_ss   = PFX(blockcopy_ss_4x4_neon);
        p.cu[BLOCK_8x8].copy_ss   = PFX(blockcopy_ss_8x8_neon);
        p.cu[BLOCK_16x16].copy_ss = PFX(blockcopy_ss_16x16_neon);
        p.cu[BLOCK_32x32].copy_ss = PFX(blockcopy_ss_32x32_neon);
        p.cu[BLOCK_64x64].copy_ss = PFX(blockcopy_ss_64x64_neon);

        // Blockcopy_ps
        p.cu[BLOCK_4x4].copy_ps   = PFX(blockcopy_ps_4x4_neon);
        p.cu[BLOCK_8x8].copy_ps   = PFX(blockcopy_ps_8x8_neon);
        p.cu[BLOCK_16x16].copy_ps = PFX(blockcopy_ps_16x16_neon);
        p.cu[BLOCK_32x32].copy_ps = PFX(blockcopy_ps_32x32_neon);
        p.cu[BLOCK_64x64].copy_ps = PFX(blockcopy_ps_64x64_neon);

        // Blockcopy_sp
        p.cu[BLOCK_4x4].copy_sp   = PFX(blockcopy_sp_4x4_neon);
        p.cu[BLOCK_8x8].copy_sp   = PFX(blockcopy_sp_8x8_neon);
        p.cu[BLOCK_16x16].copy_sp = PFX(blockcopy_sp_16x16_neon);
        p.cu[BLOCK_32x32].copy_sp = PFX(blockcopy_sp_32x32_neon);
        p.cu[BLOCK_64x64].copy_sp = PFX(blockcopy_sp_64x64_neon);

        // chroma blockcopy_ss
        p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].copy_ss   = PFX(blockcopy_ss_4x4_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].copy_ss   = PFX(blockcopy_ss_8x8_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].copy_ss = PFX(blockcopy_ss_16x16_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].copy_ss = PFX(blockcopy_ss_32x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].copy_ss   = PFX(blockcopy_ss_4x8_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].copy_ss  = PFX(blockcopy_ss_8x16_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].copy_ss = PFX(blockcopy_ss_16x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].copy_ss = PFX(blockcopy_ss_32x64_neon);

        // chroma blockcopy_ps
        p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].copy_ps   = PFX(blockcopy_ps_4x4_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].copy_ps   = PFX(blockcopy_ps_8x8_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].copy_ps = PFX(blockcopy_ps_16x16_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].copy_ps = PFX(blockcopy_ps_32x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].copy_ps   = PFX(blockcopy_ps_4x8_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].copy_ps  = PFX(blockcopy_ps_8x16_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].copy_ps = PFX(blockcopy_ps_16x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].copy_ps = PFX(blockcopy_ps_32x64_neon);

        // chroma blockcopy_sp
        p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].copy_sp   = PFX(blockcopy_sp_4x4_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].copy_sp   = PFX(blockcopy_sp_8x8_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].copy_sp = PFX(blockcopy_sp_16x16_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].copy_sp = PFX(blockcopy_sp_32x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].copy_sp   = PFX(blockcopy_sp_4x8_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].copy_sp  = PFX(blockcopy_sp_8x16_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].copy_sp = PFX(blockcopy_sp_16x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].copy_sp = PFX(blockcopy_sp_32x64_neon);

        // pixel_add_ps
        p.cu[BLOCK_4x4].add_ps   = PFX(pixel_add_ps_4x4_neon);
        p.cu[BLOCK_8x8].add_ps   = PFX(pixel_add_ps_8x8_neon);
        p.cu[BLOCK_16x16].add_ps = PFX(pixel_add_ps_16x16_neon);
        p.cu[BLOCK_32x32].add_ps = PFX(pixel_add_ps_32x32_neon);
        p.cu[BLOCK_64x64].add_ps = PFX(pixel_add_ps_64x64_neon);

        // chroma add_ps
        p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].add_ps   = PFX(pixel_add_ps_4x4_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].add_ps   = PFX(pixel_add_ps_8x8_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].add_ps = PFX(pixel_add_ps_16x16_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].add_ps = PFX(pixel_add_ps_32x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].add_ps   = PFX(pixel_add_ps_4x8_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].add_ps  = PFX(pixel_add_ps_8x16_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].add_ps = PFX(pixel_add_ps_16x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].add_ps = PFX(pixel_add_ps_32x64_neon);

        // cpy2Dto1D_shr
        p.cu[BLOCK_4x4].cpy2Dto1D_shr   = PFX(cpy2Dto1D_shr_4x4_neon);
        p.cu[BLOCK_8x8].cpy2Dto1D_shr   = PFX(cpy2Dto1D_shr_8x8_neon);
        p.cu[BLOCK_16x16].cpy2Dto1D_shr = PFX(cpy2Dto1D_shr_16x16_neon);
        p.cu[BLOCK_32x32].cpy2Dto1D_shr = PFX(cpy2Dto1D_shr_32x32_neon);

        // ssd_s
        p.cu[BLOCK_4x4].ssd_s   = PFX(pixel_ssd_s_4x4_neon);
        p.cu[BLOCK_8x8].ssd_s   = PFX(pixel_ssd_s_8x8_neon);
        p.cu[BLOCK_16x16].ssd_s = PFX(pixel_ssd_s_16x16_neon);
        p.cu[BLOCK_32x32].ssd_s = PFX(pixel_ssd_s_32x32_neon);

        // sse_ss
        p.cu[BLOCK_4x4].sse_ss   = PFX(pixel_sse_ss_4x4_neon);
        p.cu[BLOCK_8x8].sse_ss   = PFX(pixel_sse_ss_8x8_neon);
        p.cu[BLOCK_16x16].sse_ss = PFX(pixel_sse_ss_16x16_neon);
        p.cu[BLOCK_32x32].sse_ss = PFX(pixel_sse_ss_32x32_neon);
        p.cu[BLOCK_64x64].sse_ss = PFX(pixel_sse_ss_64x64_neon);

        // pixel_sub_ps
        p.cu[BLOCK_4x4].sub_ps   = PFX(pixel_sub_ps_4x4_neon);
        p.cu[BLOCK_8x8].sub_ps   = PFX(pixel_sub_ps_8x8_neon);
        p.cu[BLOCK_16x16].sub_ps = PFX(pixel_sub_ps_16x16_neon);
        p.cu[BLOCK_32x32].sub_ps = PFX(pixel_sub_ps_32x32_neon);
        p.cu[BLOCK_64x64].sub_ps = PFX(pixel_sub_ps_64x64_neon);

        // chroma sub_ps
        p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].sub_ps   = PFX(pixel_sub_ps_4x4_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].sub_ps   = PFX(pixel_sub_ps_8x8_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sub_ps = PFX(pixel_sub_ps_16x16_neon);
        p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sub_ps = PFX(pixel_sub_ps_32x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].sub_ps   = PFX(pixel_sub_ps_4x8_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].sub_ps  = PFX(pixel_sub_ps_8x16_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].sub_ps = PFX(pixel_sub_ps_16x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].sub_ps = PFX(pixel_sub_ps_32x64_neon);

        // calc_Residual
        p.cu[BLOCK_4x4].calcresidual   = PFX(getResidual4_neon);
        p.cu[BLOCK_8x8].calcresidual   = PFX(getResidual8_neon);
        p.cu[BLOCK_16x16].calcresidual = PFX(getResidual16_neon);
        p.cu[BLOCK_32x32].calcresidual = PFX(getResidual32_neon);

        // sse_pp
        p.cu[BLOCK_4x4].sse_pp   = PFX(pixel_sse_pp_4x4_neon);
        p.cu[BLOCK_8x8].sse_pp   = PFX(pixel_sse_pp_8x8_neon);
        p.cu[BLOCK_16x16].sse_pp = PFX(pixel_sse_pp_16x16_neon);
        p.cu[BLOCK_32x32].sse_pp = PFX(pixel_sse_pp_32x32_neon);
        p.cu[BLOCK_64x64].sse_pp = PFX(pixel_sse_pp_64x64_neon);

        // pixel_var
        p.cu[BLOCK_8x8].var   = PFX(pixel_var_8x8_neon);
        p.cu[BLOCK_16x16].var = PFX(pixel_var_16x16_neon);
        p.cu[BLOCK_32x32].var = PFX(pixel_var_32x32_neon);
        p.cu[BLOCK_64x64].var = PFX(pixel_var_64x64_neon);

        // blockcopy
        p.pu[LUMA_16x16].copy_pp = PFX(blockcopy_pp_16x16_neon);
        p.pu[LUMA_8x4].copy_pp   = PFX(blockcopy_pp_8x4_neon);
        p.pu[LUMA_8x8].copy_pp   = PFX(blockcopy_pp_8x8_neon);
        p.pu[LUMA_8x16].copy_pp  = PFX(blockcopy_pp_8x16_neon);
        p.pu[LUMA_8x32].copy_pp  = PFX(blockcopy_pp_8x32_neon);
        p.pu[LUMA_12x16].copy_pp = PFX(blockcopy_pp_12x16_neon);
        p.pu[LUMA_4x4].copy_pp   = PFX(blockcopy_pp_4x4_neon);
        p.pu[LUMA_4x8].copy_pp   = PFX(blockcopy_pp_4x8_neon);
        p.pu[LUMA_4x16].copy_pp  = PFX(blockcopy_pp_4x16_neon);
        p.pu[LUMA_16x4].copy_pp  = PFX(blockcopy_pp_16x4_neon);
        p.pu[LUMA_16x8].copy_pp  = PFX(blockcopy_pp_16x8_neon);
        p.pu[LUMA_16x12].copy_pp = PFX(blockcopy_pp_16x12_neon);
        p.pu[LUMA_16x32].copy_pp = PFX(blockcopy_pp_16x32_neon);
        p.pu[LUMA_16x64].copy_pp = PFX(blockcopy_pp_16x64_neon);
        p.pu[LUMA_24x32].copy_pp = PFX(blockcopy_pp_24x32_neon);
        p.pu[LUMA_32x8].copy_pp  = PFX(blockcopy_pp_32x8_neon);
        p.pu[LUMA_32x16].copy_pp = PFX(blockcopy_pp_32x16_neon);
        p.pu[LUMA_32x24].copy_pp = PFX(blockcopy_pp_32x24_neon);
        p.pu[LUMA_32x32].copy_pp = PFX(blockcopy_pp_32x32_neon);
        p.pu[LUMA_32x64].copy_pp = PFX(blockcopy_pp_32x64_neon);
        p.pu[LUMA_48x64].copy_pp = PFX(blockcopy_pp_48x64_neon);
        p.pu[LUMA_64x16].copy_pp = PFX(blockcopy_pp_64x16_neon);
        p.pu[LUMA_64x32].copy_pp = PFX(blockcopy_pp_64x32_neon);
        p.pu[LUMA_64x48].copy_pp = PFX(blockcopy_pp_64x48_neon);
        p.pu[LUMA_64x64].copy_pp = PFX(blockcopy_pp_64x64_neon);

        // chroma blockcopy
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x4].copy_pp   = PFX(blockcopy_pp_2x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_2x8].copy_pp   = PFX(blockcopy_pp_2x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x2].copy_pp   = PFX(blockcopy_pp_4x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].copy_pp   = PFX(blockcopy_pp_4x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].copy_pp   = PFX(blockcopy_pp_4x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].copy_pp  = PFX(blockcopy_pp_4x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_6x8].copy_pp   = PFX(blockcopy_pp_6x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].copy_pp   = PFX(blockcopy_pp_8x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].copy_pp   = PFX(blockcopy_pp_8x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].copy_pp   = PFX(blockcopy_pp_8x6_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].copy_pp   = PFX(blockcopy_pp_8x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].copy_pp  = PFX(blockcopy_pp_8x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].copy_pp  = PFX(blockcopy_pp_8x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].copy_pp = PFX(blockcopy_pp_12x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].copy_pp  = PFX(blockcopy_pp_16x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].copy_pp  = PFX(blockcopy_pp_16x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].copy_pp = PFX(blockcopy_pp_16x12_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].copy_pp = PFX(blockcopy_pp_16x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].copy_pp = PFX(blockcopy_pp_16x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].copy_pp = PFX(blockcopy_pp_24x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].copy_pp  = PFX(blockcopy_pp_32x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].copy_pp = PFX(blockcopy_pp_32x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].copy_pp = PFX(blockcopy_pp_32x24_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].copy_pp = PFX(blockcopy_pp_32x32_neon);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_2x16].copy_pp  = PFX(blockcopy_pp_2x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].copy_pp   = PFX(blockcopy_pp_4x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].copy_pp   = PFX(blockcopy_pp_4x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].copy_pp  = PFX(blockcopy_pp_4x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_4x32].copy_pp  = PFX(blockcopy_pp_4x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_6x16].copy_pp  = PFX(blockcopy_pp_6x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].copy_pp   = PFX(blockcopy_pp_8x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].copy_pp   = PFX(blockcopy_pp_8x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].copy_pp  = PFX(blockcopy_pp_8x12_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].copy_pp  = PFX(blockcopy_pp_8x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].copy_pp  = PFX(blockcopy_pp_8x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].copy_pp  = PFX(blockcopy_pp_8x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_12x32].copy_pp = PFX(blockcopy_pp_12x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].copy_pp  = PFX(blockcopy_pp_16x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].copy_pp = PFX(blockcopy_pp_16x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].copy_pp = PFX(blockcopy_pp_16x24_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].copy_pp = PFX(blockcopy_pp_16x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].copy_pp = PFX(blockcopy_pp_16x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].copy_pp = PFX(blockcopy_pp_24x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].copy_pp = PFX(blockcopy_pp_32x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].copy_pp = PFX(blockcopy_pp_32x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].copy_pp = PFX(blockcopy_pp_32x48_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].copy_pp = PFX(blockcopy_pp_32x64_neon);

        // sad
        p.pu[LUMA_8x4].sad    = PFX(pixel_sad_8x4_neon);
        p.pu[LUMA_8x8].sad    = PFX(pixel_sad_8x8_neon);
        p.pu[LUMA_8x16].sad   = PFX(pixel_sad_8x16_neon);
        p.pu[LUMA_8x32].sad   = PFX(pixel_sad_8x32_neon);
        p.pu[LUMA_16x4].sad   = PFX(pixel_sad_16x4_neon);
        p.pu[LUMA_16x8].sad   = PFX(pixel_sad_16x8_neon);
        p.pu[LUMA_16x16].sad  = PFX(pixel_sad_16x16_neon);
        p.pu[LUMA_16x12].sad  = PFX(pixel_sad_16x12_neon);
        p.pu[LUMA_16x32].sad  = PFX(pixel_sad_16x32_neon);
        p.pu[LUMA_16x64].sad  = PFX(pixel_sad_16x64_neon);
        p.pu[LUMA_32x8].sad   = PFX(pixel_sad_32x8_neon);
        p.pu[LUMA_32x16].sad  = PFX(pixel_sad_32x16_neon);
        p.pu[LUMA_32x32].sad  = PFX(pixel_sad_32x32_neon);
        p.pu[LUMA_32x64].sad  = PFX(pixel_sad_32x64_neon);
        p.pu[LUMA_32x24].sad  = PFX(pixel_sad_32x24_neon);
        p.pu[LUMA_64x16].sad  = PFX(pixel_sad_64x16_neon);
        p.pu[LUMA_64x32].sad  = PFX(pixel_sad_64x32_neon);
        p.pu[LUMA_64x64].sad  = PFX(pixel_sad_64x64_neon);
        p.pu[LUMA_64x48].sad  = PFX(pixel_sad_64x48_neon);
        p.pu[LUMA_12x16].sad  = PFX(pixel_sad_12x16_neon);
        p.pu[LUMA_24x32].sad  = PFX(pixel_sad_24x32_neon);
        p.pu[LUMA_48x64].sad  = PFX(pixel_sad_48x64_neon);

        // sad_x3
        p.pu[LUMA_4x4].sad_x3   = PFX(sad_x3_4x4_neon);
        p.pu[LUMA_4x8].sad_x3   = PFX(sad_x3_4x8_neon);
        p.pu[LUMA_4x16].sad_x3  = PFX(sad_x3_4x16_neon);
        p.pu[LUMA_8x4].sad_x3   = PFX(sad_x3_8x4_neon);
        p.pu[LUMA_8x8].sad_x3   = PFX(sad_x3_8x8_neon);
        p.pu[LUMA_8x16].sad_x3  = PFX(sad_x3_8x16_neon);
        p.pu[LUMA_8x32].sad_x3  = PFX(sad_x3_8x32_neon);
        p.pu[LUMA_12x16].sad_x3 = PFX(sad_x3_12x16_neon);
        p.pu[LUMA_16x4].sad_x3  = PFX(sad_x3_16x4_neon);
        p.pu[LUMA_16x8].sad_x3  = PFX(sad_x3_16x8_neon);
        p.pu[LUMA_16x12].sad_x3 = PFX(sad_x3_16x12_neon);
        p.pu[LUMA_16x16].sad_x3 = PFX(sad_x3_16x16_neon);
        p.pu[LUMA_16x32].sad_x3 = PFX(sad_x3_16x32_neon);
        p.pu[LUMA_16x64].sad_x3 = PFX(sad_x3_16x64_neon);
        p.pu[LUMA_24x32].sad_x3 = PFX(sad_x3_24x32_neon);
        p.pu[LUMA_32x8].sad_x3  = PFX(sad_x3_32x8_neon);
        p.pu[LUMA_32x16].sad_x3 = PFX(sad_x3_32x16_neon);
        p.pu[LUMA_32x24].sad_x3 = PFX(sad_x3_32x24_neon);
        p.pu[LUMA_32x32].sad_x3 = PFX(sad_x3_32x32_neon);
        p.pu[LUMA_32x64].sad_x3 = PFX(sad_x3_32x64_neon);
        p.pu[LUMA_48x64].sad_x3 = PFX(sad_x3_48x64_neon);
        p.pu[LUMA_64x16].sad_x3 = PFX(sad_x3_64x16_neon);
        p.pu[LUMA_64x32].sad_x3 = PFX(sad_x3_64x32_neon);
        p.pu[LUMA_64x48].sad_x3 = PFX(sad_x3_64x48_neon);
        p.pu[LUMA_64x64].sad_x3 = PFX(sad_x3_64x64_neon);

        // sad_x4
        p.pu[LUMA_4x4].sad_x4   = PFX(sad_x4_4x4_neon);
        p.pu[LUMA_4x8].sad_x4   = PFX(sad_x4_4x8_neon);
        p.pu[LUMA_4x16].sad_x4  = PFX(sad_x4_4x16_neon);
        p.pu[LUMA_8x4].sad_x4   = PFX(sad_x4_8x4_neon);
        p.pu[LUMA_8x8].sad_x4   = PFX(sad_x4_8x8_neon);
        p.pu[LUMA_8x16].sad_x4  = PFX(sad_x4_8x16_neon);
        p.pu[LUMA_8x32].sad_x4  = PFX(sad_x4_8x32_neon);
        p.pu[LUMA_12x16].sad_x4 = PFX(sad_x4_12x16_neon);
        p.pu[LUMA_16x4].sad_x4  = PFX(sad_x4_16x4_neon);
        p.pu[LUMA_16x8].sad_x4  = PFX(sad_x4_16x8_neon);
        p.pu[LUMA_16x12].sad_x4 = PFX(sad_x4_16x12_neon);
        p.pu[LUMA_16x16].sad_x4 = PFX(sad_x4_16x16_neon);
        p.pu[LUMA_16x32].sad_x4 = PFX(sad_x4_16x32_neon);
        p.pu[LUMA_16x64].sad_x4 = PFX(sad_x4_16x64_neon);
        p.pu[LUMA_24x32].sad_x4 = PFX(sad_x4_24x32_neon);
        p.pu[LUMA_32x8].sad_x4  = PFX(sad_x4_32x8_neon);
        p.pu[LUMA_32x16].sad_x4 = PFX(sad_x4_32x16_neon);
        p.pu[LUMA_32x24].sad_x4 = PFX(sad_x4_32x24_neon);
        p.pu[LUMA_32x32].sad_x4 = PFX(sad_x4_32x32_neon);
        p.pu[LUMA_32x64].sad_x4 = PFX(sad_x4_32x64_neon);
        p.pu[LUMA_48x64].sad_x4 = PFX(sad_x4_48x64_neon);
        p.pu[LUMA_64x16].sad_x4 = PFX(sad_x4_64x16_neon);
        p.pu[LUMA_64x32].sad_x4 = PFX(sad_x4_64x32_neon);
        p.pu[LUMA_64x48].sad_x4 = PFX(sad_x4_64x48_neon);
        p.pu[LUMA_64x64].sad_x4 = PFX(sad_x4_64x64_neon);

        // pixel_avg_pp
        p.pu[LUMA_4x4].pixelavg_pp   = PFX(pixel_avg_pp_4x4_neon);
        p.pu[LUMA_4x8].pixelavg_pp   = PFX(pixel_avg_pp_4x8_neon);
        p.pu[LUMA_4x16].pixelavg_pp  = PFX(pixel_avg_pp_4x16_neon);
        p.pu[LUMA_8x4].pixelavg_pp   = PFX(pixel_avg_pp_8x4_neon);
        p.pu[LUMA_8x8].pixelavg_pp   = PFX(pixel_avg_pp_8x8_neon);
        p.pu[LUMA_8x16].pixelavg_pp  = PFX(pixel_avg_pp_8x16_neon);
        p.pu[LUMA_8x32].pixelavg_pp  = PFX(pixel_avg_pp_8x32_neon);
        p.pu[LUMA_12x16].pixelavg_pp = PFX(pixel_avg_pp_12x16_neon);
        p.pu[LUMA_16x4].pixelavg_pp  = PFX(pixel_avg_pp_16x4_neon);
        p.pu[LUMA_16x8].pixelavg_pp  = PFX(pixel_avg_pp_16x8_neon);
        p.pu[LUMA_16x12].pixelavg_pp = PFX(pixel_avg_pp_16x12_neon);
        p.pu[LUMA_16x16].pixelavg_pp = PFX(pixel_avg_pp_16x16_neon);
        p.pu[LUMA_16x32].pixelavg_pp = PFX(pixel_avg_pp_16x32_neon);
        p.pu[LUMA_16x64].pixelavg_pp = PFX(pixel_avg_pp_16x64_neon);
        p.pu[LUMA_24x32].pixelavg_pp = PFX(pixel_avg_pp_24x32_neon);
        p.pu[LUMA_32x8].pixelavg_pp  = PFX(pixel_avg_pp_32x8_neon);
        p.pu[LUMA_32x16].pixelavg_pp = PFX(pixel_avg_pp_32x16_neon);
        p.pu[LUMA_32x24].pixelavg_pp = PFX(pixel_avg_pp_32x24_neon);
        p.pu[LUMA_32x32].pixelavg_pp = PFX(pixel_avg_pp_32x32_neon);
        p.pu[LUMA_32x64].pixelavg_pp = PFX(pixel_avg_pp_32x64_neon);
        p.pu[LUMA_48x64].pixelavg_pp = PFX(pixel_avg_pp_48x64_neon);
        p.pu[LUMA_64x16].pixelavg_pp = PFX(pixel_avg_pp_64x16_neon);
        p.pu[LUMA_64x32].pixelavg_pp = PFX(pixel_avg_pp_64x32_neon);
        p.pu[LUMA_64x48].pixelavg_pp = PFX(pixel_avg_pp_64x48_neon);
        p.pu[LUMA_64x64].pixelavg_pp = PFX(pixel_avg_pp_64x64_neon);

        // planecopy
        p.planecopy_cp = PFX(pixel_planecopy_cp_neon);

        p.cu[BLOCK_4x4].sa8d   = PFX(pixel_satd_4x4_neon);
        p.cu[BLOCK_8x8].sa8d = PFX(pixel_sa8d_8x8_neon);
        p.cu[BLOCK_16x16].sa8d = PFX(pixel_sa8d_16x16_neon);
        p.cu[BLOCK_32x32].sa8d = PFX(pixel_sa8d_32x32_neon);
        p.cu[BLOCK_64x64].sa8d = PFX(pixel_sa8d_64x64_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_8x16].sa8d = PFX(pixel_sa8d_8x16_neon); 
        p.chroma[X265_CSP_I422].cu[BLOCK_422_16x32].sa8d = PFX(pixel_sa8d_16x32_neon);
        p.chroma[X265_CSP_I422].cu[BLOCK_422_32x64].sa8d = PFX(pixel_sa8d_32x64_neon);

        // vertical interpolation filters
        p.pu[LUMA_4x4].luma_vpp     = PFX(interp_8tap_vert_pp_4x4_neon);
        p.pu[LUMA_4x8].luma_vpp     = PFX(interp_8tap_vert_pp_4x8_neon);
        p.pu[LUMA_4x16].luma_vpp    = PFX(interp_8tap_vert_pp_4x16_neon);
        p.pu[LUMA_8x4].luma_vpp     = PFX(interp_8tap_vert_pp_8x4_neon);
        p.pu[LUMA_8x8].luma_vpp     = PFX(interp_8tap_vert_pp_8x8_neon);
        p.pu[LUMA_8x16].luma_vpp    = PFX(interp_8tap_vert_pp_8x16_neon);
        p.pu[LUMA_8x32].luma_vpp    = PFX(interp_8tap_vert_pp_8x32_neon);
        p.pu[LUMA_16x4].luma_vpp    = PFX(interp_8tap_vert_pp_16x4_neon);
        p.pu[LUMA_16x8].luma_vpp    = PFX(interp_8tap_vert_pp_16x8_neon);
        p.pu[LUMA_16x16].luma_vpp   = PFX(interp_8tap_vert_pp_16x16_neon);
        p.pu[LUMA_16x32].luma_vpp   = PFX(interp_8tap_vert_pp_16x32_neon);
        p.pu[LUMA_16x64].luma_vpp   = PFX(interp_8tap_vert_pp_16x64_neon);
        p.pu[LUMA_16x12].luma_vpp   = PFX(interp_8tap_vert_pp_16x12_neon);
        p.pu[LUMA_32x8].luma_vpp    = PFX(interp_8tap_vert_pp_32x8_neon);
        p.pu[LUMA_32x16].luma_vpp   = PFX(interp_8tap_vert_pp_32x16_neon);
        p.pu[LUMA_32x32].luma_vpp   = PFX(interp_8tap_vert_pp_32x32_neon);
        p.pu[LUMA_32x64].luma_vpp   = PFX(interp_8tap_vert_pp_32x64_neon);
        p.pu[LUMA_32x24].luma_vpp   = PFX(interp_8tap_vert_pp_32x24_neon);
        p.pu[LUMA_64x16].luma_vpp   = PFX(interp_8tap_vert_pp_64x16_neon);
        p.pu[LUMA_64x32].luma_vpp   = PFX(interp_8tap_vert_pp_64x32_neon);
        p.pu[LUMA_64x64].luma_vpp   = PFX(interp_8tap_vert_pp_64x64_neon);
        p.pu[LUMA_64x48].luma_vpp   = PFX(interp_8tap_vert_pp_64x48_neon);
        p.pu[LUMA_24x32].luma_vpp   = PFX(interp_8tap_vert_pp_24x32_neon);
        p.pu[LUMA_48x64].luma_vpp   = PFX(interp_8tap_vert_pp_48x64_neon);
        p.pu[LUMA_12x16].luma_vpp   = PFX(interp_8tap_vert_pp_12x16_neon);

        p.pu[LUMA_4x4].luma_vsp     = PFX(interp_8tap_vert_sp_4x4_neon);
        p.pu[LUMA_4x8].luma_vsp     = PFX(interp_8tap_vert_sp_4x8_neon);
        p.pu[LUMA_4x16].luma_vsp    = PFX(interp_8tap_vert_sp_4x16_neon);
        p.pu[LUMA_8x4].luma_vsp     = PFX(interp_8tap_vert_sp_8x4_neon);
        p.pu[LUMA_8x8].luma_vsp     = PFX(interp_8tap_vert_sp_8x8_neon);
        p.pu[LUMA_8x16].luma_vsp    = PFX(interp_8tap_vert_sp_8x16_neon);
        p.pu[LUMA_8x32].luma_vsp    = PFX(interp_8tap_vert_sp_8x32_neon);
        p.pu[LUMA_16x4].luma_vsp    = PFX(interp_8tap_vert_sp_16x4_neon);
        p.pu[LUMA_16x8].luma_vsp    = PFX(interp_8tap_vert_sp_16x8_neon);
        p.pu[LUMA_16x16].luma_vsp   = PFX(interp_8tap_vert_sp_16x16_neon);
        p.pu[LUMA_16x32].luma_vsp   = PFX(interp_8tap_vert_sp_16x32_neon);
        p.pu[LUMA_16x64].luma_vsp   = PFX(interp_8tap_vert_sp_16x64_neon);
        p.pu[LUMA_16x12].luma_vsp   = PFX(interp_8tap_vert_sp_16x12_neon);
        p.pu[LUMA_32x8].luma_vsp    = PFX(interp_8tap_vert_sp_32x8_neon);
        p.pu[LUMA_32x16].luma_vsp   = PFX(interp_8tap_vert_sp_32x16_neon);
        p.pu[LUMA_32x32].luma_vsp   = PFX(interp_8tap_vert_sp_32x32_neon);
        p.pu[LUMA_32x64].luma_vsp   = PFX(interp_8tap_vert_sp_32x64_neon);
        p.pu[LUMA_32x24].luma_vsp   = PFX(interp_8tap_vert_sp_32x24_neon);
        p.pu[LUMA_64x16].luma_vsp   = PFX(interp_8tap_vert_sp_64x16_neon);
        p.pu[LUMA_64x32].luma_vsp   = PFX(interp_8tap_vert_sp_64x32_neon);
        p.pu[LUMA_64x64].luma_vsp   = PFX(interp_8tap_vert_sp_64x64_neon);
        p.pu[LUMA_64x48].luma_vsp   = PFX(interp_8tap_vert_sp_64x48_neon);
        p.pu[LUMA_24x32].luma_vsp   = PFX(interp_8tap_vert_sp_24x32_neon);
        p.pu[LUMA_48x64].luma_vsp   = PFX(interp_8tap_vert_sp_48x64_neon);
        p.pu[LUMA_12x16].luma_vsp   = PFX(interp_8tap_vert_sp_12x16_neon);

        p.pu[LUMA_4x4].luma_vps     = PFX(interp_8tap_vert_ps_4x4_neon);
        p.pu[LUMA_4x8].luma_vps     = PFX(interp_8tap_vert_ps_4x8_neon);
        p.pu[LUMA_4x16].luma_vps    = PFX(interp_8tap_vert_ps_4x16_neon);
        p.pu[LUMA_8x4].luma_vps     = PFX(interp_8tap_vert_ps_8x4_neon);
        p.pu[LUMA_8x8].luma_vps     = PFX(interp_8tap_vert_ps_8x8_neon);
        p.pu[LUMA_8x16].luma_vps    = PFX(interp_8tap_vert_ps_8x16_neon);
        p.pu[LUMA_8x32].luma_vps    = PFX(interp_8tap_vert_ps_8x32_neon);
        p.pu[LUMA_16x4].luma_vps    = PFX(interp_8tap_vert_ps_16x4_neon);
        p.pu[LUMA_16x8].luma_vps    = PFX(interp_8tap_vert_ps_16x8_neon);
        p.pu[LUMA_16x16].luma_vps   = PFX(interp_8tap_vert_ps_16x16_neon);
        p.pu[LUMA_16x32].luma_vps   = PFX(interp_8tap_vert_ps_16x32_neon);
        p.pu[LUMA_16x64].luma_vps   = PFX(interp_8tap_vert_ps_16x64_neon);
        p.pu[LUMA_16x12].luma_vps   = PFX(interp_8tap_vert_ps_16x12_neon);
        p.pu[LUMA_32x8].luma_vps    = PFX(interp_8tap_vert_ps_32x8_neon);
        p.pu[LUMA_32x16].luma_vps   = PFX(interp_8tap_vert_ps_32x16_neon);
        p.pu[LUMA_32x32].luma_vps   = PFX(interp_8tap_vert_ps_32x32_neon);
        p.pu[LUMA_32x64].luma_vps   = PFX(interp_8tap_vert_ps_32x64_neon);
        p.pu[LUMA_32x24].luma_vps   = PFX(interp_8tap_vert_ps_32x24_neon);
        p.pu[LUMA_64x16].luma_vps   = PFX(interp_8tap_vert_ps_64x16_neon);
        p.pu[LUMA_64x32].luma_vps   = PFX(interp_8tap_vert_ps_64x32_neon);
        p.pu[LUMA_64x64].luma_vps   = PFX(interp_8tap_vert_ps_64x64_neon);
        p.pu[LUMA_64x48].luma_vps   = PFX(interp_8tap_vert_ps_64x48_neon);
        p.pu[LUMA_24x32].luma_vps   = PFX(interp_8tap_vert_ps_24x32_neon);
        p.pu[LUMA_48x64].luma_vps   = PFX(interp_8tap_vert_ps_48x64_neon);
        p.pu[LUMA_12x16].luma_vps   = PFX(interp_8tap_vert_ps_12x16_neon);

        //vertical chroma filters
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vpp = PFX(interp_4tap_vert_pp_8x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vpp = PFX(interp_4tap_vert_pp_8x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vpp = PFX(interp_4tap_vert_pp_8x6_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vpp = PFX(interp_4tap_vert_pp_8x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vpp = PFX(interp_4tap_vert_pp_8x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vpp = PFX(interp_4tap_vert_pp_8x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vpp = PFX(interp_4tap_vert_pp_16x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vpp = PFX(interp_4tap_vert_pp_16x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vpp = PFX(interp_4tap_vert_pp_16x12_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vpp = PFX(interp_4tap_vert_pp_16x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vpp = PFX(interp_4tap_vert_pp_16x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vpp = PFX(interp_4tap_vert_pp_32x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vpp = PFX(interp_4tap_vert_pp_32x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vpp = PFX(interp_4tap_vert_pp_32x24_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vpp = PFX(interp_4tap_vert_pp_32x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vpp = PFX(interp_4tap_vert_pp_24x32_neon);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vpp = PFX(interp_4tap_vert_pp_8x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vpp = PFX(interp_4tap_vert_pp_8x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vpp = PFX(interp_4tap_vert_pp_8x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vpp = PFX(interp_4tap_vert_pp_8x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vpp = PFX(interp_4tap_vert_pp_8x12_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vpp = PFX(interp_4tap_vert_pp_8x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vpp = PFX(interp_4tap_vert_pp_16x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vpp = PFX(interp_4tap_vert_pp_16x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vpp = PFX(interp_4tap_vert_pp_16x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vpp = PFX(interp_4tap_vert_pp_16x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vpp = PFX(interp_4tap_vert_pp_16x24_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vpp = PFX(interp_4tap_vert_pp_32x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vpp = PFX(interp_4tap_vert_pp_32x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vpp = PFX(interp_4tap_vert_pp_32x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vpp = PFX(interp_4tap_vert_pp_32x48_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vpp = PFX(interp_4tap_vert_pp_24x64_neon);

        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vpp = PFX(interp_4tap_vert_pp_8x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vpp = PFX(interp_4tap_vert_pp_8x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vpp = PFX(interp_4tap_vert_pp_8x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vpp = PFX(interp_4tap_vert_pp_8x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vpp = PFX(interp_4tap_vert_pp_16x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vpp = PFX(interp_4tap_vert_pp_16x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vpp = PFX(interp_4tap_vert_pp_16x12_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vpp = PFX(interp_4tap_vert_pp_16x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vpp = PFX(interp_4tap_vert_pp_16x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vpp = PFX(interp_4tap_vert_pp_16x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vpp = PFX(interp_4tap_vert_pp_32x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vpp = PFX(interp_4tap_vert_pp_32x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vpp = PFX(interp_4tap_vert_pp_32x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vpp = PFX(interp_4tap_vert_pp_32x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vpp = PFX(interp_4tap_vert_pp_64x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vpp = PFX(interp_4tap_vert_pp_64x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vpp = PFX(interp_4tap_vert_pp_64x48_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vpp = PFX(interp_4tap_vert_pp_64x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vpp = PFX(interp_4tap_vert_pp_24x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vpp = PFX(interp_4tap_vert_pp_48x64_neon);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vps = PFX(interp_4tap_vert_ps_8x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vps = PFX(interp_4tap_vert_ps_8x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vps = PFX(interp_4tap_vert_ps_8x6_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vps = PFX(interp_4tap_vert_ps_8x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vps = PFX(interp_4tap_vert_ps_8x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vps = PFX(interp_4tap_vert_ps_8x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vps = PFX(interp_4tap_vert_ps_16x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vps = PFX(interp_4tap_vert_ps_16x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vps = PFX(interp_4tap_vert_ps_16x12_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vps = PFX(interp_4tap_vert_ps_16x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vps = PFX(interp_4tap_vert_ps_16x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vps = PFX(interp_4tap_vert_ps_32x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vps = PFX(interp_4tap_vert_ps_32x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vps = PFX(interp_4tap_vert_ps_32x24_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vps = PFX(interp_4tap_vert_ps_32x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vps = PFX(interp_4tap_vert_ps_24x32_neon);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vps = PFX(interp_4tap_vert_ps_8x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vps = PFX(interp_4tap_vert_ps_8x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vps = PFX(interp_4tap_vert_ps_8x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vps = PFX(interp_4tap_vert_ps_8x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vps = PFX(interp_4tap_vert_ps_8x12_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vps = PFX(interp_4tap_vert_ps_8x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vps = PFX(interp_4tap_vert_ps_16x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vps = PFX(interp_4tap_vert_ps_16x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vps = PFX(interp_4tap_vert_ps_16x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vps = PFX(interp_4tap_vert_ps_16x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vps = PFX(interp_4tap_vert_ps_16x24_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vps = PFX(interp_4tap_vert_ps_32x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vps = PFX(interp_4tap_vert_ps_32x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vps = PFX(interp_4tap_vert_ps_32x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vps = PFX(interp_4tap_vert_ps_32x48_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vps = PFX(interp_4tap_vert_ps_24x64_neon);

        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vps = PFX(interp_4tap_vert_ps_8x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vps = PFX(interp_4tap_vert_ps_8x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vps = PFX(interp_4tap_vert_ps_8x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vps = PFX(interp_4tap_vert_ps_8x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vps = PFX(interp_4tap_vert_ps_16x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vps = PFX(interp_4tap_vert_ps_16x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vps = PFX(interp_4tap_vert_ps_16x12_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vps = PFX(interp_4tap_vert_ps_16x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vps = PFX(interp_4tap_vert_ps_16x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vps = PFX(interp_4tap_vert_ps_16x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vps = PFX(interp_4tap_vert_ps_32x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vps = PFX(interp_4tap_vert_ps_32x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vps = PFX(interp_4tap_vert_ps_32x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vps = PFX(interp_4tap_vert_ps_32x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vps = PFX(interp_4tap_vert_ps_64x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vps = PFX(interp_4tap_vert_ps_64x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vps = PFX(interp_4tap_vert_ps_64x48_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vps = PFX(interp_4tap_vert_ps_64x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vps = PFX(interp_4tap_vert_ps_24x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vps = PFX(interp_4tap_vert_ps_48x64_neon);

        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x2].filter_vsp = PFX(interp_4tap_vert_sp_8x2_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].filter_vsp = PFX(interp_4tap_vert_sp_8x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x6].filter_vsp = PFX(interp_4tap_vert_sp_8x6_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].filter_vsp = PFX(interp_4tap_vert_sp_8x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].filter_vsp = PFX(interp_4tap_vert_sp_8x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].filter_vsp = PFX(interp_4tap_vert_sp_8x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].filter_vsp = PFX(interp_4tap_vert_sp_16x4_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].filter_vsp = PFX(interp_4tap_vert_sp_16x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].filter_vsp = PFX(interp_4tap_vert_sp_16x12_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].filter_vsp = PFX(interp_4tap_vert_sp_16x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].filter_vsp = PFX(interp_4tap_vert_sp_16x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].filter_vsp = PFX(interp_4tap_vert_sp_32x8_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].filter_vsp = PFX(interp_4tap_vert_sp_32x16_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].filter_vsp = PFX(interp_4tap_vert_sp_32x24_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].filter_vsp = PFX(interp_4tap_vert_sp_32x32_neon);
        p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].filter_vsp = PFX(interp_4tap_vert_sp_24x32_neon);

        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].filter_vsp = PFX(interp_4tap_vert_sp_8x4_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].filter_vsp = PFX(interp_4tap_vert_sp_8x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].filter_vsp = PFX(interp_4tap_vert_sp_8x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].filter_vsp = PFX(interp_4tap_vert_sp_8x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x12].filter_vsp = PFX(interp_4tap_vert_sp_8x12_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_8x64].filter_vsp = PFX(interp_4tap_vert_sp_8x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].filter_vsp = PFX(interp_4tap_vert_sp_16x8_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].filter_vsp = PFX(interp_4tap_vert_sp_16x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].filter_vsp = PFX(interp_4tap_vert_sp_16x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].filter_vsp = PFX(interp_4tap_vert_sp_16x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_16x24].filter_vsp = PFX(interp_4tap_vert_sp_16x24_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].filter_vsp = PFX(interp_4tap_vert_sp_32x16_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].filter_vsp = PFX(interp_4tap_vert_sp_32x32_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].filter_vsp = PFX(interp_4tap_vert_sp_32x64_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_32x48].filter_vsp = PFX(interp_4tap_vert_sp_32x48_neon);
        p.chroma[X265_CSP_I422].pu[CHROMA_422_24x64].filter_vsp = PFX(interp_4tap_vert_sp_24x64_neon);

        p.chroma[X265_CSP_I444].pu[LUMA_8x4].filter_vsp = PFX(interp_4tap_vert_sp_8x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x8].filter_vsp = PFX(interp_4tap_vert_sp_8x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x16].filter_vsp = PFX(interp_4tap_vert_sp_8x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_8x32].filter_vsp = PFX(interp_4tap_vert_sp_8x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x4].filter_vsp = PFX(interp_4tap_vert_sp_16x4_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x8].filter_vsp = PFX(interp_4tap_vert_sp_16x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x12].filter_vsp = PFX(interp_4tap_vert_sp_16x12_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x16].filter_vsp = PFX(interp_4tap_vert_sp_16x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x32].filter_vsp = PFX(interp_4tap_vert_sp_16x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_16x64].filter_vsp = PFX(interp_4tap_vert_sp_16x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x8].filter_vsp = PFX(interp_4tap_vert_sp_32x8_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x16].filter_vsp = PFX(interp_4tap_vert_sp_32x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x32].filter_vsp = PFX(interp_4tap_vert_sp_32x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_32x64].filter_vsp = PFX(interp_4tap_vert_sp_32x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x16].filter_vsp = PFX(interp_4tap_vert_sp_64x16_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x32].filter_vsp = PFX(interp_4tap_vert_sp_64x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x48].filter_vsp = PFX(interp_4tap_vert_sp_64x48_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_64x64].filter_vsp = PFX(interp_4tap_vert_sp_64x64_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_24x32].filter_vsp = PFX(interp_4tap_vert_sp_24x32_neon);
        p.chroma[X265_CSP_I444].pu[LUMA_48x64].filter_vsp = PFX(interp_4tap_vert_sp_48x64_neon);

        p.cu[BLOCK_4x4].dct = PFX(dct_4x4_neon);
        p.cu[BLOCK_8x8].dct = PFX(dct_8x8_neon);
        p.cu[BLOCK_16x16].dct = PFX(dct_16x16_neon);
#if !HIGH_BIT_DEPTH
        p.cu[BLOCK_4x4].psy_cost_pp = PFX(psyCost_4x4_neon);
        p.cu[BLOCK_8x8].psy_cost_pp = PFX(psyCost_8x8_neon);
#endif // !HIGH_BIT_DEPTH
    }
    if (cpuMask & X265_CPU_ARMV6)
    {
        p.pu[LUMA_4x4].sad = PFX(pixel_sad_4x4_armv6);
        p.pu[LUMA_4x8].sad = PFX(pixel_sad_4x8_armv6);
        p.pu[LUMA_4x16].sad=PFX(pixel_sad_4x16_armv6);
    }
}
} // namespace X265_NS
