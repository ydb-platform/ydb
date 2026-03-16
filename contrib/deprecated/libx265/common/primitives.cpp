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

#include "common.h"
#include "primitives.h"

namespace X265_NS {
// x265 private namespace

extern const uint8_t lumaPartitionMapTable[] =
{
//  4          8          12          16          20  24          28  32          36  40  44  48          52  56  60  64
    LUMA_4x4,  LUMA_4x8,  255,        LUMA_4x16,  255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 4
    LUMA_8x4,  LUMA_8x8,  255,        LUMA_8x16,  255, 255,        255, LUMA_8x32,  255, 255, 255, 255,        255, 255, 255, 255,        // 8
    255,        255,      255,        LUMA_12x16, 255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 12
    LUMA_16x4, LUMA_16x8, LUMA_16x12, LUMA_16x16, 255, 255,        255, LUMA_16x32, 255, 255, 255, 255,        255, 255, 255, LUMA_16x64, // 16
    255,        255,      255,        255,        255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 20
    255,        255,      255,        255,        255, 255,        255, LUMA_24x32, 255, 255, 255, 255,        255, 255, 255, 255,        // 24
    255,        255,      255,        255,        255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 28
    255,        LUMA_32x8, 255,       LUMA_32x16, 255, LUMA_32x24, 255, LUMA_32x32, 255, 255, 255, 255,        255, 255, 255, LUMA_32x64, // 32
    255,        255,      255,        255,        255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 36
    255,        255,      255,        255,        255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 40
    255,        255,      255,        255,        255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 44
    255,        255,      255,        255,        255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, LUMA_48x64, // 48
    255,        255,      255,        255,        255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 52
    255,        255,      255,        255,        255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 56
    255,        255,      255,        255,        255, 255,        255, 255,        255, 255, 255, 255,        255, 255, 255, 255,        // 60
    255,        255,      255,        LUMA_64x16, 255, 255,        255, LUMA_64x32, 255, 255, 255, LUMA_64x48, 255, 255, 255, LUMA_64x64  // 64
};

/* the "authoritative" set of encoder primitives */
EncoderPrimitives primitives;

void setupPixelPrimitives_c(EncoderPrimitives &p);
void setupDCTPrimitives_c(EncoderPrimitives &p);
void setupFilterPrimitives_c(EncoderPrimitives &p);
void setupIntraPrimitives_c(EncoderPrimitives &p);
void setupLoopFilterPrimitives_c(EncoderPrimitives &p);
void setupSaoPrimitives_c(EncoderPrimitives &p);
void setupSeaIntegralPrimitives_c(EncoderPrimitives &p);
void setupLowPassPrimitives_c(EncoderPrimitives& p);

void setupCPrimitives(EncoderPrimitives &p)
{
    setupPixelPrimitives_c(p);      // pixel.cpp
    setupDCTPrimitives_c(p);        // dct.cpp
    setupLowPassPrimitives_c(p);    // lowpassdct.cpp
    setupFilterPrimitives_c(p);     // ipfilter.cpp
    setupIntraPrimitives_c(p);      // intrapred.cpp
    setupLoopFilterPrimitives_c(p); // loopfilter.cpp
    setupSaoPrimitives_c(p);        // sao.cpp
    setupSeaIntegralPrimitives_c(p);  // framefilter.cpp
}

void enableLowpassDCTPrimitives(EncoderPrimitives &p)
{
    // update copies of the standard dct transform
    p.cu[BLOCK_4x4].standard_dct = p.cu[BLOCK_4x4].dct;
    p.cu[BLOCK_8x8].standard_dct = p.cu[BLOCK_8x8].dct;
    p.cu[BLOCK_16x16].standard_dct = p.cu[BLOCK_16x16].dct;
    p.cu[BLOCK_32x32].standard_dct = p.cu[BLOCK_32x32].dct;

    // replace active dct by lowpass dct for high dct transforms
    p.cu[BLOCK_16x16].dct = p.cu[BLOCK_16x16].lowpass_dct;
    p.cu[BLOCK_32x32].dct = p.cu[BLOCK_32x32].lowpass_dct;
}

void setupAliasPrimitives(EncoderPrimitives &p)
{
#if HIGH_BIT_DEPTH
    /* at HIGH_BIT_DEPTH, pixel == short so we can alias many primitives */
    for (int i = 0; i < NUM_CU_SIZES; i++)
    {
        p.cu[i].sse_pp = (pixel_sse_t)p.cu[i].sse_ss;

        p.cu[i].copy_ps = (copy_ps_t)p.pu[i].copy_pp;
        p.cu[i].copy_sp = (copy_sp_t)p.pu[i].copy_pp;
        p.cu[i].copy_ss = (copy_ss_t)p.pu[i].copy_pp;

        p.chroma[X265_CSP_I420].cu[i].copy_ps = (copy_ps_t)p.chroma[X265_CSP_I420].pu[i].copy_pp;
        p.chroma[X265_CSP_I420].cu[i].copy_sp = (copy_sp_t)p.chroma[X265_CSP_I420].pu[i].copy_pp;
        p.chroma[X265_CSP_I420].cu[i].copy_ss = (copy_ss_t)p.chroma[X265_CSP_I420].pu[i].copy_pp;

        p.chroma[X265_CSP_I422].cu[i].copy_ps = (copy_ps_t)p.chroma[X265_CSP_I422].pu[i].copy_pp;
        p.chroma[X265_CSP_I422].cu[i].copy_sp = (copy_sp_t)p.chroma[X265_CSP_I422].pu[i].copy_pp;
        p.chroma[X265_CSP_I422].cu[i].copy_ss = (copy_ss_t)p.chroma[X265_CSP_I422].pu[i].copy_pp;
    }
#endif

    /* alias chroma 4:4:4 from luma primitives (all but chroma filters) */

    p.chroma[X265_CSP_I444].cu[BLOCK_4x4].sa8d = NULL;

    for (int i = 0; i < NUM_PU_SIZES; i++)
    {
        p.chroma[X265_CSP_I444].pu[i].copy_pp = p.pu[i].copy_pp;
        p.chroma[X265_CSP_I444].pu[i].addAvg  = p.pu[i].addAvg;
        p.chroma[X265_CSP_I444].pu[i].satd    = p.pu[i].satd;
        p.chroma[X265_CSP_I444].pu[i].p2s     = p.pu[i].convert_p2s;
    }

    for (int i = 0; i < NUM_CU_SIZES; i++)
    {
        p.chroma[X265_CSP_I444].cu[i].sa8d    = p.cu[i].sa8d;
        p.chroma[X265_CSP_I444].cu[i].sse_pp  = p.cu[i].sse_pp;
        p.chroma[X265_CSP_I444].cu[i].sub_ps  = p.cu[i].sub_ps;
        p.chroma[X265_CSP_I444].cu[i].add_ps  = p.cu[i].add_ps;
        p.chroma[X265_CSP_I444].cu[i].copy_ps = p.cu[i].copy_ps;
        p.chroma[X265_CSP_I444].cu[i].copy_sp = p.cu[i].copy_sp;
        p.chroma[X265_CSP_I444].cu[i].copy_ss = p.cu[i].copy_ss;
    }

    p.cu[BLOCK_4x4].sa8d = p.pu[LUMA_4x4].satd;

    /* Chroma PU can often use luma satd primitives */
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x4].satd   = p.pu[LUMA_4x4].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x8].satd   = p.pu[LUMA_8x8].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x16].satd = p.pu[LUMA_16x16].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x32].satd = p.pu[LUMA_32x32].satd;

    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x4].satd   = p.pu[LUMA_8x4].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x8].satd   = p.pu[LUMA_4x8].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x8].satd  = p.pu[LUMA_16x8].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x16].satd  = p.pu[LUMA_8x16].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x16].satd = p.pu[LUMA_32x16].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x32].satd = p.pu[LUMA_16x32].satd;

    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x12].satd = p.pu[LUMA_16x12].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_12x16].satd = p.pu[LUMA_12x16].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_16x4].satd  = p.pu[LUMA_16x4].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_4x16].satd  = p.pu[LUMA_4x16].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x24].satd = p.pu[LUMA_32x24].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_24x32].satd = p.pu[LUMA_24x32].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_32x8].satd  = p.pu[LUMA_32x8].satd;
    p.chroma[X265_CSP_I420].pu[CHROMA_420_8x32].satd  = p.pu[LUMA_8x32].satd;

    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x8].satd   = p.pu[LUMA_4x8].satd;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x16].satd  = p.pu[LUMA_8x16].satd;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x32].satd = p.pu[LUMA_16x32].satd;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x64].satd = p.pu[LUMA_32x64].satd;

    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x4].satd   = p.pu[LUMA_4x4].satd;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x8].satd   = p.pu[LUMA_8x8].satd;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_4x16].satd  = p.pu[LUMA_4x16].satd;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x16].satd = p.pu[LUMA_16x16].satd;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x32].satd  = p.pu[LUMA_8x32].satd;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x32].satd = p.pu[LUMA_32x32].satd;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x64].satd = p.pu[LUMA_16x64].satd;

    //p.chroma[X265_CSP_I422].satd[CHROMA_422_8x12]  = satd4<8, 12>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_8x4].satd  = p.pu[LUMA_8x4].satd;
    //p.chroma[X265_CSP_I422].satd[CHROMA_422_16x24] = satd8<16, 24>;
    //p.chroma[X265_CSP_I422].satd[CHROMA_422_12x32] = satd4<12, 32>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_16x8].satd = p.pu[LUMA_16x8].satd;
    //p.chroma[X265_CSP_I422].satd[CHROMA_422_4x32]  = satd4<4, 32>;
    //p.chroma[X265_CSP_I422].satd[CHROMA_422_32x48] = satd8<32, 48>;
    //p.chroma[X265_CSP_I422].satd[CHROMA_422_24x64] = satd8<24, 64>;
    p.chroma[X265_CSP_I422].pu[CHROMA_422_32x16].satd = p.pu[LUMA_32x16].satd;
    //p.chroma[X265_CSP_I422].satd[CHROMA_422_8x64]  = satd8<8, 64>;

    p.chroma[X265_CSP_I420].cu[BLOCK_420_2x2].sa8d = NULL;
    p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].sa8d = p.pu[LUMA_4x4].satd;
    p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].sa8d = p.cu[BLOCK_8x8].sa8d;
    p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sa8d = p.cu[BLOCK_16x16].sa8d;
    p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sa8d = p.cu[BLOCK_32x32].sa8d;

    p.chroma[X265_CSP_I422].cu[BLOCK_422_2x4].sa8d = NULL;
    p.chroma[X265_CSP_I422].cu[BLOCK_422_4x8].sa8d = p.pu[LUMA_4x8].satd;

    /* alias CU copy_pp from square PU copy_pp */
    for (int i = 0; i < NUM_CU_SIZES; i++)
    {
        p.cu[i].copy_pp = p.pu[i].copy_pp;

        for (int c = 0; c < X265_CSP_COUNT; c++)
            p.chroma[c].cu[i].copy_pp = p.chroma[c].pu[i].copy_pp;
    }

    p.chroma[X265_CSP_I420].cu[BLOCK_420_2x2].sse_pp = NULL;
    p.chroma[X265_CSP_I420].cu[BLOCK_420_4x4].sse_pp = p.cu[BLOCK_4x4].sse_pp;
    p.chroma[X265_CSP_I420].cu[BLOCK_420_8x8].sse_pp = p.cu[BLOCK_8x8].sse_pp;
    p.chroma[X265_CSP_I420].cu[BLOCK_420_16x16].sse_pp = p.cu[BLOCK_16x16].sse_pp;
    p.chroma[X265_CSP_I420].cu[BLOCK_420_32x32].sse_pp = p.cu[BLOCK_32x32].sse_pp;

    p.chroma[X265_CSP_I422].cu[BLOCK_422_2x4].sse_pp = NULL;
}

void x265_report_simd(x265_param* param)
{
    if (param->logLevel >= X265_LOG_INFO)
    {
        int cpuid = param->cpuid;

        char buf[1000];
        char *p = buf + sprintf(buf, "using cpu capabilities:");
        char *none = p;
        for (int i = 0; X265_NS::cpu_names[i].flags; i++)
        {
            if (!strcmp(X265_NS::cpu_names[i].name, "SSE")
                && (cpuid & X265_CPU_SSE2))
                continue;
            if (!strcmp(X265_NS::cpu_names[i].name, "SSE2")
                && (cpuid & (X265_CPU_SSE2_IS_FAST | X265_CPU_SSE2_IS_SLOW)))
                continue;
            if (!strcmp(X265_NS::cpu_names[i].name, "SSE3")
                && (cpuid & X265_CPU_SSSE3 || !(cpuid & X265_CPU_CACHELINE_64)))
                continue;
            if (!strcmp(X265_NS::cpu_names[i].name, "SSE4.1")
                && (cpuid & X265_CPU_SSE42))
                continue;
            if (!strcmp(X265_NS::cpu_names[i].name, "BMI1")
                && (cpuid & X265_CPU_BMI2))
                continue;
            if ((cpuid & X265_NS::cpu_names[i].flags) == X265_NS::cpu_names[i].flags
                && (!i || X265_NS::cpu_names[i].flags != X265_NS::cpu_names[i - 1].flags))
                p += sprintf(p, " %s", X265_NS::cpu_names[i].name);
        }

        if (p == none)
            sprintf(p, " none!");
        x265_log(param, X265_LOG_INFO, "%s\n", buf);
    }
}

void x265_setup_primitives(x265_param *param)
{
    if (!primitives.pu[0].sad)
    {
        setupCPrimitives(primitives);

        /* We do not want the encoder to use the un-optimized intra all-angles
         * C references. It is better to call the individual angle functions
         * instead. We must check for NULL before using this primitive */
        for (int i = 0; i < NUM_TR_SIZE; i++)
            primitives.cu[i].intra_pred_allangs = NULL;

#if ENABLE_ASSEMBLY
#if X265_ARCH_X86
        setupInstrinsicPrimitives(primitives, param->cpuid);
#endif
        setupAssemblyPrimitives(primitives, param->cpuid);
#endif
#if HAVE_ALTIVEC
        if (param->cpuid & X265_CPU_ALTIVEC)
        {
            setupPixelPrimitives_altivec(primitives);       // pixel_altivec.cpp, overwrite the initialization for altivec optimizated functions
            setupDCTPrimitives_altivec(primitives);         // dct_altivec.cpp, overwrite the initialization for altivec optimizated functions
            setupFilterPrimitives_altivec(primitives);      // ipfilter.cpp, overwrite the initialization for altivec optimizated functions
            setupIntraPrimitives_altivec(primitives);       // intrapred_altivec.cpp, overwrite the initialization for altivec optimizated functions
        }
#endif

        setupAliasPrimitives(primitives);

        if (param->bLowPassDct)
        {
            enableLowpassDCTPrimitives(primitives); 
        }
    }

    x265_report_simd(param);
}
}

#if ENABLE_ASSEMBLY && X265_ARCH_X86
/* these functions are implemented in assembly. When assembly is not being
 * compiled, they are unnecessary and can be NOPs */
#else
extern "C" {
int PFX(cpu_cpuid_test)(void) { return 0; }
void PFX(cpu_emms)(void) {}
void PFX(cpu_cpuid)(uint32_t, uint32_t *eax, uint32_t *, uint32_t *, uint32_t *) { *eax = 0; }
void PFX(cpu_xgetbv)(uint32_t, uint32_t *, uint32_t *) {}

#if X265_ARCH_ARM == 0
void PFX(cpu_neon_test)(void) {}
int PFX(cpu_fast_neon_mrc_test)(void) { return 0; }
#endif // X265_ARCH_ARM
}
#endif
