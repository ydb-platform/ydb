/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Loren Merritt <lorenm@u.washington.edu>
 *          Laurent Aimar <fenrir@via.ecp.fr>
 *          Fiona Glaser <fiona@x264.com>
 *          Steve Borho <steve@borho.org>
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

#include "cpu.h"
#include "common.h"

#if MACOS || SYS_FREEBSD
#include <sys/types.h>
#include <sys/sysctl.h>
#endif
#if SYS_OPENBSD
#include <sys/param.h>
#include <sys/sysctl.h>
#include <machine/cpu.h>
#endif

#if X265_ARCH_ARM && !defined(HAVE_NEON)
#include <signal.h>
#include <setjmp.h>
static sigjmp_buf jmpbuf;
static volatile sig_atomic_t canjump = 0;

static void sigill_handler(int sig)
{
    if (!canjump)
    {
        signal(sig, SIG_DFL);
        raise(sig);
    }

    canjump = 0;
    siglongjmp(jmpbuf, 1);
}

#endif // if X265_ARCH_ARM

namespace X265_NS {
const cpu_name_t cpu_names[] =
{
#if X265_ARCH_X86
#define MMX2 X265_CPU_MMX | X265_CPU_MMX2 | X265_CPU_CMOV
    { "MMX2",        MMX2 },
    { "MMXEXT",      MMX2 },
    { "SSE",         MMX2 | X265_CPU_SSE },
#define SSE2 MMX2 | X265_CPU_SSE | X265_CPU_SSE2
    { "SSE2Slow",    SSE2 | X265_CPU_SSE2_IS_SLOW },
    { "SSE2",        SSE2 },
    { "SSE2Fast",    SSE2 | X265_CPU_SSE2_IS_FAST },
    { "LZCNT", X265_CPU_LZCNT },
    { "SSE3",        SSE2 | X265_CPU_SSE3 },
    { "SSSE3",       SSE2 | X265_CPU_SSE3 | X265_CPU_SSSE3 },
    { "SSE4.1",      SSE2 | X265_CPU_SSE3 | X265_CPU_SSSE3 | X265_CPU_SSE4 },
    { "SSE4",        SSE2 | X265_CPU_SSE3 | X265_CPU_SSSE3 | X265_CPU_SSE4 },
    { "SSE4.2",      SSE2 | X265_CPU_SSE3 | X265_CPU_SSSE3 | X265_CPU_SSE4 | X265_CPU_SSE42 },
#define AVX SSE2 | X265_CPU_SSE3 | X265_CPU_SSSE3 | X265_CPU_SSE4 | X265_CPU_SSE42 | X265_CPU_AVX
    { "AVX",         AVX },
    { "XOP",         AVX | X265_CPU_XOP },
    { "FMA4",        AVX | X265_CPU_FMA4 },
    { "FMA3",        AVX | X265_CPU_FMA3 },
    { "BMI1",        AVX | X265_CPU_LZCNT | X265_CPU_BMI1 },
    { "BMI2",        AVX | X265_CPU_LZCNT | X265_CPU_BMI1 | X265_CPU_BMI2 },
#define AVX2 AVX | X265_CPU_FMA3 | X265_CPU_LZCNT | X265_CPU_BMI1 | X265_CPU_BMI2 | X265_CPU_AVX2
    { "AVX2", AVX2},
#undef AVX2
#undef AVX
#undef SSE2
#undef MMX2
    { "Cache32",         X265_CPU_CACHELINE_32 },
    { "Cache64",         X265_CPU_CACHELINE_64 },
    { "SlowCTZ",         X265_CPU_SLOW_CTZ },
    { "SlowAtom",        X265_CPU_SLOW_ATOM },
    { "SlowPshufb",      X265_CPU_SLOW_PSHUFB },
    { "SlowPalignr",     X265_CPU_SLOW_PALIGNR },
    { "SlowShuffle",     X265_CPU_SLOW_SHUFFLE },
    { "UnalignedStack",  X265_CPU_STACK_MOD4 },

#elif X265_ARCH_ARM
    { "ARMv6",           X265_CPU_ARMV6 },
    { "NEON",            X265_CPU_NEON },
    { "FastNeonMRC",     X265_CPU_FAST_NEON_MRC },

#elif X265_ARCH_POWER8
    { "Altivec",         X265_CPU_ALTIVEC },

#endif // if X265_ARCH_X86
    { "", 0 },
};

#if X265_ARCH_X86

extern "C" {
/* cpu-a.asm */
int PFX(cpu_cpuid_test)(void);
void PFX(cpu_cpuid)(uint32_t op, uint32_t *eax, uint32_t *ebx, uint32_t *ecx, uint32_t *edx);
void PFX(cpu_xgetbv)(uint32_t op, uint32_t *eax, uint32_t *edx);
}

#if defined(_MSC_VER)
#pragma warning(disable: 4309) // truncation of constant value
#endif

uint32_t cpu_detect(void)
{
    uint32_t cpu = 0;

    uint32_t eax, ebx, ecx, edx;
    uint32_t vendor[4] = { 0 };
    uint32_t max_extended_cap, max_basic_cap;

#if !X86_64
    if (!PFX(cpu_cpuid_test)())
        return 0;
#endif

    PFX(cpu_cpuid)(0, &eax, vendor + 0, vendor + 2, vendor + 1);
    max_basic_cap = eax;
    if (max_basic_cap == 0)
        return 0;

    PFX(cpu_cpuid)(1, &eax, &ebx, &ecx, &edx);
    if (edx & 0x00800000)
        cpu |= X265_CPU_MMX;
    else
        return cpu;
    if (edx & 0x02000000)
        cpu |= X265_CPU_MMX2 | X265_CPU_SSE;
    if (edx & 0x00008000)
        cpu |= X265_CPU_CMOV;
    else
        return cpu;
    if (edx & 0x04000000)
        cpu |= X265_CPU_SSE2;
    if (ecx & 0x00000001)
        cpu |= X265_CPU_SSE3;
    if (ecx & 0x00000200)
        cpu |= X265_CPU_SSSE3;
    if (ecx & 0x00080000)
        cpu |= X265_CPU_SSE4;
    if (ecx & 0x00100000)
        cpu |= X265_CPU_SSE42;
    /* Check OXSAVE and AVX bits */
    if ((ecx & 0x18000000) == 0x18000000)
    {
        /* Check for OS support */
        PFX(cpu_xgetbv)(0, &eax, &edx);
        if ((eax & 0x6) == 0x6)
        {
            cpu |= X265_CPU_AVX;
            if (ecx & 0x00001000)
                cpu |= X265_CPU_FMA3;
        }
    }

    if (max_basic_cap >= 7)
    {
        PFX(cpu_cpuid)(7, &eax, &ebx, &ecx, &edx);
        /* AVX2 requires OS support, but BMI1/2 don't. */
        if ((cpu & X265_CPU_AVX) && (ebx & 0x00000020))
            cpu |= X265_CPU_AVX2;
        if (ebx & 0x00000008)
        {
            cpu |= X265_CPU_BMI1;
            if (ebx & 0x00000100)
                cpu |= X265_CPU_BMI2;
        }
    }

    if (cpu & X265_CPU_SSSE3)
        cpu |= X265_CPU_SSE2_IS_FAST;

    PFX(cpu_cpuid)(0x80000000, &eax, &ebx, &ecx, &edx);
    max_extended_cap = eax;

    if (max_extended_cap >= 0x80000001)
    {
        PFX(cpu_cpuid)(0x80000001, &eax, &ebx, &ecx, &edx);

        if (ecx & 0x00000020)
            cpu |= X265_CPU_LZCNT; /* Supported by Intel chips starting with Haswell */
        if (ecx & 0x00000040) /* SSE4a, AMD only */
        {
            int family = ((eax >> 8) & 0xf) + ((eax >> 20) & 0xff);
            cpu |= X265_CPU_SSE2_IS_FAST;      /* Phenom and later CPUs have fast SSE units */
            if (family == 0x14)
            {
                cpu &= ~X265_CPU_SSE2_IS_FAST; /* SSSE3 doesn't imply fast SSE anymore... */
                cpu |= X265_CPU_SSE2_IS_SLOW;  /* Bobcat has 64-bit SIMD units */
                cpu |= X265_CPU_SLOW_PALIGNR;  /* palignr is insanely slow on Bobcat */
            }
            if (family == 0x16)
            {
                cpu |= X265_CPU_SLOW_PSHUFB;   /* Jaguar's pshufb isn't that slow, but it's slow enough
                                                * compared to alternate instruction sequences that this
                                                * is equal or faster on almost all such functions. */
            }
        }

        if (cpu & X265_CPU_AVX)
        {
            if (ecx & 0x00000800) /* XOP */
                cpu |= X265_CPU_XOP;
            if (ecx & 0x00010000) /* FMA4 */
                cpu |= X265_CPU_FMA4;
        }

        if (!strcmp((char*)vendor, "AuthenticAMD"))
        {
            if (edx & 0x00400000)
                cpu |= X265_CPU_MMX2;
            if (!(cpu & X265_CPU_LZCNT))
                cpu |= X265_CPU_SLOW_CTZ;
            if ((cpu & X265_CPU_SSE2) && !(cpu & X265_CPU_SSE2_IS_FAST))
                cpu |= X265_CPU_SSE2_IS_SLOW; /* AMD CPUs come in two types: terrible at SSE and great at it */
        }
    }

    if (!strcmp((char*)vendor, "GenuineIntel"))
    {
        PFX(cpu_cpuid)(1, &eax, &ebx, &ecx, &edx);
        int family = ((eax >> 8) & 0xf) + ((eax >> 20) & 0xff);
        int model  = ((eax >> 4) & 0xf) + ((eax >> 12) & 0xf0);
        if (family == 6)
        {
            /* 6/9 (pentium-m "banias"), 6/13 (pentium-m "dothan"), and 6/14 (core1 "yonah")
             * theoretically support sse2, but it's significantly slower than mmx for
             * almost all of x264's functions, so let's just pretend they don't. */
            if (model == 9 || model == 13 || model == 14)
            {
                cpu &= ~(X265_CPU_SSE2 | X265_CPU_SSE3);
                X265_CHECK(!(cpu & (X265_CPU_SSSE3 | X265_CPU_SSE4)), "unexpected CPU ID %d\n", cpu);
            }
            /* Detect Atom CPU */
            else if (model == 28)
            {
                cpu |= X265_CPU_SLOW_ATOM;
                cpu |= X265_CPU_SLOW_CTZ;
                cpu |= X265_CPU_SLOW_PSHUFB;
            }

            /* Conroe has a slow shuffle unit. Check the model number to make sure not
             * to include crippled low-end Penryns and Nehalems that don't have SSE4. */
            else if ((cpu & X265_CPU_SSSE3) && !(cpu & X265_CPU_SSE4) && model < 23)
                cpu |= X265_CPU_SLOW_SHUFFLE;
        }
    }

    if ((!strcmp((char*)vendor, "GenuineIntel") || !strcmp((char*)vendor, "CyrixInstead")) && !(cpu & X265_CPU_SSE42))
    {
        /* cacheline size is specified in 3 places, any of which may be missing */
        PFX(cpu_cpuid)(1, &eax, &ebx, &ecx, &edx);
        int cache = (ebx & 0xff00) >> 5; // cflush size
        if (!cache && max_extended_cap >= 0x80000006)
        {
            PFX(cpu_cpuid)(0x80000006, &eax, &ebx, &ecx, &edx);
            cache = ecx & 0xff; // cacheline size
        }
        if (!cache && max_basic_cap >= 2)
        {
            // Cache and TLB Information
            static const char cache32_ids[] = { '\x0a','\x0c','\x41','\x42','\x43','\x44','\x45','\x82','\x83','\x84','\x85','\0' };
            static const char cache64_ids[] = { '\x22','\x23','\x25','\x29','\x2c','\x46','\x47','\x49','\x60','\x66','\x67',
                                                '\x68','\x78','\x79','\x7a','\x7b','\x7c','\x7c','\x7f','\x86','\x87','\0' };
            uint32_t buf[4];
            int max, i = 0;
            do
            {
                PFX(cpu_cpuid)(2, buf + 0, buf + 1, buf + 2, buf + 3);
                max = buf[0] & 0xff;
                buf[0] &= ~0xff;
                for (int j = 0; j < 4; j++)
                {
                    if (!(buf[j] >> 31))
                        while (buf[j])
                        {
                            if (strchr(cache32_ids, buf[j] & 0xff))
                                cache = 32;
                            if (strchr(cache64_ids, buf[j] & 0xff))
                                cache = 64;
                            buf[j] >>= 8;
                        }
                }
            }
            while (++i < max);
        }

        if (cache == 32)
            cpu |= X265_CPU_CACHELINE_32;
        else if (cache == 64)
            cpu |= X265_CPU_CACHELINE_64;
        else
            x265_log(NULL, X265_LOG_WARNING, "unable to determine cacheline size\n");
    }

#if BROKEN_STACK_ALIGNMENT
    cpu |= X265_CPU_STACK_MOD4;
#endif

    return cpu;
}

#elif X265_ARCH_ARM

extern "C" {
void PFX(cpu_neon_test)(void);
int PFX(cpu_fast_neon_mrc_test)(void);
}

uint32_t cpu_detect(void)
{
    int flags = 0;

#if HAVE_ARMV6
    flags |= X265_CPU_ARMV6;

    // don't do this hack if compiled with -mfpu=neon
#if !HAVE_NEON
    static void (* oldsig)(int);
    oldsig = signal(SIGILL, sigill_handler);
    if (sigsetjmp(jmpbuf, 1))
    {
        signal(SIGILL, oldsig);
        return flags;
    }

    canjump = 1;
    PFX(cpu_neon_test)();
    canjump = 0;
    signal(SIGILL, oldsig);
#endif // if !HAVE_NEON

    flags |= X265_CPU_NEON;

    // fast neon -> arm (Cortex-A9) detection relies on user access to the
    // cycle counter; this assumes ARMv7 performance counters.
    // NEON requires at least ARMv7, ARMv8 may require changes here, but
    // hopefully this hacky detection method will have been replaced by then.
    // Note that there is potential for a race condition if another program or
    // x264 instance disables or reinits the counters while x264 is using them,
    // which may result in incorrect detection and the counters stuck enabled.
    // right now Apple does not seem to support performance counters for this test
#ifndef __MACH__
    flags |= PFX(cpu_fast_neon_mrc_test)() ? X265_CPU_FAST_NEON_MRC : 0;
#endif
    // TODO: write dual issue test? currently it's A8 (dual issue) vs. A9 (fast mrc)
#endif // if HAVE_ARMV6
    return flags;
}

#elif X265_ARCH_POWER8

uint32_t cpu_detect(void)
{
#if HAVE_ALTIVEC
    return X265_CPU_ALTIVEC;
#else
    return 0;
#endif
}

#else // if X265_ARCH_POWER8

uint32_t cpu_detect(void)
{
    return 0;
}

#endif // if X265_ARCH_X86
}
