/*****************************************************************************
 * Copyright (C) 2013-2017 MulticoreWare, Inc
 *
 * Authors: Loren Merritt <lorenm@u.washington.edu>
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

#ifndef X265_CPU_H
#define X265_CPU_H

#include "common.h"

/* All assembly functions are prefixed with X265_NS (macro expanded) */
#define PFX3(prefix, name) prefix ## _ ## name
#define PFX2(prefix, name) PFX3(prefix, name)
#define PFX(name)          PFX2(X265_NS, name)

// from cpu-a.asm, if ASM primitives are compiled, else primitives.cpp
extern "C" void PFX(cpu_emms)(void);
extern "C" void PFX(safe_intel_cpu_indicator_init)(void);

#if _MSC_VER && _WIN64
#define x265_emms() PFX(cpu_emms)()
#elif _MSC_VER
#include <mmintrin.h>
#define x265_emms() _mm_empty()
#elif __GNUC__
// Cannot use _mm_empty() directly without compiling all the source with
// a fixed CPU arch, which we would like to avoid at the moment
#define x265_emms() PFX(cpu_emms)()
#else
#define x265_emms() PFX(cpu_emms)()
#endif

namespace X265_NS {
uint32_t cpu_detect(void);

struct cpu_name_t
{
    char name[16];
    uint32_t flags;
};

extern const cpu_name_t cpu_names[];
}

#endif // ifndef X265_CPU_H
