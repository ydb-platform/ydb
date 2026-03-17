/*
 * Copyright © 2024 Filip Wasil, Samsung Electronics
 *
 * Permission to use, copy, modify, distribute, and sell this software and its
 * documentation for any purpose is hereby granted without fee, provided that
 * the above copyright notice appear in all copies and that both that
 * copyright notice and this permission notice appear in supporting
 * documentation, and that the name of Keith Packard not be used in
 * advertising or publicity pertaining to distribution of the software without
 * specific, written prior permission.  Keith Packard makes no
 * representations about the suitability of this software for any purpose.  It
 * is provided "as is" without express or implied warranty.
 *
 * THE COPYRIGHT HOLDERS DISCLAIM ALL WARRANTIES WITH REGARD TO THIS
 * SOFTWARE, INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS, IN NO EVENT SHALL THE COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN
 * AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING
 * OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS
 * SOFTWARE.
 */

#ifdef HAVE_CONFIG_H
#include <pixman-config.h>
#endif

#include "pixman-private.h"

#ifdef USE_RVV

#if defined(__linux__)
#error #include <asm/hwprobe.h>
#include <sys/auxv.h>
#include <sys/syscall.h>
#include <unistd.h>

static int
is_rvv_1_0_available ()
{
    struct riscv_hwprobe pair = {RISCV_HWPROBE_KEY_IMA_EXT_0, 0};
    if (sys_riscv_hwprobe (&pair, 1, 0, 0, 0) < 0)
    {
	return 0;
    }
    return (pair.value & RISCV_HWPROBE_IMA_V);
}

#endif

typedef enum
{
    RVV = (1 << 0),
} riscv_cpu_features_t;

static riscv_cpu_features_t
detect_cpu_features (void)
{
    riscv_cpu_features_t features = 0;

#if defined(__linux__)
    if (is_rvv_1_0_available ())
    {
	features |= RVV;
    }
#else
#pragma message(                                                               \
    "warning: RISC-V Vector Extension runtime check not implemented for this platform. RVV will be disabled")
#endif
    return features;
}

#endif

pixman_implementation_t *
_pixman_riscv_get_implementations (pixman_implementation_t *imp)
{
#ifdef USE_RVV
    if (!_pixman_disabled ("rvv") && (detect_cpu_features () & RVV))
    {
	imp = _pixman_implementation_create_rvv (imp);
    }
#endif
    return imp;
}
