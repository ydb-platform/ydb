/* ===================================================================
 *
 * Copyright (c) 2022, Helder Eijs <helderijs@gmail.com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * ===================================================================
 */

#include <assert.h>

#include "common.h"
#include "endianess.h"
#include "multiply.h"
#include "mont.h"
#include "ec.h"
#include "modexp_utils.h"

#include <sys/time.h>

#include "ed25519.c"

int main(void)
{
    uint8_t x[32], y[32];
    uint8_t exp[32];
    Point *gp = NULL;
    unsigned i;
    struct timeval start, stop;
    double duration_ms, rate;
    const uint8_t Gx[32] = "\x21\x69\x36\xd3\xcd\x6e\x53\xfe\xc0\xa4\xe2\x31\xfd\xd6\xdc\x5c\x69\x2c\xc7\x60\x95\x25\xa7\xb2\xc9\x56\x2d\x60\x8f\x25\xd5\x1a";
    const uint8_t Gy[32] = "\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x66\x58";
    int res;

#define ITERATIONS 1000U

    /* Make almost-worst case exponent */
    for (i=0; i<32; i++) {
        exp[i] = (uint8_t)(0xFF - i);
    }

    res = ed25519_new_point(&gp, Gx, Gy, 32, NULL);
    assert(res == 0);

    /** Scalar multiplications by arbitrary point **/
    gettimeofday(&start, NULL);
    for (i=0; i<ITERATIONS; i++) {
        res = ed25519_scalar(gp, exp, 32, 0xFFF);
        assert(res == 0);
    }
    gettimeofday(&stop, NULL);
    duration_ms = (double)(stop.tv_sec - start.tv_sec) * 1000 + (double)(stop.tv_usec - start.tv_usec) / 1000;
    rate = ITERATIONS / (duration_ms/1000);
    printf("Speed (scalar mult by P) = %.0f op/s\n", rate);

    res = ed25519_get_xy(x, y, 32, gp);
    assert(res == 0);
    printf("X: ");
    for (i=0; i<32; i++)
        printf("%02X", x[i]);
    printf("\n");
    printf("Y: ");
    for (i=0; i<32; i++)
        printf("%02X", y[i]);
    printf("\n");

    ed25519_free_point(gp);

    return 0;
}
