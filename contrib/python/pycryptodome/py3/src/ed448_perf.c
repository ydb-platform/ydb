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
#include "modexp_utils.h"

#include <sys/time.h>

#include "ed448.c"

#define COORD_LEN 56

int main(void)
{
    uint8_t x[COORD_LEN], y[COORD_LEN];
    uint8_t exp[56];
    PointEd448 *gp = NULL;
    unsigned i;
    int res;
    EcContext *ec_ctx;
    struct timeval start, stop;
    double duration_ms, rate;
    const uint8_t Gx[COORD_LEN] = "\x4f\x19\x70\xc6\x6b\xed\x0d\xed\x22\x1d\x15\xa6\x22\xbf\x36\xda\x9e\x14\x65\x70\x47\x0f\x17\x67\xea\x6d\xe3\x24\xa3\xd3\xa4\x64\x12\xae\x1a\xf7\x2a\xb6\x65\x11\x43\x3b\x80\xe1\x8b\x00\x93\x8e\x26\x26\xa8\x2b\xc7\x0c\xc0\x5e";
    const uint8_t Gy[COORD_LEN] = "\x69\x3f\x46\x71\x6e\xb6\xbc\x24\x88\x76\x20\x37\x56\xc9\xc7\x62\x4b\xea\x73\x73\x6c\xa3\x98\x40\x87\x78\x9c\x1e\x05\xa0\xc2\xd7\x3a\xd3\xff\x1c\xe6\x7c\x39\xc4\xfd\xbd\x13\x2c\x4e\xd7\xc8\xad\x98\x08\x79\x5b\xf2\x30\xfa\x14";

#define ITERATIONS 5000U

    /* Make almost-worst case exponent */
    for (i=0; i<sizeof(exp); i++) {
        exp[i] = (uint8_t)(0xFF - i);
    }

    res = ed448_new_context(&ec_ctx);
    assert(res == 0);
    res = ed448_new_point(&gp, Gx, Gy, COORD_LEN, ec_ctx);
    assert(res == 0);

    /** Scalar multiplications by arbitrary point **/
    gettimeofday(&start, NULL);
    for (i=0; i<ITERATIONS && !res; i++) {
        res = ed448_scalar(gp, exp, sizeof(exp), 0xFFF);
    }
    assert(res == 0);
    gettimeofday(&stop, NULL);
    duration_ms = (double)(stop.tv_sec - start.tv_sec) * 1000 + (double)(stop.tv_usec - start.tv_usec) / 1000;
    rate = ITERATIONS / (duration_ms/1000);
    printf("Speed (scalar mult by P) = %.0f op/s\n", rate);

    res = ed448_get_xy(x, y, sizeof(x), gp);
    assert(res == 0);
    printf("X: ");
    for (i=0; i<sizeof(x); i++)
        printf("%02X", x[i]);
    printf("\n");
    printf("Y: ");
    for (i=0; i<sizeof(y); i++)
        printf("%02X", y[i]);
    printf("\n");

    ed448_free_point(gp);
    ed448_free_context(ec_ctx);

    return 0;
}
