/* ===================================================================
 *
 * Copyright (c) 2018, Helder Eijs <helderijs@gmail.com>
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

#include "ec_ws.c"

#define BYTES 48

int main(void)
{
    const uint8_t p384_mod[BYTES] = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfe\xff\xff\xff\xff\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xff";
    const uint8_t  b[BYTES] = "\xb3\x31\x2f\xa7\xe2\x3e\xe7\xe4\x98\x8e\x05\x6b\xe3\xf8\x2d\x19\x18\x1d\x9c\x6e\xfe\x81\x41\x12\x03\x14\x08\x8f\x50\x13\x87\x5a\xc6\x56\x39\x8d\x8a\x2e\xd1\x9d\x2a\x85\xc8\xed\xd3\xec\x2a\xef";
    const uint8_t order[BYTES] = "\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xc7\x63\x4d\x81\xf4\x37\x2d\xdf\x58\x1a\x0d\xb2\x48\xb0\xa7\x7a\xec\xec\x19\x6a\xcc\xc5\x29\x73";
    const uint8_t p384_Gx[BYTES] = "\xaa\x87\xca\x22\xbe\x8b\x05\x37\x8e\xb1\xc7\x1e\xf3\x20\xad\x74\x6e\x1d\x3b\x62\x8b\xa7\x9b\x98\x59\xf7\x41\xe0\x82\x54\x2a\x38\x55\x02\xf2\x5d\xbf\x55\x29\x6c\x3a\x54\x5e\x38\x72\x76\x0a\xb7";
    const uint8_t p384_Gy[BYTES] = "\x36\x17\xde\x4a\x96\x26\x2c\x6f\x5d\x9e\x98\xbf\x92\x92\xdc\x29\xf8\xf4\x1d\xbd\x28\x9a\x14\x7c\xe9\xda\x31\x13\xb5\xf0\xb8\xc0\x0a\x60\xb1\xce\x1d\x7e\x81\x9d\x7a\x43\x1d\x7c\x90\xea\x0e\x5f";
    uint8_t x[BYTES], y[BYTES];
    uint8_t exp[BYTES];
    EcContext *ec_ctx;
    EcPoint *ecp = NULL;
    EcPoint *gp = NULL;
    unsigned i;
    struct timeval start, stop;
    double duration_ms, rate;
    int res;

#define ITERATIONS 1000U

    /* Make almost-worst case exponent */
    for (i=0; i<BYTES; i++) {
        exp[i] = (uint8_t)(0xFF - i);
    }

    res = ec_ws_new_context(&ec_ctx, p384_mod, b, order, BYTES, /* seed */ 4);
    assert(res == 0);

    res = ec_ws_new_point(&gp, p384_Gx, p384_Gy, BYTES, ec_ctx);
    assert(res == 0);

    res = ec_ws_clone(&ecp, gp);
    assert(res == 0);

    /** Scalar multiplications by G **/
    gettimeofday(&start, NULL);
    for (i=0; i<ITERATIONS; i++) {
        res = ec_ws_copy(ecp, gp);
        assert(res == 0);
        res = ec_ws_scalar(ecp, exp, BYTES, 0xFFF);
        assert(res == 0);
        res = ec_ws_get_xy(x, y, BYTES, ecp);
        assert(res == 0);
    }
    gettimeofday(&stop, NULL);
    duration_ms = (double)(stop.tv_sec - start.tv_sec) * 1000 + (double)(stop.tv_usec - start.tv_usec) / 1000;
    rate = ITERATIONS / (duration_ms/1000);
    printf("Speed (scalar mult by G) = %.0f op/s\n", rate);

    res = ec_ws_get_xy(x, y, BYTES, ecp);
    assert(res == 0);
    printf("X: ");
    for (i=0; i<BYTES; i++)
        printf("%02X", x[i]);
    printf("\n");
    printf("Y: ");
    for (i=0; i<BYTES; i++)
        printf("%02X", y[i]);
    printf("\n");

#if 1
    /** Scalar multiplications by arbitrary point **/
    res = ec_ws_double(ecp);
    assert(res == 0);
    gettimeofday(&start, NULL);
    for (i=0; i<ITERATIONS; i++) {
        res = ec_ws_scalar(ecp, exp, BYTES, 0xFFF);
        assert(res == 0);
        res = ec_ws_get_xy(x, y, BYTES, ecp);
        assert(res == 0);
    }
    gettimeofday(&stop, NULL);
    duration_ms = (double)(stop.tv_sec - start.tv_sec) * 1000 + (double)(stop.tv_usec - start.tv_usec) / 1000;
    rate = ITERATIONS / (duration_ms/1000);
    printf("Speed (scalar mult by P) = %.0f op/s\n", rate);

    res = ec_ws_get_xy(x, y, BYTES, ecp);
    assert(res == 0);
    printf("X: ");
    for (i=0; i<BYTES; i++)
        printf("%02X", x[i]);
    printf("\n");
    printf("Y: ");
    for (i=0; i<BYTES; i++)
        printf("%02X", y[i]);
    printf("\n");
#endif

    ec_ws_free_point(gp);
    ec_ws_free_point(ecp);
    ec_ws_free_context(ec_ctx);

    return 0;
}
