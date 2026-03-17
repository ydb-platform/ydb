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

#define BYTES 66

int main(void)
{
    const uint8_t p521_mod[BYTES] = "\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff";
    const uint8_t b[BYTES] = "\x00\x51\x95\x3e\xb9\x61\x8e\x1c\x9a\x1f\x92\x9a\x21\xa0\xb6\x85\x40\xee\xa2\xda\x72\x5b\x99\xb3\x15\xf3\xb8\xb4\x89\x91\x8e\xf1\x09\xe1\x56\x19\x39\x51\xec\x7e\x93\x7b\x16\x52\xc0\xbd\x3b\xb1\xbf\x07\x35\x73\xdf\x88\x3d\x2c\x34\xf1\xef\x45\x1f\xd4\x6b\x50\x3f\x00";
    const uint8_t order[BYTES] = "\x01\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xfa\x51\x86\x87\x83\xbf\x2f\x96\x6b\x7f\xcc\x01\x48\xf7\x09\xa5\xd0\x3b\xb5\xc9\xb8\x89\x9c\x47\xae\xbb\x6f\xb7\x1e\x91\x38\x64\x09";
    const uint8_t p521_Gx[BYTES] = "\x00\xc6\x85\x8e\x06\xb7\x04\x04\xe9\xcd\x9e\x3e\xcb\x66\x23\x95\xb4\x42\x9c\x64\x81\x39\x05\x3f\xb5\x21\xf8\x28\xaf\x60\x6b\x4d\x3d\xba\xa1\x4b\x5e\x77\xef\xe7\x59\x28\xfe\x1d\xc1\x27\xa2\xff\xa8\xde\x33\x48\xb3\xc1\x85\x6a\x42\x9b\xf9\x7e\x7e\x31\xc2\xe5\xbd\x66";
    const uint8_t p521_Gy[BYTES] = "\x01\x18\x39\x29\x6a\x78\x9a\x3b\xc0\x04\x5c\x8a\x5f\xb4\x2c\x7d\x1b\xd9\x98\xf5\x44\x49\x57\x9b\x44\x68\x17\xaf\xbd\x17\x27\x3e\x66\x2c\x97\xee\x72\x99\x5e\xf4\x26\x40\xc5\x50\xb9\x01\x3f\xad\x07\x61\x35\x3c\x70\x86\xa2\x72\xc2\x40\x88\xbe\x94\x76\x9f\xd1\x66\x50";

    uint8_t x[BYTES], y[BYTES];
    uint8_t exp[BYTES];
    EcContext *ec_ctx;
    EcPoint *ecp = NULL;
    EcPoint *gp = NULL;
    unsigned i;
    struct timeval start, stop;
    double duration_ms, rate;
    int res;

#define ITERATIONS 500U

    /* Make almost-worst case exponent */
    for (i=0; i<BYTES; i++) {
        exp[i] = (uint8_t)(0xFF - i);
    }

    /** Only 1 bit in MSB **/
    exp[0] &= 1;

    res = ec_ws_new_context(&ec_ctx, p521_mod, b, order, BYTES, /* seed */ 4);
    assert(res == 0);

    res = ec_ws_new_point(&gp, p521_Gx, p521_Gy, BYTES, ec_ctx);
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
