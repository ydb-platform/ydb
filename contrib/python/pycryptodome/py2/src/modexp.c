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

#include <stddef.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include "common.h"
#include "endianess.h"

FAKE_INIT(modexp)

#include "mont.h"
#include "modexp_utils.h"

/** Multiplication will be replaced by a look-up **/
/** Do not change this value! **/
#define WINDOW_SIZE 4

/*
 * Modular exponentiation. All numbers are
 * encoded in big endian form, possibly with
 * zero padding on the left.
 *
 * @param out     The memory area where to store the result
 * @param base    Base number, strictly smaller than the modulus
 * @param exp     Exponent
 * @param modulus Modulus, it must be odd
 * @param len     Size in bytes of out, base, exp, and modulus
 * @param seed    A random seed, used for avoiding side-channel
 *                attacks
 * @return        0 in case of success, the appropriate error code otherwise
 */
EXPORT_SYM int monty_pow(
               uint8_t       *out,
               const uint8_t *base,
               const uint8_t *exp,
               const uint8_t *modulus,
               size_t        len,
               uint64_t      seed)
{
    unsigned i, j;
    size_t exp_len;
    int res;

    MontContext *ctx = NULL;
    uint64_t *powers[1 << WINDOW_SIZE] = { NULL };
    uint64_t *power_idx = NULL;
    ProtMemory *prot = NULL;
    uint64_t *mont_base = NULL;
    uint64_t *x = NULL;
    uint64_t *scratchpad = NULL;
    uint8_t *buf_out = NULL;

    struct BitWindow_LR bit_window;

    if (!base || !exp || !modulus || !out)
        return ERR_NULL;

    if (len == 0)
        return ERR_NOT_ENOUGH_DATA;

    /* Allocations **/
    res = mont_context_init(&ctx, modulus, len);
    if (res)
        return res;

    for (i=0; i<(1 << WINDOW_SIZE); i++) {
        res = mont_new_number(powers+i, 1, ctx);
        if (res) goto cleanup;
    }

    res = mont_new_number(&power_idx, 1, ctx);
    if (res) goto cleanup;

    res = mont_new_from_bytes(&mont_base, base, len, ctx);
    if (res) goto cleanup;

    res = mont_new_number(&x, 1, ctx);
    if (res) goto cleanup;

    res = mont_new_number(&scratchpad, SCRATCHPAD_NR, ctx);
    if (res) goto cleanup;

    buf_out = (uint8_t*)calloc(1, mont_bytes(ctx));
    if (NULL == buf_out) {
        res = ERR_MEMORY;
        goto cleanup;
    }

    /** Result is initially 1 in Montgomery form **/
    mont_set(x, 1, ctx);

    /** Pre-compute powers a^0 mod n, a^1 mod n, a^2 mod n, ... a^(2^WINDOW_SIZE-1) mod n **/
    mont_copy(powers[0], x, ctx);
    mont_copy(powers[1], mont_base, ctx);
    for (i=1; i<(1 << (WINDOW_SIZE-1)); i++) {
        mont_mult(powers[i*2],   powers[i],   powers[i], scratchpad, ctx);
        mont_mult(powers[i*2+1], powers[i*2], mont_base,      scratchpad, ctx);
    }

    res = scatter(&prot, (const void**)powers, 1<<WINDOW_SIZE, mont_bytes(ctx), seed);
    if (res) goto cleanup;

    /** Ignore leading zero bytes in the exponent **/
    exp_len = len;
    for (i=0; i<len && *exp==0; i++) {
        exp_len--;
        exp++;
    }

    /* If exponent is 0, the result is always 1 */
    if (exp_len == 0) {
        memset(out, 0, len);
        out[len-1] = 1;
        res = 0;
        goto cleanup;
    }

    bit_window = init_bit_window_lr(WINDOW_SIZE, exp, exp_len);
    
    /** Left-to-right exponentiation with fixed window **/
    for (i=0; i < bit_window.nr_windows; i++) {
        unsigned index;

        for (j=0; j<WINDOW_SIZE; j++) {
            mont_mult(x, x, x, scratchpad, ctx);
        }
        
        index = get_next_digit_lr(&bit_window);
        gather(power_idx, prot, index);
        mont_mult(x, x, power_idx, scratchpad, ctx);
    }

    /** Transform result back into big-endian, byte form **/
    res = mont_to_bytes(out, len, x, ctx);

cleanup:
    mont_context_free(ctx);
    for (i=0; i<(1 << WINDOW_SIZE); i++) {
        free(powers[i]);
    }
    free(power_idx);
    free_scattered(prot);
    free(mont_base);
    free(x);
    free(scratchpad);
    free(buf_out);

    return res;
}

/*
 * Modular multiplication. All numbers are
 * encoded in big endian form, possibly with
 * zero padding on the left.
 *
 * @param out     The memory area where to store the result
 * @param term1   First term of the multiplication, strictly smaller than the modulus
 * @param term2   Second term of the multiplication, strictly smaller than the modulus
 * @param modulus Modulus, it must be odd
 * @param len     Size in bytes of out, term1, term2, and modulus
 * @return        0 in case of success, the appropriate error code otherwise
 */
EXPORT_SYM int monty_multiply(
               uint8_t       *out,
               const uint8_t *term1,
               const uint8_t *term2,
               const uint8_t *modulus,
               size_t        len)
{
    MontContext *ctx = NULL;
    uint64_t *mont_term1 = NULL;
    uint64_t *mont_term2 = NULL;
    uint64_t *mont_output = NULL;
    uint64_t *scratchpad = NULL;
    int res;

    if (!term1 || !term2 || !modulus || !out)
        return ERR_NULL;

    if (len == 0)
        return ERR_NOT_ENOUGH_DATA;

    /* Allocations **/
    res = mont_context_init(&ctx, modulus, len);
    if (res)
        return res;

    res = mont_new_from_bytes(&mont_term1, term1, len, ctx);
    if (res) goto cleanup;

    res = mont_new_from_bytes(&mont_term2, term2, len, ctx);
    if (res) goto cleanup;

    res = mont_new_number(&mont_output, 1, ctx);
    if (res) goto cleanup;

    res = mont_new_number(&scratchpad, SCRATCHPAD_NR, ctx);
    if (res) goto cleanup;

    /* Multiply, then transform result back into big-endian, byte form **/
    res = mont_mult(mont_output, mont_term1, mont_term2, scratchpad, ctx);
    if (res) goto cleanup;

    res = mont_to_bytes(out, len, mont_output, ctx);

cleanup:
    mont_context_free(ctx);
    free(mont_term1);
    free(mont_term2);
    free(mont_output);
    free(scratchpad);

    return res;
}

#ifdef MAIN
int main(void)
{
    uint16_t length;
    uint8_t *base, *modulus, *exponent, *out;
    int result;
    size_t res;

    res = fread(&length, 2, 1, stdin);
    assert(res == 2);

    base = malloc(length);
    modulus = malloc(length);
    exponent = malloc(length);
    out = malloc(length);

    res = fread(base, 1, length, stdin);
    assert(res == length);
    res = fread(modulus, 1, length, stdin);
    assert(res == length);
    res = fread(exponent, 1, length, stdin);
    assert(res == length);
    assert(res == length);
    res = fread(out, 1, length, stdin);

    result = monty_pow(out, base, exponent, modulus, length, 12);

    free(base);
    free(modulus);
    free(exponent);
    free(out);

    return result;
}

#endif

#ifdef PROFILE
int main(void)
{
    uint8_t base[256], exponent[256], modulus[256], out[256];
    unsigned length = 256, i, j;

    for (i=0; i<256; i++) {
        base[i] = (uint8_t)i | 0x80 | 1;
        exponent[i] = base[i] = modulus[i] = base[i];
    }
    base[0] = 0x7F;

    for (j=0; j<50; j++) {
    monty_pow(out, base, exponent, modulus, length, 12);
    }

    monty_multiply(out, base, out, modulus, length);
}
#endif
