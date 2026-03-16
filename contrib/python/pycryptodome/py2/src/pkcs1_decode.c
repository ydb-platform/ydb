/* ===================================================================
 *
 * Copyright (c) 2021, Helder Eijs <helderijs@gmail.com>
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

#include "common.h"

FAKE_INIT(pkcs1_decode)

#define SIZE_T_MAX ((size_t)-1)
#define SIZE_T_LEN (sizeof(size_t))

STATIC uint8_t rol8(uint8_t x)
{
    return (uint8_t)((x << 1) | (x >> 7));
}

/*
 * Return 0 if x is 0, otherwise SIZE_T_MAX (all bits set to 1)
 */
STATIC size_t propagate_ones(uint8_t x)
{
    unsigned i;
    uint8_t result8;
    size_t result;

    result8 = x;
    for (i=0; i<8; i++) {
        x = rol8(x);
        result8 |= x;
    }
    result = 0;
    for (i=0; i<sizeof(result); i++) {
        result |= ((size_t)result8) << (i*8);
    }

    return result;
}

/*
 * Set all bits to 1 in the flag if term1 == term2
 * or leave the flag untouched otherwise.
 */
STATIC void set_if_match(uint8_t *flag, size_t term1, size_t term2)
{
    unsigned i;
    uint8_t x;

    x = 0;
    for (i=0; i<sizeof(size_t); i++) {
        x |= (uint8_t)((term1 ^ term2) >> (i*8));
    }
    *flag |= (uint8_t)~propagate_ones(x);
}

/*
 * Set all bits to 1 in the flag if term1 != term2
 * or leave the flag untouched otherwise.
 */
STATIC void set_if_no_match(uint8_t *flag, size_t term1, size_t term2)
{
    size_t i;
    uint8_t x;

    x = 0;
    for (i=0; i<sizeof(size_t); i++) {
        x |= (uint8_t)((term1 ^ term2) >> (i*8));
    }
    *flag |= (uint8_t)propagate_ones(x);
}

/*
 * Copy in1[] into out[] if choice is 0, otherwise copy in2[]
 */
STATIC void safe_select(const uint8_t *in1, const uint8_t *in2, uint8_t *out, uint8_t choice, size_t len)
{
    size_t i;
    uint8_t mask1, mask2;

    mask1 = (uint8_t)propagate_ones(choice);
    mask2 = (uint8_t)~mask1;
    for (i=0; i<len; i++) {
        out[i] = (in1[i] & mask2) | (in2[i] & mask1);
        /* yes, these rol8s are redundant, but we try to avoid compiler optimizations */
        mask1 = rol8(mask1);
        mask2 = rol8(mask2);
    }
}

/*
 * Return in1 if choice is 0, in2 otherwise.
 */
STATIC size_t safe_select_idx(size_t in1, size_t in2, uint8_t choice)
{
    size_t mask;

    mask = propagate_ones(choice);
    return (in1 & ~mask) | (in2 & mask);
}

/*
 * Return 0 if all these conditions hold:
 *  - in1[] is equal to in2[]     where eq_mask[] is 0xFF,
 *  - in1[] is NOT equal to in2[] where neq_mask[] is 0xFF.
 * Return non-zero otherwise.
 */
STATIC uint8_t safe_cmp_masks(const uint8_t *in1, const uint8_t *in2,
                 const uint8_t *eq_mask, const uint8_t *neq_mask,
                 size_t len)
{
    size_t i;
    uint8_t c, result;

    result = 0;
    for (i=0; i<len; i++) {
        c = (uint8_t)propagate_ones(*in1++ ^ *in2++);
        result |= (uint8_t)(c & *eq_mask++);    /* Set all bits to 1 if *in1 and *in2 differed
                                                and eq_mask was 0xff */
        result |= (uint8_t)(~c & *neq_mask++);  /* Set all bits to 1 if *in1 and *in2 matched
                                                and neq_mask was 0xff */
    }

    return result;
}

/*
 * Return the index of the first byte with value c,
 * the length of in1[] when c is not present,
 * or SIZE_T_MAX in case of problems.
 */
STATIC size_t safe_search(const uint8_t *in1, uint8_t c, size_t len)
{
    size_t result, mask1, mask2, i;
    uint8_t *in1_c;

    if (NULL == in1 || 0 == len) {
        return SIZE_T_MAX;
    }

    /*
     * Create a second byte string and put c at the end,
     * so that at least we will find it there.
     */
    in1_c = (uint8_t*) malloc(len + 1);
    if (NULL == in1_c) {
        return SIZE_T_MAX;
    }
    memcpy(in1_c, in1, len);
    in1_c[len] = c;

    result = 0;
    mask2 = 0;
    for (i=0; i<(len+1); i++) {
        mask1 = ~mask2 & ~propagate_ones(in1_c[i] ^ c); /* Set mask1 to 0xff if there is a match
                                                           and it is the first one. */
        result |= i & mask1;
        mask2 |= mask1;
    }

    free(in1_c);
    return result;
}

#define PKCS1_PREFIX_LEN 10

/*
 * Decode and verify the PKCS#1 padding, then put either the plaintext
 * or the sentinel value into the output buffer, all in constant time.
 *
 * The output is a buffer of equal length as the encoded message (em).
 *
 * The sentinel is put into the buffer when decryption fails.
 *
 * The caller may already know the expected length of the plaintext M,
 * which they should put into expected_pt_len.
 * Otherwise, expected_pt_len must be set to 0.
 *
 * Either the plaintext or the sentinel will be put into the buffer
 * with padding on the left.
 *
 * The function returns the number of bytes to ignore at the beginning
 * of the output buffer, or -1 in case of problems.
 */
EXPORT_SYM int pkcs1_decode(const uint8_t *em, size_t len_em_output,
                            const uint8_t *sentinel, size_t len_sentinel,
                            size_t expected_pt_len,
                            uint8_t *output)
{
    size_t pos;
    uint8_t match, selector;
    uint8_t *padded_sentinel;
    int result;

    result = -1;

    if (NULL == em || NULL == output || NULL == sentinel) {
        return -1;
    }
    if (len_em_output < (PKCS1_PREFIX_LEN + 2)) {
        return -1;
    }
    if (len_sentinel > len_em_output) {
        return -1;
    }
    if (expected_pt_len > 0 && expected_pt_len > (len_em_output - PKCS1_PREFIX_LEN - 1)) {
        return -1;
    }

    /** Pad sentinel (on the left) so that it matches em in length **/
    padded_sentinel = (uint8_t*) calloc(1, len_em_output);
    if (NULL == padded_sentinel) {
        return -1;
    }
    memcpy(padded_sentinel + (len_em_output - len_sentinel), sentinel, len_sentinel);

    /** The first 10 bytes must follow the pattern **/
    match = safe_cmp_masks(em,
                     (const uint8_t*)"\x00\x02" "\x00\x00\x00\x00\x00\x00\x00\x00",
                     (const uint8_t*)"\xFF\xFF" "\x00\x00\x00\x00\x00\x00\x00\x00",
                     (const uint8_t*)"\x00\x00" "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
                     10);

    /*
     * pos is the index of the first 0 byte. It is followed by the plaintext M.
     * It can be (len_em_output-1) when the 0 is at the end (empty M).
     * It can be len_em_output when the 0 is not present.
     * It can SIZE_T_MAX in case of other errors.
     */
    pos = safe_search(em + 10, 0, len_em_output - 10) + 10;
    if (pos == SIZE_T_MAX) {
        result = -1;
        goto end;
    }

    /*
     * selector is 0 if:
     * - there is a match for the first 10 bytes AND
     * - a 0 byte is found in the remainder of em, AND
     * - the length of the plaintext matches the expectation (if there is one)
     */
    selector = match;
    set_if_match(&selector, pos, len_em_output);
    if (expected_pt_len > 0) {
        size_t pt_len;

        pt_len = len_em_output - pos - 1;
        set_if_no_match(&selector, pt_len, expected_pt_len);
    }

    /** Select the correct data to output **/
    safe_select(em, padded_sentinel, output, selector, len_em_output);

    /** Select the number of bytes that the caller will skip in output **/
    result = (int)safe_select_idx(pos + 1, len_em_output - len_sentinel, selector);

end:
    free(padded_sentinel);
    return result;
}

/*
 * Decode and verify the OAEP padding in constant time.
 *
 * The function returns the number of bytes to ignore at the beginning
 * of db (the rest is the plaintext), or -1 in case of problems.
 */

EXPORT_SYM int oaep_decode(const uint8_t *em,
                           size_t em_len,
                           const uint8_t *lHash,
                           size_t hLen,
                           const uint8_t *db,
                           size_t db_len)   /* em_len - 1 - hLen */
{
    int result;
    size_t one_pos, search_len, i;
    uint8_t wrong_padding;
    uint8_t *eq_mask = NULL;
    uint8_t *neq_mask = NULL;
    uint8_t *target_db = NULL;

    if (NULL == em || NULL == lHash || NULL == db) {
        return -1;
    }

    if (em_len < 2*hLen+2 || db_len != em_len-1-hLen) {
        return -1;
    }

    /* Allocate */
    eq_mask = (uint8_t*) calloc(1, db_len);
    neq_mask = (uint8_t*) calloc(1, db_len);
    target_db = (uint8_t*) calloc(1, db_len);
    if (NULL == eq_mask || NULL == neq_mask || NULL == target_db) {
        result = -1;
        goto cleanup;
    }

    /* Step 3g */
    search_len = db_len - hLen;

    one_pos = safe_search(db + hLen, 0x01, search_len);
    if (SIZE_T_MAX == one_pos) {
        result = -1;
        goto cleanup;
    }

    memset(eq_mask, 0xAA, db_len);
    memcpy(target_db, lHash, hLen);
    memset(eq_mask, 0xFF, hLen);

    for (i=0; i<search_len; i++) {
        eq_mask[hLen + i] = propagate_ones(i < one_pos);
    }

    wrong_padding = em[0];
    wrong_padding |= safe_cmp_masks(db, target_db, eq_mask, neq_mask, db_len);
    set_if_match(&wrong_padding, one_pos, search_len);

    result = wrong_padding ? -1 : (int)(hLen + 1 + one_pos);

cleanup:
    free(eq_mask);
    free(neq_mask);
    free(target_db);

    return result;
}
