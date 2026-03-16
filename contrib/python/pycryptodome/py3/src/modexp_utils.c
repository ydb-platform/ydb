#include "modexp_utils.h"
#include "siphash.h"
#include "endianess.h"

#include "siphash.c"

void expand_seed(uint64_t seed_in, void* seed_out, size_t out_len)
{
    uint8_t counter[4];
    uint8_t seed_in_b[16];
    uint32_t i;

    for (i=0; i<8; i++) {
        seed_in_b[2*i] = seed_in_b[2*i+1] = (uint8_t)(seed_in >> (i*8));
    }

#define SIPHASH_LEN 16
    
    for (i=0 ;; i++, out_len-=SIPHASH_LEN) {
        STORE_U32_LITTLE(counter, i);
        if (out_len<SIPHASH_LEN)
            break;
        siphash(counter, 4, seed_in_b, seed_out, SIPHASH_LEN);
        seed_out = (uint8_t*)seed_out + SIPHASH_LEN;
    }

    if (out_len>0) {
        uint8_t buffer[SIPHASH_LEN];
        siphash(counter, 4, seed_in_b, buffer, SIPHASH_LEN);
        memcpy(seed_out, buffer, out_len);
    }

#undef SIPHASH_LEN
}

struct BitWindow_LR init_bit_window_lr(unsigned window_size, const uint8_t *exp, size_t exp_len)
{
    struct BitWindow_LR bw;

    bw.window_size = window_size;
    bw.nr_windows = (unsigned)((exp_len*8+window_size-1)/window_size);

    bw.tg = (unsigned)((exp_len*8) % window_size);
    if (bw.tg == 0) {
        bw.tg = window_size;
    }

    bw.available = 8;
    bw.scan_exp = 0;
    bw.exp = exp;

    return bw;
}

struct BitWindow_RL init_bit_window_rl(unsigned window_size, const uint8_t *exp, size_t exp_len)
{
    struct BitWindow_RL bw;

    bw.window_size = window_size;
    bw.nr_windows = (unsigned)((exp_len*8+window_size-1)/window_size);

    bw.bytes_left = (unsigned)exp_len;
    bw.bits_left = 8;
    bw.cursor = exp + (exp_len-1);

    return bw;
}

unsigned get_next_digit_lr(struct BitWindow_LR *bw)
{
    unsigned tc, index;

    /** Possibly move to the next byte **/
    if (bw->available == 0) {
        bw->available = 8;
        bw->scan_exp++;
    }

    /** Try to consume as much as possible from the current byte **/
    tc = MIN(bw->tg, bw->available);
    
    index = ((unsigned)bw->exp[bw->scan_exp] >> ((unsigned)bw->available - tc)) & ((1U << tc) - 1);
    
    bw->available -= tc;
    bw->tg -= tc;
        
    /** A few bits (<8) might still be needed from the next byte **/
    if (bw->tg > 0) {
        bw->scan_exp++;
        index = (index << bw->tg) | ((unsigned)bw->exp[bw->scan_exp] >> (8 - bw->tg));
        bw->available = 8 - bw->tg;
    }

    bw->tg = bw->window_size;

    return index;
}

unsigned get_next_digit_rl(struct BitWindow_RL *bw)
{
    unsigned res, tg, bits_used;

    if (bw->bytes_left == 0)
        return 0;

    assert(bw->bits_left > 0);

    res = (unsigned)(*(bw->cursor) >> (8 - bw->bits_left)) & (unsigned)((1U<<bw->window_size) - 1);
    bits_used = MIN(bw->bits_left, bw->window_size);

    tg = bw->window_size - bits_used;
    bw->bits_left -= bits_used;

    if (bw->bits_left == 0) {
        bw->bits_left = 8;
        if (--bw->bytes_left == 0)
            return res;
        bw->cursor--;
    }

    if (tg>0) {
        res |= (*(bw->cursor) & ((1U<<tg) - 1)) << bits_used;
        bw->bits_left -= tg;
    }

    return res;
}

#define CACHE_LINE_SIZE 64U

/**
 * Spread a number of equally-sized arrays in memory, to minimize cache
 * side-channel when one of them has to be accessed through a secret index.
 *
 * Each array is broken up in pieces, and pieces from all array collected
 * and fit into one or more 64 byte cache lines. One cache line contains
 * one piece taken from each array at the same offset.
 *
 * We assume that access to a byte in the cache line can be performed without
 * leaking its position.
 *
 * nr_array is a power of two, 64 at most
 */
int scatter(ProtMemory** pprot, const void *arrays[], uint8_t nr_arrays, size_t array_len, uint64_t seed)
{
    ProtMemory *prot;
    unsigned piece_len;
    unsigned cache_lines;
    unsigned remaining;
    unsigned i, j;
    unsigned mask;

    if (nr_arrays>CACHE_LINE_SIZE || (nr_arrays & 1) == 1 || array_len == 0)
        return ERR_VALUE;

    for (i=nr_arrays; (i & 1)==0; i>>=1);
    if (i != 1)
        return ERR_VALUE;

    piece_len = CACHE_LINE_SIZE / nr_arrays;
    cache_lines = ((unsigned)array_len + piece_len - 1) / piece_len;

    *pprot = prot = (ProtMemory*)calloc(1, sizeof(ProtMemory));
    if (NULL == prot)
        return ERR_MEMORY;

    prot->scramble = (uint16_t*)calloc(cache_lines, sizeof(prot->scramble[0]));
    if (NULL == prot->scramble) {
        free(prot);
        return ERR_MEMORY;
    }
    expand_seed(seed, prot->scramble, cache_lines*sizeof(prot->scramble[0]));

    prot->scattered = (uint8_t*)align_alloc(cache_lines*CACHE_LINE_SIZE, CACHE_LINE_SIZE);
    if (NULL == prot->scattered) {
        free(prot->scramble);
        free(prot);
        return ERR_MEMORY;
    }

    prot->nr_arrays = nr_arrays;
    prot->array_len = (unsigned)array_len;

    remaining = (unsigned)array_len;
    mask = (unsigned)(nr_arrays - 1);

    for (i=0; i<cache_lines; i++) {
        uint8_t *cache_line;
        unsigned offset;

        cache_line = (uint8_t*)prot->scattered + i*CACHE_LINE_SIZE;
        offset = i*piece_len;

        for (j=0; j<nr_arrays; j++) {
            unsigned s;
            unsigned obf;
            uint8_t *dst, *src;

            obf = (j*((prot->scramble[i] >> 8) | 1) + (prot->scramble[i] & 0xFF)) & mask;

            s = MIN(piece_len, remaining);
            dst = cache_line + piece_len*obf;
            src = (uint8_t*)arrays[j] + offset;
            memcpy(dst, src, s);
        }

        remaining -= piece_len;
    }

    return 0;
}

void gather(void *out, const ProtMemory *prot, unsigned index)
{
    unsigned piece_len;
    unsigned cache_lines;
    unsigned remaining;
    unsigned offset;
    unsigned i;
    unsigned mask;

    piece_len = CACHE_LINE_SIZE / prot->nr_arrays;
    cache_lines = (prot->array_len + piece_len - 1) / piece_len;

    remaining = prot->array_len;
    mask = prot->nr_arrays - 1;
    offset = 0;

    for (i=0; i<cache_lines; i++) {
        uint8_t *cache_line;
        unsigned obf;

        obf = (index*((prot->scramble[i] >> 8) | 1) + (prot->scramble[i] & 0xFF)) & mask;

        cache_line = (uint8_t*)prot->scattered + i*CACHE_LINE_SIZE;
        memcpy((uint8_t*)out + offset, cache_line + piece_len*obf, MIN(piece_len, remaining));

        remaining -= piece_len;
        offset += piece_len;
    }
}

void free_scattered(ProtMemory *prot)
{
    if (prot) {
        free(prot->scramble);
        align_free(prot->scattered);
    }
    free(prot);
}
