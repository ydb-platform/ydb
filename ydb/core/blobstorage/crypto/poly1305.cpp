/*
 * Public Domain poly1305 from Andrew Moon
 * Based on poly1305-donna.c, poly1305-donna-32.h and poly1305-donna.h from:
 *   https://github.com/floodyberry/poly1305-donna
 */

#include "poly1305.h"

#include <util/system/yassert.h>

#include <string.h>


struct poly1305_state_st
{
    ui32 r0, r1, r2, r3, r4;
    ui32 s1, s2, s3, s4;
    ui32 h0, h1, h2, h3, h4;
    ui8 buf[16];
    unsigned int buf_used;
    ui8 key[16];
};


static uint32_t U8TO32_LE(const uint8_t *m) {
  uint32_t r;
  memcpy(&r, m, sizeof(r));
  return r;
}

static void U32TO8_LE(uint8_t *m, uint32_t v) { memcpy(m, &v, sizeof(v)); }

inline ui64 mul32x32_64(ui32 a, ui32 b) { return (ui64)a * b; }

/* poly1305_blocks updates |state| given some amount of input data. This
 * function may only be called with a |len| that is not a multiple of 16 at the
 * end of the data. Otherwise the input must be buffered into 16 byte blocks. */
static void poly1305_update(poly1305_state_st *state, const ui8 *in, size_t len)
{
    ui32 t0, t1, t2, t3;
    ui64 t[5];
    ui32 b;
    ui64 c;
    size_t j;

    if (len < 16) {
        goto poly1305_donna_atmost15bytes;
    }

poly1305_donna_16bytes:
    t0 = U8TO32_LE(in);
    t1 = U8TO32_LE(in + 4);
    t2 = U8TO32_LE(in + 8);
    t3 = U8TO32_LE(in + 12);

    in += 16;
    len -= 16;

    state->h0 += t0 & 0x3ffffff;
    state->h1 += ((((ui64)t1 << 32) | t0) >> 26) & 0x3ffffff;
    state->h2 += ((((ui64)t2 << 32) | t1) >> 20) & 0x3ffffff;
    state->h3 += ((((ui64)t3 << 32) | t2) >> 14) & 0x3ffffff;
    state->h4 += (t3 >> 8) | (1 << 24);

poly1305_donna_mul:
    t[0] = mul32x32_64(state->h0, state->r0) + mul32x32_64(state->h1, state->s4) +
            mul32x32_64(state->h2, state->s3) + mul32x32_64(state->h3, state->s2) +
            mul32x32_64(state->h4, state->s1);
    t[1] = mul32x32_64(state->h0, state->r1) + mul32x32_64(state->h1, state->r0) +
            mul32x32_64(state->h2, state->s4) + mul32x32_64(state->h3, state->s3) +
            mul32x32_64(state->h4, state->s2);
    t[2] = mul32x32_64(state->h0, state->r2) + mul32x32_64(state->h1, state->r1) +
            mul32x32_64(state->h2, state->r0) + mul32x32_64(state->h3, state->s4) +
            mul32x32_64(state->h4, state->s3);
    t[3] = mul32x32_64(state->h0, state->r3) + mul32x32_64(state->h1, state->r2) +
            mul32x32_64(state->h2, state->r1) + mul32x32_64(state->h3, state->r0) +
            mul32x32_64(state->h4, state->s4);
    t[4] = mul32x32_64(state->h0, state->r4) + mul32x32_64(state->h1, state->r3) +
            mul32x32_64(state->h2, state->r2) + mul32x32_64(state->h3, state->r1) +
            mul32x32_64(state->h4, state->r0);

    state->h0 = (ui32)t[0] & 0x3ffffff;
    c = (t[0] >> 26);
    t[1] += c;
    state->h1 = (ui32)t[1] & 0x3ffffff;
    b = (ui32)(t[1] >> 26);
    t[2] += b;
    state->h2 = (ui32)t[2] & 0x3ffffff;
    b = (ui32)(t[2] >> 26);
    t[3] += b;
    state->h3 = (ui32)t[3] & 0x3ffffff;
    b = (ui32)(t[3] >> 26);
    t[4] += b;
    state->h4 = (ui32)t[4] & 0x3ffffff;
    b = (ui32)(t[4] >> 26);
    state->h0 += b * 5;

    if (len >= 16) {
        goto poly1305_donna_16bytes;
    }

    /* final bytes */
poly1305_donna_atmost15bytes:
    if (!len) {
        return;
    }

    ui8 mp[16];
    for (j = 0; j < len; j++) {
        mp[j] = in[j];
    }
    mp[j++] = 1;
    for (; j < 16; j++) {
        mp[j] = 0;
    }
    len = 0;

    t0 = U8TO32_LE(&mp[0] + 0);
    t1 = U8TO32_LE(mp + 4);
    t2 = U8TO32_LE(mp + 8);
    t3 = U8TO32_LE(mp + 12);

    state->h0 += t0 & 0x3ffffff;
    state->h1 += ((((ui64)t1 << 32) | t0) >> 26) & 0x3ffffff;
    state->h2 += ((((ui64)t2 << 32) | t1) >> 20) & 0x3ffffff;
    state->h3 += ((((ui64)t3 << 32) | t2) >> 14) & 0x3ffffff;
    state->h4 += (t3 >> 8);

    goto poly1305_donna_mul;
}

#if (!defined(_win_) && !defined(_arm64_))
constexpr size_t Poly1305::KEY_SIZE;
constexpr size_t Poly1305::MAC_SIZE;
#endif

void Poly1305::SetKey(const ui8* key, size_t size)
{
    Y_ASSERT((size == KEY_SIZE) && "key must be 32 bytes long");

    poly1305_state_st *st = (poly1305_state_st *)&state;
    ui32 t0, t1, t2, t3;

    t0 = U8TO32_LE(key + 0);
    t1 = U8TO32_LE(key + 4);
    t2 = U8TO32_LE(key + 8);
    t3 = U8TO32_LE(key + 12);

    /* precompute multipliers */
    st->r0 = t0 & 0x3ffffff;
    t0 >>= 26;
    t0 |= t1 << 6;
    st->r1 = t0 & 0x3ffff03;
    t1 >>= 20;
    t1 |= t2 << 12;
    st->r2 = t1 & 0x3ffc0ff;
    t2 >>= 14;
    t2 |= t3 << 18;
    st->r3 = t2 & 0x3f03fff;
    t3 >>= 8;
    st->r4 = t3 & 0x00fffff;

    st->s1 = st->r1 * 5;
    st->s2 = st->r2 * 5;
    st->s3 = st->r3 * 5;
    st->s4 = st->r4 * 5;

    /* init state */
    st->h0 = 0;
    st->h1 = 0;
    st->h2 = 0;
    st->h3 = 0;
    st->h4 = 0;

    st->buf_used = 0;
    memcpy(st->key, key + 16, sizeof(st->key));
}

void Poly1305::Update(const ui8* msg, size_t size)
{
    struct poly1305_state_st *st = (struct poly1305_state_st *)state;


    if (st->buf_used) {
        size_t todo = 16 - st->buf_used;
        if (todo > size) {
            todo = size;
        }
        for (size_t i = 0; i < todo; i++) {
            st->buf[st->buf_used + i] = msg[i];
        }
        st->buf_used += todo;
        size -= todo;
        msg += todo;

        if (st->buf_used == 16) {
            poly1305_update(st, st->buf, 16);
            st->buf_used = 0;
        }
    }

    if (size >= 16) {
        size_t todo = size & ~0xf;
        poly1305_update(st, msg, todo);
        msg += todo;
        size &= 0xf;
    }

    if (size) {
        for (size_t i = 0; i < size; i++) {
            st->buf[i] = msg[i];
        }
        st->buf_used = size;
    }
}

void Poly1305::Finish(ui8 mac[MAC_SIZE])
{
    struct poly1305_state_st *st = (struct poly1305_state_st *)state;
    ui64 f0, f1, f2, f3;
    ui32 g0, g1, g2, g3, g4;
    ui32 b, nb;

    if (st->buf_used) {
      poly1305_update(st, st->buf, st->buf_used);
    }

    b = st->h0 >> 26;
    st->h0 = st->h0 & 0x3ffffff;
    st->h1 += b;
    b = st->h1 >> 26;
    st->h1 = st->h1 & 0x3ffffff;
    st->h2 += b;
    b = st->h2 >> 26;
    st->h2 = st->h2 & 0x3ffffff;
    st->h3 += b;
    b = st->h3 >> 26;
    st->h3 = st->h3 & 0x3ffffff;
    st->h4 += b;
    b = st->h4 >> 26;
    st->h4 = st->h4 & 0x3ffffff;
    st->h0 += b * 5;

    g0 = st->h0 + 5;
    b = g0 >> 26;
    g0 &= 0x3ffffff;
    g1 = st->h1 + b;
    b = g1 >> 26;
    g1 &= 0x3ffffff;
    g2 = st->h2 + b;
    b = g2 >> 26;
    g2 &= 0x3ffffff;
    g3 = st->h3 + b;
    b = g3 >> 26;
    g3 &= 0x3ffffff;
    g4 = st->h4 + b - (1 << 26);

    b = (g4 >> 31) - 1;
    nb = ~b;
    st->h0 = (st->h0 & nb) | (g0 & b);
    st->h1 = (st->h1 & nb) | (g1 & b);
    st->h2 = (st->h2 & nb) | (g2 & b);
    st->h3 = (st->h3 & nb) | (g3 & b);
    st->h4 = (st->h4 & nb) | (g4 & b);

    f0 = ((st->h0) | (st->h1 << 26)) + (ui64)U8TO32_LE(&st->key[0]);
    f1 = ((st->h1 >> 6) | (st->h2 << 20)) +
         (ui64)U8TO32_LE(&st->key[4]);
    f2 = ((st->h2 >> 12) | (st->h3 << 14)) +
         (ui64)U8TO32_LE(&st->key[8]);
    f3 = ((st->h3 >> 18) | (st->h4 << 8)) +
         (ui64)U8TO32_LE(&st->key[12]);

    U32TO8_LE(&mac[0], f0);
    f1 += (f0 >> 32);
    U32TO8_LE(&mac[4], f1);
    f2 += (f1 >> 32);
    U32TO8_LE(&mac[8], f2);
    f3 += (f2 >> 32);
    U32TO8_LE(&mac[12], f3);
}
