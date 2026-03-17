/*
 * An implementation of the SHA3 (Keccak) hash function family.
 *
 * Algorithm specifications: http://keccak.noekeon.org/
 * NIST Announcement:
 * http://csrc.nist.gov/groups/ST/hash/sha-3/winner_sha-3.html
 *
 * Written in 2013 by Fabrizio Tarizzo <fabrizio@fabriziotarizzo.org>
 *
 * ===================================================================
 * The contents of this file are dedicated to the public domain. To
 * the extent that dedication to the public domain is not available,
 * everyone is granted a worldwide, perpetual, royalty-free,
 * non-exclusive license to exercise all rights associated with the
 * contents of this file for any purpose whatsoever.
 * No rights are reserved.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * ===================================================================
*/

#include <string.h>
#include <assert.h>
#include "common.h"
#include "endianess.h"

FAKE_INIT(keccak)

#define KECCAK_F1600_STATE 200

typedef struct
{
    uint64_t state[25];

    /*  The buffer is as long as the state,
     *  but only 'rate' bytes will be used.
     */
    uint8_t  buf[KECCAK_F1600_STATE];

    /*  When absorbing, this is the number of bytes in buf that
     *  are coming from the message and outstanding.
     *  When squeezing, this is the remaining number of bytes
     *  that can be used as digest.
     */
    unsigned valid_bytes;

    /* All values in bytes */
    unsigned capacity;
    unsigned rate;

    uint8_t  squeezing;
    uint8_t  rounds;
} keccak_state;

#undef ROL64
#define ROL64(x,y) ((((x) << (y)) | (x) >> (64-(y))) & 0xFFFFFFFFFFFFFFFFULL)

static void keccak_function (uint64_t *state, unsigned rounds);

EXPORT_SYM int keccak_reset(keccak_state *state)
{
    if (NULL == state)
        return ERR_NULL;

    memset(state->state, 0, sizeof(state->state));
    memset(state->buf, 0, sizeof(state->buf));
    state->valid_bytes = 0;
    state->squeezing = 0;

    return 0;
}

static void keccak_absorb_internal (keccak_state *self)
{
    unsigned i,j;
    uint64_t d;

    for (i=j=0; j < self->rate; ++i, j += 8) {
        d = LOAD_U64_LITTLE(self->buf + j);
        self->state[i] ^= d;
    }
}

static void
keccak_squeeze_internal (keccak_state *self)
{
    unsigned i, j;

    for (i=j=0; j < self->rate; ++i, j += 8) {
        STORE_U64_LITTLE(self->buf+j, self->state[i]);
    }
}

EXPORT_SYM int keccak_init (keccak_state **state,
                            size_t capacity_bytes,
                            uint8_t rounds)
{
    keccak_state *ks;

    if (NULL == state) {
        return ERR_NULL;
    }

    *state = ks = (keccak_state*) calloc(1, sizeof(keccak_state));
    if (NULL == ks)
        return ERR_MEMORY;

    if (capacity_bytes >= KECCAK_F1600_STATE)
        return ERR_DIGEST_SIZE;

    if ((rounds != 12) && (rounds != 24))
        return ERR_NR_ROUNDS;

    ks->capacity  = (unsigned)capacity_bytes;

    ks->rate      = KECCAK_F1600_STATE - ks->capacity;

    ks->squeezing = 0;
    ks->rounds    = rounds;

    return 0;
}

EXPORT_SYM int keccak_destroy(keccak_state *state)
{
    free(state);
    return 0;
}

EXPORT_SYM int keccak_absorb (keccak_state *self,
                              const uint8_t *in,
                              size_t length)
{
    if (NULL==self || NULL==in)
        return ERR_NULL;

    if (self->squeezing != 0)
        return ERR_UNKNOWN;

    while (length > 0) {
        unsigned tc;
        unsigned left;

        left = self->rate - self->valid_bytes;
        tc = (unsigned) MIN(length, left);
        memcpy(self->buf + self->valid_bytes, in, tc);

        self->valid_bytes += tc;
        in                += tc;
        length            -= tc;

        if (self->valid_bytes == self->rate) {
            keccak_absorb_internal (self);
            keccak_function(self->state, self->rounds);
            self->valid_bytes = 0;
        }
    }

    return 0;
}

static void keccak_finish (keccak_state *self, uint8_t padding)
{
    assert(self->squeezing == 0);
    assert(self->valid_bytes < self->rate);

    /* Padding */
    memset(self->buf + self->valid_bytes, 0, self->rate - self->valid_bytes);
    self->buf[self->valid_bytes] = padding;
    self->buf[self->rate-1] |= 0x80;

    /* Final absorb */
    keccak_absorb_internal (self);
    keccak_function (self->state, self->rounds);

    /* First squeeze */
    self->squeezing = 1;
    keccak_squeeze_internal (self);
    self->valid_bytes = self->rate;
}

EXPORT_SYM int keccak_squeeze (keccak_state *self, uint8_t *out, size_t length, uint8_t padding)
{
    if ((NULL == self) || (NULL == out))
        return ERR_NULL;

    if (self->squeezing == 0) {
        keccak_finish (self, padding);
    }

    assert(self->squeezing == 1);
    assert(self->valid_bytes > 0);
    assert(self->valid_bytes <= self->rate);

    while (length > 0) {
        unsigned tc;

        tc = (unsigned)MIN(self->valid_bytes, length);
        memcpy(out, self->buf + (self->rate - self->valid_bytes), tc);

        self->valid_bytes -= tc;
        out               += tc;
        length            -= tc;

        if (self->valid_bytes == 0) {
            keccak_function (self->state, self->rounds);
            keccak_squeeze_internal (self);
            self->valid_bytes = self->rate;
        }
    }

    return 0;
}

EXPORT_SYM int keccak_digest(keccak_state *state, uint8_t *digest, size_t len, uint8_t padding)
{
    keccak_state tmp;

    if ((NULL==state) || (NULL==digest))
        return ERR_NULL;

    if (2*len != state->capacity)
        return ERR_UNKNOWN;

    tmp = *state;
    return keccak_squeeze(&tmp, digest, len, padding);
}

EXPORT_SYM int keccak_copy(const keccak_state *src, keccak_state *dst)
{
    if (NULL == src || NULL == dst) {
        return ERR_NULL;
    }

    *dst = *src;
    return 0;
}

/* Keccak core function */

#define KECCAK_ROUNDS 24

#define ROT_01 36
#define ROT_02 3
#define ROT_03 41
#define ROT_04 18
#define ROT_05 1
#define ROT_06 44
#define ROT_07 10
#define ROT_08 45
#define ROT_09 2
#define ROT_10 62
#define ROT_11 6
#define ROT_12 43
#define ROT_13 15
#define ROT_14 61
#define ROT_15 28
#define ROT_16 55
#define ROT_17 25
#define ROT_18 21
#define ROT_19 56
#define ROT_20 27
#define ROT_21 20
#define ROT_22 39
#define ROT_23 8
#define ROT_24 14

static const uint64_t roundconstants[KECCAK_ROUNDS] = {
    0x0000000000000001ULL,
    0x0000000000008082ULL,
    0x800000000000808aULL,
    0x8000000080008000ULL,
    0x000000000000808bULL,
    0x0000000080000001ULL,
    0x8000000080008081ULL,
    0x8000000000008009ULL,
    0x000000000000008aULL,
    0x0000000000000088ULL,
    0x0000000080008009ULL,
    0x000000008000000aULL,
    0x000000008000808bULL,
    0x800000000000008bULL,
    0x8000000000008089ULL,
    0x8000000000008003ULL,
    0x8000000000008002ULL,
    0x8000000000000080ULL,
    0x000000000000800aULL,
    0x800000008000000aULL,
    0x8000000080008081ULL,
    0x8000000000008080ULL,
    0x0000000080000001ULL,
    0x8000000080008008ULL
};

static void keccak_function (uint64_t *state, unsigned rounds)
{
    unsigned i;
    unsigned start_round;

    /* Temporary variables to avoid indexing overhead */
    uint64_t a0, a1, a2, a3, a4, a5, a6, a7, a8, a9, a10, a11, a12;
    uint64_t a13, a14, a15, a16, a17, a18, a19, a20, a21, a22, a23, a24;

    uint64_t b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b11, b12;
    uint64_t b13, b14, b15, b16, b17, b18, b19, b20, b21, b22, b23, b24;

    uint64_t c0, c1, c2, c3, c4, d;

    a0  = state[0];
    a1  = state[1];
    a2  = state[2];
    a3  = state[3];
    a4  = state[4];
    a5  = state[5];
    a6  = state[6];
    a7  = state[7];
    a8  = state[8];
    a9  = state[9];
    a10 = state[10];
    a11 = state[11];
    a12 = state[12];
    a13 = state[13];
    a14 = state[14];
    a15 = state[15];
    a16 = state[16];
    a17 = state[17];
    a18 = state[18];
    a19 = state[19];
    a20 = state[20];
    a21 = state[21];
    a22 = state[22];
    a23 = state[23];
    a24 = state[24];

    if (rounds == 24)
        start_round = 0;
    else /* rounds == 12 */
        start_round = 12;

    for (i = start_round; i < KECCAK_ROUNDS; ++i) {
        /*
           Uses temporary variables and loop unrolling to
           avoid array indexing and inner loops overhead
        */

        /* Prepare column parity for Theta step */
        c0 = a0 ^ a5 ^ a10 ^ a15 ^ a20;
        c1 = a1 ^ a6 ^ a11 ^ a16 ^ a21;
        c2 = a2 ^ a7 ^ a12 ^ a17 ^ a22;
        c3 = a3 ^ a8 ^ a13 ^ a18 ^ a23;
        c4 = a4 ^ a9 ^ a14 ^ a19 ^ a24;

        /* Theta + Rho + Pi steps */
        d   = c4 ^ ROL64(c1, 1);
        b0  = d ^ a0;
        b16 = ROL64(d ^ a5,  ROT_01);
        b7  = ROL64(d ^ a10, ROT_02);
        b23 = ROL64(d ^ a15, ROT_03);
        b14 = ROL64(d ^ a20, ROT_04);

        d   = c0 ^ ROL64(c2, 1);
        b10 = ROL64(d ^ a1,  ROT_05);
        b1  = ROL64(d ^ a6,  ROT_06);
        b17 = ROL64(d ^ a11, ROT_07);
        b8  = ROL64(d ^ a16, ROT_08);
        b24 = ROL64(d ^ a21, ROT_09);

        d   = c1 ^ ROL64(c3, 1);
        b20 = ROL64(d ^ a2,  ROT_10);
        b11 = ROL64(d ^ a7,  ROT_11);
        b2  = ROL64(d ^ a12, ROT_12);
        b18 = ROL64(d ^ a17, ROT_13);
        b9  = ROL64(d ^ a22, ROT_14);

        d   = c2 ^ ROL64(c4, 1);
        b5  = ROL64(d ^ a3,  ROT_15);
        b21 = ROL64(d ^ a8,  ROT_16);
        b12 = ROL64(d ^ a13, ROT_17);
        b3  = ROL64(d ^ a18, ROT_18);
        b19 = ROL64(d ^ a23, ROT_19);

        d   = c3 ^ ROL64(c0, 1);
        b15 = ROL64(d ^ a4,  ROT_20);
        b6  = ROL64(d ^ a9,  ROT_21);
        b22 = ROL64(d ^ a14, ROT_22);
        b13 = ROL64(d ^ a19, ROT_23);
        b4  = ROL64(d ^ a24, ROT_24);

        /* Chi + Iota steps */
        a0  = b0  ^ (~b1  & b2) ^ roundconstants[i];
        a1  = b1  ^ (~b2  & b3);
        a2  = b2  ^ (~b3  & b4);
        a3  = b3  ^ (~b4  & b0);
        a4  = b4  ^ (~b0  & b1);

        a5  = b5  ^ (~b6  & b7);
        a6  = b6  ^ (~b7  & b8);
        a7  = b7  ^ (~b8  & b9);
        a8  = b8  ^ (~b9  & b5);
        a9  = b9  ^ (~b5  & b6);

        a10 = b10 ^ (~b11 & b12);
        a11 = b11 ^ (~b12 & b13);
        a12 = b12 ^ (~b13 & b14);
        a13 = b13 ^ (~b14 & b10);
        a14 = b14 ^ (~b10 & b11);

        a15 = b15 ^ (~b16 & b17);
        a16 = b16 ^ (~b17 & b18);
        a17 = b17 ^ (~b18 & b19);
        a18 = b18 ^ (~b19 & b15);
        a19 = b19 ^ (~b15 & b16);

        a20 = b20 ^ (~b21 & b22);
        a21 = b21 ^ (~b22 & b23);
        a22 = b22 ^ (~b23 & b24);
        a23 = b23 ^ (~b24 & b20);
        a24 = b24 ^ (~b20 & b21);
    }

    state[0]  = a0;
    state[1]  = a1;
    state[2]  = a2;
    state[3]  = a3;
    state[4]  = a4;
    state[5]  = a5;
    state[6]  = a6;
    state[7]  = a7;
    state[8]  = a8;
    state[9]  = a9;
    state[10] = a10;
    state[11] = a11;
    state[12] = a12;
    state[13] = a13;
    state[14] = a14;
    state[15] = a15;
    state[16] = a16;
    state[17] = a17;
    state[18] = a18;
    state[19] = a19;
    state[20] = a20;
    state[21] = a21;
    state[22] = a22;
    state[23] = a23;
    state[24] = a24;
}


