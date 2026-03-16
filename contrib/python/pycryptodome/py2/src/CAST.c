/*
   cast.c -- implementation of CAST-128 (aka CAST5) as described in RFC2144

   Written in 1997 by Wim Lewis <wiml@hhhh.org> based entirely on RFC2144.
   Minor modifications made in 2002 by Andrew M. Kuchling <amk@amk.ca>.

   ===================================================================
   The contents of this file are dedicated to the public domain.  To
   the extent that dedication to the public domain is not available,
   everyone is granted a worldwide, perpetual, royalty-free,
   non-exclusive license to exercise all rights associated with the
   contents of this file for any purpose whatsoever.
   No rights are reserved.

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
   NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
   BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
   ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
   CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
   SOFTWARE.
   ===================================================================

   Consult your local laws for possible restrictions on use, distribution, and
   import/export. RFC2144 states that this algorithm "is available worldwide
   on a royalty-free basis for commercial and non-commercial uses".

   This code is a pretty straightforward transliteration of the RFC into C.
   It has not been optimized much at all: byte-order-independent arithmetic
   operations are used where order-dependent pointer ops or unions might be
   faster; the code could be rearranged to give the optimizer a better
   chance to speed things up; etc.

   This code requires a vaguely ANSI-ish compiler.

   compile -DTEST to include main() which performs the tests
       specified in RFC2144

   Tested with gcc 2.5.8 on i486, i586, i686, hp pa-risc, mc68040, sparc;
   also with gcc 2.7.2 and (with minor changes) native Sun compiler on sparc

*/

#include "common.h"
#include "endianess.h"
#include "block_base.h"

FAKE_INIT(raw_cast)

#define MODULE_NAME pycryptodome_CAST
#define BLOCK_SIZE 8
#define KEY_SIZE 0

/* this struct probably belongs in cast.h */
struct block_state {
    /* masking and rotate keys */
    uint32_t Km[16];
    uint8_t Kr[16];
    /* number of rounds (depends on original unpadded keylength) */
    unsigned rounds;
};

/* these are the eight 32*256 S-boxes */
#include "cast5.c"

/* this is the round function f(D, Km, Kr) */
static uint32_t castfunc(uint32_t D, uint32_t Kmi, uint8_t Kri, unsigned type)
{
    uint32_t I, f;
    uint8_t Ibe[4];
    #define Ia Ibe[0]
    #define Ib Ibe[1]
    #define Ic Ibe[2]
    #define Id Ibe[3]

    switch(type) {
    case 0:
        I = (Kmi + D) ;
        break;
    case 1:
        I = (Kmi ^ D) ;
        break;
    default:
    case 2:
        I = (Kmi - D) ;
        break;
    }

    if (Kri>0) {
        I = ( I << Kri ) | ( I >> ( 32-Kri ) );
    }
    STORE_U32_BIG(Ibe, I);

    switch(type) {
    case 0:
        f = ((S1[Ia] ^ S2[Ib]) - S3[Ic]) + S4[Id];
        break;
    case 1:
        f = ((S1[Ia] - S2[Ib]) + S3[Ic]) ^ S4[Id];
        break;
    default:
    case 2:
        f = ((S1[Ia] + S2[Ib]) ^ S3[Ic]) - S4[Id];
        break;
    }

    return f;
}

/* encrypts/decrypts one block of data according to the key schedule
   pointed to by `key'. Encrypts if decrypt=0, otherwise decrypts. */
static void castcrypt(struct block_state *key, uint8_t *block, int decrypt)
{
    uint32_t L, R, tmp, f;
    uint32_t Kmi;
    uint8_t  Kri;
    unsigned functype, round;

    L = LOAD_U32_BIG(block + 0);
    R = LOAD_U32_BIG(block + 4);

    for(round = 0; round < key->rounds; round ++) {

        if (!decrypt) {
            Kmi = key->Km[round];
            Kri = key->Kr[round];
            functype = round % 3;
        } else {
            Kmi = key->Km[(key->rounds) - round - 1];
            Kri = key->Kr[(key->rounds) - round - 1];
            functype = (((key->rounds) - round - 1) % 3);
        }

        f = castfunc(R, Kmi, Kri, functype);

        tmp = L;
        L = R;
        R = tmp ^ f;
    }

    STORE_U32_BIG(block + 0, R);
    STORE_U32_BIG(block + 4, L);
}

/* fetch a uint8_t from an array of uint32_ts */
#define b(a,n) (((a)[n/4] >> (24-((n&3)*8))) & 0xFF)

/* key schedule round functions */

#define XZRound(T, F, ki1, ki2, ki3, ki4, \
        si11, si12, si13, si14, si15,\
                                si25,\
                                    si35,\
                                    si45 ) \
    T[0] = F[ki1] ^ S5[si11   ] ^ S6[si12  ] ^ S7[si13   ] ^ S8[si14  ] ^ S7[si15];\
    T[1] = F[ki2] ^ S5[b(T, 0)] ^ S6[b(T,2)] ^ S7[b(T, 1)] ^ S8[b(T,3)] ^ S8[si25];\
    T[2] = F[ki3] ^ S5[b(T, 7)] ^ S6[b(T,6)] ^ S7[b(T, 5)] ^ S8[b(T,4)] ^ S5[si35];\
    T[3] = F[ki4] ^ S5[b(T,10)] ^ S6[b(T,9)] ^ S7[b(T,11)] ^ S8[b(T,8)] ^ S6[si45];

#define zxround() XZRound(z, x, 0, 2, 3, 1, \
            b(x,13), b(x,15), b(x,12), b(x,14),\
            b(x, 8), b(x,10), b(x, 9), b(x,11))

#define xzround() XZRound(x, z, 2, 0, 1, 3, \
            b(z,5), b(z,7), b(z,4), b(z,6), \
            b(z,0), b(z,2), b(z,1), b(z,3))

#define Kround(T, base, F,\
           i11, i12, i13, i14, i15,\
           i21, i22, i23, i24, i25,\
           i31, i32, i33, i34, i35,\
           i41, i42, i43, i44, i45)\
    T[base+0] = S5[b(F,i11)] ^ S6[b(F,i12)] ^ S7[b(F,i13)] ^ S8[b(F,i14)] ^ S5[b(F,i15)];\
    T[base+1] = S5[b(F,i21)] ^ S6[b(F,i22)] ^ S7[b(F,i23)] ^ S8[b(F,i24)] ^ S6[b(F,i25)];\
    T[base+2] = S5[b(F,i31)] ^ S6[b(F,i32)] ^ S7[b(F,i33)] ^ S8[b(F,i34)] ^ S7[b(F,i35)];\
    T[base+3] = S5[b(F,i41)] ^ S6[b(F,i42)] ^ S7[b(F,i43)] ^ S8[b(F,i44)] ^ S8[b(F,i45)];

/* generates sixteen 32-bit subkeys based on a 4x32-bit input key;
   modifies the input key *in as well. */
static void schedulekeys_half(uint32_t *in, uint32_t *keys)
{
    uint32_t x[4], z[4];

    x[0] = in[0];
    x[1] = in[1];
    x[2] = in[2];
    x[3] = in[3];

    zxround();
    Kround(keys, 0, z,
           8,  9, 7, 6,  2,
           10, 11, 5, 4,  6,
           12, 13, 3, 2,  9,
           14, 15, 1, 0, 12);
    xzround();
    Kround(keys, 4, x,
           3,  2, 12, 13,  8,
           1,  0, 14, 15, 13,
           7,  6,  8,  9,  3,
           5,  4, 10, 11,  7);
    zxround();
    Kround(keys, 8, z,
           3,  2, 12, 13,  9,
           1,  0, 14, 15, 12,
           7,  6,  8,  9,  2,
           5,  4, 10, 11,  6);
    xzround();
    Kround(keys, 12, x,
           8,  9, 7, 6,  3,
           10, 11, 5, 4,  7,
           12, 13, 3, 2,  8,
           14, 15, 1, 0, 13);

    in[0] = x[0];
    in[1] = x[1];
    in[2] = x[2];
    in[3] = x[3];
}

/* generates a key schedule from an input key */
static void castschedulekeys(struct block_state *schedule, const uint8_t *key, size_t keybytes)
{
    uint32_t x[4];
    uint8_t  paddedkey[16];
    uint32_t Kr_wide[16];
    unsigned i;

    for(i = 0; i < keybytes; i++)
        paddedkey[i] = key[i];
    for(     ; i < 16      ; i++)
        paddedkey[i] = 0;

    if (keybytes <= 10)
        schedule->rounds = 12;
    else
        schedule->rounds = 16;

    x[0] = LOAD_U32_BIG(paddedkey + 0);
    x[1] = LOAD_U32_BIG(paddedkey + 4);
    x[2] = LOAD_U32_BIG(paddedkey + 8);
    x[3] = LOAD_U32_BIG(paddedkey + 12);

    schedulekeys_half(x, schedule->Km);
    schedulekeys_half(x, Kr_wide);

    for(i = 0; i < 16; i ++) {
        /* The Kr[] subkeys are used for 32-bit circular shifts,
           so we only need to keep them modulo 32 */
        schedule->Kr[i] = (uint8_t)(Kr_wide[i] & 0x1F);
    }
}

static int
block_init(struct block_state *self, const uint8_t *key, size_t keylength)
{
    /* make sure the key length is within bounds */
    if (keylength < 5 || keylength > 16) {
            return ERR_KEY_SIZE;
    }

    /* do the actual key schedule setup */
    castschedulekeys(self, key, keylength);
    return 0;
}

static void block_finalize(struct block_state* self)
{
}

static void
block_encrypt(struct block_state *self, const uint8_t *in, uint8_t *out)
{
    memcpy(out, in, 8);
    castcrypt(self, out, 0);
}

static void block_decrypt(struct block_state *self, const uint8_t *in, uint8_t *out)
{
    memcpy(out, in, 8);
    castcrypt(self, out, 1);
}

#include "block_common.c"
