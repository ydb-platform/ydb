/*
 * Salsa20.c : Source for the Salsa20 stream cipher.
 *
 * Part of the Python Cryptography Toolkit
 *
 * Contributed by Fabrizio Tarizzo <fabrizio@fabriziotarizzo.org>.
 * Based on the reference implementation by D. J. Bernstein
 * <http://cr.yp.to/snuffle/salsa20/regs/salsa20.c>
 *
 * =======================================================================
 * The contents of this file are dedicated to the public domain.  To the
 * extent that dedication to the public domain is not available, everyone
 * is granted a worldwide, perpetual, royalty-free, non-exclusive license
 * to exercise all rights associated with the contents of this file for
 * any purpose whatsoever.  No rights are reserved.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * =======================================================================
 */

#include "common.h"
#include "endianess.h"

FAKE_INIT(Salsa20)

#define ROUNDS 20
#define MAX_KEY_SIZE 32

static const uint8_t sigma[16] = "expand 32-byte k";
static const uint8_t tau[16]   = "expand 16-byte k";

#define ROTL32(v, c) (((v) << (c)) | ((v) >> (32 - (c))))
#define XOR(v,w) ((v) ^ (w))

typedef struct  {
    uint32_t input[16];
    uint8_t  block[64];
    uint8_t  blockindex;
} stream_state;

static void _salsa20_block(unsigned rounds, uint32_t *input, uint8_t *output)
{
    unsigned x0, x1, x2, x3, x4, x5, x6, x7;
    unsigned x8, x9, x10, x11, x12, x13, x14, x15;
    unsigned i;

    x0  = input[0];
    x1  = input[1];
    x2  = input[2];
    x3  = input[3];
    x4  = input[4];
    x5  = input[5];
    x6  = input[6];
    x7  = input[7];
    x8  = input[8];
    x9  = input[9];
    x10 = input[10];
    x11 = input[11];
    x12 = input[12];
    x13 = input[13];
    x14 = input[14];
    x15 = input[15];
    
    for (i = rounds; i > 0; i -= 2) {
        /* Column round */
        x4  = XOR( x4, ROTL32( x0 + x12,  7));
        x8  = XOR( x8, ROTL32( x4 +  x0,  9));
        x12 = XOR(x12, ROTL32( x8 +  x4, 13));
        x0  = XOR( x0, ROTL32(x12 +  x8, 18));
        x9  = XOR( x9, ROTL32( x5 +  x1,  7));
        x13 = XOR(x13, ROTL32( x9 +  x5,  9));
        x1  = XOR( x1, ROTL32(x13 +  x9, 13));
        x5  = XOR( x5, ROTL32( x1 + x13, 18));
        x14 = XOR(x14, ROTL32(x10 +  x6,  7));
        x2  = XOR( x2, ROTL32(x14 + x10,  9));
        x6  = XOR( x6, ROTL32( x2 + x14, 13));
        x10 = XOR(x10, ROTL32( x6 +  x2, 18));
        x3  = XOR( x3, ROTL32(x15 + x11,  7));
        x7  = XOR( x7, ROTL32( x3 + x15,  9));
        x11 = XOR(x11, ROTL32( x7 +  x3, 13));
        x15 = XOR(x15, ROTL32(x11 +  x7, 18));
        
        /* Row round */
        x1  = XOR( x1, ROTL32( x0 +  x3,  7));
        x2  = XOR( x2, ROTL32( x1 +  x0,  9));
        x3  = XOR( x3, ROTL32( x2 +  x1, 13));
        x0  = XOR( x0, ROTL32( x3 +  x2, 18));
        x6  = XOR( x6, ROTL32( x5 +  x4,  7));
        x7  = XOR( x7, ROTL32( x6 +  x5,  9));
        x4  = XOR( x4, ROTL32( x7 +  x6, 13));
        x5  = XOR( x5, ROTL32( x4 +  x7, 18));
        x11 = XOR(x11, ROTL32(x10 +  x9,  7));
        x8  = XOR( x8, ROTL32(x11 + x10,  9));
        x9  = XOR( x9, ROTL32( x8 + x11, 13));
        x10 = XOR(x10, ROTL32( x9 +  x8, 18));
        x12 = XOR(x12, ROTL32(x15 + x14,  7));
        x13 = XOR(x13, ROTL32(x12 + x15,  9));
        x14 = XOR(x14, ROTL32(x13 + x12, 13));
        x15 = XOR(x15, ROTL32(x14 + x13, 18));
    }
    
    x0  = x0 + input[0];
    x1  = x1 + input[1];
    x2  = x2 + input[2];
    x3  = x3 + input[3];
    x4  = x4 + input[4];
    x5  = x5 + input[5];
    x6  = x6 + input[6];
    x7  = x7 + input[7];
    x8  = x8 + input[8];
    x9  = x9 + input[9];
    x10 = x10 + input[10];
    x11 = x11 + input[11];
    x12 = x12 + input[12];
    x13 = x13 + input[13];
    x14 = x14 + input[14];
    x15 = x15 + input[15];
    
    STORE_U32_LITTLE (output + 0, x0);
    STORE_U32_LITTLE (output + 4, x1);
    STORE_U32_LITTLE (output + 8, x2);
    STORE_U32_LITTLE (output + 12, x3);
    STORE_U32_LITTLE (output + 16, x4);
    STORE_U32_LITTLE (output + 20, x5);
    STORE_U32_LITTLE (output + 24, x6);
    STORE_U32_LITTLE (output + 28, x7);
    STORE_U32_LITTLE (output + 32, x8);
    STORE_U32_LITTLE (output + 36, x9);
    STORE_U32_LITTLE (output + 40, x10);
    STORE_U32_LITTLE (output + 44, x11);
    STORE_U32_LITTLE (output + 48, x12);
    STORE_U32_LITTLE (output + 52, x13);
    STORE_U32_LITTLE (output + 56, x14);
    STORE_U32_LITTLE (output + 60, x15);
    
    /* Increment block counter */
    input[8] = input[8] + 1;
    if (!input[8]) {
        input[9] = input[9] + 1;
        /* stopping at 2^70 bytes per nonce is user's responsibility */
    }
}

/*
 * Salsa20/8 Core function (combined with XOR)
 *
 * This function accepts two 64-byte Python byte strings (x and y).
 * It creates a new 64-byte Python byte string with the result
 * of the expression salsa20_8(xor(x,y)).
 */
EXPORT_SYM int Salsa20_8_core(const uint8_t *x, const uint8_t *y, uint8_t *out)
{
    uint32_t input_32[16];
    int i;

    if (NULL==x || NULL==y || NULL==out)
        return ERR_NULL;

    for (i=0; i<16; i++) {
        uint32_t tmp;

        tmp = LOAD_U32_LITTLE(&x[i*4]);
        input_32[i] = LOAD_U32_LITTLE(&y[i*4]);
        input_32[i] ^= tmp;
    }

    _salsa20_block(8, input_32, out);
    return 0;
}

EXPORT_SYM int Salsa20_stream_init(uint8_t *key, size_t keylen,
                        uint8_t *nonce, size_t nonce_len,
                        stream_state **pSalsaState)
{
    const uint8_t *constants;
    uint32_t *input;
    stream_state *salsaState;
    unsigned i;

    if (NULL == pSalsaState || NULL == key || NULL == nonce)
        return ERR_NULL;

    if (keylen != 16 && keylen != 32)
        return ERR_KEY_SIZE;
    constants = keylen == 32 ? sigma : tau;

    if (nonce_len != 8)
        return ERR_NONCE_SIZE;
    
    *pSalsaState = salsaState = calloc(1, sizeof(stream_state));
    if (NULL == salsaState)
        return ERR_MEMORY;

    input = salsaState->input;

    input[0] = LOAD_U32_LITTLE(constants + 0);
    /** Set input[1..4] **/
    for (i=0; i<4; i++)
        input[i+1] = LOAD_U32_LITTLE(key + 4*i);
    input[5] = LOAD_U32_LITTLE(constants + 4);
    input[6] = LOAD_U32_LITTLE(nonce + 0);
    input[7] = LOAD_U32_LITTLE(nonce + 4);
    /* Block counter setup*/
    input[8]  = 0;
    input[9]  = 0;
    input[10] = LOAD_U32_LITTLE(constants + 8);
    /** Set input[11..14] **/ 
    if (keylen == 32) {
        key += 16;
    }
    for (i=0; i<4; i++)
        input[i+11] = LOAD_U32_LITTLE(key + 4*i);
    input[15] = LOAD_U32_LITTLE(constants + 12);

    salsaState->blockindex = 64;
    return 0;
}

EXPORT_SYM int Salsa20_stream_destroy(stream_state *salsaState)
{
    free(salsaState);
    return 0;
}

EXPORT_SYM int Salsa20_stream_encrypt(stream_state *salsaState, const uint8_t in[],
                           uint8_t out[], size_t len)
{
    unsigned i;
    for (i = 0; i < len; ++i) {
        if (salsaState->blockindex == 64) {
            salsaState->blockindex = 0;
            _salsa20_block(ROUNDS, salsaState->input, salsaState->block);
        }
        out[i] = in[i] ^ salsaState->block[salsaState->blockindex];
        salsaState->blockindex++;
    }
    return 0;
}
