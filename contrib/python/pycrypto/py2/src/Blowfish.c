/*
 *
 *  Blowfish.c : Blowfish implementation
 *
 * Written in 2008 by Dwayne C. Litzenberger <dlitz@dlitz.net>
 *
 * =======================================================================
 * The contents of this file are dedicated to the public domain.  To the extent
 * that dedication to the public domain is not available, everyone is granted a
 * worldwide, perpetual, royalty-free, non-exclusive license to exercise all
 * rights associated with the contents of this file for any purpose whatsoever.
 * No rights are reserved.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 * =======================================================================
 *
 * Country of origin: Canada
 *
 * The Blowfish algorithm is documented at
 * http://www.schneier.com/paper-blowfish-fse.html
 */

#include "config.h"
#if HAVE_STDINT_H
# include <stdint.h>
#elif defined(__sun) || defined(__sun__)
# include <sys/inttypes.h>
#else
# error "stdint.h not found"
#endif
#include <assert.h>
#include <string.h>
#include "Python.h"

#include "Blowfish-tables.h"

#define MODULE_NAME _Blowfish
#define BLOCK_SIZE 8    /* 64-bit block size */
#define KEY_SIZE 0      /* variable key size */

#define BLOWFISH_MAGIC 0xf9d565deu
typedef struct {
    uint32_t magic;

    /* P permutation */
    uint32_t P[18];

    /* Subkeys (S-boxes) */
    uint32_t S1[256];
    uint32_t S2[256];
    uint32_t S3[256];
    uint32_t S4[256];
} Blowfish_state;

/* The Blowfish round function F.  Everything is taken modulo 2**32 */
#define F(a, b, c, d) (((a) + (b)) ^ (c)) + (d)

static inline uint32_t bytes_to_word(const unsigned char *in)
{
    /* big endian */
    return (in[0] << 24) | (in[1] << 16) | (in[2] << 8) | in[3];
}

static inline void word_to_bytes(uint32_t w, unsigned char *out)
{
    /* big endian */
    out[0] = (w >> 24) & 0xff;
    out[1] = (w >> 16) & 0xff;
    out[2] = (w >> 8) & 0xff;
    out[3] = w & 0xff;
}

static inline void inline_encrypt(Blowfish_state *self, uint32_t *pxL, uint32_t *pxR)
{
    int i;
    uint32_t xL = *pxL;
    uint32_t xR = *pxR;
    uint32_t tmp;

    for (i = 0; i < 16; i++) {
        xL ^= self->P[i];

        /* a || b || c || d = xL (big endian) */
        xR ^= F(self->S1[(xL >> 24) & 0xff],    /* S1[a] */
                self->S2[(xL >> 16) & 0xff],    /* S2[b] */
                self->S3[(xL >> 8) & 0xff],     /* S3[c] */
                self->S4[xL & 0xff]);           /* S4[d] */

        /* Swap xL, xR */
        tmp = xL; xL = xR; xR = tmp;
    }

    /* Swap xL, xR */
    tmp = xL; xL = xR; xR = tmp;

    xR ^= self->P[16];
    xL ^= self->P[17];

    *pxL = xL;
    *pxR = xR;
}

static inline void inline_decrypt(Blowfish_state *self, uint32_t *pxL, uint32_t *pxR)
{
    int i;
    uint32_t xL = *pxL;
    uint32_t xR = *pxR;
    uint32_t tmp;

    xL ^= self->P[17];
    xR ^= self->P[16];

    /* Swap xL, xR */
    tmp = xL; xL = xR; xR = tmp;

    for (i = 15; i >= 0; i--) {
        /* Swap xL, xR */
        tmp = xL; xL = xR; xR = tmp;

        /* a || b || c || d = xL (big endian) */
        xR ^= F(self->S1[(xL >> 24) & 0xff],    /* S1[a] */
                self->S2[(xL >> 16) & 0xff],    /* S2[b] */
                self->S3[(xL >> 8) & 0xff],     /* S3[c] */
                self->S4[xL & 0xff]);           /* S4[d] */

        xL ^= self->P[i];
    }

    *pxL = xL;
    *pxR = xR;
}

static void Blowfish_encrypt(Blowfish_state *self, const unsigned char *in, unsigned char *out)
{
    uint32_t xL, xR;

    /* Make sure the object is initialized */
    assert(self->magic == BLOWFISH_MAGIC);

    /* big endian */
    xL = bytes_to_word(in);
    xR = bytes_to_word(in+4);

    inline_encrypt(self, &xL, &xR);

    /* big endian */
    word_to_bytes(xL, out);
    word_to_bytes(xR, out+4);
}

static void Blowfish_decrypt(Blowfish_state *self, const unsigned char *in, unsigned char *out)
{
    uint32_t xL, xR;

    /* Make sure the object is initialized */
    assert(self->magic == BLOWFISH_MAGIC);

    /* big endian */
    xL = bytes_to_word(in);
    xR = bytes_to_word(in+4);

    inline_decrypt(self, &xL, &xR);

    /* big endian */
    word_to_bytes(xL, out);
    word_to_bytes(xR, out+4);
}

static void Blowfish_init(Blowfish_state *self, const unsigned char *key, int keylen)
{
    uint32_t word;
    int i;
    uint32_t xL, xR;

    self->magic = 0;

    if (keylen < 1) {
        PyErr_SetString(PyExc_ValueError, "Key cannot be empty");
        return;
    } else if (keylen > 56) {
        PyErr_SetString(PyExc_ValueError, "Maximum key size is 448 bits");
        return;
    }

    /* Initialize the P-array with the digits of Pi, and XOR it with the key */
    word = 0;
    for (i = 0; i < 18*4; i++) {
        word = (word << 8) | key[i % keylen];
        if ((i & 3) == 3) {
            self->P[i >> 2] = initial_P[i >> 2] ^ word;
            word = 0;
        }
    }

    /* Initialize the S-boxes with more digits of Pi */
    memcpy(self->S1, initial_S1, 256*sizeof(uint32_t));
    memcpy(self->S2, initial_S2, 256*sizeof(uint32_t));
    memcpy(self->S3, initial_S3, 256*sizeof(uint32_t));
    memcpy(self->S4, initial_S4, 256*sizeof(uint32_t));

    /* Stir the subkeys */
    xL = xR = 0;
    for (i = 0; i < 18; i += 2) {
        inline_encrypt(self, &xL, &xR);
        self->P[i] = xL;
        self->P[i+1] = xR;
    }
    for (i = 0; i < 256; i += 2) {
        inline_encrypt(self, &xL, &xR);
        self->S1[i] = xL;
        self->S1[i+1] = xR;
    }
    for (i = 0; i < 256; i += 2) {
        inline_encrypt(self, &xL, &xR);
        self->S2[i] = xL;
        self->S2[i+1] = xR;
    }
    for (i = 0; i < 256; i += 2) {
        inline_encrypt(self, &xL, &xR);
        self->S3[i] = xL;
        self->S3[i+1] = xR;
    }
    for (i = 0; i < 256; i += 2) {
        inline_encrypt(self, &xL, &xR);
        self->S4[i] = xL;
        self->S4[i+1] = xR;
    }

    self->magic = BLOWFISH_MAGIC;
}

#define block_state Blowfish_state
#define block_init Blowfish_init
#define block_encrypt Blowfish_encrypt
#define block_decrypt Blowfish_decrypt

#include "block_template.c"

/* vim:set ts=4 sw=4 sts=4 expandtab: */
