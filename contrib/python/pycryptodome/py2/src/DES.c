/*
 *  DES.c: DES/3DES support for PyCrypto using LibTomCrypt
 *
 * Written in 2009 by Dwayne C. Litzenberger <dlitz@dlitz.net>
 *
 * ===================================================================
 * The contents of this file are dedicated to the public domain.  To
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
 *
 * Country of origin: Canada
 */

#include "common.h"
#include "block_base.h"

#ifndef PCT_DES3_MODULE
FAKE_INIT(raw_des)
#else
FAKE_INIT(raw_des3)
#endif

/* Setting this will cause LibTomCrypt to return CRYPT_INVALID_ARG when its
 * assert-like LTC_ARGCHK macro fails. */
#define ARGTYPE 4

/* Include the actial DES implementation */
#define LTC_NO_PROTOTYPES
#include "libtom/tomcrypt_des.c"

struct block_state {
    symmetric_key sk;
};

static int block_init(struct block_state *self, const uint8_t *key, size_t keylen)
{
    int rc;
#ifdef PCT_DES3_MODULE
    rc = des3_setup(key, keylen, 0, &self->sk);
#else
    rc = des_setup(key, keylen, 0, &self->sk);
#endif
    switch (rc) {
    case CRYPT_OK:
        return 0;
    case CRYPT_INVALID_KEYSIZE:
        return ERR_KEY_SIZE;
    case CRYPT_INVALID_ROUNDS:
        return ERR_NR_ROUNDS;
    case CRYPT_INVALID_ARG:
        return ERR_UNKNOWN;
    }
    return ERR_UNKNOWN;
}

static void block_finalize(struct block_state *self)
{
}

static void block_encrypt(struct block_state *self, const uint8_t *in, uint8_t *out)
{
#ifdef PCT_DES3_MODULE
    des3_ecb_encrypt(in, out, &self->sk);
#else
    des_ecb_encrypt(in, out, &self->sk);
#endif
}

static void block_decrypt(struct block_state *self, const uint8_t *in, uint8_t *out)
{
#ifdef PCT_DES3_MODULE
    des3_ecb_decrypt(in, out, &self->sk);
#else
    des_ecb_decrypt(in, out, &self->sk);
#endif
}

#ifdef PCT_DES3_MODULE
# define MODULE_NAME pycryptodome_DES3   /* triple DES */
# define BLOCK_SIZE 8       /* 64-bit block size */
# define KEY_SIZE  0        /* variable key size (can be 128 or 192 bits (including parity) */
#else
# define MODULE_NAME pycryptodome_DES   /* single DES */
# define BLOCK_SIZE 8       /* 64-bit block size */
# define KEY_SIZE  8        /* 64-bit keys (including parity) */
#endif
#include "block_common.c"
