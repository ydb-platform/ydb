/*
 *  md4.c : MD4 hash algorithm.
 *
 * Part of the Python Cryptography Toolkit
 *
 * Originally written by: A.M. Kuchling
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
 */

#include "common.h"
#include "endianess.h"

FAKE_INIT(MD4)

typedef struct {
    uint32_t A,B,C,D;
    uint64_t bitlen;
    uint8_t buf[64];
    unsigned count;
} hash_state;

#define F(x, y, z) (((x) & (y)) | ((~x) & (z)))
#define G(x, y, z) (((x) & (y)) | ((x) & (z)) | ((y) & (z)))
#define H(x, y, z) ((x) ^ (y) ^ (z))

/* ROTATE_LEFT rotates x left n bits */
#define ROL(x, n) (((x) << n) | ((x) >> (32-n) ))

EXPORT_SYM int md4_init (hash_state **md4State)
{
    hash_state *hs;
    
    if (NULL == md4State) {
        return ERR_NULL;
    }

    *md4State = hs = (hash_state*) calloc(1, sizeof(hash_state));
    if (NULL == hs)
        return ERR_MEMORY;
 
    hs->A=0x67452301;
    hs->B=0xefcdab89;
    hs->C=0x98badcfe;
    hs->D=0x10325476;

    return 0;
}

EXPORT_SYM int md4_destroy(hash_state *hs)
{
    free(hs);
    return 0;
}

EXPORT_SYM int md4_copy(const hash_state *src, hash_state *dst)
{
    if (NULL == src || NULL == dst) {
        return ERR_NULL;
    }

    *dst = *src;
    return 0;
}

EXPORT_SYM int md4_update(hash_state *hs, const uint8_t *buf, size_t len)
{
    if (NULL == hs || NULL == buf) {
        return ERR_NULL;
    }

    assert(hs->count < 64);

    hs->bitlen += (uint64_t)len * 8;

    while (len>0)  {
        unsigned left, tc;

        left = 64 - hs->count;
        tc = (unsigned)MIN(left, len);
        memcpy(hs->buf+hs->count, buf, tc);
        hs->count += tc;
        buf += tc;
        len -= tc;

        if (hs->count==64)  {
            uint32_t X[16], A, B, C, D;
            unsigned j;
            
            hs->count=0;
            for(j=0; j<16; j++) {
                X[j] = LOAD_U32_LITTLE(&hs->buf[j*4]);
            }

            A=hs->A; B=hs->B; C=hs->C; D=hs->D;

#define function(a,b,c,d,k,s) a=ROL(a+F(b,c,d)+X[k],s);  
            function(A,B,C,D, 0, 3);
            function(D,A,B,C, 1, 7);
            function(C,D,A,B, 2,11);
            function(B,C,D,A, 3,19);
            function(A,B,C,D, 4, 3);
            function(D,A,B,C, 5, 7);
            function(C,D,A,B, 6,11);
            function(B,C,D,A, 7,19);
            function(A,B,C,D, 8, 3);
            function(D,A,B,C, 9, 7);
            function(C,D,A,B,10,11);
            function(B,C,D,A,11,19);
            function(A,B,C,D,12, 3);
            function(D,A,B,C,13, 7);
            function(C,D,A,B,14,11);
            function(B,C,D,A,15,19);

#undef function   
#define function(a,b,c,d,k,s) a=ROL(a+G(b,c,d)+X[k]+(uint32_t)0x5a827999,s);     
            function(A,B,C,D, 0, 3);
            function(D,A,B,C, 4, 5);
            function(C,D,A,B, 8, 9);
            function(B,C,D,A,12,13);
            function(A,B,C,D, 1, 3);
            function(D,A,B,C, 5, 5);
            function(C,D,A,B, 9, 9);
            function(B,C,D,A,13,13);
            function(A,B,C,D, 2, 3);
            function(D,A,B,C, 6, 5);
            function(C,D,A,B,10, 9);
            function(B,C,D,A,14,13);
            function(A,B,C,D, 3, 3);
            function(D,A,B,C, 7, 5);
            function(C,D,A,B,11, 9);
            function(B,C,D,A,15,13);

#undef function  
#define function(a,b,c,d,k,s) a=ROL(a+H(b,c,d)+X[k]+(uint32_t)0x6ed9eba1,s);     
            function(A,B,C,D, 0, 3);
            function(D,A,B,C, 8, 9);
            function(C,D,A,B, 4,11);
            function(B,C,D,A,12,15);
            function(A,B,C,D, 2, 3);
            function(D,A,B,C,10, 9);
            function(C,D,A,B, 6,11);
            function(B,C,D,A,14,15);
            function(A,B,C,D, 1, 3);
            function(D,A,B,C, 9, 9);
            function(C,D,A,B, 5,11);
            function(B,C,D,A,13,15);
            function(A,B,C,D, 3, 3);
            function(D,A,B,C,11, 9);
            function(C,D,A,B, 7,11);
            function(B,C,D,A,15,15);

            hs->A+=A; hs->B+=B; hs->C+=C; hs->D+=D;
        }
    }

    return 0;
}

EXPORT_SYM int md4_digest(const hash_state *hs, uint8_t digest[16])
{
    static uint8_t s[8];
    uint32_t padlen;
    hash_state temp;
    unsigned i;
    uint64_t bitlen;

    static const uint8_t padding[64] = {
        0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
    };

    if (NULL==hs || NULL==digest)
        return ERR_NULL;

    temp = *hs;

    bitlen = temp.bitlen;   /* Save current length */

    padlen= (56<=hs->count) ? 56-hs->count+64: 56-hs->count;
    md4_update(&temp, padding, padlen);

    for (i=0; i<8; i++)
        s[i] = (uint8_t)(bitlen >> (i*8));
    md4_update(&temp, s, 8);

    STORE_U32_LITTLE(&digest[0], temp.A);
    STORE_U32_LITTLE(&digest[4], temp.B);
    STORE_U32_LITTLE(&digest[8], temp.C);
    STORE_U32_LITTLE(&digest[12], temp.D);

    return 0;
}
