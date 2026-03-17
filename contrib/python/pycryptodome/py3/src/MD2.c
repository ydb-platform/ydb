/*
 *  md2.c : MD2 hash algorithm.
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

FAKE_INIT(MD2)

typedef struct {
    uint8_t C[16];  /** checksum **/
    uint8_t X[48];
    unsigned count;
    uint8_t buf[16];
} hash_state;

EXPORT_SYM int md2_init(hash_state **md2State)
{
    hash_state *hs;
    
    if (NULL == md2State) {
        return ERR_NULL;
    }

    *md2State = hs = (hash_state*) calloc(1, sizeof(hash_state));
    if (NULL == hs)
        return ERR_MEMORY;

    return 0;
}

EXPORT_SYM int md2_destroy(hash_state *hs)
{
    free(hs);
    return 0;
}

static const uint8_t S[256] = {
    41, 46, 67, 201, 162, 216, 124, 1, 61, 54, 84, 161, 236, 240, 6,
    19, 98, 167, 5, 243, 192, 199, 115, 140, 152, 147, 43, 217, 188,
    76, 130, 202, 30, 155, 87, 60, 253, 212, 224, 22, 103, 66, 111, 24,
    138, 23, 229, 18, 190, 78, 196, 214, 218, 158, 222, 73, 160, 251,
    245, 142, 187, 47, 238, 122, 169, 104, 121, 145, 21, 178, 7, 63,
    148, 194, 16, 137, 11, 34, 95, 33, 128, 127, 93, 154, 90, 144, 50,
    39, 53, 62, 204, 231, 191, 247, 151, 3, 255, 25, 48, 179, 72, 165,
    181, 209, 215, 94, 146, 42, 172, 86, 170, 198, 79, 184, 56, 210,
    150, 164, 125, 182, 118, 252, 107, 226, 156, 116, 4, 241, 69, 157,
    112, 89, 100, 113, 135, 32, 134, 91, 207, 101, 230, 45, 168, 2, 27,
    96, 37, 173, 174, 176, 185, 246, 28, 70, 97, 105, 52, 64, 126, 15,
    85, 71, 163, 35, 221, 81, 175, 58, 195, 92, 249, 206, 186, 197,
    234, 38, 44, 83, 13, 110, 133, 40, 132, 9, 211, 223, 205, 244, 65,
    129, 77, 82, 106, 220, 55, 200, 108, 193, 171, 250, 36, 225, 123,
    8, 12, 189, 177, 74, 120, 136, 149, 139, 227, 99, 232, 109, 233,
    203, 213, 254, 59, 0, 29, 57, 242, 239, 183, 14, 102, 88, 208, 228,
    166, 119, 114, 248, 235, 117, 75, 10, 49, 68, 80, 180, 143, 237,
    31, 26, 219, 153, 141, 51, 159, 17, 131, 20
};

EXPORT_SYM int md2_copy(const hash_state *src, hash_state *dst)
{
    if (NULL == src || NULL == dst) {
        return ERR_NULL;
    }

    *dst = *src;
    return 0;
}

EXPORT_SYM int md2_update(hash_state *hs, const uint8_t *buf, size_t len)
{
    if (NULL == hs || NULL == buf)
        return ERR_NULL;

    while (len) {
        unsigned left, tc;

        left = 16 - hs->count;   /** 1..16 **/
        tc = (unsigned)MIN(left, len);
        memcpy(hs->buf+hs->count, buf, tc);
        hs->count += tc;
        buf += tc;
        len -= tc;
        
        if (hs->count==16) {
            uint8_t L, t;
            unsigned j;
      
            hs->count = 0;

            L = hs->C[15];
            for(j=0; j<16; j++) {
                hs->X[16+j] = hs->buf[j];
                hs->X[32+j] = hs->X[16+j] ^ hs->X[j];

                /** Update checksum **/
                hs->C[j] ^= S[hs->buf[j] ^ L];
                L = hs->C[j];
            }
      
            t = 0;
            for(j=0; j<18; j++) {
                unsigned k;
                for(k=0; k<48; k++) {
                    hs->X[k] ^= S[t];
                    t = hs->X[k];
                }
                t = (t+j) & 0xFF;
            }
        }
    }
    return 0;
}

EXPORT_SYM int md2_digest(const hash_state *hs, uint8_t digest[16])
{
    uint8_t padding[16];
    unsigned padlen, i;
    hash_state temp;
 
    if (NULL==hs || digest==NULL)
        return ERR_NULL;

    assert(hs->count < 16);

    temp = *hs;
    padlen = 16 - hs->count;  /** 1..16 **/
    for(i=0; i<padlen; i++) { 
        padding[i] = (uint8_t)padlen;
    }
    md2_update(&temp, padding, padlen);
    md2_update(&temp, temp.C, 16);
    memcpy(digest, temp.X, 16);
    return 0;
}
