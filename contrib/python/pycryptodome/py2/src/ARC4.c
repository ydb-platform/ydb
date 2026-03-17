/*
 *  arc4.c : Implementation for the Alleged-RC4 stream cipher
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

FAKE_INIT(ARC4)

typedef struct
{
    uint8_t state[256];
    uint8_t x,y;
} stream_state;

EXPORT_SYM int ARC4_stream_encrypt(stream_state *rc4State, const uint8_t in[], uint8_t out[], size_t len)
{
    size_t i;
    unsigned x=rc4State->x, y=rc4State->y;

    for (i=0; i<len; i++)
    {
        x = (x + 1) % 256;
        y = (y + rc4State->state[x]) % 256;
        {
            unsigned t;      /* Exchange state[x] and state[y] */
            t = rc4State->state[x];
            rc4State->state[x] = rc4State->state[y];
            rc4State->state[y] = (uint8_t)t;
        }
        {
            unsigned xorIndex;   /* XOR the data with the stream data */
            xorIndex=(rc4State->state[x]+rc4State->state[y]) % 256;
            out[i] = in[i] ^ rc4State->state[xorIndex];
        }
    }
    rc4State->x=(uint8_t)x;
    rc4State->y=(uint8_t)y;
    return 0;
}

EXPORT_SYM int ARC4_stream_init(uint8_t *key, size_t keylen, stream_state **pRc4State)
{
    unsigned i;
    unsigned index1, index2;
    stream_state *rc4State;

    if (NULL == pRc4State || NULL == key)
        return ERR_NULL;

    *pRc4State = rc4State = calloc(1, sizeof(stream_state));
    if (NULL == rc4State)
        return ERR_MEMORY;

    for(i=0; i<256; i++)
        rc4State->state[i]=(uint8_t)i;

    rc4State->x=0;
    rc4State->y=0;

    index1=0;
    index2=0;
    for(i=0; i<256; i++)
    {
        unsigned t;
        index2 = ( (unsigned)key[index1] + rc4State->state[i] + index2) % 256;
        t = rc4State->state[i];
        rc4State->state[i] = rc4State->state[index2];
        rc4State->state[index2] = (uint8_t)t;
        index1 = (index1 + 1) % (unsigned)keylen;
    }
    return 0;
}

EXPORT_SYM int ARC4_stream_destroy(stream_state *rc4State)
{
    free(rc4State);
    return 0;
}
