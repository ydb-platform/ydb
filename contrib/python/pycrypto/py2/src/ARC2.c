/*
 *  rc2.c : Source code for the RC2 block cipher
 *
 * Part of the Python Cryptography Toolkit
 *
 * ===================================================================
 * This file appears to contain code from the ARC2 implementation
 * "rc2.c" implementation (the "Original Code"), with modifications made
 * after it was incorporated into PyCrypto (the "Modifications").
 *
 * To the best of our knowledge, the Original Code was placed into the
 * public domain by its (anonymous) author:
 *
 *  **********************************************************************
 * * To commemorate the 1996 RSA Data Security Conference, the following  *
 * * code is released into the public domain by its author.  Prost!       *
 * *                                                                      *
 * * This cipher uses 16-bit words and little-endian byte ordering.       *
 * * I wonder which processor it was optimized for?                       *
 * *                                                                      *
 * * Thanks to CodeView, SoftIce, and D86 for helping bring this code to  *
 * * the public.                                                          *
 *  **********************************************************************
 *
 * The Modifications to this file are dedicated to the public domain.
 * To the extent that dedication to the public domain is not available,
 * everyone is granted a worldwide, perpetual, royalty-free,
 * non-exclusive license to exercise all rights associated with the
 * contents of this file for any purpose whatsoever.  No rights are
 * reserved.
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

#include <string.h>  
#include "Python.h"

#define MODULE_NAME _ARC2
#define BLOCK_SIZE 8
#define KEY_SIZE 0
#define PCT_ARC2_MODULE  /* Defined to get ARC2's additional keyword arguments */

typedef unsigned int U32;
typedef unsigned short U16;
typedef unsigned char U8;

typedef struct 
{
	U16 xkey[64];
        int effective_keylen;
} block_state;

static void
block_encrypt(block_state *self, U8 *in, U8 *out)
{
	U16 x76, x54, x32, x10;
	int i;
  
	x76 = (in[7] << 8) + in[6];
	x54 = (in[5] << 8) + in[4];
	x32 = (in[3] << 8) + in[2];
	x10 = (in[1] << 8) + in[0];
  
	for (i = 0; i < 16; i++)
	{
		x10 += (x32 & ~x76) + (x54 & x76) + self->xkey[4*i+0];
		x10 = (x10 << 1) + (x10 >> 15 & 1);
      
		x32 += (x54 & ~x10) + (x76 & x10) + self->xkey[4*i+1];
		x32 = (x32 << 2) + (x32 >> 14 & 3);
      
		x54 += (x76 & ~x32) + (x10 & x32) + self->xkey[4*i+2];
		x54 = (x54 << 3) + (x54 >> 13 & 7);
      
		x76 += (x10 & ~x54) + (x32 & x54) + self->xkey[4*i+3];
		x76 = (x76 << 5) + (x76 >> 11 & 31);
      
		if (i == 4 || i == 10) {
			x10 += self->xkey[x76 & 63];
			x32 += self->xkey[x10 & 63];
			x54 += self->xkey[x32 & 63];
			x76 += self->xkey[x54 & 63];
		}
	}
  
	out[0] = (U8)x10;
	out[1] = (U8)(x10 >> 8);
	out[2] = (U8)x32;
	out[3] = (U8)(x32 >> 8);
	out[4] = (U8)x54;
	out[5] = (U8)(x54 >> 8);
	out[6] = (U8)x76;
	out[7] = (U8)(x76 >> 8);
}


static void
block_decrypt(block_state *self, U8 *in, U8 *out)
{
	U16 x76, x54, x32, x10;
	int i;
  
	x76 = (in[7] << 8) + in[6];
	x54 = (in[5] << 8) + in[4];
	x32 = (in[3] << 8) + in[2];
	x10 = (in[1] << 8) + in[0];
  
	i = 15;
	do {
		x76 &= 65535;
		x76 = (x76 << 11) + (x76 >> 5);
		x76 -= (x10 & ~x54) + (x32 & x54) + self->xkey[4*i+3];
    
		x54 &= 65535;
		x54 = (x54 << 13) + (x54 >> 3);
		x54 -= (x76 & ~x32) + (x10 & x32) + self->xkey[4*i+2];
    
		x32 &= 65535;
		x32 = (x32 << 14) + (x32 >> 2);
		x32 -= (x54 & ~x10) + (x76 & x10) + self->xkey[4*i+1];
    
		x10 &= 65535;
		x10 = (x10 << 15) + (x10 >> 1);
		x10 -= (x32 & ~x76) + (x54 & x76) + self->xkey[4*i+0];
    
		if (i == 5 || i == 11) {
			x76 -= self->xkey[x54 & 63];
			x54 -= self->xkey[x32 & 63];
			x32 -= self->xkey[x10 & 63];
			x10 -= self->xkey[x76 & 63];
		}
	} while (i--);
  
	out[0] = (U8)x10;
	out[1] = (U8)(x10 >> 8);
	out[2] = (U8)x32;
	out[3] = (U8)(x32 >> 8);
	out[4] = (U8)x54;
	out[5] = (U8)(x54 >> 8);
	out[6] = (U8)x76;
	out[7] = (U8)(x76 >> 8);
}


static void 
block_init(block_state *self, U8 *key, int keylength)
{
	U8 x;
	U16 i;
        /* 256-entry permutation table, probably derived somehow from pi */
        static const U8 permute[256] = {
		217,120,249,196, 25,221,181,237, 40,233,253,121, 74,160,216,157,
		198,126, 55,131, 43,118, 83,142, 98, 76,100,136, 68,139,251,162,
		23,154, 89,245,135,179, 79, 19, 97, 69,109,141,  9,129,125, 50,
		189,143, 64,235,134,183,123, 11,240,149, 33, 34, 92,107, 78,130,
		84,214,101,147,206, 96,178, 28,115, 86,192, 20,167,140,241,220,
		18,117,202, 31, 59,190,228,209, 66, 61,212, 48,163, 60,182, 38,
		111,191, 14,218, 70,105,  7, 87, 39,242, 29,155,188,148, 67,  3,
		248, 17,199,246,144,239, 62,231,  6,195,213, 47,200,102, 30,215,
		8,232,234,222,128, 82,238,247,132,170,114,172, 53, 77,106, 42,
		150, 26,210,113, 90, 21, 73,116, 75,159,208, 94,  4, 24,164,236,
		194,224, 65,110, 15, 81,203,204, 36,145,175, 80,161,244,112, 57,
		153,124, 58,133, 35,184,180,122,252,  2, 54, 91, 37, 85,151, 49,
		45, 93,250,152,227,138,146,174,  5,223, 41, 16,103,108,186,201,
		211,  0,230,207,225,158,168, 44, 99, 22,  1, 63, 88,226,137,169,
		13, 56, 52, 27,171, 51,255,176,187, 72, 12, 95,185,177,205, 46,
		197,243,219, 71,229,165,156,119, 10,166, 32,104,254,127,193,173
        };

	if ((U32)keylength > sizeof(self->xkey)) {
		PyErr_SetString(PyExc_ValueError,
				"ARC2 key length must be less than 128 bytes");
		return;
	}

	memcpy(self->xkey, key, keylength);
  
	/* Phase 1: Expand input key to 128 bytes */
	if (keylength < 128) {
		i = 0;
		x = ((U8 *)self->xkey)[keylength-1];
                do {
			x = permute[(x + ((U8 *)self->xkey)[i++]) & 255];
                        ((U8 *)self->xkey)[keylength++] = x;
                } while (keylength < 128);
	}
  
	/* Phase 2 - reduce effective key size to "effective_keylen" */
        keylength = (self->effective_keylen+7) >> 3;
	i = 128-keylength;
	x = permute[((U8 *)self->xkey)[i] & (255 >>
					     (7 &
					      ((self->effective_keylen %8 ) ? 8-(self->effective_keylen%8): 0))
		)];
	((U8 *)self->xkey)[i] = x;
  
	while (i--) {
		x = permute[ x ^ ((U8 *)self->xkey)[i+keylength] ];
		((U8 *)self->xkey)[i] = x;
        }
  
	/* Phase 3 - copy to self->xkey in little-endian order */
	i = 63;
	do {
		self->xkey[i] =  ((U8 *)self->xkey)[2*i] +
			(((U8 *)self->xkey)[2*i+1] << 8);
	} while (i--);
}


#include "block_template.c"
