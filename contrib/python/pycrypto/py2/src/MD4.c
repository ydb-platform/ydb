
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
  

#include <string.h>
#include "Python.h"
#include "pycrypto_compat.h"

#define MODULE_NAME _MD4
#define DIGEST_SIZE 16
#define BLOCK_SIZE 64

typedef unsigned int U32;
typedef unsigned char U8;
#define U32_MAX (U32)4294967295

typedef struct {
	U32 A,B,C,D, count;
	U32 len1, len2;
	U8 buf[64];
} hash_state;

#define F(x, y, z) (((x) & (y)) | ((~x) & (z)))
#define G(x, y, z) (((x) & (y)) | ((x) & (z)) | ((y) & (z)))
#define H(x, y, z) ((x) ^ (y) ^ (z))

/* ROTATE_LEFT rotates x left n bits */
#define ROL(x, n) (((x) << n) | ((x) >> (32-n) ))

static void 
hash_init (hash_state *ptr)
{
	ptr->A=(U32)0x67452301;
	ptr->B=(U32)0xefcdab89;
	ptr->C=(U32)0x98badcfe;
	ptr->D=(U32)0x10325476;
	ptr->count=ptr->len1=ptr->len2=0;
}

static void
hash_copy(hash_state *src, hash_state *dest)
{
	dest->len1=src->len1;
	dest->len2=src->len2;
	dest->A=src->A;
	dest->B=src->B;
	dest->C=src->C;
	dest->D=src->D;
	dest->count=src->count;  
	memcpy(dest->buf, src->buf, dest->count);
}

static void 
hash_update (hash_state *self, const U8 *buf, U32 len)
{
	U32 L;

	if ((self->len1+(len<<3))<self->len1)
	{
		self->len2++;
	}
	self->len1+=len<< 3;
	self->len2+=len>>29;
	while (len>0) 
	{
		L=(64-self->count) < len ? (64-self->count) : len;
		memcpy(self->buf+self->count, buf, L);
		self->count+=L;
		buf+=L;
		len-=L;
		if (self->count==64) 
		{
			U32 X[16], A, B, C, D;
			int i,j;
			self->count=0;
			for(i=j=0; j<16; i+=4, j++) 
				X[j]=((U32)self->buf[i]       + ((U32)self->buf[i+1]<<8) +
				      ((U32)self->buf[i+2]<<16) + ((U32)self->buf[i+3]<<24));


			A=self->A; B=self->B; C=self->C; D=self->D;

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
#define function(a,b,c,d,k,s) a=ROL(a+G(b,c,d)+X[k]+(U32)0x5a827999,s);	 
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
#define function(a,b,c,d,k,s) a=ROL(a+H(b,c,d)+X[k]+(U32)0x6ed9eba1,s);	 
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

			self->A+=A; self->B+=B; self->C+=C; self->D+=D;
		}
	}
}

static PyObject *
hash_digest (const hash_state *self)
{
	U8 digest[16];
	static U8 s[8];
	U32 padlen, oldlen1, oldlen2;
	hash_state temp;
	static U8 padding[64] = {
		0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
	};

	memcpy(&temp, self, sizeof(hash_state));
	oldlen1=temp.len1; oldlen2=temp.len2;  /* Save current length */
	padlen= (56<=self->count) ? 56-self->count+64: 56-self->count;
	hash_update(&temp, padding, padlen);
	s[0]= oldlen1       & 255;
	s[1]=(oldlen1 >>  8) & 255;
	s[2]=(oldlen1 >> 16) & 255;
	s[3]=(oldlen1 >> 24) & 255;
	s[4]= oldlen2        & 255;
	s[5]=(oldlen2 >>  8) & 255;
	s[6]=(oldlen2 >> 16) & 255;
	s[7]=(oldlen2 >> 24) & 255;
	hash_update(&temp, s, 8);
  
	digest[ 0]= temp.A        & 255;
	digest[ 1]=(temp.A >>  8) & 255;
	digest[ 2]=(temp.A >> 16) & 255;
	digest[ 3]=(temp.A >> 24) & 255;
	digest[ 4]= temp.B        & 255;
	digest[ 5]=(temp.B >>  8) & 255;
	digest[ 6]=(temp.B >> 16) & 255;
	digest[ 7]=(temp.B >> 24) & 255;
	digest[ 8]= temp.C        & 255;
	digest[ 9]=(temp.C >>  8) & 255;
	digest[10]=(temp.C >> 16) & 255;
	digest[11]=(temp.C >> 24) & 255;
	digest[12]= temp.D        & 255;
	digest[13]=(temp.D >>  8) & 255;
	digest[14]=(temp.D >> 16) & 255;
	digest[15]=(temp.D >> 24) & 255;
  
	return PyBytes_FromStringAndSize((char *) digest, 16);
}

#include "hash_template.c"
