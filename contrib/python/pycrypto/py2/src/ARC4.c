
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

#define MODULE_NAME _ARC4
#define BLOCK_SIZE 1
#define KEY_SIZE 0

typedef struct 
{
	unsigned char state[256];
	unsigned char x,y;
} stream_state;

/* Encryption and decryption are symmetric */
#define stream_decrypt stream_encrypt	

static void stream_encrypt(stream_state *self, unsigned char *block, 
			   int len)
{
	register int i, x=self->x, y=self->y;

	for (i=0; i<len; i++) 
	{
		x = (x + 1) % 256;
		y = (y + self->state[x]) % 256;
		{
			register int t;		/* Exchange state[x] and state[y] */
			t = self->state[x];
			self->state[x] = self->state[y];
			self->state[y] = t;
		}
		{
			register int xorIndex;	/* XOR the data with the stream data */
			xorIndex=(self->state[x]+self->state[y]) % 256;
			block[i] ^= self->state[xorIndex];
		}
	}
	self->x=x;
	self->y=y;
}


static void stream_init(stream_state *self, unsigned char *key, int keylen)
{
	register int i, index1, index2;

	for(i=0; i<256; i++) self->state[i]=i;
	self->x=0; self->y=0;
	index1=0; index2=0;
	for(i=0; i<256; i++) 
	{
		register int t;
		index2 = ( key[index1] + self->state[i] + index2) % 256;
		t = self->state[i];
		self->state[i] = self->state[index2];
		self->state[index2] = t;
		index1 = (index1 + 1) % keylen;
	}
}
     
#include "stream_template.c"

  
