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

#include "Python.h"

#define MODULE_NAME _CAST
#define BLOCK_SIZE 8
#define KEY_SIZE 0

/* adjust these according to your compiler/platform. On some machines
   uint32 will have to be a long. It's OK if uint32 is more than 32 bits. */
typedef unsigned int uint32;
typedef unsigned char uint8;

/* this struct probably belongs in cast.h */
typedef struct {
	/* masking and rotate keys */
	uint32 Km[16];
	uint8 Kr[16];
	/* number of rounds (depends on original unpadded keylength) */
	int rounds;
} block_state;

/* these are the eight 32*256 S-boxes */
#include "cast5.c"

/* fetch a uint32 from an array of uint8s (with a given offset) */
#define fetch(ptr, base)   (((((( ptr[base]<< 8 ) | ptr[base+1] )<< 8 ) | ptr[base+2] )<< 8 ) | ptr[base+3])

/* this is the round function f(D, Km, Kr) */
static uint32 castfunc(uint32 D, uint32 Kmi, uint8 Kri, int type)
{
	uint32 I, f;
	short Ia, Ib, Ic, Id;
    
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
    
	I &= 0xFFFFFFFF;
	I = ( I << Kri ) | ( I >> ( 32-Kri ) );
	Ia = ( I >> 24 ) & 0xFF;
	Ib = ( I >> 16 ) & 0xFF;
	Ic = ( I >>  8 ) & 0xFF;
	Id = ( I       ) & 0xFF;
    
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
static void castcrypt(block_state *key, uint8 *block, int decrypt)
{
	uint32 L, R, tmp, f;
	uint32 Kmi;
	uint8  Kri;
	short functype, round;
    
	L = fetch(block, 0);
	R = fetch(block, 4);
    
/*  printf("L0 = %08x R0 = %08x\n", L, R); */

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

/*	printf("L%d = %08x R%d = %08x\n", round+1, L, round+1, R); */
	}
    
	block[0] = ( R & 0xFF000000 ) >> 24;
	block[1] = ( R & 0x00FF0000 ) >> 16;
	block[2] = ( R & 0x0000FF00 ) >> 8;
	block[3] = ( R & 0x000000FF );
	block[4] = ( L & 0xFF000000 ) >> 24;
	block[5] = ( L & 0x00FF0000 ) >> 16;
	block[6] = ( L & 0x0000FF00 ) >> 8;
	block[7] = ( L & 0x000000FF );
}

/* fetch a uint8 from an array of uint32s */
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
static void schedulekeys_half(uint32 *in, uint32 *keys)
{
	uint32 x[4], z[4];
    
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
static void castschedulekeys(block_state *schedule, uint8 *key, int keybytes)
{
	uint32 x[4];
	uint8  paddedkey[16];
	uint32 Kr_wide[16];
	int i;
    
	for(i = 0; i < keybytes; i++)
		paddedkey[i] = key[i];
	for(     ; i < 16      ; i++)
		paddedkey[i] = 0;
    
	if (keybytes <= 10)
		schedule->rounds = 12;
	else
		schedule->rounds = 16;
    
	x[0] = fetch(paddedkey, 0);
	x[1] = fetch(paddedkey, 4);
	x[2] = fetch(paddedkey, 8);
	x[3] = fetch(paddedkey, 12);
    
	schedulekeys_half(x, schedule->Km);
	schedulekeys_half(x, Kr_wide);
    
	for(i = 0; i < 16; i ++) {
		/* The Kr[] subkeys are used for 32-bit circular shifts,
		   so we only need to keep them modulo 32 */
		schedule->Kr[i] = (uint8)(Kr_wide[i] & 0x1F);
	}
}

#ifdef TEST

/* This performs a variety of encryptions and verifies that the results
   match those specified in RFC2144 appendix B. Also verifies that
   decryption restores the original data. */

#include <stdio.h>

static block_state sched;

void encrypt(key, keylen, in, out)
	uint8 *key;
	int keylen;
	uint8 *in, *out;
{
	int i;
	uint8 k[16];
    
	castschedulekeys(&sched, key, keylen);
    
	for(i = 0; i < 8; i++)
		out[i] = in[i];
	castcrypt(&sched, out, 0);
}

void tst(key, keylen, data, result)
	uint8 *key;
	int keylen;
	uint8 *data, *result;
{
	uint8 d[8];
	int i;
    
	encrypt(key, keylen, data, d);
    
	for(i = 0; i < 8; i++)
		if (d[i] != result[i])
			break;
    
	if (i == 8) {
		printf("-- test ok (encrypt)\n");
	} else {
		for(i = 0; i < 8; i++)
			printf(" %02x", d[i]);
		printf("   (computed)\n");
		for(i = 0; i < 8; i++)
			printf(" %02x", result[i]);
		printf("   (expected)\n");
	}
    
	/* uses key schedule already set up */
	castcrypt(&sched, d, 1);
	if (bcmp(d, data, 8))
		printf("   test FAILED (decrypt)\n");
	else
		printf("   test ok (decrypt)\n");
    
}

uint8 key[16] = { 0x01, 0x23, 0x45, 0x67, 0x12, 0x34, 0x56, 0x78,
		  0x23, 0x45, 0x67, 0x89, 0x34, 0x56, 0x78, 0x9A };
uint8 data[8] = { 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF };

/* expected results of encrypting the above with 128, 80, and 40
   bits of key length */
uint8 out1[8] =  { 0x23, 0x8B, 0x4F, 0xE5, 0x84, 0x7E, 0x44, 0xB2 };
uint8 out2[8] =  { 0xEB, 0x6A, 0x71, 0x1A, 0x2C, 0x02, 0x27, 0x1B };
uint8 out3[8] =  { 0x7A, 0xC8, 0x16, 0xD1, 0x6E, 0x9B, 0x30, 0x2E };

/* expected results of the "full maintenance test" */
uint8 afinal[16] = { 0xEE, 0xA9, 0xD0, 0xA2, 0x49, 0xFD, 0x3B, 0xA6,
		     0xB3, 0x43, 0x6F, 0xB8, 0x9D, 0x6D, 0xCA, 0x92 };
uint8 bfinal[16] = { 0xB2, 0xC9, 0x5E, 0xB0, 0x0C, 0x31, 0xAD, 0x71,
		     0x80, 0xAC, 0x05, 0xB8, 0xE8, 0x3D, 0x69, 0x6E };

main()
{
	/* Appendix B.1 : Single Plaintext-Key-Ciphertext Sets */
	tst(key, 16, data, out1);
	tst(key, 10, data, out2);
	tst(key,  5, data, out3);

    /* Appendix B.2 : Full Maintenance Test */
	{
		uint8 abuf[16];
		uint8 bbuf[16];
		int i;

		bcopy(key, abuf, 16);
		bcopy(key, bbuf, 16);

		printf("\nrunning full maintenance test...\n");

		for(i = 0; i < 1000000; i++) {
			castschedulekeys(&sched, bbuf, 16);
			castcrypt(&sched, abuf, 0);
			castcrypt(&sched, abuf+8, 0);

			castschedulekeys(&sched, abuf, 16);
			castcrypt(&sched, bbuf, 0);
			castcrypt(&sched, bbuf+8, 0);

			if (!(i % 10000)) {
				fprintf(stdout, "\r%d%%   ", i / 10000);
				fflush(stdout);
			}
		}

		printf("\r        \r");

		for(i = 0; i < 16; i ++)
			if (abuf[i] != afinal[i] || bbuf[i] != bfinal[i])
				break;

		if(i == 16) {
			printf("-- full maintenance test ok\n");
		} else {
			for(i = 0; i < 16; i++)
				printf(" %02x", abuf[i]);
			printf("\n");
			for(i = 0; i < 16; i++)
				printf(" %02x", bbuf[i]);
			printf("\n");
		}

		printf("running maintenance test in reverse...\n");
		for(i = 0; i < 1000000; i++) {
			castschedulekeys(&sched, abuf, 16);
			castcrypt(&sched, bbuf+8, 1);
			castcrypt(&sched, bbuf, 1);

			castschedulekeys(&sched, bbuf, 16);
			castcrypt(&sched, abuf+8, 1);
			castcrypt(&sched, abuf, 1);

			if (!(i % 10000)) {
				fprintf(stdout, "\r%d%%   ", i / 10000);
				fflush(stdout);
			}
		}

		printf("\r       \r");
		if (bcmp(abuf, key, 16) || bcmp(bbuf, key, 16)) 
			printf("-- reverse maintenance test FAILED\n");
		else
			printf("-- reverse maintenance test ok\n");
	}
}

#endif

static void
block_init(block_state *self, unsigned char *key, int keylength)
{
	/* presumably this will optimize out */
	if (sizeof(uint32) < 4 || sizeof(uint8) != 1) {
		PyErr_SetString(PyExc_SystemError,
				"CAST module compiled with bad typedefs!");
	}

	/* make sure the key length is within bounds */
	if (keylength < 5 || keylength > 16) {
		PyErr_SetString(PyExc_ValueError, "CAST key must be "
				"at least 5 bytes and no more than 16 bytes long");
		return;
	}

	/* do the actual key schedule setup */
	castschedulekeys(self, key, keylength);
}

static void
block_encrypt(block_state *self, unsigned char *in,
	      unsigned char *out)
{
	memcpy(out, in, 8);
	castcrypt(self, out, 0);
}

static void block_decrypt(block_state *self, 
			  unsigned char *in,
			  unsigned char *out)
{
	memcpy(out, in, 8);
	castcrypt(self, out, 1);
}

#include "block_template.c"
