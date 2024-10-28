/**********************************************************************
  Copyright(c) 2011-2015 Intel Corporation All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Intel Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>		// for memset, memcmp
#include <assert.h>
#include "erasure_code.h"
#include "test.h"

#ifndef ALIGN_SIZE
# define ALIGN_SIZE 16
#endif

//By default, test multibinary version
#ifndef FUNCTION_UNDER_TEST
# define FUNCTION_UNDER_TEST ec_encode_data_update
# define REF_FUNCTION ec_encode_data
#endif

#define TEST_LEN 8192
#define TEST_SIZE (TEST_LEN/2)

#ifndef TEST_SOURCES
# define TEST_SOURCES  127
#endif
#ifndef RANDOMS
# define RANDOMS 200
#endif

#define MMAX TEST_SOURCES
#define KMAX TEST_SOURCES

#define EFENCE_TEST_MAX_SIZE 0x100

#ifdef EC_ALIGNED_ADDR
// Define power of 2 range to check ptr, len alignment
# define PTR_ALIGN_CHK_B 0
# define LEN_ALIGN_CHK_B 0	// 0 for aligned only
#else
// Define power of 2 range to check ptr, len alignment
# define PTR_ALIGN_CHK_B ALIGN_SIZE
# define LEN_ALIGN_CHK_B ALIGN_SIZE	// 0 for aligned only
#endif

#ifndef TEST_SEED
#define TEST_SEED 11
#endif

#define str(s) #s
#define xstr(s) str(s)

typedef unsigned char u8;

void dump(unsigned char *buf, int len)
{
	int i;
	for (i = 0; i < len;) {
		printf(" %2x", 0xff & buf[i++]);
		if (i % 32 == 0)
			printf("\n");
	}
	printf("\n");
}

void dump_matrix(unsigned char **s, int k, int m)
{
	int i, j;
	for (i = 0; i < k; i++) {
		for (j = 0; j < m; j++) {
			printf(" %2x", s[i][j]);
		}
		printf("\n");
	}
	printf("\n");
}

void dump_u8xu8(unsigned char *s, int k, int m)
{
	int i, j;
	for (i = 0; i < k; i++) {
		for (j = 0; j < m; j++) {
			printf(" %2x", 0xff & s[j + (i * m)]);
		}
		printf("\n");
	}
	printf("\n");
}

// Generate Random errors
static void gen_err_list(unsigned char *src_err_list,
			 unsigned char *src_in_err, int *pnerrs, int *pnsrcerrs, int k, int m)
{
	int i, err;
	int nerrs = 0, nsrcerrs = 0;

	for (i = 0, nerrs = 0, nsrcerrs = 0; i < m && nerrs < m - k; i++) {
		err = 1 & rand();
		src_in_err[i] = err;
		if (err) {
			src_err_list[nerrs++] = i;
			if (i < k) {
				nsrcerrs++;
			}
		}
	}
	if (nerrs == 0) {	// should have at least one error
		while ((err = (rand() % KMAX)) >= m) ;
		src_err_list[nerrs++] = err;
		src_in_err[err] = 1;
		if (err < k)
			nsrcerrs = 1;
	}
	*pnerrs = nerrs;
	*pnsrcerrs = nsrcerrs;
	return;
}

#define NO_INVERT_MATRIX -2
// Generate decode matrix from encode matrix
static int gf_gen_decode_matrix(unsigned char *encode_matrix,
				unsigned char *decode_matrix,
				unsigned char *invert_matrix,
				unsigned int *decode_index,
				unsigned char *src_err_list,
				unsigned char *src_in_err,
				int nerrs, int nsrcerrs, int k, int m)
{
	int i, j, p;
	int r;
	unsigned char *backup, *b, s;
	int incr = 0;

	b = malloc(MMAX * KMAX);
	backup = malloc(MMAX * KMAX);

	if (b == NULL || backup == NULL) {
		printf("Test failure! Error with malloc\n");
		free(b);
		free(backup);
		return -1;
	}
	// Construct matrix b by removing error rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (src_in_err[r])
			r++;
		for (j = 0; j < k; j++) {
			b[k * i + j] = encode_matrix[k * r + j];
			backup[k * i + j] = encode_matrix[k * r + j];
		}
		decode_index[i] = r;
	}
	incr = 0;
	while (gf_invert_matrix(b, invert_matrix, k) < 0) {
		if (nerrs == (m - k)) {
			free(b);
			free(backup);
			printf("BAD MATRIX\n");
			return NO_INVERT_MATRIX;
		}
		incr++;
		memcpy(b, backup, MMAX * KMAX);
		for (i = nsrcerrs; i < nerrs - nsrcerrs; i++) {
			if (src_err_list[i] == (decode_index[k - 1] + incr)) {
				// skip the erased parity line
				incr++;
				continue;
			}
		}
		if (decode_index[k - 1] + incr >= m) {
			free(b);
			free(backup);
			printf("BAD MATRIX\n");
			return NO_INVERT_MATRIX;
		}
		decode_index[k - 1] += incr;
		for (j = 0; j < k; j++)
			b[k * (k - 1) + j] = encode_matrix[k * decode_index[k - 1] + j];

	};

	for (i = 0; i < nsrcerrs; i++) {
		for (j = 0; j < k; j++) {
			decode_matrix[k * i + j] = invert_matrix[k * src_err_list[i] + j];
		}
	}
	/* src_err_list from encode_matrix * invert of b for parity decoding */
	for (p = nsrcerrs; p < nerrs; p++) {
		for (i = 0; i < k; i++) {
			s = 0;
			for (j = 0; j < k; j++)
				s ^= gf_mul_erasure(invert_matrix[j * k + i],
					    encode_matrix[k * src_err_list[p] + j]);

			decode_matrix[k * p + i] = s;
		}
	}
	free(b);
	free(backup);
	return 0;
}

int main(int argc, char *argv[])
{
	int re = -1;
	int i, j, p, rtest, m, k;
	int nerrs, nsrcerrs;
	void *buf;
	unsigned int decode_index[MMAX];
	unsigned char *temp_buffs[TEST_SOURCES] = { NULL }, *buffs[TEST_SOURCES] = { NULL };
	unsigned char *update_buffs[TEST_SOURCES] = { NULL };
	unsigned char *encode_matrix = NULL, *decode_matrix = NULL, *invert_matrix =
	    NULL, *g_tbls = NULL;
	unsigned char src_in_err[TEST_SOURCES], src_err_list[TEST_SOURCES];
	unsigned char *recov[TEST_SOURCES];

	int rows, align, size;
	unsigned char *efence_buffs[TEST_SOURCES];
	unsigned char *efence_update_buffs[TEST_SOURCES];
	unsigned int offset;
	u8 *ubuffs[TEST_SOURCES];
	u8 *update_ubuffs[TEST_SOURCES];
	u8 *temp_ubuffs[TEST_SOURCES];

	printf("test " xstr(FUNCTION_UNDER_TEST) ": %dx%d ", TEST_SOURCES, TEST_LEN);
	srand(TEST_SEED);

	// Allocate the arrays
	for (i = 0; i < TEST_SOURCES; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN)) {
			printf("alloc error: Fail");
			goto exit;
		}
		buffs[i] = buf;
	}

	for (i = 0; i < TEST_SOURCES; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN)) {
			printf("alloc error: Fail");
			goto exit;
		}
		temp_buffs[i] = buf;
		memset(temp_buffs[i], 0, TEST_LEN);	// initialize the destination buffer to be zero for update function
	}

	for (i = 0; i < TEST_SOURCES; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN)) {
			printf("alloc error: Fail");
			goto exit;
		}
		update_buffs[i] = buf;
		memset(update_buffs[i], 0, TEST_LEN);	// initialize the destination buffer to be zero for update function
	}
	// Test erasure code by encode and recovery

	encode_matrix = malloc(MMAX * KMAX);
	decode_matrix = malloc(MMAX * KMAX);
	invert_matrix = malloc(MMAX * KMAX);
	g_tbls = malloc(KMAX * TEST_SOURCES * 32);
	if (encode_matrix == NULL || decode_matrix == NULL
	    || invert_matrix == NULL || g_tbls == NULL) {
		printf("Test failure! Error with malloc\n");
		goto exit;
	}
	// Pick a first test
	m = 14;
	k = 10;
	assert(!(m > MMAX || k > KMAX));

	// Make random data
	for (i = 0; i < k; i++) {
		for (j = 0; j < TEST_LEN; j++) {
			buffs[i][j] = rand();
			update_buffs[i][j] = buffs[i][j];
		}
	}

	// Generate encode matrix encode_matrix
	// The matrix generated by gf_gen_rs_matrix
	// is not always invertable.
	gf_gen_rs_matrix(encode_matrix, m, k);

	// Generate g_tbls from encode matrix encode_matrix
	ec_init_tables(k, m - k, &encode_matrix[k * k], g_tbls);

	// Perform matrix dot_prod for EC encoding
	// using g_tbls from encode matrix encode_matrix
	REF_FUNCTION(TEST_LEN, k, m - k, g_tbls, buffs, &buffs[k]);
	for (i = 0; i < k; i++) {
		FUNCTION_UNDER_TEST(TEST_LEN, k, m - k, i, g_tbls, update_buffs[i],
				    &update_buffs[k]);
	}
	for (i = 0; i < m - k; i++) {
		if (0 != memcmp(update_buffs[k + i], buffs[k + i], TEST_LEN)) {
			printf("\nupdate_buffs%d  :", i);
			dump(update_buffs[k + i], 25);
			printf("buffs%d         :", i);
			dump(buffs[k + i], 25);
			goto exit;
		}
	}

	// Choose random buffers to be in erasure
	memset(src_in_err, 0, TEST_SOURCES);
	gen_err_list(src_err_list, src_in_err, &nerrs, &nsrcerrs, k, m);

	// Generate decode matrix
	re = gf_gen_decode_matrix(encode_matrix, decode_matrix,
				  invert_matrix, decode_index, src_err_list, src_in_err,
				  nerrs, nsrcerrs, k, m);
	if (re != 0) {
		printf("Fail to gf_gen_decode_matrix\n");
		goto exit;
	}
	// Pack recovery array as list of valid sources
	// Its order must be the same as the order
	// to generate matrix b in gf_gen_decode_matrix
	for (i = 0; i < k; i++) {
		recov[i] = update_buffs[decode_index[i]];
	}

	// Recover data
	ec_init_tables(k, nerrs, decode_matrix, g_tbls);
	REF_FUNCTION(TEST_LEN, k, nerrs, g_tbls, recov, &temp_buffs[k]);
	for (i = 0; i < nerrs; i++) {

		if (0 != memcmp(temp_buffs[k + i], update_buffs[src_err_list[i]], TEST_LEN)) {
			printf("Fail error recovery (%d, %d, %d)\n", m, k, nerrs);
			printf(" - erase list = ");
			for (j = 0; j < nerrs; j++)
				printf(" %d", src_err_list[j]);
			printf(" - Index = ");
			for (p = 0; p < k; p++)
				printf(" %d", decode_index[p]);
			printf("\nencode_matrix:\n");
			dump_u8xu8((u8 *) encode_matrix, m, k);
			printf("inv b:\n");
			dump_u8xu8((u8 *) invert_matrix, k, k);
			printf("\ndecode_matrix:\n");
			dump_u8xu8((u8 *) decode_matrix, m, k);
			printf("recov %d:", src_err_list[i]);
			dump(temp_buffs[k + i], 25);
			printf("orig   :");
			dump(update_buffs[src_err_list[i]], 25);
			re = -1;
			goto exit;
		}
	}
#ifdef TEST_VERBOSE
	putchar('.');
#endif

	// Pick a first test
	m = 7;
	k = 5;
	if (m > MMAX || k > KMAX) {
		re = -1;
		goto exit;
	}

	// Zero the destination buffer for update function
	for (i = k; i < TEST_SOURCES; i++) {
		memset(buffs[i], 0, TEST_LEN);
		memset(update_buffs[i], 0, TEST_LEN);
	}
	// Make random data
	for (i = 0; i < k; i++) {
		for (j = 0; j < TEST_LEN; j++) {
			buffs[i][j] = rand();
			update_buffs[i][j] = buffs[i][j];
		}
	}

	// The matrix generated by gf_gen_cauchy1_matrix
	// is always invertable.
	gf_gen_cauchy1_matrix(encode_matrix, m, k);

	// Generate g_tbls from encode matrix encode_matrix
	ec_init_tables(k, m - k, &encode_matrix[k * k], g_tbls);

	// Perform matrix dot_prod for EC encoding
	// using g_tbls from encode matrix encode_matrix
	REF_FUNCTION(TEST_LEN, k, m - k, g_tbls, buffs, &buffs[k]);
	for (i = 0; i < k; i++) {
		FUNCTION_UNDER_TEST(TEST_LEN, k, m - k, i, g_tbls, update_buffs[i],
				    &update_buffs[k]);
	}
	for (i = 0; i < m - k; i++) {
		if (0 != memcmp(update_buffs[k + i], buffs[k + i], TEST_LEN)) {
			printf("\nupdate_buffs%d  :", i);
			dump(update_buffs[k + i], 25);
			printf("buffs%d         :", i);
			dump(buffs[k + i], 25);
			re = -1;
			goto exit;
		}
	}

	// Choose random buffers to be in erasure
	memset(src_in_err, 0, TEST_SOURCES);
	gen_err_list(src_err_list, src_in_err, &nerrs, &nsrcerrs, k, m);

	// Generate decode matrix
	re = gf_gen_decode_matrix(encode_matrix, decode_matrix,
				  invert_matrix, decode_index, src_err_list, src_in_err,
				  nerrs, nsrcerrs, k, m);
	if (re != 0) {
		printf("Fail to gf_gen_decode_matrix\n");
		goto exit;
	}
	// Pack recovery array as list of valid sources
	// Its order must be the same as the order
	// to generate matrix b in gf_gen_decode_matrix
	for (i = 0; i < k; i++) {
		recov[i] = update_buffs[decode_index[i]];
	}

	// Recover data
	for (i = 0; i < TEST_SOURCES; i++) {
		memset(temp_buffs[i], 0, TEST_LEN);
	}
	ec_init_tables(k, nerrs, decode_matrix, g_tbls);
	for (i = 0; i < k; i++) {
		FUNCTION_UNDER_TEST(TEST_LEN, k, nerrs, i, g_tbls, recov[i], &temp_buffs[k]);
	}
	for (i = 0; i < nerrs; i++) {

		if (0 != memcmp(temp_buffs[k + i], update_buffs[src_err_list[i]], TEST_LEN)) {
			printf("Fail error recovery (%d, %d, %d)\n", m, k, nerrs);
			printf(" - erase list = ");
			for (j = 0; j < nerrs; j++)
				printf(" %d", src_err_list[j]);
			printf(" - Index = ");
			for (p = 0; p < k; p++)
				printf(" %d", decode_index[p]);
			printf("\nencode_matrix:\n");
			dump_u8xu8((u8 *) encode_matrix, m, k);
			printf("inv b:\n");
			dump_u8xu8((u8 *) invert_matrix, k, k);
			printf("\ndecode_matrix:\n");
			dump_u8xu8((u8 *) decode_matrix, m, k);
			printf("recov %d:", src_err_list[i]);
			dump(temp_buffs[k + i], 25);
			printf("orig   :");
			dump(update_buffs[src_err_list[i]], 25);
			re = -1;
			goto exit;
		}
	}
#ifdef TEST_VERBOSE
	putchar('.');
#endif

	// Do more random tests
	for (rtest = 0; rtest < RANDOMS; rtest++) {
		while ((m = (rand() % MMAX)) < 2) ;
		while ((k = (rand() % KMAX)) >= m || k < 1) ;

		if (m > MMAX || k > KMAX)
			continue;

		// Zero the destination buffer for update function
		for (i = k; i < TEST_SOURCES; i++) {
			memset(buffs[i], 0, TEST_LEN);
			memset(update_buffs[i], 0, TEST_LEN);
		}
		// Make random data
		for (i = 0; i < k; i++) {
			for (j = 0; j < TEST_LEN; j++) {
				buffs[i][j] = rand();
				update_buffs[i][j] = buffs[i][j];
			}
		}

		// The matrix generated by gf_gen_cauchy1_matrix
		// is always invertable.
		gf_gen_cauchy1_matrix(encode_matrix, m, k);

		// Make parity vects
		// Generate g_tbls from encode matrix a
		ec_init_tables(k, m - k, &encode_matrix[k * k], g_tbls);
		// Perform matrix dot_prod for EC encoding
		// using g_tbls from encode matrix a
		REF_FUNCTION(TEST_LEN, k, m - k, g_tbls, buffs, &buffs[k]);
		for (i = 0; i < k; i++) {
			FUNCTION_UNDER_TEST(TEST_LEN, k, m - k, i, g_tbls, update_buffs[i],
					    &update_buffs[k]);
		}
		for (i = 0; i < m - k; i++) {
			if (0 != memcmp(update_buffs[k + i], buffs[k + i], TEST_LEN)) {
				printf("\nupdate_buffs%d  :", i);
				dump(update_buffs[k + i], 25);
				printf("buffs%d         :", i);
				dump(buffs[k + i], 25);
				re = -1;
				goto exit;
			}
		}

		// Random errors
		memset(src_in_err, 0, TEST_SOURCES);
		gen_err_list(src_err_list, src_in_err, &nerrs, &nsrcerrs, k, m);

		// Generate decode matrix
		re = gf_gen_decode_matrix(encode_matrix, decode_matrix,
					  invert_matrix, decode_index, src_err_list,
					  src_in_err, nerrs, nsrcerrs, k, m);
		if (re != 0) {
			printf("Fail to gf_gen_decode_matrix\n");
			goto exit;
		}
		// Pack recovery array as list of valid sources
		// Its order must be the same as the order
		// to generate matrix b in gf_gen_decode_matrix
		for (i = 0; i < k; i++) {
			recov[i] = update_buffs[decode_index[i]];
		}

		// Recover data
		for (i = 0; i < TEST_SOURCES; i++) {
			memset(temp_buffs[i], 0, TEST_LEN);
		}
		ec_init_tables(k, nerrs, decode_matrix, g_tbls);
		for (i = 0; i < k; i++) {
			FUNCTION_UNDER_TEST(TEST_LEN, k, nerrs, i, g_tbls, recov[i],
					    &temp_buffs[k]);
		}

		for (i = 0; i < nerrs; i++) {

			if (0 !=
			    memcmp(temp_buffs[k + i], update_buffs[src_err_list[i]],
				   TEST_LEN)) {
				printf("Fail error recovery (%d, %d, %d) - ", m, k, nerrs);
				printf(" - erase list = ");
				for (j = 0; j < nerrs; j++)
					printf(" %d", src_err_list[j]);
				printf(" - Index = ");
				for (p = 0; p < k; p++)
					printf(" %d", decode_index[p]);
				printf("\nencode_matrix:\n");
				dump_u8xu8((u8 *) encode_matrix, m, k);
				printf("inv b:\n");
				dump_u8xu8((u8 *) invert_matrix, k, k);
				printf("\ndecode_matrix:\n");
				dump_u8xu8((u8 *) decode_matrix, m, k);
				printf("orig data:\n");
				dump_matrix(update_buffs, m, 25);
				printf("orig   :");
				dump(update_buffs[src_err_list[i]], 25);
				printf("recov %d:", src_err_list[i]);
				dump(temp_buffs[k + i], 25);
				re = -1;
				goto exit;
			}
		}
#ifdef TEST_VERBOSE
		putchar('.');
#endif
	}

	// Run tests at end of buffer for Electric Fence
	k = 16;
	align = (LEN_ALIGN_CHK_B != 0) ? 1 : ALIGN_SIZE;
	if (k > KMAX) {
		re = -1;
		goto exit;
	}

	for (rows = 1; rows <= 16; rows++) {
		m = k + rows;
		if (m > MMAX) {
			re = -1;
			goto exit;
		}

		for (i = k; i < TEST_SOURCES; i++) {
			memset(buffs[i], 0, TEST_LEN);
			memset(update_buffs[i], 0, TEST_LEN);
		}
		// Make random data
		for (i = 0; i < k; i++) {
			for (j = 0; j < TEST_LEN; j++) {
				buffs[i][j] = rand();
				update_buffs[i][j] = buffs[i][j];
			}
		}

		for (size = 0; size <= EFENCE_TEST_MAX_SIZE; size += align) {
			for (i = 0; i < m; i++) {	// Line up TEST_SIZE from end
				efence_buffs[i] = buffs[i] + TEST_LEN - size;
				efence_update_buffs[i] = update_buffs[i] + TEST_LEN - size;
			}
			// Zero the destination buffer for update function
			for (i = k; i < m; i++) {
				memset(efence_buffs[i], 0, size);
				memset(efence_update_buffs[i], 0, size);
			}

			// The matrix generated by gf_gen_cauchy1_matrix
			// is always invertable.
			gf_gen_cauchy1_matrix(encode_matrix, m, k);

			// Make parity vects
			// Generate g_tbls from encode matrix a
			ec_init_tables(k, m - k, &encode_matrix[k * k], g_tbls);
			// Perform matrix dot_prod for EC encoding
			// using g_tbls from encode matrix a
			REF_FUNCTION(size, k, m - k, g_tbls, efence_buffs, &efence_buffs[k]);
			for (i = 0; i < k; i++) {
				FUNCTION_UNDER_TEST(size, k, m - k, i, g_tbls,
						    efence_update_buffs[i],
						    &efence_update_buffs[k]);
			}
			for (i = 0; i < m - k; i++) {
				if (0 !=
				    memcmp(efence_update_buffs[k + i], efence_buffs[k + i],
					   size)) {
					printf("\nefence_update_buffs%d  :", i);
					dump(efence_update_buffs[k + i], 25);
					printf("efence_buffs%d         :", i);
					dump(efence_buffs[k + i], 25);
					re = -1;
					goto exit;
				}
			}

			// Random errors
			memset(src_in_err, 0, TEST_SOURCES);
			gen_err_list(src_err_list, src_in_err, &nerrs, &nsrcerrs, k, m);

			// Generate decode matrix
			re = gf_gen_decode_matrix(encode_matrix, decode_matrix,
						  invert_matrix, decode_index, src_err_list,
						  src_in_err, nerrs, nsrcerrs, k, m);
			if (re != 0) {
				printf("Fail to gf_gen_decode_matrix\n");
				goto exit;
			}
			// Pack recovery array as list of valid sources
			// Its order must be the same as the order
			// to generate matrix b in gf_gen_decode_matrix
			for (i = 0; i < k; i++) {
				recov[i] = efence_update_buffs[decode_index[i]];
			}

			// Recover data
			for (i = 0; i < TEST_SOURCES; i++) {
				memset(temp_buffs[i], 0, TEST_LEN);
			}
			ec_init_tables(k, nerrs, decode_matrix, g_tbls);
			for (i = 0; i < k; i++) {
				FUNCTION_UNDER_TEST(size, k, nerrs, i, g_tbls, recov[i],
						    &temp_buffs[k]);
			}

			for (i = 0; i < nerrs; i++) {

				if (0 !=
				    memcmp(temp_buffs[k + i],
					   efence_update_buffs[src_err_list[i]], size)) {
					printf("Efence: Fail error recovery (%d, %d, %d)\n", m,
					       k, nerrs);

					printf("size = %d\n", size);

					printf("Test erase list = ");
					for (j = 0; j < nerrs; j++)
						printf(" %d", src_err_list[j]);
					printf(" - Index = ");
					for (p = 0; p < k; p++)
						printf(" %d", decode_index[p]);
					printf("\nencode_matrix:\n");
					dump_u8xu8((u8 *) encode_matrix, m, k);
					printf("inv b:\n");
					dump_u8xu8((u8 *) invert_matrix, k, k);
					printf("\ndecode_matrix:\n");
					dump_u8xu8((u8 *) decode_matrix, m, k);

					printf("recov %d:", src_err_list[i]);
					dump(temp_buffs[k + i], align);
					printf("orig   :");
					dump(efence_update_buffs[src_err_list[i]], align);
					re = 1;
					goto exit;
				}
			}
		}
#ifdef TEST_VERBOSE
		putchar('.');
#endif

	}

	// Test rand ptr alignment if available

	for (rtest = 0; rtest < RANDOMS; rtest++) {
		while ((m = (rand() % MMAX)) < 2) ;
		while ((k = (rand() % KMAX)) >= m || k < 1) ;

		if (m > MMAX || k > KMAX)
			continue;

		size = (TEST_LEN - PTR_ALIGN_CHK_B) & ~15;

		offset = (PTR_ALIGN_CHK_B != 0) ? 1 : PTR_ALIGN_CHK_B;
		// Add random offsets
		for (i = 0; i < m; i++) {
			memset(buffs[i], 0, TEST_LEN);	// zero pad to check write-over
			memset(update_buffs[i], 0, TEST_LEN);	// zero pad to check write-over
			memset(temp_buffs[i], 0, TEST_LEN);	// zero pad to check write-over
			ubuffs[i] = buffs[i] + (rand() & (PTR_ALIGN_CHK_B - offset));
			update_ubuffs[i] =
			    update_buffs[i] + (rand() & (PTR_ALIGN_CHK_B - offset));
			temp_ubuffs[i] = temp_buffs[i] + (rand() & (PTR_ALIGN_CHK_B - offset));
		}

		// Zero the destination buffer for update function
		for (i = k; i < m; i++) {
			memset(ubuffs[i], 0, size);
			memset(update_ubuffs[i], 0, size);
		}
		// Make random data
		for (i = 0; i < k; i++) {
			for (j = 0; j < size; j++) {
				ubuffs[i][j] = rand();
				update_ubuffs[i][j] = ubuffs[i][j];
			}
		}

		// The matrix generated by gf_gen_cauchy1_matrix
		// is always invertable.
		gf_gen_cauchy1_matrix(encode_matrix, m, k);

		// Make parity vects
		// Generate g_tbls from encode matrix a
		ec_init_tables(k, m - k, &encode_matrix[k * k], g_tbls);
		// Perform matrix dot_prod for EC encoding
		// using g_tbls from encode matrix a
		REF_FUNCTION(size, k, m - k, g_tbls, ubuffs, &ubuffs[k]);
		for (i = 0; i < k; i++) {
			FUNCTION_UNDER_TEST(size, k, m - k, i, g_tbls, update_ubuffs[i],
					    &update_ubuffs[k]);
		}
		for (i = 0; i < m - k; i++) {
			if (0 != memcmp(update_ubuffs[k + i], ubuffs[k + i], size)) {
				printf("\nupdate_ubuffs%d  :", i);
				dump(update_ubuffs[k + i], 25);
				printf("ubuffs%d         :", i);
				dump(ubuffs[k + i], 25);
				re = -1;
				goto exit;
			}
		}

		// Random errors
		memset(src_in_err, 0, TEST_SOURCES);
		gen_err_list(src_err_list, src_in_err, &nerrs, &nsrcerrs, k, m);

		// Generate decode matrix
		re = gf_gen_decode_matrix(encode_matrix, decode_matrix,
					  invert_matrix, decode_index, src_err_list,
					  src_in_err, nerrs, nsrcerrs, k, m);
		if (re != 0) {
			printf("Fail to gf_gen_decode_matrix\n");
			goto exit;
		}
		// Pack recovery array as list of valid sources
		// Its order must be the same as the order
		// to generate matrix b in gf_gen_decode_matrix
		for (i = 0; i < k; i++) {
			recov[i] = update_ubuffs[decode_index[i]];
		}

		// Recover data
		for (i = 0; i < m; i++) {
			memset(temp_ubuffs[i], 0, size);
		}
		ec_init_tables(k, nerrs, decode_matrix, g_tbls);
		for (i = 0; i < k; i++) {
			FUNCTION_UNDER_TEST(size, k, nerrs, i, g_tbls, recov[i],
					    &temp_ubuffs[k]);
		}

		for (i = 0; i < nerrs; i++) {

			if (0 !=
			    memcmp(temp_ubuffs[k + i], update_ubuffs[src_err_list[i]], size)) {
				printf("Fail error recovery (%d, %d, %d) - ", m, k, nerrs);
				printf(" - erase list = ");
				for (j = 0; j < nerrs; j++)
					printf(" %d", src_err_list[j]);
				printf(" - Index = ");
				for (p = 0; p < k; p++)
					printf(" %d", decode_index[p]);
				printf("\nencode_matrix:\n");
				dump_u8xu8((unsigned char *)encode_matrix, m, k);
				printf("inv b:\n");
				dump_u8xu8((unsigned char *)invert_matrix, k, k);
				printf("\ndecode_matrix:\n");
				dump_u8xu8((unsigned char *)decode_matrix, m, k);
				printf("orig data:\n");
				dump_matrix(update_ubuffs, m, 25);
				printf("orig   :");
				dump(update_ubuffs[src_err_list[i]], 25);
				printf("recov %d:", src_err_list[i]);
				dump(temp_ubuffs[k + i], 25);
				re = -1;
				goto exit;
			}
		}

		// Confirm that padding around dests is unchanged
		memset(temp_buffs[0], 0, PTR_ALIGN_CHK_B);	// Make reference zero buff

		for (i = 0; i < m; i++) {

			offset = update_ubuffs[i] - update_buffs[i];

			if (memcmp(update_buffs[i], temp_buffs[0], offset)) {
				printf("Fail rand ualign encode pad start\n");
				re = -1;
				goto exit;
			}
			if (memcmp
			    (update_buffs[i] + offset + size, temp_buffs[0],
			     PTR_ALIGN_CHK_B - offset)) {
				printf("Fail rand ualign encode pad end\n");
				re = -1;
				goto exit;
			}
		}

		for (i = 0; i < nerrs; i++) {

			offset = temp_ubuffs[k + i] - temp_buffs[k + i];
			if (memcmp(temp_buffs[k + i], temp_buffs[0], offset)) {
				printf("Fail rand ualign decode pad start\n");
				re = -1;
				goto exit;
			}
			if (memcmp
			    (temp_buffs[k + i] + offset + size, temp_buffs[0],
			     PTR_ALIGN_CHK_B - offset)) {
				printf("Fail rand ualign decode pad end\n");
				re = -1;
				goto exit;
			}
		}

#ifdef TEST_VERBOSE
		putchar('.');
#endif
	}

	// Test size alignment

	align = (LEN_ALIGN_CHK_B != 0) ? 13 : ALIGN_SIZE;

	for (size = TEST_LEN; size >= 0; size -= align) {
		while ((m = (rand() % MMAX)) < 2) ;
		while ((k = (rand() % KMAX)) >= m || k < 1) ;

		if (m > MMAX || k > KMAX)
			continue;

		// Zero the destination buffer for update function
		for (i = k; i < TEST_SOURCES; i++) {
			memset(buffs[i], 0, size);
			memset(update_buffs[i], 0, size);
		}
		// Make random data
		for (i = 0; i < k; i++) {
			for (j = 0; j < size; j++) {
				buffs[i][j] = rand();
				update_buffs[i][j] = buffs[i][j];
			}
		}

		// The matrix generated by gf_gen_cauchy1_matrix
		// is always invertable.
		gf_gen_cauchy1_matrix(encode_matrix, m, k);

		// Make parity vects
		// Generate g_tbls from encode matrix a
		ec_init_tables(k, m - k, &encode_matrix[k * k], g_tbls);
		// Perform matrix dot_prod for EC encoding
		// using g_tbls from encode matrix a
		REF_FUNCTION(size, k, m - k, g_tbls, buffs, &buffs[k]);
		for (i = 0; i < k; i++) {
			FUNCTION_UNDER_TEST(size, k, m - k, i, g_tbls, update_buffs[i],
					    &update_buffs[k]);
		}
		for (i = 0; i < m - k; i++) {
			if (0 != memcmp(update_buffs[k + i], buffs[k + i], size)) {
				printf("\nupdate_buffs%d (size=%d)  :", i, size);
				dump(update_buffs[k + i], 25);
				printf("buffs%d (size=%d)         :", i, size);
				dump(buffs[k + i], 25);
				re = -1;
				goto exit;
			}
		}

		// Random errors
		memset(src_in_err, 0, TEST_SOURCES);
		gen_err_list(src_err_list, src_in_err, &nerrs, &nsrcerrs, k, m);
		// Generate decode matrix
		re = gf_gen_decode_matrix(encode_matrix, decode_matrix,
					  invert_matrix, decode_index, src_err_list,
					  src_in_err, nerrs, nsrcerrs, k, m);
		if (re != 0) {
			printf("Fail to gf_gen_decode_matrix\n");
			goto exit;
		}
		// Pack recovery array as list of valid sources
		// Its order must be the same as the order
		// to generate matrix b in gf_gen_decode_matrix
		for (i = 0; i < k; i++) {
			recov[i] = update_buffs[decode_index[i]];
		}

		// Recover data
		for (i = 0; i < TEST_SOURCES; i++) {
			memset(temp_buffs[i], 0, TEST_LEN);
		}
		ec_init_tables(k, nerrs, decode_matrix, g_tbls);
		for (i = 0; i < k; i++) {
			FUNCTION_UNDER_TEST(size, k, nerrs, i, g_tbls, recov[i],
					    &temp_buffs[k]);
		}

		for (i = 0; i < nerrs; i++) {

			if (0 !=
			    memcmp(temp_buffs[k + i], update_buffs[src_err_list[i]], size)) {
				printf("Fail error recovery (%d, %d, %d) - ", m, k, nerrs);
				printf(" - erase list = ");
				for (j = 0; j < nerrs; j++)
					printf(" %d", src_err_list[j]);
				printf(" - Index = ");
				for (p = 0; p < k; p++)
					printf(" %d", decode_index[p]);
				printf("\nencode_matrix:\n");
				dump_u8xu8((unsigned char *)encode_matrix, m, k);
				printf("inv b:\n");
				dump_u8xu8((unsigned char *)invert_matrix, k, k);
				printf("\ndecode_matrix:\n");
				dump_u8xu8((unsigned char *)decode_matrix, m, k);
				printf("orig data:\n");
				dump_matrix(update_buffs, m, 25);
				printf("orig   :");
				dump(update_buffs[src_err_list[i]], 25);
				printf("recov %d:", src_err_list[i]);
				dump(temp_buffs[k + i], 25);
				re = -1;
				goto exit;
			}
		}
#ifdef TEST_VERBOSE
		putchar('.');
#endif
	}

	printf("done EC tests: Pass\n");
	re = 0;

      exit:
	for (i = 0; i < TEST_SOURCES; i++) {
		if (buffs[i])
			aligned_free(buffs[i]);
		if (temp_buffs[i])
			aligned_free(temp_buffs[i]);
		if (update_buffs[i])
			aligned_free(update_buffs[i]);
	}
	free(encode_matrix);
	free(decode_matrix);
	free(invert_matrix);
	free(g_tbls);
	return 0;
}
