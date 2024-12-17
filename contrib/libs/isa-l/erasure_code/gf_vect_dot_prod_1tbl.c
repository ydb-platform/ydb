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
#include "test.h"
#include "erasure_code.h"

#ifndef GT_L3_CACHE
# define GT_L3_CACHE  32*1024*1024	/* some number > last level cache */
#endif

#if !defined(COLD_TEST) && !defined(TEST_CUSTOM)
// Cached test, loop many times over small dataset
# define TEST_SOURCES 10
# define TEST_LEN     8*1024
# define TEST_TYPE_STR "_warm"
#elif defined (COLD_TEST)
// Uncached test.  Pull from large mem base.
# define TEST_SOURCES 10
# define TEST_LEN     (GT_L3_CACHE / TEST_SOURCES)
# define TEST_TYPE_STR "_cold"
#elif defined (TEST_CUSTOM)
# define TEST_TYPE_STR "_cus"
#endif

typedef unsigned char u8;

// Global GF(256) tables
u8 gff[256];
u8 gflog[256];
u8 gf_mul_table[256 * 256];

void mk_gf_field(void)
{
	int i;
	u8 s = 1;
	gflog[0] = 0;

	for (i = 0; i < 256; i++) {
		gff[i] = s;
		gflog[s] = i;
		s = (s << 1) ^ ((s & 0x80) ? 0x1d : 0);	// mult by GF{2}
	}
}

void mk_gf_mul_table(u8 * table)
{
	// Populate a single table with all multiply combinations for a fast,
	// single-table lookup of GF(2^8) multiply at the expense of memory.
	int i, j;
	for (i = 0; i < 256; i++)
		for (j = 0; j < 256; j++)
			table[i * 256 + j] = gf_mul_erasure(i, j);
}

void gf_vect_dot_prod_ref(int len, int vlen, u8 * v, u8 ** src, u8 * dest)
{
	int i, j;
	u8 s;
	for (i = 0; i < len; i++) {
		s = 0;
		for (j = 0; j < vlen; j++)
			s ^= gf_mul_erasure(src[j][i], v[j]);

		dest[i] = s;
	}
}

void gf_vect_dot_prod_mult(int len, int vlen, u8 * v, u8 ** src, u8 * dest)
{
	int i, j;
	u8 s;
	for (i = 0; i < len; i++) {
		s = 0;
		for (j = 0; j < vlen; j++) {
			s ^= gf_mul_table[v[j] * 256 + src[j][i]];
		}
		dest[i] = s;
	}

}

int main(void)
{
	int i, j;
	u8 vec[TEST_SOURCES], *dest1, *dest2;
	u8 *matrix[TEST_SOURCES];
	struct perf start;

	dest1 = (u8 *) malloc(TEST_LEN);
	dest2 = (u8 *) malloc(TEST_LEN);

	if (NULL == dest1 || NULL == dest2) {
		printf("buffer alloc error\n");
		return -1;
	}
	memset(dest1, 0xfe, TEST_LEN);
	memset(dest2, 0xfe, TEST_LEN);

	mk_gf_field();
	mk_gf_mul_table(gf_mul_table);

	//generate random vector and matrix/data
	for (i = 0; i < TEST_SOURCES; i++) {
		vec[i] = rand();

		if (!(matrix[i] = malloc(TEST_LEN))) {
			fprintf(stderr, "Error failure\n\n");
			return -1;
		}
		for (j = 0; j < TEST_LEN; j++)
			matrix[i][j] = rand();

	}

	BENCHMARK(&start, BENCHMARK_TIME,
		  gf_vect_dot_prod_ref(TEST_LEN, TEST_SOURCES, vec, matrix, dest1));
	printf("gf_vect_dot_prod_2tbl" TEST_TYPE_STR ": ");
	perf_print(start, (long long)TEST_LEN * (TEST_SOURCES + 1));

	BENCHMARK(&start, BENCHMARK_TIME,
		  gf_vect_dot_prod_mult(TEST_LEN, TEST_SOURCES, vec, matrix, dest2));
	printf("gf_vect_dot_prod_1tbl" TEST_TYPE_STR ": ");
	perf_print(start, (long long)TEST_LEN * (TEST_SOURCES + 1));

	// Compare with reference function
	if (0 != memcmp(dest1, dest2, TEST_LEN)) {
		printf("Error, different results!\n\n");
		return -1;
	}

	printf("Pass functional test\n");
	return 0;
}
