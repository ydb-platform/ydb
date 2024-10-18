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
#include "erasure_code.h"
#include "test.h"

//#define CACHED_TEST
#ifdef CACHED_TEST
// Cached test, loop many times over small dataset
# define TEST_SOURCES 32
# define TEST_LEN(m)  ((128*1024 / m) & ~(64-1))
# define TEST_TYPE_STR "_warm"
#else
# ifndef TEST_CUSTOM
// Uncached test.  Pull from large mem base.
#  define TEST_SOURCES 32
#  define GT_L3_CACHE  32*1024*1024	/* some number > last level cache */
#  define TEST_LEN(m)  ((GT_L3_CACHE / m) & ~(64-1))
#  define TEST_TYPE_STR "_cold"
# else
#  define TEST_TYPE_STR "_cus"
# endif
#endif

#define MMAX TEST_SOURCES
#define KMAX TEST_SOURCES

#define BAD_MATRIX -1

typedef unsigned char u8;

void ec_encode_perf(int m, int k, u8 * a, u8 * g_tbls, u8 ** buffs)
{
	ec_init_tables(k, m - k, &a[k * k], g_tbls);
	ec_encode_data_base(TEST_LEN(m), k, m - k, g_tbls, buffs, &buffs[k]);
}

int ec_decode_perf(int m, int k, u8 * a, u8 * g_tbls, u8 ** buffs, u8 * src_in_err,
		   u8 * src_err_list, int nerrs, u8 ** temp_buffs)
{
	int i, j, r;
	u8 b[MMAX * KMAX], c[MMAX * KMAX], d[MMAX * KMAX];
	u8 *recov[TEST_SOURCES];

	// Construct b by removing error rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (src_in_err[r])
			r++;
		recov[i] = buffs[r];
		for (j = 0; j < k; j++)
			b[k * i + j] = a[k * r + j];
	}

	if (gf_invert_matrix(b, d, k) < 0)
		return BAD_MATRIX;

	for (i = 0; i < nerrs; i++)
		for (j = 0; j < k; j++)
			c[k * i + j] = d[k * src_err_list[i] + j];

	// Recover data
	ec_init_tables(k, nerrs, c, g_tbls);
	ec_encode_data_base(TEST_LEN(m), k, nerrs, g_tbls, recov, temp_buffs);

	return 0;
}

int main(int argc, char *argv[])
{
	int i, j, m, k, nerrs, check;
	void *buf;
	u8 *temp_buffs[TEST_SOURCES], *buffs[TEST_SOURCES];
	u8 a[MMAX * KMAX];
	u8 g_tbls[KMAX * TEST_SOURCES * 32], src_in_err[TEST_SOURCES];
	u8 src_err_list[TEST_SOURCES];
	struct perf start;

	// Pick test parameters
	m = 14;
	k = 10;
	nerrs = 4;
	const u8 err_list[] = { 2, 4, 5, 7 };

	printf("erasure_code_base_perf: %dx%d %d\n", m, TEST_LEN(m), nerrs);

	if (m > MMAX || k > KMAX || nerrs > (m - k)) {
		printf(" Input test parameter error\n");
		return -1;
	}

	memcpy(src_err_list, err_list, nerrs);
	memset(src_in_err, 0, TEST_SOURCES);
	for (i = 0; i < nerrs; i++)
		src_in_err[src_err_list[i]] = 1;

	// Allocate the arrays
	for (i = 0; i < m; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN(m))) {
			printf("alloc error: Fail\n");
			return -1;
		}
		buffs[i] = buf;
	}

	for (i = 0; i < (m - k); i++) {
		if (posix_memalign(&buf, 64, TEST_LEN(m))) {
			printf("alloc error: Fail\n");
			return -1;
		}
		temp_buffs[i] = buf;
	}

	// Make random data
	for (i = 0; i < k; i++)
		for (j = 0; j < TEST_LEN(m); j++)
			buffs[i][j] = rand();

	gf_gen_rs_matrix(a, m, k);

	// Start encode test
	BENCHMARK(&start, BENCHMARK_TIME, ec_encode_perf(m, k, a, g_tbls, buffs));
	printf("erasure_code_base_encode" TEST_TYPE_STR ": ");
	perf_print(start, (long long)(TEST_LEN(m)) * (m));

	// Start decode test
	BENCHMARK(&start, BENCHMARK_TIME, check =
		  ec_decode_perf(m, k, a, g_tbls, buffs, src_in_err, src_err_list, nerrs,
				 temp_buffs));

	if (check == BAD_MATRIX) {
		printf("BAD MATRIX\n");
		return check;
	}

	for (i = 0; i < nerrs; i++) {
		if (0 != memcmp(temp_buffs[i], buffs[src_err_list[i]], TEST_LEN(m))) {
			printf("Fail error recovery (%d, %d, %d) - ", m, k, nerrs);
			return -1;
		}
	}

	printf("erasure_code_base_decode" TEST_TYPE_STR ": ");
	perf_print(start, (long long)(TEST_LEN(m)) * (k + nerrs));

	printf("done all: Pass\n");
	return 0;
}
