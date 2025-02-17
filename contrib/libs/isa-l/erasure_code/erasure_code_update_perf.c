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

//By default, test multibinary version
#ifndef FUNCTION_UNDER_TEST
# define FUNCTION_UNDER_TEST ec_encode_data_update
# define REF_FUNCTION ec_encode_data
#endif

//By default, test EC(8+4)
#if (!defined(VECT))
# define VECT 4
#endif

#define str(s) #s
#define xstr(s) str(s)

#ifndef GT_L3_CACHE
# define GT_L3_CACHE  32*1024*1024	/* some number > last level cache */
#endif

#if !defined(COLD_TEST) && !defined(TEST_CUSTOM)
// Cached test, loop many times over small dataset
# define TEST_SOURCES 32
# define TEST_LEN(m)  ((128*1024 / m) & ~(64-1))
# define TEST_TYPE_STR "_warm"
#elif defined (COLD_TEST)
// Uncached test.  Pull from large mem base.
# define TEST_SOURCES 32
# define TEST_LEN(m)  ((GT_L3_CACHE / m) & ~(64-1))
# define TEST_TYPE_STR "_cold"
#elif defined (TEST_CUSTOM)
# define TEST_TYPE_STR "_cus"
#endif
#ifndef TEST_SEED
# define TEST_SEED 0x1234
#endif

#define MMAX TEST_SOURCES
#define KMAX TEST_SOURCES

typedef unsigned char u8;

void usage(const char *app_name)
{
	fprintf(stderr,
		"Usage: %s [options]\n"
		"  -h        Help\n"
		"  -k <val>  Number of source buffers\n"
		"  -p <val>  Number of parity buffers\n"
		"  -e <val>  Number of simulated buffers with errors (cannot be higher than p or k)\n",
		app_name);
}

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

void encode_update_test_ref(int m, int k, u8 * g_tbls, u8 ** buffs, u8 * a)
{
	ec_init_tables(k, m - k, &a[k * k], g_tbls);
	REF_FUNCTION(TEST_LEN(m), k, m - k, g_tbls, buffs, &buffs[k]);
}

void encode_update_test(int m, int k, u8 * g_tbls, u8 ** perf_update_buffs, u8 * a)
{
	int i;

	// Make parity vects
	ec_init_tables(k, m - k, &a[k * k], g_tbls);
	for (i = 0; i < k; i++) {
		FUNCTION_UNDER_TEST(TEST_LEN(m), k, m - k, i, g_tbls,
				    perf_update_buffs[i], &perf_update_buffs[k]);
	}
}

int decode_test(int m, int k, u8 ** update_buffs, u8 ** recov, u8 * a, u8 * src_in_err,
		u8 * src_err_list, int nerrs, u8 * g_tbls, u8 ** perf_update_buffs)
{
	int i, j, r;
	u8 b[MMAX * KMAX], c[MMAX * KMAX], d[MMAX * KMAX];
	// Construct b by removing error rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (src_in_err[r])
			r++;
		recov[i] = update_buffs[r];
		for (j = 0; j < k; j++)
			b[k * i + j] = a[k * r + j];
	}

	if (gf_invert_matrix(b, d, k) < 0) {
		printf("BAD MATRIX\n");
		return -1;
	}

	for (i = 0; i < nerrs; i++)
		for (j = 0; j < k; j++)
			c[k * i + j] = d[k * src_err_list[i] + j];

	// Recover data
	ec_init_tables(k, nerrs, c, g_tbls);
	for (i = 0; i < k; i++) {
		FUNCTION_UNDER_TEST(TEST_LEN(m), k, nerrs, i, g_tbls, recov[i],
				    perf_update_buffs);
	}
	return 0;
}

int main(int argc, char *argv[])
{
	int i, j, check, m, k, p, nerrs, ret = -1;
	void *buf;
	u8 *temp_buffs[TEST_SOURCES] = { NULL };
	u8 *buffs[TEST_SOURCES] = { NULL };
	u8 *update_buffs[TEST_SOURCES] = { NULL };
	u8 *perf_update_buffs[TEST_SOURCES] = { NULL };
	u8 a[MMAX * KMAX];
	u8 g_tbls[KMAX * TEST_SOURCES * 32], src_in_err[TEST_SOURCES];
	u8 src_err_list[TEST_SOURCES], *recov[TEST_SOURCES];
	struct perf start;

	/* Set default parameters */
	k = 10;
	p = VECT;
	nerrs = VECT;

	/* Parse arguments */
	for (i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-k") == 0) {
			k = atoi(argv[++i]);
		} else if (strcmp(argv[i], "-p") == 0) {
			p = atoi(argv[++i]);
		} else if (strcmp(argv[i], "-e") == 0) {
			nerrs = atoi(argv[++i]);
		} else if (strcmp(argv[i], "-h") == 0) {
			usage(argv[0]);
			return 0;
		} else {
			usage(argv[0]);
			return -1;
		}
	}

	if (nerrs > k) {
		printf
		    ("Number of errors (%d) cannot be higher than number of data buffers (%d)\n",
		     nerrs, k);
		return -1;
	}

	if (k <= 0) {
		printf("Number of source buffers (%d) must be > 0\n", k);
		return -1;
	}

	if (p <= 0) {
		printf("Number of parity buffers (%d) must be > 0\n", p);
		return -1;
	}

	if (nerrs > p) {
		printf
		    ("Number of errors (%d) cannot be higher than number of parity buffers (%d)\n",
		     nerrs, p);
		return -1;
	}

	if (nerrs <= 0) {
		printf("Number of errors (%d) must be > 0\n", nerrs);
		return -1;
	}

	m = k + p;

	if (m > MMAX) {
		printf("Number of total buffers (data and parity) cannot be higher than %d\n",
		       MMAX);
		return -1;
	}

	u8 *err_list = malloc((size_t)nerrs);
	if (err_list == NULL) {
		printf("Error allocating list of array of error indices\n");
		return -1;
	}

	srand(TEST_SEED);

	for (i = 0; i < nerrs;) {
		u8 next_err = rand() % k;
		for (j = 0; j < i; j++)
			if (next_err == err_list[j])
				break;
		if (j != i)
			continue;
		err_list[i++] = next_err;
	}

	printf("Testing with %u data buffers and %u parity buffers (num errors = %u, in [ ", k,
	       p, nerrs);
	for (i = 0; i < nerrs; i++)
		printf("%d ", err_list[i]);

	printf("])\n");

	printf(xstr(FUNCTION_UNDER_TEST) "_perf: %dx%d %d\n", m, TEST_LEN(m), nerrs);

	memcpy(src_err_list, err_list, nerrs);
	memset(src_in_err, 0, TEST_SOURCES);
	for (i = 0; i < nerrs; i++)
		src_in_err[src_err_list[i]] = 1;

	// Allocate the arrays
	for (i = 0; i < m; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN(m))) {
			printf("Error allocating buffers\n");
			goto exit;
		}
		buffs[i] = buf;
	}

	for (i = 0; i < (m - k); i++) {
		if (posix_memalign(&buf, 64, TEST_LEN(m))) {
			printf("Error allocating buffers\n");
			goto exit;
		}
		temp_buffs[i] = buf;
		memset(temp_buffs[i], 0, TEST_LEN(m));	// initialize the destination buffer to be zero for update function
	}

	for (i = 0; i < TEST_SOURCES; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN(m))) {
			printf("Error allocating buffers\n");
			goto exit;
		}
		update_buffs[i] = buf;
		memset(update_buffs[i], 0, TEST_LEN(m));	// initialize the destination buffer to be zero for update function
	}
	for (i = 0; i < TEST_SOURCES; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN(m))) {
			printf("Error allocating buffers\n");
			goto exit;
		}
		perf_update_buffs[i] = buf;
		memset(perf_update_buffs[i], 0, TEST_LEN(m));	// initialize the destination buffer to be zero for update function
	}

	// Make random data
	for (i = 0; i < k; i++)
		for (j = 0; j < TEST_LEN(m); j++) {
			buffs[i][j] = rand();
			update_buffs[i][j] = buffs[i][j];
		}

	gf_gen_rs_matrix(a, m, k);

	encode_update_test_ref(m, k, g_tbls, buffs, a);
	encode_update_test(m, k, g_tbls, update_buffs, a);
	for (i = 0; i < m - k; i++) {
		if (0 != memcmp(update_buffs[k + i], buffs[k + i], TEST_LEN(m))) {
			printf("\nupdate_buffs%d  :", i);
			dump(update_buffs[k + i], 25);
			printf("buffs%d         :", i);
			dump(buffs[k + i], 25);
			goto exit;
		}
	}

#ifdef DO_REF_PERF
	// Start encode test
	BENCHMARK(&start, BENCHMARK_TIME, encode_update_test_ref(m, k, g_tbls, buffs, a));
	printf(xstr(REF_FUNCTION) TEST_TYPE_STR ": ");
	perf_print(start, (long long)(TEST_LEN(m)) * (m));
#endif

	// Start encode test
	BENCHMARK(&start, BENCHMARK_TIME,
		  encode_update_test(m, k, g_tbls, perf_update_buffs, a));
	printf(xstr(FUNCTION_UNDER_TEST) TEST_TYPE_STR ": ");
	perf_print(start, (long long)(TEST_LEN(m)) * (m));

	// Start encode test
	BENCHMARK(&start, BENCHMARK_TIME,
		  // Make parity vects
		  ec_init_tables(k, m - k, &a[k * k], g_tbls);
		  FUNCTION_UNDER_TEST(TEST_LEN(m), k, m - k, 0, g_tbls, perf_update_buffs[0],
				      &perf_update_buffs[k]));
	printf(xstr(FUNCTION_UNDER_TEST) "_single_src" TEST_TYPE_STR ": ");
	perf_print(start, (long long)(TEST_LEN(m)) * (m - k + 1));

	// Start encode test
	BENCHMARK(&start, BENCHMARK_TIME,
		  // Make parity vects
		  FUNCTION_UNDER_TEST(TEST_LEN(m), k, m - k, 0, g_tbls, perf_update_buffs[0],
				      &perf_update_buffs[k]));
	printf(xstr(FUNCTION_UNDER_TEST) "_single_src_simple" TEST_TYPE_STR ": ");
	perf_print(start, (long long)(TEST_LEN(m)) * (m - k + 1));

	for (i = k; i < m; i++) {
		memset(update_buffs[i], 0, TEST_LEN(m));	// initialize the destination buffer to be zero for update function
	}
	for (i = 0; i < k; i++) {
		FUNCTION_UNDER_TEST(TEST_LEN(m), k, m - k, i, g_tbls, update_buffs[i],
				    &update_buffs[k]);
	}

	decode_test(m, k, update_buffs, recov, a, src_in_err, src_err_list,
		    nerrs, g_tbls, temp_buffs);
	BENCHMARK(&start, BENCHMARK_TIME, check =
		  decode_test(m, k, update_buffs, recov, a, src_in_err, src_err_list,
			      nerrs, g_tbls, perf_update_buffs));
	if (check) {
		printf("BAD_MATRIX\n");
		ret = check;
		goto exit;
	}

	for (i = 0; i < nerrs; i++) {
		if (0 != memcmp(temp_buffs[i], update_buffs[src_err_list[i]], TEST_LEN(m))) {
			printf("Fail error recovery (%d, %d, %d) - \n", m, k, nerrs);
			goto exit;
		}
	}

	printf(xstr(FUNCTION_UNDER_TEST) "_decode" TEST_TYPE_STR ": ");
	perf_print(start, (long long)(TEST_LEN(m)) * (k + nerrs));

	printf("done all: Pass\n");

	ret = 0;

      exit:
	free(err_list);
	for (i = 0; i < TEST_SOURCES; i++) {
		free(buffs[i]);
		free(temp_buffs[i]);
		free(update_buffs[i]);
		free(perf_update_buffs[i]);
	}
	return ret;
}
