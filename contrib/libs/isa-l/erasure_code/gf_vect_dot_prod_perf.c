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

#ifndef FUNCTION_UNDER_TEST
# define FUNCTION_UNDER_TEST gf_vect_dot_prod
#endif

#define str(s) #s
#define xstr(s) str(s)

//#define CACHED_TEST
#ifdef CACHED_TEST
// Cached test, loop many times over small dataset
# define TEST_SOURCES 10
# define TEST_LEN     8*1024
# define TEST_TYPE_STR "_warm"
#else
# ifndef TEST_CUSTOM
// Uncached test.  Pull from large mem base.
#  define TEST_SOURCES 10
#  define GT_L3_CACHE  32*1024*1024	/* some number > last level cache */
#  define TEST_LEN     ((GT_L3_CACHE / TEST_SOURCES) & ~(64-1))
#  define TEST_TYPE_STR "_cold"
# else
#  define TEST_TYPE_STR "_cus"
# endif
#endif

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

void vect_dot_prod_perf(void (*fun_ptr)
			 (int, int, unsigned char *, unsigned char **, unsigned char *),
			u8 * g, u8 * g_tbls, u8 ** buffs, u8 * dest_ref)
{
	int j;
	for (j = 0; j < TEST_SOURCES; j++)
		gf_vect_mul_init(g[j], &g_tbls[j * 32]);

	(*fun_ptr) (TEST_LEN, TEST_SOURCES, &g_tbls[0], buffs, dest_ref);
}

int main(int argc, char *argv[])
{
	int i, j;
	void *buf;
	u8 g[TEST_SOURCES], g_tbls[TEST_SOURCES * 32], *dest, *dest_ref;
	u8 *temp_buff, *buffs[TEST_SOURCES];
	struct perf start;

	printf(xstr(FUNCTION_UNDER_TEST) ": %dx%d\n", TEST_SOURCES, TEST_LEN);

	// Allocate the arrays
	for (i = 0; i < TEST_SOURCES; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN)) {
			printf("alloc error: Fail");
			return -1;
		}
		buffs[i] = buf;
	}

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	dest_ref = buf;

	if (posix_memalign(&buf, 64, TEST_LEN)) {
		printf("alloc error: Fail");
		return -1;
	}
	temp_buff = buf;

	// Performance test
	for (i = 0; i < TEST_SOURCES; i++)
		for (j = 0; j < TEST_LEN; j++)
			buffs[i][j] = rand();

	memset(dest, 0, TEST_LEN);
	memset(temp_buff, 0, TEST_LEN);
	memset(dest_ref, 0, TEST_LEN);
	memset(g, 0, TEST_SOURCES);

	for (i = 0; i < TEST_SOURCES; i++)
		g[i] = rand();

#ifdef DO_REF_PERF
	BENCHMARK(&start, BENCHMARK_TIME,
		  vect_dot_prod_perf(&gf_vect_dot_prod_base, g, g_tbls, buffs, dest_ref)
	    );
	printf("gf_vect_dot_prod_base" TEST_TYPE_STR ": ");
	perf_print(start, (long long)TEST_LEN * (TEST_SOURCES + 1));
#else
	vect_dot_prod_perf(&gf_vect_dot_prod_base, g, g_tbls, buffs, dest_ref);
#endif

	BENCHMARK(&start, BENCHMARK_TIME,
		  vect_dot_prod_perf(&FUNCTION_UNDER_TEST, g, g_tbls, buffs, dest));
	printf(xstr(FUNCTION_UNDER_TEST) TEST_TYPE_STR ": ");
	perf_print(start, (long long)TEST_LEN * (TEST_SOURCES + 1));

	if (0 != memcmp(dest_ref, dest, TEST_LEN)) {
		printf("Fail zero " xstr(FUNCTION_UNDER_TEST) " test\n");
		dump_matrix(buffs, 5, TEST_SOURCES);
		printf("dprod_base:");
		dump(dest_ref, 25);
		printf("dprod:");
		dump(dest, 25);
		return -1;
	}

	printf("pass perf check\n");
	return 0;
}
