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
#include <string.h>		// for memset
#include "erasure_code.h"
#include "test.h"

//#define CACHED_TEST
#ifdef CACHED_TEST
// Cached test, loop many times over small dataset
# define TEST_LEN     8*1024
# define TEST_TYPE_STR "_warm"
#else
# ifndef TEST_CUSTOM
// Uncached test.  Pull from large mem base.
#  define TEST_SOURCES 10
#  define GT_L3_CACHE  32*1024*1024	/* some number > last level cache */
#  define TEST_LEN     GT_L3_CACHE / 2
#  define TEST_TYPE_STR "_cold"
# else
#  define TEST_TYPE_STR "_cus"
# endif
#endif

#define TEST_MEM (2 * TEST_LEN)

typedef unsigned char u8;

void gf_vect_mul_perf(u8 a, u8 * gf_const_tbl, u8 * buff1, u8 * buff2)
{
	gf_vect_mul_init(a, gf_const_tbl);
	gf_vect_mul(TEST_LEN, gf_const_tbl, buff1, buff2);
}

int main(int argc, char *argv[])
{
	u8 *buff1, *buff2, gf_const_tbl[64], a = 2;
	struct perf start;

	printf("gf_vect_mul_perf:\n");

	// Allocate large mem region
	buff1 = (u8 *) malloc(TEST_LEN);
	buff2 = (u8 *) malloc(TEST_LEN);
	if (NULL == buff1 || NULL == buff2) {
		printf("Failed to allocate %dB\n", TEST_LEN);
		return 1;
	}

	memset(buff1, 0, TEST_LEN);
	memset(buff2, 0, TEST_LEN);

	printf("Start timed tests\n");
	fflush(0);

	BENCHMARK(&start, BENCHMARK_TIME, gf_vect_mul_perf(a, gf_const_tbl, buff1, buff2));

	printf("gf_vect_mul" TEST_TYPE_STR ": ");
	perf_print(start, (long long)TEST_LEN);

	return 0;
}
