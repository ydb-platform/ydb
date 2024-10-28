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

#ifndef ALIGN_SIZE
# define ALIGN_SIZE 32
#endif

#ifndef FUNCTION_UNDER_TEST
//By default, test multi-binary version
# define FUNCTION_UNDER_TEST gf_vect_mad
# define REF_FUNCTION gf_vect_dot_prod
# define VECT 1
#endif

#ifndef TEST_MIN_SIZE
# define TEST_MIN_SIZE 64
#endif

#define str(s) #s
#define xstr(s) str(s)

#define TEST_LEN 8192
#define TEST_SIZE (TEST_LEN/2)
#define TEST_MEM  TEST_SIZE
#define TEST_LOOPS 20000
#define TEST_TYPE_STR ""

#ifndef TEST_SOURCES
# define TEST_SOURCES  16
#endif
#ifndef RANDOMS
# define RANDOMS 20
#endif

#ifdef EC_ALIGNED_ADDR
// Define power of 2 range to check ptr, len alignment
# define PTR_ALIGN_CHK_B 0
# define LEN_ALIGN_CHK_B 0	// 0 for aligned only
#else
// Define power of 2 range to check ptr, len alignment
# define PTR_ALIGN_CHK_B ALIGN_SIZE
# define LEN_ALIGN_CHK_B ALIGN_SIZE	// 0 for aligned only
#endif

#define str(s) #s
#define xstr(s) str(s)

typedef unsigned char u8;

#if (VECT == 1)
# define LAST_ARG *dest
#else
# define LAST_ARG **dest
#endif

extern void FUNCTION_UNDER_TEST(int len, int vec, int vec_i, unsigned char *gftbls,
				unsigned char *src, unsigned char LAST_ARG);
extern void REF_FUNCTION(int len, int vlen, unsigned char *gftbls, unsigned char **src,
			 unsigned char LAST_ARG);

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

int main(int argc, char *argv[])
{
	int i, j, rtest, srcs;
	void *buf;
	u8 gf[6][TEST_SOURCES];
	u8 *g_tbls;
	u8 *dest_ref[VECT];
	u8 *dest_ptrs[VECT], *buffs[TEST_SOURCES];
	int vector = VECT;

	int align, size;
	unsigned char *efence_buffs[TEST_SOURCES];
	unsigned int offset;
	u8 *ubuffs[TEST_SOURCES];
	u8 *udest_ptrs[VECT];
	printf("test" xstr(FUNCTION_UNDER_TEST) ": %dx%d ", TEST_SOURCES, TEST_LEN);

	// Allocate the arrays
	for (i = 0; i < TEST_SOURCES; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN)) {
			printf("alloc error: Fail");
			return -1;
		}
		buffs[i] = buf;
	}

	if (posix_memalign(&buf, 16, 2 * (vector * TEST_SOURCES * 32))) {
		printf("alloc error: Fail");
		return -1;
	}
	g_tbls = buf;

	for (i = 0; i < vector; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN)) {
			printf("alloc error: Fail");
			return -1;
		}
		dest_ptrs[i] = buf;
		memset(dest_ptrs[i], 0, TEST_LEN);
	}

	for (i = 0; i < vector; i++) {
		if (posix_memalign(&buf, 64, TEST_LEN)) {
			printf("alloc error: Fail");
			return -1;
		}
		dest_ref[i] = buf;
		memset(dest_ref[i], 0, TEST_LEN);
	}

	// Test of all zeros
	for (i = 0; i < TEST_SOURCES; i++)
		memset(buffs[i], 0, TEST_LEN);

	switch (vector) {
	case 6:
		memset(gf[5], 0xe6, TEST_SOURCES);
	case 5:
		memset(gf[4], 4, TEST_SOURCES);
	case 4:
		memset(gf[3], 9, TEST_SOURCES);
	case 3:
		memset(gf[2], 7, TEST_SOURCES);
	case 2:
		memset(gf[1], 1, TEST_SOURCES);
	case 1:
		memset(gf[0], 2, TEST_SOURCES);
		break;
	default:
		return -1;
	}

	for (i = 0; i < TEST_SOURCES; i++)
		for (j = 0; j < TEST_LEN; j++)
			buffs[i][j] = rand();

	for (i = 0; i < vector; i++)
		for (j = 0; j < TEST_SOURCES; j++) {
			gf[i][j] = rand();
			gf_vect_mul_init(gf[i][j], &g_tbls[i * (32 * TEST_SOURCES) + j * 32]);
		}

	for (i = 0; i < vector; i++)
		gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES, &g_tbls[i * 32 * TEST_SOURCES],
				      buffs, dest_ref[i]);

	for (i = 0; i < vector; i++)
		memset(dest_ptrs[i], 0, TEST_LEN);
	for (i = 0; i < TEST_SOURCES; i++) {
#if (VECT == 1)
		FUNCTION_UNDER_TEST(TEST_LEN, TEST_SOURCES, i, g_tbls, buffs[i], *dest_ptrs);
#else
		FUNCTION_UNDER_TEST(TEST_LEN, TEST_SOURCES, i, g_tbls, buffs[i], dest_ptrs);
#endif
	}
	for (i = 0; i < vector; i++) {
		if (0 != memcmp(dest_ref[i], dest_ptrs[i], TEST_LEN)) {
			printf("Fail zero " xstr(FUNCTION_UNDER_TEST) " test%d\n", i);
			dump_matrix(buffs, vector, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref[i], 25);
			printf("dprod_dut:");
			dump(dest_ptrs[i], 25);
			return -1;
		}
	}

#if (VECT == 1)
	REF_FUNCTION(TEST_LEN, TEST_SOURCES, g_tbls, buffs, *dest_ref);
#else
	REF_FUNCTION(TEST_LEN, TEST_SOURCES, g_tbls, buffs, dest_ref);
#endif
	for (i = 0; i < vector; i++) {
		if (0 != memcmp(dest_ref[i], dest_ptrs[i], TEST_LEN)) {
			printf("Fail zero " xstr(FUNCTION_UNDER_TEST) " test%d\n", i);
			dump_matrix(buffs, vector, TEST_SOURCES);
			printf("dprod_base:");
			dump(dest_ref[i], 25);
			printf("dprod_dut:");
			dump(dest_ptrs[i], 25);
			return -1;
		}
	}

#ifdef TEST_VERBOSE
	putchar('.');
#endif

	// Rand data test

	for (rtest = 0; rtest < RANDOMS; rtest++) {
		for (i = 0; i < TEST_SOURCES; i++)
			for (j = 0; j < TEST_LEN; j++)
				buffs[i][j] = rand();

		for (i = 0; i < vector; i++)
			for (j = 0; j < TEST_SOURCES; j++) {
				gf[i][j] = rand();
				gf_vect_mul_init(gf[i][j],
						 &g_tbls[i * (32 * TEST_SOURCES) + j * 32]);
			}

		for (i = 0; i < vector; i++)
			gf_vect_dot_prod_base(TEST_LEN, TEST_SOURCES,
					      &g_tbls[i * 32 * TEST_SOURCES], buffs,
					      dest_ref[i]);

		for (i = 0; i < vector; i++)
			memset(dest_ptrs[i], 0, TEST_LEN);
		for (i = 0; i < TEST_SOURCES; i++) {
#if (VECT == 1)
			FUNCTION_UNDER_TEST(TEST_LEN, TEST_SOURCES, i, g_tbls, buffs[i],
					    *dest_ptrs);
#else
			FUNCTION_UNDER_TEST(TEST_LEN, TEST_SOURCES, i, g_tbls, buffs[i],
					    dest_ptrs);
#endif
		}
		for (i = 0; i < vector; i++) {
			if (0 != memcmp(dest_ref[i], dest_ptrs[i], TEST_LEN)) {
				printf("Fail rand " xstr(FUNCTION_UNDER_TEST) " test%d %d\n",
				       i, rtest);
				dump_matrix(buffs, vector, TEST_SOURCES);
				printf("dprod_base:");
				dump(dest_ref[i], 25);
				printf("dprod_dut:");
				dump(dest_ptrs[i], 25);
				return -1;
			}
		}

#ifdef TEST_VERBOSE
		putchar('.');
#endif
	}

	// Rand data test with varied parameters
	for (rtest = 0; rtest < RANDOMS; rtest++) {
		for (srcs = TEST_SOURCES; srcs > 0; srcs--) {
			for (i = 0; i < srcs; i++)
				for (j = 0; j < TEST_LEN; j++)
					buffs[i][j] = rand();

			for (i = 0; i < vector; i++)
				for (j = 0; j < srcs; j++) {
					gf[i][j] = rand();
					gf_vect_mul_init(gf[i][j],
							 &g_tbls[i * (32 * srcs) + j * 32]);
				}

			for (i = 0; i < vector; i++)
				gf_vect_dot_prod_base(TEST_LEN, srcs, &g_tbls[i * 32 * srcs],
						      buffs, dest_ref[i]);

			for (i = 0; i < vector; i++)
				memset(dest_ptrs[i], 0, TEST_LEN);
			for (i = 0; i < srcs; i++) {
#if (VECT == 1)
				FUNCTION_UNDER_TEST(TEST_LEN, srcs, i, g_tbls, buffs[i],
						    *dest_ptrs);
#else
				FUNCTION_UNDER_TEST(TEST_LEN, srcs, i, g_tbls, buffs[i],
						    dest_ptrs);
#endif

			}
			for (i = 0; i < vector; i++) {
				if (0 != memcmp(dest_ref[i], dest_ptrs[i], TEST_LEN)) {
					printf("Fail rand " xstr(FUNCTION_UNDER_TEST)
					       " test%d srcs=%d\n", i, srcs);
					dump_matrix(buffs, vector, TEST_SOURCES);
					printf("dprod_base:");
					dump(dest_ref[i], 25);
					printf("dprod_dut:");
					dump(dest_ptrs[i], 25);
					return -1;
				}
			}

#ifdef TEST_VERBOSE
			putchar('.');
#endif
		}
	}

	// Run tests at end of buffer for Electric Fence
	align = (LEN_ALIGN_CHK_B != 0) ? 1 : ALIGN_SIZE;
	for (size = TEST_MIN_SIZE; size <= TEST_SIZE; size += align) {
		for (i = 0; i < TEST_SOURCES; i++)
			for (j = 0; j < TEST_LEN; j++)
				buffs[i][j] = rand();

		for (i = 0; i < TEST_SOURCES; i++)	// Line up TEST_SIZE from end
			efence_buffs[i] = buffs[i] + TEST_LEN - size;

		for (i = 0; i < vector; i++)
			for (j = 0; j < TEST_SOURCES; j++) {
				gf[i][j] = rand();
				gf_vect_mul_init(gf[i][j],
						 &g_tbls[i * (32 * TEST_SOURCES) + j * 32]);
			}

		for (i = 0; i < vector; i++)
			gf_vect_dot_prod_base(size, TEST_SOURCES,
					      &g_tbls[i * 32 * TEST_SOURCES], efence_buffs,
					      dest_ref[i]);

		for (i = 0; i < vector; i++)
			memset(dest_ptrs[i], 0, size);
		for (i = 0; i < TEST_SOURCES; i++) {
#if (VECT == 1)
			FUNCTION_UNDER_TEST(size, TEST_SOURCES, i, g_tbls, efence_buffs[i],
					    *dest_ptrs);
#else
			FUNCTION_UNDER_TEST(size, TEST_SOURCES, i, g_tbls, efence_buffs[i],
					    dest_ptrs);
#endif
		}
		for (i = 0; i < vector; i++) {
			if (0 != memcmp(dest_ref[i], dest_ptrs[i], size)) {
				printf("Fail rand " xstr(FUNCTION_UNDER_TEST)
				       " test%d size=%d\n", i, size);
				dump_matrix(buffs, vector, TEST_SOURCES);
				printf("dprod_base:");
				dump(dest_ref[i], TEST_MIN_SIZE + align);
				printf("dprod_dut:");
				dump(dest_ptrs[i], TEST_MIN_SIZE + align);
				return -1;
			}
		}

#ifdef TEST_VERBOSE
		putchar('.');
#endif
	}

	// Test rand ptr alignment if available

	for (rtest = 0; rtest < RANDOMS; rtest++) {
		size = (TEST_LEN - PTR_ALIGN_CHK_B) & ~(TEST_MIN_SIZE - 1);
		srcs = rand() % TEST_SOURCES;
		if (srcs == 0)
			continue;

		offset = (PTR_ALIGN_CHK_B != 0) ? 1 : PTR_ALIGN_CHK_B;
		// Add random offsets
		for (i = 0; i < srcs; i++)
			ubuffs[i] = buffs[i] + (rand() & (PTR_ALIGN_CHK_B - offset));

		for (i = 0; i < vector; i++) {
			udest_ptrs[i] = dest_ptrs[i] + (rand() & (PTR_ALIGN_CHK_B - offset));
			memset(dest_ptrs[i], 0, TEST_LEN);	// zero pad to check write-over
		}

		for (i = 0; i < srcs; i++)
			for (j = 0; j < size; j++)
				ubuffs[i][j] = rand();

		for (i = 0; i < vector; i++)
			for (j = 0; j < srcs; j++) {
				gf[i][j] = rand();
				gf_vect_mul_init(gf[i][j], &g_tbls[i * (32 * srcs) + j * 32]);
			}

		for (i = 0; i < vector; i++)
			gf_vect_dot_prod_base(size, srcs, &g_tbls[i * 32 * srcs], ubuffs,
					      dest_ref[i]);

		for (i = 0; i < srcs; i++) {
#if (VECT == 1)
			FUNCTION_UNDER_TEST(size, srcs, i, g_tbls, ubuffs[i], *udest_ptrs);
#else
			FUNCTION_UNDER_TEST(size, srcs, i, g_tbls, ubuffs[i], udest_ptrs);
#endif
		}
		for (i = 0; i < vector; i++) {
			if (0 != memcmp(dest_ref[i], udest_ptrs[i], size)) {
				printf("Fail rand " xstr(FUNCTION_UNDER_TEST)
				       " test%d ualign srcs=%d\n", i, srcs);
				dump_matrix(buffs, vector, TEST_SOURCES);
				printf("dprod_base:");
				dump(dest_ref[i], 25);
				printf("dprod_dut:");
				dump(udest_ptrs[i], 25);
				return -1;
			}
		}

		// Confirm that padding around dests is unchanged
		memset(dest_ref[0], 0, PTR_ALIGN_CHK_B);	// Make reference zero buff

		for (i = 0; i < vector; i++) {
			offset = udest_ptrs[i] - dest_ptrs[i];
			if (memcmp(dest_ptrs[i], dest_ref[0], offset)) {
				printf("Fail rand ualign pad1 start\n");
				return -1;
			}
			if (memcmp
			    (dest_ptrs[i] + offset + size, dest_ref[0],
			     PTR_ALIGN_CHK_B - offset)) {
				printf("Fail rand ualign pad1 end\n");
				return -1;
			}
		}

#ifdef TEST_VERBOSE
		putchar('.');
#endif
	}

	// Test all size alignment
	align = (LEN_ALIGN_CHK_B != 0) ? 1 : ALIGN_SIZE;

	for (size = TEST_LEN; size >= TEST_MIN_SIZE; size -= align) {
		for (i = 0; i < TEST_SOURCES; i++)
			for (j = 0; j < size; j++)
				buffs[i][j] = rand();

		for (i = 0; i < vector; i++) {
			for (j = 0; j < TEST_SOURCES; j++) {
				gf[i][j] = rand();
				gf_vect_mul_init(gf[i][j],
						 &g_tbls[i * (32 * TEST_SOURCES) + j * 32]);
			}
			memset(dest_ptrs[i], 0, TEST_LEN);	// zero pad to check write-over
		}

		for (i = 0; i < vector; i++)
			gf_vect_dot_prod_base(size, TEST_SOURCES,
					      &g_tbls[i * 32 * TEST_SOURCES], buffs,
					      dest_ref[i]);

		for (i = 0; i < TEST_SOURCES; i++) {
#if (VECT == 1)
			FUNCTION_UNDER_TEST(size, TEST_SOURCES, i, g_tbls, buffs[i],
					    *dest_ptrs);
#else
			FUNCTION_UNDER_TEST(size, TEST_SOURCES, i, g_tbls, buffs[i],
					    dest_ptrs);
#endif
		}
		for (i = 0; i < vector; i++) {
			if (0 != memcmp(dest_ref[i], dest_ptrs[i], size)) {
				printf("Fail rand " xstr(FUNCTION_UNDER_TEST)
				       " test%d ualign len=%d\n", i, size);
				dump_matrix(buffs, vector, TEST_SOURCES);
				printf("dprod_base:");
				dump(dest_ref[i], 25);
				printf("dprod_dut:");
				dump(dest_ptrs[i], 25);
				return -1;
			}
		}

#ifdef TEST_VERBOSE
		putchar('.');
#endif

	}

	printf("Pass\n");
	return 0;

}
