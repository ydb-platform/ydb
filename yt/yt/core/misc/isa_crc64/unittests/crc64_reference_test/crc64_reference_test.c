/**********************************************************************
  Copyright(c) 2011-2016 Intel Corporation All rights reserved.
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

#include <yt/yt/core/misc/isa_crc64/crc64_yt_norm_refs.c>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#ifndef TEST_SEED
# define TEST_SEED 0x1234
#endif

#define MAX_BUF   4096
#define TEST_SIZE  32


////////////////////////////////////////////////////////////////////////////////

/*
    This file is a patched version of `crc64_funcs_test.c` from ISA-l.
    
    Here we test that 3 implementations of CRC64 (fast assembly, slow with lookup table and very slow)
    with YT-specific polynomial all give same result.
*/

////////////////////////////////////////////////////////////////////////////////

uint64_t crc64_yt_norm_ref(
    uint64_t init_crc,
    const unsigned char *buf,
    uint64_t len);

uint64_t crc64_yt_norm_base(
    uint64_t init_crc,
    const unsigned char *buf,
    uint64_t len);

uint64_t crc64_yt_norm_by8(
    uint64_t init_crc,
    const unsigned char *buf,
    uint64_t len);

////////////////////////////////////////////////////////////////////////////////

// Generates pseudo-random data

void rand_buffer(unsigned char *buf, long buffer_size)
{
	long i;
	for (i = 0; i < buffer_size; i++)
		buf[i] = rand();
}

// Test cases
int zeros_test();
int simple_pattern_test();
int seeds_sizes_test();
int eob_test();
int update_test();

int verbose = 0;
void *buf_alloc = NULL;

int main(int argc, char *argv[])
{
	int fail = 0, fail_case = 0;
	int i, ret;

	verbose = argc - 1;

	// Align to 32B boundary
	ret = posix_memalign(&buf_alloc, TEST_SIZE, MAX_BUF * TEST_SIZE);
	if (ret) {
		printf("alloc error: Fail");
		return -1;
	}
	srand(TEST_SEED);
	printf("CRC64 Tests\n");

    fail_case += zeros_test();
    fail_case += simple_pattern_test();
    fail_case += seeds_sizes_test();
    fail_case += eob_test();
    fail_case += update_test();
    printf(" done: %s\n", fail_case ? "Fail" : "Pass");

    if (fail_case) {
        printf("\nFailed %d tests\n", fail_case);
        fail++;
    }

	printf("CRC64 Tests all done: %s\n", fail ? "Fail" : "Pass");

	return fail;
}

// Test of all zeros
int zeros_test()
{
	uint64_t crc_ref, crc_base, crc;
	int fail = 0;
	unsigned char *buf = NULL;

	buf = (unsigned char *)buf_alloc;
	memset(buf, 0, MAX_BUF * 10);
	crc_ref = crc64_yt_norm_ref(TEST_SEED, buf, MAX_BUF * 10);
	crc_base = crc64_yt_norm_base(TEST_SEED, buf, MAX_BUF * 10);
	crc = crc64_yt_norm_by8(TEST_SEED, buf, MAX_BUF * 10);

	if ((crc_base != crc_ref) || (crc != crc_ref)) {
		fail++;
		printf("\n		   opt   ref\n");
		printf("		 ------ ------\n");
		printf("crc	zero = 0x%16lx 0x%16lx 0x%16lx \n", crc_ref, crc_base, crc);
	} else
		printf(".");

	return fail;
}

// Another simple test pattern
int simple_pattern_test()
{
	uint64_t crc_ref, crc_base, crc;
	int fail = 0;
	unsigned char *buf = NULL;

	buf = (unsigned char *)buf_alloc;
	memset(buf, 0x8a, MAX_BUF);
	crc_ref = crc64_yt_norm_ref(TEST_SEED, buf, MAX_BUF);
	crc_base = crc64_yt_norm_base(TEST_SEED, buf, MAX_BUF);
	crc = crc64_yt_norm_by8(TEST_SEED, buf, MAX_BUF);

	if ((crc_base != crc_ref) || (crc != crc_ref))
		fail++;
	if (verbose)
		printf("crc  all 8a = 0x%16lx 0x%16lx 0x%16lx\n", crc_ref, crc_base, crc);
	else
		printf(".");

	return fail;
}

int seeds_sizes_test()
{
	uint64_t crc_ref, crc_base, crc;
	int fail = 0;
	int i;
	uint64_t r, s;
	unsigned char *buf = NULL;

	// Do a few random tests
	buf = (unsigned char *)buf_alloc;	//reset buf
	r = rand();
	rand_buffer(buf, MAX_BUF * TEST_SIZE);

	for (i = 0; i < TEST_SIZE; i++) {
		crc_ref = crc64_yt_norm_ref(r, buf, MAX_BUF);
		crc_base = crc64_yt_norm_base(r, buf, MAX_BUF);
		crc = crc64_yt_norm_by8(r, buf, MAX_BUF);

		if ((crc_base != crc_ref) || (crc != crc_ref))
			fail++;
		if (verbose)
			printf("crc rand%3d = 0x%16lx 0x%16lx 0x%16lx\n", i, crc_ref, crc_base,
			       crc);
		else if (i % (TEST_SIZE / 8) == 0)
			printf(".");
		buf += MAX_BUF;
	}

	// Do a few random sizes
	buf = (unsigned char *)buf_alloc;	//reset buf
	r = rand();

	for (i = MAX_BUF; i >= 0; i--) {
		crc_ref = crc64_yt_norm_ref(r, buf, i);
		crc_base = crc64_yt_norm_base(r, buf, i);
		crc = crc64_yt_norm_by8(r, buf, i);

		if ((crc_base != crc_ref) || (crc != crc_ref)) {
			fail++;
			printf("fail random size%i 0x%16lx 0x%16lx 0x%16lx\n", i, crc_ref,
			       crc_base, crc);
		} else if (i % (MAX_BUF / 8) == 0)
			printf(".");
	}

	// Try different seeds
	for (s = 0; s < 20; s++) {
		buf = (unsigned char *)buf_alloc;	//reset buf

		r = rand();	// just to get a new seed
		rand_buffer(buf, MAX_BUF * TEST_SIZE);	// new pseudo-rand data

		if (verbose)
			printf("seed = 0x%lx\n", r);

		for (i = 0; i < TEST_SIZE; i++) {
			crc_ref = crc64_yt_norm_ref(r, buf, MAX_BUF);
			crc_base = crc64_yt_norm_base(r, buf, MAX_BUF);
			crc = crc64_yt_norm_by8(r, buf, MAX_BUF);

			if ((crc_base != crc_ref) || (crc != crc_ref))
				fail++;
			if (verbose)
				printf("crc rand%3d = 0x%16lx 0x%16lx 0x%16lx\n", i, crc_ref,
				       crc_base, crc);
			else if (i % (TEST_SIZE * 20 / 8) == 0)
				printf(".");
			buf += MAX_BUF;
		}
	}

	return fail;
}

// Run tests at end of buffer
int eob_test()
{
	uint64_t crc_ref, crc_base, crc;
	int fail = 0;
	int i;
	unsigned char *buf = NULL;

	// Null test
	if (0 != crc64_yt_norm_by8(0, NULL, 0)) {
		fail++;
		printf("crc null test fail\n");
	}

	buf = (unsigned char *)buf_alloc;	//reset buf
	buf = buf + ((MAX_BUF - 1) * TEST_SIZE);	//Line up TEST_SIZE from end
	for (i = 0; i <= TEST_SIZE; i++) {
		crc_ref = crc64_yt_norm_ref(TEST_SEED, buf + i, TEST_SIZE - i);
		crc_base = crc64_yt_norm_base(TEST_SEED, buf + i, TEST_SIZE - i);
		crc = crc64_yt_norm_by8(TEST_SEED, buf + i, TEST_SIZE - i);

		if ((crc_base != crc_ref) || (crc != crc_ref))
			fail++;
		if (verbose)
			printf("crc eob rand%3d = 0x%16lx 0x%16lx 0x%16lx\n", i, crc_ref,
			       crc_base, crc);
		else if (i % (TEST_SIZE / 8) == 0)
			printf(".");
	}

	return fail;
}

int update_test()
{
	uint64_t crc_ref, crc_base, crc;
	int fail = 0;
	int i;
	uint64_t r;
	unsigned char *buf = NULL;

	buf = (unsigned char *)buf_alloc;	//reset buf
	r = rand();
	// Process the whole buf with reference func single call.
	crc_ref = crc64_yt_norm_ref(r, buf, MAX_BUF * TEST_SIZE);
	crc_base = crc64_yt_norm_base(r, buf, MAX_BUF * TEST_SIZE);
	// Process buf with update method.
	for (i = 0; i < TEST_SIZE; i++) {
		crc = crc64_yt_norm_by8(r, buf, MAX_BUF);
		// Update crc seeds and buf pointer.
		r = crc;
		buf += MAX_BUF;
	}

	if ((crc_base != crc_ref) || (crc != crc_ref))
		fail++;
	if (verbose)
		printf("crc rand%3d = 0x%16lx 0x%16lx 0x%16lx\n", i, crc_ref, crc_base, crc);
	else
		printf(".");

	return fail;
}

////////////////////////////////////////////////////////////////////////////////
