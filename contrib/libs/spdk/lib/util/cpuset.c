#include <contrib/libs/spdk/ndebug.h>
/*-
 *   BSD LICENSE
 *
 *   Copyright(c) Intel Corporation. All rights reserved.
 *   All rights reserved.
 *
 *   Redistribution and use in source and binary forms, with or without
 *   modification, are permitted provided that the following conditions
 *   are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *     * Neither the name of Intel Corporation nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 *   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 *   "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 *   LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 *   A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 *   OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 *   SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 *   LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 *   DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 *   THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 *   (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 *   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#include "spdk/cpuset.h"
#include "spdk/log.h"

struct spdk_cpuset *
spdk_cpuset_alloc(void)
{
	return (struct spdk_cpuset *)calloc(sizeof(struct spdk_cpuset), 1);
}

void
spdk_cpuset_free(struct spdk_cpuset *set)
{
	free(set);
}

bool
spdk_cpuset_equal(const struct spdk_cpuset *set1, const struct spdk_cpuset *set2)
{
	assert(set1 != NULL);
	assert(set2 != NULL);
	return memcmp(set1->cpus, set2->cpus, sizeof(set2->cpus)) == 0;
}

void
spdk_cpuset_copy(struct spdk_cpuset *dst, const struct spdk_cpuset *src)
{
	assert(dst != NULL);
	assert(src != NULL);
	memcpy(&dst->cpus, &src->cpus, sizeof(src->cpus));
}

void
spdk_cpuset_negate(struct spdk_cpuset *set)
{
	unsigned int i;
	assert(set != NULL);
	for (i = 0; i < sizeof(set->cpus); i++) {
		set->cpus[i] = ~set->cpus[i];
	}
}

void
spdk_cpuset_and(struct spdk_cpuset *dst, const struct spdk_cpuset *src)
{
	unsigned int i;
	assert(dst != NULL);
	assert(src != NULL);
	for (i = 0; i < sizeof(src->cpus); i++) {
		dst->cpus[i] &= src->cpus[i];
	}
}

void
spdk_cpuset_or(struct spdk_cpuset *dst, const struct spdk_cpuset *src)
{
	unsigned int i;
	assert(dst != NULL);
	assert(src != NULL);
	for (i = 0; i < sizeof(src->cpus); i++) {
		dst->cpus[i] |= src->cpus[i];
	}
}

void
spdk_cpuset_xor(struct spdk_cpuset *dst, const struct spdk_cpuset *src)
{
	unsigned int i;
	assert(dst != NULL);
	assert(src != NULL);
	for (i = 0; i < sizeof(src->cpus); i++) {
		dst->cpus[i] ^= src->cpus[i];
	}
}

void
spdk_cpuset_zero(struct spdk_cpuset *set)
{
	assert(set != NULL);
	memset(set->cpus, 0, sizeof(set->cpus));
}

void
spdk_cpuset_set_cpu(struct spdk_cpuset *set, uint32_t cpu, bool state)
{
	assert(set != NULL);
	assert(cpu < sizeof(set->cpus) * 8);
	if (state) {
		set->cpus[cpu / 8] |= (1U << (cpu % 8));
	} else {
		set->cpus[cpu / 8] &= ~(1U << (cpu % 8));
	}
}

bool
spdk_cpuset_get_cpu(const struct spdk_cpuset *set, uint32_t cpu)
{
	assert(set != NULL);
	assert(cpu < sizeof(set->cpus) * 8);
	return (set->cpus[cpu / 8] >> (cpu % 8)) & 1U;
}

uint32_t
spdk_cpuset_count(const struct spdk_cpuset *set)
{
	uint32_t count = 0;
	uint8_t n;
	unsigned int i;
	for (i = 0; i < sizeof(set->cpus); i++) {
		n = set->cpus[i];
		while (n) {
			n &= (n - 1);
			count++;
		}
	}
	return count;
}

const char *
spdk_cpuset_fmt(struct spdk_cpuset *set)
{
	uint32_t lcore, lcore_max = 0;
	int val, i, n;
	char *ptr;
	static const char *hex = "0123456789abcdef";

	assert(set != NULL);

	for (lcore = 0; lcore < sizeof(set->cpus) * 8; lcore++) {
		if (spdk_cpuset_get_cpu(set, lcore)) {
			lcore_max = lcore;
		}
	}

	ptr = set->str;
	n = lcore_max / 8;
	val = set->cpus[n];

	/* Store first number only if it is not leading zero */
	if ((val & 0xf0) != 0) {
		*(ptr++) = hex[(val & 0xf0) >> 4];
	}
	*(ptr++) = hex[val & 0x0f];

	for (i = n - 1; i >= 0; i--) {
		val = set->cpus[i];
		*(ptr++) = hex[(val & 0xf0) >> 4];
		*(ptr++) = hex[val & 0x0f];
	}
	*ptr = '\0';

	return set->str;
}

static int
hex_value(uint8_t c)
{
#define V(x, y) [x] = y + 1
	static const int8_t val[256] = {
		V('0', 0), V('1', 1), V('2', 2), V('3', 3), V('4', 4),
		V('5', 5), V('6', 6), V('7', 7), V('8', 8), V('9', 9),
		V('A', 0xA), V('B', 0xB), V('C', 0xC), V('D', 0xD), V('E', 0xE), V('F', 0xF),
		V('a', 0xA), V('b', 0xB), V('c', 0xC), V('d', 0xD), V('e', 0xE), V('f', 0xF),
	};
#undef V

	return val[c] - 1;
}

static int
parse_list(const char *mask, struct spdk_cpuset *set)
{
	char *end;
	const char *ptr = mask;
	uint32_t lcore;
	uint32_t lcore_min, lcore_max;

	spdk_cpuset_zero(set);
	lcore_min = UINT32_MAX;

	ptr++;
	end = (char *)ptr;
	do {
		while (isblank(*ptr)) {
			ptr++;
		}
		if (*ptr == '\0' || *ptr == ']' || *ptr == '-' || *ptr == ',') {
			goto invalid_character;
		}

		errno = 0;
		lcore = strtoul(ptr, &end, 10);
		if (errno) {
			SPDK_ERRLOG("Conversion of core mask in '%s' failed\n", mask);
			return -1;
		}

		if (lcore >= sizeof(set->cpus) * 8) {
			SPDK_ERRLOG("Core number %" PRIu32 " is out of range in '%s'\n", lcore, mask);
			return -1;
		}

		while (isblank(*end)) {
			end++;
		}

		if (*end == '-') {
			lcore_min = lcore;
		} else if (*end == ',' || *end == ']') {
			lcore_max = lcore;
			if (lcore_min == UINT32_MAX) {
				lcore_min = lcore;
			}
			if (lcore_min > lcore_max) {
				SPDK_ERRLOG("Invalid range of CPUs (%" PRIu32 " > %" PRIu32 ")\n",
					    lcore_min, lcore_max);
				return -1;
			}
			for (lcore = lcore_min; lcore <= lcore_max; lcore++) {
				spdk_cpuset_set_cpu(set, lcore, true);
			}
			lcore_min = UINT32_MAX;
		} else {
			goto invalid_character;
		}

		ptr = end + 1;

	} while (*end != ']');

	return 0;

invalid_character:
	if (*end == '\0') {
		SPDK_ERRLOG("Unexpected end of core list '%s'\n", mask);
	} else {
		SPDK_ERRLOG("Parsing of core list '%s' failed on character '%c'\n", mask, *end);
	}
	return -1;
}

static int
parse_mask(const char *mask, struct spdk_cpuset *set, size_t len)
{
	int i, j;
	char c;
	int val;
	uint32_t lcore = 0;

	if (mask[0] == '0' && (mask[1] == 'x' || mask[1] == 'X')) {
		mask += 2;
		len -= 2;
	}

	spdk_cpuset_zero(set);
	for (i = len - 1; i >= 0; i--) {
		c = mask[i];
		val = hex_value(c);
		if (val < 0) {
			/* Invalid character */
			SPDK_ERRLOG("Invalid character in core mask '%s' (%c)\n", mask, c);
			return -1;
		}
		for (j = 0; j < 4 && lcore < SPDK_CPUSET_SIZE; j++, lcore++) {
			if ((1 << j) & val) {
				spdk_cpuset_set_cpu(set, lcore, true);
			}
		}
	}

	return 0;
}

int
spdk_cpuset_parse(struct spdk_cpuset *set, const char *mask)
{
	int ret;
	size_t len;

	if (mask == NULL || set == NULL) {
		return -1;
	}

	while (isblank(*mask)) {
		mask++;
	}

	len = strlen(mask);
	while (len > 0 && isblank(mask[len - 1])) {
		len--;
	}

	if (len == 0) {
		return -1;
	}

	if (mask[0] == '[') {
		ret = parse_list(mask, set);
	} else {
		ret = parse_mask(mask, set, len);
	}

	return ret;
}
