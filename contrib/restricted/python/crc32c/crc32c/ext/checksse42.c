/*
 * Intel SSE 4.2 instruction probing implementation
 *
 * ICRAR - International Centre for Radio Astronomy Research
 * (c) UWA - The University of Western Australia, 2014
 * Copyright by UWA (in the framework of the ICRAR)
 * All rights reserved
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 * MA 02111-1307  USA
 *
 */

#include "common.h"

#if defined(IS_INTEL)

#if defined(_MSC_VER)
#include <intrin.h>

int _crc32c_intel_probe(void)
{
	int info[4];
	__cpuid(info, 1);
	return (info[2] & (1 << 20)) != 0;
}

#elif defined(__GNUC__)
#include <cpuid.h>

int _crc32c_intel_probe(void)
{
	unsigned int eax, ebx, ecx = 0, edx;
	__get_cpuid(1, &eax, &ebx, &ecx, &edx);
	return (ecx & (1 << 20)) != 0;
}

#else
int _crc32c_intel_probe(void) {

	unsigned int ecx;

	asm ("movl $1, %%eax;"
	    "cpuid;"
	    "movl %%ecx, %0;"
		: "=r"(ecx) // outputs
		: // inputs
		: "eax", "ebx", "ecx", "edx"); // clobber

	return (ecx & (1 << 20)) != 0;
}
#endif // defined(_MSC_VER)

#endif // defined(IS_INTEL)