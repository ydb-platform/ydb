/*
 * Linux-specific AArch64 CRC32 instruction probing implementation
 *
 * ICRAR - International Centre for Radio Astronomy Research
 * (c) UWA - The University of Western Australia, 2020
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

#if defined(IS_ARM) && (defined(__linux__) || defined(linux))

#include <sys/auxv.h>

#ifndef HWCAP_CRC32
/* see arch/arm64/include/uapi/asm/hwcap.h */
#define HWCAP_CRC32 (1 << 7)
#endif

int _crc32c_arm64_probe(void)
{
	unsigned long auxval;
	auxval = getauxval(AT_HWCAP);
	return (auxval & HWCAP_CRC32) != 0;
}

#endif
