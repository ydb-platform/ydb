/*
 * Definition of crc32c checksum implementation
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

#ifndef _CRC32C_H_
#define _CRC32C_H_

#include "common.h"

void crc32c_init_hw_adler(void);
uint32_t _crc32c_hw_arm64(uint32_t crc, const unsigned char* data, unsigned long length);
uint32_t _crc32c_hw_adler(uint32_t crc, const unsigned char* data, unsigned long length);
uint32_t _crc32c_sw_slicing_by_8(uint32_t crc, const unsigned char *data, unsigned long length);

#endif
