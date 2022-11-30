/*-
 *   BSD LICENSE
 *
 *   Copyright (c) Intel Corporation.
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

/**
 * \file
 * CRC-16 utility functions
 */

#ifndef SPDK_CRC16_H
#define SPDK_CRC16_H

#include "spdk/stdinc.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * T10-DIF CRC-16 polynomial
 */
#define SPDK_T10DIF_CRC16_POLYNOMIAL 0x8bb7u

/**
 * Calculate T10-DIF CRC-16 checksum.
 *
 * \param init_crc Initial CRC-16 value.
 * \param buf Data buffer to checksum.
 * \param len Length of buf in bytes.
 * \return CRC-16 value.
 */
uint16_t spdk_crc16_t10dif(uint16_t init_crc, const void *buf, size_t len);

/**
 * Calculate T10-DIF CRC-16 checksum and copy data.
 *
 * \param init_crc Initial CRC-16 value.
 * \param dst Destination data buffer for copy.
 * \param src Source data buffer for CRC calculation and copy.
 * \param len Length of buffer in bytes.
 * \return CRC-16 value.
 */
uint16_t spdk_crc16_t10dif_copy(uint16_t init_crc, uint8_t *dst, uint8_t *src,
				size_t len);
#ifdef __cplusplus
}
#endif

#endif /* SPDK_CRC16_H */
