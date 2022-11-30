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
 * CRC-32 utility functions
 */

#ifndef SPDK_CRC32_H
#define SPDK_CRC32_H

#include "spdk/stdinc.h"
#include "spdk/config.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Calculate a partial CRC-32 IEEE checksum.
 *
 * \param buf Data buffer to checksum.
 * \param len Length of buf in bytes.
 * \param crc Previous CRC-32 value.
 * \return Updated CRC-32 value.
 */
uint32_t spdk_crc32_ieee_update(const void *buf, size_t len, uint32_t crc);

/**
 * Calculate a partial CRC-32C checksum.
 *
 * \param buf Data buffer to checksum.
 * \param len Length of buf in bytes.
 * \param crc Previous CRC-32C value.
 * \return Updated CRC-32C value.
 */
uint32_t spdk_crc32c_update(const void *buf, size_t len, uint32_t crc);

#ifdef __cplusplus
}
#endif

#endif /* SPDK_CRC32_H */
