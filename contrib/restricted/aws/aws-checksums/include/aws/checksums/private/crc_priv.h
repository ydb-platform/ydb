#ifndef AWS_CHECKSUMS_PRIVATE_CRC_PRIV_H
#define AWS_CHECKSUMS_PRIVATE_CRC_PRIV_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#define AWS_CRC32_SIZE_BYTES 4

#include <aws/checksums/exports.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Computes CRC32 (Ethernet, gzip, et. al.) using a (slow) reference implementation. */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32_sw(const uint8_t *input, int length, uint32_t previousCrc32);

/* Computes the Castagnoli CRC32c (iSCSI) using a (slow) reference implementation. */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32c_sw(const uint8_t *input, int length, uint32_t previousCrc32c);

/* Computes the Castagnoli CRC32c (iSCSI). */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32c_hw(const uint8_t *data, int length, uint32_t previousCrc32);

/* Computes CRC32 (Ethernet, gzip, et. al.) using crc instructions. */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32_hw(const uint8_t *data, int length, uint32_t previousCrc32);

#ifdef __cplusplus
}
#endif

#endif /* AWS_CHECKSUMS_PRIVATE_CRC_PRIV_H */
