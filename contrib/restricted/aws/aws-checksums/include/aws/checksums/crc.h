#ifndef AWS_CHECKSUMS_CRC_H
#define AWS_CHECKSUMS_CRC_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/checksums/exports.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * The entry point function to perform a CRC32 (Ethernet, gzip) computation.
 * Selects a suitable implementation based on hardware capabilities.
 * Pass 0 in the previousCrc32 parameter as an initial value unless continuing
 * to update a running crc in a subsequent call.
 */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32(const uint8_t *input, int length, uint32_t previousCrc32);

/**
 * The entry point function to perform a Castagnoli CRC32c (iSCSI) computation.
 * Selects a suitable implementation based on hardware capabilities.
 * Pass 0 in the previousCrc32 parameter as an initial value unless continuing
 * to update a running crc in a subsequent call.
 */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32c(const uint8_t *input, int length, uint32_t previousCrc32);

#ifdef __cplusplus
}
#endif

#endif /* AWS_CHECKSUMS_CRC_H */
