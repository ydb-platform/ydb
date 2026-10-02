#ifndef AWS_CHECKSUMS_CRC_H
#define AWS_CHECKSUMS_CRC_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/checksums/exports.h>
#include <aws/common/macros.h>
#include <aws/common/stdint.h>

AWS_PUSH_SANE_WARNING_LEVEL
AWS_EXTERN_C_BEGIN

/**
 * The entry point function to perform a CRC32 (Ethernet, gzip) computation.
 * Selects a suitable implementation based on hardware capabilities.
 * Pass 0 in the previousCrc32 parameter as an initial value unless continuing
 * to update a running crc in a subsequent call.
 */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32(const uint8_t *input, int length, uint32_t previous_crc32);

/**
 * The entry point function to perform a CRC32 (Ethernet, gzip) computation.
 * Supports buffer lengths up to size_t max.
 * Selects a suitable implementation based on hardware capabilities.
 * Pass 0 in the previousCrc32 parameter as an initial value unless continuing
 * to update a running crc in a subsequent call.
 */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32_ex(const uint8_t *input, size_t length, uint32_t previous_crc32);

/**
 * The entry point function to perform a Castagnoli CRC32c (iSCSI) computation.
 * Selects a suitable implementation based on hardware capabilities.
 * Pass 0 in the previousCrc32 parameter as an initial value unless continuing
 * to update a running crc in a subsequent call.
 */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32c(const uint8_t *input, int length, uint32_t previous_crc32c);

/**
 * The entry point function to perform a Castagnoli CRC32c (iSCSI) computation.
 * Supports buffer lengths up to size_t max.
 * Selects a suitable implementation based on hardware capabilities.
 * Pass 0 in the previousCrc32 parameter as an initial value unless continuing
 * to update a running crc in a subsequent call.
 */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32c_ex(const uint8_t *input, size_t length, uint32_t previous_crc32c);

/**
 * The entry point function to perform a CRC64-NVME (a.k.a. CRC64-Rocksoft) computation.
 * Selects a suitable implementation based on hardware capabilities.
 * Pass 0 in the previousCrc64 parameter as an initial value unless continuing
 * to update a running crc in a subsequent call.
 * There are many variants of CRC64 algorithms. This CRC64 variant is bit-reflected (based on
 * the non bit-reflected polynomial 0xad93d23594c93659) and inverts the CRC input and output bits.
 */
AWS_CHECKSUMS_API uint64_t aws_checksums_crc64nvme(const uint8_t *input, int length, uint64_t previous_crc64);

/**
 * The entry point function to perform a CRC64-NVME (a.k.a. CRC64-Rocksoft) computation.
 * Supports buffer lengths up to size_t max.
 * Selects a suitable implementation based on hardware capabilities.
 * Pass 0 in the previousCrc64 parameter as an initial value unless continuing
 * to update a running crc in a subsequent call.
 * There are many variants of CRC64 algorithms. This CRC64 variant is bit-reflected (based on
 * the non bit-reflected polynomial 0xad93d23594c93659) and inverts the CRC input and output bits.
 */
AWS_CHECKSUMS_API uint64_t aws_checksums_crc64nvme_ex(const uint8_t *input, size_t length, uint64_t previous_crc64);

/**
 * Combines two CRC32 (Ethernet, gzip) checksums computed over separate data blocks.
 * This is equivalent to computing the CRC32 of the concatenated data blocks without
 * having to re-scan the data.
 *
 * Given:
 *   crc1 = CRC32(data_block_A)
 *   crc2 = CRC32(data_block_B)
 *
 * This function computes:
 *   result = CRC32(data_block_A || data_block_B)
 *
 * @param crc1 The CRC32 checksum of the first data block
 * @param crc2 The CRC32 checksum of the second data block
 * @param len2 The length (in bytes) of the original data that produced crc2.
 *             This is NOT the size of the checksum (which is always 4 bytes),
 *             but rather the size of the data block that was checksummed.
 * @return The combined CRC32 checksum as if computed over the concatenated data
 */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32_combine(uint32_t crc1, uint32_t crc2, uint64_t len2);

/**
 * Combines two CRC32C (Castagnoli, iSCSI) checksums computed over separate data blocks.
 * This is equivalent to computing the CRC32C of the concatenated data blocks without
 * having to re-scan the data.
 *
 * Given:
 *   crc1 = CRC32C(data_block_A)
 *   crc2 = CRC32C(data_block_B)
 *
 * This function computes:
 *   result = CRC32C(data_block_A || data_block_B)
 *
 * @param crc1 The CRC32C checksum of the first data block
 * @param crc2 The CRC32C checksum of the second data block
 * @param len2 The length (in bytes) of the original data that produced crc2.
 *             This is NOT the size of the checksum (which is always 4 bytes),
 *             but rather the size of the data block that was checksummed.
 * @return The combined CRC32C checksum as if computed over the concatenated data
 */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32c_combine(uint32_t crc1, uint32_t crc2, uint64_t len2);

/**
 * Combines two CRC64-NVME (CRC64-Rocksoft) checksums computed over separate data blocks.
 * This is equivalent to computing the CRC64-NVME of the concatenated data blocks without
 * having to re-scan the data.
 *
 * Given:
 *   crc1 = CRC64_NVME(data_block_A)
 *   crc2 = CRC64_NVME(data_block_B)
 *
 * This function computes:
 *   result = CRC64_NVME(data_block_A || data_block_B)
 *
 * @param crc1 The CRC64-NVME checksum of the first data block
 * @param crc2 The CRC64-NVME checksum of the second data block
 * @param len2 The length (in bytes) of the original data that produced crc2.
 *             This is NOT the size of the checksum (which is always 8 bytes),
 *             but rather the size of the data block that was checksummed.
 * @return The combined CRC64-NVME checksum as if computed over the concatenated data
 */
AWS_CHECKSUMS_API uint64_t aws_checksums_crc64nvme_combine(uint64_t crc1, uint64_t crc2, uint64_t len2);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_CHECKSUMS_CRC_H */
