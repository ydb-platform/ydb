#ifndef AWS_CHECKSUMS_PRIVATE_CRC_PRIV_H
#define AWS_CHECKSUMS_PRIVATE_CRC_PRIV_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#define AWS_CRC32_SIZE_BYTES 4
#include <aws/checksums/exports.h>
#include <aws/common/common.h>

#include <aws/common/config.h>
#include <stdint.h>

AWS_EXTERN_C_BEGIN

/* Computes CRC32 (Ethernet, gzip, et. al.) using a (slow) reference implementation. */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32_sw(const uint8_t *input, int length, uint32_t previousCrc32);

/* Computes the Castagnoli CRC32c (iSCSI) using a (slow) reference implementation. */
AWS_CHECKSUMS_API uint32_t aws_checksums_crc32c_sw(const uint8_t *input, int length, uint32_t previousCrc32c);

AWS_CHECKSUMS_API uint32_t aws_checksums_crc32_combine_sw(uint32_t crc1, uint32_t crc2, uint64_t len);

AWS_CHECKSUMS_API uint32_t aws_checksums_crc32c_combine_sw(uint32_t crc1, uint32_t crc2, uint64_t len);

#if defined(AWS_USE_CPU_EXTENSIONS) && defined(AWS_ARCH_ARM64)
uint32_t aws_checksums_crc32_armv8(const uint8_t *input, int length, uint32_t previous_crc32);
uint32_t aws_checksums_crc32c_armv8(const uint8_t *input, int length, uint32_t previous_crc32c);
#elif defined(AWS_USE_CPU_EXTENSIONS) && defined(AWS_ARCH_INTEL)
#    if defined(AWS_ARCH_INTEL_X64)
typedef uint64_t *slice_ptr_type;
typedef uint64_t slice_ptr_int_type;
#        define crc_intrin_fn _mm_crc32_u64

#        if !defined(_MSC_VER)
uint32_t aws_checksums_crc32c_clmul_sse42(const uint8_t *data, int length, uint32_t previous_crc32c);
#        endif

#    else
typedef uint32_t *slice_ptr_type;
typedef uint32_t slice_ptr_int_type;
#        define crc_intrin_fn _mm_crc32_u32
#    endif
uint32_t aws_checksums_crc32c_intel_avx512_with_sse_fallback(
    const uint8_t *input,
    int length,
    uint32_t previous_crc32c);

#endif

typedef struct {
    uint64_t mu_poly[2]; /* Barrett mu / polynomial P(x) */
    uint64_t shift_factors[16][16][2];
} aws_checksums_crc32_constants_t;

extern aws_checksums_crc32_constants_t aws_checksums_crc32_constants;
extern aws_checksums_crc32_constants_t aws_checksums_crc32c_constants;

AWS_EXTERN_C_END

#endif /* AWS_CHECKSUMS_PRIVATE_CRC_PRIV_H */
