/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/checksums/crc.h>
#include <aws/checksums/private/crc_priv.h>

#include <aws/common/cpuid.h>

static uint32_t (*s_crc32c_fn_ptr)(const uint8_t *input, int length, uint32_t previousCrc32) = 0;
static uint32_t (*s_crc32_fn_ptr)(const uint8_t *input, int length, uint32_t previousCrc32) = 0;

uint32_t aws_checksums_crc32(const uint8_t *input, int length, uint32_t previousCrc32) {
    if (AWS_UNLIKELY(!s_crc32_fn_ptr)) {
        if (aws_cpu_has_feature(AWS_CPU_FEATURE_ARM_CRC)) {
            s_crc32_fn_ptr = aws_checksums_crc32_hw;
        } else {
            s_crc32_fn_ptr = aws_checksums_crc32_sw;
        }
    }
    return s_crc32_fn_ptr(input, length, previousCrc32);
}

uint32_t aws_checksums_crc32c(const uint8_t *input, int length, uint32_t previousCrc32) {
    if (AWS_UNLIKELY(!s_crc32c_fn_ptr)) {
        if (aws_cpu_has_feature(AWS_CPU_FEATURE_SSE_4_2) || aws_cpu_has_feature(AWS_CPU_FEATURE_ARM_CRC)) {
            s_crc32c_fn_ptr = aws_checksums_crc32c_hw;
        } else {
            s_crc32c_fn_ptr = aws_checksums_crc32c_sw;
        }
    }
    return s_crc32c_fn_ptr(input, length, previousCrc32);
}
