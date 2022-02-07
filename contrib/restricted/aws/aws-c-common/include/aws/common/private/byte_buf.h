#ifndef AWS_COMMON_PRIVATE_BYTE_BUF_H
#define AWS_COMMON_PRIVATE_BYTE_BUF_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>

/**
 * If index >= bound, bound > (SIZE_MAX / 2), or index > (SIZE_MAX / 2), returns
 * 0. Otherwise, returns UINTPTR_MAX.  This function is designed to return the correct
 * value even under CPU speculation conditions, and is intended to be used for
 * SPECTRE mitigation purposes.
 */
AWS_COMMON_API size_t aws_nospec_mask(size_t index, size_t bound);

#endif /* AWS_COMMON_PRIVATE_BYTE_BUF_H */
