#ifndef AWS_CHECKSUMS_CHECKSUMS_H
#define AWS_CHECKSUMS_CHECKSUMS_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/checksums/exports.h>
#include <aws/common/common.h>

AWS_PUSH_SANE_WARNING_LEVEL
AWS_EXTERN_C_BEGIN

/**
 * Initializes internal data structures used by aws-checksums.
 * MUST be called before using any functionality in aws-checksums.
 * Note: historically aws-checksums lazily initialized stuff and things worked without init.
 * However, DO NOT rely on that behavior and explicitly call init instead.
 */
AWS_CHECKSUMS_API void aws_checksums_library_init(struct aws_allocator *allocator);

/**
 * Shuts down the internal data structures used by aws-checksums.
 */
AWS_CHECKSUMS_API void aws_checksums_library_clean_up(void);

AWS_EXTERN_C_END
AWS_POP_SANE_WARNING_LEVEL

#endif /* AWS_CHECKSUMS_CHECKSUMS_H */
