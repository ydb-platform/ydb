#ifndef AWS_CHECKSUMS_PRIVATE_XXHASH_PRIV_H
#define AWS_CHECKSUMS_PRIVATE_XXHASH_PRIV_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/checksums/exports.h>
#include <aws/common/macros.h>

AWS_EXTERN_C_BEGIN

void aws_checksums_xxhash_init(struct aws_allocator *allocator);

AWS_EXTERN_C_END

#endif /* AWS_CHECKSUMS_PRIVATE_XXHASH_PRIV_H */
