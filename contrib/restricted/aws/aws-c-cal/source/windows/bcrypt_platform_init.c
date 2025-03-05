/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/allocator.h>

void aws_cal_platform_init(struct aws_allocator *allocator) {
    (void)allocator;
}

void aws_cal_platform_clean_up(void) {}
