/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#ifndef AWS_COMMON_RESOURCE_NAME_H
#define AWS_COMMON_RESOURCE_NAME_H
#pragma once

#include <aws/common/byte_buf.h>
#include <aws/common/common.h>

struct aws_resource_name {
    struct aws_byte_cursor partition;
    struct aws_byte_cursor service;
    struct aws_byte_cursor region;
    struct aws_byte_cursor account_id;
    struct aws_byte_cursor resource_id;
};

AWS_EXTERN_C_BEGIN

/**
    Given an ARN "Amazon Resource Name" represented as an in memory a
    structure representing the parts
*/
AWS_COMMON_API
int aws_resource_name_init_from_cur(struct aws_resource_name *arn, const struct aws_byte_cursor *input);

/**
    Calculates the space needed to write an ARN to a byte buf
*/
AWS_COMMON_API
int aws_resource_name_length(const struct aws_resource_name *arn, size_t *size);

/**
    Serializes an ARN structure into the lexical string format
*/
AWS_COMMON_API
int aws_byte_buf_append_resource_name(struct aws_byte_buf *buf, const struct aws_resource_name *arn);

AWS_EXTERN_C_END

#endif /* AWS_COMMON_RESOURCE_NAME_H */
