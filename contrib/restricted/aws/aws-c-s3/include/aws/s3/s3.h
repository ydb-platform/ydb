#ifndef AWS_S3_H
#define AWS_S3_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/common.h>
#include <aws/io/logging.h>
#include <aws/s3/exports.h>

#define AWS_C_S3_PACKAGE_ID 14

enum aws_s3_errors {
    AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER = AWS_ERROR_ENUM_BEGIN_RANGE(AWS_C_S3_PACKAGE_ID),
    AWS_ERROR_S3_INVALID_CONTENT_RANGE_HEADER,
    AWS_ERROR_S3_MISSING_CONTENT_LENGTH_HEADER,
    AWS_ERROR_S3_INVALID_CONTENT_LENGTH_HEADER,
    AWS_ERROR_S3_MISSING_ETAG,
    AWS_ERROR_S3_INTERNAL_ERROR,
    AWS_ERROR_S3_SLOW_DOWN,
    AWS_ERROR_S3_INVALID_RESPONSE_STATUS,
    AWS_ERROR_S3_MISSING_UPLOAD_ID,
    AWS_ERROR_S3_PROXY_PARSE_FAILED,
    AWS_ERROR_S3_UNSUPPORTED_PROXY_SCHEME,
    AWS_ERROR_S3_CANCELED,
    AWS_ERROR_S3_INVALID_RANGE_HEADER,
    AWS_ERROR_S3_MULTIRANGE_HEADER_UNSUPPORTED,
    AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH,
    AWS_ERROR_S3_CHECKSUM_CALCULATION_FAILED,
    AWS_ERROR_S3_PAUSED,
    AWS_ERROR_S3_LIST_PARTS_PARSE_FAILED,
    AWS_ERROR_S3_RESUMED_PART_CHECKSUM_MISMATCH,
    AWS_ERROR_S3_RESUME_FAILED,
    AWS_ERROR_S3_OBJECT_MODIFIED,
    AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR,
    AWS_ERROR_S3_END_RANGE = AWS_ERROR_ENUM_END_RANGE(AWS_C_S3_PACKAGE_ID)
};

enum aws_s3_subject {
    AWS_LS_S3_GENERAL = AWS_LOG_SUBJECT_BEGIN_RANGE(AWS_C_S3_PACKAGE_ID),
    AWS_LS_S3_CLIENT,
    AWS_LS_S3_CLIENT_STATS,
    AWS_LS_S3_REQUEST,
    AWS_LS_S3_META_REQUEST,
    AWS_LS_S3_ENDPOINT,
    AWS_LS_S3_LAST = AWS_LOG_SUBJECT_END_RANGE(AWS_C_S3_PACKAGE_ID)
};

struct aws_s3_cpu_group_info {
    /* group index, this usually refers to a particular numa node */
    uint16_t cpu_group;
    /* array of network devices on this node */
    const struct aws_byte_cursor *nic_name_array;
    /* length of network devices array */
    size_t nic_name_array_length;
};

struct aws_s3_compute_platform_info {
    /* name of the instance-type: example c5n.18xlarge */
    const struct aws_byte_cursor instance_type;
    /* max throughput for this instance type */
    uint16_t max_throughput_gbps;
    /* array of cpu group info. This will always have at least one entry. */
    const struct aws_s3_cpu_group_info *cpu_group_info_array;
    /* length of cpu group info array */
    size_t cpu_group_info_array_length;
};

AWS_EXTERN_C_BEGIN

/**
 * Initializes internal datastructures used by aws-c-s3.
 * Must be called before using any functionality in aws-c-s3.
 */
AWS_S3_API
void aws_s3_library_init(struct aws_allocator *allocator);

/**
 * Retrieves the pre-configured metadata for an ec2 instance type. If no such pre-configuration exists, returns NULL.
 */
AWS_S3_API
struct aws_s3_compute_platform_info *aws_s3_get_compute_platform_info_for_instance_type(
    const struct aws_byte_cursor instance_type_name);

/**
 * Shuts down the internal datastructures used by aws-c-s3.
 */
AWS_S3_API
void aws_s3_library_clean_up(void);

AWS_EXTERN_C_END

#endif /* AWS_S3_H */
