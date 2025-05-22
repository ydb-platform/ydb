#ifndef AWS_S3_AUTO_RANGED_GET_H
#define AWS_S3_AUTO_RANGED_GET_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_meta_request_impl.h"

enum aws_s3_auto_ranged_get_request_type {
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_HEAD_OBJECT,
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART,
    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_INITIAL_MESSAGE,
};

struct aws_s3_auto_ranged_get {
    struct aws_s3_meta_request base;

    enum aws_s3_checksum_algorithm validation_algorithm;
    /* Members to only be used when the mutex in the base type is locked. */
    struct {
        /* The starting byte of the data that we will be retrieved from the object.*/
        uint64_t object_range_start;

        /* The last byte of the data that will be retrieved from the object.*/
        uint64_t object_range_end;

        /* The total number of parts that are being used in downloading the object range. Note that "part" here
         * currently refers to a range-get, and does not require a "part" on the service side. */
        uint32_t total_num_parts;

        uint32_t num_parts_requested;
        uint32_t num_parts_completed;
        uint32_t num_parts_successful;
        uint32_t num_parts_failed;
        uint32_t num_parts_checksum_validated;

        uint32_t object_range_known : 1;
        uint32_t head_object_sent : 1;
        uint32_t head_object_completed : 1;
        uint32_t get_without_range_sent : 1;
        uint32_t get_without_range_completed : 1;
        uint32_t read_window_warning_issued : 1;
    } synced_data;

    uint32_t initial_message_has_range_header : 1;
    uint32_t initial_message_has_if_match_header : 1;

    struct aws_string *etag;
};

AWS_EXTERN_C_BEGIN

/* Creates a new auto-ranged get meta request.  This will do multiple parallel ranged-gets when appropriate. */
AWS_S3_API struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    const struct aws_s3_meta_request_options *options);

AWS_EXTERN_C_END

#endif /* AWS_S3_AUTO_RANGED_GET_H */
