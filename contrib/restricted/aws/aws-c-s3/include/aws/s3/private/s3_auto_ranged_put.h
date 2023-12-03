#ifndef AWS_S3_AUTO_RANGED_PUT_H
#define AWS_S3_AUTO_RANGED_PUT_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_meta_request_impl.h"
#include "s3_paginator.h"

enum aws_s3_auto_ranged_put_request_tag {
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_LIST_PARTS,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_PART,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD,
    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD,

    AWS_S3_AUTO_RANGED_PUT_REQUEST_TAG_MAX,
};

struct aws_s3_auto_ranged_put {
    struct aws_s3_meta_request base;

    /* Initialized either during creation in resume flow or as result of create multipart upload during normal flow. */
    struct aws_string *upload_id;

    /* Resume token used to resume the operation */
    struct aws_s3_meta_request_resume_token *resume_token;

    uint64_t content_length;

    /* Only meant for use in the update function, which is never called concurrently. */
    struct {
        /*
         * Next part number to send.
         * Note: this follows s3 part number convention and counting starts with 1.
         * Throughout codebase 0 based part numbers are usually referred to as part index.
         */
        uint32_t next_part_number;
    } threaded_update_data;

    /*
     * Should only be used during prepare requests. Note: stream reads must be sequential,
     * so prepare currently never runs concurrently with another prepare
     */
    struct {
        /*
         * How many parts have been read from input steam.
         * Since reads are always sequential, this is essentially the number of how many parts were read from start of
         * stream.
         */
        uint32_t num_parts_read_from_stream;
    } prepare_data;

    /*
     * Very similar to the etag_list used in complete_multipart_upload to create the XML payload. Each part will set the
     * corresponding index to it's checksum result, so while the list is shared across threads each index will only be
     * accessed once to initialize by the corresponding part number, and then again during the complete multipart upload
     * request which will only be invoked after all other parts/threads have completed.
     */
    struct aws_byte_buf *encoded_checksum_list;

    /* Members to only be used when the mutex in the base type is locked. */
    struct {
        /* Array list of `struct aws_string *`. */
        struct aws_array_list etag_list;

        struct aws_s3_paginated_operation *list_parts_operation;
        struct aws_string *list_parts_continuation_token;

        uint32_t total_num_parts;
        uint32_t num_parts_sent;
        uint32_t num_parts_completed;
        uint32_t num_parts_successful;
        uint32_t num_parts_failed;

        struct aws_http_headers *needed_response_headers;

        int list_parts_error_code;
        int create_multipart_upload_error_code;
        int complete_multipart_upload_error_code;
        int abort_multipart_upload_error_code;

        struct {
            /* Mark a single ListParts request has started or not */
            uint32_t started : 1;
            /* Mark ListParts need to continue or not */
            uint32_t continues : 1;
            /* Mark ListParts has completed all the pages or not */
            uint32_t completed : 1;
        } list_parts_state;
        uint32_t create_multipart_upload_sent : 1;
        uint32_t create_multipart_upload_completed : 1;
        uint32_t complete_multipart_upload_sent : 1;
        uint32_t complete_multipart_upload_completed : 1;
        uint32_t abort_multipart_upload_sent : 1;
        uint32_t abort_multipart_upload_completed : 1;

    } synced_data;
};

AWS_EXTERN_C_BEGIN

/* Creates a new auto-ranged put meta request.  This will do a multipart upload in parallel when appropriate. */

AWS_S3_API struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_put_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    uint64_t content_length,
    uint32_t num_parts,
    const struct aws_s3_meta_request_options *options);

AWS_EXTERN_C_END

#endif /* AWS_S3_AUTO_RANGED_PUT_H */
