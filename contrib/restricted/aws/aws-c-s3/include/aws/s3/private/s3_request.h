#ifndef AWS_S3_REQUEST_H
#define AWS_S3_REQUEST_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/linked_list.h>
#include <aws/common/ref_count.h>
#include <aws/s3/s3.h>

#include <aws/s3/private/s3_checksums.h>

struct aws_http_message;
struct aws_signable;
struct aws_s3_meta_request;

enum aws_s3_request_flags {
    AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS = 0x00000001,
    AWS_S3_REQUEST_FLAG_PART_SIZE_RESPONSE_BODY = 0x00000002,
    AWS_S3_REQUEST_FLAG_ALWAYS_SEND = 0x00000004,
};

/* Represents a single request made to S3. */
struct aws_s3_request {

    /* Linked list node used for queuing. */
    struct aws_linked_list_node node;

    /* TODO Ref count on the request is no longer needed--only one part of code should ever be holding onto a request,
     * and we can just transfer ownership.*/
    struct aws_ref_count ref_count;

    struct aws_allocator *allocator;

    /* Owning meta request. */
    struct aws_s3_meta_request *meta_request;

    /* Request body to use when sending the request. The contents of this body will be re-used if a request is
     * retried.*/
    struct aws_byte_buf request_body;

    /* Beginning range of this part. */
    /* TODO currently only used by auto_range_get, could be hooked up to auto_range_put as well. */
    uint64_t part_range_start;

    /* Last byte of this part.*/
    /* TODO currently only used by auto_range_get, could be hooked up to auto_range_put as well. */
    uint64_t part_range_end;

    /* Part number that this request refers to.  If this is not a part, this can be 0.  (S3 Part Numbers start at 1.)
     * However, must currently be a valid part number (ie: greater than 0) if the response body is to be streamed to the
     * caller.
     */
    uint32_t part_number;

    /* Number of times aws_s3_meta_request_prepare has been called for a request. During the first call to the virtual
     * prepare function, this will be 0.*/
    uint32_t num_times_prepared;

    /* checksum found in the header of an individual get part http request */
    struct aws_byte_buf request_level_response_header_checksum;

    /* running checksum of the response to an individual get part http request */
    struct aws_s3_checksum *request_level_running_response_sum;
    /* The algorithm used to validate the checksum */
    enum aws_s3_checksum_algorithm validation_algorithm;

    /* Get request only, was there a checksum to validate */
    bool did_validate;

    /* Get request only, if there was an attached checksum to validate did it match the computed checksum */
    bool checksum_match;

    /* Tag that defines what the built request will actually consist of.  This is meant to be space for an enum defined
     * by the derived type.  Request tags do not necessarily map 1:1 with actual S3 API requests.  For example, they can
     * be more contextual, like "first part" instead of just "part".) */

    /* TODO: this should be a union type to make it clear that this could be one of two enums for puts, and gets. */
    int request_tag;

    /* Members of this structure will be repopulated each time the request is sent. If the request fails, and needs to
     * be retried, then the members of this structure will be cleaned up and re-populated on the next send.
     */
    /* TODO rename this anonymous structure to something more intuitive. (Maybe "attempt_data")*/
    struct {

        /* The HTTP message to send for this request. */
        struct aws_http_message *message;

        /* Signable created for the above message. */
        struct aws_signable *signable;

        /* Recorded response headers for the request. Set only when the request desc has record_response_headers set to
         * true or when this response indicates an error. */
        struct aws_http_headers *response_headers;

        /* Recorded response body of the request. */
        struct aws_byte_buf response_body;

        /* Returned response status of this request. */
        int response_status;

    } send_data;

    /* When true, response headers from the request will be stored in the request's response_headers variable. */
    uint32_t record_response_headers : 1;

    /* When true, the response body buffer will be allocated in the size of a part. */
    uint32_t part_size_response_body : 1;

    /* When true, this request is being tracked by the client for limiting the amount of in-flight-requests/stats. */
    uint32_t tracked_by_client : 1;

    /* When true, even when the meta request has a finish result set, this request will be sent. */
    uint32_t always_send : 1;

    /* When true, this request is intended to find out the object size. This is currently only used by auto_range_get.
     */
    uint32_t discovers_object_size : 1;
};

AWS_EXTERN_C_BEGIN

/* Create a new s3 request structure with the given options. */
AWS_S3_API
struct aws_s3_request *aws_s3_request_new(
    struct aws_s3_meta_request *meta_request,
    int request_tag,
    uint32_t part_number,
    uint32_t flags);

/* Set up the request to be sent. Called each time before the request is sent. Will initially call
 * aws_s3_request_clean_up_send_data to clear out anything previously existing in send_data. */
AWS_S3_API
void aws_s3_request_setup_send_data(struct aws_s3_request *request, struct aws_http_message *message);

/* Clear out send_data members so that they can be repopulated before the next send. */
AWS_S3_API
void aws_s3_request_clean_up_send_data(struct aws_s3_request *request);

AWS_S3_API
void aws_s3_request_acquire(struct aws_s3_request *request);

AWS_S3_API
void aws_s3_request_release(struct aws_s3_request *request);

AWS_EXTERN_C_END

#endif
