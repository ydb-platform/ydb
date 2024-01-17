#ifndef AWS_S3_META_REQUEST_IMPL_H
#define AWS_S3_META_REQUEST_IMPL_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/auth/signing.h>
#include <aws/common/atomics.h>
#include <aws/common/linked_list.h>
#include <aws/common/mutex.h>
#include <aws/common/ref_count.h>
#include <aws/common/task_scheduler.h>
#include <aws/http/request_response.h>

#include "aws/s3/private/s3_checksums.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_request.h"

struct aws_s3_client;
struct aws_s3_connection;
struct aws_s3_meta_request;
struct aws_s3_request;
struct aws_s3_request_options;
struct aws_http_headers;
struct aws_http_make_request_options;
struct aws_retry_strategy;

enum aws_s3_meta_request_state {
    AWS_S3_META_REQUEST_STATE_ACTIVE,
    AWS_S3_META_REQUEST_STATE_FINISHED,
};

enum aws_s3_meta_request_update_flags {
    /* The client potentially has multiple meta requests that it can spread across connections, and the given meta
       request can selectively not return a request if there is a performance reason to do so.*/
    AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE = 0x00000002,
};

typedef void(aws_s3_meta_request_prepare_request_callback_fn)(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data);

struct aws_s3_prepare_request_payload {
    struct aws_s3_request *request;
    aws_s3_meta_request_prepare_request_callback_fn *callback;
    void *user_data;
    struct aws_task task;
};

struct aws_s3_meta_request_vtable {
    /* Update the meta request.  out_request is required to be non-null. Returns true if there is any work in
     * progress, false if there is not. */
    bool (*update)(struct aws_s3_meta_request *meta_request, uint32_t flags, struct aws_s3_request **out_request);

    void (*schedule_prepare_request)(
        struct aws_s3_meta_request *meta_request,
        struct aws_s3_request *request,
        aws_s3_meta_request_prepare_request_callback_fn *callback,
        void *user_data);

    /* Given a request, prepare it for sending (ie: creating the correct HTTP message, reading from a stream (if
     * necessary), signing it, computing hashes, etc.) */
    int (*prepare_request)(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request);

    void (*init_signing_date_time)(struct aws_s3_meta_request *meta_request, struct aws_date_time *date_time);

    /* Sign the given request. */
    void (*sign_request)(
        struct aws_s3_meta_request *meta_request,
        struct aws_s3_request *request,
        aws_signing_complete_fn *on_signing_complete,
        void *user_data);

    /* Called when any sending of the request is finished, including for each retry. */
    void (*send_request_finish)(struct aws_s3_connection *connection, struct aws_http_stream *stream, int error_code);

    /* Called when the request is done being sent, and will not be retried/sent again. */
    void (*finished_request)(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request, int error_code);

    /* Called by the derived meta request when the meta request is completely finished. */
    void (*finish)(struct aws_s3_meta_request *meta_request);

    /* Handle de-allocation of the meta request. */
    void (*destroy)(struct aws_s3_meta_request *);

    /* Pause the given request */
    int (*pause)(struct aws_s3_meta_request *meta_request, struct aws_s3_meta_request_resume_token **resume_token);
};

/**
 * This represents one meta request, ie, one accelerated file transfer.  One S3 meta request can represent multiple S3
 * requests.
 */
struct aws_s3_meta_request {
    struct aws_allocator *allocator;

    struct aws_ref_count ref_count;

    void *impl;

    struct aws_s3_meta_request_vtable *vtable;

    /* Initial HTTP Message that this meta request is based on. */
    struct aws_http_message *initial_request_message;

    /* Part size to use for uploads and downloads.  Passed down by the creating client. */
    const size_t part_size;

    struct aws_cached_signing_config_aws *cached_signing_config;

    /* Client that created this meta request which also processes this request. After the meta request is finished, this
     * reference is removed.*/
    struct aws_s3_client *client;

    struct aws_s3_endpoint *endpoint;

    /* Event loop to schedule IO work related on, ie, reading from streams, streaming parts back to the caller, etc..
     * After the meta request is finished, this will be reset along with the client reference.*/
    struct aws_event_loop *io_event_loop;

    /* User data to be passed to each customer specified callback.*/
    void *user_data;

    /* Customer specified callbacks. */
    aws_s3_meta_request_headers_callback_fn *headers_callback;
    aws_s3_meta_request_receive_body_callback_fn *body_callback;
    aws_s3_meta_request_finish_fn *finish_callback;
    aws_s3_meta_request_shutdown_fn *shutdown_callback;
    aws_s3_meta_request_progress_fn *progress_callback;

    /* Customer specified callbacks to be called by our specialized callback to calculate the response checksum. */
    aws_s3_meta_request_headers_callback_fn *headers_user_callback_after_checksum;
    aws_s3_meta_request_receive_body_callback_fn *body_user_callback_after_checksum;
    aws_s3_meta_request_finish_fn *finish_user_callback_after_checksum;

    enum aws_s3_meta_request_type type;

    struct {
        struct aws_mutex lock;

        /* Priority queue for pending streaming requests.  We use a priority queue to keep parts in order so that we
         * can stream them to the caller in order. */
        struct aws_priority_queue pending_body_streaming_requests;

        /* Current state of the meta request. */
        enum aws_s3_meta_request_state state;

        /* The sum of initial_read_window, plus all window_increment() calls. This number never goes down. */
        uint64_t read_window_running_total;

        /* The next expected streaming part number needed to continue streaming part bodies.  (For example, this will
         * initially be 1 for part 1, and after that part is received, it will be 2, then 3, etc.. */
        uint32_t next_streaming_part;

        /* Number of parts scheduled for delivery. */
        uint32_t num_parts_delivery_sent;

        /* Total number of parts that have been attempted to be delivered. (Will equal the sum of succeeded and
         * failed.)*/
        uint32_t num_parts_delivery_completed;

        /* Number of parts that have been successfully delivered to the caller. */
        uint32_t num_parts_delivery_succeeded;

        /* Number of parts that have failed while trying to be delivered to the caller. */
        uint32_t num_parts_delivery_failed;

        /* The end finish result of the meta request. */
        struct aws_s3_meta_request_result finish_result;

        /* True if the finish result has been set. */
        uint32_t finish_result_set : 1;

    } synced_data;

    /* Anything in this structure should only ever be accessed by the client on its process work event loop task. */
    struct {

        /* Linked list node for the meta requests linked list in the client. */
        /* Note: this needs to be first for using AWS_CONTAINER_OF with the nested structure. */
        struct aws_linked_list_node node;

        /* True if this meta request is currently in the client's list. */
        bool scheduled;

    } client_process_work_threaded_data;

    const bool should_compute_content_md5;

    /* deep copy of the checksum config. */
    struct checksum_config checksum_config;

    /* checksum found in either a default get request, or in the initial head request of a multipart get */
    struct aws_byte_buf meta_request_level_response_header_checksum;

    /* running checksum of all of the parts of a default get, or ranged get meta request*/
    struct aws_s3_checksum *meta_request_level_running_response_sum;
};

AWS_EXTERN_C_BEGIN

/* Initialize the base meta request structure. */
AWS_S3_API
int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    bool should_compute_content_md5,
    const struct aws_s3_meta_request_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *base_type);

/* Returns true if the meta request is still in the "active" state. */
AWS_S3_API
bool aws_s3_meta_request_is_active(struct aws_s3_meta_request *meta_request);

/* Returns true if the meta request is in the "finished" state. */
AWS_S3_API
bool aws_s3_meta_request_is_finished(struct aws_s3_meta_request *meta_request);

/* Returns true if the meta request has a finish result, which indicates that the meta request has trying to finish or
 * has already finished. */
AWS_S3_API
bool aws_s3_meta_request_has_finish_result(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request);

AWS_S3_API
void aws_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request);

/* Called by the client to retrieve the next request and update the meta request's internal state. out_request is
 * optional, and can be NULL if just desiring to update internal state. */
AWS_S3_API
bool aws_s3_meta_request_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request);

AWS_S3_API
void aws_s3_meta_request_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_s3_meta_request_prepare_request_callback_fn *callback,
    void *user_data);

AWS_S3_API
void aws_s3_meta_request_send_request(struct aws_s3_meta_request *meta_request, struct aws_s3_connection *connection);

AWS_S3_API
void aws_s3_meta_request_init_signing_date_time_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time);

AWS_S3_API
void aws_s3_meta_request_sign_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_signing_complete_fn *on_signing_complete,
    void *user_data);

/* Default implementation for when a request finishes a particular send. */
AWS_S3_API
void aws_s3_meta_request_send_request_finish_default(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code);

/* Implementation for when a request finishes a particular send to handle possible async error from S3. */
AWS_S3_API
void aws_s3_meta_request_send_request_finish_handle_async_error(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code);

/* Called by the client when a request is completely finished and not doing any further retries. */
AWS_S3_API
void aws_s3_meta_request_finished_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code);

/* Called to place the request in the meta request's priority queue for streaming back to the caller.  Once all requests
 * with a part number less than the given request has been received, the given request and the previous requests will
 * scheduled for streaming.  */
AWS_S3_API
void aws_s3_meta_request_stream_response_body_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

/* Read from the meta request's input stream. Should always be done outside of any mutex, as reading from the stream
 * could cause user code to call back into aws-c-s3.*/
AWS_S3_API
int aws_s3_meta_request_read_body(struct aws_s3_meta_request *meta_request, struct aws_byte_buf *buffer);

/* Set the meta request finish result as failed. This is meant to be called sometime before aws_s3_meta_request_finish.
 * Subsequent calls to this function or to aws_s3_meta_request_set_success_synced will not overwrite the end result of
 * the meta request. */
AWS_S3_API
void aws_s3_meta_request_set_fail_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int error_code);

/* Set the meta request finish result as successful. This is meant to be called sometime before
 * aws_s3_meta_request_finish. Subsequent calls this function or to aws_s3_meta_request_set_fail_synced will not
 * overwrite the end result of the meta request. */
AWS_S3_API
void aws_s3_meta_request_set_success_synced(struct aws_s3_meta_request *meta_request, int response_status);

/* Returns true if the finish result has been set (ie: either aws_s3_meta_request_set_fail_synced or
 * aws_s3_meta_request_set_success_synced have been called.) */
AWS_S3_API
bool aws_s3_meta_request_has_finish_result_synced(struct aws_s3_meta_request *meta_request);

/* Virtual function called by the meta request derived type when it's completely finished and there is no other work to
 * be done. */
AWS_S3_API
void aws_s3_meta_request_finish(struct aws_s3_meta_request *meta_request);

/* Default implementation of the meta request finish function. */
AWS_S3_API
void aws_s3_meta_request_finish_default(struct aws_s3_meta_request *meta_request);

/* Sets up a meta request result structure. */
AWS_S3_API
void aws_s3_meta_request_result_setup(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_result *result,
    struct aws_s3_request *request,
    int response_status,
    int error_code);

/* Cleans up a meta request result structure. */
AWS_S3_API
void aws_s3_meta_request_result_clean_up(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_result *result);

AWS_S3_API
bool aws_s3_meta_request_checksum_config_has_algorithm(
    struct aws_s3_meta_request *meta_request,
    enum aws_s3_checksum_algorithm algorithm);

AWS_EXTERN_C_END

#endif /* AWS_S3_META_REQUEST_IMPL_H */
