/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_checksums.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_parallel_input_stream.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include "aws/s3/s3express_credentials_provider.h"
#include <aws/auth/signable.h>
#include <aws/auth/signing.h>
#include <aws/auth/signing_config.h>
#include <aws/auth/signing_result.h>
#include <aws/common/clock.h>
#include <aws/common/encoding.h>
#include <aws/common/string.h>
#include <aws/common/system_info.h>
#include <aws/io/async_stream.h>
#include <aws/io/event_loop.h>
#include <aws/io/retry_strategy.h>
#include <aws/io/socket.h>
#include <aws/io/stream.h>
#include <inttypes.h>

static const size_t s_dynamic_body_initial_buf_size = KB_TO_BYTES(1);
static const size_t s_default_body_streaming_priority_queue_size = 16;
static const size_t s_default_event_delivery_array_size = 16;

static int s_s3_request_priority_queue_pred(const void *a, const void *b);
static void s_s3_meta_request_destroy(void *user_data);

static void s_s3_meta_request_init_signing_date_time(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time);

static void s_s3_meta_request_sign_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_signing_complete_fn *on_signing_complete,
    void *user_data);

static void s_s3_meta_request_request_on_signed(
    struct aws_signing_result *signing_result,
    int error_code,
    void *user_data);

static int s_s3_meta_request_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data);

static int s_s3_meta_request_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data);

static int s_s3_meta_request_headers_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data);

static void s_s3_meta_request_stream_metrics(
    struct aws_http_stream *stream,
    const struct aws_http_stream_metrics *metrics,
    void *user_data);

static void s_s3_meta_request_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data);

static void s_s3_meta_request_send_request_finish(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code);

static int s_s3_meta_request_read_from_pending_async_writes(
    struct aws_s3_meta_request *meta_request,
    struct aws_byte_buf *dest,
    bool *eof);

void aws_s3_meta_request_lock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_lock(&meta_request->synced_data.lock);
}

void aws_s3_meta_request_unlock_synced_data(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_mutex_unlock(&meta_request->synced_data.lock);
}

static int s_meta_request_get_response_headers_checksum_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_headers *headers,
    int response_status,
    void *user_data) {
    for (int i = AWS_SCA_INIT; i <= AWS_SCA_END; i++) {
        if (!aws_s3_meta_request_checksum_config_has_algorithm(meta_request, i)) {
            /* If user doesn't select this algorithm, skip */
            continue;
        }
        const struct aws_byte_cursor *algorithm_header_name = aws_get_http_header_name_from_algorithm(i);
        if (aws_http_headers_has(headers, *algorithm_header_name) &&
            !aws_http_headers_has(headers, g_mp_parts_count_header_name)) {
            struct aws_byte_cursor header_sum;
            aws_http_headers_get(headers, *algorithm_header_name, &header_sum);
            size_t encoded_len = 0;
            aws_base64_compute_encoded_len(aws_get_digest_size_from_algorithm(i), &encoded_len);
            if (header_sum.len == encoded_len - 1) {
                /* encoded_len includes the nullptr length. -1 is the expected length. */
                aws_byte_buf_init_copy_from_cursor(
                    &meta_request->meta_request_level_response_header_checksum, meta_request->allocator, header_sum);
                meta_request->meta_request_level_running_response_sum = aws_checksum_new(meta_request->allocator, i);
            }
            break;
        }
    }
    if (meta_request->headers_user_callback_after_checksum) {
        return meta_request->headers_user_callback_after_checksum(meta_request, headers, response_status, user_data);
    } else {
        return AWS_OP_SUCCESS;
    }
}

/* warning this might get screwed up with retries/restarts */
static int s_meta_request_get_response_body_checksum_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_byte_cursor *body,
    uint64_t range_start,
    void *user_data) {
    if (meta_request->meta_request_level_running_response_sum) {
        aws_checksum_update(meta_request->meta_request_level_running_response_sum, body);
    }

    if (meta_request->body_user_callback_after_checksum) {
        return meta_request->body_user_callback_after_checksum(meta_request, body, range_start, user_data);
    } else {
        return AWS_OP_SUCCESS;
    }
}

static void s_meta_request_get_response_finish_checksum_callback(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_result *meta_request_result,
    void *user_data) {
    struct aws_byte_buf response_body_sum;
    struct aws_byte_buf encoded_response_body_sum;
    AWS_ZERO_STRUCT(response_body_sum);
    AWS_ZERO_STRUCT(encoded_response_body_sum);

    struct aws_s3_meta_request_result *mut_meta_request_result =
        (struct aws_s3_meta_request_result *)meta_request_result;
    if (meta_request_result->error_code == AWS_OP_SUCCESS && meta_request->meta_request_level_running_response_sum) {
        mut_meta_request_result->did_validate = true;
        mut_meta_request_result->validation_algorithm =
            meta_request->meta_request_level_running_response_sum->algorithm;
        size_t encoded_checksum_len = 0;
        /* what error should I raise for these? */
        aws_base64_compute_encoded_len(
            meta_request->meta_request_level_running_response_sum->digest_size, &encoded_checksum_len);
        aws_byte_buf_init(&encoded_response_body_sum, meta_request->allocator, encoded_checksum_len);
        aws_byte_buf_init(
            &response_body_sum,
            meta_request->allocator,
            meta_request->meta_request_level_running_response_sum->digest_size);
        aws_checksum_finalize(meta_request->meta_request_level_running_response_sum, &response_body_sum, 0);
        struct aws_byte_cursor response_body_sum_cursor = aws_byte_cursor_from_buf(&response_body_sum);
        aws_base64_encode(&response_body_sum_cursor, &encoded_response_body_sum);
        if (!aws_byte_buf_eq(&encoded_response_body_sum, &meta_request->meta_request_level_response_header_checksum)) {
            mut_meta_request_result->error_code = AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH;
        }
    }
    if (meta_request->finish_user_callback_after_checksum) {
        meta_request->finish_user_callback_after_checksum(meta_request, meta_request_result, user_data);
    }
    aws_byte_buf_clean_up(&response_body_sum);
    aws_byte_buf_clean_up(&encoded_response_body_sum);
    aws_checksum_destroy(meta_request->meta_request_level_running_response_sum);
    aws_byte_buf_clean_up(&meta_request->meta_request_level_response_header_checksum);
}

int aws_s3_meta_request_init_base(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    bool should_compute_content_md5,
    const struct aws_s3_meta_request_options *options,
    void *impl,
    struct aws_s3_meta_request_vtable *vtable,
    struct aws_s3_meta_request *meta_request) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->message);
    AWS_PRECONDITION(impl);
    AWS_PRECONDITION(meta_request);

    AWS_ZERO_STRUCT(*meta_request);

    AWS_ASSERT(vtable->update);
    AWS_ASSERT(vtable->prepare_request);
    AWS_ASSERT(vtable->destroy);
    AWS_ASSERT(vtable->sign_request);
    AWS_ASSERT(vtable->init_signing_date_time);
    AWS_ASSERT(vtable->finished_request);
    AWS_ASSERT(vtable->send_request_finish);

    meta_request->allocator = allocator;
    meta_request->type = options->type;
    /* Set up reference count. */
    aws_ref_count_init(&meta_request->ref_count, meta_request, s_s3_meta_request_destroy);
    aws_linked_list_init(&meta_request->synced_data.cancellable_http_streams_list);

    if (part_size == SIZE_MAX) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    if (aws_mutex_init(&meta_request->synced_data.lock)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p Could not initialize mutex for meta request", (void *)meta_request);
        goto error;
    }

    if (aws_priority_queue_init_dynamic(
            &meta_request->synced_data.pending_body_streaming_requests,
            meta_request->allocator,
            s_default_body_streaming_priority_queue_size,
            sizeof(struct aws_s3_request *),
            s_s3_request_priority_queue_pred)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p Could not initialize priority queue for meta request", (void *)meta_request);
        /* Priority queue */
        goto error;
    }

    aws_array_list_init_dynamic(
        &meta_request->synced_data.event_delivery_array,
        meta_request->allocator,
        s_default_event_delivery_array_size,
        sizeof(struct aws_s3_meta_request_event));

    aws_array_list_init_dynamic(
        &meta_request->io_threaded_data.event_delivery_array,
        meta_request->allocator,
        s_default_event_delivery_array_size,
        sizeof(struct aws_s3_meta_request_event));

    *((size_t *)&meta_request->part_size) = part_size;
    *((bool *)&meta_request->should_compute_content_md5) = should_compute_content_md5;
    checksum_config_init(&meta_request->checksum_config, options->checksum_config);

    if (options->signing_config) {
        meta_request->cached_signing_config = aws_cached_signing_config_new(client, options->signing_config);
    }

    /* Client is currently optional to allow spinning up a meta_request without a client in a test. */
    if (client != NULL) {
        meta_request->client = aws_s3_client_acquire(client);
        meta_request->io_event_loop = aws_event_loop_group_get_next_loop(client->body_streaming_elg);
        meta_request->synced_data.read_window_running_total = client->initial_read_window;
    }

    /* Keep original message around, for headers, method, and synchronous body-stream (if any) */
    meta_request->initial_request_message = aws_http_message_acquire(options->message);

    /* If the request's body is being passed in some other way, set that up.
     * (we checked earlier that the request body is not being passed multiple ways) */
    if (options->send_filepath.len > 0) {
        /* Create parallel read stream from file */
        meta_request->request_body_parallel_stream =
            client->vtable->parallel_input_stream_new_from_file(allocator, options->send_filepath);
        if (meta_request->request_body_parallel_stream == NULL) {
            goto error;
        }

    } else if (options->send_async_stream != NULL) {
        meta_request->request_body_async_stream = aws_async_input_stream_acquire(options->send_async_stream);

    } else if (options->send_using_async_writes == true) {
        meta_request->request_body_using_async_writes = true;
        aws_byte_buf_init(&meta_request->synced_data.async_write.buffered_data, allocator, 0);
    }

    meta_request->synced_data.next_streaming_part = 1;

    meta_request->meta_request_level_running_response_sum = NULL;
    meta_request->user_data = options->user_data;
    meta_request->shutdown_callback = options->shutdown_callback;
    meta_request->progress_callback = options->progress_callback;
    meta_request->telemetry_callback = options->telemetry_callback;
    meta_request->upload_review_callback = options->upload_review_callback;

    if (meta_request->checksum_config.validate_response_checksum) {
        /* TODO: the validate for auto range get should happen for each response received. */
        meta_request->headers_user_callback_after_checksum = options->headers_callback;
        meta_request->body_user_callback_after_checksum = options->body_callback;
        meta_request->finish_user_callback_after_checksum = options->finish_callback;

        meta_request->headers_callback = s_meta_request_get_response_headers_checksum_callback;
        meta_request->body_callback = s_meta_request_get_response_body_checksum_callback;
        meta_request->finish_callback = s_meta_request_get_response_finish_checksum_callback;
    } else {
        meta_request->headers_callback = options->headers_callback;
        meta_request->body_callback = options->body_callback;
        meta_request->finish_callback = options->finish_callback;
    }

    /* Nothing can fail after here. Leave the impl not affected by failure of initializing base. */
    meta_request->impl = impl;
    meta_request->vtable = vtable;

    return AWS_OP_SUCCESS;
error:
    s_s3_meta_request_destroy((void *)meta_request);
    return AWS_OP_ERR;
}

void aws_s3_meta_request_increment_read_window(struct aws_s3_meta_request *meta_request, uint64_t bytes) {
    AWS_PRECONDITION(meta_request);

    if (bytes == 0) {
        return;
    }

    if (!meta_request->client->enable_read_backpressure) {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Ignoring call to increment read window. This client has not enabled read backpressure.",
            (void *)meta_request);
        return;
    }

    AWS_LOGF_TRACE(AWS_LS_S3_META_REQUEST, "id=%p: Incrementing read window by %" PRIu64, (void *)meta_request, bytes);

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);

    /* Response will never approach UINT64_MAX, so do a saturating sum instead of worrying about overflow */
    meta_request->synced_data.read_window_running_total =
        aws_add_u64_saturating(bytes, meta_request->synced_data.read_window_running_total);

    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    /* Schedule the work task, to continue processing the meta-request */
    aws_s3_client_schedule_process_work(meta_request->client);
}

void aws_s3_meta_request_cancel(struct aws_s3_meta_request *meta_request) {
    struct aws_future_void *write_future_to_cancel = NULL;

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);
    aws_s3_meta_request_set_fail_synced(meta_request, NULL, AWS_ERROR_S3_CANCELED);
    aws_s3_meta_request_cancel_cancellable_requests_synced(meta_request, AWS_ERROR_S3_CANCELED);
    if (meta_request->synced_data.async_write.future != NULL) {
        write_future_to_cancel = meta_request->synced_data.async_write.future;
        meta_request->synced_data.async_write.future = NULL;
    }
    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    if (write_future_to_cancel != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_META_REQUEST, "id=%p: write future complete due to cancellation", (void *)meta_request);
        aws_future_void_set_error(write_future_to_cancel, AWS_ERROR_S3_REQUEST_HAS_COMPLETED);
        aws_future_void_release(write_future_to_cancel);
    }

    /* Schedule the work task, to continue processing the meta-request */
    aws_s3_client_schedule_process_work(meta_request->client);
}

int aws_s3_meta_request_pause(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_resume_token **out_resume_token) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);

    *out_resume_token = NULL;

    if (!meta_request->vtable->pause) {
        return aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
    }

    return meta_request->vtable->pause(meta_request, out_resume_token);
}

void aws_s3_meta_request_set_fail_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *failed_request,
    int error_code) {
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    /* Protect against bugs */
    if (error_code == AWS_ERROR_SUCCESS) {
        AWS_ASSERT(false);
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request failed but error code not set, AWS_ERROR_UNKNOWN will be reported",
            (void *)meta_request);
        error_code = AWS_ERROR_UNKNOWN;
    }

    if (meta_request->synced_data.finish_result_set) {
        return;
    }

    meta_request->synced_data.finish_result_set = true;

    if ((error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS || error_code == AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR ||
         error_code == AWS_ERROR_S3_OBJECT_MODIFIED) &&
        failed_request != NULL) {
        aws_s3_meta_request_result_setup(
            meta_request,
            &meta_request->synced_data.finish_result,
            failed_request,
            failed_request->send_data.response_status,
            error_code);
    } else {
        AWS_ASSERT(error_code != AWS_ERROR_S3_INVALID_RESPONSE_STATUS);

        aws_s3_meta_request_result_setup(meta_request, &meta_request->synced_data.finish_result, NULL, 0, error_code);
    }
}

void aws_s3_meta_request_set_success_synced(struct aws_s3_meta_request *meta_request, int response_status) {
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    if (meta_request->synced_data.finish_result_set) {
        return;
    }

    meta_request->synced_data.finish_result_set = true;

    aws_s3_meta_request_result_setup(
        meta_request, &meta_request->synced_data.finish_result, NULL, response_status, AWS_ERROR_SUCCESS);
}

bool aws_s3_meta_request_has_finish_result(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);
    bool is_finishing = aws_s3_meta_request_has_finish_result_synced(meta_request);
    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    return is_finishing;
}

bool aws_s3_meta_request_has_finish_result_synced(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    if (!meta_request->synced_data.finish_result_set) {
        return false;
    }

    return true;
}

struct aws_s3_meta_request *aws_s3_meta_request_acquire(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    aws_ref_count_acquire(&meta_request->ref_count);
    return meta_request;
}

struct aws_s3_meta_request *aws_s3_meta_request_release(struct aws_s3_meta_request *meta_request) {
    if (meta_request != NULL) {
        aws_ref_count_release(&meta_request->ref_count);
    }

    return NULL;
}

static void s_s3_meta_request_destroy(void *user_data) {
    struct aws_s3_meta_request *meta_request = user_data;
    AWS_PRECONDITION(meta_request);
    void *log_id = meta_request;

    AWS_LOGF_DEBUG(AWS_LS_S3_META_REQUEST, "id=%p Cleaning up meta request", (void *)meta_request);

    /* Clean up our initial http message */
    meta_request->request_body_async_stream = aws_async_input_stream_release(meta_request->request_body_async_stream);
    meta_request->initial_request_message = aws_http_message_release(meta_request->initial_request_message);

    void *meta_request_user_data = meta_request->user_data;
    aws_s3_meta_request_shutdown_fn *shutdown_callback = meta_request->shutdown_callback;

    aws_cached_signing_config_destroy(meta_request->cached_signing_config);
    aws_string_destroy(meta_request->s3express_session_host);
    aws_mutex_clean_up(&meta_request->synced_data.lock);
    /* endpoint should have already been released and set NULL by the meta request finish call.
     * But call release() again, just in case we're tearing down a half-initialized meta request */
    aws_s3_endpoint_release(meta_request->endpoint);
    meta_request->client = aws_s3_client_release(meta_request->client);

    AWS_ASSERT(aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests) == 0);
    aws_priority_queue_clean_up(&meta_request->synced_data.pending_body_streaming_requests);

    AWS_ASSERT(aws_array_list_length(&meta_request->synced_data.event_delivery_array) == 0);
    aws_array_list_clean_up(&meta_request->synced_data.event_delivery_array);

    AWS_ASSERT(aws_array_list_length(&meta_request->io_threaded_data.event_delivery_array) == 0);
    aws_array_list_clean_up(&meta_request->io_threaded_data.event_delivery_array);

    AWS_ASSERT(aws_linked_list_empty(&meta_request->synced_data.cancellable_http_streams_list));

    aws_s3_meta_request_result_clean_up(meta_request, &meta_request->synced_data.finish_result);

    aws_byte_buf_clean_up(&meta_request->synced_data.async_write.buffered_data);

    if (meta_request->vtable != NULL) {
        AWS_LOGF_TRACE(AWS_LS_S3_META_REQUEST, "id=%p Calling virtual meta request destroy function.", log_id);
        meta_request->vtable->destroy(meta_request);
    }
    meta_request = NULL;

    if (shutdown_callback != NULL) {
        AWS_LOGF_TRACE(AWS_LS_S3_META_REQUEST, "id=%p Calling meta request shutdown callback.", log_id);
        shutdown_callback(meta_request_user_data);
    }

    AWS_LOGF_TRACE(AWS_LS_S3_META_REQUEST, "id=%p Meta request clean up finished.", log_id);
}

static int s_s3_request_priority_queue_pred(const void *a, const void *b) {
    const struct aws_s3_request *const *request_a = a;
    AWS_PRECONDITION(request_a);
    AWS_PRECONDITION(*request_a);

    const struct aws_s3_request *const *request_b = b;
    AWS_PRECONDITION(request_b);
    AWS_PRECONDITION(*request_b);

    return (*request_a)->part_number > (*request_b)->part_number;
}

bool aws_s3_meta_request_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->update);

    return meta_request->vtable->update(meta_request, flags, out_request);
}

bool aws_s3_meta_request_is_active(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);
    bool active = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_ACTIVE;
    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    return active;
}

bool aws_s3_meta_request_is_finished(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);
    bool is_finished = meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED;
    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    return is_finished;
}

static void s_s3_meta_request_prepare_request_task(struct aws_task *task, void *arg, enum aws_task_status task_status);
static void s_s3_meta_request_on_request_prepared(void *user_data);

/* TODO: document how this is final step in prepare-request sequence.
 * Could be invoked on any thread. */
static void s_s3_prepare_request_payload_callback_and_destroy(
    struct aws_s3_prepare_request_payload *payload,
    int error_code) {
    AWS_PRECONDITION(payload);
    AWS_PRECONDITION(payload->request);

    struct aws_s3_meta_request *meta_request = payload->request->meta_request;
    AWS_PRECONDITION(meta_request);

    ++payload->request->num_times_prepared;

    if (error_code) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not prepare request %p due to error %d (%s).",
            (void *)meta_request,
            (void *)payload->request,
            error_code,
            aws_error_str(error_code));

        /* BEGIN CRITICAL SECTION */
        aws_s3_meta_request_lock_synced_data(meta_request);
        aws_s3_meta_request_set_fail_synced(meta_request, payload->request, error_code);
        aws_s3_meta_request_unlock_synced_data(meta_request);
        /* END CRITICAL SECTION */
    }

    if (payload->callback != NULL) {
        payload->callback(meta_request, payload->request, error_code, payload->user_data);
    }

    aws_future_void_release(payload->asyncstep_prepare_request);
    aws_mem_release(payload->allocator, payload);
}

static void s_s3_meta_request_schedule_prepare_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_s3_meta_request_prepare_request_callback_fn *callback,
    void *user_data);

void aws_s3_meta_request_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_s3_meta_request_prepare_request_callback_fn *callback,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);

    if (meta_request->vtable->schedule_prepare_request) {
        meta_request->vtable->schedule_prepare_request(meta_request, request, callback, user_data);
    } else {
        s_s3_meta_request_schedule_prepare_request_default(meta_request, request, callback, user_data);
    }
}

static void s_s3_meta_request_schedule_prepare_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_s3_meta_request_prepare_request_callback_fn *callback,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    struct aws_s3_client *client = meta_request->client;
    AWS_PRECONDITION(client);

    struct aws_allocator *allocator = client->allocator;
    AWS_PRECONDITION(allocator);

    struct aws_s3_prepare_request_payload *payload =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_prepare_request_payload));

    payload->allocator = allocator;
    payload->request = request;
    payload->callback = callback;
    payload->user_data = user_data;

    aws_task_init(
        &payload->task, s_s3_meta_request_prepare_request_task, payload, "s3_meta_request_prepare_request_task");
    if (meta_request->request_body_parallel_stream) {
        /* The body stream supports reading in parallel, so schedule task on any I/O thread.
         * If we always used the meta-request's dedicated io_event_loop, we wouldn't get any parallelism. */
        struct aws_event_loop *loop = aws_event_loop_group_get_next_loop(client->body_streaming_elg);
        aws_event_loop_schedule_task_now(loop, &payload->task);
    } else {
        aws_event_loop_schedule_task_now(meta_request->io_event_loop, &payload->task);
    }
}

static void s_s3_meta_request_prepare_request_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;
    (void)task_status;

    struct aws_s3_prepare_request_payload *payload = arg;
    AWS_PRECONDITION(payload);

    struct aws_s3_request *request = payload->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    const struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);

    /* Client owns this event loop group. A cancel should not be possible. */
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    if (!request->always_send && aws_s3_meta_request_has_finish_result(meta_request)) {
        s_s3_prepare_request_payload_callback_and_destroy(payload, AWS_ERROR_S3_CANCELED);
        return;
    }

    /* Kick off the async vtable->prepare_request()
     * Each subclass has its own implementation of this. */
    payload->asyncstep_prepare_request = vtable->prepare_request(request);
    aws_future_void_register_callback(
        payload->asyncstep_prepare_request, s_s3_meta_request_on_request_prepared, payload);
    return;
}

/* Called after vtable->prepare_request has succeeded or failed. */
static void s_s3_meta_request_on_request_prepared(void *user_data) {
    struct aws_s3_prepare_request_payload *payload = user_data;
    struct aws_s3_request *request = payload->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;

    int error_code = aws_future_void_get_error(payload->asyncstep_prepare_request);
    if (error_code) {
        s_s3_prepare_request_payload_callback_and_destroy(payload, error_code);
        return;
    }

    aws_s3_add_user_agent_header(meta_request->allocator, request->send_data.message);

    /* Next step is to sign the newly created message (completion callback could happen on any thread) */
    s_s3_meta_request_sign_request(meta_request, request, s_s3_meta_request_request_on_signed, payload);
}

static void s_s3_meta_request_init_signing_date_time(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->init_signing_date_time);

    meta_request->vtable->init_signing_date_time(meta_request, date_time);
}

void aws_s3_meta_request_init_signing_date_time_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_date_time *date_time) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(date_time);
    (void)meta_request;

    aws_date_time_init_now(date_time);
}

static void s_s3_meta_request_sign_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_signing_complete_fn *on_signing_complete,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->sign_request);

    if (request->send_data.metrics) {
        struct aws_s3_request_metrics *metric = request->send_data.metrics;
        aws_high_res_clock_get_ticks((uint64_t *)&metric->time_metrics.sign_start_timestamp_ns);
    }

    meta_request->vtable->sign_request(meta_request, request, on_signing_complete, user_data);
}

struct aws_get_s3express_credentials_user_data {
    /* Keep our own reference to allocator, because the meta request can be gone after the callback invoked. */
    struct aws_allocator *allocator;

    struct aws_s3_meta_request *meta_request;
    struct aws_s3_request *request;
    aws_signing_complete_fn *on_signing_complete;

    const struct aws_credentials *original_credentials;

    struct aws_signing_config_aws base_signing_config;
    struct aws_credentials_properties_s3express properties;
    void *user_data;
};

static void s_aws_get_s3express_credentials_user_data_destroy(struct aws_get_s3express_credentials_user_data *context) {
    aws_s3_meta_request_release(context->meta_request);
    aws_credentials_release(context->original_credentials);
    aws_mem_release(context->allocator, context);
}

static void s_get_s3express_credentials_callback(struct aws_credentials *credentials, int error_code, void *user_data) {
    struct aws_get_s3express_credentials_user_data *context = user_data;
    struct aws_signing_config_aws signing_config = context->base_signing_config;

    if (error_code) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Failed to get S3 Express credentials %p. due to error code %d (%s)",
            (void *)context->meta_request,
            (void *)context->request,
            error_code,
            aws_error_str(error_code));
        context->on_signing_complete(NULL, error_code, context->user_data);
        goto done;
    }
    s_s3_meta_request_init_signing_date_time(context->meta_request, &signing_config.date);
    /* Override the credentials */
    signing_config.credentials = credentials;
    signing_config.algorithm = AWS_SIGNING_ALGORITHM_V4_S3EXPRESS;
    if (aws_sign_request_aws(
            context->allocator,
            context->request->send_data.signable,
            (struct aws_signing_config_base *)&signing_config,
            context->on_signing_complete,
            context->user_data)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Could not sign request %p. due to error code %d (%s)",
            (void *)context->meta_request,
            (void *)context->request,
            aws_last_error_or_unknown(),
            aws_error_str(aws_last_error_or_unknown()));
        context->on_signing_complete(NULL, aws_last_error_or_unknown(), context->user_data);
    }
done:
    s_aws_get_s3express_credentials_user_data_destroy(context);
}

static void s_get_original_credentials_callback(struct aws_credentials *credentials, int error_code, void *user_data) {
    struct aws_get_s3express_credentials_user_data *context = user_data;
    struct aws_s3_meta_request *meta_request = context->meta_request;
    if (error_code) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Failed to get S3 Express credentials %p. due to error code %d (%s)",
            (void *)context->meta_request,
            (void *)context->request,
            error_code,
            aws_error_str(error_code));
        context->on_signing_complete(NULL, error_code, context->user_data);
        s_aws_get_s3express_credentials_user_data_destroy(context);
        return;
    }
    context->original_credentials = credentials;
    aws_credentials_acquire(context->original_credentials);

    /**
     * Derive the credentials for S3 Express.
     */
    struct aws_s3_client *client = meta_request->client;
    if (aws_s3express_credentials_provider_get_credentials(
            client->s3express_provider,
            context->original_credentials,
            &context->properties,
            s_get_s3express_credentials_callback,
            context)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Could not get S3 Express credentials %p",
            (void *)meta_request,
            (void *)context->request);
        context->on_signing_complete(NULL, aws_last_error_or_unknown(), user_data);
        s_aws_get_s3express_credentials_user_data_destroy(context);
    }
}

static int s_meta_request_resolve_signing_config(
    struct aws_signing_config_aws *out_signing_config,
    struct aws_s3_request *request,
    struct aws_s3_meta_request *meta_request) {
    struct aws_s3_client *client = meta_request->client;
    if (meta_request->cached_signing_config != NULL) {
        *out_signing_config = meta_request->cached_signing_config->config;

        if (out_signing_config->credentials == NULL && out_signing_config->credentials_provider == NULL) {
            /* When no credentials available from meta request level override, we use the credentials from client */
            out_signing_config->credentials = client->cached_signing_config->config.credentials;
            out_signing_config->credentials_provider = client->cached_signing_config->config.credentials_provider;
        }
    } else if (client->cached_signing_config != NULL) {
        *out_signing_config = client->cached_signing_config->config;
    } else {
        /* Not possible to have no cached signing config from both client and request */
        AWS_FATAL_ASSERT(false);
    }

    /* If the checksum is configured to be added to the trailer, the payload will be aws-chunked encoded. The payload
     * will need to be streaming signed/unsigned. */
    if (meta_request->checksum_config.location == AWS_SCL_TRAILER &&
        aws_byte_cursor_eq(&out_signing_config->signed_body_value, &g_aws_signed_body_value_unsigned_payload)) {
        out_signing_config->signed_body_value = g_aws_signed_body_value_streaming_unsigned_payload_trailer;
    }
    /* However the initial request for a multipart upload does not have a trailing checksum and is not chunked so it
     * must have an unsigned_payload signed_body value*/
    if (request->part_number == 0 &&
        aws_byte_cursor_eq(
            &out_signing_config->signed_body_value, &g_aws_signed_body_value_streaming_unsigned_payload_trailer)) {
        out_signing_config->signed_body_value = g_aws_signed_body_value_unsigned_payload;
    }
    return AWS_OP_SUCCESS;
}

/* Handles signing a message for the caller. */
void aws_s3_meta_request_sign_request_default(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    aws_signing_complete_fn *on_signing_complete,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(on_signing_complete);

    struct aws_s3_client *client = meta_request->client;
    AWS_ASSERT(client);

    struct aws_signing_config_aws signing_config;

    if (s_meta_request_resolve_signing_config(&signing_config, request, meta_request)) {
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p: No signing config present. Not signing request %p.",
            (void *)meta_request,
            (void *)request);

        on_signing_complete(NULL, AWS_ERROR_SUCCESS, user_data);
        return;
    }

    request->send_data.signable = aws_signable_new_http_request(meta_request->allocator, request->send_data.message);

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p Created signable %p for request %p with message %p",
        (void *)meta_request,
        (void *)request->send_data.signable,
        (void *)request,
        (void *)request->send_data.message);

    if (request->send_data.signable == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Could not allocate signable for request %p",
            (void *)meta_request,
            (void *)request);

        on_signing_complete(NULL, aws_last_error_or_unknown(), user_data);
        return;
    }

    if (signing_config.algorithm == AWS_SIGNING_ALGORITHM_V4_S3EXPRESS) {
        /* Fetch credentials from S3 Express provider. */
        struct aws_get_s3express_credentials_user_data *context =
            aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_get_s3express_credentials_user_data));

        context->allocator = meta_request->allocator;
        context->base_signing_config = signing_config;
        context->meta_request = aws_s3_meta_request_acquire(meta_request);
        context->on_signing_complete = on_signing_complete;
        context->request = request;
        context->user_data = user_data;
        context->properties.host = aws_byte_cursor_from_string(meta_request->s3express_session_host);
        context->properties.region = signing_config.region;

        if (signing_config.credentials) {
            context->original_credentials = signing_config.credentials;
            aws_credentials_acquire(context->original_credentials);
            /**
             * Derive the credentials for S3 Express.
             */
            if (aws_s3express_credentials_provider_get_credentials(
                    client->s3express_provider,
                    context->original_credentials,
                    &context->properties,
                    s_get_s3express_credentials_callback,
                    context)) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p: Could not get S3 Express credentials %p",
                    (void *)meta_request,
                    (void *)request);
                on_signing_complete(NULL, aws_last_error_or_unknown(), user_data);
                s_aws_get_s3express_credentials_user_data_destroy(context);
                return;
            }
        } else if (signing_config.credentials_provider) {
            /* Get the credentials from provider first. */
            if (aws_credentials_provider_get_credentials(
                    signing_config.credentials_provider, s_get_original_credentials_callback, context)) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p: Could not get S3 Express credentials %p",
                    (void *)meta_request,
                    (void *)request);
                on_signing_complete(NULL, aws_last_error_or_unknown(), user_data);
                s_aws_get_s3express_credentials_user_data_destroy(context);
                return;
            }
        }
    } else {
        /* Regular signing. */
        s_s3_meta_request_init_signing_date_time(meta_request, &signing_config.date);
        if (aws_sign_request_aws(
                meta_request->allocator,
                request->send_data.signable,
                (struct aws_signing_config_base *)&signing_config,
                on_signing_complete,
                user_data)) {

            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST, "id=%p: Could not sign request %p", (void *)meta_request, (void *)request);

            on_signing_complete(NULL, aws_last_error_or_unknown(), user_data);
            return;
        }
    }
}

/* Handle the signing result */
static void s_s3_meta_request_request_on_signed(
    struct aws_signing_result *signing_result,
    int error_code,
    void *user_data) {

    struct aws_s3_prepare_request_payload *payload = user_data;
    AWS_PRECONDITION(payload);

    struct aws_s3_request *request = payload->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    if (error_code != AWS_ERROR_SUCCESS) {
        goto finish;
    }

    if (signing_result != NULL &&
        aws_apply_signing_result_to_http_request(request->send_data.message, meta_request->allocator, signing_result)) {

        error_code = aws_last_error_or_unknown();

        goto finish;
    }

    if (request->send_data.metrics) {
        struct aws_s3_request_metrics *metric = request->send_data.metrics;
        aws_high_res_clock_get_ticks((uint64_t *)&metric->time_metrics.sign_end_timestamp_ns);
        AWS_ASSERT(metric->time_metrics.sign_start_timestamp_ns != 0);
        metric->time_metrics.signing_duration_ns =
            metric->time_metrics.sign_end_timestamp_ns - metric->time_metrics.sign_start_timestamp_ns;
    }

finish:

    if (error_code != AWS_ERROR_SUCCESS) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Meta request could not sign HTTP request due to error code %d (%s)",
            (void *)meta_request,
            error_code,
            aws_error_str(error_code));
    }

    s_s3_prepare_request_payload_callback_and_destroy(payload, error_code);
}

void aws_s3_meta_request_send_request(struct aws_s3_meta_request *meta_request, struct aws_s3_connection *connection) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(connection);
    AWS_PRECONDITION(connection->http_connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    /* Now that we have a signed request and a connection, go ahead and issue the request. */
    struct aws_http_make_request_options options;
    AWS_ZERO_STRUCT(options);

    options.self_size = sizeof(struct aws_http_make_request_options);
    options.request = request->send_data.message;
    options.user_data = connection;
    options.on_response_headers = s_s3_meta_request_incoming_headers;
    options.on_response_header_block_done = s_s3_meta_request_headers_block_done;
    options.on_response_body = s_s3_meta_request_incoming_body;
    if (request->send_data.metrics) {
        options.on_metrics = s_s3_meta_request_stream_metrics;
    }
    options.on_complete = s_s3_meta_request_stream_complete;
    if (request->request_type == AWS_S3_REQUEST_TYPE_UPLOAD_PART) {
        options.response_first_byte_timeout_ms = aws_atomic_load_int(&meta_request->client->upload_timeout_ms);
        request->upload_timeout_ms = (size_t)options.response_first_byte_timeout_ms;
    }

    struct aws_http_stream *stream = aws_http_connection_make_request(connection->http_connection, &options);

    if (stream == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Could not make HTTP request %p", (void *)meta_request, (void *)request);

        goto error_finish;
    }

    AWS_LOGF_TRACE(AWS_LS_S3_META_REQUEST, "id=%p: Sending request %p", (void *)meta_request, (void *)request);

    if (!request->always_send) {
        /* BEGIN CRITICAL SECTION */
        aws_s3_meta_request_lock_synced_data(meta_request);
        if (aws_s3_meta_request_has_finish_result_synced(meta_request)) {
            /* The meta request has finish result already, for this request, treat it as canceled. */
            aws_raise_error(AWS_ERROR_S3_CANCELED);
            aws_s3_meta_request_unlock_synced_data(meta_request);
            goto error_finish;
        }

        /* Activate the stream within the lock as once the activate invoked, the HTTP level callback can happen right
         * after.  */
        if (aws_http_stream_activate(stream) != AWS_OP_SUCCESS) {
            aws_s3_meta_request_unlock_synced_data(meta_request);
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Could not activate HTTP stream %p",
                (void *)meta_request,
                (void *)request);
            goto error_finish;
        }
        aws_linked_list_push_back(
            &meta_request->synced_data.cancellable_http_streams_list, &request->cancellable_http_streams_list_node);
        request->synced_data.cancellable_http_stream = stream;

        aws_s3_meta_request_unlock_synced_data(meta_request);
        /* END CRITICAL SECTION */
    } else {
        /* If the request always send, it is not cancellable. We simply activate the stream. */
        if (aws_http_stream_activate(stream) != AWS_OP_SUCCESS) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Could not activate HTTP stream %p",
                (void *)meta_request,
                (void *)request);
            goto error_finish;
        }
    }
    return;

error_finish:
    if (stream) {
        aws_http_stream_release(stream);
        stream = NULL;
    }

    s_s3_meta_request_send_request_finish(connection, NULL, aws_last_error_or_unknown());
}

static int s_s3_meta_request_error_code_from_response_status(int response_status) {
    int error_code = AWS_ERROR_UNKNOWN;

    switch (response_status) {
        case AWS_HTTP_STATUS_CODE_200_OK:
        case AWS_HTTP_STATUS_CODE_206_PARTIAL_CONTENT:
        case AWS_HTTP_STATUS_CODE_204_NO_CONTENT:
            error_code = AWS_ERROR_SUCCESS;
            break;
        case AWS_HTTP_STATUS_CODE_500_INTERNAL_SERVER_ERROR:
            error_code = AWS_ERROR_S3_INTERNAL_ERROR;
            break;
        case AWS_HTTP_STATUS_CODE_503_SERVICE_UNAVAILABLE:
            /* S3 response 503 for throttling, slow down the sending */
            error_code = AWS_ERROR_S3_SLOW_DOWN;
            break;
        default:
            error_code = AWS_ERROR_S3_INVALID_RESPONSE_STATUS;
            break;
    }

    return error_code;
}

static bool s_header_value_from_list(
    const struct aws_http_header *headers,
    size_t headers_count,
    const struct aws_byte_cursor *name,
    struct aws_byte_cursor *out_value) {
    for (size_t i = 0; i < headers_count; ++i) {
        if (aws_byte_cursor_eq(&headers[i].name, name)) {
            *out_value = headers[i].value;
            return true;
        }
    }
    return false;
}

static void s_get_part_response_headers_checksum_helper(
    struct aws_s3_connection *connection,
    struct aws_s3_meta_request *meta_request,
    const struct aws_http_header *headers,
    size_t headers_count) {
    for (int i = AWS_SCA_INIT; i <= AWS_SCA_END; i++) {
        if (!aws_s3_meta_request_checksum_config_has_algorithm(meta_request, i)) {
            /* If user doesn't select this algorithm, skip */
            continue;
        }
        const struct aws_byte_cursor *algorithm_header_name = aws_get_http_header_name_from_algorithm(i);
        struct aws_byte_cursor header_sum;
        if (s_header_value_from_list(headers, headers_count, algorithm_header_name, &header_sum)) {
            size_t encoded_len = 0;
            aws_base64_compute_encoded_len(aws_get_digest_size_from_algorithm(i), &encoded_len);
            if (header_sum.len == encoded_len - 1) {
                aws_byte_buf_init_copy_from_cursor(
                    &connection->request->request_level_response_header_checksum, meta_request->allocator, header_sum);
                connection->request->request_level_running_response_sum = aws_checksum_new(meta_request->allocator, i);
            }
            break;
        }
    }
}

/* warning this might get screwed up with retries/restarts */
static void s_get_part_response_body_checksum_helper(
    struct aws_s3_checksum *running_response_sum,
    const struct aws_byte_cursor *body) {
    if (running_response_sum) {
        aws_checksum_update(running_response_sum, body);
    }
}

static void s_get_response_part_finish_checksum_helper(struct aws_s3_connection *connection, int error_code) {
    struct aws_byte_buf response_body_sum;
    struct aws_byte_buf encoded_response_body_sum;
    AWS_ZERO_STRUCT(response_body_sum);
    AWS_ZERO_STRUCT(encoded_response_body_sum);

    struct aws_s3_request *request = connection->request;
    if (error_code == AWS_OP_SUCCESS && request->request_level_running_response_sum) {
        size_t encoded_checksum_len = 0;
        request->did_validate = true;
        aws_base64_compute_encoded_len(request->request_level_running_response_sum->digest_size, &encoded_checksum_len);
        aws_byte_buf_init(&encoded_response_body_sum, request->allocator, encoded_checksum_len);
        aws_byte_buf_init(
            &response_body_sum, request->allocator, request->request_level_running_response_sum->digest_size);
        aws_checksum_finalize(request->request_level_running_response_sum, &response_body_sum, 0);
        struct aws_byte_cursor response_body_sum_cursor = aws_byte_cursor_from_buf(&response_body_sum);
        aws_base64_encode(&response_body_sum_cursor, &encoded_response_body_sum);
        request->checksum_match =
            aws_byte_buf_eq(&encoded_response_body_sum, &request->request_level_response_header_checksum);

        request->validation_algorithm = request->request_level_running_response_sum->algorithm;
        aws_byte_buf_clean_up(&response_body_sum);
        aws_byte_buf_clean_up(&encoded_response_body_sum);
    } else {
        request->did_validate = false;
    }
    aws_checksum_destroy(request->request_level_running_response_sum);
    aws_byte_buf_clean_up(&request->request_level_response_header_checksum);
    request->request_level_running_response_sum = NULL;
}

static int s_s3_meta_request_incoming_headers(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    const struct aws_http_header *headers,
    size_t headers_count,
    void *user_data) {
    (void)header_block;

    AWS_PRECONDITION(stream);

    struct aws_s3_connection *connection = user_data;
    AWS_PRECONDITION(connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    if (aws_http_stream_get_incoming_response_status(stream, &request->send_data.response_status)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not get incoming response status for request %p",
            (void *)meta_request,
            (void *)request);
    }
    if (request->send_data.metrics) {
        /* Record the headers to the metrics */
        struct aws_s3_request_metrics *s3_metrics = request->send_data.metrics;
        if (s3_metrics->req_resp_info_metrics.response_headers == NULL) {
            s3_metrics->req_resp_info_metrics.response_headers = aws_http_headers_new(meta_request->allocator);
        }

        for (size_t i = 0; i < headers_count; ++i) {
            const struct aws_byte_cursor *name = &headers[i].name;
            const struct aws_byte_cursor *value = &headers[i].value;
            if (aws_byte_cursor_eq(name, &g_request_id_header_name)) {
                s3_metrics->req_resp_info_metrics.request_id =
                    aws_string_new_from_cursor(connection->request->allocator, value);
            }

            aws_http_headers_add(s3_metrics->req_resp_info_metrics.response_headers, *name, *value);
        }
        s3_metrics->req_resp_info_metrics.response_status = request->send_data.response_status;
    }

    bool successful_response =
        s_s3_meta_request_error_code_from_response_status(request->send_data.response_status) == AWS_ERROR_SUCCESS;

    if (successful_response && meta_request->checksum_config.validate_response_checksum &&
        request->request_type == AWS_S3_REQUEST_TYPE_GET_OBJECT) {
        s_get_part_response_headers_checksum_helper(connection, meta_request, headers, headers_count);
    }

    /* Only record headers if an error has taken place, or if the request_desc has asked for them. */
    bool should_record_headers = !successful_response || request->record_response_headers;

    if (should_record_headers) {
        if (request->send_data.response_headers == NULL) {
            request->send_data.response_headers = aws_http_headers_new(meta_request->allocator);
        }

        for (size_t i = 0; i < headers_count; ++i) {
            const struct aws_byte_cursor *name = &headers[i].name;
            const struct aws_byte_cursor *value = &headers[i].value;

            aws_http_headers_add(request->send_data.response_headers, *name, *value);
        }
    }

    return AWS_OP_SUCCESS;
}

static int s_s3_meta_request_headers_block_done(
    struct aws_http_stream *stream,
    enum aws_http_header_block header_block,
    void *user_data) {
    (void)stream;

    if (header_block != AWS_HTTP_HEADER_BLOCK_MAIN) {
        return AWS_OP_SUCCESS;
    }

    struct aws_s3_connection *connection = user_data;
    AWS_PRECONDITION(connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    /*
     * When downloading parts via partNumber, if the size is larger than expected, cancel the request immediately so we
     * don't end up downloading more into memory than we can handle. We'll retry the download using ranged gets instead.
     */
    if (request->request_type == AWS_S3_REQUEST_TYPE_GET_OBJECT &&
        request->request_tag == AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_GET_OBJECT_WITH_PART_NUMBER_1) {
        uint64_t content_length;
        if (!aws_s3_parse_content_length_response_header(
                request->allocator, request->send_data.response_headers, &content_length) &&
            content_length > meta_request->part_size) {
            return aws_raise_error(AWS_ERROR_S3_INTERNAL_PART_SIZE_MISMATCH_RETRYING_WITH_RANGE);
        }
    }
    return AWS_OP_SUCCESS;
}

/*
 * Small helper to either do a static or dynamic append.
 * TODO: something like this would be useful in common.
 */
static int s_response_body_append(struct aws_byte_buf *buf, const struct aws_byte_cursor *data) {
    return buf->allocator != NULL ? aws_byte_buf_append_dynamic(buf, data) : aws_byte_buf_append(buf, data);
}

static int s_s3_meta_request_incoming_body(
    struct aws_http_stream *stream,
    const struct aws_byte_cursor *data,
    void *user_data) {
    (void)stream;

    struct aws_s3_connection *connection = user_data;
    AWS_PRECONDITION(connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);

    AWS_LOGF_TRACE(
        AWS_LS_S3_META_REQUEST,
        "id=%p Incoming body for request %p. Response status: %d. Data Size: %" PRIu64 ". connection: %p.",
        (void *)meta_request,
        (void *)request,
        request->send_data.response_status,
        (uint64_t)data->len,
        (void *)connection);
    bool successful_response =
        s_s3_meta_request_error_code_from_response_status(request->send_data.response_status) == AWS_ERROR_SUCCESS;
    if (!successful_response) {
        AWS_LOGF_TRACE(AWS_LS_S3_META_REQUEST, "response body: \n" PRInSTR "\n", AWS_BYTE_CURSOR_PRI(*data));
    }

    if (meta_request->checksum_config.validate_response_checksum) {
        s_get_part_response_body_checksum_helper(request->request_level_running_response_sum, data);
    }

    if (request->send_data.response_body.capacity == 0) {
        if (request->has_part_size_response_body && request->ticket != NULL) {
            request->send_data.response_body =
                aws_s3_buffer_pool_acquire_buffer(request->meta_request->client->buffer_pool, request->ticket);
        } else {
            size_t buffer_size = s_dynamic_body_initial_buf_size;
            aws_byte_buf_init(&request->send_data.response_body, meta_request->allocator, buffer_size);
        }
    }

    /* Note: not having part sized response body means the buffer is dynamic and
     * can grow. */
    if (s_response_body_append(&request->send_data.response_body, data)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Request %p could not append to response body due to error %d (%s)",
            (void *)meta_request,
            (void *)request,
            aws_last_error_or_unknown(),
            aws_error_str(aws_last_error_or_unknown()));

        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_s3_meta_request_stream_metrics(
    struct aws_http_stream *stream,
    const struct aws_http_stream_metrics *http_metrics,
    void *user_data) {
    (void)stream;
    struct aws_s3_connection *connection = user_data;
    AWS_PRECONDITION(connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);
    AWS_ASSERT(request->send_data.metrics);
    struct aws_s3_request_metrics *s3_metrics = request->send_data.metrics;
    /* Copy over the time metrics from aws_http_stream_metrics to aws_s3_request_metrics */
    s3_metrics->time_metrics.send_start_timestamp_ns = http_metrics->send_start_timestamp_ns;
    s3_metrics->time_metrics.send_end_timestamp_ns = http_metrics->send_end_timestamp_ns;
    s3_metrics->time_metrics.sending_duration_ns = http_metrics->sending_duration_ns;
    s3_metrics->time_metrics.receive_start_timestamp_ns = http_metrics->receive_start_timestamp_ns;
    s3_metrics->time_metrics.receive_end_timestamp_ns = http_metrics->receive_end_timestamp_ns;
    s3_metrics->time_metrics.receiving_duration_ns = http_metrics->receiving_duration_ns;

    s3_metrics->crt_info_metrics.stream_id = http_metrics->stream_id;

    /* Also related metrics from the request/response. */
    s3_metrics->crt_info_metrics.connection_id = (void *)connection->http_connection;
    const struct aws_socket_endpoint *endpoint = aws_http_connection_get_remote_endpoint(connection->http_connection);
    request->send_data.metrics->crt_info_metrics.ip_address =
        aws_string_new_from_c_str(request->allocator, endpoint->address);
    AWS_ASSERT(request->send_data.metrics->crt_info_metrics.ip_address != NULL);

    s3_metrics->crt_info_metrics.thread_id = aws_thread_current_thread_id();
}

/* Finish up the processing of the request work. */
static void s_s3_meta_request_stream_complete(struct aws_http_stream *stream, int error_code, void *user_data) {

    struct aws_s3_connection *connection = user_data;
    AWS_PRECONDITION(connection);
    struct aws_s3_request *request = connection->request;
    struct aws_s3_meta_request *meta_request = request->meta_request;

    if (meta_request->checksum_config.validate_response_checksum) {
        s_get_response_part_finish_checksum_helper(connection, error_code);
    }
    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_meta_request_lock_synced_data(meta_request);
        if (request->synced_data.cancellable_http_stream) {
            aws_linked_list_remove(&request->cancellable_http_streams_list_node);
            request->synced_data.cancellable_http_stream = NULL;
        }
        aws_s3_meta_request_unlock_synced_data(meta_request);
    }
    /* END CRITICAL SECTION */
    s_s3_meta_request_send_request_finish(connection, stream, error_code);
}

static void s_s3_meta_request_send_request_finish(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code) {
    AWS_PRECONDITION(connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_meta_request_vtable *vtable = meta_request->vtable;
    AWS_PRECONDITION(vtable);

    vtable->send_request_finish(connection, stream, error_code);
}

/* Return whether the response to this request might contain an error, even though we got 200 OK.
 * see: https://repost.aws/knowledge-center/s3-resolve-200-internalerror */
static bool s_should_check_for_error_despite_200_OK(const struct aws_s3_request *request) {
    /* We handle async error for every request BUT get object. */
    struct aws_s3_meta_request *meta_request = request->meta_request;
    if (meta_request->type == AWS_S3_META_REQUEST_TYPE_GET_OBJECT) {
        return false;
    }
    return true;
}

static int s_s3_meta_request_error_code_from_response(struct aws_s3_request *request) {
    AWS_PRECONDITION(request);

    int error_code_from_status = s_s3_meta_request_error_code_from_response_status(request->send_data.response_status);

    /* Response body might be XML with an <Error><Code> inside.
     * The is very likely when status-code is bad.
     * In some cases, it's even possible after 200 OK. */
    int error_code_from_xml = AWS_ERROR_SUCCESS;
    if (error_code_from_status != AWS_ERROR_SUCCESS || s_should_check_for_error_despite_200_OK(request)) {
        if (request->send_data.response_body.len > 0) {
            /* Attempt to read as XML, it's fine if this fails. */
            struct aws_byte_cursor xml_doc = aws_byte_cursor_from_buf(&request->send_data.response_body);
            struct aws_byte_cursor error_code_string = {0};
            const char *xml_path[] = {"Error", "Code", NULL};
            if (aws_xml_get_body_at_path(request->allocator, xml_doc, xml_path, &error_code_string) == AWS_OP_SUCCESS) {
                /* Found an <Error><Code> string! Map it to CRT error code. */
                error_code_from_xml = aws_s3_crt_error_code_from_server_error_code_string(error_code_string);
            }
        }
    }

    if (error_code_from_status == AWS_ERROR_SUCCESS) {
        /* Status-code was OK, so assume everything's good, unless we found an <Error><Code> in the XML */
        switch (error_code_from_xml) {
            case AWS_ERROR_SUCCESS:
                return AWS_ERROR_SUCCESS;
            case AWS_ERROR_UNKNOWN:
                return AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR;
            default:
                return error_code_from_xml;
        }
    } else {
        /* Return error based on status-code, unless we got something more specific from XML */
        switch (error_code_from_xml) {
            case AWS_ERROR_SUCCESS:
                return error_code_from_status;
            case AWS_ERROR_UNKNOWN:
                return error_code_from_status;
            default:
                return error_code_from_xml;
        }
    }
}

void aws_s3_meta_request_send_request_finish_default(
    struct aws_s3_connection *connection,
    struct aws_http_stream *stream,
    int error_code) {

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_client *client = meta_request->client;
    AWS_PRECONDITION(client);

    int response_status = request->send_data.response_status;
    /* If our error code is currently success, then we have some other calls to make that could still indicate a
     * failure. */
    if (error_code == AWS_ERROR_SUCCESS) {
        error_code = s_s3_meta_request_error_code_from_response(request);
        if (error_code != AWS_ERROR_SUCCESS) {
            aws_raise_error(error_code);
        }
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Request %p finished with error code %d (%s) and response status %d",
        (void *)meta_request,
        (void *)request,
        error_code,
        aws_error_debug_str(error_code),
        response_status);

    enum aws_s3_connection_finish_code finish_code = AWS_S3_CONNECTION_FINISH_CODE_FAILED;

    if (error_code == AWS_ERROR_SUCCESS) {
        if (connection->request->meta_request->type == AWS_S3_META_REQUEST_TYPE_GET_OBJECT && request->did_validate &&
            !request->checksum_match) {
            finish_code = AWS_S3_CONNECTION_FINISH_CODE_FAILED;
            error_code = AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH;
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "id=%p Meta request cannot recover from checksum mismatch. (request=%p, response status=%d)",
                (void *)meta_request,
                (void *)request,
                response_status);
        } else {
            finish_code = AWS_S3_CONNECTION_FINISH_CODE_SUCCESS;
        }

    } else {
        /* BEGIN CRITICAL SECTION */
        aws_s3_meta_request_lock_synced_data(meta_request);
        bool meta_request_finishing = aws_s3_meta_request_has_finish_result_synced(meta_request);
        aws_s3_meta_request_unlock_synced_data(meta_request);
        /* END CRITICAL SECTION */

        /* If the request failed due to an invalid (ie: unrecoverable) response status, or the meta request already
         * has a result, then make sure that this request isn't retried. */
        if (error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS ||
            error_code == AWS_ERROR_S3_INTERNAL_PART_SIZE_MISMATCH_RETRYING_WITH_RANGE ||
            error_code == AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR || meta_request_finishing) {
            finish_code = AWS_S3_CONNECTION_FINISH_CODE_FAILED;
            if (error_code == AWS_ERROR_S3_INTERNAL_PART_SIZE_MISMATCH_RETRYING_WITH_RANGE) {
                /* Log at info level instead of error as it's expected and not a fatal error */
                AWS_LOGF_INFO(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Cancelling the request because of error %d (%s). (request=%p, response status=%d)",
                    (void *)meta_request,
                    error_code,
                    aws_error_str(error_code),
                    (void *)request,
                    response_status);
            } else {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Meta request cannot recover from error %d (%s). (request=%p, response status=%d)",
                    (void *)meta_request,
                    error_code,
                    aws_error_str(error_code),
                    (void *)request,
                    response_status);
            }

        } else {
            if (error_code == AWS_ERROR_HTTP_RESPONSE_FIRST_BYTE_TIMEOUT) {
                /* Log at info level instead of error as it's somewhat expected. */
                AWS_LOGF_INFO(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Request failed from error %d (%s). (request=%p). Try to setup a retry.",
                    (void *)meta_request,
                    error_code,
                    aws_error_str(error_code),
                    (void *)request);
            } else {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Request failed from error %d (%s). (request=%p, response status=%d). Try to setup a "
                    "retry.",
                    (void *)meta_request,
                    error_code,
                    aws_error_str(error_code),
                    (void *)request,
                    response_status);
            }

            /* Otherwise, set this up for a retry if the meta request is active. */
            finish_code = AWS_S3_CONNECTION_FINISH_CODE_RETRY;
        }
    }

    if (stream != NULL) {
        aws_http_stream_release(stream);
        stream = NULL;
    }

    aws_s3_client_notify_connection_finished(client, connection, error_code, finish_code);
}

void aws_s3_meta_request_finished_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->finished_request);

    meta_request->vtable->finished_request(meta_request, request, error_code);
}

/* Pushes a request into the body streaming priority queue. Derived meta request types should not call this--they
 * should instead call aws_s3_meta_request_stream_response_body_synced.*/
static void s_s3_meta_request_body_streaming_push_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

/* Pops the next available request from the body streaming priority queue. If the parts previous the next request in
 * the priority queue have not been placed in the priority queue yet, the priority queue will remain the same, and
 * NULL will be returned. (Should not be needed to be called by derived types.) */
static struct aws_s3_request *s_s3_meta_request_body_streaming_pop_next_synced(
    struct aws_s3_meta_request *meta_request);

static void s_s3_meta_request_event_delivery_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

void aws_s3_meta_request_stream_response_body_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {

    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);
    AWS_PRECONDITION(request->part_number > 0);

    /* Push it into the priority queue. */
    s_s3_meta_request_body_streaming_push_synced(meta_request, request);

    struct aws_s3_client *client = meta_request->client;
    AWS_PRECONDITION(client);
    aws_atomic_fetch_add(&client->stats.num_requests_stream_queued_waiting, 1);

    /* Grab any requests that can be streamed back to the caller
     * and send them for delivery on io_event_loop thread. */
    uint32_t num_streaming_requests = 0;
    struct aws_s3_request *next_streaming_request;
    while ((next_streaming_request = s_s3_meta_request_body_streaming_pop_next_synced(meta_request)) != NULL) {
        struct aws_s3_meta_request_event event = {.type = AWS_S3_META_REQUEST_EVENT_RESPONSE_BODY};
        event.u.response_body.completed_request = next_streaming_request;
        aws_s3_meta_request_add_event_for_delivery_synced(meta_request, &event);

        ++num_streaming_requests;
    }

    if (num_streaming_requests == 0) {
        return;
    }

    aws_atomic_fetch_add(&client->stats.num_requests_streaming_response, num_streaming_requests);
    aws_atomic_fetch_sub(&client->stats.num_requests_stream_queued_waiting, num_streaming_requests);

    meta_request->synced_data.num_parts_delivery_sent += num_streaming_requests;
}

void aws_s3_meta_request_add_event_for_delivery_synced(
    struct aws_s3_meta_request *meta_request,
    const struct aws_s3_meta_request_event *event) {

    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    aws_array_list_push_back(&meta_request->synced_data.event_delivery_array, event);

    /* If the array was empty before, schedule task to deliver all events in the array.
     * If the array already had things in it, then the task is already scheduled and will run soon. */
    if (aws_array_list_length(&meta_request->synced_data.event_delivery_array) == 1) {
        aws_s3_meta_request_acquire(meta_request);

        aws_task_init(
            &meta_request->synced_data.event_delivery_task,
            s_s3_meta_request_event_delivery_task,
            meta_request,
            "s3_meta_request_event_delivery");
        aws_event_loop_schedule_task_now(meta_request->io_event_loop, &meta_request->synced_data.event_delivery_task);
    }
}

bool aws_s3_meta_request_are_events_out_for_delivery_synced(struct aws_s3_meta_request *meta_request) {
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);
    return aws_array_list_length(&meta_request->synced_data.event_delivery_array) > 0 ||
           meta_request->synced_data.event_delivery_active;
}

void aws_s3_meta_request_cancel_cancellable_requests_synced(struct aws_s3_meta_request *meta_request, int error_code) {
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);
    while (!aws_linked_list_empty(&meta_request->synced_data.cancellable_http_streams_list)) {
        struct aws_linked_list_node *request_node =
            aws_linked_list_pop_front(&meta_request->synced_data.cancellable_http_streams_list);
        struct aws_s3_request *request =
            AWS_CONTAINER_OF(request_node, struct aws_s3_request, cancellable_http_streams_list_node);
        AWS_ASSERT(!request->always_send);

        aws_http_stream_cancel(request->synced_data.cancellable_http_stream, error_code);
        request->synced_data.cancellable_http_stream = NULL;
    }
}

static struct aws_s3_request_metrics *s_s3_request_finish_up_and_release_metrics(
    struct aws_s3_request_metrics *metrics,
    struct aws_s3_meta_request *meta_request) {

    if (metrics != NULL) {
        /* Request is done streaming the body, complete the metrics for the request now. */

        if (metrics->time_metrics.end_timestamp_ns == -1) {
            aws_high_res_clock_get_ticks((uint64_t *)&metrics->time_metrics.end_timestamp_ns);
            metrics->time_metrics.total_duration_ns =
                metrics->time_metrics.end_timestamp_ns - metrics->time_metrics.start_timestamp_ns;
        }

        if (meta_request->telemetry_callback != NULL) {
            /* We already in the meta request event thread, invoke the telemetry callback directly */
            meta_request->telemetry_callback(meta_request, metrics, meta_request->user_data);
        }
        aws_s3_request_metrics_release(metrics);
    }
    return NULL;
}

/* Deliver events in event_delivery_array.
 * This task runs on the meta-request's io_event_loop thread. */
static void s_s3_meta_request_event_delivery_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    (void)task;
    (void)task_status;
    struct aws_s3_meta_request *meta_request = arg;
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);

    struct aws_s3_client *client = meta_request->client;
    AWS_PRECONDITION(client);

    /* Client owns this event loop group. A cancel should not be possible. */
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    /* Swap contents of synced_data.event_delivery_array into this pre-allocated array-list, then process events */
    struct aws_array_list *event_delivery_array = &meta_request->io_threaded_data.event_delivery_array;
    AWS_FATAL_ASSERT(aws_array_list_length(event_delivery_array) == 0);

    /* If an error occurs, don't fire callbacks anymore. */
    int error_code = AWS_ERROR_SUCCESS;
    uint32_t num_parts_delivered = 0;

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_meta_request_lock_synced_data(meta_request);

        aws_array_list_swap_contents(event_delivery_array, &meta_request->synced_data.event_delivery_array);
        meta_request->synced_data.event_delivery_active = true;

        if (aws_s3_meta_request_has_finish_result_synced(meta_request)) {
            error_code = AWS_ERROR_S3_CANCELED;
        }

        aws_s3_meta_request_unlock_synced_data(meta_request);
    }
    /* END CRITICAL SECTION */

    /* Deliver all events */
    for (size_t event_i = 0; event_i < aws_array_list_length(event_delivery_array); ++event_i) {
        struct aws_s3_meta_request_event event;
        aws_array_list_get_at(event_delivery_array, &event, event_i);
        switch (event.type) {

            case AWS_S3_META_REQUEST_EVENT_RESPONSE_BODY: {
                struct aws_s3_request *request = event.u.response_body.completed_request;
                AWS_ASSERT(meta_request == request->meta_request);
                struct aws_byte_cursor response_body = aws_byte_cursor_from_buf(&request->send_data.response_body);

                AWS_ASSERT(request->part_number >= 1);

                if (error_code == AWS_ERROR_SUCCESS && response_body.len > 0 && meta_request->body_callback != NULL) {
                    if (meta_request->body_callback(
                            meta_request, &response_body, request->part_range_start, meta_request->user_data)) {

                        error_code = aws_last_error_or_unknown();
                        AWS_LOGF_ERROR(
                            AWS_LS_S3_META_REQUEST,
                            "id=%p Response body callback raised error %d (%s).",
                            (void *)meta_request,
                            error_code,
                            aws_error_str(error_code));
                    }
                }
                aws_atomic_fetch_sub(&client->stats.num_requests_streaming_response, 1);

                ++num_parts_delivered;
                request->send_data.metrics =
                    s_s3_request_finish_up_and_release_metrics(request->send_data.metrics, meta_request);

                aws_s3_request_release(request);
            } break;

            case AWS_S3_META_REQUEST_EVENT_PROGRESS: {
                if (error_code == AWS_ERROR_SUCCESS && meta_request->progress_callback != NULL) {
                    /* Don't report 0 byte progress events.
                     * The reasoning behind this is:
                     *
                     * In some code paths, when no data is transferred, there are no progress events,
                     * but in other code paths there might be one progress event of 0 bytes.
                     * We want to be consistent, either:
                     * - REPORT AT LEAST ONCE: even if no data is being transferred.
                     *   This would require finding every code path where no progress events are sent,
                     *   and sending an appropriate progress event, even if it's for 0 bytes.
                     *   One example of ending early is: when resuming a paused upload,
                     *   we do ListParts on the UploadID, and if that 404s we assume the
                     *   previous "paused" meta-request actually completed,
                     *   and so we immediately end the "resuming" meta-request
                     *   as successful without sending any further HTTP requests.
                     *   It would be tough to accurately report progress here because
                     *   we don't know the total size, since we never read the request body,
                     *   and didn't get any info about the previous upload.
                     * OR
                     * - NEVER REPORT ZERO BYTES: even if that means no progress events at all.
                     *   This is easy to do. We'd only send progress events when data is transferred,
                     *   and if a 0 byte event slips through somehow, just check before firing the callback.
                     * Since the NEVER REPORT ZERO BYTES path is simpler to implement, we went with that. */
                    if (event.u.progress.info.bytes_transferred > 0) {
                        meta_request->progress_callback(meta_request, &event.u.progress.info, meta_request->user_data);
                    }
                }
            } break;

            case AWS_S3_META_REQUEST_EVENT_TELEMETRY: {
                struct aws_s3_request_metrics *metrics = event.u.telemetry.metrics;
                AWS_FATAL_ASSERT(meta_request->telemetry_callback != NULL);
                AWS_FATAL_ASSERT(metrics != NULL);

                event.u.telemetry.metrics =
                    s_s3_request_finish_up_and_release_metrics(event.u.telemetry.metrics, meta_request);
            } break;

            default:
                AWS_FATAL_ASSERT(false);
        }
    }

    /* Done delivering events */
    aws_array_list_clear(event_delivery_array);

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_meta_request_lock_synced_data(meta_request);
        if (error_code != AWS_ERROR_SUCCESS) {
            aws_s3_meta_request_set_fail_synced(meta_request, NULL, error_code);
        }

        meta_request->synced_data.num_parts_delivery_completed += num_parts_delivered;
        meta_request->synced_data.event_delivery_active = false;
        aws_s3_meta_request_unlock_synced_data(meta_request);
    }
    /* END CRITICAL SECTION */

    aws_s3_client_schedule_process_work(client);
    aws_s3_meta_request_release(meta_request);
}

static void s_s3_meta_request_body_streaming_push_synced(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    AWS_ASSERT(request->meta_request == meta_request);

    aws_s3_request_acquire(request);

    aws_priority_queue_push(&meta_request->synced_data.pending_body_streaming_requests, &request);
}

static struct aws_s3_request *s_s3_meta_request_body_streaming_pop_next_synced(
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(meta_request);

    if (0 == aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests)) {
        return NULL;
    }

    struct aws_s3_request **top_request = NULL;

    aws_priority_queue_top(&meta_request->synced_data.pending_body_streaming_requests, (void **)&top_request);

    AWS_ASSERT(top_request);

    AWS_FATAL_ASSERT(*top_request);

    if ((*top_request)->part_number != meta_request->synced_data.next_streaming_part) {
        return NULL;
    }

    struct aws_s3_request *request = NULL;
    aws_priority_queue_pop(&meta_request->synced_data.pending_body_streaming_requests, (void **)&request);

    ++meta_request->synced_data.next_streaming_part;

    return request;
}

void aws_s3_meta_request_finish(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->vtable);
    AWS_PRECONDITION(meta_request->vtable->finish);

    meta_request->vtable->finish(meta_request);
}

void aws_s3_meta_request_finish_default(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    bool already_finished = false;
    struct aws_linked_list release_request_list;
    aws_linked_list_init(&release_request_list);

    struct aws_future_void *pending_async_write_future = NULL;

    struct aws_s3_meta_request_result finish_result;
    AWS_ZERO_STRUCT(finish_result);

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_meta_request_lock_synced_data(meta_request);

        if (meta_request->synced_data.state == AWS_S3_META_REQUEST_STATE_FINISHED) {
            already_finished = true;
            goto unlock;
        }

        meta_request->synced_data.state = AWS_S3_META_REQUEST_STATE_FINISHED;

        /* Clean out the pending-stream-to-caller priority queue*/
        while (aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests) > 0) {
            struct aws_s3_request *request = NULL;
            aws_priority_queue_pop(&meta_request->synced_data.pending_body_streaming_requests, (void **)&request);
            AWS_FATAL_ASSERT(request != NULL);

            aws_linked_list_push_back(&release_request_list, &request->node);
        }

        /* Clean out any pending async-write future */
        pending_async_write_future = meta_request->synced_data.async_write.future;
        meta_request->synced_data.async_write.future = NULL;

        finish_result = meta_request->synced_data.finish_result;
        AWS_ZERO_STRUCT(meta_request->synced_data.finish_result);

    unlock:
        aws_s3_meta_request_unlock_synced_data(meta_request);
    }
    /* END CRITICAL SECTION */

    if (already_finished) {
        return;
    }

    if (pending_async_write_future != NULL) {
        AWS_LOGF_TRACE(
            AWS_LS_S3_META_REQUEST,
            "id=%p: write future complete due to meta request's early finish",
            (void *)meta_request);
        aws_future_void_set_error(pending_async_write_future, AWS_ERROR_S3_REQUEST_HAS_COMPLETED);
        pending_async_write_future = aws_future_void_release(pending_async_write_future);
    }

    while (!aws_linked_list_empty(&release_request_list)) {
        struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&release_request_list);
        struct aws_s3_request *release_request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);
        AWS_FATAL_ASSERT(release_request != NULL);
        /* This pending-body-streaming request was never moved to the event-delivery queue,
         * so its metrics were never finished. Finish them now. */
        release_request->send_data.metrics =
            s_s3_request_finish_up_and_release_metrics(release_request->send_data.metrics, meta_request);
        aws_s3_request_release(release_request);
    }

    if (meta_request->headers_callback && finish_result.error_response_headers) {
        if (meta_request->headers_callback(
                meta_request,
                finish_result.error_response_headers,
                finish_result.response_status,
                meta_request->user_data)) {
            finish_result.error_code = aws_last_error_or_unknown();
        }
        meta_request->headers_callback = NULL;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Meta request finished with error code %d (%s)",
        (void *)meta_request,
        finish_result.error_code,
        aws_error_str(finish_result.error_code));

    /* As the meta request has been finished with any HTTP message, we can safely release the http message that
     * hold. So that, the downstream high level language doesn't need to wait for shutdown to clean related resource
     * (eg: input stream) */
    meta_request->request_body_async_stream = aws_async_input_stream_release(meta_request->request_body_async_stream);
    meta_request->request_body_parallel_stream =
        aws_parallel_input_stream_release(meta_request->request_body_parallel_stream);
    meta_request->initial_request_message = aws_http_message_release(meta_request->initial_request_message);

    if (meta_request->finish_callback != NULL) {
        meta_request->finish_callback(meta_request, &finish_result, meta_request->user_data);
    }

    aws_s3_meta_request_result_clean_up(meta_request, &finish_result);

    aws_s3_endpoint_release(meta_request->endpoint);
    meta_request->endpoint = NULL;

    meta_request->io_event_loop = NULL;
}

struct aws_future_bool *aws_s3_meta_request_read_body(
    struct aws_s3_meta_request *meta_request,
    uint64_t offset,
    struct aws_byte_buf *buffer) {

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(buffer);

    /* If async-stream, simply call read_to_fill() */
    if (meta_request->request_body_async_stream != NULL) {
        return aws_async_input_stream_read_to_fill(meta_request->request_body_async_stream, buffer);
    }

    /* If parallel-stream, simply call read(), which must fill the buffer and/or EOF */
    if (meta_request->request_body_parallel_stream != NULL) {
        return aws_parallel_input_stream_read(meta_request->request_body_parallel_stream, offset, buffer);
    }

    /* Further techniques are synchronous... */
    struct aws_future_bool *synchronous_read_future = aws_future_bool_new(meta_request->allocator);

    /* If using async-writes, call function which fills the buffer and/or hits EOF  */
    if (meta_request->request_body_using_async_writes == true) {
        bool eof = false;
        if (s_s3_meta_request_read_from_pending_async_writes(meta_request, buffer, &eof) == AWS_OP_SUCCESS) {
            aws_future_bool_set_result(synchronous_read_future, eof);
        } else {
            aws_future_bool_set_error(synchronous_read_future, aws_last_error());
        }
        return synchronous_read_future;
    }

    /* Else synchronous aws_input_stream */
    struct aws_input_stream *synchronous_stream =
        aws_http_message_get_body_stream(meta_request->initial_request_message);
    AWS_FATAL_ASSERT(synchronous_stream);

    /* Keep calling read() until we fill the buffer, or hit EOF */
    struct aws_stream_status status = {.is_end_of_stream = false, .is_valid = true};
    while ((buffer->len < buffer->capacity) && !status.is_end_of_stream) {
        /* Read from stream */
        if (aws_input_stream_read(synchronous_stream, buffer) != AWS_OP_SUCCESS) {
            aws_future_bool_set_error(synchronous_read_future, aws_last_error());
            goto synchronous_read_done;
        }

        /* Check if stream is done */
        if (aws_input_stream_get_status(synchronous_stream, &status) != AWS_OP_SUCCESS) {
            aws_future_bool_set_error(synchronous_read_future, aws_last_error());
            goto synchronous_read_done;
        }
    }

    aws_future_bool_set_result(synchronous_read_future, status.is_end_of_stream);

synchronous_read_done:
    return synchronous_read_future;
}

void aws_s3_meta_request_result_setup(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_result *result,
    struct aws_s3_request *failed_request,
    int response_status,
    int error_code) {

    if (failed_request != NULL) {
        if (failed_request->send_data.response_headers != NULL) {
            result->error_response_headers = failed_request->send_data.response_headers;
            aws_http_headers_acquire(result->error_response_headers);
        }

        if (failed_request->send_data.response_body.capacity > 0) {
            result->error_response_body = aws_mem_calloc(meta_request->allocator, 1, sizeof(struct aws_byte_buf));

            aws_byte_buf_init_copy(
                result->error_response_body, meta_request->allocator, &failed_request->send_data.response_body);
        }

        if (failed_request->operation_name != NULL) {
            result->error_response_operation_name =
                aws_string_new_from_string(meta_request->allocator, failed_request->operation_name);
        }
    }

    result->response_status = response_status;
    result->error_code = error_code;
}

struct aws_future_void *aws_s3_meta_request_write(
    struct aws_s3_meta_request *meta_request,
    struct aws_byte_cursor data,
    bool eof) {

    struct aws_future_void *write_future = aws_future_void_new(meta_request->allocator);

    /* Set this true, while lock is held, if we're ready to send data */
    bool ready_to_send = false;

    /* Set this true, while lock is held, if write() was called illegally
     * and the meta-request should terminate */
    bool illegal_usage_terminate_meta_request = false;

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);

    if (aws_s3_meta_request_has_finish_result_synced(meta_request)) {
        /* The meta-request is already complete */
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Ignoring write(), the meta request is already complete.",
            (void *)meta_request);
        aws_future_void_set_error(write_future, AWS_ERROR_S3_REQUEST_HAS_COMPLETED);

    } else if (!meta_request->request_body_using_async_writes) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Illegal call to write(). The meta-request must be configured to send-using-data-writes.",
            (void *)meta_request);
        illegal_usage_terminate_meta_request = true;

    } else if (meta_request->synced_data.async_write.future != NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Illegal call to write(). The previous write is not complete.",
            (void *)meta_request);
        illegal_usage_terminate_meta_request = true;

    } else if (meta_request->synced_data.async_write.eof) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST, "id=%p: Illegal call to write(). EOF already set.", (void *)meta_request);
        illegal_usage_terminate_meta_request = true;

    } else if (eof || (meta_request->synced_data.async_write.buffered_data.len + data.len >= meta_request->part_size)) {
        /* This write makes us ready to send (EOF, or we have enough data now to send at least 1 part) */
        AWS_LOGF_TRACE(
            AWS_LS_S3_META_REQUEST,
            "id=%p: write(data=%zu, eof=%d) previously-buffered=%zu. Ready to upload part...",
            (void *)meta_request,
            data.len,
            eof,
            meta_request->synced_data.async_write.buffered_data.len);

        meta_request->synced_data.async_write.unbuffered_cursor = data;
        meta_request->synced_data.async_write.eof = eof;
        meta_request->synced_data.async_write.future = aws_future_void_acquire(write_future);
        ready_to_send = true;

    } else {
        /* Can't send yet. Buffer the data and complete its future, so we can get more data */
        AWS_LOGF_TRACE(
            AWS_LS_S3_META_REQUEST,
            "id=%p: write(data=%zu, eof=%d) previously-buffered=%zu. Buffering data, not enough to upload.",
            (void *)meta_request,
            data.len,
            eof,
            meta_request->synced_data.async_write.buffered_data.len);

        /* TODO: something smarter with this buffer: like get it from buffer-pool,
         * or reserve exactly part-size, or reserve exactly how much we need */
        aws_byte_buf_append_dynamic(&meta_request->synced_data.async_write.buffered_data, &data);

        /* TODO: does a future that completes immediately risk stack overflow?
         * If a user does tiny writes, and registers callbacks on the write-future,
         * they'll fire synchronously. If the user repeats, the stack will just grow and grow. */
        aws_future_void_set_result(write_future);
    }

    if (illegal_usage_terminate_meta_request) {
        aws_future_void_set_error(write_future, AWS_ERROR_INVALID_STATE);
        aws_s3_meta_request_set_fail_synced(meta_request, NULL, AWS_ERROR_INVALID_STATE);
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    if (ready_to_send || illegal_usage_terminate_meta_request) {
        /* Schedule the work task, to continue processing the meta-request */
        aws_s3_client_schedule_process_work(meta_request->client);
    }

    return write_future;
}

/* Copy pending async-write data into the buffer.
 * This is only called when there's enough data for the next part. */
static int s_s3_meta_request_read_from_pending_async_writes(
    struct aws_s3_meta_request *meta_request,
    struct aws_byte_buf *dest,
    bool *eof) {

    *eof = false;

    struct aws_future_void *write_future_to_complete = NULL;
    int error_code = 0;

    /* BEGIN CRITICAL SECTION */
    aws_s3_meta_request_lock_synced_data(meta_request);

    /* If user calls aws_s3_meta_request_cancel(), it will synchronously complete any pending async-writes.
     * So if the write-future is unexpectedly gone, that's what happened, don't touch the data. */
    if (meta_request->synced_data.async_write.future == NULL) {
        error_code = AWS_ERROR_S3_CANCELED;
        goto unlock;
    }

    /* Buffered data should not exceed part-size */
    AWS_FATAL_ASSERT(dest->capacity - dest->len >= meta_request->synced_data.async_write.buffered_data.len);

    /* Copy all buffered data */
    aws_byte_buf_write_from_whole_buffer(dest, meta_request->synced_data.async_write.buffered_data);
    meta_request->synced_data.async_write.buffered_data.len = 0;

    /* Copy as much unbuffered data as possible */
    aws_byte_buf_write_to_capacity(dest, &meta_request->synced_data.async_write.unbuffered_cursor);

    /* We should have filled the dest buffer, unless this is the final write */
    AWS_FATAL_ASSERT(dest->len == dest->capacity || meta_request->synced_data.async_write.eof);

    /* If we haven't received EOF, and there's not enough data in unbuffered_cursor to fill another part,
     * then we need to move it into buffered_data, so we can complete the write's future and get more data */
    if (!meta_request->synced_data.async_write.eof &&
        meta_request->synced_data.async_write.unbuffered_cursor.len < meta_request->part_size) {

        aws_byte_buf_append_dynamic(
            &meta_request->synced_data.async_write.buffered_data,
            &meta_request->synced_data.async_write.unbuffered_cursor);
        meta_request->synced_data.async_write.unbuffered_cursor.len = 0;
    }

    /* If all unbuffered data is consumed (we sent it, or buffered it) then complete the write's future */
    if (meta_request->synced_data.async_write.unbuffered_cursor.len == 0) {
        write_future_to_complete = meta_request->synced_data.async_write.future;
        meta_request->synced_data.async_write.future = NULL;

        if (meta_request->synced_data.async_write.eof) {
            *eof = true;
        }
    }
unlock:
    aws_s3_meta_request_unlock_synced_data(meta_request);
    /* END CRITICAL SECTION */

    /* Don't hold locks while completing the future, it might trigger a user callback */
    if (write_future_to_complete != NULL) {
        AWS_LOGF_TRACE(AWS_LS_S3_META_REQUEST, "id=%p: write future complete", (void *)meta_request);
        aws_future_void_set_result(write_future_to_complete);
        aws_future_void_release(write_future_to_complete);
    }

    return error_code == 0 ? AWS_OP_SUCCESS : aws_raise_error(error_code);
}

void aws_s3_meta_request_result_clean_up(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_meta_request_result *result) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(result);

    aws_http_headers_release(result->error_response_headers);

    if (result->error_response_body != NULL) {
        aws_byte_buf_clean_up(result->error_response_body);
        aws_mem_release(meta_request->allocator, result->error_response_body);
    }

    aws_string_destroy(result->error_response_operation_name);

    AWS_ZERO_STRUCT(*result);
}

bool aws_s3_meta_request_checksum_config_has_algorithm(
    struct aws_s3_meta_request *meta_request,
    enum aws_s3_checksum_algorithm algorithm) {
    AWS_PRECONDITION(meta_request);

    switch (algorithm) {
        case AWS_SCA_CRC32C:
            return meta_request->checksum_config.response_checksum_algorithms.crc32c;
        case AWS_SCA_CRC32:
            return meta_request->checksum_config.response_checksum_algorithms.crc32;
        case AWS_SCA_SHA1:
            return meta_request->checksum_config.response_checksum_algorithms.sha1;
        case AWS_SCA_SHA256:
            return meta_request->checksum_config.response_checksum_algorithms.sha256;
        default:
            return false;
    }
}
