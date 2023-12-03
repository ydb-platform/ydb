/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/string.h>
#include <inttypes.h>

#ifdef _MSC_VER
/* sscanf warning (not currently scanning for strings) */
#    pragma warning(disable : 4996)
#endif

const uint32_t s_conservative_max_requests_in_flight = 8;
const struct aws_byte_cursor g_application_xml_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("application/xml");
const struct aws_byte_cursor g_object_size_value = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ActualObjectSize");

static void s_s3_meta_request_auto_ranged_get_destroy(struct aws_s3_meta_request *meta_request);

static bool s_s3_auto_ranged_get_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request);

static int s_s3_auto_ranged_get_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request);

static void s_s3_auto_ranged_get_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code);

static struct aws_s3_meta_request_vtable s_s3_auto_ranged_get_vtable = {
    .update = s_s3_auto_ranged_get_update,
    .send_request_finish = aws_s3_meta_request_send_request_finish_default,
    .prepare_request = s_s3_auto_ranged_get_prepare_request,
    .init_signing_date_time = aws_s3_meta_request_init_signing_date_time_default,
    .sign_request = aws_s3_meta_request_sign_request_default,
    .finished_request = s_s3_auto_ranged_get_request_finished,
    .destroy = s_s3_meta_request_auto_ranged_get_destroy,
    .finish = aws_s3_meta_request_finish_default,
};

static int s_s3_auto_ranged_get_success_status(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_get);

    if (auto_ranged_get->initial_message_has_range_header) {
        return AWS_S3_RESPONSE_STATUS_RANGE_SUCCESS;
    }

    return AWS_S3_RESPONSE_STATUS_SUCCESS;
}

/* Allocate a new auto-ranged-get meta request. */
struct aws_s3_meta_request *aws_s3_meta_request_auto_ranged_get_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    size_t part_size,
    const struct aws_s3_meta_request_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->message);

    struct aws_s3_auto_ranged_get *auto_ranged_get =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_auto_ranged_get));

    /* Try to initialize the base type. */
    if (aws_s3_meta_request_init_base(
            allocator,
            client,
            part_size,
            false,
            options,
            auto_ranged_get,
            &s_s3_auto_ranged_get_vtable,
            &auto_ranged_get->base)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not initialize base type for Auto-Ranged-Get Meta Request.",
            (void *)auto_ranged_get);
        aws_mem_release(allocator, auto_ranged_get);
        return NULL;
    }

    struct aws_http_headers *headers = aws_http_message_get_headers(auto_ranged_get->base.initial_request_message);
    AWS_ASSERT(headers != NULL);

    auto_ranged_get->initial_message_has_range_header = aws_http_headers_has(headers, g_range_header_name);
    auto_ranged_get->initial_message_has_if_match_header = aws_http_headers_has(headers, g_if_match_header_name);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST, "id=%p Created new Auto-Ranged Get Meta Request.", (void *)&auto_ranged_get->base);

    return &auto_ranged_get->base;
}

static void s_s3_meta_request_auto_ranged_get_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    aws_string_destroy(auto_ranged_get->etag);
    aws_mem_release(meta_request->allocator, auto_ranged_get);
}

static bool s_s3_auto_ranged_get_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(out_request);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    struct aws_s3_request *request = NULL;
    bool work_remaining = false;

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_meta_request_lock_synced_data(meta_request);

        /* If nothing has set the "finish result" then this meta request is still in progress, and we can potentially
         * send additional requests. */
        if (!aws_s3_meta_request_has_finish_result_synced(meta_request)) {

            if ((flags & AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE) != 0) {
                uint32_t num_requests_in_flight =
                    (auto_ranged_get->synced_data.num_parts_requested -
                     auto_ranged_get->synced_data.num_parts_completed) +
                    (uint32_t)aws_priority_queue_size(&meta_request->synced_data.pending_body_streaming_requests);

                /* auto-ranged-gets make use of body streaming, which will hold onto response bodies if parts earlier in
                 * the file haven't arrived yet. This can potentially create a lot of backed up requests, causing us to
                 * hit our global request limit. To help mitigate this, when the "conservative" flag is passed in, we
                 * only allow the total amount of requests being sent/streamed to be inside of a set limit.  */
                if (num_requests_in_flight > s_conservative_max_requests_in_flight) {
                    goto has_work_remaining;
                }
            }

            /* If the overall range of the object that we are trying to retrieve isn't known yet, then we need to send a
             * request to figure that out. */
            if (!auto_ranged_get->synced_data.object_range_known) {

                /* If there exists a range header or we require validation of the response checksum, we currently always
                 * do a head request first.
                 * S3 returns the checksum of the entire object from the HEAD response
                 *
                 * For the range header value could be parsed client-side, doing so presents a number of
                 * complications. For example, the given range could be an unsatisfiable range, and might not even
                 * specify a complete range. To keep things simple, we are currently relying on the service to handle
                 * turning the Range header into a Content-Range response header.*/
                bool head_object_required = auto_ranged_get->initial_message_has_range_header != 0 ||
                                            meta_request->checksum_config.validate_response_checksum;

                if (head_object_required) {
                    /* If the head object request hasn't been sent yet, then send it now. */
                    if (!auto_ranged_get->synced_data.head_object_sent) {
                        request = aws_s3_request_new(
                            meta_request,
                            AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_HEAD_OBJECT,
                            0,
                            AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS | AWS_S3_REQUEST_FLAG_PART_SIZE_RESPONSE_BODY);

                        request->discovers_object_size = true;

                        auto_ranged_get->synced_data.head_object_sent = true;
                    }
                } else if (auto_ranged_get->synced_data.num_parts_requested == 0) {
                    /* If we aren't using a head object, then discover the size of the object while trying to get the
                     * first part. */
                    request = aws_s3_request_new(
                        meta_request,
                        AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART,
                        1,
                        AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS | AWS_S3_REQUEST_FLAG_PART_SIZE_RESPONSE_BODY);

                    request->part_range_start = 0;
                    request->part_range_end = meta_request->part_size - 1;
                    request->discovers_object_size = true;

                    ++auto_ranged_get->synced_data.num_parts_requested;
                }

                goto has_work_remaining;
            }

            /* If the object range is known and that range is empty, then we have an empty file to request. */
            if (auto_ranged_get->synced_data.object_range_start == 0 &&
                auto_ranged_get->synced_data.object_range_end == 0) {
                if (auto_ranged_get->synced_data.get_without_range_sent) {
                    if (auto_ranged_get->synced_data.get_without_range_completed) {
                        goto no_work_remaining;
                    } else {
                        goto has_work_remaining;
                    }
                }
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_INITIAL_MESSAGE,
                    0,
                    AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

                auto_ranged_get->synced_data.get_without_range_sent = true;
                goto has_work_remaining;
            }

            /* If there are still more parts to be requested */
            if (auto_ranged_get->synced_data.num_parts_requested < auto_ranged_get->synced_data.total_num_parts) {

                if (meta_request->client->enable_read_backpressure) {
                    /* Don't start a part until we have enough window to send bytes to the user.
                     *
                     * Note that we start a part once we have enough window to deliver ANY of its bytes.
                     * If we waited until the window was large enough for the WHOLE part,
                     * we could end up stuck in a situation where the user is
                     * waiting for more bytes before they'll open the window,
                     * and this implementation is waiting for more window before it will send more parts. */
                    uint64_t read_data_requested =
                        auto_ranged_get->synced_data.num_parts_requested * meta_request->part_size;
                    if (read_data_requested >= meta_request->synced_data.read_window_running_total) {

                        /* Avoid spamming users with this DEBUG message */
                        if (auto_ranged_get->synced_data.read_window_warning_issued == 0) {
                            auto_ranged_get->synced_data.read_window_warning_issued = 1;

                            AWS_LOGF_DEBUG(
                                AWS_LS_S3_META_REQUEST,
                                "id=%p: Download paused because read window is zero. "
                                "You must increment to window to continue.",
                                (void *)meta_request);
                        }

                        goto has_work_remaining;
                    }

                    auto_ranged_get->synced_data.read_window_warning_issued = 0;
                }

                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART,
                    auto_ranged_get->synced_data.num_parts_requested + 1,
                    AWS_S3_REQUEST_FLAG_PART_SIZE_RESPONSE_BODY);

                aws_s3_get_part_range(
                    auto_ranged_get->synced_data.object_range_start,
                    auto_ranged_get->synced_data.object_range_end,
                    meta_request->part_size,
                    request->part_number,
                    &request->part_range_start,
                    &request->part_range_end);

                ++auto_ranged_get->synced_data.num_parts_requested;
                goto has_work_remaining;
            }

            /* If there are parts that have not attempted delivery to the caller, then there is still work being done.
             */
            if (meta_request->synced_data.num_parts_delivery_completed < auto_ranged_get->synced_data.total_num_parts) {
                goto has_work_remaining;
            }
        } else {
            /* Else, if there is a finish result set, make sure that all work-in-progress winds down before the meta
             * request completely exits. */

            if (auto_ranged_get->synced_data.head_object_sent && !auto_ranged_get->synced_data.head_object_completed) {
                goto has_work_remaining;
            }

            /* Wait for all requests to complete (successfully or unsuccessfully) before finishing.*/
            if (auto_ranged_get->synced_data.num_parts_completed < auto_ranged_get->synced_data.num_parts_requested) {
                goto has_work_remaining;
            }

            if (auto_ranged_get->synced_data.get_without_range_sent &&
                !auto_ranged_get->synced_data.get_without_range_completed) {
                goto has_work_remaining;
            }

            /* If some parts are still being delivered to the caller, then wait for those to finish. */
            if (meta_request->synced_data.num_parts_delivery_completed <
                meta_request->synced_data.num_parts_delivery_sent) {
                goto has_work_remaining;
            }
        }

        goto no_work_remaining;

    has_work_remaining:
        work_remaining = true;

        if (request != NULL) {
            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Returning request %p for part %d of %d",
                (void *)meta_request,
                (void *)request,
                request->part_number,
                auto_ranged_get->synced_data.total_num_parts);
        }

    no_work_remaining:

        if (!work_remaining) {
            aws_s3_meta_request_set_success_synced(meta_request, s_s3_auto_ranged_get_success_status(meta_request));
            if (auto_ranged_get->synced_data.num_parts_checksum_validated ==
                auto_ranged_get->synced_data.num_parts_requested) {
                /* If we have validated the checksum for every parts, we set the meta request level checksum validation
                 * result.*/
                meta_request->synced_data.finish_result.did_validate = true;
                meta_request->synced_data.finish_result.validation_algorithm = auto_ranged_get->validation_algorithm;
            }
        }

        aws_s3_meta_request_unlock_synced_data(meta_request);
    }
    /* END CRITICAL SECTION */

    if (work_remaining) {
        *out_request = request;
    } else {
        AWS_ASSERT(request == NULL);
        aws_s3_meta_request_finish(meta_request);
    }

    return work_remaining;
}

/* Given a request, prepare it for sending based on its description. */
static int s_s3_auto_ranged_get_prepare_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    /* Generate a new ranged get request based on the original message. */
    struct aws_http_message *message = NULL;
    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;

    switch (request->request_tag) {
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_HEAD_OBJECT:
            /* A head object will be a copy of the original headers but with a HEAD request method. */
            message = aws_s3_message_util_copy_http_message_no_body_all_headers(
                meta_request->allocator, meta_request->initial_request_message);
            if (message) {
                aws_http_message_set_request_method(message, g_head_method);
            }
            break;
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART:
            message = aws_s3_ranged_get_object_message_new(
                meta_request->allocator,
                meta_request->initial_request_message,
                request->part_range_start,
                request->part_range_end);
            break;
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_INITIAL_MESSAGE:
            message = aws_s3_message_util_copy_http_message_no_body_all_headers(
                meta_request->allocator, meta_request->initial_request_message);
            break;
    }

    if (message == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not create message for request with tag %d for auto-ranged-get meta request.",
            (void *)meta_request,
            request->request_tag);
        goto message_alloc_failed;
    }
    if (meta_request->checksum_config.validate_response_checksum) {
        aws_http_headers_set(aws_http_message_get_headers(message), g_request_validation_mode, g_enabled);
    }
    if (!auto_ranged_get->initial_message_has_if_match_header && auto_ranged_get->etag) {
        /* Add the if_match to the request */
        AWS_LOGF_DEBUG(
            AWS_LS_S3_META_REQUEST,
            "id=%p: Added the If-Match header to request %p for part %d",
            (void *)meta_request,
            (void *)request,
            request->part_number);
        aws_http_headers_set(
            aws_http_message_get_headers(message),
            g_if_match_header_name,
            aws_byte_cursor_from_string(auto_ranged_get->etag));
    }

    aws_s3_request_setup_send_data(request, message);
    aws_http_message_release(message);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Created request %p for part %d",
        (void *)meta_request,
        (void *)request,
        request->part_number);

    return AWS_OP_SUCCESS;

message_alloc_failed:

    return AWS_OP_ERR;
}

/* Check the finish result of meta request, in case of the request failed because of downloading an empty file */
static bool s_check_empty_file_download_error(struct aws_s3_request *failed_request) {
    struct aws_http_headers *failed_headers = failed_request->send_data.response_headers;
    struct aws_byte_buf failed_body = failed_request->send_data.response_body;
    if (failed_headers && failed_body.capacity > 0) {
        struct aws_byte_cursor content_type;
        AWS_ZERO_STRUCT(content_type);
        if (!aws_http_headers_get(failed_headers, g_content_type_header_name, &content_type)) {
            /* Content type found */
            if (aws_byte_cursor_eq_ignore_case(&content_type, &g_application_xml_value)) {
                /* XML response */
                struct aws_byte_cursor body_cursor = aws_byte_cursor_from_buf(&failed_body);
                struct aws_string *size =
                    aws_xml_get_top_level_tag(failed_request->allocator, &g_object_size_value, &body_cursor);
                bool check_size = aws_string_eq_c_str(size, "0");
                aws_string_destroy(size);
                if (check_size) {
                    return true;
                }
            }
        }
    }
    return false;
}

static int s_discover_object_range_and_content_length(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    uint64_t *out_total_content_length,
    uint64_t *out_object_range_start,
    uint64_t *out_object_range_end) {
    AWS_PRECONDITION(out_total_content_length);
    AWS_PRECONDITION(out_object_range_start);
    AWS_PRECONDITION(out_object_range_end);

    int result = AWS_OP_ERR;

    uint64_t total_content_length = 0;
    uint64_t object_range_start = 0;
    uint64_t object_range_end = 0;

    AWS_ASSERT(request->discovers_object_size);
    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    switch (request->request_tag) {
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_HEAD_OBJECT:
            if (error_code != AWS_ERROR_SUCCESS) {
                /* If the head request failed, there's nothing we can do, so resurface the error code. */
                aws_raise_error(error_code);
                break;
            }

            /* There should be a Content-Length header that indicates the total size of the range.*/
            if (aws_s3_parse_content_length_response_header(
                    meta_request->allocator, request->send_data.response_headers, &total_content_length)) {

                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not find content-length header for request %p",
                    (void *)meta_request,
                    (void *)request);
                break;
            }

            /* if the inital message had a ranged header, there should also be a Content-Range header that specifies the
             * object range and total object size. Otherwise the size and range should be equal to the
             * total_content_length. */
            if (!auto_ranged_get->initial_message_has_range_header) {
                object_range_end = total_content_length - 1;
            } else if (aws_s3_parse_content_range_response_header(
                           meta_request->allocator,
                           request->send_data.response_headers,
                           &object_range_start,
                           &object_range_end,
                           NULL)) {

                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not find content-range header for request %p",
                    (void *)meta_request,
                    (void *)request);
                break;
            }

            result = AWS_OP_SUCCESS;
            break;
        case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART:
            AWS_ASSERT(request->part_number == 1);

            if (error_code != AWS_ERROR_SUCCESS) {
                /* If we hit an empty file while trying to discover the object-size via part, then this request
                failure
                 * is as designed. */
                if (s_check_empty_file_download_error(request)) {
                    AWS_LOGF_DEBUG(
                        AWS_LS_S3_META_REQUEST,
                        "id=%p Detected empty file with request %p. Sending new request without range header.",
                        (void *)meta_request,
                        (void *)request);

                    total_content_length = 0ULL;

                    result = AWS_OP_SUCCESS;
                } else {
                    /* Otherwise, resurface the error code. */
                    aws_raise_error(error_code);
                }
                break;
            }

            AWS_ASSERT(request->send_data.response_headers != NULL);

            /* Parse the object size from the part response. */
            if (aws_s3_parse_content_range_response_header(
                    meta_request->allocator, request->send_data.response_headers, NULL, NULL, &total_content_length)) {

                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p Could not find content-range header for request %p",
                    (void *)meta_request,
                    (void *)request);

                break;
            }

            /* When discovering the object size via first-part, the object range is the entire object. */
            object_range_start = 0;
            object_range_end = total_content_length - 1;

            result = AWS_OP_SUCCESS;
            break;
        default:
            AWS_ASSERT(false);
            break;
    }

    if (result == AWS_OP_SUCCESS) {
        *out_total_content_length = total_content_length;
        *out_object_range_start = object_range_start;
        *out_object_range_end = object_range_end;
    }

    return result;
}

static void s_s3_auto_ranged_get_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);
    AWS_PRECONDITION(request);

    struct aws_s3_auto_ranged_get *auto_ranged_get = meta_request->impl;
    AWS_PRECONDITION(auto_ranged_get);

    uint64_t total_content_length = 0ULL;
    uint64_t object_range_start = 0ULL;
    uint64_t object_range_end = 0ULL;

    bool found_object_size = false;
    bool request_failed = error_code != AWS_ERROR_SUCCESS;

    if (request->discovers_object_size) {

        /* Try to discover the object-range and content length.*/
        if (s_discover_object_range_and_content_length(
                meta_request, request, error_code, &total_content_length, &object_range_start, &object_range_end)) {

            error_code = aws_last_error_or_unknown();

            goto update_synced_data;
        }

        if (!request_failed && !auto_ranged_get->initial_message_has_if_match_header) {
            AWS_ASSERT(auto_ranged_get->etag == NULL);
            struct aws_byte_cursor etag_header_value;

            if (aws_http_headers_get(request->send_data.response_headers, g_etag_header_name, &etag_header_value)) {
                aws_raise_error(AWS_ERROR_S3_MISSING_ETAG);
                error_code = AWS_ERROR_S3_MISSING_ETAG;
                goto update_synced_data;
            }

            AWS_LOGF_TRACE(
                AWS_LS_S3_META_REQUEST,
                "id=%p Etag received for the meta request. value is: " PRInSTR "",
                (void *)meta_request,
                AWS_BYTE_CURSOR_PRI(etag_header_value));
            auto_ranged_get->etag = aws_string_new_from_cursor(auto_ranged_get->base.allocator, &etag_header_value);
        }

        /* If we were able to discover the object-range/content length successfully, then any error code that was passed
         * into this function is being handled and does not indicate an overall failure.*/
        error_code = AWS_ERROR_SUCCESS;
        found_object_size = true;

        if (meta_request->headers_callback != NULL) {
            struct aws_http_headers *response_headers = aws_http_headers_new(meta_request->allocator);

            copy_http_headers(request->send_data.response_headers, response_headers);

            /* If this request is a part, then the content range isn't applicable. */
            if (request->request_tag == AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART) {
                /* For now, we can assume that discovery of size via the first part of the object does not apply to
                 * breaking up a ranged request. If it ever does, then we will need to repopulate this header. */
                AWS_ASSERT(!auto_ranged_get->initial_message_has_range_header);

                aws_http_headers_erase(response_headers, g_content_range_header_name);
            }

            char content_length_buffer[64] = "";
            snprintf(content_length_buffer, sizeof(content_length_buffer), "%" PRIu64, total_content_length);
            aws_http_headers_set(
                response_headers, g_content_length_header_name, aws_byte_cursor_from_c_str(content_length_buffer));

            if (meta_request->headers_callback(
                    meta_request,
                    response_headers,
                    s_s3_auto_ranged_get_success_status(meta_request),
                    meta_request->user_data)) {

                error_code = aws_last_error_or_unknown();
            }
            meta_request->headers_callback = NULL;

            aws_http_headers_release(response_headers);
        }
    }

update_synced_data:

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_meta_request_lock_synced_data(meta_request);

        /* If the object range was found, then record it. */
        if (found_object_size) {
            AWS_ASSERT(!auto_ranged_get->synced_data.object_range_known);

            auto_ranged_get->synced_data.object_range_known = true;
            auto_ranged_get->synced_data.object_range_start = object_range_start;
            auto_ranged_get->synced_data.object_range_end = object_range_end;
            auto_ranged_get->synced_data.total_num_parts =
                aws_s3_get_num_parts(meta_request->part_size, object_range_start, object_range_end);
        }

        switch (request->request_tag) {
            case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_HEAD_OBJECT:
                auto_ranged_get->synced_data.head_object_completed = true;
                AWS_LOGF_DEBUG(AWS_LS_S3_META_REQUEST, "id=%p Head object completed.", (void *)meta_request);
                break;
            case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_PART:
                ++auto_ranged_get->synced_data.num_parts_completed;

                if (!request_failed) {

                    /* Record the number of parts that checksum has been validated */
                    if (request->did_validate) {
                        if (auto_ranged_get->validation_algorithm == AWS_SCA_NONE) {
                            auto_ranged_get->validation_algorithm = request->validation_algorithm;
                        }
                        /* They should be the same. */
                        AWS_ASSERT(auto_ranged_get->validation_algorithm == request->validation_algorithm);
                        ++auto_ranged_get->synced_data.num_parts_checksum_validated;
                    }
                    ++auto_ranged_get->synced_data.num_parts_successful;

                    aws_s3_meta_request_stream_response_body_synced(meta_request, request);

                    AWS_LOGF_DEBUG(
                        AWS_LS_S3_META_REQUEST,
                        "id=%p: %d out of %d parts have completed.",
                        (void *)meta_request,
                        (auto_ranged_get->synced_data.num_parts_successful +
                         auto_ranged_get->synced_data.num_parts_failed),
                        auto_ranged_get->synced_data.total_num_parts);
                } else {
                    ++auto_ranged_get->synced_data.num_parts_failed;
                }
                break;
            case AWS_S3_AUTO_RANGE_GET_REQUEST_TYPE_INITIAL_MESSAGE:
                AWS_LOGF_DEBUG(
                    AWS_LS_S3_META_REQUEST, "id=%p Get of file using initial message completed.", (void *)meta_request);
                auto_ranged_get->synced_data.get_without_range_completed = true;
                break;
        }

        if (error_code != AWS_ERROR_SUCCESS) {
            if (error_code == AWS_ERROR_S3_INVALID_RESPONSE_STATUS &&
                request->send_data.response_status == AWS_HTTP_STATUS_CODE_412_PRECONDITION_FAILED &&
                !auto_ranged_get->initial_message_has_if_match_header) {
                /* Use more clear error code as we added the if-match header under the hood. */
                error_code = AWS_ERROR_S3_OBJECT_MODIFIED;
            }
            aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            if (error_code == AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH) {
                /* It's a mismatch of checksum, tell user that we validated the checksum and the algorithm we validated
                 */
                meta_request->synced_data.finish_result.did_validate = true;
                meta_request->synced_data.finish_result.validation_algorithm = request->validation_algorithm;
            }
        }

        aws_s3_meta_request_unlock_synced_data(meta_request);
    }
    /* END CRITICAL SECTION */
}
