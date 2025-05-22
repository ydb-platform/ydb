/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_copy_object.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"
#include <aws/common/string.h>
#include <aws/io/stream.h>

/* Objects with size smaller than the constant below are bypassed as S3 CopyObject instead of multipart copy */
static const size_t s_multipart_copy_minimum_object_size = 1L * 1024L * 1024L * 1024L;

static const size_t s_etags_initial_capacity = 16;
static const struct aws_byte_cursor s_upload_id = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("UploadId");
static const size_t s_complete_multipart_upload_init_body_size_bytes = 512;
static const size_t s_abort_multipart_upload_init_body_size_bytes = 512;

static const struct aws_byte_cursor s_create_multipart_upload_copy_headers[] = {
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-algorithm"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-customer-key-MD5"),
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-server-side-encryption-context"),
};

static void s_s3_meta_request_copy_object_destroy(struct aws_s3_meta_request *meta_request);

static bool s_s3_copy_object_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request);

static int s_s3_copy_object_prepare_request(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request);

static void s_s3_copy_object_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code);

static struct aws_s3_meta_request_vtable s_s3_copy_object_vtable = {
    .update = s_s3_copy_object_update,
    .send_request_finish = aws_s3_meta_request_send_request_finish_handle_async_error,
    .prepare_request = s_s3_copy_object_prepare_request,
    .init_signing_date_time = aws_s3_meta_request_init_signing_date_time_default,
    .sign_request = aws_s3_meta_request_sign_request_default,
    .finished_request = s_s3_copy_object_request_finished,
    .destroy = s_s3_meta_request_copy_object_destroy,
    .finish = aws_s3_meta_request_finish_default,
};

/* Allocate a new copy object meta request */
struct aws_s3_meta_request *aws_s3_meta_request_copy_object_new(
    struct aws_allocator *allocator,
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    /* These should already have been validated by the caller. */
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);
    AWS_PRECONDITION(options->message);

    struct aws_s3_copy_object *copy_object = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_copy_object));

    /* part size and content length will be fetched later using a HEAD object request */
    const size_t UNKNOWN_PART_SIZE = 0;
    const size_t UNKNOWN_CONTENT_LENGTH = 0;
    const int UNKNOWN_NUM_PARTS = 0;

    /* TODO Handle and test multipart copy */
    if (aws_s3_meta_request_init_base(
            allocator,
            client,
            UNKNOWN_PART_SIZE,
            false,
            options,
            copy_object,
            &s_s3_copy_object_vtable,
            &copy_object->base)) {
        aws_mem_release(allocator, copy_object);
        return NULL;
    }

    aws_array_list_init_dynamic(
        &copy_object->synced_data.etag_list, allocator, s_etags_initial_capacity, sizeof(struct aws_string *));

    copy_object->synced_data.content_length = UNKNOWN_CONTENT_LENGTH;
    copy_object->synced_data.total_num_parts = UNKNOWN_NUM_PARTS;
    copy_object->threaded_update_data.next_part_number = 1;

    AWS_LOGF_DEBUG(AWS_LS_S3_META_REQUEST, "id=%p Created new CopyObject Meta Request.", (void *)&copy_object->base);

    return &copy_object->base;
}

static void s_s3_meta_request_copy_object_destroy(struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);

    struct aws_s3_copy_object *copy_object = meta_request->impl;

    aws_string_destroy(copy_object->upload_id);
    copy_object->upload_id = NULL;

    for (size_t etag_index = 0; etag_index < aws_array_list_length(&copy_object->synced_data.etag_list); ++etag_index) {
        struct aws_string *etag = NULL;

        aws_array_list_get_at(&copy_object->synced_data.etag_list, &etag, etag_index);
        aws_string_destroy(etag);
    }

    aws_array_list_clean_up(&copy_object->synced_data.etag_list);
    aws_http_headers_release(copy_object->synced_data.needed_response_headers);
    aws_mem_release(meta_request->allocator, copy_object);
}

static bool s_s3_copy_object_update(
    struct aws_s3_meta_request *meta_request,
    uint32_t flags,
    struct aws_s3_request **out_request) {

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(out_request);

    struct aws_s3_request *request = NULL;
    bool work_remaining = false;

    struct aws_s3_copy_object *copy_object = meta_request->impl;

    aws_s3_meta_request_lock_synced_data(meta_request);

    if (!aws_s3_meta_request_has_finish_result_synced(meta_request)) {

        /* If we haven't already sent the GetObject HEAD request to get the source object size, do so now. */
        if (!copy_object->synced_data.head_object_sent) {
            request = aws_s3_request_new(
                meta_request,
                AWS_S3_COPY_OBJECT_REQUEST_TAG_GET_OBJECT_SIZE,
                0,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

            copy_object->synced_data.head_object_sent = true;

            goto has_work_remaining;
        }

        if (!copy_object->synced_data.head_object_completed) {
            /* we have not received the object size response yet */
            goto has_work_remaining;
        }

        if (copy_object->synced_data.content_length < s_multipart_copy_minimum_object_size) {
            /* object is too small to use multipart upload: forwards the original CopyObject request to S3 instead. */
            if (!copy_object->synced_data.copy_request_bypass_sent) {
                request = aws_s3_request_new(
                    meta_request,
                    AWS_S3_COPY_OBJECT_REQUEST_TAG_BYPASS,
                    1,
                    AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

                AWS_LOGF_DEBUG(
                    AWS_LS_S3_META_REQUEST,
                    "id=%p: Meta Request CopyObject created bypass request %p",
                    (void *)meta_request,
                    (void *)request);

                copy_object->synced_data.copy_request_bypass_sent = true;
                goto has_work_remaining;
            }

            /* If the bypass request hasn't been completed, then wait for it to be completed. */
            if (!copy_object->synced_data.copy_request_bypass_completed) {
                goto has_work_remaining;
            } else {
                goto no_work_remaining;
            }
        }

        /* Object size is large enough to use multipart copy. If we haven't already sent a create-multipart-upload
         * message, do so now. */
        if (!copy_object->synced_data.create_multipart_upload_sent) {
            request = aws_s3_request_new(
                meta_request,
                AWS_S3_COPY_OBJECT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD,
                0,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

            copy_object->synced_data.create_multipart_upload_sent = true;
            goto has_work_remaining;
        }

        /* If the create-multipart-upload message hasn't been completed, then there is still additional work to do, but
         * it can't be done yet. */
        if (!copy_object->synced_data.create_multipart_upload_completed) {
            goto has_work_remaining;
        }

        /* If we haven't sent all of the parts yet, then set up to send a new part now. */
        if (copy_object->synced_data.num_parts_sent < copy_object->synced_data.total_num_parts) {

            if ((flags & AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE) != 0) {
                uint32_t num_parts_in_flight =
                    (copy_object->synced_data.num_parts_sent - copy_object->synced_data.num_parts_completed);

                /* TODO: benchmark if there is need to limit the amount of upload part copy in flight requests */
                if (num_parts_in_flight > 0) {
                    goto has_work_remaining;
                }
            }

            /* Allocate a request for another part. */
            request = aws_s3_request_new(
                meta_request,
                AWS_S3_COPY_OBJECT_REQUEST_TAG_MULTIPART_COPY,
                0,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

            request->part_number = copy_object->threaded_update_data.next_part_number;

            ++copy_object->threaded_update_data.next_part_number;
            ++copy_object->synced_data.num_parts_sent;

            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p: Returning request %p for part %d",
                (void *)meta_request,
                (void *)request,
                request->part_number);

            goto has_work_remaining;
        }

        /* There is one more request to send after all of the parts (the complete-multipart-upload) but it can't be done
         * until all of the parts have been completed.*/
        if (copy_object->synced_data.num_parts_completed != copy_object->synced_data.total_num_parts) {
            goto has_work_remaining;
        }

        /* If the complete-multipart-upload request hasn't been set yet, then send it now. */
        if (!copy_object->synced_data.complete_multipart_upload_sent) {
            request = aws_s3_request_new(
                meta_request,
                AWS_S3_COPY_OBJECT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD,
                0,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS);

            copy_object->synced_data.complete_multipart_upload_sent = true;
            goto has_work_remaining;
        }

        /* Wait for the complete-multipart-upload request to finish. */
        if (!copy_object->synced_data.complete_multipart_upload_completed) {
            goto has_work_remaining;
        }

        goto no_work_remaining;
    } else {

        /* If the create multipart upload hasn't been sent, then there is nothing left to do when canceling. */
        if (!copy_object->synced_data.create_multipart_upload_sent) {
            goto no_work_remaining;
        }

        /* If the create-multipart-upload request is still in flight, wait for it to finish. */
        if (!copy_object->synced_data.create_multipart_upload_completed) {
            goto has_work_remaining;
        }

        /* If the number of parts completed is less than the number of parts sent, then we need to wait until all of
         * those parts are done sending before aborting. */
        if (copy_object->synced_data.num_parts_completed < copy_object->synced_data.num_parts_sent) {
            goto has_work_remaining;
        }

        /* If the complete-multipart-upload is already in flight, then we can't necessarily send an abort. */
        if (copy_object->synced_data.complete_multipart_upload_sent &&
            !copy_object->synced_data.complete_multipart_upload_completed) {
            goto has_work_remaining;
        }

        /* If the complete-multipart-upload completed successfully, then there is nothing to abort since the transfer
         * has already finished. */
        if (copy_object->synced_data.complete_multipart_upload_completed &&
            copy_object->synced_data.complete_multipart_upload_error_code == AWS_ERROR_SUCCESS) {
            goto no_work_remaining;
        }

        /* If we made it here, and the abort-multipart-upload message hasn't been sent yet, then do so now. */
        if (!copy_object->synced_data.abort_multipart_upload_sent) {
            if (copy_object->upload_id == NULL) {
                goto no_work_remaining;
            }

            request = aws_s3_request_new(
                meta_request,
                AWS_S3_COPY_OBJECT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD,
                0,
                AWS_S3_REQUEST_FLAG_RECORD_RESPONSE_HEADERS | AWS_S3_REQUEST_FLAG_ALWAYS_SEND);

            copy_object->synced_data.abort_multipart_upload_sent = true;

            goto has_work_remaining;
        }

        /* Wait for the multipart upload to be completed. */
        if (!copy_object->synced_data.abort_multipart_upload_completed) {
            goto has_work_remaining;
        }

        goto no_work_remaining;
    }

has_work_remaining:
    work_remaining = true;

no_work_remaining:

    if (!work_remaining) {
        aws_s3_meta_request_set_success_synced(meta_request, AWS_S3_RESPONSE_STATUS_SUCCESS);
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);

    if (work_remaining) {
        *out_request = request;
    } else {
        AWS_ASSERT(request == NULL);

        aws_s3_meta_request_finish(meta_request);
    }

    return work_remaining;
}

/* Given a request, prepare it for sending based on its description. */
static int s_s3_copy_object_prepare_request(struct aws_s3_meta_request *meta_request, struct aws_s3_request *request) {
    AWS_PRECONDITION(meta_request);

    struct aws_s3_copy_object *copy_object = meta_request->impl;
    AWS_PRECONDITION(copy_object);

    aws_s3_meta_request_lock_synced_data(meta_request);

    struct aws_http_message *message = NULL;

    switch (request->request_tag) {

        /* Prepares the GetObject HEAD request to get the source object size. */
        case AWS_S3_COPY_OBJECT_REQUEST_TAG_GET_OBJECT_SIZE: {
            message = aws_s3_get_source_object_size_message_new(
                meta_request->allocator, meta_request->initial_request_message);
            break;
        }

        /* The S3 object is not large enough for multi-part copy. Bypasses a copy of the original CopyObject request to
         * S3. */
        case AWS_S3_COPY_OBJECT_REQUEST_TAG_BYPASS: {
            message = aws_s3_message_util_copy_http_message_no_body_all_headers(
                meta_request->allocator, meta_request->initial_request_message);
            break;
        }

        /* Prepares the CreateMultipartUpload sub-request. */
        case AWS_S3_COPY_OBJECT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD: {
            uint64_t part_size_uint64 = copy_object->synced_data.content_length / (uint64_t)g_s3_max_num_upload_parts;

            if (part_size_uint64 > SIZE_MAX) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "Could not create multipart copy meta request; required part size of %" PRIu64
                    " bytes is too large for platform.",
                    part_size_uint64);

                aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                return AWS_OP_ERR;
            }

            size_t part_size = (size_t)part_size_uint64;

            const size_t MIN_PART_SIZE = 64L * 1024L * 1024L; /* minimum partition size */
            if (part_size < MIN_PART_SIZE) {
                part_size = MIN_PART_SIZE;
            }

            uint32_t num_parts = (uint32_t)(copy_object->synced_data.content_length / part_size);

            if ((copy_object->synced_data.content_length % part_size) > 0) {
                ++num_parts;
            }

            copy_object->synced_data.total_num_parts = num_parts;
            copy_object->synced_data.part_size = part_size;

            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "Starting multi-part Copy using part size=%zu, total_num_parts=%zu",
                part_size,
                (size_t)num_parts);

            /* Create the message to create a new multipart upload. */
            message = aws_s3_create_multipart_upload_message_new(
                meta_request->allocator,
                meta_request->initial_request_message,
                meta_request->checksum_config.checksum_algorithm);

            break;
        }

        /* Prepares the UploadPartCopy sub-request. */
        case AWS_S3_COPY_OBJECT_REQUEST_TAG_MULTIPART_COPY: {
            /* Create a new uploadPartCopy message to upload a part. */
            /* compute sub-request range */
            uint64_t range_start = (request->part_number - 1) * copy_object->synced_data.part_size;
            uint64_t range_end = range_start + copy_object->synced_data.part_size - 1;
            if (range_end >= copy_object->synced_data.content_length) {
                /* adjust size of last part */
                range_end = copy_object->synced_data.content_length - 1;
            }

            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "Starting UploadPartCopy for partition %" PRIu32 ", range_start=%" PRIu64 ", range_end=%" PRIu64
                ", full object length=%" PRIu64,
                request->part_number,
                range_start,
                range_end,
                copy_object->synced_data.content_length);

            message = aws_s3_upload_part_copy_message_new(
                meta_request->allocator,
                meta_request->initial_request_message,
                &request->request_body,
                request->part_number,
                range_start,
                range_end,
                copy_object->upload_id,
                meta_request->should_compute_content_md5);
            break;
        }

        /* Prepares the CompleteMultiPartUpload sub-request. */
        case AWS_S3_COPY_OBJECT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD: {

            if (request->num_times_prepared == 0) {
                aws_byte_buf_init(
                    &request->request_body, meta_request->allocator, s_complete_multipart_upload_init_body_size_bytes);
            } else {
                aws_byte_buf_reset(&request->request_body, false);
            }

            AWS_FATAL_ASSERT(copy_object->upload_id);
            AWS_ASSERT(request->request_body.capacity > 0);
            aws_byte_buf_reset(&request->request_body, false);

            /* Build the message to complete our multipart upload, which includes a payload describing all of our
             * completed parts. */
            message = aws_s3_complete_multipart_message_new(
                meta_request->allocator,
                meta_request->initial_request_message,
                &request->request_body,
                copy_object->upload_id,
                &copy_object->synced_data.etag_list,
                NULL,
                AWS_SCA_NONE);

            break;
        }

        /* Prepares the AbortMultiPartUpload sub-request. */
        case AWS_S3_COPY_OBJECT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD: {
            AWS_FATAL_ASSERT(copy_object->upload_id);
            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p Abort multipart upload request for upload id %s.",
                (void *)meta_request,
                aws_string_c_str(copy_object->upload_id));

            if (request->num_times_prepared == 0) {
                aws_byte_buf_init(
                    &request->request_body, meta_request->allocator, s_abort_multipart_upload_init_body_size_bytes);
            } else {
                aws_byte_buf_reset(&request->request_body, false);
            }

            /* Build the message to abort our multipart upload */
            message = aws_s3_abort_multipart_upload_message_new(
                meta_request->allocator, meta_request->initial_request_message, copy_object->upload_id);

            break;
        }
    }

    aws_s3_meta_request_unlock_synced_data(meta_request);

    if (message == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_META_REQUEST,
            "id=%p Could not allocate message for request with tag %d for CopyObject meta request.",
            (void *)meta_request,
            request->request_tag);
        goto message_create_failed;
    }

    aws_s3_request_setup_send_data(request, message);

    aws_http_message_release(message);

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p: Prepared request %p for part %d",
        (void *)meta_request,
        (void *)request,
        request->part_number);

    return AWS_OP_SUCCESS;

message_create_failed:

    return AWS_OP_ERR;
}

/* For UploadPartCopy requests, etag is sent in the request body, within XML entity quotes */
static struct aws_string *s_etag_new_from_upload_part_copy_response(
    struct aws_allocator *allocator,
    struct aws_byte_buf *response_body) {
    struct aws_string *etag = NULL;

    struct aws_byte_cursor response_body_cursor = aws_byte_cursor_from_buf(response_body);

    struct aws_string *etag_within_xml_quotes =
        aws_xml_get_top_level_tag(allocator, &g_etag_header_name, &response_body_cursor);

    struct aws_byte_buf etag_within_quotes_byte_buf;
    AWS_ZERO_STRUCT(etag_within_quotes_byte_buf);
    replace_quote_entities(allocator, etag_within_xml_quotes, &etag_within_quotes_byte_buf);

    /* Remove the quotes surrounding the etag. */
    struct aws_byte_cursor etag_within_quotes_byte_cursor = aws_byte_cursor_from_buf(&etag_within_quotes_byte_buf);
    if (etag_within_quotes_byte_cursor.len >= 2 && etag_within_quotes_byte_cursor.ptr[0] == '"' &&
        etag_within_quotes_byte_cursor.ptr[etag_within_quotes_byte_cursor.len - 1] == '"') {

        aws_byte_cursor_advance(&etag_within_quotes_byte_cursor, 1);
        --etag_within_quotes_byte_cursor.len;
    }

    etag = aws_string_new_from_cursor(allocator, &etag_within_quotes_byte_cursor);
    aws_byte_buf_clean_up(&etag_within_quotes_byte_buf);
    aws_string_destroy(etag_within_xml_quotes);

    return etag;
}

static void s_s3_copy_object_request_finished(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->impl);
    AWS_PRECONDITION(request);

    struct aws_s3_copy_object *copy_object = meta_request->impl;
    aws_s3_meta_request_lock_synced_data(meta_request);

    switch (request->request_tag) {

        case AWS_S3_COPY_OBJECT_REQUEST_TAG_GET_OBJECT_SIZE: {
            if (error_code == AWS_ERROR_SUCCESS) {
                struct aws_byte_cursor content_length_cursor;
                if (!aws_http_headers_get(
                        request->send_data.response_headers, g_content_length_header_name, &content_length_cursor)) {

                    if (!aws_byte_cursor_utf8_parse_u64(
                            content_length_cursor, &copy_object->synced_data.content_length)) {
                        copy_object->synced_data.head_object_completed = true;
                    } else {
                        /* HEAD request returned an invalid content-length */
                        aws_s3_meta_request_set_fail_synced(
                            meta_request, request, AWS_ERROR_S3_INVALID_CONTENT_LENGTH_HEADER);
                    }
                } else {
                    /* HEAD request didn't return content-length header */
                    aws_s3_meta_request_set_fail_synced(
                        meta_request, request, AWS_ERROR_S3_INVALID_CONTENT_LENGTH_HEADER);
                }
            } else {
                aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            }

            break;
        }

        /* The S3 object is not large enough for multi-part copy. A copy of the original CopyObject request
         * was bypassed to S3 and is now finished. */
        case AWS_S3_COPY_OBJECT_REQUEST_TAG_BYPASS: {

            /* Invoke headers callback if it was requested for this meta request */
            if (meta_request->headers_callback != NULL) {
                struct aws_http_headers *final_response_headers = aws_http_headers_new(meta_request->allocator);

                /* Copy all the response headers from this request. */
                copy_http_headers(request->send_data.response_headers, final_response_headers);

                /* Notify the user of the headers. */
                if (meta_request->headers_callback(
                        meta_request,
                        final_response_headers,
                        request->send_data.response_status,
                        meta_request->user_data)) {

                    error_code = aws_last_error_or_unknown();
                }
                meta_request->headers_callback = NULL;

                aws_http_headers_release(final_response_headers);
            }

            /* Signals completion of the meta request */
            if (error_code == AWS_ERROR_SUCCESS) {
                copy_object->synced_data.copy_request_bypass_completed = true;
            } else {
                /* Bypassed CopyObject request failed */
                aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            }
            break;
        }

        case AWS_S3_COPY_OBJECT_REQUEST_TAG_CREATE_MULTIPART_UPLOAD: {
            struct aws_http_headers *needed_response_headers = NULL;

            if (error_code == AWS_ERROR_SUCCESS) {
                needed_response_headers = aws_http_headers_new(meta_request->allocator);
                const size_t copy_header_count = AWS_ARRAY_SIZE(s_create_multipart_upload_copy_headers);

                /* Copy any headers now that we'll need for the final, transformed headers later. */
                for (size_t header_index = 0; header_index < copy_header_count; ++header_index) {
                    const struct aws_byte_cursor *header_name = &s_create_multipart_upload_copy_headers[header_index];
                    struct aws_byte_cursor header_value;
                    AWS_ZERO_STRUCT(header_value);

                    if (!aws_http_headers_get(request->send_data.response_headers, *header_name, &header_value)) {
                        aws_http_headers_set(needed_response_headers, *header_name, header_value);
                    }
                }

                struct aws_byte_cursor buffer_byte_cursor = aws_byte_cursor_from_buf(&request->send_data.response_body);

                /* Find the upload id for this multipart upload. */
                struct aws_string *upload_id =
                    aws_xml_get_top_level_tag(meta_request->allocator, &s_upload_id, &buffer_byte_cursor);

                if (upload_id == NULL) {
                    AWS_LOGF_ERROR(
                        AWS_LS_S3_META_REQUEST,
                        "id=%p Could not find upload-id in create-multipart-upload response",
                        (void *)meta_request);

                    aws_raise_error(AWS_ERROR_S3_MISSING_UPLOAD_ID);
                    error_code = AWS_ERROR_S3_MISSING_UPLOAD_ID;
                } else {
                    /* Store the multipart upload id. */
                    copy_object->upload_id = upload_id;
                }
            }

            AWS_ASSERT(copy_object->synced_data.needed_response_headers == NULL);
            copy_object->synced_data.needed_response_headers = needed_response_headers;

            copy_object->synced_data.create_multipart_upload_completed = true;
            copy_object->synced_data.create_multipart_upload_error_code = error_code;

            if (error_code != AWS_ERROR_SUCCESS) {
                aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            }
            break;
        }

        case AWS_S3_COPY_OBJECT_REQUEST_TAG_MULTIPART_COPY: {
            size_t part_number = request->part_number;
            AWS_FATAL_ASSERT(part_number > 0);
            size_t part_index = part_number - 1;

            ++copy_object->synced_data.num_parts_completed;

            AWS_LOGF_DEBUG(
                AWS_LS_S3_META_REQUEST,
                "id=%p: %d out of %d parts have completed.",
                (void *)meta_request,
                copy_object->synced_data.num_parts_completed,
                copy_object->synced_data.total_num_parts);

            if (error_code == AWS_ERROR_SUCCESS) {
                struct aws_string *etag = s_etag_new_from_upload_part_copy_response(
                    meta_request->allocator, &request->send_data.response_body);

                AWS_ASSERT(etag != NULL);

                ++copy_object->synced_data.num_parts_successful;
                if (meta_request->progress_callback != NULL) {
                    struct aws_s3_meta_request_progress progress = {
                        .bytes_transferred = copy_object->synced_data.part_size,
                        .content_length = copy_object->synced_data.content_length};
                    meta_request->progress_callback(meta_request, &progress, meta_request->user_data);
                }

                struct aws_string *null_etag = NULL;
                /* ETags need to be associated with their part number, so we keep the etag indices consistent with
                 * part numbers. This means we may have to add padding to the list in the case that parts finish out
                 * of order. */
                while (aws_array_list_length(&copy_object->synced_data.etag_list) < part_number) {
                    int push_back_result = aws_array_list_push_back(&copy_object->synced_data.etag_list, &null_etag);
                    AWS_FATAL_ASSERT(push_back_result == AWS_OP_SUCCESS);
                }
                aws_array_list_set_at(&copy_object->synced_data.etag_list, &etag, part_index);
            } else {
                ++copy_object->synced_data.num_parts_failed;
                aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            }

            break;
        }

        case AWS_S3_COPY_OBJECT_REQUEST_TAG_COMPLETE_MULTIPART_UPLOAD: {
            if (error_code == AWS_ERROR_SUCCESS && meta_request->headers_callback != NULL) {
                struct aws_http_headers *final_response_headers = aws_http_headers_new(meta_request->allocator);

                /* Copy all the response headers from this request. */
                copy_http_headers(request->send_data.response_headers, final_response_headers);

                /* Copy over any response headers that we've previously determined are needed for this final
                 * response.
                 */
                copy_http_headers(copy_object->synced_data.needed_response_headers, final_response_headers);

                struct aws_byte_cursor response_body_cursor =
                    aws_byte_cursor_from_buf(&request->send_data.response_body);

                /* Grab the ETag for the entire object, and set it as a header. */
                struct aws_string *etag_header_value =
                    aws_xml_get_top_level_tag(meta_request->allocator, &g_etag_header_name, &response_body_cursor);

                if (etag_header_value != NULL) {
                    struct aws_byte_buf etag_header_value_byte_buf;
                    AWS_ZERO_STRUCT(etag_header_value_byte_buf);

                    replace_quote_entities(meta_request->allocator, etag_header_value, &etag_header_value_byte_buf);

                    aws_http_headers_set(
                        final_response_headers,
                        g_etag_header_name,
                        aws_byte_cursor_from_buf(&etag_header_value_byte_buf));

                    aws_string_destroy(etag_header_value);
                    aws_byte_buf_clean_up(&etag_header_value_byte_buf);
                }

                /* Notify the user of the headers. */
                if (meta_request->headers_callback(
                        meta_request,
                        final_response_headers,
                        request->send_data.response_status,
                        meta_request->user_data)) {

                    error_code = aws_last_error_or_unknown();
                }
                meta_request->headers_callback = NULL;

                aws_http_headers_release(final_response_headers);
            }

            copy_object->synced_data.complete_multipart_upload_completed = true;
            copy_object->synced_data.complete_multipart_upload_error_code = error_code;

            if (error_code != AWS_ERROR_SUCCESS) {
                aws_s3_meta_request_set_fail_synced(meta_request, request, error_code);
            }

            break;
        }
        case AWS_S3_COPY_OBJECT_REQUEST_TAG_ABORT_MULTIPART_UPLOAD: {
            copy_object->synced_data.abort_multipart_upload_error_code = error_code;
            copy_object->synced_data.abort_multipart_upload_completed = true;
            break;
        }
    }
    aws_s3_meta_request_unlock_synced_data(meta_request);
}
