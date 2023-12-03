/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/s3.h>

#include <aws/auth/auth.h>
#include <aws/common/error.h>
#include <aws/common/hash_table.h>
#include <aws/http/http.h>

#define AWS_DEFINE_ERROR_INFO_S3(CODE, STR) AWS_DEFINE_ERROR_INFO(CODE, STR, "aws-c-s3")

/* clang-format off */
static struct aws_error_info s_errors[] = {
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER, "Response missing required Content-Range header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_CONTENT_RANGE_HEADER, "Response contains invalid Content-Range header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_CONTENT_LENGTH_HEADER, "Response missing required Content-Length header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_CONTENT_LENGTH_HEADER, "Response contains invalid Content-Length header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_ETAG, "Response missing required ETag header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INTERNAL_ERROR, "Response code indicates internal server error"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_SLOW_DOWN, "Response code indicates throttling"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_RESPONSE_STATUS, "Invalid response status from request"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MISSING_UPLOAD_ID, "Upload Id not found in create-multipart-upload response"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_PROXY_PARSE_FAILED, "Could not parse proxy URI"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_UNSUPPORTED_PROXY_SCHEME, "Given Proxy URI has an unsupported scheme"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_CANCELED, "Request successfully cancelled"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_RANGE_HEADER, "Range header has invalid syntax"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_MULTIRANGE_HEADER_UNSUPPORTED, "Range header specifies multiple ranges which is unsupported"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_RESPONSE_CHECKSUM_MISMATCH, "response checksum header does not match calculated checksum"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_CHECKSUM_CALCULATION_FAILED, "failed to calculate a checksum for the provided stream"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_PAUSED, "Request successfully paused"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_LIST_PARTS_PARSE_FAILED, "Failed to parse result from list parts"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_RESUMED_PART_CHECKSUM_MISMATCH, "Checksum does not match previously uploaded part"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_RESUME_FAILED, "Resuming request failed"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_OBJECT_MODIFIED, "The object modifed during download."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR, "Async error received from S3 and not recoverable from retry.")
};
/* clang-format on */

static struct aws_error_info_list s_error_list = {
    .error_list = s_errors,
    .count = AWS_ARRAY_SIZE(s_errors),
};

static struct aws_log_subject_info s_s3_log_subject_infos[] = {
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_GENERAL, "S3General", "Subject for aws-c-s3 logging that defies categorization."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_CLIENT, "S3Client", "Subject for aws-c-s3 logging from an aws_s3_client."),
    DEFINE_LOG_SUBJECT_INFO(
        AWS_LS_S3_CLIENT_STATS,
        "S3ClientStats",
        "Subject for aws-c-s3 logging for stats tracked by an aws_s3_client."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_REQUEST, "S3Request", "Subject for aws-c-s3 logging from an aws_s3_request."),
    DEFINE_LOG_SUBJECT_INFO(
        AWS_LS_S3_META_REQUEST,
        "S3MetaRequest",
        "Subject for aws-c-s3 logging from an aws_s3_meta_request."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_S3_ENDPOINT, "S3Endpoint", "Subject for aws-c-s3 logging from an aws_s3_endpoint."),
};

static struct aws_log_subject_info_list s_s3_log_subject_list = {
    .subject_list = s_s3_log_subject_infos,
    .count = AWS_ARRAY_SIZE(s_s3_log_subject_infos),
};

/**** Configuration info for the c5n.18xlarge *****/
static struct aws_byte_cursor s_c5n_18xlarge_nic_array[] = {AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("eth0")};

static struct aws_s3_cpu_group_info s_c5n_18xlarge_cpu_group_info_array[] = {
    {
        .cpu_group = 0u,
        .nic_name_array = s_c5n_18xlarge_nic_array,
        .nic_name_array_length = AWS_ARRAY_SIZE(s_c5n_18xlarge_nic_array),
    },
    {
        .cpu_group = 1u,
        .nic_name_array = NULL,
        .nic_name_array_length = 0u,
    },
};

static struct aws_s3_compute_platform_info s_c5n_18xlarge_platform_info = {
    .instance_type = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("c5n.18xlarge"),
    .max_throughput_gbps = 100u,
    .cpu_group_info_array = s_c5n_18xlarge_cpu_group_info_array,
    .cpu_group_info_array_length = AWS_ARRAY_SIZE(s_c5n_18xlarge_cpu_group_info_array),
};
/****** End c5n.18xlarge *****/

static struct aws_hash_table s_compute_platform_info_table;

static bool s_library_initialized = false;
static struct aws_allocator *s_library_allocator = NULL;

void aws_s3_library_init(struct aws_allocator *allocator) {
    if (s_library_initialized) {
        return;
    }

    if (allocator) {
        s_library_allocator = allocator;
    } else {
        s_library_allocator = aws_default_allocator();
    }

    aws_auth_library_init(s_library_allocator);
    aws_http_library_init(s_library_allocator);

    aws_register_error_info(&s_error_list);
    aws_register_log_subject_info_list(&s_s3_log_subject_list);

    AWS_FATAL_ASSERT(
        !aws_hash_table_init(
            &s_compute_platform_info_table,
            allocator,
            32,
            aws_hash_byte_cursor_ptr_ignore_case,
            (bool (*)(const void *, const void *))aws_byte_cursor_eq_ignore_case,
            NULL,
            NULL) &&
        "Hash table init failed!");

    AWS_FATAL_ASSERT(
        !aws_hash_table_put(
            &s_compute_platform_info_table,
            &s_c5n_18xlarge_platform_info.instance_type,
            &s_c5n_18xlarge_platform_info,
            NULL) &&
        "hash table put failed!");

    s_library_initialized = true;
}

void aws_s3_library_clean_up(void) {
    if (!s_library_initialized) {
        return;
    }

    s_library_initialized = false;
    aws_thread_join_all_managed();

    aws_hash_table_clean_up(&s_compute_platform_info_table);
    aws_unregister_log_subject_info_list(&s_s3_log_subject_list);
    aws_unregister_error_info(&s_error_list);
    aws_http_library_clean_up();
    aws_auth_library_clean_up();
    s_library_allocator = NULL;
}

struct aws_s3_compute_platform_info *aws_s3_get_compute_platform_info_for_instance_type(
    const struct aws_byte_cursor instance_type_name) {
    AWS_LOGF_TRACE(
        AWS_LS_S3_GENERAL,
        "static: looking up compute platform info for instance type " PRInSTR,
        AWS_BYTE_CURSOR_PRI(instance_type_name));

    struct aws_hash_element *platform_info_element = NULL;
    aws_hash_table_find(&s_compute_platform_info_table, &instance_type_name, &platform_info_element);

    if (platform_info_element) {
        AWS_LOGF_INFO(
            AWS_LS_S3_GENERAL,
            "static: found compute platform info for instance type " PRInSTR,
            AWS_BYTE_CURSOR_PRI(instance_type_name));
        return platform_info_element->value;
    }

    AWS_LOGF_INFO(
        AWS_LS_S3_GENERAL,
        "static: compute platform info for instance type " PRInSTR " not found",
        AWS_BYTE_CURSOR_PRI(instance_type_name));
    return NULL;
}
