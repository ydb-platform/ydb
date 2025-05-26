/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_platform_info.h>
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
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_LIST_PARTS_PARSE_FAILED, "Failed to parse response from ListParts"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_RESUMED_PART_CHECKSUM_MISMATCH, "Checksum does not match previously uploaded part"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_RESUME_FAILED, "Resuming request failed"),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_OBJECT_MODIFIED, "The object modifed during download."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_NON_RECOVERABLE_ASYNC_ERROR, "Async error received from S3 and not recoverable from retry."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_METRIC_DATA_NOT_AVAILABLE, "The metric data is not available, the requests ends before the metric happens."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INCORRECT_CONTENT_LENGTH, "Request body length must match Content-Length header."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_REQUEST_TIME_TOO_SKEWED, "RequestTimeTooSkewed error received from S3."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_FILE_MODIFIED, "The file was modified during upload."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_EXCEEDS_MEMORY_LIMIT, "Request was not created due to used memory exceeding memory limit."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INVALID_MEMORY_LIMIT_CONFIG, "Specified memory configuration is invalid for the system. "
        "Memory limit should be at least 1GiB. Part size and max part size should be smaller than memory limit."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3EXPRESS_CREATE_SESSION_FAILED, "CreateSession call failed when signing with S3 Express."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_INTERNAL_PART_SIZE_MISMATCH_RETRYING_WITH_RANGE, "part_size mismatch, possibly due to wrong object_size_hint. Retrying with Range instead of partNumber."),
    AWS_DEFINE_ERROR_INFO_S3(AWS_ERROR_S3_REQUEST_HAS_COMPLETED, "Request has already completed, action cannot be performed."),
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

static bool s_library_initialized = false;
static struct aws_allocator *s_library_allocator = NULL;
static struct aws_s3_platform_info_loader *s_loader;

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
    s_loader = aws_s3_platform_info_loader_new(allocator);
    AWS_FATAL_ASSERT(s_loader);
    s_library_initialized = true;
}

const struct aws_s3_platform_info *aws_s3_get_current_platform_info(void) {
    return aws_s3_get_platform_info_for_current_environment(s_loader);
}

struct aws_byte_cursor aws_s3_get_current_platform_ec2_intance_type(bool cached_only) {
    return aws_s3_get_ec2_instance_type(s_loader, cached_only);
}

struct aws_array_list aws_s3_get_platforms_with_recommended_config(void) {
    return aws_s3_get_recommended_platforms(s_loader);
}

void aws_s3_library_clean_up(void) {
    if (!s_library_initialized) {
        return;
    }

    s_library_initialized = false;
    s_loader = aws_s3_platform_info_loader_release(s_loader);
    aws_thread_join_all_managed();

    aws_unregister_log_subject_info_list(&s_s3_log_subject_list);
    aws_unregister_error_info(&s_error_list);
    aws_http_library_clean_up();
    aws_auth_library_clean_up();
    s_library_allocator = NULL;
}
