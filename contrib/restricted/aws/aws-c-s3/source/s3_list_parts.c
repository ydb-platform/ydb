/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/s3/private/s3_list_parts.h>
#include <aws/s3/private/s3_paginator.h>
#include <aws/s3/private/s3_util.h>

#include <aws/common/ref_count.h>
#include <aws/common/xml_parser.h>

#include <aws/io/uri.h>

#include <aws/http/http.h>
#include <aws/http/request_response.h>

struct aws_s3_operation_data {
    struct aws_allocator *allocator;

    struct aws_string *key;
    struct aws_string *upload_id;

    struct aws_ref_count ref_count;

    aws_s3_on_part_fn *on_part;

    void *user_data;
};

static void s_ref_count_zero_callback(void *arg) {
    struct aws_s3_operation_data *operation_data = arg;

    if (operation_data->key) {
        aws_string_destroy(operation_data->key);
    }

    if (operation_data->upload_id) {
        aws_string_destroy(operation_data->upload_id);
    }

    aws_mem_release(operation_data->allocator, operation_data);
}

static void s_on_paginator_cleanup(void *user_data) {
    struct aws_s3_operation_data *operation_data = user_data;

    aws_ref_count_release(&operation_data->ref_count);
}

struct result_wrapper {
    struct aws_allocator *allocator;
    struct aws_s3_part_info part_info;
};

/* invoked when the ListPartResult/Parts node is iterated. */
static bool s_on_parts_node(struct aws_xml_parser *parser, struct aws_xml_node *node, void *user_data) {
    struct result_wrapper *result_wrapper = user_data;
    struct aws_s3_part_info *part_info = &result_wrapper->part_info;

    /* for each Parts node, get the info from it and send it off as an part we've encountered */
    struct aws_byte_cursor node_name;
    aws_xml_node_get_name(node, &node_name);

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "ETag")) {
        return aws_xml_node_as_body(parser, node, &part_info->e_tag) == AWS_OP_SUCCESS;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "LastModified")) {
        struct aws_byte_cursor date_cur;
        if (aws_xml_node_as_body(parser, node, &date_cur) == AWS_OP_SUCCESS) {
            aws_date_time_init_from_str_cursor(&part_info->last_modified, &date_cur, AWS_DATE_FORMAT_ISO_8601);
            return true;
        }

        return false;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "Size")) {
        struct aws_byte_cursor size_cur;

        if (aws_xml_node_as_body(parser, node, &size_cur) == AWS_OP_SUCCESS) {
            if (aws_byte_cursor_utf8_parse_u64(size_cur, &part_info->size)) {
                return false;
            }
            return true;
        }
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "PartNumber")) {
        struct aws_byte_cursor part_number_cur;

        if (aws_xml_node_as_body(parser, node, &part_number_cur) == AWS_OP_SUCCESS) {
            uint64_t part_number = 0;
            if (aws_byte_cursor_utf8_parse_u64(part_number_cur, &part_number)) {
                return false;
            }
            if (part_number > UINT32_MAX) {
                aws_raise_error(AWS_ERROR_OVERFLOW_DETECTED);
                return false;
            }
            part_info->part_number = (uint32_t)part_number;
            return true;
        }
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "ChecksumCRC32")) {
        return aws_xml_node_as_body(parser, node, &part_info->checksumCRC32) == AWS_OP_SUCCESS;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "ChecksumCRC32C")) {
        return aws_xml_node_as_body(parser, node, &part_info->checksumCRC32C) == AWS_OP_SUCCESS;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "ChecksumSHA1")) {
        return aws_xml_node_as_body(parser, node, &part_info->checksumSHA1) == AWS_OP_SUCCESS;
    }

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "ChecksumSHA256")) {
        return aws_xml_node_as_body(parser, node, &part_info->checksumSHA256) == AWS_OP_SUCCESS;
    }

    return true;
}

static bool s_on_list_bucket_result_node_encountered(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data) {
    struct aws_s3_operation_data *operation_data = user_data;

    struct aws_byte_cursor node_name;
    aws_xml_node_get_name(node, &node_name);

    struct result_wrapper result_wrapper;
    AWS_ZERO_STRUCT(result_wrapper);

    if (aws_byte_cursor_eq_c_str_ignore_case(&node_name, "Part")) {
        result_wrapper.allocator = operation_data->allocator;
        /* this will traverse the current Parts node, get the metadata necessary to construct
         * an instance of fs_info so we can invoke the callback on it. This happens once per part. */
        bool ret_val = aws_xml_node_traverse(parser, node, s_on_parts_node, &result_wrapper) == AWS_OP_SUCCESS;

        struct aws_byte_buf trimmed_etag;
        AWS_ZERO_STRUCT(trimmed_etag);

        if (result_wrapper.part_info.e_tag.len) {
            struct aws_string *quoted_etag_str =
                aws_string_new_from_cursor(result_wrapper.allocator, &result_wrapper.part_info.e_tag);
            replace_quote_entities(result_wrapper.allocator, quoted_etag_str, &trimmed_etag);
            result_wrapper.part_info.e_tag = aws_byte_cursor_from_buf(&trimmed_etag);
            aws_string_destroy(quoted_etag_str);
        }

        if (ret_val && operation_data->on_part) {
            ret_val |= operation_data->on_part(&result_wrapper.part_info, operation_data->user_data);
        }

        if (trimmed_etag.len) {
            aws_byte_buf_clean_up(&trimmed_etag);
        }

        return ret_val;
    }

    return true;
}

static int s_construct_next_request_http_message(
    struct aws_byte_cursor *continuation_token,
    void *user_data,
    struct aws_http_message **out_message) {

    AWS_PRECONDITION(user_data);

    struct aws_s3_operation_data *operation_data = user_data;

    struct aws_byte_buf request_path;
    struct aws_byte_cursor key_val = aws_byte_cursor_from_string(operation_data->key);
    aws_byte_buf_init_copy_from_cursor(&request_path, operation_data->allocator, key_val);

    if (operation_data->upload_id) {
        struct aws_byte_cursor upload_id = aws_byte_cursor_from_c_str("?uploadId=");
        aws_byte_buf_append_dynamic(&request_path, &upload_id);
        struct aws_byte_cursor upload_id_val = aws_byte_cursor_from_string(operation_data->upload_id);
        aws_byte_buf_append_dynamic(&request_path, &upload_id_val);
    }

    if (continuation_token) {
        struct aws_byte_cursor continuation = aws_byte_cursor_from_c_str("&part-number-marker=");
        aws_byte_buf_append_dynamic(&request_path, &continuation);
        aws_byte_buf_append_encoding_uri_param(&request_path, continuation_token);
    }

    struct aws_http_message *list_parts_request = aws_http_message_new_request(operation_data->allocator);
    aws_http_message_set_request_path(list_parts_request, aws_byte_cursor_from_buf(&request_path));

    aws_byte_buf_clean_up(&request_path);

    struct aws_http_header accept_header = {
        .name = aws_byte_cursor_from_c_str("accept"),
        .value = aws_byte_cursor_from_c_str("application/xml"),
    };

    aws_http_message_add_header(list_parts_request, accept_header);

    aws_http_message_set_request_method(list_parts_request, aws_http_method_get);

    *out_message = list_parts_request;
    return AWS_OP_SUCCESS;
}

struct aws_s3_paginator *aws_s3_initiate_list_parts(
    struct aws_allocator *allocator,
    const struct aws_s3_list_parts_params *params) {
    AWS_FATAL_PRECONDITION(params);
    AWS_FATAL_PRECONDITION(params->client);
    AWS_FATAL_PRECONDITION(params->bucket_name.len);
    AWS_FATAL_PRECONDITION(params->key.len);
    AWS_FATAL_PRECONDITION(params->upload_id.len);
    AWS_FATAL_PRECONDITION(params->endpoint.len);

    struct aws_s3_operation_data *operation_data = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_operation_data));
    operation_data->allocator = allocator;
    operation_data->key = aws_string_new_from_cursor(allocator, &params->key);
    operation_data->upload_id = aws_string_new_from_cursor(allocator, &params->upload_id);
    operation_data->on_part = params->on_part;
    operation_data->user_data = params->user_data;

    aws_ref_count_init(&operation_data->ref_count, operation_data, s_ref_count_zero_callback);

    struct aws_byte_cursor xml_result_node_name = aws_byte_cursor_from_c_str("ListPartsResult");
    const struct aws_byte_cursor continuation_node_name = aws_byte_cursor_from_c_str("NextPartNumberMarker");

    struct aws_s3_paginated_operation_params operation_params = {
        .next_message = s_construct_next_request_http_message,
        .on_result_node_encountered_fn = s_on_list_bucket_result_node_encountered,
        .on_paginated_operation_cleanup = s_on_paginator_cleanup,
        .result_xml_node_name = &xml_result_node_name,
        .continuation_token_node_name = &continuation_node_name,
        .user_data = operation_data,
    };

    struct aws_s3_paginated_operation *operation = aws_s3_paginated_operation_new(allocator, &operation_params);

    struct aws_s3_paginator_params paginator_params = {
        .client = params->client,
        .bucket_name = params->bucket_name,
        .endpoint = params->endpoint,
        .operation = operation,
        .on_page_finished_fn = params->on_list_finished,
        .user_data = params->user_data,
    };

    struct aws_s3_paginator *paginator = aws_s3_initiate_paginator(allocator, &paginator_params);

    // transfer control to paginator
    aws_s3_paginated_operation_release(operation);

    return paginator;
}

struct aws_s3_paginated_operation *aws_s3_list_parts_operation_new(
    struct aws_allocator *allocator,
    const struct aws_s3_list_parts_params *params) {
    AWS_FATAL_PRECONDITION(params);
    AWS_FATAL_PRECONDITION(params->key.len);
    AWS_FATAL_PRECONDITION(params->upload_id.len);

    struct aws_s3_operation_data *operation_data = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_operation_data));
    operation_data->allocator = allocator;
    operation_data->key = aws_string_new_from_cursor(allocator, &params->key);
    operation_data->upload_id = aws_string_new_from_cursor(allocator, &params->upload_id);
    operation_data->on_part = params->on_part;
    operation_data->user_data = params->user_data;

    aws_ref_count_init(&operation_data->ref_count, operation_data, s_ref_count_zero_callback);

    struct aws_byte_cursor xml_result_node_name = aws_byte_cursor_from_c_str("ListPartsResult");
    const struct aws_byte_cursor continuation_node_name = aws_byte_cursor_from_c_str("NextPartNumberMarker");

    struct aws_s3_paginated_operation_params operation_params = {
        .next_message = s_construct_next_request_http_message,
        .on_result_node_encountered_fn = s_on_list_bucket_result_node_encountered,
        .on_paginated_operation_cleanup = s_on_paginator_cleanup,
        .result_xml_node_name = &xml_result_node_name,
        .continuation_token_node_name = &continuation_node_name,
        .user_data = operation_data,
    };

    struct aws_s3_paginated_operation *operation = aws_s3_paginated_operation_new(allocator, &operation_params);

    return operation;
}
