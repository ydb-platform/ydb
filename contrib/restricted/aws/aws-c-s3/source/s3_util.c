/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_util.h"
#include "aws/s3/private/s3_client_impl.h"
#include <aws/auth/credentials.h>
#include <aws/common/string.h>
#include <aws/common/xml_parser.h>
#include <aws/http/request_response.h>
#include <aws/s3/s3.h>
#include <aws/s3/s3_client.h>
#include <inttypes.h>

#ifdef _MSC_VER
/* sscanf warning (not currently scanning for strings) */
#    pragma warning(disable : 4996)
#endif

const struct aws_byte_cursor g_s3_client_version = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(AWS_S3_CLIENT_VERSION);
const struct aws_byte_cursor g_s3_service_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("s3");
const struct aws_byte_cursor g_host_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Host");
const struct aws_byte_cursor g_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Range");
const struct aws_byte_cursor g_if_match_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("If-Match");
const struct aws_byte_cursor g_etag_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ETag");
const struct aws_byte_cursor g_content_range_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Range");
const struct aws_byte_cursor g_content_type_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Type");
const struct aws_byte_cursor g_content_encoding_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Encoding");
const struct aws_byte_cursor g_content_encoding_header_aws_chunked =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("aws-chunked");
const struct aws_byte_cursor g_content_length_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-Length");
const struct aws_byte_cursor g_decoded_content_length_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-decoded-content-length");
const struct aws_byte_cursor g_content_md5_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Content-MD5");
const struct aws_byte_cursor g_trailer_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-trailer");
const struct aws_byte_cursor g_request_validation_mode = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-mode");
const struct aws_byte_cursor g_enabled = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("enabled");

const struct aws_byte_cursor g_create_mpu_checksum_header_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-algorithm");
const struct aws_byte_cursor g_crc32c_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-crc32c");
const struct aws_byte_cursor g_crc32_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-crc32");
const struct aws_byte_cursor g_sha1_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-sha1");
const struct aws_byte_cursor g_sha256_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-checksum-sha256");
const struct aws_byte_cursor g_crc32c_create_mpu_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRC32C");
const struct aws_byte_cursor g_crc32_create_mpu_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRC32");
const struct aws_byte_cursor g_sha1_create_mpu_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SHA1");
const struct aws_byte_cursor g_sha256_create_mpu_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SHA256");
const struct aws_byte_cursor g_crc32c_complete_mpu_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumCRC32C");
const struct aws_byte_cursor g_crc32_complete_mpu_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumCRC32");
const struct aws_byte_cursor g_sha1_complete_mpu_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumSHA1");
const struct aws_byte_cursor g_sha256_complete_mpu_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("ChecksumSHA256");
const struct aws_byte_cursor g_accept_ranges_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("accept-ranges");
const struct aws_byte_cursor g_acl_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("x-amz-acl");
const struct aws_byte_cursor g_post_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("POST");
const struct aws_byte_cursor g_head_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("HEAD");
const struct aws_byte_cursor g_delete_method = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("DELETE");

const struct aws_byte_cursor g_user_agent_header_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("User-Agent");
const struct aws_byte_cursor g_user_agent_header_product_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("CRTS3NativeClient");

const struct aws_byte_cursor g_error_body_xml_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Error");
const struct aws_byte_cursor g_code_body_xml_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("Code");

const struct aws_byte_cursor g_s3_internal_error_code = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("InternalError");
const struct aws_byte_cursor g_s3_slow_down_error_code = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("SlowDown");
/* The special error code as Asynchronous Error Codes */
const struct aws_byte_cursor g_s3_internal_errors_code = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("InternalErrors");

const uint32_t g_s3_max_num_upload_parts = 10000;
const size_t g_s3_min_upload_part_size = MB_TO_BYTES(5);

void copy_http_headers(const struct aws_http_headers *src, struct aws_http_headers *dest) {
    AWS_PRECONDITION(src);
    AWS_PRECONDITION(dest);

    size_t headers_count = aws_http_headers_count(src);

    for (size_t header_index = 0; header_index < headers_count; ++header_index) {
        struct aws_http_header header;

        aws_http_headers_get_index(src, header_index, &header);
        aws_http_headers_set(dest, header.name, header.value);
    }
}

struct top_level_xml_tag_value_with_root_value_user_data {
    struct aws_allocator *allocator;
    const struct aws_byte_cursor *tag_name;
    const struct aws_byte_cursor *expected_root_name;
    bool *root_name_mismatch;
    struct aws_string *result;
};

static bool s_top_level_xml_tag_value_child_xml_node(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data) {

    struct aws_byte_cursor node_name;

    /* If we can't get the name of the node, stop traversing. */
    if (aws_xml_node_get_name(node, &node_name)) {
        return false;
    }

    struct top_level_xml_tag_value_with_root_value_user_data *xml_user_data = user_data;

    /* If the name of the node is what we are looking for, store the body of the node in our result, and stop
     * traversing. */
    if (aws_byte_cursor_eq(&node_name, xml_user_data->tag_name)) {

        struct aws_byte_cursor node_body;
        aws_xml_node_as_body(parser, node, &node_body);

        xml_user_data->result = aws_string_new_from_cursor(xml_user_data->allocator, &node_body);

        return false;
    }

    /* If we made it here, the tag hasn't been found yet, so return true to keep looking. */
    return true;
}

static bool s_top_level_xml_tag_value_root_xml_node(
    struct aws_xml_parser *parser,
    struct aws_xml_node *node,
    void *user_data) {
    struct top_level_xml_tag_value_with_root_value_user_data *xml_user_data = user_data;
    if (xml_user_data->expected_root_name) {
        /* If we can't get the name of the node, stop traversing. */
        struct aws_byte_cursor node_name;
        if (aws_xml_node_get_name(node, &node_name)) {
            return false;
        }
        if (!aws_byte_cursor_eq(&node_name, xml_user_data->expected_root_name)) {
            /* Not match the expected root name, stop parsing. */
            *xml_user_data->root_name_mismatch = true;
            return false;
        }
    }

    /* Traverse the root node, and then return false to stop. */
    aws_xml_node_traverse(parser, node, s_top_level_xml_tag_value_child_xml_node, user_data);
    return false;
}

struct aws_string *aws_xml_get_top_level_tag_with_root_name(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *tag_name,
    const struct aws_byte_cursor *expected_root_name,
    bool *out_root_name_mismatch,
    struct aws_byte_cursor *xml_body) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(tag_name);
    AWS_PRECONDITION(xml_body);

    struct aws_xml_parser_options parser_options = {.doc = *xml_body};
    struct aws_xml_parser *parser = aws_xml_parser_new(allocator, &parser_options);
    bool root_name_mismatch = false;

    struct top_level_xml_tag_value_with_root_value_user_data xml_user_data = {
        allocator,
        tag_name,
        expected_root_name,
        &root_name_mismatch,
        NULL,
    };

    if (aws_xml_parser_parse(parser, s_top_level_xml_tag_value_root_xml_node, (void *)&xml_user_data)) {
        aws_string_destroy(xml_user_data.result);
        xml_user_data.result = NULL;
        goto clean_up;
    }
    if (out_root_name_mismatch) {
        *out_root_name_mismatch = root_name_mismatch;
    }

clean_up:

    aws_xml_parser_destroy(parser);

    return xml_user_data.result;
}

struct aws_string *aws_xml_get_top_level_tag(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *tag_name,
    struct aws_byte_cursor *xml_body) {
    return aws_xml_get_top_level_tag_with_root_name(allocator, tag_name, NULL, NULL, xml_body);
}

struct aws_cached_signing_config_aws *aws_cached_signing_config_new(
    struct aws_allocator *allocator,
    const struct aws_signing_config_aws *signing_config) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(signing_config);

    struct aws_cached_signing_config_aws *cached_signing_config =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_cached_signing_config_aws));

    cached_signing_config->allocator = allocator;

    cached_signing_config->config.config_type = signing_config->config_type;
    cached_signing_config->config.algorithm = signing_config->algorithm;
    cached_signing_config->config.signature_type = signing_config->signature_type;

    AWS_ASSERT(aws_byte_cursor_is_valid(&signing_config->region));

    if (signing_config->region.len > 0) {
        cached_signing_config->region = aws_string_new_from_cursor(allocator, &signing_config->region);

        cached_signing_config->config.region = aws_byte_cursor_from_string(cached_signing_config->region);
    }

    AWS_ASSERT(aws_byte_cursor_is_valid(&signing_config->service));

    if (signing_config->service.len > 0) {
        cached_signing_config->service = aws_string_new_from_cursor(allocator, &signing_config->service);

        cached_signing_config->config.service = aws_byte_cursor_from_string(cached_signing_config->service);
    }

    cached_signing_config->config.date = signing_config->date;

    cached_signing_config->config.should_sign_header = signing_config->should_sign_header;
    cached_signing_config->config.flags = signing_config->flags;

    AWS_ASSERT(aws_byte_cursor_is_valid(&signing_config->signed_body_value));

    if (signing_config->service.len > 0) {
        cached_signing_config->signed_body_value =
            aws_string_new_from_cursor(allocator, &signing_config->signed_body_value);

        cached_signing_config->config.signed_body_value =
            aws_byte_cursor_from_string(cached_signing_config->signed_body_value);
    }

    cached_signing_config->config.signed_body_header = signing_config->signed_body_header;

    if (signing_config->credentials != NULL) {
        aws_credentials_acquire(signing_config->credentials);
        cached_signing_config->config.credentials = signing_config->credentials;
    }

    if (signing_config->credentials_provider != NULL) {
        aws_credentials_provider_acquire(signing_config->credentials_provider);
        cached_signing_config->config.credentials_provider = signing_config->credentials_provider;
    }

    cached_signing_config->config.expiration_in_seconds = signing_config->expiration_in_seconds;

    return cached_signing_config;
}

void aws_cached_signing_config_destroy(struct aws_cached_signing_config_aws *cached_signing_config) {
    if (cached_signing_config == NULL) {
        return;
    }

    aws_credentials_release(cached_signing_config->config.credentials);
    aws_credentials_provider_release(cached_signing_config->config.credentials_provider);

    aws_string_destroy(cached_signing_config->service);
    aws_string_destroy(cached_signing_config->region);
    aws_string_destroy(cached_signing_config->signed_body_value);

    aws_mem_release(cached_signing_config->allocator, cached_signing_config);
}

void aws_s3_init_default_signing_config(
    struct aws_signing_config_aws *signing_config,
    const struct aws_byte_cursor region,
    struct aws_credentials_provider *credentials_provider) {
    AWS_PRECONDITION(signing_config);
    AWS_PRECONDITION(credentials_provider);

    AWS_ZERO_STRUCT(*signing_config);

    signing_config->config_type = AWS_SIGNING_CONFIG_AWS;
    signing_config->algorithm = AWS_SIGNING_ALGORITHM_V4;
    signing_config->credentials_provider = credentials_provider;
    signing_config->region = region;
    signing_config->service = g_s3_service_name;
    signing_config->signed_body_header = AWS_SBHT_X_AMZ_CONTENT_SHA256;
    signing_config->signed_body_value = g_aws_signed_body_value_unsigned_payload;
}

void replace_quote_entities(struct aws_allocator *allocator, struct aws_string *str, struct aws_byte_buf *out_buf) {
    AWS_PRECONDITION(str);

    aws_byte_buf_init(out_buf, allocator, str->len);

    struct aws_byte_cursor quote_entity = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("&quot;");
    struct aws_byte_cursor quote = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("\"");

    size_t i = 0;

    while (i < str->len) {
        size_t chars_remaining = str->len - i;

        if (chars_remaining >= quote_entity.len &&
            !strncmp((const char *)&str->bytes[i], (const char *)quote_entity.ptr, quote_entity.len)) {
            /* Append quote */
            aws_byte_buf_append(out_buf, &quote);
            i += quote_entity.len;
        } else {
            /* Append character */
            struct aws_byte_cursor character_cursor = aws_byte_cursor_from_array(&str->bytes[i], 1);
            aws_byte_buf_append(out_buf, &character_cursor);
            ++i;
        }
    }
}

struct aws_string *aws_strip_quotes(struct aws_allocator *allocator, struct aws_byte_cursor in_cur) {

    if (in_cur.len >= 2 && in_cur.ptr[0] == '"' && in_cur.ptr[in_cur.len - 1] == '"') {
        aws_byte_cursor_advance(&in_cur, 1);
        --in_cur.len;
    }

    return aws_string_new_from_cursor(allocator, &in_cur);
}

int aws_last_error_or_unknown() {
    int error = aws_last_error();

    if (error == AWS_ERROR_SUCCESS) {
        return AWS_ERROR_UNKNOWN;
    }

    return error;
}

void aws_s3_add_user_agent_header(struct aws_allocator *allocator, struct aws_http_message *message) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(message);

    const struct aws_byte_cursor space_delimeter = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(" ");
    const struct aws_byte_cursor forward_slash = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("/");

    const size_t user_agent_product_version_length =
        g_user_agent_header_product_name.len + forward_slash.len + g_s3_client_version.len;

    struct aws_http_headers *headers = aws_http_message_get_headers(message);
    AWS_ASSERT(headers != NULL);

    struct aws_byte_cursor current_user_agent_header;
    AWS_ZERO_STRUCT(current_user_agent_header);

    struct aws_byte_buf user_agent_buffer;
    AWS_ZERO_STRUCT(user_agent_buffer);

    if (aws_http_headers_get(headers, g_user_agent_header_name, &current_user_agent_header) == AWS_OP_SUCCESS) {
        /* If the header was found, then create a buffer with the total size we'll need, and append the curent user
         * agent header with a trailing space. */
        aws_byte_buf_init(
            &user_agent_buffer,
            allocator,
            current_user_agent_header.len + space_delimeter.len + user_agent_product_version_length);

        aws_byte_buf_append_dynamic(&user_agent_buffer, &current_user_agent_header);

        aws_byte_buf_append_dynamic(&user_agent_buffer, &space_delimeter);

    } else {
        AWS_ASSERT(aws_last_error() == AWS_ERROR_HTTP_HEADER_NOT_FOUND);

        /* If the header was not found, then create a buffer with just the size of the user agent string that is about
         * to be appended to the buffer. */
        aws_byte_buf_init(&user_agent_buffer, allocator, user_agent_product_version_length);
    }

    /* Append the client's user-agent string. */
    {
        aws_byte_buf_append_dynamic(&user_agent_buffer, &g_user_agent_header_product_name);
        aws_byte_buf_append_dynamic(&user_agent_buffer, &forward_slash);
        aws_byte_buf_append_dynamic(&user_agent_buffer, &g_s3_client_version);
    }

    /* Apply the updated header. */
    aws_http_headers_set(headers, g_user_agent_header_name, aws_byte_cursor_from_buf(&user_agent_buffer));

    /* Clean up the scratch buffer. */
    aws_byte_buf_clean_up(&user_agent_buffer);
}

int aws_s3_parse_content_range_response_header(
    struct aws_allocator *allocator,
    struct aws_http_headers *response_headers,
    uint64_t *out_range_start,
    uint64_t *out_range_end,
    uint64_t *out_object_size) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(response_headers);

    struct aws_byte_cursor content_range_header_value;

    if (aws_http_headers_get(response_headers, g_content_range_header_name, &content_range_header_value)) {
        aws_raise_error(AWS_ERROR_S3_MISSING_CONTENT_RANGE_HEADER);
        return AWS_OP_ERR;
    }

    int result = AWS_OP_ERR;

    uint64_t range_start = 0;
    uint64_t range_end = 0;
    uint64_t object_size = 0;

    struct aws_string *content_range_header_value_str =
        aws_string_new_from_cursor(allocator, &content_range_header_value);

    /* Expected Format of header is: "bytes StartByte-EndByte/TotalObjectSize" */
    int num_fields_found = sscanf(
        (const char *)content_range_header_value_str->bytes,
        "bytes %" PRIu64 "-%" PRIu64 "/%" PRIu64,
        &range_start,
        &range_end,
        &object_size);

    if (num_fields_found < 3) {
        aws_raise_error(AWS_ERROR_S3_INVALID_CONTENT_RANGE_HEADER);
        goto clean_up;
    }

    if (out_range_start != NULL) {
        *out_range_start = range_start;
    }

    if (out_range_end != NULL) {
        *out_range_end = range_end;
    }

    if (out_object_size != NULL) {
        *out_object_size = object_size;
    }

    result = AWS_OP_SUCCESS;

clean_up:
    aws_string_destroy(content_range_header_value_str);
    content_range_header_value_str = NULL;

    return result;
}

int aws_s3_parse_content_length_response_header(
    struct aws_allocator *allocator,
    struct aws_http_headers *response_headers,
    uint64_t *out_content_length) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(response_headers);
    AWS_PRECONDITION(out_content_length);

    struct aws_byte_cursor content_length_header_value;

    if (aws_http_headers_get(response_headers, g_content_length_header_name, &content_length_header_value)) {
        aws_raise_error(AWS_ERROR_S3_MISSING_CONTENT_LENGTH_HEADER);
        return AWS_OP_ERR;
    }

    struct aws_string *content_length_header_value_str =
        aws_string_new_from_cursor(allocator, &content_length_header_value);

    int result = AWS_OP_ERR;

    if (sscanf((const char *)content_length_header_value_str->bytes, "%" PRIu64, out_content_length) == 1) {
        result = AWS_OP_SUCCESS;
    } else {
        aws_raise_error(AWS_ERROR_S3_INVALID_CONTENT_LENGTH_HEADER);
    }

    aws_string_destroy(content_length_header_value_str);
    return result;
}

uint32_t aws_s3_get_num_parts(size_t part_size, uint64_t object_range_start, uint64_t object_range_end) {
    uint32_t num_parts = 1;

    uint64_t first_part_size = part_size;
    uint64_t first_part_alignment_offset = object_range_start % part_size;

    /* If the first part size isn't aligned on the assumed part boundary, make it smaller so that it is. */
    if (first_part_alignment_offset > 0) {
        first_part_size = part_size - first_part_alignment_offset;
    }

    uint64_t second_part_start = object_range_start + first_part_size;

    /* If the range has room for a second part, calculate the additional amount of parts. */
    if (second_part_start <= object_range_end) {
        uint64_t aligned_range_remainder = object_range_end + 1 - second_part_start;
        num_parts += (uint32_t)(aligned_range_remainder / (uint64_t)part_size);

        if ((aligned_range_remainder % part_size) > 0) {
            ++num_parts;
        }
    }

    return num_parts;
}

void aws_s3_get_part_range(
    uint64_t object_range_start,
    uint64_t object_range_end,
    size_t part_size,
    uint32_t part_number,
    uint64_t *out_part_range_start,
    uint64_t *out_part_range_end) {
    AWS_PRECONDITION(out_part_range_start);
    AWS_PRECONDITION(out_part_range_end);

    AWS_ASSERT(part_number > 0);

    const uint32_t part_index = part_number - 1;

    /* Part index is assumed to be in a valid range. */
    AWS_ASSERT(part_index < aws_s3_get_num_parts(part_size, object_range_start, object_range_end));

    uint64_t part_size_uint64 = (uint64_t)part_size;
    uint64_t first_part_size = part_size_uint64;
    uint64_t first_part_alignment_offset = object_range_start % part_size_uint64;

    /* Shrink the part to a smaller size if need be to align to the assumed part boundary. */
    if (first_part_alignment_offset > 0) {
        first_part_size = part_size_uint64 - first_part_alignment_offset;
    }

    if (part_index == 0) {
        /* If this is the first part, then use the first part size. */
        *out_part_range_start = object_range_start;
        *out_part_range_end = *out_part_range_start + first_part_size - 1;
    } else {
        /* Else, find the next part by adding the object range + total number of whole parts before this one + initial
         * part size*/
        *out_part_range_start = object_range_start + ((uint64_t)(part_index - 1)) * part_size_uint64 + first_part_size;
        *out_part_range_end = *out_part_range_start + part_size_uint64 - 1;
    }

    /* Cap the part's range end using the object's range end. */
    if (*out_part_range_end > object_range_end) {
        *out_part_range_end = object_range_end;
    }
}

int aws_s3_crt_error_code_from_server_error_code_string(const struct aws_string *error_code_string) {
    if (aws_string_eq_byte_cursor(error_code_string, &g_s3_slow_down_error_code)) {
        return AWS_ERROR_S3_SLOW_DOWN;
    }
    if (aws_string_eq_byte_cursor(error_code_string, &g_s3_internal_error_code) ||
        aws_string_eq_byte_cursor(error_code_string, &g_s3_internal_errors_code)) {
        return AWS_ERROR_S3_INTERNAL_ERROR;
    }
    return AWS_ERROR_UNKNOWN;
}
