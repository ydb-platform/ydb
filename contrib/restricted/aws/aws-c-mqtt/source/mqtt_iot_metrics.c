/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/system_info.h>
#include <aws/common/uri.h>
#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/mqtt_iot_metrics.h>

#include <stdio.h>

// Use packet encoding limit for now: https://github.com/awslabs/aws-c-mqtt/blob/v0.13.3/source/packets.c#L26
const size_t AWS_IOT_MAX_USERNAME_SIZE = UINT16_MAX;
const size_t DEFAULT_QUERY_PARAM_COUNT = 10;

/**
 * Builds final username with query parameters appended.
 *
 * @param base_username The original username cursor
 * @param base_username_length Length of base username to use (may differ from cursor length)
 * @param params_list List of query parameters to append
 * @param output_username [Optional] Buffer to write result. Caller must init/cleanup the buffer.
 * @param out_final_username_size [Optional] Outputs the final username size
 *
 * @return AWS_OP_SUCCESS on success, AWS_OP_ERR on failure
 */
int s_build_username_query(
    const struct aws_byte_cursor *base_username,
    size_t base_username_length,
    const struct aws_array_list *params_list,
    struct aws_byte_buf *output_username,
    size_t *out_final_username_size) {

    AWS_ASSERT(base_username);
    AWS_ASSERT(params_list);

    if (output_username) {
        if (!aws_byte_buf_write(output_username, base_username->ptr, base_username_length)) {
            return AWS_OP_ERR;
        }
    }

    if (out_final_username_size) {
        *out_final_username_size = base_username_length;
    }

    struct aws_byte_cursor query_delim = aws_byte_cursor_from_c_str("?");
    struct aws_byte_cursor query_param_amp = aws_byte_cursor_from_c_str("&");
    struct aws_byte_cursor key_value_delim = aws_byte_cursor_from_c_str("=");

    size_t params_count = aws_array_list_length(params_list);
    for (size_t i = 0; i < params_count; ++i) {
        struct aws_uri_param param;
        AWS_ZERO_STRUCT(param);
        aws_array_list_get_at(params_list, &param, i);

        if (output_username) {
            if (i == 0) {
                aws_byte_buf_append(output_username, &query_delim);
            } else {
                aws_byte_buf_append(output_username, &query_param_amp);
            }
        }

        if (out_final_username_size) {
            *out_final_username_size += 1;
        }

        if (output_username) {
            if (param.key.len > 0) {
                // append key if key exists
                if (aws_byte_buf_append(output_username, &param.key)) {
                    return AWS_OP_ERR;
                }
            }

            // append value if value exists
            if (param.value.len > 0)
                // Note: If value exists without a key, append "=" and value (e.g., "?=abc").
                // Please note server treats "a=", "a", and "=a" equivalently.
                if ((aws_byte_buf_append(output_username, &key_value_delim)) ||
                    aws_byte_buf_append(output_username, &param.value)) {
                    return AWS_OP_ERR;
                }
        }

        if (out_final_username_size) {
            *out_final_username_size += param.key.len + (param.value.len > 0 ? 1 : 0) + param.value.len;
        }
    }

    return AWS_OP_SUCCESS;
}

// TODO Future Work: we ignored the metadata field for now, will add them in future support
int aws_mqtt_append_sdk_metrics_to_username(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *original_username,
    const struct aws_mqtt_iot_metrics *metrics,
    struct aws_byte_buf *output_username,
    size_t *out_full_username_size) {
    AWS_PRECONDITION(
        output_username == NULL ||
        (aws_byte_buf_is_valid(output_username) && (output_username && output_username->buffer == NULL)));

    if (!allocator) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }
    const struct aws_byte_cursor local_original_username =
        original_username == NULL ? aws_byte_cursor_from_c_str("") : *original_username;

    if (!metrics) {
        if (out_full_username_size) {
            *out_full_username_size = local_original_username.len;
        }

        if (output_username) {
            return aws_byte_buf_init_copy_from_cursor(output_username, allocator, local_original_username);
        }

        return AWS_OP_SUCCESS;
    }

    if (aws_mqtt_validate_iot_metrics(metrics)) {
        return AWS_OP_ERR;
    }

    int result = AWS_OP_ERR;
    // The length of the base username part not including query parameters
    size_t base_username_length = 0;
    struct aws_byte_cursor question_mark_str = aws_byte_cursor_from_c_str("?");
    struct aws_byte_cursor sdk_str = aws_byte_cursor_from_c_str("SDK");
    struct aws_byte_cursor platform_str = aws_byte_cursor_from_c_str("Platform");

    struct aws_array_list params_list;
    aws_array_list_init_dynamic(&params_list, allocator, DEFAULT_QUERY_PARAM_COUNT, sizeof(struct aws_uri_param));

    // Looking for any existing query in the original username
    if (local_original_username.len > 0) {
        struct aws_byte_cursor question_mark_find = local_original_username;

        bool found_query = false;
        // Find the last question mark. The IoT service will trim string after last question mark and handle it as
        // metrics metadata
        while (AWS_OP_SUCCESS ==
               aws_byte_cursor_find_exact(&question_mark_find, &question_mark_str, &question_mark_find)) {
            // Advance cursor to skip the "?" character
            aws_byte_cursor_advance(&question_mark_find, 1);
            found_query = true;
        }

        if (found_query) {
            // Trim out the query delimiter from the base username
            base_username_length = question_mark_find.ptr - 1 - local_original_username.ptr;
            aws_query_string_params(question_mark_find, &params_list);
        } else {
            base_username_length = local_original_username.len;
        }
    }

    // Verify if the username already contains "SDK" and "Platform" fields.
    bool found_sdk = false;
    bool found_platform = false;

    size_t params_count = aws_array_list_length(&params_list);
    for (size_t i = 0; i < params_count; ++i) {
        struct aws_uri_param param;
        AWS_ZERO_STRUCT(param);
        aws_array_list_get_at(&params_list, &param, i);
        if (aws_byte_cursor_eq(&param.key, &sdk_str)) {
            found_sdk = true;
        } else if (aws_byte_cursor_eq(&param.key, &platform_str)) {
            found_platform = true;
        }
    }

    if (!found_sdk) {
        struct aws_uri_param sdk_params = {
            .key = sdk_str,
            .value =
                metrics->library_name.len > 0 ? metrics->library_name : aws_byte_cursor_from_c_str("IoTDeviceSDK/C"),
        };
        aws_array_list_push_back(&params_list, &sdk_params);
    }

    if (!found_platform) {
        struct aws_uri_param platform_params = {
            .key = platform_str,
            .value = aws_get_platform_build_os_string(),
        };
        aws_array_list_push_back(&params_list, &platform_params);
    }

    // Rebuild metrics string from params_list
    // First parse to get final username size
    size_t total_size = 0;
    s_build_username_query(&local_original_username, base_username_length, &params_list, NULL, &total_size);

    if (total_size > AWS_IOT_MAX_USERNAME_SIZE) {
        AWS_LOGF_ERROR(
            AWS_LL_DEBUG, "Failed to append SDK metrics to username: resulting username exceeds max username size.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto cleanup;
    }

    if (output_username && aws_byte_buf_init(output_username, allocator, total_size)) {
        goto cleanup;
    }

    // build final output username
    if (s_build_username_query(
            &local_original_username, base_username_length, &params_list, output_username, out_full_username_size)) {
        goto cleanup;
    }

    result = AWS_OP_SUCCESS;

cleanup:
    aws_array_list_clean_up(&params_list);

    if (result == AWS_OP_ERR) {
        aws_byte_buf_clean_up(output_username);
    }
    return result;
}

/*********************************************************************************************************************
 * IoT SDK Metrics
 ********************************************************************************************************************/

size_t aws_mqtt_iot_metrics_compute_storage_size(const struct aws_mqtt_iot_metrics *metrics) {
    if (metrics == NULL) {
        return 0;
    }

    size_t storage_size = 0;
    storage_size += metrics->library_name.len;

    return storage_size;
}

struct aws_mqtt_iot_metrics_storage *aws_mqtt_iot_metrics_storage_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt_iot_metrics *metrics_options) {

    if (allocator == NULL || metrics_options == NULL) {
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_mqtt_iot_metrics_storage *metrics_storage =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_mqtt_iot_metrics_storage));

    size_t storage_capacity = aws_mqtt_iot_metrics_compute_storage_size(metrics_options);
    if (aws_byte_buf_init(&metrics_storage->storage, allocator, storage_capacity)) {
        goto cleanup_storage;
    }

    metrics_storage->allocator = allocator;

    struct aws_mqtt_iot_metrics *storage_view = &metrics_storage->storage_view;

    if (metrics_options->library_name.len > 0) {
        metrics_storage->library_name = metrics_options->library_name;
        if (aws_byte_buf_append_and_update(&metrics_storage->storage, &metrics_storage->library_name)) {
            goto cleanup_storage;
        }
        storage_view->library_name = metrics_storage->library_name;
    }

    return metrics_storage;

cleanup_storage:
    // TODO Future Work: add metadata entries once we implemented the metadata feature
    // if (aws_array_list_is_valid(&metrics_storage->metadata_entries)) {
    //     aws_array_list_clean_up(&metrics_storage->metadata_entries);
    // }

    aws_byte_buf_clean_up(&metrics_storage->storage);
    aws_mem_release(allocator, metrics_storage);
    return NULL;
}

void aws_mqtt_iot_metrics_storage_destroy(struct aws_mqtt_iot_metrics_storage *metrics_storage) {
    if (metrics_storage == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&metrics_storage->storage);

    aws_mem_release(metrics_storage->allocator, metrics_storage);
}

int aws_mqtt_validate_iot_metrics(const struct aws_mqtt_iot_metrics *metrics) {
    if (metrics == NULL) {
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    if (aws_mqtt_validate_utf8_text(metrics->library_name)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}
