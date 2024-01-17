/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/auth/private/credentials_utils.h>

#include <aws/common/date_time.h>
#include <aws/common/json.h>
#include <aws/common/string.h>
#include <aws/common/uuid.h>
#include <aws/http/connection.h>
#include <aws/http/request_response.h>
#include <aws/http/status_code.h>

#if defined(_MSC_VER)
#    pragma warning(disable : 4232)
#endif /* _MSC_VER */

static struct aws_auth_http_system_vtable s_default_function_table = {
    .aws_http_connection_manager_new = aws_http_connection_manager_new,
    .aws_http_connection_manager_release = aws_http_connection_manager_release,
    .aws_http_connection_manager_acquire_connection = aws_http_connection_manager_acquire_connection,
    .aws_http_connection_manager_release_connection = aws_http_connection_manager_release_connection,
    .aws_http_connection_make_request = aws_http_connection_make_request,
    .aws_http_stream_activate = aws_http_stream_activate,
    .aws_http_stream_get_connection = aws_http_stream_get_connection,
    .aws_http_stream_get_incoming_response_status = aws_http_stream_get_incoming_response_status,
    .aws_http_stream_release = aws_http_stream_release,
    .aws_http_connection_close = aws_http_connection_close,
};

const struct aws_auth_http_system_vtable *g_aws_credentials_provider_http_function_table = &s_default_function_table;

void aws_credentials_query_init(
    struct aws_credentials_query *query,
    struct aws_credentials_provider *provider,
    aws_on_get_credentials_callback_fn *callback,
    void *user_data) {
    AWS_ZERO_STRUCT(*query);

    query->provider = provider;
    query->user_data = user_data;
    query->callback = callback;

    aws_credentials_provider_acquire(provider);
}

void aws_credentials_query_clean_up(struct aws_credentials_query *query) {
    if (query != NULL) {
        aws_credentials_provider_release(query->provider);
    }
}

void aws_credentials_provider_init_base(
    struct aws_credentials_provider *provider,
    struct aws_allocator *allocator,
    struct aws_credentials_provider_vtable *vtable,
    void *impl) {

    provider->allocator = allocator;
    provider->vtable = vtable;
    provider->impl = impl;

    aws_atomic_init_int(&provider->ref_count, 1);
}

void aws_credentials_provider_invoke_shutdown_callback(struct aws_credentials_provider *provider) {
    if (provider && provider->shutdown_options.shutdown_callback) {
        provider->shutdown_options.shutdown_callback(provider->shutdown_options.shutdown_user_data);
    }
}

static bool s_parse_expiration_value_from_json_object(
    struct aws_json_value *value,
    const struct aws_parse_credentials_from_json_doc_options *options,
    uint64_t *expiration_timepoint_in_seconds) {

    if (value == NULL) {
        AWS_LOGF_INFO(AWS_LS_AUTH_CREDENTIALS_PROVIDER, "No credentials Expiration field in Json document.");
        return false;
    }

    struct aws_byte_cursor expiration_cursor = {
        .ptr = NULL,
        .len = 0,
    };

    switch (options->expiration_format) {
        case AWS_PCEF_STRING_ISO_8601_DATE: {

            if (aws_json_value_get_string(value, &expiration_cursor)) {
                AWS_LOGF_INFO(
                    AWS_LS_AUTH_CREDENTIALS_PROVIDER,
                    "Unabled to extract credentials Expiration field from Json document.");
                return false;
            }

            if (expiration_cursor.len == 0) {
                AWS_LOGF_INFO(
                    AWS_LS_AUTH_CREDENTIALS_PROVIDER, "Parsed a credentials json document with empty expiration.");
                return false;
            }

            struct aws_date_time expiration;
            if (aws_date_time_init_from_str_cursor(&expiration, &expiration_cursor, AWS_DATE_FORMAT_ISO_8601)) {
                AWS_LOGF_INFO(
                    AWS_LS_AUTH_CREDENTIALS_PROVIDER,
                    "credentials Expiration in Json document is not a valid ISO_8601 date string.");
                return false;
            }

            *expiration_timepoint_in_seconds = (uint64_t)aws_date_time_as_epoch_secs(&expiration);
            return true;
        }

        case AWS_PCEF_NUMBER_UNIX_EPOCH: {
            double expiration_value = 0;
            if (aws_json_value_get_number(value, &expiration_value)) {
                AWS_LOGF_INFO(
                    AWS_LS_AUTH_CREDENTIALS_PROVIDER,
                    "Unabled to extract credentials Expiration field from Json document.");
                return false;
            }

            *expiration_timepoint_in_seconds = (uint64_t)expiration_value;
            return true;
        }

        default:
            return false;
    }
}

struct aws_credentials *aws_parse_credentials_from_aws_json_object(
    struct aws_allocator *allocator,
    struct aws_json_value *document_root,
    const struct aws_parse_credentials_from_json_doc_options *options) {

    AWS_FATAL_ASSERT(allocator);
    AWS_FATAL_ASSERT(document_root);
    AWS_FATAL_ASSERT(options);
    AWS_FATAL_ASSERT(options->access_key_id_name);
    AWS_FATAL_ASSERT(options->secret_access_key_name);

    if (options->token_required) {
        AWS_FATAL_ASSERT(options->token_name);
    }

    if (options->expiration_required) {
        AWS_FATAL_ASSERT(options->expiration_name);
    }

    struct aws_credentials *credentials = NULL;
    struct aws_json_value *access_key_id = NULL;
    struct aws_json_value *secrete_access_key = NULL;
    struct aws_json_value *token = NULL;
    struct aws_json_value *creds_expiration = NULL;

    bool parse_error = true;

    /*
     * Pull out the credentials components
     */
    struct aws_byte_cursor access_key_id_cursor;
    access_key_id =
        aws_json_value_get_from_object(document_root, aws_byte_cursor_from_c_str((char *)options->access_key_id_name));
    if (!aws_json_value_is_string(access_key_id) ||
        aws_json_value_get_string(access_key_id, &access_key_id_cursor) == AWS_OP_ERR) {
        AWS_LOGF_ERROR(AWS_LS_AUTH_CREDENTIALS_PROVIDER, "Failed to parse AccessKeyId from Json document.");
        goto done;
    }

    struct aws_byte_cursor secrete_access_key_cursor;
    secrete_access_key = aws_json_value_get_from_object(
        document_root, aws_byte_cursor_from_c_str((char *)options->secret_access_key_name));
    if (!aws_json_value_is_string(secrete_access_key) ||
        aws_json_value_get_string(secrete_access_key, &secrete_access_key_cursor) == AWS_OP_ERR) {
        AWS_LOGF_ERROR(AWS_LS_AUTH_CREDENTIALS_PROVIDER, "Failed to parse SecretAccessKey from Json document.");
        goto done;
    }

    struct aws_byte_cursor token_cursor;
    if (options->token_name) {
        token = aws_json_value_get_from_object(document_root, aws_byte_cursor_from_c_str((char *)options->token_name));
        if (!aws_json_value_is_string(token) || aws_json_value_get_string(token, &token_cursor) == AWS_OP_ERR) {
            if (options->token_required) {
                AWS_LOGF_ERROR(AWS_LS_AUTH_CREDENTIALS_PROVIDER, "Failed to parse Token from Json document.");
                goto done;
            }
        }
    }

    // needed to avoid uninitialized local variable error
    uint64_t expiration_timepoint_in_seconds = UINT64_MAX;
    if (options->expiration_name) {
        creds_expiration =
            aws_json_value_get_from_object(document_root, aws_byte_cursor_from_c_str((char *)options->expiration_name));

        if (!s_parse_expiration_value_from_json_object(creds_expiration, options, &expiration_timepoint_in_seconds)) {
            if (options->expiration_required) {
                AWS_LOGF_ERROR(AWS_LS_AUTH_CREDENTIALS_PROVIDER, "Failed to parse Expiration from Json document.");
                goto done;
            }
        }
    }

    /*
     * Build the credentials
     */
    if (access_key_id_cursor.len == 0 || secrete_access_key_cursor.len == 0) {
        AWS_LOGF_ERROR(
            AWS_LS_AUTH_CREDENTIALS_PROVIDER,
            "Parsed an unexpected credentials json document, either access key, secret key is empty.");
        goto done;
    }

    struct aws_byte_cursor session_token_cursor;
    AWS_ZERO_STRUCT(session_token_cursor);

    if (token) {
        aws_json_value_get_string(token, &session_token_cursor);
        if (options->token_required && session_token_cursor.len == 0) {
            AWS_LOGF_ERROR(
                AWS_LS_AUTH_CREDENTIALS_PROVIDER, "Parsed an unexpected credentials json document with empty token.");
            goto done;
        }
    }

    credentials = aws_credentials_new(
        allocator,
        access_key_id_cursor,
        secrete_access_key_cursor,
        session_token_cursor,
        expiration_timepoint_in_seconds);

    if (credentials == NULL) {
        AWS_LOGF_ERROR(AWS_LS_AUTH_CREDENTIALS_PROVIDER, "Failed to allocate memory for credentials.");
        parse_error = false;
        goto done;
    }

done:

    if (parse_error) {
        aws_raise_error(AWS_AUTH_PROVIDER_PARSER_UNEXPECTED_RESPONSE);
    }

    return credentials;
}

struct aws_credentials *aws_parse_credentials_from_json_document(
    struct aws_allocator *allocator,
    const char *document,
    const struct aws_parse_credentials_from_json_doc_options *options) {

    struct aws_json_value *document_root =
        aws_json_value_new_from_string(allocator, aws_byte_cursor_from_c_str(document));
    if (document_root == NULL) {
        AWS_LOGF_ERROR(AWS_LS_AUTH_CREDENTIALS_PROVIDER, "Failed to parse document as Json document.");
        return NULL;
    }
    struct aws_credentials *credentials = aws_parse_credentials_from_aws_json_object(allocator, document_root, options);
    aws_json_value_destroy(document_root);
    return credentials;
}

static bool s_is_transient_network_error(int error_code) {
    return error_code == AWS_ERROR_HTTP_CONNECTION_CLOSED || error_code == AWS_ERROR_HTTP_SERVER_CLOSED ||
           error_code == AWS_IO_SOCKET_CLOSED || error_code == AWS_IO_SOCKET_CONNECT_ABORTED ||
           error_code == AWS_IO_SOCKET_CONNECTION_REFUSED || error_code == AWS_IO_SOCKET_NETWORK_DOWN ||
           error_code == AWS_IO_DNS_QUERY_FAILED || error_code == AWS_IO_DNS_NO_ADDRESS_FOR_HOST ||
           error_code == AWS_IO_SOCKET_TIMEOUT || error_code == AWS_IO_TLS_NEGOTIATION_TIMEOUT ||
           error_code == AWS_HTTP_STATUS_CODE_408_REQUEST_TIMEOUT;
}

enum aws_retry_error_type aws_credentials_provider_compute_retry_error_type(int response_code, int error_code) {

    enum aws_retry_error_type error_type = response_code >= 400 && response_code < 500
                                               ? AWS_RETRY_ERROR_TYPE_CLIENT_ERROR
                                               : AWS_RETRY_ERROR_TYPE_SERVER_ERROR;

    if (s_is_transient_network_error(error_code)) {
        error_type = AWS_RETRY_ERROR_TYPE_TRANSIENT;
    }

    /* server throttling us is retryable */
    if (response_code == AWS_HTTP_STATUS_CODE_429_TOO_MANY_REQUESTS) {
        /* force a new connection on this. */
        error_type = AWS_RETRY_ERROR_TYPE_THROTTLING;
    }

    return error_type;
}
