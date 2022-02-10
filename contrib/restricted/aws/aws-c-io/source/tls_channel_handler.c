/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/io/channel.h>
#include <aws/io/file_utils.h>
#include <aws/io/logging.h>
#include <aws/io/tls_channel_handler.h>

#define AWS_DEFAULT_TLS_TIMEOUT_MS 10000

#include <aws/common/string.h>

void aws_tls_ctx_options_init_default_client(struct aws_tls_ctx_options *options, struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*options);
    options->allocator = allocator;
    options->minimum_tls_version = AWS_IO_TLS_VER_SYS_DEFAULTS;
    options->cipher_pref = AWS_IO_TLS_CIPHER_PREF_SYSTEM_DEFAULT;
    options->verify_peer = true;
    options->max_fragment_size = g_aws_channel_max_fragment_size;
}

void aws_tls_ctx_options_clean_up(struct aws_tls_ctx_options *options) {
    if (options->ca_file.len) {
        aws_byte_buf_clean_up(&options->ca_file);
    }

    if (options->ca_path) {
        aws_string_destroy(options->ca_path);
    }

    if (options->certificate.len) {
        aws_byte_buf_clean_up(&options->certificate);
    }

    if (options->private_key.len) {
        aws_byte_buf_clean_up_secure(&options->private_key);
    }

#ifdef __APPLE__
    if (options->pkcs12.len) {
        aws_byte_buf_clean_up_secure(&options->pkcs12);
    }

    if (options->pkcs12_password.len) {
        aws_byte_buf_clean_up_secure(&options->pkcs12_password);
    }
#endif

    if (options->alpn_list) {
        aws_string_destroy(options->alpn_list);
    }

    AWS_ZERO_STRUCT(*options);
}

static int s_load_null_terminated_buffer_from_cursor(
    struct aws_byte_buf *load_into,
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *from) {
    if (from->ptr[from->len - 1] == 0) {
        if (aws_byte_buf_init_copy_from_cursor(load_into, allocator, *from)) {
            return AWS_OP_ERR;
        }

        load_into->len -= 1;
    } else {
        if (aws_byte_buf_init(load_into, allocator, from->len + 1)) {
            return AWS_OP_ERR;
        }

        memcpy(load_into->buffer, from->ptr, from->len);
        load_into->buffer[from->len] = 0;
        load_into->len = from->len;
    }

    return AWS_OP_SUCCESS;
}

#if !defined(AWS_OS_IOS)

int aws_tls_ctx_options_init_client_mtls(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *cert,
    const struct aws_byte_cursor *pkey) {
    AWS_ZERO_STRUCT(*options);
    options->minimum_tls_version = AWS_IO_TLS_VER_SYS_DEFAULTS;
    options->cipher_pref = AWS_IO_TLS_CIPHER_PREF_SYSTEM_DEFAULT;
    options->verify_peer = true;
    options->allocator = allocator;
    options->max_fragment_size = g_aws_channel_max_fragment_size;

    /* s2n relies on null terminated c_strings, so we need to make sure we're properly
     * terminated, but we don't want length to reflect the terminator because
     * Apple and Windows will fail hard if you use a null terminator. */
    if (s_load_null_terminated_buffer_from_cursor(&options->certificate, allocator, cert)) {
        return AWS_OP_ERR;
    }

    if (s_load_null_terminated_buffer_from_cursor(&options->private_key, allocator, pkey)) {
        aws_byte_buf_clean_up(&options->certificate);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_tls_ctx_options_init_client_mtls_from_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *cert_path,
    const char *pkey_path) {
    AWS_ZERO_STRUCT(*options);
    options->minimum_tls_version = AWS_IO_TLS_VER_SYS_DEFAULTS;
    options->cipher_pref = AWS_IO_TLS_CIPHER_PREF_SYSTEM_DEFAULT;
    options->verify_peer = true;
    options->allocator = allocator;
    options->max_fragment_size = g_aws_channel_max_fragment_size;

    if (aws_byte_buf_init_from_file(&options->certificate, allocator, cert_path)) {
        return AWS_OP_ERR;
    }

    if (aws_byte_buf_init_from_file(&options->private_key, allocator, pkey_path)) {
        aws_byte_buf_clean_up(&options->certificate);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

#endif /* AWS_OS_IOS */

#ifdef _WIN32
void aws_tls_ctx_options_init_client_mtls_from_system_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *cert_reg_path) {
    AWS_ZERO_STRUCT(*options);
    options->minimum_tls_version = AWS_IO_TLS_VER_SYS_DEFAULTS;
    options->verify_peer = true;
    options->allocator = allocator;
    options->max_fragment_size = g_aws_channel_max_fragment_size;
    options->system_certificate_path = cert_reg_path;
}

void aws_tls_ctx_options_init_default_server_from_system_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *cert_reg_path) {
    aws_tls_ctx_options_init_client_mtls_from_system_path(options, allocator, cert_reg_path);
    options->verify_peer = false;
}
#endif /* _WIN32 */

#ifdef __APPLE__
int aws_tls_ctx_options_init_client_mtls_pkcs12_from_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *pkcs12_path,
    struct aws_byte_cursor *pkcs_pwd) {
    AWS_ZERO_STRUCT(*options);
    options->minimum_tls_version = AWS_IO_TLS_VER_SYS_DEFAULTS;
    options->verify_peer = true;
    options->allocator = allocator;
    options->max_fragment_size = g_aws_channel_max_fragment_size;

    if (aws_byte_buf_init_from_file(&options->pkcs12, allocator, pkcs12_path)) {
        return AWS_OP_ERR;
    }

    if (aws_byte_buf_init_copy_from_cursor(&options->pkcs12_password, allocator, *pkcs_pwd)) {
        aws_byte_buf_clean_up_secure(&options->pkcs12);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_tls_ctx_options_init_client_mtls_pkcs12(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    struct aws_byte_cursor *pkcs12,
    struct aws_byte_cursor *pkcs_pwd) {
    AWS_ZERO_STRUCT(*options);
    options->minimum_tls_version = AWS_IO_TLS_VER_SYS_DEFAULTS;
    options->verify_peer = true;
    options->allocator = allocator;
    options->max_fragment_size = g_aws_channel_max_fragment_size;

    if (s_load_null_terminated_buffer_from_cursor(&options->pkcs12, allocator, pkcs12)) {
        return AWS_OP_ERR;
    }

    if (s_load_null_terminated_buffer_from_cursor(&options->pkcs12_password, allocator, pkcs_pwd)) {
        aws_byte_buf_clean_up_secure(&options->pkcs12);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_tls_ctx_options_init_server_pkcs12_from_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *pkcs12_path,
    struct aws_byte_cursor *pkcs_password) {
    if (aws_tls_ctx_options_init_client_mtls_pkcs12_from_path(options, allocator, pkcs12_path, pkcs_password)) {
        return AWS_OP_ERR;
    }

    options->verify_peer = false;
    return AWS_OP_SUCCESS;
}

int aws_tls_ctx_options_init_server_pkcs12(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    struct aws_byte_cursor *pkcs12,
    struct aws_byte_cursor *pkcs_password) {
    if (aws_tls_ctx_options_init_client_mtls_pkcs12(options, allocator, pkcs12, pkcs_password)) {
        return AWS_OP_ERR;
    }

    options->verify_peer = false;
    return AWS_OP_SUCCESS;
}

#endif /* __APPLE__ */

#if !defined(AWS_OS_IOS)

int aws_tls_ctx_options_init_default_server_from_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *cert_path,
    const char *pkey_path) {
    if (aws_tls_ctx_options_init_client_mtls_from_path(options, allocator, cert_path, pkey_path)) {
        return AWS_OP_ERR;
    }

    options->verify_peer = false;
    return AWS_OP_SUCCESS;
}

int aws_tls_ctx_options_init_default_server(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    struct aws_byte_cursor *cert,
    struct aws_byte_cursor *pkey) {
    if (aws_tls_ctx_options_init_client_mtls(options, allocator, cert, pkey)) {
        return AWS_OP_ERR;
    }

    options->verify_peer = false;
    return AWS_OP_SUCCESS;
}

#endif /* AWS_OS_IOS */

int aws_tls_ctx_options_set_alpn_list(struct aws_tls_ctx_options *options, const char *alpn_list) {
    options->alpn_list = aws_string_new_from_c_str(options->allocator, alpn_list);
    if (!options->alpn_list) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_tls_ctx_options_set_verify_peer(struct aws_tls_ctx_options *options, bool verify_peer) {
    options->verify_peer = verify_peer;
}

void aws_tls_ctx_options_set_minimum_tls_version(
    struct aws_tls_ctx_options *options,
    enum aws_tls_versions minimum_tls_version) {
    options->minimum_tls_version = minimum_tls_version;
}

int aws_tls_ctx_options_override_default_trust_store_from_path(
    struct aws_tls_ctx_options *options,
    const char *ca_path,
    const char *ca_file) {

    if (ca_path) {
        options->ca_path = aws_string_new_from_c_str(options->allocator, ca_path);
        if (!options->ca_path) {
            return AWS_OP_ERR;
        }
    }

    if (ca_file) {
        if (aws_byte_buf_init_from_file(&options->ca_file, options->allocator, ca_file)) {
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

int aws_tls_ctx_options_override_default_trust_store(
    struct aws_tls_ctx_options *options,
    const struct aws_byte_cursor *ca_file) {

    /* s2n relies on null terminated c_strings, so we need to make sure we're properly
     * terminated, but we don't want length to reflect the terminator because
     * Apple and Windows will fail hard if you use a null terminator. */
    if (s_load_null_terminated_buffer_from_cursor(&options->ca_file, options->allocator, ca_file)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

void aws_tls_connection_options_init_from_ctx(
    struct aws_tls_connection_options *conn_options,
    struct aws_tls_ctx *ctx) {
    AWS_ZERO_STRUCT(*conn_options);
    /* the assumption here, is that if it was set in the context, we WANT it to be NULL here unless it's different.
     * so only set verify peer at this point. */
    conn_options->ctx = aws_tls_ctx_acquire(ctx);

    conn_options->timeout_ms = AWS_DEFAULT_TLS_TIMEOUT_MS;
}

int aws_tls_connection_options_copy(
    struct aws_tls_connection_options *to,
    const struct aws_tls_connection_options *from) {
    /* copy everything copyable over, then override the rest with deep copies. */
    *to = *from;

    to->ctx = aws_tls_ctx_acquire(from->ctx);

    if (from->alpn_list) {
        to->alpn_list = aws_string_new_from_string(from->alpn_list->allocator, from->alpn_list);

        if (!to->alpn_list) {
            return AWS_OP_ERR;
        }
    }

    if (from->server_name) {
        to->server_name = aws_string_new_from_string(from->server_name->allocator, from->server_name);

        if (!to->server_name) {
            aws_string_destroy(to->server_name);
            return AWS_OP_ERR;
        }
    }

    return AWS_OP_SUCCESS;
}

void aws_tls_connection_options_clean_up(struct aws_tls_connection_options *connection_options) {
    aws_tls_ctx_release(connection_options->ctx);

    if (connection_options->alpn_list) {
        aws_string_destroy(connection_options->alpn_list);
    }

    if (connection_options->server_name) {
        aws_string_destroy(connection_options->server_name);
    }

    AWS_ZERO_STRUCT(*connection_options);
}

void aws_tls_connection_options_set_callbacks(
    struct aws_tls_connection_options *conn_options,
    aws_tls_on_negotiation_result_fn *on_negotiation_result,
    aws_tls_on_data_read_fn *on_data_read,
    aws_tls_on_error_fn *on_error,
    void *user_data) {
    conn_options->on_negotiation_result = on_negotiation_result;
    conn_options->on_data_read = on_data_read;
    conn_options->on_error = on_error;
    conn_options->user_data = user_data;
}

int aws_tls_connection_options_set_server_name(
    struct aws_tls_connection_options *conn_options,
    struct aws_allocator *allocator,
    struct aws_byte_cursor *server_name) {
    conn_options->server_name = aws_string_new_from_cursor(allocator, server_name);
    if (!conn_options->server_name) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_tls_connection_options_set_alpn_list(
    struct aws_tls_connection_options *conn_options,
    struct aws_allocator *allocator,
    const char *alpn_list) {

    conn_options->alpn_list = aws_string_new_from_c_str(allocator, alpn_list);
    if (!conn_options->alpn_list) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

int aws_channel_setup_client_tls(
    struct aws_channel_slot *right_of_slot,
    struct aws_tls_connection_options *tls_options) {

    AWS_FATAL_ASSERT(right_of_slot != NULL);
    struct aws_channel *channel = right_of_slot->channel;
    struct aws_allocator *allocator = right_of_slot->alloc;

    struct aws_channel_slot *tls_slot = aws_channel_slot_new(channel);

    /* as far as cleanup goes, since this stuff is being added to a channel, the caller will free this memory
       when they clean up the channel. */
    if (!tls_slot) {
        return AWS_OP_ERR;
    }

    struct aws_channel_handler *tls_handler = aws_tls_client_handler_new(allocator, tls_options, tls_slot);
    if (!tls_handler) {
        aws_mem_release(allocator, tls_slot);
        return AWS_OP_ERR;
    }

    /*
     * From here on out, channel shutdown will handle slot/handler cleanup
     */
    aws_channel_slot_insert_right(right_of_slot, tls_slot);
    AWS_LOGF_TRACE(
        AWS_LS_IO_CHANNEL,
        "id=%p: Setting up client TLS with handler %p on slot %p",
        (void *)channel,
        (void *)tls_handler,
        (void *)tls_slot);

    if (aws_channel_slot_set_handler(tls_slot, tls_handler) != AWS_OP_SUCCESS) {
        return AWS_OP_ERR;
    }

    if (aws_tls_client_handler_start_negotiation(tls_handler) != AWS_OP_SUCCESS) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

struct aws_tls_ctx *aws_tls_ctx_acquire(struct aws_tls_ctx *ctx) {
    if (ctx != NULL) {
        aws_ref_count_acquire(&ctx->ref_count);
    }

    return ctx;
}

void aws_tls_ctx_release(struct aws_tls_ctx *ctx) {
    if (ctx != NULL) {
        aws_ref_count_release(&ctx->ref_count);
    }
}
