/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/io/channel.h>
#include <aws/io/file_utils.h>
#include <aws/io/logging.h>
#include <aws/io/pkcs11.h>
#include <aws/io/private/pem_utils.h>
#include <aws/io/private/tls_channel_handler_shared.h>
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
    aws_byte_buf_clean_up(&options->ca_file);
    aws_string_destroy(options->ca_path);
    aws_byte_buf_clean_up(&options->certificate);
    aws_byte_buf_clean_up_secure(&options->private_key);

#ifdef __APPLE__
    aws_byte_buf_clean_up_secure(&options->pkcs12);
    aws_byte_buf_clean_up_secure(&options->pkcs12_password);

#    if !defined(AWS_OS_IOS)
    aws_string_destroy(options->keychain_path);
#    endif
#endif

    aws_string_destroy(options->alpn_list);

    aws_pkcs11_lib_release(options->pkcs11.lib);
    aws_string_destroy_secure(options->pkcs11.user_pin);
    aws_string_destroy(options->pkcs11.token_label);
    aws_string_destroy(options->pkcs11.private_key_object_label);

    AWS_ZERO_STRUCT(*options);
}

int aws_tls_ctx_options_init_client_mtls(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *cert,
    const struct aws_byte_cursor *pkey) {

#if !defined(AWS_OS_IOS)

    aws_tls_ctx_options_init_default_client(options, allocator);

    if (aws_byte_buf_init_copy_from_cursor(&options->certificate, allocator, *cert)) {
        goto error;
    }

    if (aws_sanitize_pem(&options->certificate, allocator)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Invalid certificate. File must contain PEM encoded data");
        goto error;
    }

    if (aws_byte_buf_init_copy_from_cursor(&options->private_key, allocator, *pkey)) {
        goto error;
    }

    if (aws_sanitize_pem(&options->private_key, allocator)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Invalid private key. File must contain PEM encoded data");
        goto error;
    }

    return AWS_OP_SUCCESS;
error:
    aws_tls_ctx_options_clean_up(options);
    return AWS_OP_ERR;

#else
    (void)allocator;
    (void)cert;
    (void)pkey;
    AWS_ZERO_STRUCT(*options);
    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: This platform does not support PEM certificates");
    return aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
#endif
}

int aws_tls_ctx_options_init_client_mtls_from_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *cert_path,
    const char *pkey_path) {

#if !defined(AWS_OS_IOS)
    aws_tls_ctx_options_init_default_client(options, allocator);

    if (aws_byte_buf_init_from_file(&options->certificate, allocator, cert_path)) {
        goto error;
    }

    if (aws_sanitize_pem(&options->certificate, allocator)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Invalid certificate. File must contain PEM encoded data");
        goto error;
    }

    if (aws_byte_buf_init_from_file(&options->private_key, allocator, pkey_path)) {
        goto error;
    }

    if (aws_sanitize_pem(&options->private_key, allocator)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Invalid private key. File must contain PEM encoded data");
        goto error;
    }

    return AWS_OP_SUCCESS;
error:
    aws_tls_ctx_options_clean_up(options);
    return AWS_OP_ERR;

#else
    (void)allocator;
    (void)cert_path;
    (void)pkey_path;
    AWS_ZERO_STRUCT(*options);
    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: This platform does not support PEM certificates");
    return aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
#endif
}

int aws_tls_ctx_options_init_client_mtls_with_pkcs11(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const struct aws_tls_ctx_pkcs11_options *pkcs11_options) {

#if defined(_WIN32) || defined(__APPLE__)
    (void)allocator;
    (void)pkcs11_options;
    AWS_ZERO_STRUCT(*options);
    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: This platform does not currently support TLS with PKCS#11.");
    return aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
#else

    aws_tls_ctx_options_init_default_client(options, allocator);

    /* pkcs11_lib is required */
    if (pkcs11_options->pkcs11_lib == NULL) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: A PKCS#11 library must be specified.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }
    options->pkcs11.lib = aws_pkcs11_lib_acquire(pkcs11_options->pkcs11_lib); /* cannot fail */

    /* user_pin is optional */
    if (pkcs11_options->user_pin.ptr != NULL) {
        options->pkcs11.user_pin = aws_string_new_from_cursor(allocator, &pkcs11_options->user_pin);
    }

    /* slot_id is optional */
    if (pkcs11_options->slot_id != NULL) {
        options->pkcs11.slot_id = *pkcs11_options->slot_id;
        options->pkcs11.has_slot_id = true;
    }

    /* token_label is optional */
    if (pkcs11_options->token_label.ptr != NULL) {
        options->pkcs11.token_label = aws_string_new_from_cursor(allocator, &pkcs11_options->token_label);
    }

    /* private_key_object_label is optional */
    if (pkcs11_options->private_key_object_label.ptr != NULL) {
        options->pkcs11.private_key_object_label =
            aws_string_new_from_cursor(allocator, &pkcs11_options->private_key_object_label);
    }

    /* certificate required, but there are multiple ways to pass it in */
    if ((pkcs11_options->cert_file_path.ptr != NULL) && (pkcs11_options->cert_file_contents.ptr != NULL)) {
        AWS_LOGF_ERROR(
            AWS_LS_IO_TLS, "static: Both certificate filepath and contents are specified. Only one may be set.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    } else if (pkcs11_options->cert_file_path.ptr != NULL) {
        struct aws_string *tmp_string = aws_string_new_from_cursor(allocator, &pkcs11_options->cert_file_path);
        int op = aws_byte_buf_init_from_file(&options->certificate, allocator, aws_string_c_str(tmp_string));
        aws_string_destroy(tmp_string);
        if (op != AWS_OP_SUCCESS) {
            goto error;
        }
    } else if (pkcs11_options->cert_file_contents.ptr != NULL) {
        if (aws_byte_buf_init_copy_from_cursor(&options->certificate, allocator, pkcs11_options->cert_file_contents)) {
            goto error;
        }
    } else {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: A certificate must be specified.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        goto error;
    }

    if (aws_sanitize_pem(&options->certificate, allocator)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Invalid certificate. File must contain PEM encoded data");
        goto error;
    }

    /* Success! */
    return AWS_OP_SUCCESS;

error:
    aws_tls_ctx_options_clean_up(options);
    return AWS_OP_ERR;
#endif /* PLATFORM-SUPPORTS-PKCS11-TLS */
}

int aws_tls_ctx_options_set_keychain_path(
    struct aws_tls_ctx_options *options,
    struct aws_byte_cursor *keychain_path_cursor) {

#if defined(__APPLE__) && !defined(AWS_OS_IOS)
    AWS_LOGF_WARN(AWS_LS_IO_TLS, "static: Keychain path is deprecated.");

    options->keychain_path = aws_string_new_from_cursor(options->allocator, keychain_path_cursor);
    if (!options->keychain_path) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
#else
    (void)options;
    (void)keychain_path_cursor;
    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Keychain path can only be set on MacOS.");
    return aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
#endif
}

int aws_tls_ctx_options_init_client_mtls_from_system_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *cert_reg_path) {

#ifdef _WIN32
    aws_tls_ctx_options_init_default_client(options, allocator);
    options->system_certificate_path = cert_reg_path;
    return AWS_OP_SUCCESS;
#else
    (void)allocator;
    (void)cert_reg_path;
    AWS_ZERO_STRUCT(*options);
    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: System certificate path can only be set on Windows.");
    return aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
#endif
}

int aws_tls_ctx_options_init_default_server_from_system_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *cert_reg_path) {
    if (aws_tls_ctx_options_init_client_mtls_from_system_path(options, allocator, cert_reg_path)) {
        return AWS_OP_ERR;
    }
    options->verify_peer = false;
    return AWS_OP_SUCCESS;
}

int aws_tls_ctx_options_init_client_mtls_pkcs12_from_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *pkcs12_path,
    struct aws_byte_cursor *pkcs_pwd) {

#ifdef __APPLE__
    aws_tls_ctx_options_init_default_client(options, allocator);

    if (aws_byte_buf_init_from_file(&options->pkcs12, allocator, pkcs12_path)) {
        return AWS_OP_ERR;
    }

    if (aws_byte_buf_init_copy_from_cursor(&options->pkcs12_password, allocator, *pkcs_pwd)) {
        aws_byte_buf_clean_up_secure(&options->pkcs12);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
#else
    (void)allocator;
    (void)pkcs12_path;
    (void)pkcs_pwd;
    AWS_ZERO_STRUCT(*options);
    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: This platform does not support PKCS#12 files.");
    return aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
#endif
}

int aws_tls_ctx_options_init_client_mtls_pkcs12(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    struct aws_byte_cursor *pkcs12,
    struct aws_byte_cursor *pkcs_pwd) {

#ifdef __APPLE__
    aws_tls_ctx_options_init_default_client(options, allocator);

    if (aws_byte_buf_init_copy_from_cursor(&options->pkcs12, allocator, *pkcs12)) {
        return AWS_OP_ERR;
    }

    if (aws_byte_buf_init_copy_from_cursor(&options->pkcs12_password, allocator, *pkcs_pwd)) {
        aws_byte_buf_clean_up_secure(&options->pkcs12);
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
#else
    (void)allocator;
    (void)pkcs12;
    (void)pkcs_pwd;
    AWS_ZERO_STRUCT(*options);
    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: This platform does not support PKCS#12 files.");
    return aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
#endif
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

int aws_tls_ctx_options_init_default_server_from_path(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    const char *cert_path,
    const char *pkey_path) {

#if !defined(AWS_OS_IOS)
    if (aws_tls_ctx_options_init_client_mtls_from_path(options, allocator, cert_path, pkey_path)) {
        return AWS_OP_ERR;
    }

    options->verify_peer = false;
    return AWS_OP_SUCCESS;
#else
    (void)allocator;
    (void)cert_path;
    (void)pkey_path;
    AWS_ZERO_STRUCT(*options);
    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Cannot create a server on this platform.");
    return aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
#endif
}

int aws_tls_ctx_options_init_default_server(
    struct aws_tls_ctx_options *options,
    struct aws_allocator *allocator,
    struct aws_byte_cursor *cert,
    struct aws_byte_cursor *pkey) {

#if !defined(AWS_OS_IOS)
    if (aws_tls_ctx_options_init_client_mtls(options, allocator, cert, pkey)) {
        return AWS_OP_ERR;
    }

    options->verify_peer = false;
    return AWS_OP_SUCCESS;
#else
    (void)allocator;
    (void)cert;
    (void)pkey;
    AWS_ZERO_STRUCT(*options);
    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Cannot create a server on this platform.");
    return aws_raise_error(AWS_ERROR_PLATFORM_NOT_SUPPORTED);
#endif
}

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

    /* Note: on success these are not cleaned up, their data is "moved" into the options struct */
    struct aws_string *ca_path_tmp = NULL;
    struct aws_byte_buf ca_file_tmp;
    AWS_ZERO_STRUCT(ca_file_tmp);

    if (ca_path) {
        if (options->ca_path) {
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: cannot override trust store multiple times");
            aws_raise_error(AWS_ERROR_INVALID_STATE);
            goto error;
        }

        ca_path_tmp = aws_string_new_from_c_str(options->allocator, ca_path);
        if (!ca_path_tmp) {
            goto error;
        }
    }

    if (ca_file) {
        if (aws_tls_options_buf_is_set(&options->ca_file)) {
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: cannot override trust store multiple times");
            aws_raise_error(AWS_ERROR_INVALID_STATE);
            goto error;
        }

        if (aws_byte_buf_init_from_file(&ca_file_tmp, options->allocator, ca_file)) {
            goto error;
        }

        if (aws_sanitize_pem(&ca_file_tmp, options->allocator)) {
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Invalid CA file. File must contain PEM encoded data");
            goto error;
        }
    }

    /* Success, set new values. (no need to clean up old values, we checked earlier that they were unallocated) */
    if (ca_path) {
        options->ca_path = ca_path_tmp;
    }
    if (ca_file) {
        options->ca_file = ca_file_tmp;
    }
    return AWS_OP_SUCCESS;

error:
    aws_string_destroy_secure(ca_path_tmp);
    aws_byte_buf_clean_up_secure(&ca_file_tmp);
    return AWS_OP_ERR;
}

void aws_tls_ctx_options_set_extension_data(struct aws_tls_ctx_options *options, void *extension_data) {
    options->ctx_options_extension = extension_data;
}

int aws_tls_ctx_options_override_default_trust_store(
    struct aws_tls_ctx_options *options,
    const struct aws_byte_cursor *ca_file) {

    if (aws_tls_options_buf_is_set(&options->ca_file)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: cannot override trust store multiple times");
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    if (aws_byte_buf_init_copy_from_cursor(&options->ca_file, options->allocator, *ca_file)) {
        goto error;
    }

    if (aws_sanitize_pem(&options->ca_file, options->allocator)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: Invalid CA file. File must contain PEM encoded data");
        goto error;
    }

    return AWS_OP_SUCCESS;

error:
    aws_byte_buf_clean_up_secure(&options->ca_file);
    return AWS_OP_ERR;
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

    /* clean up the options before copy. */
    aws_tls_connection_options_clean_up(to);

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

    if (conn_options->server_name != NULL) {
        aws_string_destroy(conn_options->server_name);
        conn_options->server_name = NULL;
    }

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

    if (conn_options->alpn_list != NULL) {
        aws_string_destroy(conn_options->alpn_list);
        conn_options->alpn_list = NULL;
    }

    conn_options->alpn_list = aws_string_new_from_c_str(allocator, alpn_list);
    if (!conn_options->alpn_list) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

#ifdef BYO_CRYPTO

struct aws_tls_ctx *aws_tls_server_ctx_new(struct aws_allocator *alloc, const struct aws_tls_ctx_options *options) {
    (void)alloc;
    (void)options;
    AWS_FATAL_ASSERT(
        false &&
        "When using BYO_CRYPTO, user is responsible for creating aws_tls_ctx manually. You cannot call this function.");
}

struct aws_tls_ctx *aws_tls_client_ctx_new(struct aws_allocator *alloc, const struct aws_tls_ctx_options *options) {
    (void)alloc;
    (void)options;
    AWS_FATAL_ASSERT(
        false &&
        "When using BYO_CRYPTO, user is responsible for creating aws_tls_ctx manually. You cannot call this function.");
}

static aws_tls_handler_new_fn *s_client_handler_new = NULL;
static aws_tls_client_handler_start_negotiation_fn *s_start_negotiation_fn = NULL;
static void *s_client_user_data = NULL;

static aws_tls_handler_new_fn *s_server_handler_new = NULL;
static void *s_server_user_data = NULL;

struct aws_channel_handler *aws_tls_client_handler_new(
    struct aws_allocator *allocator,
    struct aws_tls_connection_options *options,
    struct aws_channel_slot *slot) {
    AWS_FATAL_ASSERT(
        s_client_handler_new &&
        "For BYO_CRYPTO, you must call aws_tls_client_handler_new_set_callback() with a non-null value.");
    return s_client_handler_new(allocator, options, slot, s_client_user_data);
}

struct aws_channel_handler *aws_tls_server_handler_new(
    struct aws_allocator *allocator,
    struct aws_tls_connection_options *options,
    struct aws_channel_slot *slot) {
    AWS_FATAL_ASSERT(
        s_client_handler_new &&
        "For BYO_CRYPTO, you must call aws_tls_server_handler_new_set_callback() with a non-null value.")
    return s_server_handler_new(allocator, options, slot, s_server_user_data);
}

void aws_tls_byo_crypto_set_client_setup_options(const struct aws_tls_byo_crypto_setup_options *options) {
    AWS_FATAL_ASSERT(options);
    AWS_FATAL_ASSERT(options->new_handler_fn);
    AWS_FATAL_ASSERT(options->start_negotiation_fn);

    s_client_handler_new = options->new_handler_fn;
    s_start_negotiation_fn = options->start_negotiation_fn;
    s_client_user_data = options->user_data;
}

void aws_tls_byo_crypto_set_server_setup_options(const struct aws_tls_byo_crypto_setup_options *options) {
    AWS_FATAL_ASSERT(options);
    AWS_FATAL_ASSERT(options->new_handler_fn);

    s_server_handler_new = options->new_handler_fn;
    s_server_user_data = options->user_data;
}

int aws_tls_client_handler_start_negotiation(struct aws_channel_handler *handler) {
    AWS_FATAL_ASSERT(
        s_start_negotiation_fn &&
        "For BYO_CRYPTO, you must call aws_tls_client_handler_set_start_negotiation_callback() with a non-null value.")
    return s_start_negotiation_fn(handler, s_client_user_data);
}

void aws_tls_init_static_state(struct aws_allocator *alloc) {
    (void)alloc;
}

void aws_tls_clean_up_static_state(void) {}

#endif /* BYO_CRYPTO */

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
