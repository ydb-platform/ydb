/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/io/tls_channel_handler.h>

#include <aws/common/clock.h>
#include <aws/common/mutex.h>

#include <aws/io/channel.h>
#include <aws/io/event_loop.h>
#include <aws/io/file_utils.h>
#include <aws/io/logging.h>
#include <aws/io/pkcs11.h>
#include <aws/io/private/pki_utils.h>
#include <aws/io/private/tls_channel_handler_shared.h>
#include <aws/io/statistics.h>

#include "../pkcs11_private.h"

#include <aws/common/encoding.h>
#include <aws/common/string.h>
#include <aws/common/task_scheduler.h>
#include <aws/common/thread.h>

#include <errno.h>
#include <inttypes.h>
#include <math.h>
#include <s2n.h>
#include <stdio.h>
#include <stdlib.h>

#define EST_TLS_RECORD_OVERHEAD 53 /* 5 byte header + 32 + 16 bytes for padding */
#define KB_1 1024
#define MAX_RECORD_SIZE (KB_1 * 16)
#define EST_HANDSHAKE_SIZE (7 * KB_1)

static const char *s_default_ca_dir = NULL;
static const char *s_default_ca_file = NULL;

struct s2n_delayed_shutdown_task {
    struct aws_channel_task task;
    struct aws_channel_slot *slot;
    int error;
};

struct s2n_handler {
    struct aws_channel_handler handler;
    struct aws_tls_channel_handler_shared shared_state;
    struct s2n_connection *connection;
    struct s2n_ctx *s2n_ctx;
    struct aws_channel_slot *slot;
    struct aws_linked_list input_queue;
    struct aws_byte_buf protocol;
    struct aws_byte_buf server_name;
    aws_channel_on_message_write_completed_fn *latest_message_on_completion;
    struct aws_channel_task sequential_tasks;
    void *latest_message_completion_user_data;
    aws_tls_on_negotiation_result_fn *on_negotiation_result;
    aws_tls_on_data_read_fn *on_data_read;
    aws_tls_on_error_fn *on_error;
    void *user_data;
    bool advertise_alpn_message;
    enum {
        NEGOTIATION_ONGOING,
        NEGOTIATION_FAILED,
        NEGOTIATION_SUCCEEDED,
    } state;
    struct s2n_delayed_shutdown_task delayed_shutdown_task;
    struct aws_channel_task async_pkey_task;
};

struct s2n_ctx {
    struct aws_tls_ctx ctx;
    struct s2n_config *s2n_config;

    /* Only used in special circumstances (ex: have cert but no key, because key is in PKCS#11) */
    struct s2n_cert_chain_and_key *custom_cert_chain_and_key;

    /* Use a single PKCS#11 session for all TLS connections on this s2n_ctx.
     * We do this because PKCS#11 tokens may only support a
     * limited number of sessions (PKCS11-UG-v2.40 section 2.6.7).
     * If this one shared session turns out to be a severe bottleneck,
     * we could look into other setups (ex: put session on its own thread,
     * 1 session per event-loop, 1 session per connection, etc).
     *
     * The lock must be held while performing session operations.
     * Otherwise, it would not be safe for multiple threads to share a
     * session (PKCS11-UG-v2.40 section 2.6.7). The lock isn't needed for
     * setup and teardown though, since we ensure nothing parallel is going
     * on at these times */
    struct {
        struct aws_pkcs11_lib *lib;
        struct aws_mutex session_lock;
        CK_SESSION_HANDLE session_handle;
        CK_OBJECT_HANDLE private_key_handle;
        CK_KEY_TYPE private_key_type;
    } pkcs11;
};

AWS_STATIC_STRING_FROM_LITERAL(s_debian_path, "/etc/ssl/certs");
AWS_STATIC_STRING_FROM_LITERAL(s_rhel_path, "/etc/pki/tls/certs");
AWS_STATIC_STRING_FROM_LITERAL(s_android_path, "/system/etc/security/cacerts");
AWS_STATIC_STRING_FROM_LITERAL(s_free_bsd_path, "/usr/local/share/certs");
AWS_STATIC_STRING_FROM_LITERAL(s_net_bsd_path, "/etc/openssl/certs");

static const char *s_determine_default_pki_dir(void) {
    /* debian variants */
    if (aws_path_exists(s_debian_path)) {
        return aws_string_c_str(s_debian_path);
    }

    /* RHEL variants */
    if (aws_path_exists(s_rhel_path)) {
        return aws_string_c_str(s_rhel_path);
    }

    /* android */
    if (aws_path_exists(s_android_path)) {
        return aws_string_c_str(s_android_path);
    }

    /* Free BSD */
    if (aws_path_exists(s_free_bsd_path)) {
        return aws_string_c_str(s_free_bsd_path);
    }

    /* Net BSD */
    if (aws_path_exists(s_net_bsd_path)) {
        return aws_string_c_str(s_net_bsd_path);
    }

    return NULL;
}

AWS_STATIC_STRING_FROM_LITERAL(s_debian_ca_file_path, "/etc/ssl/certs/ca-certificates.crt");
AWS_STATIC_STRING_FROM_LITERAL(s_old_rhel_ca_file_path, "/etc/pki/tls/certs/ca-bundle.crt");
AWS_STATIC_STRING_FROM_LITERAL(s_open_suse_ca_file_path, "/etc/ssl/ca-bundle.pem");
AWS_STATIC_STRING_FROM_LITERAL(s_open_elec_ca_file_path, "/etc/pki/tls/cacert.pem");
AWS_STATIC_STRING_FROM_LITERAL(s_modern_rhel_ca_file_path, "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem");

static const char *s_determine_default_pki_ca_file(void) {
    /* debian variants */
    if (aws_path_exists(s_debian_ca_file_path)) {
        return aws_string_c_str(s_debian_ca_file_path);
    }

    /* Old RHEL variants */
    if (aws_path_exists(s_old_rhel_ca_file_path)) {
        return aws_string_c_str(s_old_rhel_ca_file_path);
    }

    /* Open SUSE */
    if (aws_path_exists(s_open_suse_ca_file_path)) {
        return aws_string_c_str(s_open_suse_ca_file_path);
    }

    /* Open ELEC */
    if (aws_path_exists(s_open_elec_ca_file_path)) {
        return aws_string_c_str(s_open_elec_ca_file_path);
    }

    /* Modern RHEL variants */
    if (aws_path_exists(s_modern_rhel_ca_file_path)) {
        return aws_string_c_str(s_modern_rhel_ca_file_path);
    }

    return NULL;
}

void aws_tls_init_static_state(struct aws_allocator *alloc) {
    (void)alloc;
    AWS_LOGF_INFO(AWS_LS_IO_TLS, "static: Initializing TLS using s2n.");

    setenv("S2N_ENABLE_CLIENT_MODE", "1", 1);
    setenv("S2N_DONT_MLOCK", "1", 1);

    /* Disable atexit behavior, so that s2n_cleanup() fully cleans things up.
     *
     * By default, s2n uses an ataexit handler and doesn't fully clean up until the program exits.
     * This can cause a crash if s2n is compiled into a shared library and
     * that library is unloaded before the appexit handler runs. */
    s2n_disable_atexit();

    if (s2n_init() != S2N_SUCCESS) {
        fprintf(stderr, "s2n_init() failed: %d (%s)\n", s2n_errno, s2n_strerror(s2n_errno, "EN"));
        AWS_FATAL_ASSERT(0 && "s2n_init() failed");
    }

    s_default_ca_dir = s_determine_default_pki_dir();
    s_default_ca_file = s_determine_default_pki_ca_file();
    if (s_default_ca_dir || s_default_ca_file) {
        AWS_LOGF_DEBUG(
            AWS_LS_IO_TLS,
            "ctx: Based on OS, we detected the default PKI path as %s, and ca file as %s",
            s_default_ca_dir,
            s_default_ca_file);
    } else {
        AWS_LOGF_WARN(
            AWS_LS_IO_TLS,
            "Default TLS trust store not found on this system."
            " TLS connections will fail unless trusted CA certificates are installed,"
            " or \"override default trust store\" is used while creating the TLS context.");
    }
}

void aws_tls_clean_up_static_state(void) {
    s2n_cleanup();
}

bool aws_tls_is_alpn_available(void) {
    return true;
}

bool aws_tls_is_cipher_pref_supported(enum aws_tls_cipher_pref cipher_pref) {
    switch (cipher_pref) {
        case AWS_IO_TLS_CIPHER_PREF_SYSTEM_DEFAULT:
            return true;
            /* PQ Crypto no-ops on android for now */
#ifndef ANDROID
        case AWS_IO_TLS_CIPHER_PREF_PQ_TLSv1_0_2021_05:
            return true;
#endif

        default:
            return false;
    }
}

static int s_generic_read(struct s2n_handler *handler, struct aws_byte_buf *buf) {

    size_t written = 0;

    while (!aws_linked_list_empty(&handler->input_queue) && written < buf->len) {
        struct aws_linked_list_node *node = aws_linked_list_pop_front(&handler->input_queue);
        struct aws_io_message *message = AWS_CONTAINER_OF(node, struct aws_io_message, queueing_handle);

        size_t remaining_message_len = message->message_data.len - message->copy_mark;
        size_t remaining_buf_len = buf->len - written;

        size_t to_write = remaining_message_len < remaining_buf_len ? remaining_message_len : remaining_buf_len;

        struct aws_byte_cursor message_cursor = aws_byte_cursor_from_buf(&message->message_data);
        aws_byte_cursor_advance(&message_cursor, message->copy_mark);
        aws_byte_cursor_read(&message_cursor, buf->buffer + written, to_write);

        written += to_write;

        message->copy_mark += to_write;

        if (message->copy_mark == message->message_data.len) {
            aws_mem_release(message->allocator, message);
        } else {
            aws_linked_list_push_front(&handler->input_queue, &message->queueing_handle);
        }
    }

    if (written) {
        return (int)written;
    }

    errno = EAGAIN;
    return -1;
}

static int s_s2n_handler_recv(void *io_context, uint8_t *buf, uint32_t len) {
    struct s2n_handler *handler = (struct s2n_handler *)io_context;

    struct aws_byte_buf read_buffer = aws_byte_buf_from_array(buf, len);
    return s_generic_read(handler, &read_buffer);
}

static int s_generic_send(struct s2n_handler *handler, struct aws_byte_buf *buf) {

    struct aws_byte_cursor buffer_cursor = aws_byte_cursor_from_buf(buf);

    size_t processed = 0;
    while (processed < buf->len) {
        const size_t overhead = aws_channel_slot_upstream_message_overhead(handler->slot);
        const size_t message_size_hint = (buf->len - processed) + overhead;
        struct aws_io_message *message = aws_channel_acquire_message_from_pool(
            handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, message_size_hint);

        if (!message || message->message_data.capacity <= overhead) {
            errno = ENOMEM;
            return -1;
        }

        const size_t available_msg_write_capacity = message->message_data.capacity - overhead;
        const size_t to_write =
            available_msg_write_capacity >= buffer_cursor.len ? buffer_cursor.len : available_msg_write_capacity;

        struct aws_byte_cursor chunk = aws_byte_cursor_advance(&buffer_cursor, to_write);
        if (aws_byte_buf_append(&message->message_data, &chunk)) {
            aws_mem_release(message->allocator, message);
            return -1;
        }
        processed += message->message_data.len;

        if (processed == buf->len) {
            message->on_completion = handler->latest_message_on_completion;
            message->user_data = handler->latest_message_completion_user_data;
            handler->latest_message_on_completion = NULL;
            handler->latest_message_completion_user_data = NULL;
        }

        if (aws_channel_slot_send_message(handler->slot, message, AWS_CHANNEL_DIR_WRITE)) {
            aws_mem_release(message->allocator, message);
            errno = EPIPE;
            return -1;
        }
    }

    if (processed) {
        return (int)processed;
    }

    errno = EAGAIN;
    return -1;
}

static int s_s2n_handler_send(void *io_context, const uint8_t *buf, uint32_t len) {
    struct s2n_handler *handler = (struct s2n_handler *)io_context;
    struct aws_byte_buf send_buf = aws_byte_buf_from_array(buf, len);

    return s_generic_send(handler, &send_buf);
}

static void s_s2n_handler_destroy(struct aws_channel_handler *handler) {
    if (handler) {
        struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;
        aws_tls_channel_handler_shared_clean_up(&s2n_handler->shared_state);
        if (s2n_handler->connection) {
            s2n_connection_free(s2n_handler->connection);
        }
        if (s2n_handler->s2n_ctx) {
            aws_tls_ctx_release(&s2n_handler->s2n_ctx->ctx);
        }
        aws_mem_release(handler->alloc, (void *)s2n_handler);
    }
}

static void s_on_negotiation_result(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    int error_code,
    void *user_data) {

    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;

    aws_on_tls_negotiation_completed(&s2n_handler->shared_state, error_code);

    if (s2n_handler->on_negotiation_result) {
        s2n_handler->on_negotiation_result(handler, slot, error_code, user_data);
    }
}

static int s_drive_negotiation(struct aws_channel_handler *handler) {
    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;

    AWS_ASSERT(s2n_handler->state == NEGOTIATION_ONGOING);

    aws_on_drive_tls_negotiation(&s2n_handler->shared_state);

    s2n_blocked_status blocked = S2N_NOT_BLOCKED;
    do {
        int negotiation_code = s2n_negotiate(s2n_handler->connection, &blocked);

        int s2n_error = s2n_errno;
        if (negotiation_code == S2N_ERR_T_OK) {
            s2n_handler->state = NEGOTIATION_SUCCEEDED;

            const char *protocol = s2n_get_application_protocol(s2n_handler->connection);
            if (protocol) {
                AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "id=%p: Alpn protocol negotiated as %s", (void *)handler, protocol);
                s2n_handler->protocol = aws_byte_buf_from_c_str(protocol);
            }

            const char *server_name = s2n_get_server_name(s2n_handler->connection);

            if (server_name) {
                AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "id=%p: Remote server name is %s", (void *)handler, server_name);
                s2n_handler->server_name = aws_byte_buf_from_c_str(server_name);
            }

            if (s2n_handler->slot->adj_right && s2n_handler->advertise_alpn_message && protocol) {
                struct aws_io_message *message = aws_channel_acquire_message_from_pool(
                    s2n_handler->slot->channel,
                    AWS_IO_MESSAGE_APPLICATION_DATA,
                    sizeof(struct aws_tls_negotiated_protocol_message));
                message->message_tag = AWS_TLS_NEGOTIATED_PROTOCOL_MESSAGE;
                struct aws_tls_negotiated_protocol_message *protocol_message =
                    (struct aws_tls_negotiated_protocol_message *)message->message_data.buffer;

                protocol_message->protocol = s2n_handler->protocol;
                message->message_data.len = sizeof(struct aws_tls_negotiated_protocol_message);
                if (aws_channel_slot_send_message(s2n_handler->slot, message, AWS_CHANNEL_DIR_READ)) {
                    aws_mem_release(message->allocator, message);
                    aws_channel_shutdown(s2n_handler->slot->channel, aws_last_error());
                    return AWS_OP_SUCCESS;
                }
            }

            s_on_negotiation_result(handler, s2n_handler->slot, AWS_OP_SUCCESS, s2n_handler->user_data);

            break;
        }
        if (s2n_error_get_type(s2n_error) != S2N_ERR_T_BLOCKED) {
            AWS_LOGF_WARN(
                AWS_LS_IO_TLS,
                "id=%p: negotiation failed with error %s (%s)",
                (void *)handler,
                s2n_strerror(s2n_error, "EN"),
                s2n_strerror_debug(s2n_error, "EN"));

            if (s2n_error_get_type(s2n_error) == S2N_ERR_T_ALERT) {
                AWS_LOGF_DEBUG(
                    AWS_LS_IO_TLS,
                    "id=%p: Alert code %d",
                    (void *)handler,
                    s2n_connection_get_alert(s2n_handler->connection));
            }

            const char *err_str = s2n_strerror_debug(s2n_error, NULL);
            (void)err_str;
            s2n_handler->state = NEGOTIATION_FAILED;

            aws_raise_error(AWS_IO_TLS_ERROR_NEGOTIATION_FAILURE);

            s_on_negotiation_result(
                handler, s2n_handler->slot, AWS_IO_TLS_ERROR_NEGOTIATION_FAILURE, s2n_handler->user_data);

            return AWS_OP_ERR;
        }
    } while (blocked == S2N_NOT_BLOCKED);

    return AWS_OP_SUCCESS;
}

static void s_negotiation_task(struct aws_channel_task *task, void *arg, aws_task_status status) {
    task->task_fn = NULL;
    task->arg = NULL;

    if (status == AWS_TASK_STATUS_RUN_READY) {
        struct aws_channel_handler *handler = arg;
        struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;
        if (s2n_handler->state == NEGOTIATION_ONGOING) {
            s_drive_negotiation(handler);
        }
    }
}

int aws_tls_client_handler_start_negotiation(struct aws_channel_handler *handler) {
    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;

    AWS_LOGF_TRACE(AWS_LS_IO_TLS, "id=%p: Kicking off TLS negotiation.", (void *)handler)
    if (aws_channel_thread_is_callers_thread(s2n_handler->slot->channel)) {
        if (s2n_handler->state == NEGOTIATION_ONGOING) {
            s_drive_negotiation(handler);
        }
        return AWS_OP_SUCCESS;
    }

    aws_channel_task_init(
        &s2n_handler->sequential_tasks, s_negotiation_task, handler, "s2n_channel_handler_negotiation");
    aws_channel_schedule_task_now(s2n_handler->slot->channel, &s2n_handler->sequential_tasks);

    return AWS_OP_SUCCESS;
}

static int s_s2n_handler_process_read_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {

    struct s2n_handler *s2n_handler = handler->impl;

    if (AWS_UNLIKELY(s2n_handler->state == NEGOTIATION_FAILED)) {
        return aws_raise_error(AWS_IO_TLS_ERROR_NEGOTIATION_FAILURE);
    }

    if (message) {
        aws_linked_list_push_back(&s2n_handler->input_queue, &message->queueing_handle);

        if (s2n_handler->state == NEGOTIATION_ONGOING) {
            size_t message_len = message->message_data.len;
            if (!s_drive_negotiation(handler)) {
                aws_channel_slot_increment_read_window(slot, message_len);
            } else {
                aws_channel_shutdown(s2n_handler->slot->channel, AWS_IO_TLS_ERROR_NEGOTIATION_FAILURE);
            }
            return AWS_OP_SUCCESS;
        }
    }

    s2n_blocked_status blocked = S2N_NOT_BLOCKED;
    size_t downstream_window = SIZE_MAX;
    if (slot->adj_right) {
        downstream_window = aws_channel_slot_downstream_read_window(slot);
    }

    size_t processed = 0;
    AWS_LOGF_TRACE(
        AWS_LS_IO_TLS, "id=%p: Downstream window %llu", (void *)handler, (unsigned long long)downstream_window);

    while (processed < downstream_window && blocked == S2N_NOT_BLOCKED) {

        struct aws_io_message *outgoing_read_message = aws_channel_acquire_message_from_pool(
            slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, downstream_window - processed);
        if (!outgoing_read_message) {
            return AWS_OP_ERR;
        }

        ssize_t read = s2n_recv(
            s2n_handler->connection,
            outgoing_read_message->message_data.buffer,
            outgoing_read_message->message_data.capacity,
            &blocked);

        AWS_LOGF_TRACE(AWS_LS_IO_TLS, "id=%p: Bytes read %lld", (void *)handler, (long long)read);

        /* weird race where we received an alert from the peer, but s2n doesn't tell us about it.....
         * if this happens, it's a graceful shutdown, so kick it off here.
         *
         * In other words, s2n, upon graceful shutdown, follows the unix EOF idiom. So just shutdown with
         * SUCCESS.
         */
        if (read == 0) {
            AWS_LOGF_DEBUG(
                AWS_LS_IO_TLS,
                "id=%p: Alert code %d",
                (void *)handler,
                s2n_connection_get_alert(s2n_handler->connection));
            aws_mem_release(outgoing_read_message->allocator, outgoing_read_message);
            aws_channel_shutdown(slot->channel, AWS_OP_SUCCESS);
            return AWS_OP_SUCCESS;
        }

        if (read < 0) {
            aws_mem_release(outgoing_read_message->allocator, outgoing_read_message);
            continue;
        };

        processed += read;
        outgoing_read_message->message_data.len = (size_t)read;

        if (s2n_handler->on_data_read) {
            s2n_handler->on_data_read(handler, slot, &outgoing_read_message->message_data, s2n_handler->user_data);
        }

        if (slot->adj_right) {
            aws_channel_slot_send_message(slot, outgoing_read_message, AWS_CHANNEL_DIR_READ);
        } else {
            aws_mem_release(outgoing_read_message->allocator, outgoing_read_message);
        }
    }

    AWS_LOGF_TRACE(
        AWS_LS_IO_TLS,
        "id=%p: Remaining window for this event-loop tick: %llu",
        (void *)handler,
        (unsigned long long)downstream_window - processed);

    return AWS_OP_SUCCESS;
}

static int s_s2n_handler_process_write_message(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    struct aws_io_message *message) {
    (void)slot;
    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;

    if (AWS_UNLIKELY(s2n_handler->state != NEGOTIATION_SUCCEEDED)) {
        return aws_raise_error(AWS_IO_TLS_ERROR_NOT_NEGOTIATED);
    }

    s2n_handler->latest_message_on_completion = message->on_completion;
    s2n_handler->latest_message_completion_user_data = message->user_data;

    s2n_blocked_status blocked;
    ssize_t write_code =
        s2n_send(s2n_handler->connection, message->message_data.buffer, (ssize_t)message->message_data.len, &blocked);

    AWS_LOGF_TRACE(AWS_LS_IO_TLS, "id=%p: Bytes written: %llu", (void *)handler, (unsigned long long)write_code);

    ssize_t message_len = (ssize_t)message->message_data.len;

    if (write_code < message_len) {
        return aws_raise_error(AWS_IO_TLS_ERROR_WRITE_FAILURE);
    }

    aws_mem_release(message->allocator, message);

    return AWS_OP_SUCCESS;
}

static void s_delayed_shutdown_task_fn(struct aws_channel_task *channel_task, void *arg, enum aws_task_status status) {
    (void)channel_task;

    struct aws_channel_handler *handler = arg;
    struct s2n_handler *s2n_handler = handler->impl;

    if (status == AWS_TASK_STATUS_RUN_READY) {
        AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "id=%p: Delayed shut down in write direction", (void *)handler)
        s2n_blocked_status blocked;
        /* make a best effort, but the channel is going away after this run, so.... you only get one shot anyways */
        s2n_shutdown(s2n_handler->connection, &blocked);
    }
    aws_channel_slot_on_handler_shutdown_complete(
        s2n_handler->delayed_shutdown_task.slot,
        AWS_CHANNEL_DIR_WRITE,
        s2n_handler->delayed_shutdown_task.error,
        false);
}

static enum aws_tls_signature_algorithm s_s2n_to_aws_signature_algorithm(s2n_tls_signature_algorithm s2n_alg) {
    switch (s2n_alg) {
        case S2N_TLS_SIGNATURE_RSA:
            return AWS_TLS_SIGNATURE_RSA;
        case S2N_TLS_SIGNATURE_ECDSA:
            return AWS_TLS_SIGNATURE_ECDSA;
        default:
            return AWS_TLS_SIGNATURE_UNKNOWN;
    }
}

static enum aws_tls_hash_algorithm s_s2n_to_aws_hash_algorithm(s2n_tls_hash_algorithm s2n_alg) {
    switch (s2n_alg) {
        case (S2N_TLS_HASH_SHA1):
            return AWS_TLS_HASH_SHA1;
        case (S2N_TLS_HASH_SHA224):
            return AWS_TLS_HASH_SHA224;
        case (S2N_TLS_HASH_SHA256):
            return AWS_TLS_HASH_SHA256;
        case (S2N_TLS_HASH_SHA384):
            return AWS_TLS_HASH_SHA384;
        case (S2N_TLS_HASH_SHA512):
            return AWS_TLS_HASH_SHA512;
        default:
            return AWS_TLS_HASH_UNKNOWN;
    }
}

/* This task performs the PKCS#11 private key operations.
 * This task is scheduled because the s2n async private key operation is not allowed to complete synchronously */
static void s_s2n_pkcs11_async_pkey_task(
    struct aws_channel_task *channel_task,
    void *arg,
    enum aws_task_status status) {

    struct s2n_handler *s2n_handler = AWS_CONTAINER_OF(channel_task, struct s2n_handler, async_pkey_task);
    struct aws_channel_handler *handler = &s2n_handler->handler;
    struct s2n_async_pkey_op *op = arg;
    bool success = false;

    uint8_t *input_data = NULL;     /* allocated later */
    struct aws_byte_buf output_buf; /* initialized later */
    AWS_ZERO_STRUCT(output_buf);

    /* if things started failing since this task was scheduled, just clean up and bail out */
    if (status != AWS_TASK_STATUS_RUN_READY || s2n_handler->state != NEGOTIATION_ONGOING) {
        goto clean_up;
    }

    AWS_LOGF_TRACE(AWS_LS_IO_TLS, "id=%p: Running PKCS#11 async pkey task", (void *)handler);

    /* We check all s2n_async_pkey_op functions for success,
     * but they shouldn't fail if they're called correctly.
     * Even if the output is bad, the failure will happen later in s2n_negotiate() */

    uint32_t input_size = 0;
    if (s2n_async_pkey_op_get_input_size(op, &input_size)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "id=%p: Failed querying s2n async pkey op size", (void *)handler);
        aws_raise_error(AWS_ERROR_INVALID_STATE);
        goto error;
    }

    input_data = aws_mem_acquire(handler->alloc, input_size);
    if (s2n_async_pkey_op_get_input(op, input_data, input_size)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "id=%p: Failed querying s2n async pkey input", (void *)handler);
        aws_raise_error(AWS_ERROR_INVALID_STATE);
        goto error;
    }
    struct aws_byte_cursor input_cursor = aws_byte_cursor_from_array(input_data, input_size);

    s2n_async_pkey_op_type op_type = 0;
    if (s2n_async_pkey_op_get_op_type(op, &op_type)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "id=%p: Failed querying s2n async pkey op type", (void *)handler);
        aws_raise_error(AWS_ERROR_INVALID_STATE);
        goto error;
    }

    /* Gather additional information if this is a SIGN operation */
    enum aws_tls_signature_algorithm aws_sign_alg = 0;
    enum aws_tls_hash_algorithm aws_digest_alg = 0;
    if (op_type == S2N_ASYNC_SIGN) {
        s2n_tls_signature_algorithm s2n_sign_alg = 0;
        if (s2n_connection_get_selected_client_cert_signature_algorithm(s2n_handler->connection, &s2n_sign_alg)) {
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "id=%p: Failed getting s2n client cert signature algorithm", (void *)handler);
            aws_raise_error(AWS_ERROR_INVALID_STATE);
            goto error;
        }

        aws_sign_alg = s_s2n_to_aws_signature_algorithm(s2n_sign_alg);
        if (aws_sign_alg == AWS_TLS_SIGNATURE_UNKNOWN) {
            AWS_LOGF_ERROR(
                AWS_LS_IO_TLS,
                "id=%p: Cannot sign with s2n_tls_signature_algorithm=%d. Algorithm currently unsupported",
                (void *)handler,
                s2n_sign_alg);
            aws_raise_error(AWS_IO_TLS_SIGNATURE_ALGORITHM_UNSUPPORTED);
            goto error;
        }

        s2n_tls_hash_algorithm s2n_digest_alg = 0;
        if (s2n_connection_get_selected_client_cert_digest_algorithm(s2n_handler->connection, &s2n_digest_alg)) {
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "id=%p: Failed getting s2n client cert digest algorithm", (void *)handler);
            aws_raise_error(AWS_ERROR_INVALID_STATE);
            goto error;
        }

        aws_digest_alg = s_s2n_to_aws_hash_algorithm(s2n_digest_alg);
        if (aws_digest_alg == AWS_TLS_HASH_UNKNOWN) {
            AWS_LOGF_ERROR(
                AWS_LS_IO_TLS,
                "id=%p: Cannot sign digest created with s2n_tls_hash_algorithm=%d. Algorithm currently unsupported",
                (void *)handler,
                s2n_digest_alg);
            aws_raise_error(AWS_IO_TLS_DIGEST_ALGORITHM_UNSUPPORTED);
            goto error;
        }
    }

    /*********** BEGIN CRITICAL SECTION ***********/
    aws_mutex_lock(&s2n_handler->s2n_ctx->pkcs11.session_lock);
    bool success_while_locked = false;

    switch (op_type) {
        case S2N_ASYNC_DECRYPT:
            if (aws_pkcs11_lib_decrypt(
                    s2n_handler->s2n_ctx->pkcs11.lib,
                    s2n_handler->s2n_ctx->pkcs11.session_handle,
                    s2n_handler->s2n_ctx->pkcs11.private_key_handle,
                    s2n_handler->s2n_ctx->pkcs11.private_key_type,
                    input_cursor,
                    handler->alloc,
                    &output_buf)) {

                AWS_LOGF_ERROR(
                    AWS_LS_IO_TLS,
                    "id=%p: PKCS#11 decrypt failed, error %s",
                    (void *)handler,
                    aws_error_name(aws_last_error()));
                goto unlock;
            }
            break;

        case S2N_ASYNC_SIGN:
            if (aws_pkcs11_lib_sign(
                    s2n_handler->s2n_ctx->pkcs11.lib,
                    s2n_handler->s2n_ctx->pkcs11.session_handle,
                    s2n_handler->s2n_ctx->pkcs11.private_key_handle,
                    s2n_handler->s2n_ctx->pkcs11.private_key_type,
                    input_cursor,
                    handler->alloc,
                    aws_digest_alg,
                    aws_sign_alg,
                    &output_buf)) {

                AWS_LOGF_ERROR(
                    AWS_LS_IO_TLS,
                    "id=%p: PKCS#11 sign failed, error %s",
                    (void *)handler,
                    aws_error_name(aws_last_error()));
                goto unlock;
            }
            break;

        default:
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "id=%p: Unknown s2n_async_pkey_op_type:%d", (void *)handler, (int)op_type);
            aws_raise_error(AWS_ERROR_INVALID_STATE);
            goto unlock;
    }

    success_while_locked = true;
unlock:
    aws_mutex_unlock(&s2n_handler->s2n_ctx->pkcs11.session_lock);
    /*********** END CRITICAL SECTION ***********/

    if (!success_while_locked) {
        goto error;
    }

    AWS_LOGF_TRACE(
        AWS_LS_IO_TLS, "id=%p: PKCS#11 operation complete. output-size:%zu", (void *)handler, output_buf.len);

    if (s2n_async_pkey_op_set_output(op, output_buf.buffer, output_buf.len)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "id=%p: Failed setting output on s2n async pkey op", (void *)handler);
        aws_raise_error(AWS_ERROR_INVALID_STATE);
        goto error;
    }

    if (s2n_async_pkey_op_apply(op, s2n_handler->connection)) {
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "id=%p: Failed applying s2n async pkey op", (void *)handler);
        aws_raise_error(AWS_ERROR_INVALID_STATE);
        goto error;
    }

    /* Success! */
    success = true;
    goto clean_up;

error:
    aws_channel_shutdown(s2n_handler->slot->channel, aws_last_error());

clean_up:
    s2n_async_pkey_op_free(op);
    aws_mem_release(handler->alloc, input_data);
    aws_byte_buf_clean_up(&output_buf);

    if (success) {
        s_drive_negotiation(handler);
    }
}

static int s_s2n_pkcs11_async_pkey_callback(struct s2n_connection *conn, struct s2n_async_pkey_op *op) {
    struct s2n_handler *s2n_handler = s2n_connection_get_ctx(conn);
    struct aws_channel_handler *handler = &s2n_handler->handler;

    AWS_ASSERT(conn == s2n_handler->connection);
    (void)conn;

    /* Schedule a task to do the work.
     * s2n can't deal with the async private key operation completing synchronously, so we can't just do it now */
    AWS_LOGF_TRACE(AWS_LS_IO_TLS, "id=%p: async pkey callback received, scheduling PKCS#11 task", (void *)handler);

    aws_channel_task_init(&s2n_handler->async_pkey_task, s_s2n_pkcs11_async_pkey_task, op, "s2n_pkcs11_async_pkey_op");
    aws_channel_schedule_task_now(s2n_handler->slot->channel, &s2n_handler->async_pkey_task);

    return S2N_SUCCESS;
}

static int s_s2n_do_delayed_shutdown(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    int error_code) {
    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;

    s2n_handler->delayed_shutdown_task.slot = slot;
    s2n_handler->delayed_shutdown_task.error = error_code;

    uint64_t shutdown_delay = s2n_connection_get_delay(s2n_handler->connection);
    uint64_t now = 0;

    if (aws_channel_current_clock_time(slot->channel, &now)) {
        return AWS_OP_ERR;
    }

    uint64_t shutdown_time = aws_add_u64_saturating(shutdown_delay, now);
    aws_channel_schedule_task_future(slot->channel, &s2n_handler->delayed_shutdown_task.task, shutdown_time);

    return AWS_OP_SUCCESS;
}

static int s_s2n_handler_shutdown(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    enum aws_channel_direction dir,
    int error_code,
    bool abort_immediately) {
    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;

    if (dir == AWS_CHANNEL_DIR_WRITE) {
        if (!abort_immediately && error_code != AWS_IO_SOCKET_CLOSED) {
            AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "id=%p: Scheduling delayed write direction shutdown", (void *)handler)
            if (s_s2n_do_delayed_shutdown(handler, slot, error_code) == AWS_OP_SUCCESS) {
                return AWS_OP_SUCCESS;
            }
        }
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_IO_TLS, "id=%p: Shutting down read direction with error code %d", (void *)handler, error_code);

        /* If negotiation hasn't succeeded yet, it's certainly not going to succeed now */
        if (s2n_handler->state == NEGOTIATION_ONGOING) {
            s2n_handler->state = NEGOTIATION_FAILED;
        }

        while (!aws_linked_list_empty(&s2n_handler->input_queue)) {
            struct aws_linked_list_node *node = aws_linked_list_pop_front(&s2n_handler->input_queue);
            struct aws_io_message *message = AWS_CONTAINER_OF(node, struct aws_io_message, queueing_handle);
            aws_mem_release(message->allocator, message);
        }
    }

    return aws_channel_slot_on_handler_shutdown_complete(slot, dir, error_code, abort_immediately);
}

static void s_run_read(struct aws_channel_task *task, void *arg, aws_task_status status) {
    task->task_fn = NULL;
    task->arg = NULL;

    if (status == AWS_TASK_STATUS_RUN_READY) {
        struct aws_channel_handler *handler = (struct aws_channel_handler *)arg;
        struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;
        s_s2n_handler_process_read_message(handler, s2n_handler->slot, NULL);
    }
}

static int s_s2n_handler_increment_read_window(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    size_t size) {
    (void)size;
    struct s2n_handler *s2n_handler = handler->impl;

    size_t downstream_size = aws_channel_slot_downstream_read_window(slot);
    size_t current_window_size = slot->window_size;

    AWS_LOGF_TRACE(
        AWS_LS_IO_TLS, "id=%p: Increment read window message received %llu", (void *)handler, (unsigned long long)size);

    size_t likely_records_count = (size_t)ceil((double)(downstream_size) / (double)(MAX_RECORD_SIZE));
    size_t offset_size = aws_mul_size_saturating(likely_records_count, EST_TLS_RECORD_OVERHEAD);
    size_t total_desired_size = aws_add_size_saturating(offset_size, downstream_size);

    if (total_desired_size > current_window_size) {
        size_t window_update_size = total_desired_size - current_window_size;
        AWS_LOGF_TRACE(
            AWS_LS_IO_TLS,
            "id=%p: Propagating read window increment of size %llu",
            (void *)handler,
            (unsigned long long)window_update_size);
        aws_channel_slot_increment_read_window(slot, window_update_size);
    }

    if (s2n_handler->state == NEGOTIATION_SUCCEEDED && !s2n_handler->sequential_tasks.node.next) {
        /* TLS requires full records before it can decrypt anything. As a result we need to check everything we've
         * buffered instead of just waiting on a read from the socket, or we'll hit a deadlock.
         *
         * We have messages in a queue and they need to be run after the socket has popped (even if it didn't have data
         * to read). Alternatively, s2n reads entire records at a time, so we'll need to grab whatever we can and we
         * have no idea what's going on inside there. So we need to attempt another read.*/
        aws_channel_task_init(
            &s2n_handler->sequential_tasks, s_run_read, handler, "s2n_channel_handler_read_on_window_increment");
        aws_channel_schedule_task_now(slot->channel, &s2n_handler->sequential_tasks);
    }

    return AWS_OP_SUCCESS;
}

static size_t s_s2n_handler_message_overhead(struct aws_channel_handler *handler) {
    (void)handler;
    return EST_TLS_RECORD_OVERHEAD;
}

static size_t s_s2n_handler_initial_window_size(struct aws_channel_handler *handler) {
    (void)handler;

    return EST_HANDSHAKE_SIZE;
}

static void s_s2n_handler_reset_statistics(struct aws_channel_handler *handler) {
    struct s2n_handler *s2n_handler = handler->impl;

    aws_crt_statistics_tls_reset(&s2n_handler->shared_state.stats);
}

static void s_s2n_handler_gather_statistics(struct aws_channel_handler *handler, struct aws_array_list *stats) {
    struct s2n_handler *s2n_handler = handler->impl;

    void *stats_base = &s2n_handler->shared_state.stats;
    aws_array_list_push_back(stats, &stats_base);
}

struct aws_byte_buf aws_tls_handler_protocol(struct aws_channel_handler *handler) {
    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;
    return s2n_handler->protocol;
}

struct aws_byte_buf aws_tls_handler_server_name(struct aws_channel_handler *handler) {
    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;
    return s2n_handler->server_name;
}

static struct aws_channel_handler_vtable s_handler_vtable = {
    .destroy = s_s2n_handler_destroy,
    .process_read_message = s_s2n_handler_process_read_message,
    .process_write_message = s_s2n_handler_process_write_message,
    .shutdown = s_s2n_handler_shutdown,
    .increment_read_window = s_s2n_handler_increment_read_window,
    .initial_window_size = s_s2n_handler_initial_window_size,
    .message_overhead = s_s2n_handler_message_overhead,
    .reset_statistics = s_s2n_handler_reset_statistics,
    .gather_statistics = s_s2n_handler_gather_statistics,
};

static int s_parse_protocol_preferences(
    struct aws_string *alpn_list_str,
    const char protocol_output[4][128],
    size_t *protocol_count) {
    size_t max_count = *protocol_count;
    *protocol_count = 0;

    struct aws_byte_cursor alpn_list_buffer[4];
    AWS_ZERO_ARRAY(alpn_list_buffer);
    struct aws_array_list alpn_list;
    struct aws_byte_cursor user_alpn_str = aws_byte_cursor_from_string(alpn_list_str);

    aws_array_list_init_static(&alpn_list, alpn_list_buffer, 4, sizeof(struct aws_byte_cursor));

    if (aws_byte_cursor_split_on_char(&user_alpn_str, ';', &alpn_list)) {
        aws_raise_error(AWS_IO_TLS_CTX_ERROR);
        return AWS_OP_ERR;
    }

    size_t protocols_list_len = aws_array_list_length(&alpn_list);
    if (protocols_list_len < 1) {
        aws_raise_error(AWS_IO_TLS_CTX_ERROR);
        return AWS_OP_ERR;
    }

    for (size_t i = 0; i < protocols_list_len && i < max_count; ++i) {
        struct aws_byte_cursor cursor;
        AWS_ZERO_STRUCT(cursor);
        if (aws_array_list_get_at(&alpn_list, (void *)&cursor, (size_t)i)) {
            aws_raise_error(AWS_IO_TLS_CTX_ERROR);
            return AWS_OP_ERR;
        }
        AWS_FATAL_ASSERT(cursor.ptr && cursor.len > 0);
        memcpy((void *)protocol_output[i], cursor.ptr, cursor.len);
        *protocol_count += 1;
    }

    return AWS_OP_SUCCESS;
}

static size_t s_tl_cleanup_key = 0; /* Address of variable serves as key in hash table */

/*
 * This local object is added to the table of every event loop that has a (s2n) tls connection
 * added to it at some point in time
 */
static struct aws_event_loop_local_object s_tl_cleanup_object = {
    .key = &s_tl_cleanup_key,
    .object = NULL,
    .on_object_removed = NULL,
};

static void s_aws_cleanup_s2n_thread_local_state(void *user_data) {
    (void)user_data;

    s2n_cleanup();
}

/* s2n allocates thread-local data structures. We need to clean these up when the event loop's thread exits. */
static int s_s2n_tls_channel_handler_schedule_thread_local_cleanup(struct aws_channel_slot *slot) {
    struct aws_channel *channel = slot->channel;

    struct aws_event_loop_local_object existing_marker;
    AWS_ZERO_STRUCT(existing_marker);

    /*
     * Check whether another s2n_tls_channel_handler has already scheduled the cleanup task.
     */
    if (aws_channel_fetch_local_object(channel, &s_tl_cleanup_key, &existing_marker)) {
        /* Doesn't exist in event loop table: add it and add the at-exit cleanup callback */
        if (aws_channel_put_local_object(channel, &s_tl_cleanup_key, &s_tl_cleanup_object)) {
            return AWS_OP_ERR;
        }

        aws_thread_current_at_exit(s_aws_cleanup_s2n_thread_local_state, NULL);
    }

    return AWS_OP_SUCCESS;
}

static struct aws_channel_handler *s_new_tls_handler(
    struct aws_allocator *allocator,
    struct aws_tls_connection_options *options,
    struct aws_channel_slot *slot,
    s2n_mode mode) {

    AWS_ASSERT(options->ctx);
    struct s2n_handler *s2n_handler = aws_mem_calloc(allocator, 1, sizeof(struct s2n_handler));
    s2n_handler->handler.impl = s2n_handler;
    s2n_handler->handler.alloc = allocator;
    s2n_handler->handler.vtable = &s_handler_vtable;
    s2n_handler->handler.slot = slot;

    aws_tls_ctx_acquire(options->ctx);
    s2n_handler->s2n_ctx = options->ctx->impl;

    s2n_handler->connection = s2n_connection_new(mode);

    if (!s2n_handler->connection) {
        goto cleanup_conn;
    }

    aws_tls_channel_handler_shared_init(&s2n_handler->shared_state, &s2n_handler->handler, options);

    s2n_handler->user_data = options->user_data;
    s2n_handler->on_data_read = options->on_data_read;
    s2n_handler->on_error = options->on_error;
    s2n_handler->on_negotiation_result = options->on_negotiation_result;
    s2n_handler->advertise_alpn_message = options->advertise_alpn_message;

    s2n_handler->latest_message_completion_user_data = NULL;
    s2n_handler->latest_message_on_completion = NULL;
    s2n_handler->slot = slot;
    aws_linked_list_init(&s2n_handler->input_queue);

    s2n_handler->protocol = aws_byte_buf_from_array(NULL, 0);

    if (options->server_name) {

        if (s2n_set_server_name(s2n_handler->connection, aws_string_c_str(options->server_name))) {
            aws_raise_error(AWS_IO_TLS_CTX_ERROR);
            goto cleanup_conn;
        }
    }

    s2n_handler->state = NEGOTIATION_ONGOING;

    s2n_connection_set_recv_cb(s2n_handler->connection, s_s2n_handler_recv);
    s2n_connection_set_recv_ctx(s2n_handler->connection, s2n_handler);
    s2n_connection_set_send_cb(s2n_handler->connection, s_s2n_handler_send);
    s2n_connection_set_send_ctx(s2n_handler->connection, s2n_handler);
    s2n_connection_set_ctx(s2n_handler->connection, s2n_handler);
    s2n_connection_set_blinding(s2n_handler->connection, S2N_SELF_SERVICE_BLINDING);

    if (options->alpn_list) {
        AWS_LOGF_DEBUG(
            AWS_LS_IO_TLS,
            "id=%p: Setting ALPN list %s",
            (void *)&s2n_handler->handler,
            aws_string_c_str(options->alpn_list));

        const char protocols_cpy[4][128];
        AWS_ZERO_ARRAY(protocols_cpy);
        size_t protocols_size = 4;
        if (s_parse_protocol_preferences(options->alpn_list, protocols_cpy, &protocols_size)) {
            aws_raise_error(AWS_IO_TLS_CTX_ERROR);
            goto cleanup_conn;
        }

        const char *protocols[4];
        AWS_ZERO_ARRAY(protocols);
        for (size_t i = 0; i < protocols_size; ++i) {
            protocols[i] = protocols_cpy[i];
        }

        if (s2n_connection_set_protocol_preferences(
                s2n_handler->connection, (const char *const *)protocols, (int)protocols_size)) {
            aws_raise_error(AWS_IO_TLS_CTX_ERROR);
            goto cleanup_conn;
        }
    }

    if (s2n_connection_set_config(s2n_handler->connection, s2n_handler->s2n_ctx->s2n_config)) {
        AWS_LOGF_WARN(
            AWS_LS_IO_TLS,
            "id=%p: configuration error %s (%s)",
            (void *)&s2n_handler->handler,
            s2n_strerror(s2n_errno, "EN"),
            s2n_strerror_debug(s2n_errno, "EN"));
        aws_raise_error(AWS_IO_TLS_CTX_ERROR);
        goto cleanup_conn;
    }

    aws_channel_task_init(
        &s2n_handler->delayed_shutdown_task.task,
        s_delayed_shutdown_task_fn,
        &s2n_handler->handler,
        "s2n_delayed_shutdown");

    if (s_s2n_tls_channel_handler_schedule_thread_local_cleanup(slot)) {
        goto cleanup_conn;
    }

    return &s2n_handler->handler;

cleanup_conn:
    s_s2n_handler_destroy(&s2n_handler->handler);

    return NULL;
}

struct aws_channel_handler *aws_tls_client_handler_new(
    struct aws_allocator *allocator,
    struct aws_tls_connection_options *options,
    struct aws_channel_slot *slot) {

    return s_new_tls_handler(allocator, options, slot, S2N_CLIENT);
}

struct aws_channel_handler *aws_tls_server_handler_new(
    struct aws_allocator *allocator,
    struct aws_tls_connection_options *options,
    struct aws_channel_slot *slot) {

    return s_new_tls_handler(allocator, options, slot, S2N_SERVER);
}

static void s_s2n_ctx_destroy(struct s2n_ctx *s2n_ctx) {
    if (s2n_ctx != NULL) {
        if (s2n_ctx->pkcs11.session_handle != 0) {
            aws_pkcs11_lib_close_session(s2n_ctx->pkcs11.lib, s2n_ctx->pkcs11.session_handle);
        }
        aws_mutex_clean_up(&s2n_ctx->pkcs11.session_lock);
        aws_pkcs11_lib_release(s2n_ctx->pkcs11.lib);
        s2n_config_free(s2n_ctx->s2n_config);

        if (s2n_ctx->custom_cert_chain_and_key) {
            s2n_cert_chain_and_key_free(s2n_ctx->custom_cert_chain_and_key);
        }

        aws_mem_release(s2n_ctx->ctx.alloc, s2n_ctx);
    }
}

static int s2n_wall_clock_time_nanoseconds(void *context, uint64_t *time_in_ns) {
    (void)context;
    if (aws_sys_clock_get_ticks(time_in_ns)) {
        *time_in_ns = 0;
        return -1;
    }

    return 0;
}

static int s2n_monotonic_clock_time_nanoseconds(void *context, uint64_t *time_in_ns) {
    (void)context;
    if (aws_high_res_clock_get_ticks(time_in_ns)) {
        *time_in_ns = 0;
        return -1;
    }

    return 0;
}

static int s_tls_ctx_pkcs11_setup(struct s2n_ctx *s2n_ctx, const struct aws_tls_ctx_options *options) {
    /* PKCS#11 options were already sanitized (ie: check for required args) in tls_channel_handler.c */

    /* anything initialized in this function is cleaned up during s_s2n_ctx_destroy()
     * so don't worry about cleaning up unless it's some tmp heap allocation */

    s2n_ctx->pkcs11.lib = aws_pkcs11_lib_acquire(options->pkcs11.lib); /* cannot fail */
    aws_mutex_init(&s2n_ctx->pkcs11.session_lock);

    CK_SLOT_ID slot_id = 0;
    if (aws_pkcs11_lib_find_slot_with_token(
            s2n_ctx->pkcs11.lib,
            options->pkcs11.has_slot_id ? &options->pkcs11.slot_id : NULL,
            options->pkcs11.token_label,
            &slot_id /*out*/)) {
        return AWS_OP_ERR;
    }

    if (aws_pkcs11_lib_open_session(s2n_ctx->pkcs11.lib, slot_id, &s2n_ctx->pkcs11.session_handle)) {
        return AWS_OP_ERR;
    }

    if (aws_pkcs11_lib_login_user(s2n_ctx->pkcs11.lib, s2n_ctx->pkcs11.session_handle, options->pkcs11.user_pin)) {
        return AWS_OP_ERR;
    }

    if (aws_pkcs11_lib_find_private_key(
            s2n_ctx->pkcs11.lib,
            s2n_ctx->pkcs11.session_handle,
            options->pkcs11.private_key_object_label,
            &s2n_ctx->pkcs11.private_key_handle /*out*/,
            &s2n_ctx->pkcs11.private_key_type /*out*/)) {
        return AWS_OP_ERR;
    }

    return AWS_OP_SUCCESS;
}

static void s_log_and_raise_s2n_errno(const char *msg) {
    AWS_LOGF_ERROR(
        AWS_LS_IO_TLS, "%s: %s (%s)", msg, s2n_strerror(s2n_errno, "EN"), s2n_strerror_debug(s2n_errno, "EN"));
    aws_raise_error(AWS_IO_TLS_CTX_ERROR);
}

static struct aws_tls_ctx *s_tls_ctx_new(
    struct aws_allocator *alloc,
    const struct aws_tls_ctx_options *options,
    s2n_mode mode) {
    struct s2n_ctx *s2n_ctx = aws_mem_calloc(alloc, 1, sizeof(struct s2n_ctx));

    if (!s2n_ctx) {
        return NULL;
    }

    if (!aws_tls_is_cipher_pref_supported(options->cipher_pref)) {
        aws_raise_error(AWS_IO_TLS_CIPHER_PREF_UNSUPPORTED);
        AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: TLS Cipher Preference is not supported: %d.", options->cipher_pref);
        return NULL;
    }

    s2n_ctx->ctx.alloc = alloc;
    s2n_ctx->ctx.impl = s2n_ctx;
    aws_ref_count_init(&s2n_ctx->ctx.ref_count, s2n_ctx, (aws_simple_completion_callback *)s_s2n_ctx_destroy);

    s2n_ctx->s2n_config = s2n_config_new();
    if (!s2n_ctx->s2n_config) {
        s_log_and_raise_s2n_errno("ctx: creation failed");
        goto cleanup_s2n_config;
    }

    int set_clock_result = s2n_config_set_wall_clock(s2n_ctx->s2n_config, s2n_wall_clock_time_nanoseconds, NULL);
    if (set_clock_result != S2N_ERR_T_OK) {
        s_log_and_raise_s2n_errno("ctx: failed to set wall clock");
        goto cleanup_s2n_config;
    }

    set_clock_result = s2n_config_set_monotonic_clock(s2n_ctx->s2n_config, s2n_monotonic_clock_time_nanoseconds, NULL);
    if (set_clock_result != S2N_ERR_T_OK) {
        s_log_and_raise_s2n_errno("ctx: failed to set monotonic clock");
        goto cleanup_s2n_config;
    }

    if (options->pkcs11.lib != NULL) {
        /* PKCS#11 integration hasn't been tested with TLS 1.3, so don't use cipher preferences that allow 1.3 */
        switch (options->minimum_tls_version) {
            case AWS_IO_SSLv3:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "CloudFront-SSL-v-3");
                break;
            case AWS_IO_TLSv1:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "CloudFront-TLS-1-0-2014");
                break;
            case AWS_IO_TLSv1_1:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "ELBSecurityPolicy-TLS-1-1-2017-01");
                break;
            case AWS_IO_TLSv1_2:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "ELBSecurityPolicy-TLS-1-2-Ext-2018-06");
                break;
            case AWS_IO_TLSv1_3:
                AWS_LOGF_ERROR(AWS_LS_IO_TLS, "TLS 1.3 with PKCS#11 is not supported yet.");
                aws_raise_error(AWS_IO_TLS_VERSION_UNSUPPORTED);
                goto cleanup_s2n_config;
            case AWS_IO_TLS_VER_SYS_DEFAULTS:
            default:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "ELBSecurityPolicy-TLS-1-1-2017-01");
        }
    } else {
        switch (options->minimum_tls_version) {
            case AWS_IO_SSLv3:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "AWS-CRT-SDK-SSLv3.0");
                break;
            case AWS_IO_TLSv1:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "AWS-CRT-SDK-TLSv1.0");
                break;
            case AWS_IO_TLSv1_1:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "AWS-CRT-SDK-TLSv1.1");
                break;
            case AWS_IO_TLSv1_2:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "AWS-CRT-SDK-TLSv1.2");
                break;
            case AWS_IO_TLSv1_3:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "AWS-CRT-SDK-TLSv1.3");
                break;
            case AWS_IO_TLS_VER_SYS_DEFAULTS:
            default:
                s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "AWS-CRT-SDK-TLSv1.0");
        }
    }

    switch (options->cipher_pref) {
        case AWS_IO_TLS_CIPHER_PREF_SYSTEM_DEFAULT:
            /* No-Op, if the user configured a minimum_tls_version then a version-specific Cipher Preference was set */
            break;
        case AWS_IO_TLS_CIPHER_PREF_PQ_TLSv1_0_2021_05:
            s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "PQ-TLS-1-0-2021-05-26");
            break;
        default:
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "Unrecognized TLS Cipher Preference: %d", options->cipher_pref);
            aws_raise_error(AWS_IO_TLS_CIPHER_PREF_UNSUPPORTED);
            goto cleanup_s2n_config;
    }

    if (aws_tls_options_buf_is_set(&options->certificate) && aws_tls_options_buf_is_set(&options->private_key)) {
        AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "ctx: Certificate and key have been set, setting them up now.");

        if (!aws_text_is_utf8(options->certificate.buffer, options->certificate.len)) {
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: failed to import certificate, must be ASCII/UTF-8 encoded");
            aws_raise_error(AWS_IO_FILE_VALIDATION_FAILURE);
            goto cleanup_s2n_config;
        }

        if (!aws_text_is_utf8(options->private_key.buffer, options->private_key.len)) {
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: failed to import private key, must be ASCII/UTF-8 encoded");
            aws_raise_error(AWS_IO_FILE_VALIDATION_FAILURE);
            goto cleanup_s2n_config;
        }

        /* Ensure that what we pass to s2n is zero-terminated */
        struct aws_string *certificate_string = aws_string_new_from_buf(alloc, &options->certificate);
        struct aws_string *private_key_string = aws_string_new_from_buf(alloc, &options->private_key);

        int err_code = s2n_config_add_cert_chain_and_key(
            s2n_ctx->s2n_config, (const char *)certificate_string->bytes, (const char *)private_key_string->bytes);

        aws_string_destroy(certificate_string);
        aws_string_destroy_secure(private_key_string);

        if (mode == S2N_CLIENT) {
            s2n_config_set_client_auth_type(s2n_ctx->s2n_config, S2N_CERT_AUTH_REQUIRED);
        }

        if (err_code != S2N_ERR_T_OK) {
            s_log_and_raise_s2n_errno("ctx: Failed to add certificate and private key");
            goto cleanup_s2n_config;
        }
    } else if (options->pkcs11.lib != NULL) {
        AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "ctx: PKCS#11 has been set, setting it up now.");
        if (s_tls_ctx_pkcs11_setup(s2n_ctx, options)) {
            goto cleanup_s2n_config;
        }

        /* set callback so that we can do private key operations through PKCS#11 */
        if (s2n_config_set_async_pkey_callback(s2n_ctx->s2n_config, s_s2n_pkcs11_async_pkey_callback)) {
            s_log_and_raise_s2n_errno("ctx: failed to set private key callback");
            goto cleanup_s2n_config;
        }

        /* set certificate.
         * we need to create a custom s2n_cert_chain_and_key that knows the cert but not the key */
        s2n_ctx->custom_cert_chain_and_key = s2n_cert_chain_and_key_new();
        if (!s2n_ctx->custom_cert_chain_and_key) {
            s_log_and_raise_s2n_errno("ctx: creation failed");
            goto cleanup_s2n_config;
        }

        if (s2n_cert_chain_and_key_load_public_pem_bytes(
                s2n_ctx->custom_cert_chain_and_key, options->certificate.buffer, options->certificate.len)) {
            s_log_and_raise_s2n_errno("ctx: failed to load certificate");
            goto cleanup_s2n_config;
        }

        if (s2n_config_add_cert_chain_and_key_to_store(s2n_ctx->s2n_config, s2n_ctx->custom_cert_chain_and_key)) {
            s_log_and_raise_s2n_errno("ctx: failed to add certificate to store");
            goto cleanup_s2n_config;
        }

        if (mode == S2N_CLIENT) {
            s2n_config_set_client_auth_type(s2n_ctx->s2n_config, S2N_CERT_AUTH_REQUIRED);
        }
    }

    if (options->verify_peer) {
        if (s2n_config_set_check_stapled_ocsp_response(s2n_ctx->s2n_config, 1) == S2N_SUCCESS) {
            if (s2n_config_set_status_request_type(s2n_ctx->s2n_config, S2N_STATUS_REQUEST_OCSP) != S2N_SUCCESS) {
                s_log_and_raise_s2n_errno("ctx: ocsp status request cannot be set");
                goto cleanup_s2n_config;
            }
        } else {
            if (s2n_error_get_type(s2n_errno) == S2N_ERR_T_USAGE) {
                AWS_LOGF_INFO(AWS_LS_IO_TLS, "ctx: cannot enable ocsp stapling: %s", s2n_strerror(s2n_errno, "EN"));
            } else {
                s_log_and_raise_s2n_errno("ctx: cannot enable ocsp stapling");
                goto cleanup_s2n_config;
            }
        }

        if (options->ca_path || aws_tls_options_buf_is_set(&options->ca_file)) {
            /* The user called an override_default_trust_store() function.
             * Begin by wiping anything that s2n loaded by default */
            if (s2n_config_wipe_trust_store(s2n_ctx->s2n_config)) {
                s_log_and_raise_s2n_errno("ctx: failed to wipe default trust store");
                goto cleanup_s2n_config;
            }

            if (options->ca_path) {
                if (s2n_config_set_verification_ca_location(
                        s2n_ctx->s2n_config, NULL, aws_string_c_str(options->ca_path))) {
                    s_log_and_raise_s2n_errno("ctx: configuration error");
                    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "Failed to set ca_path %s\n", aws_string_c_str(options->ca_path));
                    goto cleanup_s2n_config;
                }
            }

            if (aws_tls_options_buf_is_set(&options->ca_file)) {
                /* Ensure that what we pass to s2n is zero-terminated */
                struct aws_string *ca_file_string = aws_string_new_from_buf(alloc, &options->ca_file);
                int set_ca_result =
                    s2n_config_add_pem_to_trust_store(s2n_ctx->s2n_config, (const char *)ca_file_string->bytes);
                aws_string_destroy(ca_file_string);

                if (set_ca_result) {
                    s_log_and_raise_s2n_errno("ctx: configuration error");
                    AWS_LOGF_ERROR(AWS_LS_IO_TLS, "Failed to set ca_file %s\n", (const char *)options->ca_file.buffer);
                    goto cleanup_s2n_config;
                }
            }
        } else if (s_default_ca_file || s_default_ca_dir) {
            /* User wants to use the system's default trust store.
             *
             * Note that s2n's trust store always starts with libcrypto's default locations.
             * These paths are configured when libcrypto is built (--openssldir),
             * but might not be right for the current machine (e.g. if libcrypto
             * is statically linked into an application that is distributed
             * to multiple flavors of Linux). Therefore, load the locations that
             * were found at library startup. */
            if (s2n_config_set_verification_ca_location(s2n_ctx->s2n_config, s_default_ca_file, s_default_ca_dir)) {
                s_log_and_raise_s2n_errno("ctx: configuration error");
                AWS_LOGF_ERROR(
                    AWS_LS_IO_TLS, "Failed to set ca_path: %s and ca_file %s\n", s_default_ca_dir, s_default_ca_file);
                goto cleanup_s2n_config;
            }
        } else {
            /* Cannot find system's trust store */
            aws_raise_error(AWS_IO_TLS_ERROR_DEFAULT_TRUST_STORE_NOT_FOUND);
            AWS_LOGF_ERROR(
                AWS_LS_IO_TLS,
                "Default TLS trust store not found on this system."
                " Install CA certificates, or \"override default trust store\".");
            goto cleanup_s2n_config;
        }

        if (mode == S2N_SERVER && s2n_config_set_client_auth_type(s2n_ctx->s2n_config, S2N_CERT_AUTH_REQUIRED)) {
            s_log_and_raise_s2n_errno("ctx: failed to set client auth type");
            goto cleanup_s2n_config;
        }
    } else if (mode != S2N_SERVER) {
        AWS_LOGF_WARN(
            AWS_LS_IO_TLS,
            "ctx: X.509 validation has been disabled. "
            "If this is not running in a test environment, this is likely a security vulnerability.");
        if (s2n_config_disable_x509_verification(s2n_ctx->s2n_config)) {
            s_log_and_raise_s2n_errno("ctx: failed to disable x509 verification");
            goto cleanup_s2n_config;
        }
    }

    if (options->alpn_list) {
        AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "ctx: Setting ALPN list %s", aws_string_c_str(options->alpn_list));
        const char protocols_cpy[4][128];
        AWS_ZERO_ARRAY(protocols_cpy);
        size_t protocols_size = 4;
        if (s_parse_protocol_preferences(options->alpn_list, protocols_cpy, &protocols_size)) {
            s_log_and_raise_s2n_errno("ctx: Failed to parse ALPN list");
            goto cleanup_s2n_config;
        }

        const char *protocols[4];
        AWS_ZERO_ARRAY(protocols);
        for (size_t i = 0; i < protocols_size; ++i) {
            protocols[i] = protocols_cpy[i];
        }

        if (s2n_config_set_protocol_preferences(s2n_ctx->s2n_config, protocols, (int)protocols_size)) {
            s_log_and_raise_s2n_errno("ctx: Failed to set protocol preferences");
            goto cleanup_s2n_config;
        }
    }

    if (options->max_fragment_size == 512) {
        s2n_config_send_max_fragment_length(s2n_ctx->s2n_config, S2N_TLS_MAX_FRAG_LEN_512);
    } else if (options->max_fragment_size == 1024) {
        s2n_config_send_max_fragment_length(s2n_ctx->s2n_config, S2N_TLS_MAX_FRAG_LEN_1024);
    } else if (options->max_fragment_size == 2048) {
        s2n_config_send_max_fragment_length(s2n_ctx->s2n_config, S2N_TLS_MAX_FRAG_LEN_2048);
    } else if (options->max_fragment_size == 4096) {
        s2n_config_send_max_fragment_length(s2n_ctx->s2n_config, S2N_TLS_MAX_FRAG_LEN_4096);
    }

    return &s2n_ctx->ctx;

cleanup_s2n_config:
    s_s2n_ctx_destroy(s2n_ctx);

    return NULL;
}

struct aws_tls_ctx *aws_tls_server_ctx_new(struct aws_allocator *alloc, const struct aws_tls_ctx_options *options) {
    aws_io_fatal_assert_library_initialized();
    return s_tls_ctx_new(alloc, options, S2N_SERVER);
}

struct aws_tls_ctx *aws_tls_client_ctx_new(struct aws_allocator *alloc, const struct aws_tls_ctx_options *options) {
    aws_io_fatal_assert_library_initialized();
    return s_tls_ctx_new(alloc, options, S2N_CLIENT);
}
