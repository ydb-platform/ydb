/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/io/tls_channel_handler.h>

#include <aws/io/channel.h>
#include <aws/io/event_loop.h>
#include <aws/io/file_utils.h>
#include <aws/io/logging.h>
#include <aws/io/pki_utils.h>
#include <aws/io/private/tls_channel_handler_shared.h>
#include <aws/io/statistics.h>

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

struct s2n_handler {
    struct aws_channel_handler handler;
    struct aws_tls_channel_handler_shared shared_state;
    struct s2n_connection *connection;
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
    bool negotiation_finished;
};

struct s2n_ctx {
    struct aws_tls_ctx ctx;
    struct s2n_config *s2n_config;
};

static const char *s_determine_default_pki_dir(void) {
    /* debian variants */
    if (aws_path_exists("/etc/ssl/certs")) {
        return "/etc/ssl/certs";
    }

    /* RHEL variants */
    if (aws_path_exists("/etc/pki/tls/certs")) {
        return "/etc/pki/tls/certs";
    }

    /* android */
    if (aws_path_exists("/system/etc/security/cacerts")) {
        return "/system/etc/security/cacerts";
    }

    /* Free BSD */
    if (aws_path_exists("/usr/local/share/certs")) {
        return "/usr/local/share/certs";
    }

    /* Net BSD */
    if (aws_path_exists("/etc/openssl/certs")) {
        return "/etc/openssl/certs";
    }

    return NULL;
}

static const char *s_determine_default_pki_ca_file(void) {
    /* debian variants */
    if (aws_path_exists("/etc/ssl/certs/ca-certificates.crt")) {
        return "/etc/ssl/certs/ca-certificates.crt";
    }

    /* Old RHEL variants */
    if (aws_path_exists("/etc/pki/tls/certs/ca-bundle.crt")) {
        return "/etc/pki/tls/certs/ca-bundle.crt";
    }

    /* Open SUSE */
    if (aws_path_exists("/etc/ssl/ca-bundle.pem")) {
        return "/etc/ssl/ca-bundle.pem";
    }

    /* Open ELEC */
    if (aws_path_exists("/etc/pki/tls/cacert.pem")) {
        return "/etc/pki/tls/cacert.pem";
    }

    /* Modern RHEL variants */
    if (aws_path_exists("/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem")) {
        return "/etc/pki/ca-trust/extracted/pem/tls-ca-bundle.pem";
    }

    return NULL;
}

void aws_tls_init_static_state(struct aws_allocator *alloc) {
    (void)alloc;
    AWS_LOGF_INFO(AWS_LS_IO_TLS, "static: Initializing TLS using s2n.");

    setenv("S2N_ENABLE_CLIENT_MODE", "1", 1);
    setenv("S2N_DONT_MLOCK", "1", 1);
    s2n_init();

    s_default_ca_dir = s_determine_default_pki_dir();
    s_default_ca_file = s_determine_default_pki_ca_file();
    AWS_LOGF_DEBUG(
        AWS_LS_IO_TLS,
        "ctx: Based on OS, we detected the default PKI path as %s, and ca file as %s",
        s_default_ca_dir,
        s_default_ca_file);
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
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_TLSv1_0_2019_06:
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_SIKE_TLSv1_0_2019_11:
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_TLSv1_0_2020_02:
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_SIKE_TLSv1_0_2020_02:
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_TLSv1_0_2020_07:
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
        struct aws_io_message *message = aws_channel_acquire_message_from_pool(
            handler->slot->channel, AWS_IO_MESSAGE_APPLICATION_DATA, buf->len - processed);

        if (!message) {
            errno = ENOMEM;
            return -1;
        }

        const size_t overhead = aws_channel_slot_upstream_message_overhead(handler->slot);
        const size_t available_msg_write_capacity = buffer_cursor.len - overhead;

        const size_t to_write = message->message_data.capacity > available_msg_write_capacity
                                    ? available_msg_write_capacity
                                    : message->message_data.capacity;

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
        s2n_connection_free(s2n_handler->connection);
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

    aws_on_drive_tls_negotiation(&s2n_handler->shared_state);

    s2n_blocked_status blocked = S2N_NOT_BLOCKED;
    do {
        int negotiation_code = s2n_negotiate(s2n_handler->connection, &blocked);

        int s2n_error = s2n_errno;
        if (negotiation_code == S2N_ERR_T_OK) {
            s2n_handler->negotiation_finished = true;

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
            s2n_handler->negotiation_finished = false;

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
        s_drive_negotiation(handler);
    }
}

int aws_tls_client_handler_start_negotiation(struct aws_channel_handler *handler) {
    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;

    AWS_LOGF_TRACE(AWS_LS_IO_TLS, "id=%p: Kicking off TLS negotiation.", (void *)handler)
    if (aws_channel_thread_is_callers_thread(s2n_handler->slot->channel)) {
        return s_drive_negotiation(handler);
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

    if (message) {
        aws_linked_list_push_back(&s2n_handler->input_queue, &message->queueing_handle);

        if (!s2n_handler->negotiation_finished) {
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

    if (AWS_UNLIKELY(!s2n_handler->negotiation_finished)) {
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

static int s_s2n_handler_shutdown(
    struct aws_channel_handler *handler,
    struct aws_channel_slot *slot,
    enum aws_channel_direction dir,
    int error_code,
    bool abort_immediately) {
    struct s2n_handler *s2n_handler = (struct s2n_handler *)handler->impl;

    if (dir == AWS_CHANNEL_DIR_WRITE) {
        if (!abort_immediately && error_code != AWS_IO_SOCKET_CLOSED) {
            AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "id=%p: Shutting down write direction", (void *)handler)
            s2n_blocked_status blocked;
            /* make a best effort, but the channel is going away after this run, so.... you only get one shot anyways */
            s2n_shutdown(s2n_handler->connection, &blocked);
        }
    } else {
        AWS_LOGF_DEBUG(
            AWS_LS_IO_TLS, "id=%p: Shutting down read direction with error code %d", (void *)handler, error_code);

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

    if (s2n_handler->negotiation_finished && !s2n_handler->sequential_tasks.node.next) {
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
static struct aws_event_loop_local_object s_tl_cleanup_object = {.key = &s_tl_cleanup_key,
                                                                 .object = NULL,
                                                                 .on_object_removed = NULL};

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
    if (!s2n_handler) {
        return NULL;
    }

    struct s2n_ctx *s2n_ctx = (struct s2n_ctx *)options->ctx->impl;
    s2n_handler->connection = s2n_connection_new(mode);

    if (!s2n_handler->connection) {
        goto cleanup_s2n_handler;
    }

    aws_tls_channel_handler_shared_init(&s2n_handler->shared_state, &s2n_handler->handler, options);

    s2n_handler->handler.impl = s2n_handler;
    s2n_handler->handler.alloc = allocator;
    s2n_handler->handler.vtable = &s_handler_vtable;
    s2n_handler->handler.slot = slot;
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

    s2n_handler->negotiation_finished = false;

    s2n_connection_set_recv_cb(s2n_handler->connection, s_s2n_handler_recv);
    s2n_connection_set_recv_ctx(s2n_handler->connection, s2n_handler);
    s2n_connection_set_send_cb(s2n_handler->connection, s_s2n_handler_send);
    s2n_connection_set_send_ctx(s2n_handler->connection, s2n_handler);
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

    if (s2n_connection_set_config(s2n_handler->connection, s2n_ctx->s2n_config)) {
        AWS_LOGF_WARN(
            AWS_LS_IO_TLS,
            "id=%p: configuration error %s (%s)",
            (void *)&s2n_handler->handler,
            s2n_strerror(s2n_errno, "EN"),
            s2n_strerror_debug(s2n_errno, "EN"));
        aws_raise_error(AWS_IO_TLS_CTX_ERROR);
        goto cleanup_conn;
    }

    if (s_s2n_tls_channel_handler_schedule_thread_local_cleanup(slot)) {
        goto cleanup_conn;
    }

    return &s2n_handler->handler;

cleanup_conn:
    s2n_connection_free(s2n_handler->connection);

cleanup_s2n_handler:
    aws_mem_release(allocator, s2n_handler);

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
        s2n_config_free(s2n_ctx->s2n_config);
        aws_mem_release(s2n_ctx->ctx.alloc, s2n_ctx);
    }
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
        goto cleanup_s2n_ctx;
    }

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
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "TLS 1.3 is not supported yet.");
            /* sorry guys, we'll add this as soon as s2n does. */
            aws_raise_error(AWS_IO_TLS_VERSION_UNSUPPORTED);
            goto cleanup_s2n_ctx;
        case AWS_IO_TLS_VER_SYS_DEFAULTS:
        default:
            s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "ELBSecurityPolicy-TLS-1-1-2017-01");
    }

    switch (options->cipher_pref) {
        case AWS_IO_TLS_CIPHER_PREF_SYSTEM_DEFAULT:
            /* No-Op, if the user configured a minimum_tls_version then a version-specific Cipher Preference was set */
            break;
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_TLSv1_0_2019_06:
            s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "KMS-PQ-TLS-1-0-2019-06");
            break;
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_SIKE_TLSv1_0_2019_11:
            s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "PQ-SIKE-TEST-TLS-1-0-2019-11");
            break;
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_TLSv1_0_2020_02:
            s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "KMS-PQ-TLS-1-0-2020-02");
            break;
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_SIKE_TLSv1_0_2020_02:
            s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "PQ-SIKE-TEST-TLS-1-0-2020-02");
            break;
        case AWS_IO_TLS_CIPHER_PREF_KMS_PQ_TLSv1_0_2020_07:
            s2n_config_set_cipher_preferences(s2n_ctx->s2n_config, "KMS-PQ-TLS-1-0-2020-07");
            break;
        default:
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "Unrecognized TLS Cipher Preference: %d", options->cipher_pref);
            aws_raise_error(AWS_IO_TLS_CIPHER_PREF_UNSUPPORTED);
            goto cleanup_s2n_ctx;
    }

    if (options->certificate.len && options->private_key.len) {
        AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "ctx: Certificate and key have been set, setting them up now.");

        if (!aws_text_is_utf8(options->certificate.buffer, options->certificate.len)) {
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: failed to import certificate, must be ASCII/UTF-8 encoded");
            aws_raise_error(AWS_IO_FILE_VALIDATION_FAILURE);
            goto cleanup_s2n_ctx;
        }

        if (!aws_text_is_utf8(options->private_key.buffer, options->private_key.len)) {
            AWS_LOGF_ERROR(AWS_LS_IO_TLS, "static: failed to import private key, must be ASCII/UTF-8 encoded");
            aws_raise_error(AWS_IO_FILE_VALIDATION_FAILURE);
            goto cleanup_s2n_ctx;
        }

        int err_code = s2n_config_add_cert_chain_and_key(
            s2n_ctx->s2n_config, (const char *)options->certificate.buffer, (const char *)options->private_key.buffer);

        if (mode == S2N_CLIENT) {
            s2n_config_set_client_auth_type(s2n_ctx->s2n_config, S2N_CERT_AUTH_REQUIRED);
        }

        if (err_code != S2N_ERR_T_OK) {
            AWS_LOGF_ERROR(
                AWS_LS_IO_TLS,
                "ctx: configuration error %s (%s)",
                s2n_strerror(s2n_errno, "EN"),
                s2n_strerror_debug(s2n_errno, "EN"));
            aws_raise_error(AWS_IO_TLS_CTX_ERROR);
            goto cleanup_s2n_config;
        }
    }

    if (options->verify_peer) {
        if (s2n_config_set_check_stapled_ocsp_response(s2n_ctx->s2n_config, 1) == S2N_SUCCESS) {
            if (s2n_config_set_status_request_type(s2n_ctx->s2n_config, S2N_STATUS_REQUEST_OCSP) != S2N_SUCCESS) {
                AWS_LOGF_ERROR(
                    AWS_LS_IO_TLS,
                    "ctx: ocsp status request cannot be set: %s (%s)",
                    s2n_strerror(s2n_errno, "EN"),
                    s2n_strerror_debug(s2n_errno, "EN"));
                aws_raise_error(AWS_IO_TLS_CTX_ERROR);
                goto cleanup_s2n_config;
            }
        } else {
            if (s2n_error_get_type(s2n_errno) == S2N_ERR_T_USAGE) {
                AWS_LOGF_INFO(AWS_LS_IO_TLS, "ctx: cannot enable ocsp stapling: %s", s2n_strerror(s2n_errno, "EN"));
            } else {
                AWS_LOGF_ERROR(
                    AWS_LS_IO_TLS,
                    "ctx: cannot enable ocsp stapling: %s (%s)",
                    s2n_strerror(s2n_errno, "EN"),
                    s2n_strerror_debug(s2n_errno, "EN"));
                aws_raise_error(AWS_IO_TLS_CTX_ERROR);
                goto cleanup_s2n_config;
            }
        }

        if (options->ca_path) {
            if (s2n_config_set_verification_ca_location(
                    s2n_ctx->s2n_config, NULL, aws_string_c_str(options->ca_path))) {
                AWS_LOGF_ERROR(
                    AWS_LS_IO_TLS,
                    "ctx: configuration error %s (%s)",
                    s2n_strerror(s2n_errno, "EN"),
                    s2n_strerror_debug(s2n_errno, "EN"));
                AWS_LOGF_ERROR(AWS_LS_IO_TLS, "Failed to set ca_path %s\n", aws_string_c_str(options->ca_path));
                aws_raise_error(AWS_IO_TLS_CTX_ERROR);
                goto cleanup_s2n_config;
            }
        }

        if (options->ca_file.len) {
            if (s2n_config_add_pem_to_trust_store(s2n_ctx->s2n_config, (const char *)options->ca_file.buffer)) {
                AWS_LOGF_ERROR(
                    AWS_LS_IO_TLS,
                    "ctx: configuration error %s (%s)",
                    s2n_strerror(s2n_errno, "EN"),
                    s2n_strerror_debug(s2n_errno, "EN"));
                AWS_LOGF_ERROR(AWS_LS_IO_TLS, "Failed to set ca_file %s\n", (const char *)options->ca_file.buffer);
                aws_raise_error(AWS_IO_TLS_CTX_ERROR);
                goto cleanup_s2n_config;
            }
        }

        if (!options->ca_path && !options->ca_file.len) {
            if (s2n_config_set_verification_ca_location(s2n_ctx->s2n_config, s_default_ca_file, s_default_ca_dir)) {
                AWS_LOGF_ERROR(
                    AWS_LS_IO_TLS,
                    "ctx: configuration error %s (%s)",
                    s2n_strerror(s2n_errno, "EN"),
                    s2n_strerror_debug(s2n_errno, "EN"));
                AWS_LOGF_ERROR(
                    AWS_LS_IO_TLS, "Failed to set ca_path: %s and ca_file %s\n", s_default_ca_dir, s_default_ca_file);
                aws_raise_error(AWS_IO_TLS_CTX_ERROR);
                goto cleanup_s2n_config;
            }
        }

        if (mode == S2N_SERVER && s2n_config_set_client_auth_type(s2n_ctx->s2n_config, S2N_CERT_AUTH_REQUIRED)) {
            AWS_LOGF_ERROR(
                AWS_LS_IO_TLS,
                "ctx: configuration error %s (%s)",
                s2n_strerror(s2n_errno, "EN"),
                s2n_strerror_debug(s2n_errno, "EN"));
            aws_raise_error(AWS_IO_TLS_CTX_ERROR);
            goto cleanup_s2n_config;
        }
    } else if (mode != S2N_SERVER) {
        AWS_LOGF_WARN(
            AWS_LS_IO_TLS,
            "ctx: X.509 validation has been disabled. "
            "If this is not running in a test environment, this is likely a security vulnerability.");
        if (s2n_config_disable_x509_verification(s2n_ctx->s2n_config)) {
            aws_raise_error(AWS_IO_TLS_CTX_ERROR);
            goto cleanup_s2n_config;
        }
    }

    if (options->alpn_list) {
        AWS_LOGF_DEBUG(AWS_LS_IO_TLS, "ctx: Setting ALPN list %s", aws_string_c_str(options->alpn_list));
        const char protocols_cpy[4][128];
        AWS_ZERO_ARRAY(protocols_cpy);
        size_t protocols_size = 4;
        if (s_parse_protocol_preferences(options->alpn_list, protocols_cpy, &protocols_size)) {
            aws_raise_error(AWS_IO_TLS_CTX_ERROR);
            goto cleanup_s2n_config;
        }

        const char *protocols[4];
        AWS_ZERO_ARRAY(protocols);
        for (size_t i = 0; i < protocols_size; ++i) {
            protocols[i] = protocols_cpy[i];
        }

        if (s2n_config_set_protocol_preferences(s2n_ctx->s2n_config, protocols, (int)protocols_size)) {
            aws_raise_error(AWS_IO_TLS_CTX_ERROR);
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
    s2n_config_free(s2n_ctx->s2n_config);

cleanup_s2n_ctx:
    aws_mem_release(alloc, s2n_ctx);

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
