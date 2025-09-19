/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/event-stream/event_stream_rpc_client.h>

#include <aws/common/atomics.h>
#include <aws/common/hash_table.h>
#include <aws/common/mutex.h>
#include <aws/event-stream/event_stream_channel_handler.h>
#include <aws/event-stream/private/event_stream_rpc_priv.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>

#include <inttypes.h>

#ifdef _MSC_VER
/* allow declared initializer using address of automatic variable */
#    pragma warning(disable : 4221)
/* allow non-constant aggregate initializers */
#    pragma warning(disable : 4204)

#endif

static void s_clear_continuation_table(struct aws_event_stream_rpc_client_connection *connection);

struct aws_event_stream_rpc_client_connection {
    /* const-at-creation data */
    struct aws_allocator *allocator;
    struct aws_client_bootstrap *bootstrap_ref;
    struct aws_event_loop *event_loop;
    aws_event_stream_rpc_client_on_connection_setup_fn *on_connection_setup;
    aws_event_stream_rpc_client_connection_protocol_message_fn *on_connection_protocol_message;
    aws_event_stream_rpc_client_on_connection_shutdown_fn *on_connection_shutdown;
    aws_event_stream_rpc_client_on_connection_terminated_fn *on_connection_terminated;
    void *user_data;

    /* independent */
    struct aws_atomic_var ref_count;

    /* private to event loop thread.  unsafe to access otherwise. */
    struct aws_channel *channel;
    struct aws_channel_handler *event_stream_handler;
    bool bootstrap_owned;

    /* lock and synchronized data */
    struct aws_mutex lock;
    struct {
        struct aws_hash_table continuation_table;
        uint32_t latest_stream_id;
        bool is_open;
        enum aws_event_stream_connection_handshake_state handshake_state;
    } synced_data;
};

struct aws_event_stream_rpc_client_continuation_token {
    uint32_t stream_id;
    struct aws_event_stream_rpc_client_connection *connection;
    aws_event_stream_rpc_client_stream_continuation_fn *continuation_fn;
    aws_event_stream_rpc_client_stream_continuation_closed_fn *closed_fn;
    aws_event_stream_rpc_client_stream_continuation_terminated_fn *terminated_fn;
    void *user_data;
    struct aws_atomic_var ref_count;
    struct aws_atomic_var is_closed;
    struct aws_atomic_var is_complete;
};

static void s_on_message_received(struct aws_event_stream_message *message, int error_code, void *user_data);

static int s_create_connection_on_channel(
    struct aws_event_stream_rpc_client_connection *connection,
    struct aws_channel *channel) {

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(connection->event_loop));

    struct aws_channel_handler *event_stream_handler = NULL;
    struct aws_channel_slot *slot = NULL;

    struct aws_event_stream_channel_handler_options handler_options = {
        .on_message_received = s_on_message_received,
        .user_data = connection,
        .initial_window_size = 0,
        .manual_window_management = false,
    };

    AWS_LOGF_TRACE(
        AWS_LS_EVENT_STREAM_RPC_CLIENT,
        "id=%p: creating an event-stream handler on channel %p",
        (void *)connection,
        (void *)channel);
    event_stream_handler = aws_event_stream_channel_handler_new(connection->allocator, &handler_options);

    if (!event_stream_handler) {
        AWS_LOGF_ERROR(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: creating an event-stream handler failed with error %s",
            (void *)connection,
            aws_error_debug_str(aws_last_error()));
        goto error;
    }

    slot = aws_channel_slot_new(channel);

    if (!slot) {
        AWS_LOGF_ERROR(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: creating channel slot failed with error %s",
            (void *)connection,
            aws_error_debug_str(aws_last_error()));
        goto error;
    }

    aws_channel_slot_insert_end(channel, slot);
    if (aws_channel_slot_set_handler(slot, event_stream_handler)) {
        AWS_LOGF_ERROR(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: setting handler on channel slot failed with error %s",
            (void *)connection,
            aws_error_debug_str(aws_last_error()));
        goto error;
    }

    connection->event_stream_handler = event_stream_handler;
    connection->channel = channel;
    aws_channel_acquire_hold(channel);

    return AWS_OP_SUCCESS;

error:
    if (!slot && event_stream_handler) {
        aws_channel_handler_destroy(event_stream_handler);
    }

    return AWS_OP_ERR;
}

static void s_on_channel_setup_fn(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {
    (void)bootstrap;

    struct aws_event_stream_rpc_client_connection *connection = user_data;
    AWS_LOGF_DEBUG(
        AWS_LS_EVENT_STREAM_RPC_CLIENT,
        "id=%p: on_channel_setup_fn invoked with error_code %d with channel %p",
        (void *)connection,
        error_code,
        (void *)channel);

    if (!error_code) {
        AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(connection->event_loop));
        connection->bootstrap_owned = true;

        if (s_create_connection_on_channel(connection, channel)) {
            int last_error = aws_last_error();
            connection->on_connection_setup(NULL, last_error, connection->user_data);

            /* we have to shut down the successfully-created channel, so we don't want to release the connection yet */
            aws_channel_shutdown(channel, last_error);
            return;
        }

        AWS_LOGF_DEBUG(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: successful event-stream channel setup %p",
            (void *)connection,
            (void *)channel);

        /* keep the connection alive past this function */
        aws_event_stream_rpc_client_connection_acquire(connection);
        connection->on_connection_setup(connection, AWS_OP_SUCCESS, connection->user_data);
    } else {
        connection->on_connection_setup(NULL, error_code, connection->user_data);
    }

    /*
     * 2 -> 1 on a successful connection, 1 -> 0 on a failed one
     *
     * Note that the 1 ref is the channel's hold on the connection.  The user is required to acquire the connection
     * in the setup callback to avoid early destruction.
     *
     * A corollary of this is that if you release the connection without calling close first, it will stay alive and
     * leak.
     */
    aws_event_stream_rpc_client_connection_release(connection);
}

static void s_on_channel_shutdown_fn(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {
    (void)bootstrap;

    struct aws_event_stream_rpc_client_connection *connection = user_data;
    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(connection->event_loop));

    AWS_LOGF_DEBUG(
        AWS_LS_EVENT_STREAM_RPC_CLIENT,
        "id=%p: on_channel_shutdown_fn invoked with error_code %d with channel %p",
        (void *)connection,
        error_code,
        (void *)channel);

    aws_mutex_lock(&connection->lock);
    connection->synced_data.is_open = false;
    aws_mutex_unlock(&connection->lock);

    bool is_externally_visible_shutdown = connection->bootstrap_owned;
    connection->channel = NULL;
    connection->event_stream_handler = NULL;

    if (is_externally_visible_shutdown) {
        s_clear_continuation_table(connection);

        aws_event_stream_rpc_client_connection_acquire(connection);
        connection->on_connection_shutdown(connection, error_code, connection->user_data);
        aws_event_stream_rpc_client_connection_release(connection);
    }

    aws_channel_release_hold(channel);
    aws_event_stream_rpc_client_connection_release(connection);
}

/* Set each continuation's is_closed=true. */
static int s_mark_each_continuation_closed(void *context, struct aws_hash_element *p_element) {
    (void)context;
    struct aws_event_stream_rpc_client_continuation_token *continuation = p_element->value;

    aws_atomic_store_int(&continuation->is_closed, 1U);

    return AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
}

/* Invoke continuation's on_closed() callback.
 * A lock must NOT be hold while calling this */
static void s_complete_continuation(struct aws_event_stream_rpc_client_continuation_token *token) {
    size_t expect_not_complete = 0U;
    if (aws_atomic_compare_exchange_int(&token->is_complete, &expect_not_complete, 1U)) {

        AWS_LOGF_DEBUG(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "token=%p: completing continuation with stream-id %" PRIu32,
            (void *)token,
            token->stream_id);

        if (token->stream_id) {
            token->closed_fn(token, token->user_data);
        }

        aws_event_stream_rpc_client_continuation_release(token);
    }
}

static int s_complete_and_clear_each_continuation(void *context, struct aws_hash_element *p_element) {
    (void)context;
    struct aws_event_stream_rpc_client_continuation_token *continuation = p_element->value;

    s_complete_continuation(continuation);

    return AWS_COMMON_HASH_TABLE_ITER_DELETE | AWS_COMMON_HASH_TABLE_ITER_CONTINUE;
}

/* Remove each continuation from hash-table and invoke its on_closed() callback.
 * The connection->is_open must be set false before calling this. */
static void s_clear_continuation_table(struct aws_event_stream_rpc_client_connection *connection) {
    AWS_ASSERT(!aws_event_stream_rpc_client_connection_is_open(connection));

    struct aws_hash_table temp_table;
    aws_hash_table_init(
        &temp_table,
        connection->allocator,
        64,
        aws_event_stream_rpc_hash_streamid,
        aws_event_stream_rpc_streamid_eq,
        NULL,
        NULL);

    /* Use lock to ensure synchronization with code that adds entries to table.
     * Since connection was just marked closed, no further entries will be
     * added to table once we acquire the lock.
     *
     *  While no further entries can be added, there are concurrent execution paths where things can be
     *  removed.  So rather than iterating the connection's table, swap it out for an empty one and iterate
     *  the temporary table instead.  Removing from an empty table will be harmless.
     */
    aws_mutex_lock(&connection->lock);
    aws_hash_table_swap(&temp_table, &connection->synced_data.continuation_table);
    aws_mutex_unlock(&connection->lock);

    aws_hash_table_foreach(&temp_table, s_mark_each_continuation_closed, NULL);
    aws_hash_table_foreach(&temp_table, s_complete_and_clear_each_continuation, NULL);
    aws_hash_table_clean_up(&temp_table);
}

int aws_event_stream_rpc_client_connection_connect(
    struct aws_allocator *allocator,
    const struct aws_event_stream_rpc_client_connection_options *conn_options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(conn_options);
    AWS_PRECONDITION(conn_options->on_connection_protocol_message);
    AWS_PRECONDITION(conn_options->on_connection_setup);
    AWS_PRECONDITION(conn_options->on_connection_shutdown);

    struct aws_event_stream_rpc_client_connection *connection =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_event_stream_rpc_client_connection));

    AWS_LOGF_TRACE(AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: creating new connection", (void *)connection);

    connection->allocator = allocator;
    aws_atomic_init_int(&connection->ref_count, 1);
    connection->bootstrap_ref = conn_options->bootstrap;
    connection->event_loop = aws_event_loop_group_get_next_loop(connection->bootstrap_ref->event_loop_group);

    /* this is released in the connection release which gets called regardless of if this function is successful or
     * not*/
    aws_client_bootstrap_acquire(connection->bootstrap_ref);
    connection->synced_data.handshake_state = CONNECTION_HANDSHAKE_STATE_INITIALIZED;
    connection->synced_data.is_open = true;
    aws_mutex_init(&connection->lock);

    connection->on_connection_shutdown = conn_options->on_connection_shutdown;
    connection->on_connection_terminated = conn_options->on_connection_terminated;
    connection->on_connection_protocol_message = conn_options->on_connection_protocol_message;
    connection->on_connection_setup = conn_options->on_connection_setup;
    connection->user_data = conn_options->user_data;

    if (aws_hash_table_init(
            &connection->synced_data.continuation_table,
            allocator,
            64,
            aws_event_stream_rpc_hash_streamid,
            aws_event_stream_rpc_streamid_eq,
            NULL,
            NULL)) {
        AWS_LOGF_ERROR(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: failed initializing continuation table with error %s.",
            (void *)connection,
            aws_error_debug_str(aws_last_error()));

        goto error;
    }

    struct aws_socket_channel_bootstrap_options bootstrap_options = {
        .bootstrap = connection->bootstrap_ref,
        .tls_options = conn_options->tls_options,
        .socket_options = conn_options->socket_options,
        .user_data = connection,
        .host_name = conn_options->host_name,
        .port = conn_options->port,
        .setup_callback = s_on_channel_setup_fn,
        .shutdown_callback = s_on_channel_shutdown_fn,
        .requested_event_loop = connection->event_loop,
    };

    if (aws_client_bootstrap_new_socket_channel(&bootstrap_options)) {
        AWS_LOGF_ERROR(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: failed creating new socket channel with error %s.",
            (void *)connection,
            aws_error_debug_str(aws_last_error()));
        goto error;
    }

    return AWS_OP_SUCCESS;

error:
    aws_event_stream_rpc_client_connection_release(connection);
    return AWS_OP_ERR;
}

void aws_event_stream_rpc_client_connection_acquire(const struct aws_event_stream_rpc_client_connection *connection) {
    AWS_PRECONDITION(connection);
    size_t current_count = aws_atomic_fetch_add_explicit(
        &((struct aws_event_stream_rpc_client_connection *)connection)->ref_count, 1, aws_memory_order_relaxed);
    AWS_LOGF_TRACE(
        AWS_LS_EVENT_STREAM_RPC_CLIENT,
        "id=%p: connection acquired, new ref count is %zu.",
        (void *)connection,
        current_count + 1);
}

static void s_destroy_connection(struct aws_event_stream_rpc_client_connection *connection) {
    AWS_LOGF_DEBUG(AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: destroying connection.", (void *)connection);
    aws_hash_table_clean_up(&connection->synced_data.continuation_table);
    aws_client_bootstrap_release(connection->bootstrap_ref);
    aws_mutex_clean_up(&connection->lock);

    aws_event_stream_rpc_client_on_connection_terminated_fn *terminated_fn = connection->on_connection_terminated;
    void *terminated_user_data = connection->user_data;

    aws_mem_release(connection->allocator, connection);

    if (terminated_fn) {
        terminated_fn(terminated_user_data);
    }
}

void aws_event_stream_rpc_client_connection_release(const struct aws_event_stream_rpc_client_connection *connection) {
    if (!connection) {
        return;
    }

    struct aws_event_stream_rpc_client_connection *connection_mut =
        (struct aws_event_stream_rpc_client_connection *)connection;
    size_t ref_count = aws_atomic_fetch_sub_explicit(&connection_mut->ref_count, 1, aws_memory_order_seq_cst);

    AWS_LOGF_TRACE(
        AWS_LS_EVENT_STREAM_RPC_CLIENT,
        "id=%p: connection released, new ref count is %zu.",
        (void *)connection,
        ref_count - 1);

    AWS_FATAL_ASSERT(ref_count != 0 && "Connection ref count has gone negative");

    if (ref_count == 1) {
        s_destroy_connection(connection_mut);
    }
}

struct aws_event_stream_client_connection_close_task {
    struct aws_allocator *allocator;

    struct aws_task base;
    struct aws_event_stream_rpc_client_connection *connection;
    int error_code;
};

static void s_aws_event_stream_client_connection_close_task_destroy(
    struct aws_event_stream_client_connection_close_task *task) {
    aws_event_stream_rpc_client_connection_release(task->connection);
    aws_mem_release(task->allocator, task);
}

static void s_close_connection(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;
    (void)status;

    struct aws_event_stream_client_connection_close_task *close_task = arg;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(close_task->connection->event_loop));

    if (close_task->connection->channel != NULL) {
        aws_channel_shutdown(close_task->connection->channel, close_task->error_code);
    }

    s_aws_event_stream_client_connection_close_task_destroy(close_task);
}

static struct aws_event_stream_client_connection_close_task *s_aws_event_stream_client_connection_close_task_new(
    struct aws_allocator *allocator,
    struct aws_event_stream_rpc_client_connection *connection,
    int error_code) {
    struct aws_event_stream_client_connection_close_task *task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_event_stream_client_connection_close_task));
    task->allocator = allocator;
    aws_task_init(&task->base, s_close_connection, task, "aws_event_stream_client_connection_close_task");
    task->connection = connection;
    task->error_code = error_code;
    aws_event_stream_rpc_client_connection_acquire(connection);

    return task;
}

void aws_event_stream_rpc_client_connection_close(
    struct aws_event_stream_rpc_client_connection *connection,
    int shutdown_error_code) {

    AWS_LOGF_TRACE(
        AWS_LS_EVENT_STREAM_RPC_CLIENT,
        "id=%p: connection close invoked with reason %s.",
        (void *)connection,
        aws_error_debug_str(shutdown_error_code));

    bool should_close = false;
    aws_mutex_lock(&connection->lock);
    should_close = connection->synced_data.is_open;
    connection->synced_data.is_open = false;
    aws_mutex_unlock(&connection->lock);

    if (should_close) {
        /* We may need to access the channel, which is only allowed on the event loop thread. */
        struct aws_event_stream_client_connection_close_task *close_task =
            s_aws_event_stream_client_connection_close_task_new(connection->allocator, connection, shutdown_error_code);
        aws_event_loop_schedule_task_now(connection->event_loop, &close_task->base);
    } else {
        AWS_LOGF_INFO(AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: connection already closed.", (void *)connection);
    }
}

bool aws_event_stream_rpc_client_connection_is_open(const struct aws_event_stream_rpc_client_connection *connection) {
    struct aws_event_stream_rpc_client_connection *mutable_connection =
        (struct aws_event_stream_rpc_client_connection *)connection;

    bool is_open = false;
    aws_mutex_lock(&mutable_connection->lock);
    is_open = connection->synced_data.is_open;
    aws_mutex_unlock(&mutable_connection->lock);

    return is_open;
}

struct event_stream_connection_send_message_args {
    struct aws_allocator *allocator;
    struct aws_event_stream_message message;
    enum aws_event_stream_rpc_message_type message_type;
    struct aws_event_stream_rpc_client_connection *connection;
    struct aws_event_stream_rpc_client_continuation_token *continuation;
    aws_event_stream_rpc_client_message_flush_fn *flush_fn;
    void *user_data;
    bool end_stream;
    bool terminate_connection;
};

static void s_event_stream_connection_send_message_args_delete(struct event_stream_connection_send_message_args *args) {
    if (args == NULL) {
        return;
    }

    if (args->continuation) {
        aws_event_stream_rpc_client_continuation_release(args->continuation);
    }

    if (args->connection) {
        aws_event_stream_rpc_client_connection_release(args->connection);
    }

    aws_event_stream_message_clean_up(&args->message);

    aws_mem_release(args->allocator, args);
}

static struct event_stream_connection_send_message_args *s_event_stream_connection_send_message_args_new(
    struct aws_event_stream_rpc_client_connection *connection,
    struct aws_event_stream_rpc_client_continuation_token *continuation,
    struct aws_byte_cursor *operation_name,
    const struct aws_event_stream_rpc_message_args *message_args,
    int32_t stream_id,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data) {

    struct aws_array_list headers_list;
    AWS_ZERO_STRUCT(headers_list);

    struct event_stream_connection_send_message_args *args =
        aws_mem_calloc(connection->allocator, 1, sizeof(struct event_stream_connection_send_message_args));
    args->allocator = connection->allocator;
    args->user_data = user_data;
    args->message_type = message_args->message_type;
    args->connection = connection;
    aws_event_stream_rpc_client_connection_acquire(connection);

    args->flush_fn = flush_fn;

    if (continuation) {
        AWS_LOGF_TRACE(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: creating message on continuation %p",
            (void *)connection,
            (void *)continuation);
        args->continuation = continuation;
        aws_event_stream_rpc_client_continuation_acquire(continuation);

        if (message_args->message_flags & AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_TERMINATE_STREAM) {
            AWS_LOGF_DEBUG(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p:end stream flag was specified on continuation %p",
                (void *)connection,
                (void *)continuation);
            args->end_stream = true;
        }
    }

    if (message_args->message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK &&
        !(message_args->message_flags & AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_CONNECTION_ACCEPTED)) {
        AWS_LOGF_DEBUG(AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: terminating connection", (void *)connection);
        args->terminate_connection = true;
    }

    args->flush_fn = flush_fn;

    size_t headers_count = 0;

    if (operation_name) {
        if (aws_add_size_checked(message_args->headers_count, 4, &headers_count)) {
            goto error;
        }
    } else {
        if (aws_add_size_checked(message_args->headers_count, 3, &headers_count)) {
            goto error;
        }
    }

    aws_array_list_init_dynamic(
        &headers_list, connection->allocator, headers_count, sizeof(struct aws_event_stream_header_value_pair));

    /* since we preallocated the space for the headers, these can't fail, but we'll go ahead an assert on them just in
     * case */
    for (size_t i = 0; i < message_args->headers_count; ++i) {
        AWS_FATAL_ASSERT(!aws_array_list_push_back(&headers_list, &message_args->headers[i]));
    }

    AWS_FATAL_ASSERT(!aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        (uint8_t)aws_event_stream_rpc_message_type_name.len,
        message_args->message_type));
    AWS_FATAL_ASSERT(!aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        (uint8_t)aws_event_stream_rpc_message_flags_name.len,
        message_args->message_flags));
    AWS_FATAL_ASSERT(!aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        (uint8_t)aws_event_stream_rpc_stream_id_name.len,
        stream_id));

    if (operation_name) {
        AWS_LOGF_DEBUG(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: operation name specified " PRInSTR,
            (void *)connection,
            AWS_BYTE_CURSOR_PRI(*operation_name));
        AWS_FATAL_ASSERT(!aws_event_stream_add_string_header(
            &headers_list,
            (const char *)aws_event_stream_rpc_operation_name.ptr,
            (uint8_t)aws_event_stream_rpc_operation_name.len,
            (const char *)operation_name->ptr,
            (uint16_t)operation_name->len,
            0));
    }

    int init_result =
        aws_event_stream_message_init(&args->message, connection->allocator, &headers_list, message_args->payload);
    aws_array_list_clean_up(&headers_list);

    if (init_result) {
        AWS_LOGF_ERROR(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: message init failed with error %s",
            (void *)connection,
            aws_error_debug_str(aws_last_error()));
        goto error;
    }

    return args;

error:

    s_event_stream_connection_send_message_args_delete(args);

    return NULL;
}

struct aws_event_stream_client_connection_send_message_task {
    struct aws_allocator *allocator;

    struct aws_task base;
    struct event_stream_connection_send_message_args *message_args;
};

static void s_aws_event_stream_client_connection_send_message_task_destroy(
    struct aws_event_stream_client_connection_send_message_task *task) {

    if (task->message_args != NULL) {
        s_event_stream_connection_send_message_args_delete(task->message_args);
    }

    aws_mem_release(task->allocator, task);
}

static void s_on_protocol_message_written_fn(
    struct aws_event_stream_message *message,
    int error_code,
    void *user_data) {
    (void)message;

    struct event_stream_connection_send_message_args *message_args = user_data;

    if (message_args->message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT) {
        AWS_LOGF_DEBUG(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: connect message flushed to the wire.",
            (void *)message_args->connection);
    } else {
        AWS_LOGF_TRACE(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: message %p flushed to channel.",
            (void *)message_args->connection,
            (void *)message);
    }

    if (message_args->end_stream) {
        AWS_LOGF_DEBUG(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: the end stream flag was set, closing continuation %p.",
            (void *)message_args->connection,
            (void *)message_args->continuation);
        AWS_FATAL_ASSERT(message_args->continuation && "end stream flag was set but it wasn't on a continuation");
        aws_atomic_store_int(&message_args->continuation->is_closed, 1U);

        int was_present = 0;
        aws_mutex_lock(&message_args->connection->lock);
        aws_hash_table_remove(
            &message_args->connection->synced_data.continuation_table,
            &message_args->continuation->stream_id,
            NULL,
            &was_present);
        aws_mutex_unlock(&message_args->connection->lock);

        /*
         * Whoever successfully removes the continuation from the table gets to complete it.
         * Lock must NOT be held while invoking callback
         */
        if (was_present) {
            s_complete_continuation(message_args->continuation);
        }
    }

    message_args->flush_fn(error_code, message_args->user_data);

    if (message_args->terminate_connection) {
        AWS_LOGF_DEBUG(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: terminate_connection flag was specified. Shutting down the connection.",
            (void *)message_args->connection);
        aws_event_stream_rpc_client_connection_close(message_args->connection, AWS_ERROR_SUCCESS);
    }

    s_event_stream_connection_send_message_args_delete(message_args);
}

static void s_send_message_task_fn(struct aws_task *task, void *arg, enum aws_task_status status) {
    (void)task;

    struct aws_event_stream_client_connection_send_message_task *send_task = arg;
    struct event_stream_connection_send_message_args *message_args = send_task->message_args;
    struct aws_event_stream_rpc_client_connection *connection = message_args->connection;
    enum aws_event_stream_rpc_message_type message_type = message_args->message_type;

    AWS_FATAL_ASSERT(aws_event_loop_thread_is_callers_thread(message_args->connection->event_loop));

    int failure_error_code = AWS_ERROR_UNKNOWN;
    bool should_fail = true;
    if (status == AWS_TASK_STATUS_RUN_READY) {
        bool is_open = false;
        bool is_handshake_state_valid = false;
        aws_mutex_lock(&connection->lock);

        is_open = connection->synced_data.is_open;
        switch (connection->synced_data.handshake_state) {
            case CONNECTION_HANDSHAKE_STATE_INITIALIZED:
                is_handshake_state_valid = message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT;
                if (is_handshake_state_valid) {
                    connection->synced_data.handshake_state = CONNECTION_HANDSHAKE_STATE_CONNECT_PROCESSED;
                }
                break;

            case CONNECTION_HANDSHAKE_STATE_CONNECT_ACK_PROCESSED:
                is_handshake_state_valid = message_type != AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT &&
                                           message_type != AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK;
                break;

            default:
                // nothing is valid to send while awaiting the connack
                break;
        }

        aws_mutex_unlock(&connection->lock);

        if (!is_open) {
            AWS_LOGF_INFO(
                AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: connection closed, cannot send messages", (void *)connection);
            failure_error_code = AWS_ERROR_EVENT_STREAM_RPC_CONNECTION_CLOSED;
            goto should_fail_block;
        }

        if (connection->channel == NULL || connection->event_stream_handler == NULL) {
            AWS_LOGF_INFO(
                AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: channel destroyed, cannot send messages", (void *)connection);
            failure_error_code = AWS_ERROR_EVENT_STREAM_RPC_CONNECTION_CLOSED;
            goto should_fail_block;
        }

        if (!is_handshake_state_valid) {
            AWS_LOGF_ERROR(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: invalid handshake state for message type (%d)",
                (void *)connection,
                (int)message_args->message_type);
            failure_error_code = AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR;
            goto should_fail_block;
        }

        should_fail = false;
    }

    if (!should_fail) {
        if (aws_event_stream_channel_handler_write_message(
                connection->event_stream_handler,
                &message_args->message,
                s_on_protocol_message_written_fn,
                message_args)) {
            failure_error_code = aws_last_error();
            should_fail = true;
            AWS_LOGF_ERROR(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: writing message failed with error %s",
                (void *)connection,
                aws_error_debug_str(failure_error_code));
        } else {
            /* ownership transfer to flush callback, so don't destroy on task destruction */
            send_task->message_args = NULL;
        }
    }

should_fail_block:

    if (should_fail) {
        AWS_FATAL_ASSERT(send_task->message_args != NULL);

        /*
         * We didn't transfer ownership of the message args to the flush callback via a successful handler send
         * call.  We must invoke a failed callback.
         */
        if (message_args->flush_fn != NULL) {
            (*message_args->flush_fn)(failure_error_code, message_args->user_data);
        }

        /* Fail-to-send is connection fatal */
        if (connection->channel != NULL) {
            aws_channel_shutdown(connection->channel, failure_error_code);
        }
    }

    s_aws_event_stream_client_connection_send_message_task_destroy(send_task);
}

static struct aws_event_stream_client_connection_send_message_task *
    s_aws_event_stream_client_connection_send_message_task_new(
        struct aws_allocator *allocator,
        struct event_stream_connection_send_message_args *message_args) {
    struct aws_event_stream_client_connection_send_message_task *task =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_event_stream_client_connection_send_message_task));
    task->allocator = allocator;
    aws_task_init(&task->base, s_send_message_task_fn, task, "aws_event_stream_client_connection_send_message_task");
    task->message_args = message_args;

    return task;
}

static int s_send_protocol_message(
    struct aws_event_stream_rpc_client_connection *connection,
    struct aws_event_stream_rpc_client_continuation_token *continuation,
    struct aws_byte_cursor *operation_name,
    const struct aws_event_stream_rpc_message_args *message_args,
    int32_t stream_id,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data) {

    AWS_LOGF_TRACE(AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: attempting to send protocol message", (void *)connection);

    struct event_stream_connection_send_message_args *args = s_event_stream_connection_send_message_args_new(
        connection, continuation, operation_name, message_args, stream_id, flush_fn, user_data);
    if (args == NULL) {
        return AWS_OP_ERR;
    }

    struct aws_event_stream_client_connection_send_message_task *task =
        s_aws_event_stream_client_connection_send_message_task_new(connection->allocator, args);
    aws_event_loop_schedule_task_now(connection->event_loop, &task->base);

    return AWS_OP_SUCCESS;
}

int aws_event_stream_rpc_client_connection_send_protocol_message(
    struct aws_event_stream_rpc_client_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data) {

    return s_send_protocol_message(connection, NULL, NULL, message_args, 0, flush_fn, user_data);
}

static void s_connection_error_message_flush_fn(int error_code, void *user_data) {
    (void)error_code;

    struct aws_event_stream_rpc_client_connection *connection = user_data;
    aws_event_stream_rpc_client_connection_close(connection, AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR);
}

static void s_send_connection_level_error(
    struct aws_event_stream_rpc_client_connection *connection,
    uint32_t message_type,
    uint32_t message_flags,
    const struct aws_byte_cursor *message) {
    struct aws_byte_buf payload_buf = aws_byte_buf_from_array(message->ptr, message->len);

    AWS_LOGF_DEBUG(
        AWS_LS_EVENT_STREAM_RPC_CLIENT,
        "id=%p: sending connection-level error\n" PRInSTR,
        (void *)connection,
        AWS_BYTE_BUF_PRI(payload_buf));

    struct aws_event_stream_header_value_pair content_type_header =
        aws_event_stream_create_string_header(s_json_content_type_name, s_json_content_type_value);

    struct aws_event_stream_header_value_pair headers[] = {
        content_type_header,
    };

    struct aws_event_stream_rpc_message_args message_args = {
        .message_type = message_type,
        .message_flags = message_flags,
        .payload = &payload_buf,
        .headers_count = 1,
        .headers = headers,
    };

    aws_event_stream_rpc_client_connection_send_protocol_message(
        connection, &message_args, s_connection_error_message_flush_fn, connection);
}

static void s_route_message_by_type(
    struct aws_event_stream_rpc_client_connection *connection,
    struct aws_event_stream_message *message,
    struct aws_array_list *headers_list,
    uint32_t stream_id,
    uint32_t message_type,
    uint32_t message_flags) {
    struct aws_byte_buf payload_buf = aws_byte_buf_from_array(
        aws_event_stream_message_payload(message), aws_event_stream_message_payload_len(message));

    struct aws_event_stream_rpc_message_args message_args = {
        .headers = headers_list->data,
        .headers_count = aws_array_list_length(headers_list),
        .payload = &payload_buf,
        .message_flags = message_flags,
        .message_type = message_type,
    };

    aws_mutex_lock(&connection->lock);
    enum aws_event_stream_connection_handshake_state handshake_state = connection->synced_data.handshake_state;
    aws_mutex_unlock(&connection->lock);

    /* make sure if this is not a CONNECT message being received, the handshake has been completed. */
    if (handshake_state < CONNECTION_HANDSHAKE_STATE_CONNECT_ACK_PROCESSED &&
        message_type != AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK) {
        AWS_LOGF_ERROR(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: a message was received on this connection prior to the "
            "connect handshake completing",
            (void *)connection);
        aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR);
        s_send_connection_level_error(
            connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_connect_not_completed_error);
        return;
    }

    /* stream_id being non zero ALWAYS indicates APPLICATION_DATA or APPLICATION_ERROR. */
    if (stream_id > 0) {
        AWS_LOGF_TRACE(AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: stream id %" PRIu32, (void *)connection, stream_id);
        struct aws_event_stream_rpc_client_continuation_token *continuation = NULL;
        if (message_type > AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_ERROR) {
            AWS_LOGF_ERROR(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: only application messages can be sent on a stream id, "
                "but this message is the incorrect type",
                (void *)connection);
            aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR);
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_invalid_stream_id_error);
            return;
        }

        aws_mutex_lock(&connection->lock);
        struct aws_hash_element *continuation_element = NULL;
        if (aws_hash_table_find(&connection->synced_data.continuation_table, &stream_id, &continuation_element) ||
            !continuation_element) {
            bool old_stream_id = stream_id <= connection->synced_data.latest_stream_id;
            aws_mutex_unlock(&connection->lock);
            if (!old_stream_id) {
                AWS_LOGF_ERROR(
                    AWS_LS_EVENT_STREAM_RPC_CLIENT,
                    "id=%p: a stream id was received that was not created by this client",
                    (void *)connection);
                aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR);
                s_send_connection_level_error(
                    connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_invalid_client_stream_id_error);
            } else {
                AWS_LOGF_WARN(
                    AWS_LS_EVENT_STREAM_RPC_CLIENT,
                    "id=%p: a stream id was received that corresponds to an already-closed stream",
                    (void *)connection);
            }
            return;
        }

        continuation = continuation_element->value;
        AWS_FATAL_ASSERT(continuation != NULL);
        aws_event_stream_rpc_client_continuation_acquire(continuation);

        aws_mutex_unlock(&connection->lock);

        continuation->continuation_fn(continuation, &message_args, continuation->user_data);

        /* if it was a terminal stream message purge it from the hash table. The delete will decref the continuation. */
        if (message_flags & AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_TERMINATE_STREAM) {
            AWS_LOGF_DEBUG(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: the terminate stream flag was specified for continuation %p",
                (void *)connection,
                (void *)continuation);
            aws_atomic_store_int(&continuation->is_closed, 1U);
            int was_present = 0;
            aws_mutex_lock(&connection->lock);
            aws_hash_table_remove(&connection->synced_data.continuation_table, &stream_id, NULL, &was_present);
            aws_mutex_unlock(&connection->lock);

            /*
             * Whoever successfully removes the continuation from the table gets to complete it.
             * Lock must NOT be held while invoking callback
             */
            if (was_present) {
                s_complete_continuation(continuation);
            }
        }

        aws_event_stream_rpc_client_continuation_release(continuation);
    } else {
        if (message_type <= AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_ERROR ||
            message_type >= AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_COUNT) {
            AWS_LOGF_ERROR(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: a zero stream id was received with an invalid message-type %" PRIu32,
                (void *)connection,
                message_type);
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_invalid_message_type_error);
            return;
        }

        if (message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK) {
            aws_mutex_lock(&connection->lock);
            bool bad_handshake_state =
                connection->synced_data.handshake_state != CONNECTION_HANDSHAKE_STATE_CONNECT_PROCESSED;
            if (!bad_handshake_state) {
                connection->synced_data.handshake_state = CONNECTION_HANDSHAKE_STATE_CONNECT_ACK_PROCESSED;
            }
            aws_mutex_unlock(&connection->lock);

            if (bad_handshake_state) {
                AWS_LOGF_ERROR(
                    AWS_LS_EVENT_STREAM_RPC_CLIENT,
                    "id=%p: connect ack received but the handshake is already completed. Only one is allowed.",
                    (void *)connection);
                /* only one connect is allowed. This would be a duplicate. */
                s_send_connection_level_error(
                    connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_connect_not_completed_error);
                return;
            }

            AWS_LOGF_INFO(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: connect ack received, connection handshake completed",
                (void *)connection);
        }

        connection->on_connection_protocol_message(connection, &message_args, connection->user_data);
    }
}

/* invoked by the event stream channel handler when a complete message has been read from the channel. */
static void s_on_message_received(struct aws_event_stream_message *message, int error_code, void *user_data) {

    if (!error_code) {
        struct aws_event_stream_rpc_client_connection *connection = user_data;

        AWS_LOGF_TRACE(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: message received on connection of length %" PRIu32,
            (void *)connection,
            aws_event_stream_message_total_length(message));

        struct aws_array_list headers;
        if (aws_array_list_init_dynamic(
                &headers, connection->allocator, 8, sizeof(struct aws_event_stream_header_value_pair))) {
            AWS_LOGF_ERROR(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: error initializing headers %s",
                (void *)connection,
                aws_error_debug_str(aws_last_error()));
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_INTERNAL_ERROR, 0, &s_internal_error);
            return;
        }

        if (aws_event_stream_message_headers(message, &headers)) {
            AWS_LOGF_ERROR(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: error fetching headers %s",
                (void *)connection,
                aws_error_debug_str(aws_last_error()));
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_INTERNAL_ERROR, 0, &s_internal_error);
            goto clean_up;
        }

        int32_t stream_id = -1;
        int32_t message_type = -1;
        int32_t message_flags = -1;

        struct aws_byte_buf operation_name_buf;
        AWS_ZERO_STRUCT(operation_name_buf);
        if (aws_event_stream_rpc_extract_message_metadata(
                &headers, &stream_id, &message_type, &message_flags, &operation_name_buf)) {
            AWS_LOGF_ERROR(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: invalid protocol message with error %s",
                (void *)connection,
                aws_error_debug_str(aws_last_error()));
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_invalid_message_error);
            goto clean_up;
        }

        (void)operation_name_buf;

        AWS_LOGF_TRACE(AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: routing message", (void *)connection);
        s_route_message_by_type(connection, message, &headers, stream_id, message_type, message_flags);

    clean_up:
        aws_event_stream_headers_list_cleanup(&headers);
    }
}

struct aws_event_stream_rpc_client_continuation_token *aws_event_stream_rpc_client_connection_new_stream(
    struct aws_event_stream_rpc_client_connection *connection,
    const struct aws_event_stream_rpc_client_stream_continuation_options *continuation_options) {
    AWS_PRECONDITION(continuation_options->on_continuation_closed);
    AWS_PRECONDITION(continuation_options->on_continuation);

    AWS_LOGF_TRACE(AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: creating a new stream on connection", (void *)connection);

    struct aws_event_stream_rpc_client_continuation_token *continuation =
        aws_mem_calloc(connection->allocator, 1, sizeof(struct aws_event_stream_rpc_client_continuation_token));

    if (!continuation) {
        AWS_LOGF_ERROR(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: error while allocating continuation %s",
            (void *)connection,
            aws_error_debug_str(aws_last_error()));
        return NULL;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: continuation created %p", (void *)connection, (void *)continuation);
    continuation->connection = connection;
    aws_event_stream_rpc_client_connection_acquire(continuation->connection);
    aws_atomic_init_int(&continuation->ref_count, 1);
    aws_atomic_init_int(&continuation->is_closed, 0);
    aws_atomic_init_int(&continuation->is_complete, 0);
    continuation->continuation_fn = continuation_options->on_continuation;
    continuation->closed_fn = continuation_options->on_continuation_closed;
    continuation->terminated_fn = continuation_options->on_continuation_terminated;
    continuation->user_data = continuation_options->user_data;

    return continuation;
}

void *aws_event_stream_rpc_client_continuation_get_user_data(
    struct aws_event_stream_rpc_client_continuation_token *continuation) {
    return continuation->user_data;
}

void aws_event_stream_rpc_client_continuation_acquire(
    const struct aws_event_stream_rpc_client_continuation_token *continuation) {
    size_t current_count = aws_atomic_fetch_add_explicit(
        &((struct aws_event_stream_rpc_client_continuation_token *)continuation)->ref_count,
        1u,
        aws_memory_order_relaxed);
    AWS_LOGF_TRACE(
        AWS_LS_EVENT_STREAM_RPC_CLIENT,
        "id=%p: continuation acquired, new ref count is %zu.",
        (void *)continuation,
        current_count + 1);
}

void aws_event_stream_rpc_client_continuation_release(
    const struct aws_event_stream_rpc_client_continuation_token *continuation) {
    if (AWS_UNLIKELY(!continuation)) {
        return;
    }

    struct aws_event_stream_rpc_client_continuation_token *continuation_mut =
        (struct aws_event_stream_rpc_client_continuation_token *)continuation;
    size_t ref_count = aws_atomic_fetch_sub_explicit(&continuation_mut->ref_count, 1, aws_memory_order_seq_cst);

    AWS_LOGF_TRACE(
        AWS_LS_EVENT_STREAM_RPC_CLIENT,
        "id=%p: continuation released, new ref count is %zu.",
        (void *)continuation,
        ref_count - 1);

    AWS_FATAL_ASSERT(ref_count != 0 && "Continuation ref count has gone negative");

    if (ref_count == 1) {
        struct aws_allocator *allocator = continuation_mut->connection->allocator;
        aws_event_stream_rpc_client_connection_release(continuation_mut->connection);

        aws_event_stream_rpc_client_stream_continuation_terminated_fn *terminated_fn = continuation_mut->terminated_fn;
        void *terminated_user_data = continuation_mut->user_data;

        aws_mem_release(allocator, continuation_mut);

        if (terminated_fn) {
            terminated_fn(terminated_user_data);
        }
    }
}

bool aws_event_stream_rpc_client_continuation_is_closed(
    const struct aws_event_stream_rpc_client_continuation_token *continuation) {
    return aws_atomic_load_int(&continuation->is_closed) == 1u;
}

int aws_event_stream_rpc_client_continuation_activate(
    struct aws_event_stream_rpc_client_continuation_token *continuation,
    struct aws_byte_cursor operation_name,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data) {

    AWS_LOGF_TRACE(AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: activating continuation", (void *)continuation);
    int ret_val = AWS_OP_ERR;

    if (continuation->stream_id) {
        AWS_LOGF_ERROR(
            AWS_LS_EVENT_STREAM_RPC_CLIENT, "id=%p: stream has already been activated", (void *)continuation);
        return aws_raise_error(AWS_ERROR_INVALID_STATE);
    }

    aws_mutex_lock(&continuation->connection->lock);

    if (continuation->connection->synced_data.is_open &&
        continuation->connection->synced_data.handshake_state == CONNECTION_HANDSHAKE_STATE_CONNECT_ACK_PROCESSED) {
        continuation->stream_id = continuation->connection->synced_data.latest_stream_id + 1;
        AWS_LOGF_DEBUG(
            AWS_LS_EVENT_STREAM_RPC_CLIENT,
            "id=%p: continuation's new stream id is %" PRIu32,
            (void *)continuation,
            continuation->stream_id);

        if (aws_hash_table_put(
                &continuation->connection->synced_data.continuation_table,
                &continuation->stream_id,
                continuation,
                NULL)) {
            AWS_LOGF_ERROR(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: storing the new stream failed with %s",
                (void *)continuation,
                aws_error_debug_str(aws_last_error()));
            continuation->stream_id = 0;
            goto clean_up;
        }

        /*
         * In the original event stream, this was a synchronous operation and so the stream id increment happened after
         * flush callback.  Post-refactor, this submits an async cross-thread task to the event loop that sends the
         * message.
         *
         * Cross thread tasks are handled in-order and we hold the lock while allocating and submitting the task, so we
         * know that the send tasks will be processed in stream id order on the event loop.  The question remains, what
         * happens if there's a failure to send; will that disrupt stream id order due to a need to rewind?  The answer
         * is no because any failures to send after this point are connection-fatal, so it doesn't matter that the
         * stream ids get out-of-order.
         */
        if (s_send_protocol_message(
                continuation->connection,
                continuation,
                &operation_name,
                message_args,
                continuation->stream_id,
                flush_fn,
                user_data)) {
            aws_hash_table_remove(
                &continuation->connection->synced_data.continuation_table, &continuation->stream_id, NULL, NULL);
            continuation->stream_id = 0;
            AWS_LOGF_ERROR(
                AWS_LS_EVENT_STREAM_RPC_CLIENT,
                "id=%p: failed to flush the new stream to the channel with error %s",
                (void *)continuation,
                aws_error_debug_str(aws_last_error()));
            goto clean_up;
        }

        /* The continuation table gets a ref count on the continuation. Take it here. */
        aws_event_stream_rpc_client_continuation_acquire(continuation);

        continuation->connection->synced_data.latest_stream_id = continuation->stream_id;
        ret_val = AWS_OP_SUCCESS;
    } else {
        /*
         * This catches two cases:
         *
         * (1) Attempting to activate a continuation before the connack is processed.  This would result in a protocol
         *  error that would shut down the channel, so fail it before that can happen.
         * (2) Attempting to activate a continuation after the channel has gone down.  This leads to a situation where
         *  the activate is guaranteed to fail but the table cleanup may already have run before we insert ourselves,
         *  creating an orphaned continuation that leaks.
         */
        aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_CONNECTION_CLOSED);
    }

clean_up:
    aws_mutex_unlock(&continuation->connection->lock);
    return ret_val;
}

int aws_event_stream_rpc_client_continuation_send_message(
    struct aws_event_stream_rpc_client_continuation_token *continuation,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data) {

    if (aws_event_stream_rpc_client_continuation_is_closed(continuation)) {
        return aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_STREAM_CLOSED);
    }

    if (!continuation->stream_id) {
        return aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_STREAM_NOT_ACTIVATED);
    }

    return s_send_protocol_message(
        continuation->connection, continuation, NULL, message_args, continuation->stream_id, flush_fn, user_data);
}

struct aws_event_loop *aws_event_stream_rpc_client_connection_get_event_loop(
    const struct aws_event_stream_rpc_client_connection *connection) {

    if (!connection) {
        return NULL;
    }

    return connection->event_loop;
}
