/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/io/io.h>

#include <aws/io/logging.h>

#include <aws/cal/cal.h>

#define AWS_DEFINE_ERROR_INFO_IO(CODE, STR) [(CODE)-0x0400] = AWS_DEFINE_ERROR_INFO(CODE, STR, "aws-c-io")

/* clang-format off */
static struct aws_error_info s_errors[] = {
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_CHANNEL_ERROR_ERROR_CANT_ACCEPT_INPUT,
        "Channel cannot accept input"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_CHANNEL_UNKNOWN_MESSAGE_TYPE,
        "Channel unknown message type"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_CHANNEL_READ_WOULD_EXCEED_WINDOW,
        "A channel handler attempted to propagate a read larger than the upstream window"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_EVENT_LOOP_ALREADY_ASSIGNED,
        "An attempt was made to assign an io handle to an event loop, but the handle was already assigned."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_EVENT_LOOP_SHUTDOWN,
        "Event loop has shutdown and a resource was still using it, the resource has been removed from the loop."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_TLS_ERROR_NEGOTIATION_FAILURE,
        "TLS (SSL) negotiation failed"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_TLS_ERROR_NOT_NEGOTIATED,
        "Attempt to read/write, but TLS (SSL) hasn't been negotiated"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_TLS_ERROR_WRITE_FAILURE,
        "Failed to write to TLS handler"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_TLS_ERROR_ALERT_RECEIVED,
        "Fatal TLS Alert was received"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_TLS_CTX_ERROR,
        "Failed to create tls context"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_TLS_VERSION_UNSUPPORTED,
        "A TLS version was specified that is currently not supported. Consider using AWS_IO_TLS_VER_SYS_DEFAULTS, "
        " and when this lib or the operating system is updated, it will automatically be used."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_TLS_CIPHER_PREF_UNSUPPORTED,
        "A TLS Cipher Preference was specified that is currently not supported by the current platform. Consider "
        " using AWS_IO_TLS_CIPHER_SYSTEM_DEFAULT, and when this lib or the operating system is updated, it will "
        "automatically be used."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_MISSING_ALPN_MESSAGE,
        "An ALPN message was expected but not received"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_UNHANDLED_ALPN_PROTOCOL_MESSAGE,
        "An ALPN message was received but a handler was not created by the user"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_FILE_VALIDATION_FAILURE,
        "A file was read and the input did not match the expected value"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_ERROR_IO_EVENT_LOOP_THREAD_ONLY,
        "Attempt to perform operation that must be run inside the event loop thread"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_ERROR_IO_ALREADY_SUBSCRIBED,
        "Already subscribed to receive events"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_ERROR_IO_NOT_SUBSCRIBED,
        "Not subscribed to receive events"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_ERROR_IO_OPERATION_CANCELLED,
        "Operation cancelled before it could complete"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_READ_WOULD_BLOCK,
        "Read operation would block, try again later"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_BROKEN_PIPE,
        "Attempt to read or write to io handle that has already been closed."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_UNSUPPORTED_ADDRESS_FAMILY,
        "Socket, unsupported address family."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_INVALID_OPERATION_FOR_TYPE,
        "Invalid socket operation for socket type."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_CONNECTION_REFUSED,
        "socket connection refused."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_TIMEOUT,
        "socket operation timed out."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_NO_ROUTE_TO_HOST,
        "socket connect failure, no route to host."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_NETWORK_DOWN,
        "network is down."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_CLOSED,
        "socket is closed."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_NOT_CONNECTED,
        "socket not connected."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_INVALID_OPTIONS,
        "Invalid socket options."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_ADDRESS_IN_USE,
        "Socket address already in use."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_INVALID_ADDRESS,
        "Invalid socket address."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_ILLEGAL_OPERATION_FOR_STATE,
        "Illegal operation for socket state."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SOCKET_CONNECT_ABORTED,
        "Incoming connection was aborted."),
    AWS_DEFINE_ERROR_INFO_IO (
        AWS_IO_DNS_QUERY_FAILED,
        "A query to dns failed to resolve."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_DNS_INVALID_NAME,
        "Host name was invalid for dns resolution."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_DNS_NO_ADDRESS_FOR_HOST,
        "No address was found for the supplied host name."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_DNS_HOST_REMOVED_FROM_CACHE,
        "The entries for host name were removed from the local dns cache."),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_STREAM_INVALID_SEEK_POSITION,
        "The seek position was outside of a stream's bounds"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_STREAM_READ_FAILED,
        "Stream failed to read from the underlying io source"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_INVALID_FILE_HANDLE,
        "Operation failed because the file handle was invalid"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SHARED_LIBRARY_LOAD_FAILURE,
        "System call error during attempt to load shared library"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_SHARED_LIBRARY_FIND_SYMBOL_FAILURE,
        "System call error during attempt to find shared library symbol"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_TLS_NEGOTIATION_TIMEOUT,
        "Channel shutdown due to tls negotiation timeout"),
    AWS_DEFINE_ERROR_INFO_IO(
        AWS_IO_TLS_ALERT_NOT_GRACEFUL,
       "Channel shutdown due to tls alert. The alert was not for a graceful shutdown."),
    AWS_DEFINE_ERROR_INFO_IO(
       AWS_IO_MAX_RETRIES_EXCEEDED,
       "Retry cannot be attempted because the maximum number of retries has been exceeded."),
    AWS_DEFINE_ERROR_INFO_IO(
       AWS_IO_RETRY_PERMISSION_DENIED,
       "Retry cannot be attempted because the retry strategy has prevented the operation."),
};
/* clang-format on */

static struct aws_error_info_list s_list = {
    .error_list = s_errors,
    .count = sizeof(s_errors) / sizeof(struct aws_error_info),
};

static struct aws_log_subject_info s_io_log_subject_infos[] = {
    DEFINE_LOG_SUBJECT_INFO(
        AWS_LS_IO_GENERAL,
        "aws-c-io",
        "Subject for IO logging that doesn't belong to any particular category"),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_EVENT_LOOP, "event-loop", "Subject for Event-loop specific logging."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_SOCKET, "socket", "Subject for Socket specific logging."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_SOCKET_HANDLER, "socket-handler", "Subject for a socket channel handler."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_TLS, "tls-handler", "Subject for TLS-related logging"),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_ALPN, "alpn", "Subject for ALPN-related logging"),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_DNS, "dns", "Subject for DNS-related logging"),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_PKI, "pki-utils", "Subject for Pki utilities."),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_CHANNEL, "channel", "Subject for Channels"),
    DEFINE_LOG_SUBJECT_INFO(
        AWS_LS_IO_CHANNEL_BOOTSTRAP,
        "channel-bootstrap",
        "Subject for channel bootstrap (client and server modes)"),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_FILE_UTILS, "file-utils", "Subject for file operations"),
    DEFINE_LOG_SUBJECT_INFO(AWS_LS_IO_SHARED_LIBRARY, "shared-library", "Subject for shared library operations"),
    DEFINE_LOG_SUBJECT_INFO(
        AWS_LS_IO_EXPONENTIAL_BACKOFF_RETRY_STRATEGY,
        "exp-backoff-strategy",
        "Subject for exponential backoff retry strategy")};

static struct aws_log_subject_info_list s_io_log_subject_list = {
    .subject_list = s_io_log_subject_infos,
    .count = AWS_ARRAY_SIZE(s_io_log_subject_infos),
};

static bool s_io_library_initialized = false;

void aws_tls_init_static_state(struct aws_allocator *alloc);
void aws_tls_clean_up_static_state(void);

void aws_io_library_init(struct aws_allocator *allocator) {
    if (!s_io_library_initialized) {
        s_io_library_initialized = true;
        aws_common_library_init(allocator);
        aws_cal_library_init(allocator);
        aws_register_error_info(&s_list);
        aws_register_log_subject_info_list(&s_io_log_subject_list);
        aws_tls_init_static_state(allocator);
    }
}

void aws_io_library_clean_up(void) {
    if (s_io_library_initialized) {
        s_io_library_initialized = false;
        aws_tls_clean_up_static_state();
        aws_unregister_error_info(&s_list);
        aws_unregister_log_subject_info_list(&s_io_log_subject_list);
        aws_cal_library_clean_up();
        aws_common_library_clean_up();
    }
}

void aws_io_fatal_assert_library_initialized(void) {
    if (!s_io_library_initialized) {
        AWS_LOGF_FATAL(
            AWS_LS_IO_GENERAL, "aws_io_library_init() must be called before using any functionality in aws-c-io.");

        AWS_FATAL_ASSERT(s_io_library_initialized);
    }
}
