/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/mqtt.h>

#include <aws/io/logging.h>

#ifdef AWS_MQTT_WITH_WEBSOCKETS
#    include <aws/http/http.h>
#endif

/*******************************************************************************
 * Topic Validation
 ******************************************************************************/

static bool s_is_valid_topic(const struct aws_byte_cursor *topic, bool is_filter) {

    /* [MQTT-4.7.3-1] Check existance and length */
    if (!topic->ptr || !topic->len) {
        return false;
    }

    /* [MQTT-4.7.3-2] Check for the null character */
    if (memchr(topic->ptr, 0, topic->len)) {
        return false;
    }

    /* [MQTT-4.7.3-3] Topic must not be too long */
    if (topic->len > 65535) {
        return false;
    }

    bool saw_hash = false;

    struct aws_byte_cursor topic_part;
    AWS_ZERO_STRUCT(topic_part);
    while (aws_byte_cursor_next_split(topic, '/', &topic_part)) {

        if (saw_hash) {
            /* [MQTT-4.7.1-2] If last part was a '#' and there's still another part, it's an invalid topic */
            return false;
        }

        if (topic_part.len == 0) {
            /* 0 length parts are fine */
            continue;
        }

        /* Check single level wildcard */
        if (memchr(topic_part.ptr, '+', topic_part.len)) {
            if (!is_filter) {
                /* [MQTT-4.7.1-3] + only allowed on filters */
                return false;
            }
            if (topic_part.len > 1) {
                /* topic part must be 1 character long */
                return false;
            }
        }

        /* Check multi level wildcard */
        if (memchr(topic_part.ptr, '#', topic_part.len)) {
            if (!is_filter) {
                /* [MQTT-4.7.1-2] # only allowed on filters */
                return false;
            }
            if (topic_part.len > 1) {
                /* topic part must be 1 character long */
                return false;
            }
            saw_hash = true;
        }
    }

    return true;
}

bool aws_mqtt_is_valid_topic(const struct aws_byte_cursor *topic) {

    return s_is_valid_topic(topic, false);
}
bool aws_mqtt_is_valid_topic_filter(const struct aws_byte_cursor *topic_filter) {

    return s_is_valid_topic(topic_filter, true);
}

/*******************************************************************************
 * Library Init
 ******************************************************************************/

#define AWS_DEFINE_ERROR_INFO_MQTT(C, ES) AWS_DEFINE_ERROR_INFO(C, ES, "libaws-c-mqtt")
/* clang-format off */
        static struct aws_error_info s_errors[] = {
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_RESERVED_BITS,
                "Bits marked as reserved in the MQTT spec were incorrectly set."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_BUFFER_TOO_BIG,
                "[MQTT-1.5.3] Encoded UTF-8 buffers may be no bigger than 65535 bytes."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_REMAINING_LENGTH,
                "[MQTT-2.2.3] Encoded remaining length field is malformed."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_NAME,
                "[MQTT-3.1.2-1] Protocol name specified is unsupported."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_UNSUPPORTED_PROTOCOL_LEVEL,
                "[MQTT-3.1.2-2] Protocol level specified is unsupported."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_CREDENTIALS,
                "[MQTT-3.1.2-21] Connect packet may not include password when no username is present."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_QOS,
                "Both bits in a QoS field must not be set."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_PACKET_TYPE,
                "Packet type in packet fixed header is invalid."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_INVALID_TOPIC,
                "Topic or filter is invalid."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_TIMEOUT,
                "Time limit between request and response has been exceeded."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_PROTOCOL_ERROR,
                "Protocol error occurred."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_NOT_CONNECTED,
                "The requested operation is invalid as the connection is not open."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_ALREADY_CONNECTED,
                "The requested operation is invalid as the connection is already open."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_BUILT_WITHOUT_WEBSOCKETS,
                "Library built without MQTT_WITH_WEBSOCKETS option."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_UNEXPECTED_HANGUP,
                "The connection was closed unexpectedly."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_CONNECTION_SHUTDOWN,
                "MQTT operation interrupted by connection shutdown."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_CONNECTION_DESTROYED,
                "Connection has started destroying process, all uncompleted requests will fail."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_CONNECTION_DISCONNECTING,
                "Connection is disconnecting, it's not safe to do this operation until the connection finishes shutdown."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_CANCELLED_FOR_CLEAN_SESSION,
                "Old requests from the previous session are cancelled, and offline request will not be accept."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT_QUEUE_FULL,
                "MQTT request queue is full."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_CLIENT_OPTIONS_VALIDATION,
                "Invalid mqtt5 client options value."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_CONNECT_OPTIONS_VALIDATION,
                "Invalid mqtt5 connect packet options value."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_DISCONNECT_OPTIONS_VALIDATION,
                "Invalid mqtt5 disconnect packet options value."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_PUBLISH_OPTIONS_VALIDATION,
                "Invalid mqtt5 publish packet options value."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_SUBSCRIBE_OPTIONS_VALIDATION,
                "Invalid mqtt5 subscribe packet options value."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_UNSUBSCRIBE_OPTIONS_VALIDATION,
                "Invalid mqtt5 unsubscribe packet options value."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_USER_PROPERTY_VALIDATION,
                "Invalid mqtt5 user property value."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_PACKET_VALIDATION,
                "General mqtt5 packet validation error"),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_ENCODE_FAILURE,
                "Error occurred while encoding an outgoing mqtt5 packet"),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_DECODE_PROTOCOL_ERROR,
                "Mqtt5 decoder received an invalid packet that broke mqtt5 protocol rules"),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_CONNACK_CONNECTION_REFUSED,
                "Remote endpoint rejected the CONNECT attempt by returning an unsuccessful CONNACK"),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_CONNACK_TIMEOUT,
                "Remote endpoint did not respond to a CONNECT request before timeout exceeded"),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_PING_RESPONSE_TIMEOUT,
                "Remote endpoint did not respond to a PINGREQ before timeout exceeded"),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_USER_REQUESTED_STOP,
                "Mqtt5 client connection interrupted by user request."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_DISCONNECT_RECEIVED,
                "Mqtt5 client connection interrupted by server DISCONNECT."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_CLIENT_TERMINATED,
                "Mqtt5 client terminated by user request."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_OPERATION_FAILED_DUE_TO_OFFLINE_QUEUE_POLICY,
                "Mqtt5 operation failed due to a disconnection event in conjunction with the client's offline queue retention policy."),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_ENCODE_SIZE_UNSUPPORTED_PACKET_TYPE,
                "Unsupported packet type for encode size calculation"),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_OPERATION_PROCESSING_FAILURE,
                "Error while processing mqtt5 operational state"),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_INVALID_INBOUND_TOPIC_ALIAS,
                "Incoming publish contained an invalid (too large or unknown) topic alias"),
            AWS_DEFINE_ERROR_INFO_MQTT(
                AWS_ERROR_MQTT5_INVALID_OUTBOUND_TOPIC_ALIAS,
                "Outgoing publish contained an invalid (too large or unknown) topic alias"),
        };
/* clang-format on */
#undef AWS_DEFINE_ERROR_INFO_MQTT

static struct aws_error_info_list s_error_list = {
    .error_list = s_errors,
    .count = AWS_ARRAY_SIZE(s_errors),
};

/* clang-format off */
        static struct aws_log_subject_info s_logging_subjects[] = {
            DEFINE_LOG_SUBJECT_INFO(AWS_LS_MQTT_GENERAL, "mqtt", "Misc MQTT logging"),
            DEFINE_LOG_SUBJECT_INFO(AWS_LS_MQTT_CLIENT, "mqtt-client", "MQTT client and connections"),
            DEFINE_LOG_SUBJECT_INFO(AWS_LS_MQTT_TOPIC_TREE, "mqtt-topic-tree", "MQTT subscription tree"),
            DEFINE_LOG_SUBJECT_INFO(AWS_LS_MQTT5_GENERAL, "mqtt5-general", "Misc MQTT5 logging"),
            DEFINE_LOG_SUBJECT_INFO(AWS_LS_MQTT5_CLIENT, "mqtt5-client", "MQTT5 client and connections"),
            DEFINE_LOG_SUBJECT_INFO(AWS_LS_MQTT5_CANARY, "mqtt5-canary", "MQTT5 canary logging"),
        };
/* clang-format on */

static struct aws_log_subject_info_list s_logging_subjects_list = {
    .subject_list = s_logging_subjects,
    .count = AWS_ARRAY_SIZE(s_logging_subjects),
};

static bool s_mqtt_library_initialized = false;

void aws_mqtt_library_init(struct aws_allocator *allocator) {

    (void)allocator;

    if (!s_mqtt_library_initialized) {
        s_mqtt_library_initialized = true;
        aws_io_library_init(allocator);
#ifdef AWS_MQTT_WITH_WEBSOCKETS
        aws_http_library_init(allocator);
#endif
        aws_register_error_info(&s_error_list);
        aws_register_log_subject_info_list(&s_logging_subjects_list);
    }
}

void aws_mqtt_library_clean_up(void) {
    if (s_mqtt_library_initialized) {
        s_mqtt_library_initialized = false;
        aws_thread_join_all_managed();
        aws_unregister_error_info(&s_error_list);
        aws_unregister_log_subject_info_list(&s_logging_subjects_list);
#ifdef AWS_MQTT_WITH_WEBSOCKETS
        aws_http_library_clean_up();
#endif
        aws_io_library_clean_up();
    }
}

void aws_mqtt_fatal_assert_library_initialized(void) {
    if (!s_mqtt_library_initialized) {
        AWS_LOGF_FATAL(
            AWS_LS_MQTT_GENERAL,
            "aws_mqtt_library_init() must be called before using any functionality in aws-c-mqtt.");

        AWS_FATAL_ASSERT(s_mqtt_library_initialized);
    }
}
