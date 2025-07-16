#ifndef AWS_MQTT_PRIVATE_REQUEST_RESPONSE_SUBSCRIPTION_SET_H
#define AWS_MQTT_PRIVATE_REQUEST_RESPONSE_SUBSCRIPTION_SET_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/byte_buf.h>
#include <aws/common/hash_table.h>
#include <aws/common/linked_list.h>
#include <aws/mqtt/exports.h>

struct aws_mqtt_rr_incoming_publish_event;

/*
 * Handles subscriptions for request-response client.
 * Lifetime of this struct is bound to request-response client.
 */
struct aws_request_response_subscriptions {
    struct aws_allocator *allocator;

    /*
     * Map from cursor (topic filter) -> list of streaming operations using that filter
     *
     * We don't garbage collect this table over the course of normal client operation.  We only clean it up
     * when the client is shutting down.
     */
    struct aws_hash_table streaming_operation_subscription_lists;

    /*
     * Map from cursor (topic filter with wildcards) -> list of streaming operations using that filter
     *
     * We don't garbage collect this table over the course of normal client operation.  We only clean it up
     * when the client is shutting down.
     */
    struct aws_hash_table streaming_operation_wildcards_subscription_lists;

    /*
     * Map from cursor (topic) -> request response path (topic, correlation token json path)
     */
    struct aws_hash_table request_response_paths;
};

/*
 * This is the (key and) value in stream subscriptions tables.
 */
struct aws_rr_operation_list_topic_filter_entry {
    struct aws_allocator *allocator;

    struct aws_byte_cursor topic_filter_cursor;
    struct aws_byte_buf topic_filter;

    struct aws_linked_list operations;
};

/*
 * Value in request subscriptions table.
 */
struct aws_rr_response_path_entry {
    struct aws_allocator *allocator;

    size_t ref_count;

    struct aws_byte_cursor topic_cursor;
    struct aws_byte_buf topic;

    struct aws_byte_buf correlation_token_json_path;
};

/*
 * Callback type for matched stream subscriptions.
 */
typedef void(aws_mqtt_stream_operation_subscription_match_fn)(
    const struct aws_linked_list *operations,
    const struct aws_byte_cursor *topic_filter,
    const struct aws_mqtt_rr_incoming_publish_event *publish_event,
    void *user_data);

/*
 * Callback type for matched request subscriptions.
 */
typedef void(aws_mqtt_request_operation_subscription_match_fn)(
    struct aws_rr_response_path_entry *entry,
    const struct aws_mqtt_rr_incoming_publish_event *publish_event,
    void *user_data);

AWS_EXTERN_C_BEGIN

/*
 * Initialize internal state of a provided request-response subscriptions structure.
 */
AWS_MQTT_API int aws_mqtt_request_response_client_subscriptions_init(
    struct aws_request_response_subscriptions *subscriptions,
    struct aws_allocator *allocator);

/*
 * Clean up internals of a provided request-response subscriptions structure.
 */
AWS_MQTT_API void aws_mqtt_request_response_client_subscriptions_clean_up(
    struct aws_request_response_subscriptions *subscriptions);

/*
 * Add a subscription for stream operations.
 * If subscription with the same topic filter is already added, previously created
 * aws_rr_operation_list_topic_filter_entry instance is returned.
 */
AWS_MQTT_API struct aws_rr_operation_list_topic_filter_entry *
    aws_mqtt_request_response_client_subscriptions_add_stream_subscription(
        struct aws_request_response_subscriptions *subscriptions,
        const struct aws_byte_cursor *topic_filter);

/*
 * Add a subscription for request operation.
 */
AWS_MQTT_API int aws_mqtt_request_response_client_subscriptions_add_request_subscription(
    struct aws_request_response_subscriptions *subscriptions,
    const struct aws_byte_cursor *topic_filter,
    const struct aws_byte_cursor *correlation_token_json_path);

/*
 * Remove a subscription for a given request operation.
 */
AWS_MQTT_API int aws_mqtt_request_response_client_subscriptions_remove_request_subscription(
    struct aws_request_response_subscriptions *subscriptions,
    const struct aws_byte_cursor *topic_filter);

/*
 * Call specified callbacks for all stream and request operations with subscriptions matching a provided publish event.
 */
AWS_MQTT_API void aws_mqtt_request_response_client_subscriptions_handle_incoming_publish(
    const struct aws_request_response_subscriptions *subscriptions,
    const struct aws_mqtt_rr_incoming_publish_event *publish_event,
    aws_mqtt_stream_operation_subscription_match_fn *on_stream_operation_subscription_match,
    aws_mqtt_request_operation_subscription_match_fn *on_request_operation_subscription_match,
    void *user_data);

AWS_EXTERN_C_END

#endif /* AWS_MQTT_PRIVATE_REQUEST_RESPONSE_REQUEST_SET_H */
