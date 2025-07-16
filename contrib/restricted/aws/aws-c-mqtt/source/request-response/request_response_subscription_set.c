/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/mqtt/private/request-response/request_response_subscription_set.h>

#include <aws/mqtt/mqtt.h>
#include <aws/mqtt/private/client_impl_shared.h>
#include <aws/mqtt/request-response/request_response_client.h>

#define MQTT_RR_CLIENT_RESPONSE_TABLE_DEFAULT_SIZE 50
#define MQTT_RR_CLIENT_OPERATION_TABLE_DEFAULT_SIZE 50

static void s_aws_rr_operation_list_topic_filter_entry_destroy(struct aws_rr_operation_list_topic_filter_entry *entry) {
    if (entry == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&entry->topic_filter);

    aws_mem_release(entry->allocator, entry);
}

static void s_aws_rr_operation_list_topic_filter_entry_hash_element_destroy(void *value) {
    s_aws_rr_operation_list_topic_filter_entry_destroy(value);
}

static struct aws_rr_response_path_entry *s_aws_rr_response_path_entry_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic,
    struct aws_byte_cursor correlation_token_json_path) {
    struct aws_rr_response_path_entry *entry = aws_mem_calloc(allocator, 1, sizeof(struct aws_rr_response_path_entry));

    entry->allocator = allocator;
    entry->ref_count = 1;
    aws_byte_buf_init_copy_from_cursor(&entry->topic, allocator, topic);
    entry->topic_cursor = aws_byte_cursor_from_buf(&entry->topic);

    aws_byte_buf_init_copy_from_cursor(&entry->correlation_token_json_path, allocator, correlation_token_json_path);

    return entry;
}

static void s_aws_rr_response_path_entry_destroy(struct aws_rr_response_path_entry *entry) {
    if (entry == NULL) {
        return;
    }

    aws_byte_buf_clean_up(&entry->topic);
    aws_byte_buf_clean_up(&entry->correlation_token_json_path);

    aws_mem_release(entry->allocator, entry);
}

static void s_aws_rr_response_path_table_hash_element_destroy(void *value) {
    s_aws_rr_response_path_entry_destroy(value);
}

int aws_mqtt_request_response_client_subscriptions_init(
    struct aws_request_response_subscriptions *subscriptions,
    struct aws_allocator *allocator) {
    AWS_FATAL_ASSERT(subscriptions);

    subscriptions->allocator = allocator;

    if (aws_hash_table_init(
            &subscriptions->streaming_operation_subscription_lists,
            allocator,
            MQTT_RR_CLIENT_OPERATION_TABLE_DEFAULT_SIZE,
            aws_hash_byte_cursor_ptr,
            aws_mqtt_byte_cursor_hash_equality,
            NULL,
            s_aws_rr_operation_list_topic_filter_entry_hash_element_destroy)) {
        goto clean_up;
    }

    if (aws_hash_table_init(
            &subscriptions->streaming_operation_wildcards_subscription_lists,
            allocator,
            MQTT_RR_CLIENT_OPERATION_TABLE_DEFAULT_SIZE,
            aws_hash_byte_cursor_ptr,
            aws_mqtt_byte_cursor_hash_equality,
            NULL,
            s_aws_rr_operation_list_topic_filter_entry_hash_element_destroy)) {
        goto clean_up;
    }

    if (aws_hash_table_init(
            &subscriptions->request_response_paths,
            allocator,
            MQTT_RR_CLIENT_RESPONSE_TABLE_DEFAULT_SIZE,
            aws_hash_byte_cursor_ptr,
            aws_mqtt_byte_cursor_hash_equality,
            NULL,
            s_aws_rr_response_path_table_hash_element_destroy)) {
        goto clean_up;
    }

    return AWS_OP_SUCCESS;

clean_up:
    aws_mqtt_request_response_client_subscriptions_clean_up(subscriptions);
    return AWS_OP_ERR;
}

void aws_mqtt_request_response_client_subscriptions_clean_up(struct aws_request_response_subscriptions *subscriptions) {
    if (subscriptions == NULL) {
        return;
    }

    if (aws_hash_table_is_valid(&subscriptions->streaming_operation_subscription_lists)) {
        aws_hash_table_clean_up(&subscriptions->streaming_operation_subscription_lists);
    }
    if (aws_hash_table_is_valid(&subscriptions->streaming_operation_wildcards_subscription_lists)) {
        aws_hash_table_clean_up(&subscriptions->streaming_operation_wildcards_subscription_lists);
    }
    if (aws_hash_table_is_valid(&subscriptions->request_response_paths)) {
        aws_hash_table_clean_up(&subscriptions->request_response_paths);
    }
}

static struct aws_rr_operation_list_topic_filter_entry *s_aws_rr_operation_list_topic_filter_entry_new(
    struct aws_allocator *allocator,
    struct aws_byte_cursor topic_filter) {
    struct aws_rr_operation_list_topic_filter_entry *entry =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_rr_operation_list_topic_filter_entry));

    entry->allocator = allocator;
    aws_byte_buf_init_copy_from_cursor(&entry->topic_filter, allocator, topic_filter);
    entry->topic_filter_cursor = aws_byte_cursor_from_buf(&entry->topic_filter);

    aws_linked_list_init(&entry->operations);

    return entry;
}

static bool s_is_topic_filter_with_wildcard(const struct aws_byte_cursor *topic_filter) {
    bool res = (memchr(topic_filter->ptr, '+', topic_filter->len) || memchr(topic_filter->ptr, '#', topic_filter->len));
    return res;
}

struct aws_rr_operation_list_topic_filter_entry *aws_mqtt_request_response_client_subscriptions_add_stream_subscription(
    struct aws_request_response_subscriptions *subscriptions,
    const struct aws_byte_cursor *topic_filter) {
    AWS_FATAL_ASSERT(subscriptions);

    struct aws_hash_table *subscription_lists = s_is_topic_filter_with_wildcard(topic_filter)
                                                    ? &subscriptions->streaming_operation_wildcards_subscription_lists
                                                    : &subscriptions->streaming_operation_subscription_lists;

    struct aws_hash_element *element = NULL;
    if (aws_hash_table_find(subscription_lists, topic_filter, &element)) {
        aws_raise_error(AWS_ERROR_MQTT_REQUEST_RESPONSE_INTERNAL_ERROR);
        return NULL;
    }

    struct aws_rr_operation_list_topic_filter_entry *entry = NULL;
    if (element == NULL) {
        entry = s_aws_rr_operation_list_topic_filter_entry_new(subscriptions->allocator, *topic_filter);
        aws_hash_table_put(subscription_lists, &entry->topic_filter_cursor, entry, NULL);
    } else {
        entry = element->value;
    }

    AWS_FATAL_ASSERT(entry != NULL);

    return entry;
}

int aws_mqtt_request_response_client_subscriptions_add_request_subscription(
    struct aws_request_response_subscriptions *subscriptions,
    const struct aws_byte_cursor *topic_filter,
    const struct aws_byte_cursor *correlation_token_json_path) {
    struct aws_hash_element *element = NULL;
    if (aws_hash_table_find(&subscriptions->request_response_paths, topic_filter, &element)) {
        return aws_raise_error(AWS_ERROR_MQTT_REQUEST_RESPONSE_INTERNAL_ERROR);
    }

    if (element != NULL) {
        struct aws_rr_response_path_entry *entry = element->value;
        ++entry->ref_count;
        return AWS_OP_SUCCESS;
    }

    struct aws_rr_response_path_entry *entry =
        s_aws_rr_response_path_entry_new(subscriptions->allocator, *topic_filter, *correlation_token_json_path);
    if (aws_hash_table_put(&subscriptions->request_response_paths, &entry->topic_cursor, entry, NULL)) {
        s_aws_rr_response_path_entry_destroy(entry);
        return aws_raise_error(AWS_ERROR_MQTT_REQUEST_RESPONSE_INTERNAL_ERROR);
    }

    return AWS_OP_SUCCESS;
}

int aws_mqtt_request_response_client_subscriptions_remove_request_subscription(
    struct aws_request_response_subscriptions *subscriptions,
    const struct aws_byte_cursor *topic_filter) {

    AWS_FATAL_ASSERT(subscriptions);
    AWS_FATAL_ASSERT(topic_filter);

    struct aws_hash_element *element = NULL;
    if (aws_hash_table_find(&subscriptions->request_response_paths, topic_filter, &element) || element == NULL) {
        return AWS_OP_ERR;
    }

    struct aws_rr_response_path_entry *entry = element->value;
    --entry->ref_count;

    if (entry->ref_count == 0) {
        aws_hash_table_remove(&subscriptions->request_response_paths, topic_filter, NULL, NULL);
    }

    return AWS_OP_SUCCESS;
}

static void s_match_stream_subscriptions(
    const struct aws_hash_table *subscriptions,
    const struct aws_mqtt_rr_incoming_publish_event *publish_event,
    aws_mqtt_stream_operation_subscription_match_fn *on_stream_operation_subscription_match,
    void *user_data) {
    struct aws_hash_element *subscription_filter_element = NULL;
    if (aws_hash_table_find(subscriptions, &publish_event->topic, &subscription_filter_element) == AWS_OP_SUCCESS &&
        subscription_filter_element != NULL) {

        struct aws_rr_operation_list_topic_filter_entry *entry = subscription_filter_element->value;
        on_stream_operation_subscription_match(
            &entry->operations, &entry->topic_filter_cursor, publish_event, user_data);
    }
}

static bool s_is_topic_matched_to_subscription(
    struct aws_byte_cursor topic,
    struct aws_byte_cursor topic_filter_cursor) {
    bool is_matched = true;
    bool multi_level_wildcard = false;

    struct aws_byte_cursor subscription_topic_filter_segment;
    AWS_ZERO_STRUCT(subscription_topic_filter_segment);

    struct aws_byte_cursor topic_segment;
    AWS_ZERO_STRUCT(topic_segment);

    while (aws_byte_cursor_next_split(&topic_filter_cursor, '/', &subscription_topic_filter_segment)) {
        if (!aws_byte_cursor_next_split(&topic, '/', &topic_segment)) {
            is_matched = false;
            break;
        }

        if (aws_byte_cursor_eq_c_str(&subscription_topic_filter_segment, "#")) {
            multi_level_wildcard = true;
            is_matched = true;
            break;
        }

        if (!aws_byte_cursor_eq_c_str(&subscription_topic_filter_segment, "+") &&
            !aws_byte_cursor_eq(&topic_segment, &subscription_topic_filter_segment)) {
            is_matched = false;
            break;
        }
    }
    if (!multi_level_wildcard && aws_byte_cursor_next_split(&topic, '/', &topic_segment)) {
        is_matched = false;
    }

    return is_matched;
}

static void s_match_wildcard_stream_subscriptions(
    const struct aws_hash_table *subscriptions,
    const struct aws_mqtt_rr_incoming_publish_event *publish_event,
    aws_mqtt_stream_operation_subscription_match_fn *on_stream_operation_subscription_match,
    void *user_data) {

    /*
     * Incoming event's topic is checked against each registered stream with wildcard. While this approach is far from
     * optimal, it should be sufficient for request-response client where not many subscriptions with wildcards are
     * used.
     */
    for (struct aws_hash_iter iter = aws_hash_iter_begin(subscriptions); !aws_hash_iter_done(&iter);
         aws_hash_iter_next(&iter)) {
        struct aws_rr_operation_list_topic_filter_entry *entry = iter.element.value;
        struct aws_byte_cursor topic_filter_cursor = entry->topic_filter_cursor;

        bool is_matched = s_is_topic_matched_to_subscription(publish_event->topic, topic_filter_cursor);
        if (is_matched) {
            on_stream_operation_subscription_match(
                &entry->operations, &entry->topic_filter_cursor, publish_event, user_data);
        }
    }
}

void s_match_request_response_subscriptions(
    const struct aws_hash_table *request_response_paths,
    const struct aws_mqtt_rr_incoming_publish_event *publish_event,
    aws_mqtt_request_operation_subscription_match_fn *on_request_operation_subscription_match,
    void *user_data) {

    struct aws_hash_element *response_path_element = NULL;
    if (aws_hash_table_find(request_response_paths, &publish_event->topic, &response_path_element) == AWS_OP_SUCCESS &&
        response_path_element != NULL) {

        on_request_operation_subscription_match(response_path_element->value, publish_event, user_data);
    }
}

void aws_mqtt_request_response_client_subscriptions_handle_incoming_publish(
    const struct aws_request_response_subscriptions *subscriptions,
    const struct aws_mqtt_rr_incoming_publish_event *publish_event,
    aws_mqtt_stream_operation_subscription_match_fn *on_stream_operation_subscription_match,
    aws_mqtt_request_operation_subscription_match_fn *on_request_operation_subscription_match,
    void *user_data) {

    AWS_FATAL_PRECONDITION(subscriptions);
    AWS_FATAL_PRECONDITION(publish_event);
    AWS_FATAL_PRECONDITION(on_stream_operation_subscription_match);
    AWS_FATAL_PRECONDITION(on_request_operation_subscription_match);

    /* Streaming operation handling */
    s_match_stream_subscriptions(
        &subscriptions->streaming_operation_subscription_lists,
        publish_event,
        on_stream_operation_subscription_match,
        user_data);

    s_match_wildcard_stream_subscriptions(
        &subscriptions->streaming_operation_wildcards_subscription_lists,
        publish_event,
        on_stream_operation_subscription_match,
        user_data);

    /* Request-Response handling */
    s_match_request_response_subscriptions(
        &subscriptions->request_response_paths, publish_event, on_request_operation_subscription_match, user_data);
}
