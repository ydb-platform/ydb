/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#ifndef AWS_MQTT_IOT_METRICS_H
#define AWS_MQTT_IOT_METRICS_H

#include <aws/mqtt/mqtt.h>

/* Storage for `aws_mqtt_iot_metrics`. */
struct aws_mqtt_iot_metrics_storage {
    struct aws_allocator *allocator;

    struct aws_mqtt_iot_metrics storage_view;

    struct aws_byte_cursor library_name;

    struct aws_byte_buf storage;
};

AWS_MQTT_API struct aws_mqtt_iot_metrics_storage *aws_mqtt_iot_metrics_storage_new(
    struct aws_allocator *allocator,
    const struct aws_mqtt_iot_metrics *metrics_options);

AWS_MQTT_API void aws_mqtt_iot_metrics_storage_destroy(struct aws_mqtt_iot_metrics_storage *metrics_storage);

/**
 * Builds a new username by appending SDK metrics to the original username.
 *
 * @param allocator The allocator to use for memory allocation
 * @param original_username The original username
 * @param metrics The metrics configuration
 * @param output_username Buffer that will be initialized and populated with the new final username. The function
 * expects this buffer to be uninitialized. On success, the caller is responsible for cleaning up the buffer.
 * @param out_full_username_size If not NULL, will be set to the full size of the final username
 *
 * @return AWS_OP_SUCCESS on success, AWS_OP_ERR on failure
 */
AWS_MQTT_API
int aws_mqtt_append_sdk_metrics_to_username(
    struct aws_allocator *allocator,
    const struct aws_byte_cursor *original_username,
    const struct aws_mqtt_iot_metrics *metrics,
    struct aws_byte_buf *output_username,
    size_t *out_full_username_size);

/**
 * Validates all string fields in aws_mqtt_iot_metrics
 *
 * @param metrics The metrics structure to validate
 * @return AWS_OP_SUCCESS if metrics is not null and all metrics value are valid, AWS_OP_ERR otherwise
 */
AWS_MQTT_API
int aws_mqtt_validate_iot_metrics(const struct aws_mqtt_iot_metrics *metrics);

#endif /* AWS_MQTT_IOT_METRICS_H */
