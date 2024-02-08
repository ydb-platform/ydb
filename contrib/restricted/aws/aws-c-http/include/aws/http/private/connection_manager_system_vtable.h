#ifndef AWS_HTTP_CONNECTION_MANAGER_SYSTEM_VTABLE_H
#define AWS_HTTP_CONNECTION_MANAGER_SYSTEM_VTABLE_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/http/http.h>

#include <aws/http/connection.h>

struct aws_http_connection_manager;

typedef int(aws_http_connection_manager_create_connection_fn)(const struct aws_http_client_connection_options *options);
typedef void(aws_http_connection_manager_close_connection_fn)(struct aws_http_connection *connection);
typedef void(aws_http_connection_release_connection_fn)(struct aws_http_connection *connection);
typedef bool(aws_http_connection_is_connection_available_fn)(const struct aws_http_connection *connection);
typedef bool(aws_http_connection_manager_is_callers_thread_fn)(struct aws_channel *channel);
typedef struct aws_channel *(aws_http_connection_manager_connection_get_channel_fn)(
    struct aws_http_connection *connection);
typedef enum aws_http_version(aws_http_connection_manager_connection_get_version_fn)(
    const struct aws_http_connection *connection);

struct aws_http_connection_manager_system_vtable {
    /*
     * Downstream http functions
     */
    aws_http_connection_manager_create_connection_fn *create_connection;
    aws_http_connection_manager_close_connection_fn *close_connection;
    aws_http_connection_release_connection_fn *release_connection;
    aws_http_connection_is_connection_available_fn *is_connection_available;
    aws_io_clock_fn *get_monotonic_time;
    aws_http_connection_manager_is_callers_thread_fn *is_callers_thread;
    aws_http_connection_manager_connection_get_channel_fn *connection_get_channel;
    aws_http_connection_manager_connection_get_version_fn *connection_get_version;
};

AWS_HTTP_API
bool aws_http_connection_manager_system_vtable_is_valid(const struct aws_http_connection_manager_system_vtable *table);

AWS_HTTP_API
void aws_http_connection_manager_set_system_vtable(
    struct aws_http_connection_manager *manager,
    const struct aws_http_connection_manager_system_vtable *system_vtable);

AWS_HTTP_API
extern const struct aws_http_connection_manager_system_vtable *g_aws_http_connection_manager_default_system_vtable_ptr;

#endif /* AWS_HTTP_CONNECTION_MANAGER_SYSTEM_VTABLE_H */
