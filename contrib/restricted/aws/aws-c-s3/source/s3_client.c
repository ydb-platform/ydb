/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include "aws/s3/private/s3_auto_ranged_get.h"
#include "aws/s3/private/s3_auto_ranged_put.h"
#include "aws/s3/private/s3_client_impl.h"
#include "aws/s3/private/s3_default_meta_request.h"
#include "aws/s3/private/s3_meta_request_impl.h"
#include "aws/s3/private/s3_request_messages.h"
#include "aws/s3/private/s3_util.h"

#include <aws/auth/credentials.h>
#include <aws/common/assert.h>
#include <aws/common/atomics.h>
#include <aws/common/clock.h>
#include <aws/common/device_random.h>
#include <aws/common/environment.h>
#include <aws/common/json.h>
#include <aws/common/string.h>
#include <aws/common/system_info.h>
#include <aws/http/connection.h>
#include <aws/http/connection_manager.h>
#include <aws/http/proxy.h>
#include <aws/http/request_response.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/host_resolver.h>
#include <aws/io/retry_strategy.h>
#include <aws/io/socket.h>
#include <aws/io/stream.h>
#include <aws/io/tls_channel_handler.h>
#include <aws/io/uri.h>

#include <aws/s3/private/s3_copy_object.h>
#include <inttypes.h>
#include <math.h>

#ifdef _MSC_VER
#    pragma warning(disable : 4232) /* function pointer to dll symbol */
#endif                              /* _MSC_VER */

struct aws_s3_meta_request_work {
    struct aws_linked_list_node node;
    struct aws_s3_meta_request *meta_request;
};

static const enum aws_log_level s_log_level_client_stats = AWS_LL_INFO;

static const uint32_t s_max_requests_multiplier = 4;

/* TODO Provide analysis on origins of this value. */
static const double s_throughput_per_vip_gbps = 4.0;

/* Preferred amount of active connections per meta request type. */
const uint32_t g_num_conns_per_vip_meta_request_look_up[AWS_S3_META_REQUEST_TYPE_MAX] = {
    10, /* AWS_S3_META_REQUEST_TYPE_DEFAULT */
    10, /* AWS_S3_META_REQUEST_TYPE_GET_OBJECT */
    10, /* AWS_S3_META_REQUEST_TYPE_PUT_OBJECT */
    10  /* AWS_S3_META_REQUEST_TYPE_COPY_OBJECT */
};

/* Should be max of s_num_conns_per_vip_meta_request_look_up */
const uint32_t g_max_num_connections_per_vip = 10;

/**
 * Default part size is 8 MiB to reach the best performance from the experiments we had.
 * Default max part size is SIZE_MAX on 32bit systems, which is around 4GiB; and 5GiB on a 64bit system.
 *      The server limit is 5GiB, but object size limit is 5TiB for now. We should be good enough for all the cases.
 *      The max number of upload parts is 10000, which limits the object size to 39TiB on 32bit and 49TiB on 64bit.
 * TODO Provide more information on other values.
 */
static const size_t s_default_part_size = 8 * 1024 * 1024;
static const uint64_t s_default_max_part_size = SIZE_MAX < 5368709120ULL ? SIZE_MAX : 5368709120ULL;
static const double s_default_throughput_target_gbps = 10.0;
static const uint32_t s_default_max_retries = 5;
static size_t s_dns_host_address_ttl_seconds = 5 * 60;

/* Default time until a connection is declared dead, while handling a request but seeing no activity.
 * 30 seconds mirrors the value currently used by the Java SDK. */
static const uint32_t s_default_throughput_failure_interval_seconds = 30;

/* Called when ref count is 0. */
static void s_s3_client_start_destroy(void *user_data);

/* Called by s_s3_client_process_work_default when all shutdown criteria has been met. */
static void s_s3_client_finish_destroy_default(struct aws_s3_client *client);

/* Called when the body streaming elg shutdown has completed. */
static void s_s3_client_body_streaming_elg_shutdown(void *user_data);

static void s_s3_client_create_connection_for_request(struct aws_s3_client *client, struct aws_s3_request *request);

/* Callback which handles the HTTP connection retrieved by acquire_http_connection. */
static void s_s3_client_on_acquire_http_connection(
    struct aws_http_connection *http_connection,
    int error_code,
    void *user_data);

static void s_s3_client_push_meta_request_synced(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request);

/* Schedule task for processing work. (Calls the corresponding vtable function.) */
static void s_s3_client_schedule_process_work_synced(struct aws_s3_client *client);

/* Default implementation for scheduling processing of work. */
static void s_s3_client_schedule_process_work_synced_default(struct aws_s3_client *client);

/* Actual task function that processes work. */
static void s_s3_client_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status);

static void s_s3_client_process_work_default(struct aws_s3_client *client);

static void s_s3_client_endpoint_shutdown_callback(struct aws_s3_client *client);

/* Default factory function for creating a meta request. */
static struct aws_s3_meta_request *s_s3_client_meta_request_factory_default(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options);

static struct aws_s3_client_vtable s_s3_client_default_vtable = {
    .meta_request_factory = s_s3_client_meta_request_factory_default,
    .acquire_http_connection = aws_http_connection_manager_acquire_connection,
    .get_host_address_count = aws_host_resolver_get_host_address_count,
    .schedule_process_work_synced = s_s3_client_schedule_process_work_synced_default,
    .process_work = s_s3_client_process_work_default,
    .endpoint_shutdown_callback = s_s3_client_endpoint_shutdown_callback,
    .finish_destroy = s_s3_client_finish_destroy_default,
};

void aws_s3_set_dns_ttl(size_t ttl) {
    s_dns_host_address_ttl_seconds = ttl;
}

/* Returns the max number of connections allowed.
 *
 * When meta request is NULL, this will return the overall allowed number of connections.
 *
 * If meta_request is not NULL, this will give the max number of connections allowed for that meta request type on
 * that endpoint.
 */
uint32_t aws_s3_client_get_max_active_connections(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);

    uint32_t num_connections_per_vip = g_max_num_connections_per_vip;
    uint32_t num_vips = client->ideal_vip_count;

    if (meta_request != NULL) {
        num_connections_per_vip = g_num_conns_per_vip_meta_request_look_up[meta_request->type];

        struct aws_s3_endpoint *endpoint = meta_request->endpoint;
        AWS_ASSERT(endpoint != NULL);

        AWS_ASSERT(client->vtable->get_host_address_count);
        size_t num_known_vips = client->vtable->get_host_address_count(
            client->client_bootstrap->host_resolver, endpoint->host_name, AWS_GET_HOST_ADDRESS_COUNT_RECORD_TYPE_A);

        /* If the number of known vips is less than our ideal VIP count, clamp it. */
        if (num_known_vips < (size_t)num_vips) {
            num_vips = (uint32_t)num_known_vips;
        }
    }

    /* We always want to allow for at least one VIP worth of connections. */
    if (num_vips == 0) {
        num_vips = 1;
    }

    uint32_t max_active_connections = num_vips * num_connections_per_vip;

    if (client->max_active_connections_override > 0 &&
        client->max_active_connections_override < max_active_connections) {
        max_active_connections = client->max_active_connections_override;
    }

    return max_active_connections;
}

/* Returns the max number of requests allowed to be in memory */
uint32_t aws_s3_client_get_max_requests_in_flight(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    return aws_s3_client_get_max_active_connections(client, NULL) * s_max_requests_multiplier;
}

/* Returns the max number of requests that should be in preparation stage (ie: reading from a stream, being signed,
 * etc.) */
uint32_t aws_s3_client_get_max_requests_prepare(struct aws_s3_client *client) {
    return aws_s3_client_get_max_active_connections(client, NULL);
}

static uint32_t s_s3_client_get_num_requests_network_io(
    struct aws_s3_client *client,
    enum aws_s3_meta_request_type meta_request_type) {
    AWS_PRECONDITION(client);

    uint32_t num_requests_network_io = 0;

    if (meta_request_type == AWS_S3_META_REQUEST_TYPE_MAX) {
        for (uint32_t i = 0; i < AWS_S3_META_REQUEST_TYPE_MAX; ++i) {
            num_requests_network_io += (uint32_t)aws_atomic_load_int(&client->stats.num_requests_network_io[i]);
        }
    } else {
        num_requests_network_io =
            (uint32_t)aws_atomic_load_int(&client->stats.num_requests_network_io[meta_request_type]);
    }

    return num_requests_network_io;
}

void aws_s3_client_lock_synced_data(struct aws_s3_client *client) {
    aws_mutex_lock(&client->synced_data.lock);
}

void aws_s3_client_unlock_synced_data(struct aws_s3_client *client) {
    aws_mutex_unlock(&client->synced_data.lock);
}

struct aws_s3_client *aws_s3_client_new(
    struct aws_allocator *allocator,
    const struct aws_s3_client_config *client_config) {

    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(client_config);

    if (client_config->client_bootstrap == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Cannot create client from client_config; client_bootstrap provided in options is invalid.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    /* Cannot be less than zero.  If zero, use default. */
    if (client_config->throughput_target_gbps < 0.0) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Cannot create client from client_config; throughput_target_gbps cannot less than or equal to 0.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

#ifdef BYO_CRYPTO
    if (client_config->tls_mode == AWS_MR_TLS_ENABLED && client_config->tls_connection_options == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "Cannot create client from client_config; when using BYO_CRYPTO, tls_connection_options can not be "
            "NULL when TLS is enabled.");
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }
#endif

    struct aws_s3_client *client = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_client));

    client->allocator = allocator;
    client->vtable = &s_s3_client_default_vtable;

    aws_ref_count_init(&client->ref_count, client, (aws_simple_completion_callback *)s_s3_client_start_destroy);

    if (aws_mutex_init(&client->synced_data.lock) != AWS_OP_SUCCESS) {
        goto lock_init_fail;
    }

    aws_linked_list_init(&client->synced_data.pending_meta_request_work);
    aws_linked_list_init(&client->synced_data.prepared_requests);

    aws_linked_list_init(&client->threaded_data.meta_requests);
    aws_linked_list_init(&client->threaded_data.request_queue);

    aws_atomic_init_int(&client->stats.num_requests_in_flight, 0);

    for (uint32_t i = 0; i < (uint32_t)AWS_S3_META_REQUEST_TYPE_MAX; ++i) {
        aws_atomic_init_int(&client->stats.num_requests_network_io[i], 0);
    }

    aws_atomic_init_int(&client->stats.num_requests_stream_queued_waiting, 0);
    aws_atomic_init_int(&client->stats.num_requests_streaming, 0);

    *((uint32_t *)&client->max_active_connections_override) = client_config->max_active_connections_override;

    /* Store our client bootstrap. */
    client->client_bootstrap = aws_client_bootstrap_acquire(client_config->client_bootstrap);

    struct aws_event_loop_group *event_loop_group = client_config->client_bootstrap->event_loop_group;
    aws_event_loop_group_acquire(event_loop_group);

    client->process_work_event_loop = aws_event_loop_group_get_next_loop(event_loop_group);

    /* Make a copy of the region string. */
    client->region = aws_string_new_from_array(allocator, client_config->region.ptr, client_config->region.len);

    if (client_config->part_size != 0) {
        *((size_t *)&client->part_size) = client_config->part_size;
    } else {
        *((size_t *)&client->part_size) = s_default_part_size;
    }

    if (client_config->max_part_size != 0) {
        *((size_t *)&client->max_part_size) = client_config->max_part_size;
    } else {
        *((size_t *)&client->max_part_size) = (size_t)s_default_max_part_size;
    }

    if (client_config->max_part_size < client_config->part_size) {
        *((size_t *)&client_config->max_part_size) = client_config->part_size;
    }

    client->connect_timeout_ms = client_config->connect_timeout_ms;
    if (client_config->proxy_ev_settings) {
        client->proxy_ev_settings = aws_mem_calloc(allocator, 1, sizeof(struct proxy_env_var_settings));
        *client->proxy_ev_settings = *client_config->proxy_ev_settings;

        if (client_config->proxy_ev_settings->tls_options) {
            client->proxy_ev_tls_options = aws_mem_calloc(allocator, 1, sizeof(struct aws_tls_connection_options));
            if (aws_tls_connection_options_copy(client->proxy_ev_tls_options, client->proxy_ev_settings->tls_options)) {
                goto on_error;
            }
            client->proxy_ev_settings->tls_options = client->proxy_ev_tls_options;
        }
    }

    if (client_config->tcp_keep_alive_options) {
        client->tcp_keep_alive_options = aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_tcp_keep_alive_options));
        *client->tcp_keep_alive_options = *client_config->tcp_keep_alive_options;
    }

    if (client_config->monitoring_options) {
        client->monitoring_options = *client_config->monitoring_options;
    } else {
        client->monitoring_options.minimum_throughput_bytes_per_second = 1;
        client->monitoring_options.allowable_throughput_failure_interval_seconds =
            s_default_throughput_failure_interval_seconds;
    }

    if (client_config->tls_mode == AWS_MR_TLS_ENABLED) {
        client->tls_connection_options =
            aws_mem_calloc(client->allocator, 1, sizeof(struct aws_tls_connection_options));

        if (client_config->tls_connection_options != NULL) {
            aws_tls_connection_options_copy(client->tls_connection_options, client_config->tls_connection_options);
        } else {
#ifdef BYO_CRYPTO
            AWS_FATAL_ASSERT(false);
            goto on_error;
#else
            struct aws_tls_ctx_options default_tls_ctx_options;
            AWS_ZERO_STRUCT(default_tls_ctx_options);

            aws_tls_ctx_options_init_default_client(&default_tls_ctx_options, allocator);

            struct aws_tls_ctx *default_tls_ctx = aws_tls_client_ctx_new(allocator, &default_tls_ctx_options);
            if (default_tls_ctx == NULL) {
                goto on_error;
            }

            aws_tls_connection_options_init_from_ctx(client->tls_connection_options, default_tls_ctx);

            aws_tls_ctx_release(default_tls_ctx);
            aws_tls_ctx_options_clean_up(&default_tls_ctx_options);
#endif
        }
    }

    if (client_config->proxy_options) {
        client->proxy_config = aws_http_proxy_config_new_from_proxy_options_with_tls_info(
            allocator, client_config->proxy_options, client_config->tls_mode == AWS_MR_TLS_ENABLED);
        if (client->proxy_config == NULL) {
            goto on_error;
        }
    }

    /* Set up body streaming ELG */
    {
        uint16_t num_event_loops =
            (uint16_t)aws_array_list_length(&client->client_bootstrap->event_loop_group->event_loops);
        uint16_t num_streaming_threads = num_event_loops;

        if (num_streaming_threads < 1) {
            num_streaming_threads = 1;
        }

        struct aws_shutdown_callback_options body_streaming_elg_shutdown_options = {
            .shutdown_callback_fn = s_s3_client_body_streaming_elg_shutdown,
            .shutdown_callback_user_data = client,
        };

        if (aws_get_cpu_group_count() > 1) {
            client->body_streaming_elg = aws_event_loop_group_new_default_pinned_to_cpu_group(
                client->allocator, num_streaming_threads, 1, &body_streaming_elg_shutdown_options);
        } else {
            client->body_streaming_elg = aws_event_loop_group_new_default(
                client->allocator, num_streaming_threads, &body_streaming_elg_shutdown_options);
        }
        if (!client->body_streaming_elg) {
            /* Fail to create elg, we should fail the call */
            goto on_error;
        }
        client->synced_data.body_streaming_elg_allocated = true;
    }
    /* Setup cannot fail after this point. */

    if (client_config->throughput_target_gbps != 0.0) {
        *((double *)&client->throughput_target_gbps) = client_config->throughput_target_gbps;
    } else {
        *((double *)&client->throughput_target_gbps) = s_default_throughput_target_gbps;
    }

    *((enum aws_s3_meta_request_compute_content_md5 *)&client->compute_content_md5) =
        client_config->compute_content_md5;

    /* Determine how many vips are ideal by dividing target-throughput by throughput-per-vip. */
    {
        double ideal_vip_count_double = client->throughput_target_gbps / s_throughput_per_vip_gbps;
        *((uint32_t *)&client->ideal_vip_count) = (uint32_t)ceil(ideal_vip_count_double);
    }

    if (client_config->signing_config) {
        client->cached_signing_config = aws_cached_signing_config_new(client->allocator, client_config->signing_config);
    }

    client->synced_data.active = true;

    if (client_config->retry_strategy != NULL) {
        aws_retry_strategy_acquire(client_config->retry_strategy);
        client->retry_strategy = client_config->retry_strategy;
    } else {
        struct aws_exponential_backoff_retry_options backoff_retry_options = {
            .el_group = client_config->client_bootstrap->event_loop_group,
            .max_retries = s_default_max_retries,
        };

        struct aws_standard_retry_options retry_options = {
            .backoff_retry_options = backoff_retry_options,
        };

        client->retry_strategy = aws_retry_strategy_new_standard(allocator, &retry_options);
    }

    aws_hash_table_init(
        &client->synced_data.endpoints,
        client->allocator,
        10,
        aws_hash_string,
        aws_hash_callback_string_eq,
        aws_hash_callback_string_destroy,
        NULL);

    /* Initialize shutdown options and tracking. */
    client->shutdown_callback = client_config->shutdown_callback;
    client->shutdown_callback_user_data = client_config->shutdown_callback_user_data;

    *((bool *)&client->enable_read_backpressure) = client_config->enable_read_backpressure;
    *((size_t *)&client->initial_read_window) = client_config->initial_read_window;

    return client;

on_error:
    aws_string_destroy(client->region);

    if (client->tls_connection_options) {
        aws_tls_connection_options_clean_up(client->tls_connection_options);
        aws_mem_release(client->allocator, client->tls_connection_options);
        client->tls_connection_options = NULL;
    }
    if (client->proxy_config) {
        aws_http_proxy_config_destroy(client->proxy_config);
    }
    if (client->proxy_ev_tls_options) {
        aws_tls_connection_options_clean_up(client->proxy_ev_tls_options);
        aws_mem_release(client->allocator, client->proxy_ev_tls_options);
        client->proxy_ev_settings->tls_options = NULL;
    }
    aws_mem_release(client->allocator, client->proxy_ev_settings);
    aws_mem_release(client->allocator, client->tcp_keep_alive_options);

    aws_event_loop_group_release(client->client_bootstrap->event_loop_group);
    aws_client_bootstrap_release(client->client_bootstrap);
    aws_mutex_clean_up(&client->synced_data.lock);
lock_init_fail:
    aws_mem_release(client->allocator, client);
    return NULL;
}

struct aws_s3_client *aws_s3_client_acquire(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    aws_ref_count_acquire(&client->ref_count);
    return client;
}

struct aws_s3_client *aws_s3_client_release(struct aws_s3_client *client) {
    if (client != NULL) {
        aws_ref_count_release(&client->ref_count);
    }

    return NULL;
}

static void s_s3_client_start_destroy(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client starting destruction.", (void *)client);

    struct aws_linked_list local_vip_list;
    aws_linked_list_init(&local_vip_list);

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_client_lock_synced_data(client);

        client->synced_data.active = false;

        /* Prevent the client from cleaning up in between the mutex unlock/re-lock below.*/
        client->synced_data.start_destroy_executing = true;

        aws_s3_client_unlock_synced_data(client);
    }
    /* END CRITICAL SECTION */

    aws_event_loop_group_release(client->body_streaming_elg);
    client->body_streaming_elg = NULL;

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_client_lock_synced_data(client);
        client->synced_data.start_destroy_executing = false;

        /* Schedule the work task to clean up outstanding connections and to call s_s3_client_finish_destroy function if
         * everything cleaning up asynchronously has finished.  */
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
    /* END CRITICAL SECTION */
}

static void s_s3_client_finish_destroy_default(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client finishing destruction.", (void *)client);

    aws_string_destroy(client->region);
    client->region = NULL;

    if (client->tls_connection_options) {
        aws_tls_connection_options_clean_up(client->tls_connection_options);
        aws_mem_release(client->allocator, client->tls_connection_options);
        client->tls_connection_options = NULL;
    }

    if (client->proxy_config) {
        aws_http_proxy_config_destroy(client->proxy_config);
    }

    if (client->proxy_ev_tls_options) {
        aws_tls_connection_options_clean_up(client->proxy_ev_tls_options);
        aws_mem_release(client->allocator, client->proxy_ev_tls_options);
        client->proxy_ev_settings->tls_options = NULL;
    }
    aws_mem_release(client->allocator, client->proxy_ev_settings);
    aws_mem_release(client->allocator, client->tcp_keep_alive_options);

    aws_mutex_clean_up(&client->synced_data.lock);

    AWS_ASSERT(aws_linked_list_empty(&client->synced_data.pending_meta_request_work));
    AWS_ASSERT(aws_linked_list_empty(&client->threaded_data.meta_requests));
    aws_hash_table_clean_up(&client->synced_data.endpoints);

    aws_retry_strategy_release(client->retry_strategy);

    aws_event_loop_group_release(client->client_bootstrap->event_loop_group);

    aws_client_bootstrap_release(client->client_bootstrap);
    aws_cached_signing_config_destroy(client->cached_signing_config);

    aws_s3_client_shutdown_complete_callback_fn *shutdown_callback = client->shutdown_callback;
    void *shutdown_user_data = client->shutdown_callback_user_data;

    aws_mem_release(client->allocator, client);
    client = NULL;

    if (shutdown_callback != NULL) {
        shutdown_callback(shutdown_user_data);
    }
}

static void s_s3_client_body_streaming_elg_shutdown(void *user_data) {
    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Client body streaming ELG shutdown.", (void *)client);

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_client_lock_synced_data(client);
        client->synced_data.body_streaming_elg_allocated = false;
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
    /* END CRITICAL SECTION */
}

uint32_t aws_s3_client_queue_requests_threaded(
    struct aws_s3_client *client,
    struct aws_linked_list *request_list,
    bool queue_front) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request_list);

    uint32_t request_list_size = 0;

    for (struct aws_linked_list_node *node = aws_linked_list_begin(request_list);
         node != aws_linked_list_end(request_list);
         node = aws_linked_list_next(node)) {
        ++request_list_size;
    }

    if (queue_front) {
        aws_linked_list_move_all_front(&client->threaded_data.request_queue, request_list);
    } else {
        aws_linked_list_move_all_back(&client->threaded_data.request_queue, request_list);
    }

    client->threaded_data.request_queue_size += request_list_size;
    return request_list_size;
}

struct aws_s3_request *aws_s3_client_dequeue_request_threaded(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    if (aws_linked_list_empty(&client->threaded_data.request_queue)) {
        return NULL;
    }

    struct aws_linked_list_node *request_node = aws_linked_list_pop_front(&client->threaded_data.request_queue);
    struct aws_s3_request *request = AWS_CONTAINER_OF(request_node, struct aws_s3_request, node);

    --client->threaded_data.request_queue_size;

    return request;
}

/*
 * There is currently some overlap between user provided Host header and endpoint
 * override. This function handles the corner cases for when either or both are provided.
 */
int s_apply_endpoint_override(
    const struct aws_s3_client *client,
    struct aws_http_headers *message_headers,
    const struct aws_uri *endpoint) {
    AWS_PRECONDITION(message_headers);

    const struct aws_byte_cursor *endpoint_authority = endpoint == NULL ? NULL : aws_uri_authority(endpoint);

    if (!aws_http_headers_has(message_headers, g_host_header_name)) {
        if (endpoint_authority == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Cannot create meta s3 request; message provided in options does not have either 'Host' header "
                "set or endpoint override.",
                (void *)client);
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }

        if (aws_http_headers_set(message_headers, g_host_header_name, *endpoint_authority)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Cannot create meta s3 request; failed to set 'Host' header based on endpoint override.",
                (void *)client);
            return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        }
    }

    struct aws_byte_cursor host_value;
    AWS_FATAL_ASSERT(aws_http_headers_get(message_headers, g_host_header_name, &host_value) == AWS_OP_SUCCESS);

    if (endpoint_authority != NULL && !aws_byte_cursor_eq(&host_value, endpoint_authority)) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Cannot create meta s3 request; host header value " PRInSTR
            " does not match endpoint override " PRInSTR,
            (void *)client,
            AWS_BYTE_CURSOR_PRI(host_value),
            AWS_BYTE_CURSOR_PRI(*endpoint_authority));
        return aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
    }

    return AWS_OP_SUCCESS;
}

/* Public facing make-meta-request function. */
struct aws_s3_meta_request *aws_s3_client_make_meta_request(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {

    AWS_LOGF_INFO(AWS_LS_S3_CLIENT, "id=%p Initiating making of meta request", (void *)client);

    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->meta_request_factory);
    AWS_PRECONDITION(options);

    if (options->type >= AWS_S3_META_REQUEST_TYPE_MAX) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Cannot create meta s3 request; invalid meta request type specified.",
            (void *)client);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (options->message == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Cannot create meta s3 request; message provided in options is invalid.",
            (void *)client);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    struct aws_http_headers *message_headers = aws_http_message_get_headers(options->message);

    if (message_headers == NULL) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Cannot create meta s3 request; message provided in options does not contain headers.",
            (void *)client);
        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
        return NULL;
    }

    if (options->checksum_config) {
        if (options->checksum_config->location == AWS_SCL_TRAILER) {
            struct aws_http_headers *headers = aws_http_message_get_headers(options->message);
            struct aws_byte_cursor existing_encoding;
            AWS_ZERO_STRUCT(existing_encoding);
            if (aws_http_headers_get(headers, g_content_encoding_header_name, &existing_encoding) == AWS_OP_SUCCESS) {
                if (aws_byte_cursor_find_exact(&existing_encoding, &g_content_encoding_header_aws_chunked, NULL) ==
                    AWS_OP_SUCCESS) {
                    AWS_LOGF_ERROR(
                        AWS_LS_S3_CLIENT,
                        "id=%p Cannot create meta s3 request; for trailer checksum, the original request cannot be "
                        "aws-chunked encoding. The client will encode the request instead.",
                        (void *)client);
                    aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                    return NULL;
                }
            }
        }

        if (options->checksum_config->location == AWS_SCL_HEADER) {
            /* TODO: support calculate checksum to add to header */
            aws_raise_error(AWS_ERROR_UNSUPPORTED_OPERATION);
            return NULL;
        }

        if (options->checksum_config->location != AWS_SCL_NONE &&
            options->checksum_config->checksum_algorithm == AWS_SCA_NONE) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Cannot create meta s3 request; checksum algorithm must be set to calculate checksum.",
                (void *)client);
            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            return NULL;
        }
        if (options->checksum_config->checksum_algorithm != AWS_SCA_NONE &&
            options->checksum_config->location == AWS_SCL_NONE) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Cannot create meta s3 request; checksum algorithm cannot be set if not calculate checksum from "
                "client.",
                (void *)client);
            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            return NULL;
        }
    }

    if (s_apply_endpoint_override(client, message_headers, options->endpoint)) {
        return NULL;
    }

    struct aws_byte_cursor host_header_value;
    /* The Host header must be set from s_apply_endpoint_override, if not errored out */
    AWS_FATAL_ASSERT(aws_http_headers_get(message_headers, g_host_header_name, &host_header_value) == AWS_OP_SUCCESS);

    bool is_https = true;
    uint16_t port = 0;

    if (options->endpoint != NULL) {
        struct aws_byte_cursor https_scheme = aws_byte_cursor_from_c_str("https");
        struct aws_byte_cursor http_scheme = aws_byte_cursor_from_c_str("http");

        const struct aws_byte_cursor *scheme = aws_uri_scheme(options->endpoint);

        is_https = aws_byte_cursor_eq_ignore_case(scheme, &https_scheme);

        if (!is_https && !aws_byte_cursor_eq_ignore_case(scheme, &http_scheme)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Cannot create meta s3 request; unexpected scheme '" PRInSTR "' in endpoint override.",
                (void *)client,
                AWS_BYTE_CURSOR_PRI(*scheme));
            aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
            return NULL;
        }

        port = aws_uri_port(options->endpoint);
    }

    struct aws_s3_meta_request *meta_request = client->vtable->meta_request_factory(client, options);

    if (meta_request == NULL) {
        AWS_LOGF_ERROR(AWS_LS_S3_CLIENT, "id=%p: Could not create new meta request.", (void *)client);
        return NULL;
    }

    bool error_occurred = false;

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_client_lock_synced_data(client);

        struct aws_string *endpoint_host_name = NULL;

        if (options->endpoint != NULL) {
            endpoint_host_name = aws_string_new_from_cursor(client->allocator, aws_uri_host_name(options->endpoint));
        } else {
            struct aws_uri host_uri;
            if (aws_uri_init_parse(&host_uri, client->allocator, &host_header_value)) {
                error_occurred = true;
                goto unlock;
            }

            endpoint_host_name = aws_string_new_from_cursor(client->allocator, aws_uri_host_name(&host_uri));
            aws_uri_clean_up(&host_uri);
        }

        struct aws_s3_endpoint *endpoint = NULL;
        struct aws_hash_element *endpoint_hash_element = NULL;

        int was_created = 0;
        if (aws_hash_table_create(
                &client->synced_data.endpoints, endpoint_host_name, &endpoint_hash_element, &was_created)) {
            aws_string_destroy(endpoint_host_name);
            error_occurred = true;
            goto unlock;
        }

        if (was_created) {
            struct aws_s3_endpoint_options endpoint_options = {
                .host_name = endpoint_host_name,
                .client_bootstrap = client->client_bootstrap,
                .tls_connection_options = is_https ? client->tls_connection_options : NULL,
                .dns_host_address_ttl_seconds = s_dns_host_address_ttl_seconds,
                .client = client,
                .max_connections = aws_s3_client_get_max_active_connections(client, NULL),
                .port = port,
                .proxy_config = client->proxy_config,
                .proxy_ev_settings = client->proxy_ev_settings,
                .connect_timeout_ms = client->connect_timeout_ms,
                .tcp_keep_alive_options = client->tcp_keep_alive_options,
                .monitoring_options = &client->monitoring_options,
            };

            endpoint = aws_s3_endpoint_new(client->allocator, &endpoint_options);

            if (endpoint == NULL) {
                aws_hash_table_remove(&client->synced_data.endpoints, endpoint_host_name, NULL, NULL);
                aws_string_destroy(endpoint_host_name);
                error_occurred = true;
                goto unlock;
            }
            endpoint_hash_element->value = endpoint;
            ++client->synced_data.num_endpoints_allocated;
        } else {
            endpoint = endpoint_hash_element->value;

            aws_s3_endpoint_acquire(endpoint, true /*already_holding_lock*/);

            aws_string_destroy(endpoint_host_name);
            endpoint_host_name = NULL;
        }

        meta_request->endpoint = endpoint;

        s_s3_client_push_meta_request_synced(client, meta_request);
        s_s3_client_schedule_process_work_synced(client);

    unlock:
        aws_s3_client_unlock_synced_data(client);
    }
    /* END CRITICAL SECTION */

    if (error_occurred) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Could not create meta request due to error %d (%s)",
            (void *)client,
            aws_last_error(),
            aws_error_str(aws_last_error()));

        meta_request = aws_s3_meta_request_release(meta_request);
    } else {
        AWS_LOGF_INFO(AWS_LS_S3_CLIENT, "id=%p: Created meta request %p", (void *)client, (void *)meta_request);
    }

    return meta_request;
}

static void s_s3_client_endpoint_shutdown_callback(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_client_lock_synced_data(client);
        --client->synced_data.num_endpoints_allocated;
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
    /* END CRITICAL SECTION */
}

static struct aws_s3_meta_request *s_s3_client_meta_request_factory_default(
    struct aws_s3_client *client,
    const struct aws_s3_meta_request_options *options) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(options);

    struct aws_http_headers *initial_message_headers = aws_http_message_get_headers(options->message);
    AWS_ASSERT(initial_message_headers);

    uint64_t content_length = 0;
    struct aws_byte_cursor content_length_cursor;
    bool content_length_header_found = false;

    if (!aws_http_headers_get(initial_message_headers, g_content_length_header_name, &content_length_cursor)) {
        if (aws_byte_cursor_utf8_parse_u64(content_length_cursor, &content_length)) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_META_REQUEST,
                "Could not parse Content-Length header. header value is:" PRInSTR "",
                AWS_BYTE_CURSOR_PRI(content_length_cursor));
            aws_raise_error(AWS_ERROR_S3_INVALID_CONTENT_LENGTH_HEADER);
            return NULL;
        }
        content_length_header_found = true;
    }

    /* Call the appropriate meta-request new function. */
    switch (options->type) {
        case AWS_S3_META_REQUEST_TYPE_GET_OBJECT: {
            /* If the initial request already has partNumber, the request is not
             * splittable(?). Treat it as a Default request.
             * TODO: Still need tests to verify that the request of a part is
             * splittable or not */
            if (aws_http_headers_has(initial_message_headers, aws_byte_cursor_from_c_str("partNumber"))) {
                return aws_s3_meta_request_default_new(client->allocator, client, content_length, false, options);
            }

            return aws_s3_meta_request_auto_ranged_get_new(client->allocator, client, client->part_size, options);
        }
        case AWS_S3_META_REQUEST_TYPE_PUT_OBJECT: {

            if (!content_length_header_found) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "Could not create auto-ranged-put meta request; there is no Content-Length header present.");
                aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                return NULL;
            }

            struct aws_input_stream *input_stream = aws_http_message_get_body_stream(options->message);

            if ((input_stream == NULL) && (options->send_filepath.len == 0)) {
                AWS_LOGF_ERROR(
                    AWS_LS_S3_META_REQUEST,
                    "Could not create auto-ranged-put meta request; filepath or body stream must be set.");
                aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                return NULL;
            }

            if (options->resume_token == NULL) {

                size_t client_part_size = client->part_size;
                size_t client_max_part_size = client->max_part_size;

                if (client_part_size < g_s3_min_upload_part_size) {
                    AWS_LOGF_WARN(
                        AWS_LS_S3_META_REQUEST,
                        "Client config part size of %" PRIu64 " is less than the minimum upload part size of %" PRIu64
                        ". Using to the minimum part-size for upload.",
                        (uint64_t)client_part_size,
                        (uint64_t)g_s3_min_upload_part_size);

                    client_part_size = g_s3_min_upload_part_size;
                }

                if (client_max_part_size < g_s3_min_upload_part_size) {
                    AWS_LOGF_WARN(
                        AWS_LS_S3_META_REQUEST,
                        "Client config max part size of %" PRIu64
                        " is less than the minimum upload part size of %" PRIu64
                        ". Clamping to the minimum part-size for upload.",
                        (uint64_t)client_max_part_size,
                        (uint64_t)g_s3_min_upload_part_size);

                    client_max_part_size = g_s3_min_upload_part_size;
                }
                if (content_length <= client_part_size) {
                    return aws_s3_meta_request_default_new(
                        client->allocator,
                        client,
                        content_length,
                        client->compute_content_md5 == AWS_MR_CONTENT_MD5_ENABLED &&
                            !aws_http_headers_has(initial_message_headers, g_content_md5_header_name),
                        options);
                } else {
                    if (aws_s3_message_util_check_checksum_header(options->message)) {
                        /* The checksum header has been set and the request will be splitted. We fail the request */
                        AWS_LOGF_ERROR(
                            AWS_LS_S3_META_REQUEST,
                            "Could not create auto-ranged-put meta request; checksum headers has been set for "
                            "auto-ranged-put that will be split. Pre-calculated checksums are only supported for "
                            "single "
                            "part upload.");
                        aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                        return NULL;
                    }
                }

                uint64_t part_size_uint64 = content_length / (uint64_t)g_s3_max_num_upload_parts;

                if (part_size_uint64 > SIZE_MAX) {
                    AWS_LOGF_ERROR(
                        AWS_LS_S3_META_REQUEST,
                        "Could not create auto-ranged-put meta request; required part size of %" PRIu64
                        " bytes is too large for platform.",
                        part_size_uint64);

                    aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                    return NULL;
                }

                size_t part_size = (size_t)part_size_uint64;

                if (part_size > client_max_part_size) {
                    AWS_LOGF_ERROR(
                        AWS_LS_S3_META_REQUEST,
                        "Could not create auto-ranged-put meta request; required part size for put request is %" PRIu64
                        ", but current maximum part size is %" PRIu64,
                        (uint64_t)part_size,
                        (uint64_t)client_max_part_size);
                    aws_raise_error(AWS_ERROR_INVALID_ARGUMENT);
                    return NULL;
                }

                if (part_size < client_part_size) {
                    part_size = client_part_size;
                }

                uint32_t num_parts = (uint32_t)(content_length / part_size);

                if ((content_length % part_size) > 0) {
                    ++num_parts;
                }

                return aws_s3_meta_request_auto_ranged_put_new(
                    client->allocator, client, part_size, content_length, num_parts, options);
            } else {
                /* dont pass part size and total num parts. constructor will pick it up from token */
                return aws_s3_meta_request_auto_ranged_put_new(
                    client->allocator, client, 0, content_length, 0, options);
            }
        }
        case AWS_S3_META_REQUEST_TYPE_COPY_OBJECT: {
            /* TODO: support copy object correctly. */
            AWS_LOGF_ERROR(AWS_LS_S3_META_REQUEST, "CopyObject is not currently supported");
            aws_raise_error(AWS_ERROR_UNIMPLEMENTED);
            return NULL;
        }
        case AWS_S3_META_REQUEST_TYPE_DEFAULT:
            return aws_s3_meta_request_default_new(client->allocator, client, content_length, false, options);
        default:
            AWS_FATAL_ASSERT(false);
    }

    return NULL;
}

static void s_s3_client_push_meta_request_synced(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    struct aws_s3_meta_request_work *meta_request_work =
        aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_meta_request_work));

    aws_s3_meta_request_acquire(meta_request);
    meta_request_work->meta_request = meta_request;
    aws_linked_list_push_back(&client->synced_data.pending_meta_request_work, &meta_request_work->node);
}

static void s_s3_client_schedule_process_work_synced(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->schedule_process_work_synced);

    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    client->vtable->schedule_process_work_synced(client);
}

static void s_s3_client_schedule_process_work_synced_default(struct aws_s3_client *client) {
    ASSERT_SYNCED_DATA_LOCK_HELD(client);

    if (client->synced_data.process_work_task_scheduled) {
        return;
    }

    aws_task_init(
        &client->synced_data.process_work_task, s_s3_client_process_work_task, client, "s3_client_process_work_task");

    aws_event_loop_schedule_task_now(client->process_work_event_loop, &client->synced_data.process_work_task);

    client->synced_data.process_work_task_scheduled = true;
}

void aws_s3_client_schedule_process_work(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_client_lock_synced_data(client);
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
    /* END CRITICAL SECTION */
}

static void s_s3_client_remove_meta_request_threaded(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(meta_request);
    (void)client;

    aws_linked_list_remove(&meta_request->client_process_work_threaded_data.node);
    meta_request->client_process_work_threaded_data.scheduled = false;
    aws_s3_meta_request_release(meta_request);
}

/* Task function for trying to find a request that can be processed. */
static void s_s3_client_process_work_task(struct aws_task *task, void *arg, enum aws_task_status task_status) {
    AWS_PRECONDITION(task);
    (void)task;
    (void)task_status;

    /* Client keeps a reference to the event loop group; a 'canceled' status should not happen.*/
    AWS_ASSERT(task_status == AWS_TASK_STATUS_RUN_READY);

    struct aws_s3_client *client = arg;
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->process_work);

    client->vtable->process_work(client);
}

static void s_s3_client_process_work_default(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);
    AWS_PRECONDITION(client->vtable->finish_destroy);

    struct aws_linked_list meta_request_work_list;
    aws_linked_list_init(&meta_request_work_list);

    /*******************/
    /* Step 1: Move relevant data into thread local memory. */
    /*******************/
    AWS_LOGF_DEBUG(
        AWS_LS_S3_CLIENT,
        "id=%p s_s3_client_process_work_default - Moving relevant synced_data into threaded_data.",
        (void *)client);

    /* BEGIN CRITICAL SECTION */
    aws_s3_client_lock_synced_data(client);
    /* Once we exit this mutex, someone can reschedule this task. */
    client->synced_data.process_work_task_scheduled = false;
    client->synced_data.process_work_task_in_progress = true;

    aws_linked_list_swap_contents(&meta_request_work_list, &client->synced_data.pending_meta_request_work);

    uint32_t num_requests_queued =
        aws_s3_client_queue_requests_threaded(client, &client->synced_data.prepared_requests, false);

    {
        int sub_result = aws_sub_u32_checked(
            client->threaded_data.num_requests_being_prepared,
            num_requests_queued,
            &client->threaded_data.num_requests_being_prepared);

        AWS_ASSERT(sub_result == AWS_OP_SUCCESS);
        (void)sub_result;
    }

    {
        int sub_result = aws_sub_u32_checked(
            client->threaded_data.num_requests_being_prepared,
            client->synced_data.num_failed_prepare_requests,
            &client->threaded_data.num_requests_being_prepared);

        client->synced_data.num_failed_prepare_requests = 0;

        AWS_ASSERT(sub_result == AWS_OP_SUCCESS);
        (void)sub_result;
    }

    uint32_t num_endpoints_in_table = (uint32_t)aws_hash_table_get_entry_count(&client->synced_data.endpoints);
    uint32_t num_endpoints_allocated = client->synced_data.num_endpoints_allocated;

    aws_s3_client_unlock_synced_data(client);
    /* END CRITICAL SECTION */

    /*******************/
    /* Step 2: Push meta requests into the thread local list if they haven't already been scheduled. */
    /*******************/
    AWS_LOGF_DEBUG(
        AWS_LS_S3_CLIENT, "id=%p s_s3_client_process_work_default - Processing any new meta requests.", (void *)client);

    while (!aws_linked_list_empty(&meta_request_work_list)) {
        struct aws_linked_list_node *node = aws_linked_list_pop_back(&meta_request_work_list);
        struct aws_s3_meta_request_work *meta_request_work =
            AWS_CONTAINER_OF(node, struct aws_s3_meta_request_work, node);

        AWS_FATAL_ASSERT(meta_request_work != NULL);
        AWS_FATAL_ASSERT(meta_request_work->meta_request != NULL);

        struct aws_s3_meta_request *meta_request = meta_request_work->meta_request;

        if (!meta_request->client_process_work_threaded_data.scheduled) {
            aws_linked_list_push_back(
                &client->threaded_data.meta_requests, &meta_request->client_process_work_threaded_data.node);

            meta_request->client_process_work_threaded_data.scheduled = true;
        } else {
            meta_request = aws_s3_meta_request_release(meta_request);
        }

        aws_mem_release(client->allocator, meta_request_work);
    }

    /*******************/
    /* Step 3: Update relevant meta requests and connections. */
    /*******************/
    {
        AWS_LOGF_DEBUG(AWS_LS_S3_CLIENT, "id=%p Updating meta requests.", (void *)client);
        aws_s3_client_update_meta_requests_threaded(client);

        AWS_LOGF_DEBUG(
            AWS_LS_S3_CLIENT, "id=%p Updating connections, assigning requests where possible.", (void *)client);
        aws_s3_client_update_connections_threaded(client);
    }

    /*******************/
    /* Step 4: Log client stats. */
    /*******************/
    {
        uint32_t num_requests_tracked_requests = (uint32_t)aws_atomic_load_int(&client->stats.num_requests_in_flight);

        uint32_t num_auto_ranged_get_network_io =
            s_s3_client_get_num_requests_network_io(client, AWS_S3_META_REQUEST_TYPE_GET_OBJECT);
        uint32_t num_auto_ranged_put_network_io =
            s_s3_client_get_num_requests_network_io(client, AWS_S3_META_REQUEST_TYPE_PUT_OBJECT);
        uint32_t num_auto_default_network_io =
            s_s3_client_get_num_requests_network_io(client, AWS_S3_META_REQUEST_TYPE_DEFAULT);

        uint32_t num_requests_network_io =
            s_s3_client_get_num_requests_network_io(client, AWS_S3_META_REQUEST_TYPE_MAX);

        uint32_t num_requests_stream_queued_waiting =
            (uint32_t)aws_atomic_load_int(&client->stats.num_requests_stream_queued_waiting);
        uint32_t num_requests_streaming = (uint32_t)aws_atomic_load_int(&client->stats.num_requests_streaming);

        uint32_t total_approx_requests = num_requests_network_io + num_requests_stream_queued_waiting +
                                         num_requests_streaming + client->threaded_data.num_requests_being_prepared +
                                         client->threaded_data.request_queue_size;
        AWS_LOGF(
            s_log_level_client_stats,
            AWS_LS_S3_CLIENT_STATS,
            "id=%p Requests-in-flight(approx/exact):%d/%d  Requests-preparing:%d  Requests-queued:%d  "
            "Requests-network(get/put/default/total):%d/%d/%d/%d  Requests-streaming-waiting:%d  Requests-streaming:%d "
            " Endpoints(in-table/allocated):%d/%d",
            (void *)client,
            total_approx_requests,
            num_requests_tracked_requests,
            client->threaded_data.num_requests_being_prepared,
            client->threaded_data.request_queue_size,
            num_auto_ranged_get_network_io,
            num_auto_ranged_put_network_io,
            num_auto_default_network_io,
            num_requests_network_io,
            num_requests_stream_queued_waiting,
            num_requests_streaming,
            num_endpoints_in_table,
            num_endpoints_allocated);
    }

    /*******************/
    /* Step 5: Check for client shutdown. */
    /*******************/
    {
        /* BEGIN CRITICAL SECTION */
        aws_s3_client_lock_synced_data(client);
        client->synced_data.process_work_task_in_progress = false;

        /* This flag should never be set twice. If it was, that means a double-free could occur.*/
        AWS_ASSERT(!client->synced_data.finish_destroy);

        bool finish_destroy = client->synced_data.active == false &&
                              client->synced_data.start_destroy_executing == false &&
                              client->synced_data.body_streaming_elg_allocated == false &&
                              client->synced_data.process_work_task_scheduled == false &&
                              client->synced_data.process_work_task_in_progress == false &&
                              client->synced_data.num_endpoints_allocated == 0;

        client->synced_data.finish_destroy = finish_destroy;

        if (!client->synced_data.active) {
            AWS_LOGF_DEBUG(
                AWS_LS_S3_CLIENT,
                "id=%p Client shutdown progress: starting_destroy_executing=%d  body_streaming_elg_allocated=%d  "
                "process_work_task_scheduled=%d  process_work_task_in_progress=%d  num_endpoints_allocated=%d "
                "finish_destroy=%d",
                (void *)client,
                (int)client->synced_data.start_destroy_executing,
                (int)client->synced_data.body_streaming_elg_allocated,
                (int)client->synced_data.process_work_task_scheduled,
                (int)client->synced_data.process_work_task_in_progress,
                (int)client->synced_data.num_endpoints_allocated,
                (int)client->synced_data.finish_destroy);
        }

        aws_s3_client_unlock_synced_data(client);
        /* END CRITICAL SECTION */

        if (finish_destroy) {
            client->vtable->finish_destroy(client);
        }
    }
}

static void s_s3_client_prepare_callback_queue_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data);

void aws_s3_client_update_meta_requests_threaded(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);

    const uint32_t max_requests_in_flight = aws_s3_client_get_max_requests_in_flight(client);
    const uint32_t max_requests_prepare = aws_s3_client_get_max_requests_prepare(client);

    struct aws_linked_list meta_requests_work_remaining;
    aws_linked_list_init(&meta_requests_work_remaining);

    uint32_t num_requests_in_flight = (uint32_t)aws_atomic_load_int(&client->stats.num_requests_in_flight);

    const uint32_t pass_flags[] = {
        AWS_S3_META_REQUEST_UPDATE_FLAG_CONSERVATIVE,
        0,
    };

    const uint32_t num_passes = AWS_ARRAY_SIZE(pass_flags);

    for (uint32_t pass_index = 0; pass_index < num_passes; ++pass_index) {

        /* While:
         *     * Number of being-prepared + already-prepared-and-queued requests is less than the max that can be in the
         * preparation stage.
         *     * Total number of requests tracked by the client is less than the max tracked ("in flight") requests.
         *     * There are meta requests to get requests from.
         *
         * Then update meta requests to get new requests that can then be prepared (reading from any streams, signing,
         * etc.) for sending.
         */
        while ((client->threaded_data.num_requests_being_prepared + client->threaded_data.request_queue_size) <
                   max_requests_prepare &&
               num_requests_in_flight < max_requests_in_flight &&
               !aws_linked_list_empty(&client->threaded_data.meta_requests)) {

            struct aws_linked_list_node *meta_request_node =
                aws_linked_list_begin(&client->threaded_data.meta_requests);
            struct aws_s3_meta_request *meta_request =
                AWS_CONTAINER_OF(meta_request_node, struct aws_s3_meta_request, client_process_work_threaded_data);

            struct aws_s3_endpoint *endpoint = meta_request->endpoint;
            AWS_ASSERT(endpoint != NULL);

            AWS_ASSERT(client->vtable->get_host_address_count);
            size_t num_known_vips = client->vtable->get_host_address_count(
                client->client_bootstrap->host_resolver, endpoint->host_name, AWS_GET_HOST_ADDRESS_COUNT_RECORD_TYPE_A);

            /* If this particular endpoint doesn't have any known addresses yet, then we don't want to go full speed in
             * ramping up requests just yet. If there is already enough in the queue for one address (even if those
             * aren't for this particular endpoint) we skip over this meta request for now. */
            if (num_known_vips == 0 && (client->threaded_data.num_requests_being_prepared +
                                        client->threaded_data.request_queue_size) >= g_max_num_connections_per_vip) {
                aws_linked_list_remove(&meta_request->client_process_work_threaded_data.node);
                aws_linked_list_push_back(
                    &meta_requests_work_remaining, &meta_request->client_process_work_threaded_data.node);
                continue;
            }

            struct aws_s3_request *request = NULL;

            /* Try to grab the next request from the meta request. */
            bool work_remaining = aws_s3_meta_request_update(meta_request, pass_flags[pass_index], &request);

            if (work_remaining) {
                /* If there is work remaining, but we didn't get a request back, take the meta request out of the
                 * list so that we don't use it again during this function, with the intention of putting it back in
                 * the list before this function ends. */
                if (request == NULL) {
                    aws_linked_list_remove(&meta_request->client_process_work_threaded_data.node);
                    aws_linked_list_push_back(
                        &meta_requests_work_remaining, &meta_request->client_process_work_threaded_data.node);
                } else {
                    request->tracked_by_client = true;

                    ++client->threaded_data.num_requests_being_prepared;

                    num_requests_in_flight =
                        (uint32_t)aws_atomic_fetch_add(&client->stats.num_requests_in_flight, 1) + 1;

                    aws_s3_meta_request_prepare_request(
                        meta_request, request, s_s3_client_prepare_callback_queue_request, client);
                }
            } else {
                s_s3_client_remove_meta_request_threaded(client, meta_request);
            }
        }

        aws_linked_list_move_all_front(&client->threaded_data.meta_requests, &meta_requests_work_remaining);
    }
}

static void s_s3_client_meta_request_finished_request(
    struct aws_s3_client *client,
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request);

    if (request->tracked_by_client) {
        /* BEGIN CRITICAL SECTION */
        aws_s3_client_lock_synced_data(client);
        aws_atomic_fetch_sub(&client->stats.num_requests_in_flight, 1);
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
        /* END CRITICAL SECTION */
    }
    aws_s3_meta_request_finished_request(meta_request, request, error_code);
}

static void s_s3_client_prepare_callback_queue_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(request);

    struct aws_s3_client *client = user_data;
    AWS_PRECONDITION(client);

    if (error_code != AWS_ERROR_SUCCESS) {
        s_s3_client_meta_request_finished_request(client, meta_request, request, error_code);

        aws_s3_request_release(request);
        request = NULL;
    }

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_client_lock_synced_data(client);

        if (error_code == AWS_ERROR_SUCCESS) {
            aws_linked_list_push_back(&client->synced_data.prepared_requests, &request->node);
        } else {
            ++client->synced_data.num_failed_prepare_requests;
        }

        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
    /* END CRITICAL SECTION */
}

void aws_s3_client_update_connections_threaded(struct aws_s3_client *client) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);

    struct aws_linked_list left_over_requests;
    aws_linked_list_init(&left_over_requests);

    while (s_s3_client_get_num_requests_network_io(client, AWS_S3_META_REQUEST_TYPE_MAX) <
               aws_s3_client_get_max_active_connections(client, NULL) &&
           !aws_linked_list_empty(&client->threaded_data.request_queue)) {

        struct aws_s3_request *request = aws_s3_client_dequeue_request_threaded(client);
        const uint32_t max_active_connections = aws_s3_client_get_max_active_connections(client, request->meta_request);

        /* Unless the request is marked "always send", if this meta request has a finish result, then finish the request
         * now and release it. */
        if (!request->always_send && aws_s3_meta_request_has_finish_result(request->meta_request)) {
            s_s3_client_meta_request_finished_request(client, request->meta_request, request, AWS_ERROR_S3_CANCELED);

            aws_s3_request_release(request);
            request = NULL;
        } else if (
            s_s3_client_get_num_requests_network_io(client, request->meta_request->type) < max_active_connections) {
            s_s3_client_create_connection_for_request(client, request);
        } else {
            /* Push the request into the left-over list to be used in a future call of this function. */
            aws_linked_list_push_back(&left_over_requests, &request->node);
        }
    }

    aws_s3_client_queue_requests_threaded(client, &left_over_requests, true);
}

static void s_s3_client_acquired_retry_token(
    struct aws_retry_strategy *retry_strategy,
    int error_code,
    struct aws_retry_token *token,
    void *user_data);

static void s_s3_client_retry_ready(struct aws_retry_token *token, int error_code, void *user_data);

static void s_s3_client_create_connection_for_request_default(
    struct aws_s3_client *client,
    struct aws_s3_request *request);

static void s_s3_client_create_connection_for_request(struct aws_s3_client *client, struct aws_s3_request *request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(client->vtable);

    if (client->vtable->create_connection_for_request) {
        client->vtable->create_connection_for_request(client, request);
        return;
    }

    s_s3_client_create_connection_for_request_default(client, request);
}

static void s_s3_client_create_connection_for_request_default(
    struct aws_s3_client *client,
    struct aws_s3_request *request) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    aws_atomic_fetch_add(&client->stats.num_requests_network_io[meta_request->type], 1);

    struct aws_s3_connection *connection = aws_mem_calloc(client->allocator, 1, sizeof(struct aws_s3_connection));

    connection->endpoint = aws_s3_endpoint_acquire(meta_request->endpoint, false /*already_holding_lock*/);
    connection->request = request;

    struct aws_byte_cursor host_header_value;
    AWS_ZERO_STRUCT(host_header_value);

    struct aws_http_headers *message_headers = aws_http_message_get_headers(meta_request->initial_request_message);
    AWS_ASSERT(message_headers);

    int get_header_result = aws_http_headers_get(message_headers, g_host_header_name, &host_header_value);
    AWS_ASSERT(get_header_result == AWS_OP_SUCCESS);
    (void)get_header_result;

    if (aws_retry_strategy_acquire_retry_token(
            client->retry_strategy, &host_header_value, s_s3_client_acquired_retry_token, connection, 0)) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Client could not acquire retry token for request %p due to error %d (%s)",
            (void *)client,
            (void *)request,
            aws_last_error_or_unknown(),
            aws_error_str(aws_last_error_or_unknown()));

        goto reset_connection;
    }

    return;

reset_connection:

    aws_s3_client_notify_connection_finished(
        client, connection, aws_last_error_or_unknown(), AWS_S3_CONNECTION_FINISH_CODE_FAILED);
}

static void s_s3_client_acquired_retry_token(
    struct aws_retry_strategy *retry_strategy,
    int error_code,
    struct aws_retry_token *token,
    void *user_data) {

    AWS_PRECONDITION(retry_strategy);
    (void)retry_strategy;

    struct aws_s3_connection *connection = user_data;
    AWS_PRECONDITION(connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_endpoint *endpoint = meta_request->endpoint;
    AWS_ASSERT(endpoint != NULL);

    struct aws_s3_client *client = endpoint->client;
    AWS_ASSERT(client != NULL);

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Client could not get retry token for connection %p processing request %p due to error %d (%s)",
            (void *)client,
            (void *)connection,
            (void *)request,
            error_code,
            aws_error_str(error_code));

        goto error_clean_up;
    }

    AWS_ASSERT(token);

    connection->retry_token = token;

    AWS_ASSERT(client->vtable->acquire_http_connection);

    /* client needs to be kept alive until s_s3_client_on_acquire_http_connection completes */
    /* TODO: not a blocker, consider managing the life time of aws_s3_client from aws_s3_endpoint to simplify usage */
    aws_s3_client_acquire(client);

    client->vtable->acquire_http_connection(
        endpoint->http_connection_manager, s_s3_client_on_acquire_http_connection, connection);

    return;

error_clean_up:

    aws_s3_client_notify_connection_finished(client, connection, error_code, AWS_S3_CONNECTION_FINISH_CODE_FAILED);
}

static void s_s3_client_on_acquire_http_connection(
    struct aws_http_connection *incoming_http_connection,
    int error_code,
    void *user_data) {

    struct aws_s3_connection *connection = user_data;
    AWS_PRECONDITION(connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_endpoint *endpoint = meta_request->endpoint;
    AWS_ASSERT(endpoint != NULL);

    struct aws_s3_client *client = endpoint->client;
    AWS_ASSERT(client != NULL);

    if (error_code != AWS_ERROR_SUCCESS) {
        AWS_LOGF_ERROR(
            AWS_LS_S3_ENDPOINT,
            "id=%p: Could not acquire connection due to error code %d (%s)",
            (void *)endpoint,
            error_code,
            aws_error_str(error_code));

        if (error_code == AWS_IO_DNS_INVALID_NAME) {
            goto error_fail;
        }

        goto error_retry;
    }

    connection->http_connection = incoming_http_connection;
    aws_s3_meta_request_send_request(meta_request, connection);
    aws_s3_client_release(client); /* kept since this callback was registered */
    return;

error_retry:

    aws_s3_client_notify_connection_finished(client, connection, error_code, AWS_S3_CONNECTION_FINISH_CODE_RETRY);
    aws_s3_client_release(client); /* kept since this callback was registered */
    return;

error_fail:

    aws_s3_client_notify_connection_finished(client, connection, error_code, AWS_S3_CONNECTION_FINISH_CODE_FAILED);
    aws_s3_client_release(client); /* kept since this callback was registered */
}

/* Called by aws_s3_meta_request when it has finished using this connection for a single request. */
void aws_s3_client_notify_connection_finished(
    struct aws_s3_client *client,
    struct aws_s3_connection *connection,
    int error_code,
    enum aws_s3_connection_finish_code finish_code) {
    AWS_PRECONDITION(client);
    AWS_PRECONDITION(connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;

    AWS_PRECONDITION(meta_request);
    AWS_PRECONDITION(meta_request->initial_request_message);

    struct aws_s3_endpoint *endpoint = meta_request->endpoint;
    AWS_PRECONDITION(endpoint);

    /* If we're trying to setup a retry... */
    if (finish_code == AWS_S3_CONNECTION_FINISH_CODE_RETRY) {

        if (connection->retry_token == NULL) {
            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Client could not schedule retry of request %p for meta request %p, as retry token is NULL.",
                (void *)client,
                (void *)request,
                (void *)meta_request);

            goto reset_connection;
        }

        if (aws_s3_meta_request_is_finished(meta_request)) {
            AWS_LOGF_DEBUG(
                AWS_LS_S3_CLIENT,
                "id=%p Client not scheduling retry of request %p for meta request %p with token %p because meta "
                "request has been flagged as finished.",
                (void *)client,
                (void *)request,
                (void *)meta_request,
                (void *)connection->retry_token);

            goto reset_connection;
        }

        AWS_LOGF_DEBUG(
            AWS_LS_S3_CLIENT,
            "id=%p Client scheduling retry of request %p for meta request %p with token %p.",
            (void *)client,
            (void *)request,
            (void *)meta_request,
            (void *)connection->retry_token);

        enum aws_retry_error_type error_type = AWS_RETRY_ERROR_TYPE_TRANSIENT;

        switch (error_code) {
            case AWS_ERROR_S3_INTERNAL_ERROR:
                error_type = AWS_RETRY_ERROR_TYPE_SERVER_ERROR;
                break;

            case AWS_ERROR_S3_SLOW_DOWN:
                error_type = AWS_RETRY_ERROR_TYPE_THROTTLING;
                break;
        }

        if (connection->http_connection != NULL) {
            AWS_ASSERT(endpoint->http_connection_manager);

            aws_http_connection_manager_release_connection(
                endpoint->http_connection_manager, connection->http_connection);

            connection->http_connection = NULL;
        }

        /* Ask the retry strategy to schedule a retry of the request. */
        if (aws_retry_strategy_schedule_retry(
                connection->retry_token, error_type, s_s3_client_retry_ready, connection)) {

            AWS_LOGF_ERROR(
                AWS_LS_S3_CLIENT,
                "id=%p Client could not retry request %p for meta request %p with token %p due to error %d (%s)",
                (void *)client,
                (void *)request,
                (void *)meta_request,
                (void *)connection->retry_token,
                aws_last_error_or_unknown(),
                aws_error_str(aws_last_error_or_unknown()));

            goto reset_connection;
        }

        return;
    }

reset_connection:

    if (connection->retry_token != NULL) {
        /* If we have a retry token and successfully finished, record that success. */
        if (finish_code == AWS_S3_CONNECTION_FINISH_CODE_SUCCESS) {
            aws_retry_token_record_success(connection->retry_token);
        }

        aws_retry_token_release(connection->retry_token);
        connection->retry_token = NULL;
    }

    /* If we weren't successful, and we're here, that means this failure is not eligible for a retry. So finish the
     * request, and close our HTTP connection. */
    if (finish_code != AWS_S3_CONNECTION_FINISH_CODE_SUCCESS) {
        if (connection->http_connection != NULL) {
            aws_http_connection_close(connection->http_connection);
        }
    }

    aws_atomic_fetch_sub(&client->stats.num_requests_network_io[meta_request->type], 1);

    s_s3_client_meta_request_finished_request(client, meta_request, request, error_code);

    if (connection->http_connection != NULL) {
        AWS_ASSERT(endpoint->http_connection_manager);

        aws_http_connection_manager_release_connection(endpoint->http_connection_manager, connection->http_connection);

        connection->http_connection = NULL;
    }

    if (connection->request != NULL) {
        aws_s3_request_release(connection->request);
        connection->request = NULL;
    }

    aws_retry_token_release(connection->retry_token);
    connection->retry_token = NULL;

    aws_s3_endpoint_release(connection->endpoint);
    connection->endpoint = NULL;

    aws_mem_release(client->allocator, connection);
    connection = NULL;

    /* BEGIN CRITICAL SECTION */
    {
        aws_s3_client_lock_synced_data(client);
        s_s3_client_schedule_process_work_synced(client);
        aws_s3_client_unlock_synced_data(client);
    }
    /* END CRITICAL SECTION */
}

static void s_s3_client_prepare_request_callback_retry_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data);

static void s_s3_client_retry_ready(struct aws_retry_token *token, int error_code, void *user_data) {
    AWS_PRECONDITION(token);
    (void)token;

    struct aws_s3_connection *connection = user_data;
    AWS_PRECONDITION(connection);

    struct aws_s3_request *request = connection->request;
    AWS_PRECONDITION(request);

    struct aws_s3_meta_request *meta_request = request->meta_request;
    AWS_PRECONDITION(meta_request);

    struct aws_s3_endpoint *endpoint = meta_request->endpoint;
    AWS_PRECONDITION(endpoint);

    struct aws_s3_client *client = endpoint->client;
    AWS_PRECONDITION(client);

    /* If we couldn't retry this request, then bail on the entire meta request. */
    if (error_code != AWS_ERROR_SUCCESS) {

        AWS_LOGF_ERROR(
            AWS_LS_S3_CLIENT,
            "id=%p Client could not retry request %p for meta request %p due to error %d (%s)",
            (void *)client,
            (void *)meta_request,
            (void *)request,
            error_code,
            aws_error_str(error_code));

        goto error_clean_up;
    }

    AWS_LOGF_DEBUG(
        AWS_LS_S3_META_REQUEST,
        "id=%p Client retrying request %p for meta request %p on connection %p with retry token %p",
        (void *)client,
        (void *)request,
        (void *)meta_request,
        (void *)connection,
        (void *)connection->retry_token);

    aws_s3_meta_request_prepare_request(
        meta_request, request, s_s3_client_prepare_request_callback_retry_request, connection);

    return;

error_clean_up:

    aws_s3_client_notify_connection_finished(client, connection, error_code, AWS_S3_CONNECTION_FINISH_CODE_FAILED);
}

static void s_s3_client_prepare_request_callback_retry_request(
    struct aws_s3_meta_request *meta_request,
    struct aws_s3_request *request,
    int error_code,
    void *user_data) {
    AWS_PRECONDITION(meta_request);
    (void)meta_request;

    AWS_PRECONDITION(request);
    (void)request;

    struct aws_s3_connection *connection = user_data;
    AWS_PRECONDITION(connection);

    struct aws_s3_endpoint *endpoint = meta_request->endpoint;
    AWS_ASSERT(endpoint != NULL);

    struct aws_s3_client *client = endpoint->client;
    AWS_ASSERT(client != NULL);

    if (error_code == AWS_ERROR_SUCCESS) {
        AWS_ASSERT(connection->retry_token);

        s_s3_client_acquired_retry_token(
            client->retry_strategy, AWS_ERROR_SUCCESS, connection->retry_token, connection);
    } else {
        aws_s3_client_notify_connection_finished(client, connection, error_code, AWS_S3_CONNECTION_FINISH_CODE_FAILED);
    }
}

static void s_resume_token_ref_count_zero_callback(void *arg) {
    struct aws_s3_meta_request_resume_token *token = arg;

    aws_string_destroy(token->multipart_upload_id);

    aws_mem_release(token->allocator, token);
}

struct aws_s3_meta_request_resume_token *aws_s3_meta_request_resume_token_new(struct aws_allocator *allocator) {
    struct aws_s3_meta_request_resume_token *token =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_s3_meta_request_resume_token));

    token->allocator = allocator;
    aws_ref_count_init(&token->ref_count, token, s_resume_token_ref_count_zero_callback);

    return token;
}

struct aws_s3_meta_request_resume_token *aws_s3_meta_request_resume_token_new_upload(
    struct aws_allocator *allocator,
    const struct aws_s3_upload_resume_token_options *options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(options);

    struct aws_s3_meta_request_resume_token *token = aws_s3_meta_request_resume_token_new(allocator);
    token->multipart_upload_id = aws_string_new_from_cursor(allocator, &options->upload_id);
    token->part_size = options->part_size;
    token->total_num_parts = options->total_num_parts;
    token->num_parts_completed = options->num_parts_completed;
    token->type = AWS_S3_META_REQUEST_TYPE_PUT_OBJECT;
    return token;
}

struct aws_s3_meta_request_resume_token *aws_s3_meta_request_resume_token_acquire(
    struct aws_s3_meta_request_resume_token *resume_token) {
    if (resume_token) {
        aws_ref_count_acquire(&resume_token->ref_count);
    }
    return resume_token;
}

struct aws_s3_meta_request_resume_token *aws_s3_meta_request_resume_token_release(
    struct aws_s3_meta_request_resume_token *resume_token) {
    if (resume_token) {
        aws_ref_count_release(&resume_token->ref_count);
    }
    return NULL;
}

enum aws_s3_meta_request_type aws_s3_meta_request_resume_token_type(
    struct aws_s3_meta_request_resume_token *resume_token) {
    AWS_FATAL_PRECONDITION(resume_token);
    return resume_token->type;
}

size_t aws_s3_meta_request_resume_token_part_size(struct aws_s3_meta_request_resume_token *resume_token) {
    AWS_FATAL_PRECONDITION(resume_token);
    return resume_token->part_size;
}

size_t aws_s3_meta_request_resume_token_total_num_parts(struct aws_s3_meta_request_resume_token *resume_token) {
    AWS_FATAL_PRECONDITION(resume_token);
    return resume_token->total_num_parts;
}

size_t aws_s3_meta_request_resume_token_num_parts_completed(struct aws_s3_meta_request_resume_token *resume_token) {
    AWS_FATAL_PRECONDITION(resume_token);
    return resume_token->num_parts_completed;
}

struct aws_byte_cursor aws_s3_meta_request_resume_token_upload_id(
    struct aws_s3_meta_request_resume_token *resume_token) {
    AWS_FATAL_PRECONDITION(resume_token);
    if (resume_token->type == AWS_S3_META_REQUEST_TYPE_PUT_OBJECT && resume_token->multipart_upload_id != NULL) {
        return aws_byte_cursor_from_string(resume_token->multipart_upload_id);
    }

    return aws_byte_cursor_from_c_str("");
}
