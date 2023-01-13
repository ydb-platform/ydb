#ifndef AWS_IO_HOST_RESOLVER_H
#define AWS_IO_HOST_RESOLVER_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/ref_count.h>
#include <aws/io/io.h>

struct aws_event_loop_group;

enum aws_address_record_type {
    /* ipv4 address. */
    AWS_ADDRESS_RECORD_TYPE_A,
    /* ipv6 address. */
    AWS_ADDRESS_RECORD_TYPE_AAAA
};

enum aws_get_host_address_flags {
    /* get number of ipv4 addresses. */
    AWS_GET_HOST_ADDRESS_COUNT_RECORD_TYPE_A = 0x00000001,
    /* get number of ipv6 addresses. */
    AWS_GET_HOST_ADDRESS_COUNT_RECORD_TYPE_AAAA = 0x00000002
};

struct aws_string;

struct aws_host_address {
    struct aws_allocator *allocator;
    const struct aws_string *host;
    const struct aws_string *address;
    enum aws_address_record_type record_type;
    uint64_t expiry;
    /* This next section is strictly for mitigating the impact of sticky hosts that aren't performing well. */
    /*for use in DNS-based load balancing.*/
    size_t use_count;
    /* give a hint on when to remove a bad host from service. */
    size_t connection_failure_count;
    /* we don't implement this yet, but we will asap. */
    uint8_t weight;
};

struct aws_host_resolver;
/**
 * Invoked once an address has been resolved for host. The type in host_addresses is struct aws_host_address (by-value).
 * The caller does not own this memory and you must copy the host address before returning from this function if you
 * plan to use it later. For convenience, we've provided the aws_host_address_copy() and aws_host_address_clean_up()
 * functions.
 */
typedef void(aws_on_host_resolved_result_fn)(
    struct aws_host_resolver *resolver,
    const struct aws_string *host_name,
    int err_code,
    const struct aws_array_list *host_addresses,
    void *user_data);

/**
 * Function signature for configuring your own resolver (the default just uses getaddrinfo()). The type in
 * output_addresses is struct aws_host_address (by-value). We assume this function blocks, hence this absurdly
 * complicated design.
 */
typedef int(aws_resolve_host_implementation_fn)(
    struct aws_allocator *allocator,
    const struct aws_string *host_name,
    struct aws_array_list *output_addresses,
    void *user_data);

struct aws_host_resolution_config {
    aws_resolve_host_implementation_fn *impl;
    size_t max_ttl;
    void *impl_data;
};

struct aws_host_listener;

struct aws_host_listener_options;

/** should you absolutely disdain the default implementation, feel free to implement your own. */
struct aws_host_resolver_vtable {
    /** clean up everything you allocated, but not resolver itself. */
    void (*destroy)(struct aws_host_resolver *resolver);
    /** resolve the host by host_name, the user owns host_name, so it needs to be copied if you persist it,
     * invoke res with the result. This function should never block. */
    int (*resolve_host)(
        struct aws_host_resolver *resolver,
        const struct aws_string *host_name,
        aws_on_host_resolved_result_fn *res,
        const struct aws_host_resolution_config *config,
        void *user_data);
    /** gives your implementation a hint that an address has some failed connections occuring. Do whatever you want (or
     * nothing) about it.
     */
    int (*record_connection_failure)(struct aws_host_resolver *resolver, struct aws_host_address *address);
    /** wipe out anything you have cached. */
    int (*purge_cache)(struct aws_host_resolver *resolver);
    /** get number of addresses for a given host. */
    size_t (*get_host_address_count)(
        struct aws_host_resolver *resolver,
        const struct aws_string *host_name,
        uint32_t flags);

    /** creates and adds a listener to the host resolver. */
    struct aws_host_listener *(
        *add_host_listener)(struct aws_host_resolver *resolver, const struct aws_host_listener_options *options);

    /** removes a host listener from the host resolver and frees it. */
    int (*remove_host_listener)(struct aws_host_resolver *resolver, struct aws_host_listener *listener);
};

struct aws_host_resolver {
    struct aws_allocator *allocator;
    void *impl;
    struct aws_host_resolver_vtable *vtable;
    struct aws_ref_count ref_count;
    struct aws_shutdown_callback_options shutdown_options;
};

struct aws_host_resolver_default_options {
    size_t max_entries;
    struct aws_event_loop_group *el_group;
    const struct aws_shutdown_callback_options *shutdown_options;
    aws_io_clock_fn *system_clock_override_fn;
};

AWS_EXTERN_C_BEGIN

/**
 * Copies `from` to `to`.
 */
AWS_IO_API int aws_host_address_copy(const struct aws_host_address *from, struct aws_host_address *to);

/**
 * Moves `from` to `to`. After this call, from is no longer usable. Though, it could be resused for another
 * move or copy operation.
 */
AWS_IO_API void aws_host_address_move(struct aws_host_address *from, struct aws_host_address *to);

/**
 * Cleans up the memory for `address`
 */
AWS_IO_API void aws_host_address_clean_up(struct aws_host_address *address);

/** WARNING! do not call this function directly (getaddrinfo()): it blocks. Provide a pointer to this function for other
 * resolution functions. */
AWS_IO_API int aws_default_dns_resolve(
    struct aws_allocator *allocator,
    const struct aws_string *host_name,
    struct aws_array_list *output_addresses,
    void *user_data);

/**
 * Creates a host resolver with the default behavior. Here's the behavior:
 *
 * Since there's not a reliable way to do non-blocking DNS without a ton of risky work that would need years of testing
 * on every Unix system in existence, we work around it by doing a threaded implementation.
 *
 * When you request an address, it checks the cache. If the entry isn't in the cache it creates a new one.
 * Each entry has a potentially short lived back-ground thread based on ttl for the records. Once we've populated the
 * cache and you keep the resolver active, the resolution callback will be invoked immediately. When it's idle, it will
 * take a little while in the background thread to fetch more, evaluate TTLs etc... In that case your callback will be
 * invoked from the background thread.
 *
 * --------------------------------------------------------------------------------------------------------------------
 *
 * A few things to note about TTLs and connection failures.
 *
 * We attempt to honor your max ttl but will not honor it if dns queries are failing or all of your connections are
 * marked as failed. Once we are able to query dns again, we will re-evaluate the TTLs.
 *
 * Upon notification connection failures, we move them to a separate list. Eventually we retry them when it's likely
 * that the endpoint is healthy again or we don't really have another choice, but we try to keep them out of your
 * hot path.
 *
 * ---------------------------------------------------------------------------------------------------------------------
 *
 * Finally, this entire design attempts to prevent problems where developers have to choose between large TTLs and thus
 * sticky hosts or short TTLs and good fleet utilization but now higher latencies. In this design, we resolve every
 * second in the background (only while you're actually using the record), but we do not expire the earlier resolved
 * addresses until max ttl has passed.
 *
 * This for example, should enable you to hit thousands of hosts in the Amazon S3 fleet instead of just one or two.
 */
AWS_IO_API struct aws_host_resolver *aws_host_resolver_new_default(
    struct aws_allocator *allocator,
    const struct aws_host_resolver_default_options *options);

/**
 * Increments the reference count on the host resolver, allowing the caller to take a reference to it.
 *
 * Returns the same host resolver passed in.
 */
AWS_IO_API struct aws_host_resolver *aws_host_resolver_acquire(struct aws_host_resolver *resolver);

/**
 * Decrements a host resolver's ref count.  When the ref count drops to zero, the resolver will be destroyed.
 */
AWS_IO_API void aws_host_resolver_release(struct aws_host_resolver *resolver);

/**
 * calls resolve_host on the vtable. config will be copied.
 */
AWS_IO_API int aws_host_resolver_resolve_host(
    struct aws_host_resolver *resolver,
    const struct aws_string *host_name,
    aws_on_host_resolved_result_fn *res,
    const struct aws_host_resolution_config *config,
    void *user_data);

/**
 * calls record_connection_failure on the vtable.
 */
AWS_IO_API int aws_host_resolver_record_connection_failure(
    struct aws_host_resolver *resolver,
    struct aws_host_address *address);

/**
 * calls purge_cache on the vtable.
 */
AWS_IO_API int aws_host_resolver_purge_cache(struct aws_host_resolver *resolver);

/**
 * get number of addresses for a given host.
 */
AWS_IO_API size_t aws_host_resolver_get_host_address_count(
    struct aws_host_resolver *resolver,
    const struct aws_string *host_name,
    uint32_t flags);

/* Callback for receiving new host addresses from a listener. Memory for the new address list is only guaranteed to
 * exist during the callback, and must be copied if the caller needs it to persist after. */
typedef void(aws_host_listener_resolved_address_fn)(
    /* Listener that owns this callback. */
    struct aws_host_listener *listener,

    /* Array list of aws_host_address structures.  To get an item:
     *
     * struct aws_host_address *host_address = NULL;
     * aws_array_list_get_at_ptr(new_address_list, (void **)&host_address, address_index);
     * */
    const struct aws_array_list *new_address_list,

    /* User data that was specified when adding the listener. */
    void *user_data);

/* Callback for learning of an expired address from a listener. Memory for the expired address list is only guaranteed
 * to exist during the callback, and must be copied if the caller needs it to persist after. */
typedef void(aws_host_listener_expired_address_fn)(
    /* Listener that owns this callback. */
    struct aws_host_listener *listener,

    /* Array list of aws_host_address structures.  To get an item:
     *
     * struct aws_host_address *host_address = NULL;
     * aws_array_list_get_at_ptr(new_address_list, (void **)&host_address, address_index);
     * */
    const struct aws_array_list *expired_address_list,

    /* User data that was specified when adding the listener. */
    void *user_data);

/* Callback for when the listener has completed its clean up. */
typedef void(aws_host_listener_shutdown_fn)(void *user_data);

struct aws_host_listener_options {

    /* Name of the host to listen for notifications from. */
    struct aws_byte_cursor host_name;

    /* Callback for when an address is resolved for the specified host. */
    aws_host_listener_resolved_address_fn *resolved_address_callback;

    /* Callback for when a resolved address expires for the specified host. */
    aws_host_listener_expired_address_fn *expired_address_callback;

    /* Callback for when a listener has completely shutdown. */
    aws_host_listener_shutdown_fn *shutdown_callback;

    /* User data to be passed into each callback. */
    void *user_data;

    /* Lets the resolver know to keep the resolution thread alive for as long as this listener is attached */
    bool pin_host_entry;
};

/* Create and add a listener to the host resolver using the specified options. */
AWS_IO_API struct aws_host_listener *aws_host_resolver_add_host_listener(
    struct aws_host_resolver *resolver,
    const struct aws_host_listener_options *options);

/* Remove the specified listener from the host resolver, triggering clean up of the listener. Note: this may not happen
 * synchronously, and it is necessary to wait for the listener's shutdown callback to trigger. */
AWS_IO_API int aws_host_resolver_remove_host_listener(
    struct aws_host_resolver *resolver,
    struct aws_host_listener *listener);

AWS_EXTERN_C_END

#endif /* AWS_IO_HOST_RESOLVER_H */
