#ifndef AWS_COMMON_BUS_H
#define AWS_COMMON_BUS_H

/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/common.h>

/*
 * A message bus is a mapping of integer message addresses/types -> listeners/callbacks.
 * A listener can listen to a single message, or to all messages on a bus
 * Message addresses/types can be any 64-bit integer, starting at 1.
 * AWS_BUS_ADDRESS_ALL (0xffffffffffffffff) is reserved for broadcast to all listeners.
 * AWS_BUS_ADDRESS_CLOSE (0) is reserved for notifying listeners to clean up
 * Listeners will be sent a message of type AWS_BUS_ADDRESS_CLOSE when it is time to clean any state up.
 * Listeners are owned by the subscriber, and are no longer referenced by the bus once unsubscribed.
 * Under the AWS_BUS_ASYNC policy, message delivery happens in a separate thread from sending, so listeners are
 * responsible for their own thread safety.
 */
struct aws_bus;

enum aws_bus_policy {
    /**
     * Messages will be delivered, even if dynamic allocation is required. Default.
     */
    AWS_BUS_ASYNC_RELIABLE = 0x0,
    /**
     * Only memory from the bus's internal buffer will be used (if a buffer size is supplied at bus creation time).
     * If the buffer is full, older buffered messages will be discarded to make room for newer messages.
     */
    AWS_BUS_ASYNC_UNRELIABLE = 0x1,
    /**
     * Message delivery is immediate, and therefore reliable by definition
     */
    AWS_BUS_SYNC_RELIABLE = 0x2,
};

/**
 * Subscribing to AWS_BUS_ADDRESS_ALL will cause the listener to be invoked for every message sent to the bus
 * It is possible to send to AWS_BUS_ADDRESS_ALL, just be aware that this will only send to listeners subscribed
 * to AWS_BUS_ADDRESS_ALL.
 */
#define AWS_BUS_ADDRESS_ALL ((uint64_t)-1)
#define AWS_BUS_ADDRESS_CLOSE 0

struct aws_bus_options {
    enum aws_bus_policy policy;
    /**
     * Size of buffer for unreliable message delivery queue.
     * Unused if policy is AWS_BUS_ASYNC_RELIABNLE or AWS_BUS_SYNC_RELIABLE
     * Messages are 40 bytes. Default buffer_size is 4K. The bus will not allocate memory beyond this size.
     */
    size_t buffer_size;
    /* Not supported yet, but event loop group for delivery */
    struct aws_event_loop_group *event_loop_group;
};

/* Signature for listener callbacks */
typedef void(aws_bus_listener_fn)(uint64_t address, const void *payload, void *user_data);

/**
 * Allocates and initializes a message bus
 */
AWS_COMMON_API
struct aws_bus *aws_bus_new(struct aws_allocator *allocator, const struct aws_bus_options *options);

/**
 * Cleans up a message bus, including notifying all remaining listeners to close
 */
AWS_COMMON_API
void aws_bus_destroy(struct aws_bus *bus);

/**
 * Subscribes a listener to a message type. user_data's lifetime is the responsibility of the subscriber.
 */
AWS_COMMON_API
int aws_bus_subscribe(struct aws_bus *bus, uint64_t address, aws_bus_listener_fn *listener, void *user_data);

/**
 * Unsubscribe a listener from a specific message. This is only necessary if the listener has lifetime concerns.
 * Otherwise, the listener will be called with an address of AWS_BUS_ADDRESS_CLOSE, which indicates that user_data
 * can be cleaned up if necessary and the listener will never be called again.
 */
AWS_COMMON_API
void aws_bus_unsubscribe(struct aws_bus *bus, uint64_t address, aws_bus_listener_fn *listener, void *user_data);

/**
 * Sends a message to any listeners. payload will live until delivered, and then the destructor (if
 * provided) will be called. Note that anything payload references must also live at least until it is destroyed.
 * Will return AWS_OP_ERR if the bus is closing/has been closed
 */
AWS_COMMON_API
int aws_bus_send(struct aws_bus *bus, uint64_t address, void *payload, void (*destructor)(void *));

#endif /* AWS_COMMON_BUS_H */
