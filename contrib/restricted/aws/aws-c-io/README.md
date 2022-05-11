## AWS-C-IO

This is a module for the AWS SDK for C. It handles all IO and TLS work for application protocols.

aws-c-io is an event driven framework for implementing application protocols. It is built on top of
cross-platform abstractions that allow you as a developer to think only about the state machine and API
for your protocols. A typical use-case would be to write something like Http on top of asynchronous-io
with TLS already baked in. All of the platform and security concerns are already handled for you.

It is designed to be light-weight, fast, portable, and flexible for multiple domain use-cases such as:
embedded, server, client, and mobile.

## License

This library is licensed under the Apache 2.0 License.

## Usage

### Building

CMake 3.1+ is required to build.

`<install-path>` must be an absolute path in the following instructions.

#### Linux-Only Dependencies

If you are building on Linux, you will need to build aws-lc and s2n-tls first.

```
git clone git@github.com:awslabs/aws-lc.git
cmake -S aws-lc -B aws-lc/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-lc/build --target install

git clone git@github.com:aws/s2n-tls.git
cmake -S s2n-tls -B s2n-tls/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build s2n-tls/build --target install
```

#### Building aws-c-io and Remaining Dependencies

```
git clone git@github.com:awslabs/aws-c-common.git
cmake -S aws-c-common -B aws-c-common/build -DCMAKE_INSTALL_PREFIX=<install-path>
cmake --build aws-c-common/build --target install

git clone git@github.com:awslabs/aws-c-cal.git
cmake -S aws-c-cal -B aws-c-cal/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-cal/build --target install

git clone git@github.com:awslabs/aws-c-io.git
cmake -S aws-c-io -B aws-c-io/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-io/build --target install
```

### Usage Patterns

This library contains many primitive building blocks that can be configured in a myriad of ways. However, most likely
you simply need to use the `aws_event_loop_group` and `aws_channel_bootstrap` APIs.

Typical Client API Usage Pattern:

        /* setup */
        aws_io_library_init(allocator);

        struct aws_event_loop_group el_group;

        if (aws_event_loop_group_init_default(&el_group, allocator)) {
            goto cleanup;
        }

        struct aws_tls_ctx_options tls_options = { ... };
        struct aws_tls_ctx *tls_ctx = aws_tls_client_ctx_new(allocator, &tls_options);

        struct aws_tls_connection_options tls_client_conn_options = { ... };

        struct aws_client_bootstrap client_bootstrap;

        if (aws_client_bootstrap_init(&client_bootstrap, allocator, &el_group) {
            goto cleanup;
        }

        aws_client_bootstrap_set_tls_ctx(&client_bootstrap, tls_ctx);
        aws_client_bootstrap_set_alpn_callback(&client_bootstrap, your_alpn_callback);

        /* throughout your application's lifetime */
        struct aws_socket_options sock_options = { ... };
        struct aws_socket_endpoint endpoint = { ... };

        if (aws_client_bootstrap_new_tls_socket_channel(&client_bootrap, &endpoint, &sock_options, &tls_options,
                your_channel_setup_callback, your_channel_shutdown_callback, your_context_data) {
            goto cleanup;
        }

        /* shutdown */
        aws_client_bootstrap_clean_up(&client_bootstrap);
        aws_tls_client_ctx_destroy(tls_ctx);
        aws_event_loop_group_clean_up(&el_group);
        aws_io_library_clean_up();

Typical Server API Usage Pattern:

        /* setup */
        aws_io_library_init(allocator);

        struct aws_event_loop_group el_group;

        if (aws_event_loop_group_init_default(&el_group, allocator)) {
            goto cleanup;
        }

        struct aws_tls_ctx_options tls_options = { ... };
        struct aws_tls_ctx *tls_ctx = aws_tls_server_ctx_new(allocator, &tls_options);

        struct aws_tls_connection_options tls_server_conn_options = { ... };

        struct aws_socket_options sock_options = { ... };
        struct aws_socket_endpoint endpoint = { ... };

        struct aws_server_bootstrap server_bootstrap;

        if (aws_server_bootstrap_init(&server_bootstrap, allocator, &el_group) {
            goto cleanup;
        }

        aws_server_bootstrap_set_tls_ctx(&server_bootstrap, tls_ctx);
        aws_server_bootstrap_set_alpn_callback(&server_bootstrap, your_alpn_callback);

        struct aws_socket *listener = aws_server_bootstrap_add_tls_socket_listener(&server_bootstrap, &endpoint, &sock_options, &tls_options,
                                                      your_incoming_channel_callback, your_channel_shutdown_callback, your_context_data);

        if (!listener) {
            goto cleanup;
        }


        /* shutdown */
        aws_server_bootstrap_remove_socket_listener(listener);
        aws_server_bootstrap_clean_up(&server_bootstrap);
        aws_tls_server_ctx_destroy(tls_ctx);
        awS_event_loop_group_clean_up(&el_group);
        aws_io_library_clean_up();

If you are building a protocol on top of sockets without the use of TLS, you can still use this pattern as your starting point.
Simply call the `aws_client_bootstrap_new_socket_channel` `aws_server_bootstrap_add_socket_listener` respectively: instead of the TLS variants.

## Concepts

### Event Loop
Core to Async-IO is the event-loop. We provide an implementation for most platforms out of the box:

Platform | Implementation
--- | ---
Linux | Edge-Triggered Epoll
BSD Variants and Apple Devices | KQueue
Windows | IOCP (IO Completion Ports)

Also, you can always implement your own as well.

An Event Loop has a few jobs.

1. Notify subscribers of IO Events
2. Execute and maintain a task scheduler
3. Maintain an opaque data store for consumers

The threading model for a channel (see below) is pinned to the thread of the event-loop. Each event-loop implementation
provides an API to move a cross-thread call into the event-loop thread if necessary.

### Channels and Slots
A channel is simply a container that drives the slots. It is responsible for providing an interface
between slots and the underlying event-loop as well as invoking the slots to pass messages. As a channel
runs. It also provides utilities for making sure slots and their handlers run in the correct thread and moving execution
to that thread if necessary.

![Channels and Slots Diagram](docs/images/channels_slots.png)

In this diagram, a channel is a collection of slots, and it knows how to make them communicate. It also controls the
lifetime of slots.

When a channel is being shutdown, it will issue shutdown_direction messages in the appropriate direction. If it is in the read
direction, it will call shutdown_direction on the first slot. Conversely, in the write direction, it will call shutdown_direction
on the last slot in the channel. When all slots have successfully shutdown, the channel can be safely cleaned up and de-allocated.

### Slots
![Slots Diagram](docs/images/slots.png)

Slots maintain their links to adjacent slots in the channel. So as the channel
is processed, each slot will read from its left-adjacent slot, send those messages to the handler, and call their right-adjacent
slot when it needs to send a message. Conversely, each slot will read from its right-adjacent slot,
send those messages to the handler, and send messages to the left-adjacent slot in the channel. Most importantly, slots contain a
reference to a handler. Handlers are responsible for doing most of the work (see below). Finally, slots have
utilities for manipulating the connections of the slots themselves.

Slots can also be added, removed, or replaced dynamically from a channel.

### Channel Handlers
The channel handler is the fundamental unit that protocol developers will implement. It contains all of your
state machinery, framing, and optionally end-user APIs.

![Handler Diagram](docs/images/handler.png)


#### Special, pre-defined handlers
Out of the box you get a few handlers pre-implemented.
1. Sockets. We've done the heavy lifting of implementing a consistent sockets interface for each platform.
Sockets interact directly with the underlying io and are invoked directly by the event-loop for io events.
2. Pipes (or something like them depending on platform), these are particularly useful for testing.
3. TLS. We provide TLS implementations for most platforms.

Platform | Implementation
--- | ---
Linux | Signal-to-noise (s2n) see github.com/awslabs/s2n
BSD Variants | s2n
Apple Devices | Security Framework/ Secure Transport. See https://developer.apple.com/documentation/security/secure_transport
Windows | Secure Channel. See https://msdn.microsoft.com/en-us/library/windows/desktop/aa380123(v=vs.85).aspx

In addition, you can always write your own handler around your favorite implementation and use that. To provide your own
TLS implementation, you must build this library with the cmake argument `-DBYO_CRYPTO=ON`. You will no longer need s2n or
libcrypto once you do this. Instead, your application provides an implementation of `aws_tls_ctx`, and `aws_channel_handler`.
At startup time, you must invoke the functions: `aws_tls_byo_crypto_set_client_setup_options()` and `aws_tls_byo_crypto_set_server_setup_options()`.

### Typical Channel
![Typical Channel Diagram](docs/images/typical_channel.png)

A typical channel will contain a socket handler, which receives io events from the event-loop.
It will read up to 16 kb and pass the data to the next handler. The next handler is typically
feeding a TLS implementation (see the above section on pre-defined handlers). The TLS handler
will then pass the data to an application protocol. The application protocol could then expose an
API to an application. When the application wants to send data, the whole process runs in reverse.

Channels can be much more complex though. For example, there could be nested channels for multiplexing/de-multiplexing,
or there could be more handlers to cut down on handler complexity.

Note however, that a channel is always pinned to a single thread. It provides utilities for applications and
handlers to move a task into that thread, but it is very important that handlers and application users
of your handlers never block.

### Channel IO Operation Fairness

Since multiple channels run in the same event-loop, we need to make sure channels are not starved by other active channels.
To address this, the handlers consuming IO events from the event-loop should determine the appropriate max read and write
and context switch before continuing. A context switch is performed, simply by scheduling a task to run at the current timestamp,
to continue the IO operation.

A reasonable default is 16kb, but a savvy implementation may want to upgrade a few connections to 256kb if they notice a particularly
fast connection (e.g. you notice EAGAIN or EWOULDBLOCK is never returned from write() calls).

### Read Back Pressure

One of the most challenging aspects of asynchronous io programming, is managing when back-pressure should be applied to
the underlying io layer. In the read direction, this is managed via update_window messages. Let's look at the below diagram
for an example of how this works.

In this example, we have a channel setup with an event-loop which manages io event notifications. The first slot contains a socket handler.
The socket handler will read directly from the socket. The second slot has a TLS handler.
Its only job is to encrypt/decrypt the data passed to it and pass it back to the channel.
The third and final slot contains the actual application protocol handler (could be Http, SIP, RTP it doesn't really matter).

The application protocol exposes an API to the application. As data is processed, we don't want to endlessly read, allocate,
and process data faster than the application can use it. As a result, it has a 20kb window.

![Read Back Pressure Diagram](docs/images/read_backpressure.png)

1. The event-loop notifies the socket handler that it has data available to read. The handler knows it can read up to 20kb
so it reads a full 16kb from the socket and passes it to the next slot. Since the socket sees that there is still an open window,
it, schedules a task to read again after the other channels have had a chance to process their pending reads.

    Likewise, the TLS handler decrypts the data and passes
it to the slot containing the application protocol.

    The application protocol processes the 16 kb and hands it off to the application.
At this point, the application hasn't notified the channel it is finished with the data (suppose application queues it), so the
new window for the slot is 4 kb.

2. The event-loop runs the scheduled read task from (1) after processing the other channels. The socket handler sees it
can read 4kb more of data. Even though it can read 16kb at a time, to honor the window, it reads 4kb and passes it on. This
time however, the window is 0, so the socket does not schedule another read task.

    The TLS handler decrypts the data and passes it on

    The application protocol reads 4kb, passes it to the application and its window is 0kb.

    The channel now goes idle waiting on the application to finish processing its data.

3. The application notifies the channel (via the API on the application protocol handler) it has processed 20kb
of data. This causes the protocol handler to issue an update_window message with an update of 20kb.

    Slot 2 passes the message on to the TLS handler. It evaluates the message and simply, sends a 20kb window update message
    to its slot.

    The socket handler receives the update_window message and schedules a new read task.

4. The event-loop runs the scheduled read task from (3). The socket reads on the io-handle, but it returns EAGAIN or
EWOULD_BLOCK. The channel now goes back to an idle state waiting on the event-loop to notify it that the socket is readable.

### Write Back Pressure

Write back pressure comes into play when the application can produce data more quickly than it can be sent to the
underlying io. To manage this, messages have members to attach a promise fn and context data to. When a handler exposes
an API, it has the responsibility to take a fn and data from the user if over write is a possibility. The io-handler will
invoke the promise after it has successfully written the last byte to the underlying io.

### Thread Safety

In general, the plan for addressing thread-safety is to not share memory across threads. This library is designed around
single threaded event-loops which process one or more channels. Anywhere a handler or channel exposes a back-channel API,
it is responsible for checking which thread it was invoked from. If it is invoked from the event-loop's thread, then it may
proceed as planned. If it is not, it is required to queue a task to do the work. When the task is executed, it will be executed
in the correct thread.

The functions we specify as thread-safe, we do so because those functions are necessary for abiding by the stated threading model.
For example, since scheduling a task is the main function for addressing cross-threaded operations, it has to be thread-safe.

## Terminology
We use a few terms in the following sections that are not necessarily "C concepts". We assume you know C, but here are some
definitions that may be helpful.

### Run-time Polymorphic
This means that the API is driven by a virtual-table. This is simply a struct of function pointers. They are invoked via
a c extern style API, but ultimately those public functions simply invoke the corresponding function in the v-table.

These are reserved for types that:
a.) Need to be configurable, changable at runtime
b.) Do not have immediate performance concerns caused by an indirect function call.

### Compile-time Polymorphic
This means that the API is not necessarily driven by a virtual-table. It is exposed as a c extern style API, but the
build system makes a decision about which symbols to compile based on factors such as platform and compile-time flags.

These are reserved for types that:
a.) Need to be configurable at compile-time based on platform or compile-time options.
b.) Have performance concerns caused by an indirect function call.

Note: that runtime configurability may still be something we need to expose here. In that case, a compiler flag
will be used to denote that we are using a custom implementation for x feature. Then we will expose an implementation
that indirectly invokes from a v-table and provides hooks for the application to plug into at runtime.

### Promise, Promise Context
There are many phrases for this, callback, baton, event-handler etc... The key idea is that APIs that need to notify a
caller when an asynchronous action is completed, should take a callback function and a pointer to an opaque object
and invoke it upon completion. This term doesn't refer to the layout of the data. A promise in some instances may be a
collection of functions in a structure. It's simply the language we use for the concept.

## API

**Note: unless otherwise stated,**

* no functions in this API are allowed to block.
* nothing is thread-safe unless explicitly stated.


### Event Loop

Event Loops are run-time polymorphic. We provide some implementations out of the box and a way to get an implementation
without having to call a different function per platform. However, you can also create your own implementation and use it
on any API that takes an event-loop as a parameter.

From a design perspective, the event-loop is not aware of channels or any of its handlers. It interacts with other entities
only via its API.

#### Layout
    struct aws_event_loop {
        struct aws_event_loop_vtable vtable;
        aws_clock clock;
        struct aws_allocator *allocator;
        struct aws_common_hash_table local_storage;
        void *impl_data;
    };

#### V-Table

    struct aws_event_loop_vtable {
        void (*destroy)(struct aws_event_loop *);
        int (*run) (struct aws_event_loop *);
        int (*stop) (struct aws_event_loop *, void (*on_stopped) (struct aws_event_loop *, void *), void *promise_user_data);
        int (*schedule_task) (struct aws_event_loop *, struct aws_task *task, uint64_t run_at);
        int (*subscribe_to_io_events) (struct aws_event_loop *, struct aws_io_handle *, int events,
            void(*on_event)(struct aws_event_loop *, struct aws_io_handle *, void *), void *user_data);
        int (*unsubscribe_from_io_events) (struct aws_event_loop *, struct aws_io_handle *);
        BOOL (*is_on_callers_thread) (struct aws_event_loop *);
    };

Every implementation of aws_event_loop must implement this table. Let's look at some details for what each entry does.

    void (*destroy)(struct aws_event_loop *);

This function is invoked when the event-loop is finished processing and is ready to be cleaned up and deallocated.

    int (*run) (struct aws_event_loop *);

This function starts the running of the event-loop and then immediately returns. This could kick off a thread, or setup some resources to run and
receive events in a back channel API. For example, you could have an epoll loop that runs in a thread, or you could have an event-loop pumped by a system
loop such as glib, or libevent etc... and then publish events to your event-loop implementation.

    int (*stop) (struct aws_event_loop *,
     void (*on_stopped) (struct aws_event_loop *, void *), void *promise_user_data);

The stop function signals the event-loop to shutdown. This function should not block but it should remove active io handles from the
currently monitored or polled set and should begin notifying current subscribers via the on_event callback that the handle was removed._
Once the event-loop has shutdown to a safe state, it should invoke the on_stopped function.

    int (*schedule_task) (struct aws_event_loop *, struct aws_task *task, uint64_t run_at);

This function schedules a task to run in its task scheduler at the time specified by run_at. Each event-loop is responsible for implementing
a task scheduler. This function must not block, and must be thread-safe. How this is implemented will depend on platform. For example,
one reasonable implementation is if the call comes from the event-loop's thread, to queue it in the task scheduler directly. Otherwise,
write to a pipe that the event-loop is listening for events on. Upon noticing the write to the pipe, it can read the task from the pipe
and schedule the task.

`task` must be copied.

`run_at` is using the system `RAW_MONOTONIC` clock (or the closest thing to it for that platform). It is represented as nanos since unix epoch.

    int (*subscribe_to_io_events) (struct aws_event_loop *, struct aws_io_handle *, int events,
            void(*on_event)(struct aws_event_loop *, struct aws_io_handle *, int events, void *), void *user_data);

A subscriber will call this function to register an io_handle for event monitoring. This function is thread-safe.

`events` is a bit field of the events the subscriber wants to receive. A few events will always be registered (regardless of the value passed here), such
as `AWS_IO_EVENT_HANDLE_REMOVED`. The event-loop will invoke `on_event` anytime it receives one or more of the registered events.

**NOTE: The event-loop is not responsible for manipulating or setting io flags on io_handles. It will never call, read(), write(), connect(), accept(), close() etc...
on any io handle it does not explicitly own. It is the subscriber's responsibility to know how to respond to the event.**

**NOTE: The event-loop will not maintain any state other than the io handles it is polling. So, for example, in edge-triggered epoll, it does
not maintain a read ready list. It is the subscriber's responsibility to know it has more data to read or write and to schedule its tasks
appropriately.**

    int (*unsubscribe_from_io_events) (struct aws_event_loop *, struct aws_io_handle *);

A subscriber will call this function to remove its io handle from the monitored events. For example, it would may this immediately before calling
close() on a socket or pipe. `on_event` will still be invoked with `AWS_IO_EVENT_HANDLE_REMOVED` when this occurs.

    BOOL (*is_on_callers_thread) (struct aws_event_loop *);

Returns `TRUE` if the caller is on the same thread as the event-loop. Returns `FALSE` otherwise. This allows users of the event-loop to make a decision
about whether it is safe to interact with the loop directly, or if they need to schedule a task to run in the correct thread.
This function is thread-safe.

#### API

    int aws_event_loop_init_base (struct aws_allocator *, aws_clock clock, ...);

Initializes common data for all event-loops regardless of implementation. All implementations must call this function before returning from their allocation function.

    struct aws_event_loop *aws_event_loop_new_default (struct aws_allocator *, aws_clock clock, ...);

Allocates and initializes the default event-loop implementation for the current platform. Calls `aws_event_loop_init_base` before returning.

    struct aws_event_loop *aws_event_loop_destroy (struct aws_event_loop *);

Cleans up internal state of the event-loop implementation, and then calls the v-table `destroy` function.

    int aws_event_loop_fetch_local_object ( struct aws_event_loop *, void *key, void **item);

All event-loops contain local storage for all users of the event-loop to store common data into. This function is for fetching one of those objects by key. The key for this
store is of type `void *`. This function is NOT thread safe, and it expects the caller to be calling from the event-loop's thread. If this is not the case,
the caller must first schedule a task on the event-loop to enter the correct thread.

    int aws_event_loop_put_local_object ( struct aws_event_loop *, void *key, void *item);

All event-loops contain local storage for all users of the event-loop to store common data into. This function is for putting one of those objects by key. The key for this
store is of type `size_t`. This function is NOT thread safe, and it expects the caller to be calling from the event-loop's thread. If this is not the case,
the caller must first schedule a task on the event-loop to enter the correct thread.

    int aws_event_loop_remove_local_object ( struct aws_event_loop *, void *key, void **item);

All event loops contain local storage for all users of the event loop to store common data into. This function is for removing one of those objects by key. The key for this
store is of type `void *`. This function is NOT thread safe, and it expects the caller to be calling from the event loop's thread. If this is not the case,
the caller must first schedule a task on the event loop to enter the correct thread. If found, and item is not NULL, the removed item is moved to `item`.
It is the removers responsibility to free the memory pointed to by item. If it is NULL, the default deallocation strategy for the event loop will be used.

    int aws_event_loop_current_ticks ( struct aws_event_loop *, uint64_t *ticks);

Gets the current tick count/timestamp for the event loop's clock. This function is thread-safe.

#### V-Table Shims
The remaining exported functions on event loop simply invoke the v-table functions and return. See the v-table section for more details.

### Channels and Slots

#### Layout
    struct aws_channel {
        struct aws_allocator *alloc;
        struct aws_event_loop *loop;
        struct aws_channel_slot *first;
    };

    struct aws_channel_slot {
        struct aws_allocator *alloc;
        struct aws_channel *channel;
        struct aws_channel_slot *adj_left;
        struct aws_channel_slot *adj_right;
        struct aws_channel_handler *handler;
    };

#### API (Channel/Slot interaction)

    struct aws_channel_slot_ref *aws_channel_slot_new (struct aws_channel *channel);

Creates a new slot using the channel's allocator, if it is the first slot in the channel, it will be added as the first
slot in the channel. Otherwise, you'll need to use the insert or replace APIs for slots.

    int aws_channel_slot_set_handler ( struct aws_channel_slot *, struct aws_channel_handler *handler );

Sets the handler on the slot. This should only be called once per slot.

    int aws_channel_slot_remove (struct aws_channel_slot *slot);

Removes a slot from its channel. The slot and its handler will be cleaned up and deallocated.

    int aws_channel_slot_replace (struct aws_channel_slot *remove, struct aws_channel_slot *new);

Replaces `remove` in the channel with `new` and cleans up and deallocates `remove` and its handler.

    int aws_channel_slot_insert_right (struct aws_channel_slot *slot, struct aws_channel_slot_ref *right);

Adds a slot to the right of slot.

    int aws_channel_slot_insert_left (struct aws_channel_slot *slot, struct aws_channel_slot_ref *left);

Adds a slot to the left of slot.

    int aws_channel_slot_send_message (struct aws_channel_slot *slot, struct aws_io_message *message, enum aws_channel_direction dir);

Usually called by a handler, this calls the adjacent slot in the channel based on the `dir` argument. You may want to return
any unneeded messages to the channel pool to avoid unnecessary allocations.

    int aws_channel_slot_increment_read_window (struct aws_channel_slot *slot, size_t window);

Usually called by a handler, this function calls the left-adjacent slot.

    int aws_channel_slot_on_handler_shutdown_complete(struct aws_channel_slot *slot, enum aws_channel_direction dir,
                                                                  int err_code, bool abort_immediately);

Usually called by a handler, this function calls the adjacent slot's shutdown based on the `dir` argument.


### API (Channel specific)

    int aws_channel_init (struct aws_channel *channel, struct aws_allocator *alloc, struct aws_event_loop *el);

Initializes a channel for operation. The event loop will be used for driving the channel.

    int aws_channel_clean_up (struct aws_channel *channel);

Cleans up resources for the channel.

    int aws_channel_shutdown (struct aws_channel *channel,
        void (*on_shutdown_completed)(struct aws_channel *channel, void *user_data), void *user_data);

Starts the shutdown process, invokes on_shutdown_completed once each handler has shutdown.

    int aws_channel_current_clock_time( struct aws_channel *, uint64_t *ticks);

Gets the current ticks from the event loop's clock.

    int aws_channel_fetch_local_object ( struct aws_channel *, void *key, void **item);

Fetches data from the event loop's data store. This data is shared by each channel using that event loop.

    int aws_channel_put_local_object ( struct aws_channel *, void *key, void *item);

Puts data into the event loop's data store. This data is shared by each channel using that event loop.

    int aws_channel_schedule_task (struct aws_channel *, struct aws_task *task, uint64_t run_at);

Schedules a task to run on the event loop. This function is thread-safe.

    BOOL aws_channel_thread_is_callers_thread (struct aws_channel *);

Checks if the caller is on the event loop's thread. This function is thread-safe.

### Channel Handlers
Channel Handlers are runtime polymorphic. Here's some details on the virtual table (v-table):

#### Layout
    struct aws_channel_handler {
        struct aws_channel_handler_vtable *vtable;
        struct aws_allocator *alloc;
        void *impl;
    };

#### V-Table
    struct aws_channel_handler_vtable {
        int (*data_in) ( struct aws_channel_handler *handler, struct aws_channel_slot *slot,
            struct aws_io_message *message );
        int (*data_out) ( struct aws_channel_handler *handler, struct aws_channel_slot *slot,
                    struct aws_io_message *message );
        int (*on_window_update) (struct aws_channel_handler *handler, struct aws_channel_slot *slot, size_t size)
        int (*on_shutdown_notify) (struct aws_channel_handler *handler, struct aws_channel_slot *slot,
            enum aws_channel_direction dir, int error_code);
        int (*shutdown_direction) (struct aws_channel_handler *handler, struct aws_channel_slot *slot,
                    enum aws_channel_direction dir);
        size_t (*initial_window_size) (struct aws_channel_handler *handler);
        void (*destroy)(struct aws_channel_handler *handler);
    };


`int data_in ( struct aws_channel_handler *handler, struct aws_channel_slot *slot,
                           struct aws_io_message *message)`

Data in is invoked by the slot when an application level message is received in the read direction (from the io).
The job of the implementer is to process the data in msg and either notify a user or queue a new message on the slot's
read queue.

`int data_out (struct aws_channel_handler *handler, struct aws_channel_slot *slot,
                                   struct aws_io_message *message)`

Data Out is invoked by the slot when an application level message is received in the write direction (to the io).
The job of the implementer is to process the data in msg and either notify a user or queue a new message on the slot's
write queue.

`int increment_window (struct aws_channel_handler *handler, struct aws_channel_slot *slot, size_t size)`

Increment Window is invoked by the slot when a framework level message is received from a downstream handler.
It only applies in the read direction. This gives the handler a chance to make a programmatic decision about
what its read window should be. Upon receiving an update_window message, a handler decides what its window should be and
likely issues an increment window message to its slot. Shrinking a window has no effect. If a handler makes its window larger
than a downstream window, it is responsible for honoring the downstream window and buffering any data it produces that is
greater than that window.

`int (*shutdown) (struct aws_channel_handler *handler, struct aws_channel_slot *slot, enum aws_channel_direction dir,
     int error_code, bool abort_immediately);`

Shutdown is invoked by the slot when a framework level message is received from an adjacent handler.
This notifies the handler that the previous handler in the chain has shutdown and will no longer be sending or
receiving messages. The handler should make a decision about what it wants to do in response, and likely begins
its shutdown process (if any). Once the handler has safely reached a safe state, if should call
'aws_channel_slot_on_handler_shutdown_complete'

`size_t initial_window_size (struct aws_channel_handler *handler)`

When a handler is added to a slot, the slot will call this function to determine the initial window size and will propagate
a window_update message down the channel.

`void destroy(struct aws_channel_handler *handler)`

Clean up any memory or resources owned by this handler, and then deallocate the handler itself.

#### API

All exported functions, simply shim into the v-table and return.

### Sockets

We include a cross-platform API for sockets. We support TCP and UDP using IPv4 and IPv6, and Unix Domain sockets. On Windows,
we use Named Pipes to support the functionality of Unix Domain sockets. On Windows, this is implemented with winsock2, and on
all unix platforms we use the posix API.

Upon a connection being established, the new socket (either as the result of a `connect()` or `start_accept()` call)
will not be attached to any event loops. It is your responsibility to register it with an event loop to begin receiving
notifications.

#### API
    typedef enum aws_socket_domain {
        AWS_SOCKET_IPV4,
        AWS_SOCKET_IPV6,
        AWS_SOCKET_LOCAL,
        AWS_SOCKET_VSOCK,
    } aws_socket_domain;

`AWS_SOCKET_IPV4` means an IPv4 address will be used.

`AWS_SOCKET_IPV6` means an IPv6 address will be used.

`AWS_SOCKET_LOCAL` means a socket path will be used for either a Unix Domain Socket or a Named Pipe on Windows.

    typedef enum aws_socket_type {
        AWS_SOCKET_STREAM,
        AWS_SOCKET_DGRAM
    } aws_socket_type;

`AWS_SOCKET_VSOCK` means a CID address will be used. Note: VSOCK is currently only available on Linux with an appropriate VSOCK kernel driver installed. `-DUSE_VSOCK` needs to be passed during compilation to enable VSOCK support.

`AWS_SOCKET_STREAM` is TCP or a connection oriented socket.

`AWS_SOCKET_DGRAM` is UDP

    struct aws_socket_creation_args {
        void(*on_incoming_connection)(struct aws_socket *socket, struct aws_socket *new_socket, void *user_data);
        void(*on_connection_established)(struct aws_socket *socket, void *user_data);
        void(*on_error)(struct aws_socket *socket, int err_code, void *user_data);
        void *user_data;
    };

`on_incoming_connection()` will be invoked on a listening socket when new connections arrive. `socket` is the listening
socket. `new_socket` is the newly created socket. It is the connection to the remote endpoint.

NOTE: You are responsible for calling `aws_socket_clean_up()` and `aws_mem_release()` on `new_socket` when you are finished
with it.

`on_connection_established()` will be invoked after a connect call, upon a successful connection to the remote endpoint.

`on_error()` will be invoked on both listening and connecting sockets to indicate any error conditions.

    struct aws_socket_endpoint {
        char address[48];
        char socket_name[108];
        char port[10];
    };

`address` can be either an IPv4, IPv6 or VSOCK CID address. This can be used for UDP or TCP.
`socket_name` is only used in LOCAL mode.
`port` can be used for TCP or UDP.

    int aws_socket_init(struct aws_socket *socket, struct aws_allocator *alloc,
                                        struct aws_socket_options *options,
                                        struct aws_event_loop *connection_loop,
                                        struct aws_socket_creation_args *creation_args);

Initializes a socket object with socket options, an event loop to use for non-blocking operations, and callbacks to invoke
upon completion of asynchronous operations. If you are using UDP or LOCAL, `connection_loop` may be `NULL`.

    void aws_socket_clean_up(struct aws_socket *socket);

Shuts down any pending operations on the socket, and cleans up state. The socket object can be re initialized after this operation.

    int aws_socket_connect(struct aws_socket *socket, struct aws_socket_endpoint *remote_endpoint);

Connects to a remote endpoint. In UDP, this simply binds the socket to a remote address for use with `aws_socket_write()`,
and if the operation is successful, the socket can immediately be used for write operations.

In TCP, this will function will not block. If the return value is successful, then you must wait on the `on_connection_established()`
callback to be invoked before using the socket.

For LOCAL (Unix Domain Sockets or Named Pipes), the socket will be immediately ready for use upon a successful return.

    int aws_socket_bind(struct aws_socket *socket, struct aws_socket_endpoint *local_endpoint);

Binds the socket to a local address. In UDP mode, the socket is ready for `aws_socket_read()` operations. In connection oriented
modes, you still must call `aws_socket_listen()` and `aws_socket_start_accept()` before using the socket.

    int aws_socket_listen(struct aws_socket *socket, int backlog_size);

TCP and LOCAL only. Sets up the socket to listen on the address bound to in `aws_socket_bind()`.

    int aws_socket_start_accept(struct aws_socket *socket);

TCP and LOCAL only. The socket will begin accepting new connections. This is an asynchronous operation. New connections will
arrive via the `on_incoming_connection()` callback.

    int aws_socket_stop_accept(struct aws_socket *socket);

TCP and LOCAL only. The socket will shutdown the listener. It is safe to call `aws_socket_start_accept()` again after this
operation.

    int aws_socket_close(struct aws_socket *socket);

Calls `close()` on the socket and unregisters all io operations from the event loop.

    struct aws_io_handle *aws_socket_get_io_handle(struct aws_socket *socket);

Fetches the underlying io handle for use in event loop registrations and channel handlers.

    int aws_socket_set_options(struct aws_socket *socket, struct aws_socket_options *options);

Sets new socket options on the underlying socket. This is mainly useful in context of accepting a new connection via:
`on_incoming_connection()`.

    int aws_socket_read(struct aws_socket *socket, struct aws_byte_buf *buffer, size_t *amount_read);

Reads from the socket. This call is non-blocking and will return `AWS_IO_SOCKET_READ_WOULD_BLOCK` if no data is available.
`amount_read` is the amount of data read into `buffer`.

    int aws_socket_write(struct aws_socket *socket, const struct aws_byte_buf *buffer, size_t *written);

Writes to the socket. This call is non-blocking and will return `AWS_IO_SOCKET_WRITE_WOULD_BLOCK` if no data could be written.
`written` is the amount of data read from `buffer` and successfully written to `socket`.
