# libvhost-server architecture

`libvhost-server` (henceforth `libvhost`) is a component to help implementing
vhost-user device servers.  Its main purpose is to insulate the server from the
vhost-user control protocol, memory mapping, address translation, virtio queue
processing, and so on.

It's designed to transfer requests and responses between the guest drivers and
the backend efficiently, with as little added latency and device's mutual
interference as possible.

This component is implemented as a library.  An application implementing a
vhost-user device (slave in vhost-user speak) links to this library and calls
into the API it exposes (so it's called _user_ hereinafter); this allows it to
accept connections from a _client_ (vhost-user master, e.g. qemu) process its
virtio queues.

## Lockless event loops and bottom halves

For efficiency in a highly-concurrent environment the library implements
lockless event loops: it tends to avoid using sleeping synchronization
primitives like mutexes.

To coordinate different contexts that execute event loops, _bottom halves_ are
used: functions that are scheduled to run (soon) on the target event loop.

## Contexts of execution

The library assumes there are different kinds of execution contexts:

1. dataplane aka request queue event loop

   This is where virtio queues are processed.  There may be multiple request
   queue event loops.  Typically every request queue event loop is run in its
   own thread.  Every virtio queue is associated permanently with a request
   queue.  _Currently all virtio queues of a device are associated with a
   single request queue, but this limitation will be lifted._

   Request queue event loop is supposed to be run explicitly by the user.  On
   each iteration, the event loop blocks until a host notification is signaled
   on any of its virtio queues.  Once it's woken up, it extracts all available
   virtio elements from all signaled virtio queues and forms device requests
   out of them.  It may then process some simple ones of them synchronously;
   otherwise it enqueues them in a double-ended queue, common for all
   associated virtio queues (this allows to avoid starvation).

   The user dequeues the requests from this request queue and submits them for
   asynchronous processing in another context outside of `libvhost` scope.

   Once the request is fully processed, it submits a completion function (via
   bottom half) back onto the request queue event loop; this leads to releasing
   the resources associated with the request and publishing the result to the
   client.

2. control event loop

   This is a **single** library-global event loop handling state transitions
   and vhost-user socket communications for all devices served by the library.
   The thread running the control event loop is owned by the library: it's
   created at the library initialization and stopped on deinitialization.

3. external contexts

   These are external contexts that initialize and deinitialize devices to be
   handled by the library.  These operations are **not** lockless: the
   respective functions block until the control loop acknowledges the requested
   state transition of the device.

## Device state machine

Device (`struct vhd_vdev`) state transitions happen in response to client
actions -- connect, disconnect, vhost-user control messages, and user actions
-- device start and stop.  They all happen in the control event loop.

However, some of the device state transitions require associated state
transitions in the device virtio queues (`struct vhd_vring`).  In order to
maintain lockless nature of the library, such device state transitions happen
in several stages:

- first the transition is started in the control event loop where input
  parameters are verified and internal state is prepared, to be later exposed
  in the dataplane

- then corresponding state transitions of the virtio queues in the respective
  request queue event loop(s) are scheduled via bottom halves

- those, in turn, put the internal state prepared earlier into effect in the
  dataplane and signal completion of the transition via bottom halves back to
  the control event loop

- then the device state transition finishes and the reply is sent to the
  client, if needed.

The messages from a single client are never handled concurrently: upon
reception of a message the state machine suspends reception of further messages
on the socket until the current one is fully handled and the reply is sent.
The only action which may intervene is the device disconnection, either due to
the client shutting down its end or due to the external context issuing the
device stop.

### User-initiated device stop or client disconnection

Certain complexity arises from the fact that when the device disconnection
happens some requests on the device may have been dequeued from the request
queue and submitted to the backend for asynchronous handling.

Therefore disconnection is multi-stage:

- if the disconnection is initiated by the user doing a device stop call in an
  external context, it passes the request to the device in the control event
  loop and blocks on a semaphore

- the socket connected to the client is closed

- all virtio queues are requested to stop via bottom halves in the request
  queue event loop(s)

- in the request queue event loop

  * the virtio queue is stopped, i.e. no more requests are fetched from the
    virtio queue

  * the requests that have already been fetched from the virtio queue but still
    remain in the request queue are canceled: completed immediately with a
    special status such that the completion is not exposed to the client,
    effectively dropping the request, in the expectation that it will be
    resubmitted by the vhost-user in_flight mechanism; this ensures that no
    more requests from this virtio queue will enter the backend

  * the virtio queue acknowledges the stop to the device in the control event
    loop context via a bottom half

- if the disconnection was user-initiated, once the device sees all its virtio
  queues acknowledge the stop, it releases the semaphore so that the device
  stop call unblocks and returns in its external context; from this point on
  the backend is guaranteed that no more requests will be submitted

- once all device requests that were caught in the backend by the disconnection
  are completed, leaving no more requests in the whole pipeline and thus
  ensuring that nothing will touch the guest memory any more, the device
  proceeds with the cleanup:

  * if it the disconnection was user-initiated, the device shuts itself down,
    closes the listening socket, releases remaining resources and executes a
    previously set up callback to inform the external context that the device
    is fully terminated and freed

  * otherwise the device resets its state and resumes listening for incoming
    connections.

### Live migration support

Live migration support basically adheres to the virtio spec, with a notable
extension, which appears underspecified there:

Before the `VHOST_USER_GET_VRING_BASE` message is replied to, all requests in
the virtio queue are drained and completed to the client, leaving no in-flight
requests and thus making it safe to resume operation upon migration.

### Reconnection support

The library supports starting in a mode where the client survived the server
premature termination and wants to re-establish the connection and resume
operation.

The library basically adheres to the spec in this regard, with a few points of
note:

- the in-flight region is initially created in a memfd (there's no requirement
  that it's in memfd when the connection is re-established0)

- when the device is stopped by the user while there still is an open
  connection with the client, the requests that happen to be in flight are
  canceled, ensuring that the backend internals don't touch the client's memory
  after the stop call is acknowledged.
