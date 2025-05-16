## AWS C MQTT

C99 implementation of the MQTT 3.1.1 and MQTT 5 specifications.

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

#### Building aws-c-mqtt and Remaining Dependencies

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

git clone git@github.com:awslabs/aws-c-compression.git
cmake -S aws-c-compression -B aws-c-compression/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-compression/build --target install

git clone git@github.com:awslabs/aws-c-http.git
cmake -S aws-c-http -B aws-c-http/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-http/build --target install

git clone git@github.com:awslabs/aws-c-mqtt.git
cmake -S aws-c-mqtt -B aws-c-mqtt/build -DCMAKE_INSTALL_PREFIX=<install-path> -DCMAKE_PREFIX_PATH=<install-path>
cmake --build aws-c-mqtt/build --target install
```

### Overview

This library contains an MQTT implementation that is simple and easy to use, but also quite powerful and low on
unnecessary copies. Here is a general overview of the API:

### `struct aws_mqtt_client;`

`aws_mqtt_client` is meant to be created once per application to pool common resources required for opening MQTT
connections. The instance does not need to be allocated, and may be managed by the user.

```c
int aws_mqtt_client_init(
    struct aws_mqtt_client *client,
    struct aws_allocator *allocator,
    struct aws_event_loop_group *elg);
```
Initializes an instance of `aws_mqtt_client` with the required parameters.
* `client` is effectively the `this` parameter.
* `allocator` will be used to initialize the client (note that the client itself is NOT allocated).
    *This resource must outlive `client`*.
* `bootstrap` will be used to initiate new socket connections MQTT.
    *This resource must outlive `client`*.
    See [aws-c-io][aws-c-io] for more information about `aws_client_bootstrap`.

```c
void aws_mqtt_client_clean_up(struct aws_mqtt_client *client);
```
Cleans up a client and frees all owned resources.

**NOTE**: DO NOT CALL THIS FUNCTION UNTIL ALL OUTSTANDING CONNECTIONS ARE CLOSED.

### `struct aws_mqtt_client_connection;`

```c
struct aws_mqtt_client_connection *aws_mqtt_client_connection_new(
    struct aws_mqtt_client *client,
    struct aws_mqtt_client_connection_callbacks callbacks,
    const struct aws_byte_cursor *host_name,
    uint16_t port,
    struct aws_socket_options *socket_options,
    struct aws_tls_ctx_options *tls_options);
```
Allocates and initializes a new connection object (does NOT actually connect). You may use the returned object to
configure connection parameters, and then call `aws_mqtt_client_connection_connect` to actually open the connection.
* `client` is required in order to use an existing DNS resolver, event loop group, and allocator.
* `callbacks` provides the connection-level (not operation level) callbacks and the userdata to be given back.
* `host_name` lists the end point to connect to. This may be a DNS address or an IP address.
    *This resource may be freed immediately after return.*
* `port` the port to connect to on `host_name`.
* `socket_options` describes how to open the connection.
    See [aws-c-io][aws-c-io] for more information about `aws_socket_options`.
* `tls_options` provides TLS credentials to connect with. Pass `NULL` to not use TLS (**NOT RECOMMENDED**).
    See [aws-c-io][aws-c-io] for more information about `aws_tls_ctx_options`.

```c
void aws_mqtt_client_connection_destroy(struct aws_mqtt_client_connection *connection);
```
Destroys a connection and frees all outstanding resources.

**NOTE**: DO NOT CALL THIS FUNCTION UNTIL THE CONNECTION IS CLOSED.

```c
int aws_mqtt_client_connection_set_will(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    bool retain,
    const struct aws_byte_cursor *payload);
```
Sets the last will and testament to be distributed by the server upon client disconnection. Must be called before
`aws_mqtt_client_connection_connect`. See `aws_mqtt_client_connection_publish` for information on the parameters.
`topic` and `payload` must persist past the call to `aws_mqtt_client_connection_connect`.

```c
int aws_mqtt_client_connection_set_login(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *username,
    const struct aws_byte_cursor *password);
```
Sets the username and password to be sent to the server on connection. Must be called before
`aws_mqtt_client_connection_connect`. `username` and `password` must persist past the call to
`aws_mqtt_client_connection_connect`.

```c
int aws_mqtt_client_connection_set_reconnect_timeout(
    struct aws_mqtt_client_connection *connection,
    uint64_t min_timeout,
    uint64_t max_timeout);
```
Sets the minimum and maximum reconnect timeouts. The time between reconnect attempts will start at min and multipy by 2
until max is reached.

```c
int aws_mqtt_client_connection_connect(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *client_id,
    bool clean_session,
    uint16_t keep_alive_time);
```
Connects to the remote endpoint. The parameters here are set in the MQTT CONNECT packet directly. `client_id` must persist until the `on_connack` connection callback is called.

```c
int aws_mqtt_client_connection_disconnect(struct aws_mqtt_client_connection *connection);
```
Closes an open connection. Does not clean up any resources, that's to be done by `aws_mqtt_client_connection_destroy`,
probably from the `on_disconnected` connection callback.

```c
uint16_t aws_mqtt_client_connection_subscribe_single(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    enum aws_mqtt_qos qos,
    aws_mqtt_client_publish_received_fn *on_publish,
    void *on_publish_ud,
    aws_mqtt_suback_single_fn *on_suback,
    void *on_suback_ud);
```
Subscribes to the topic filter given with the given QoS. `on_publish` will be called whenever a packet matching
`topic_filter` arrives. `on_suback` will be called when the SUBACK packet has been received. `topic_filter` must persist until `on_suback` is called. The packet_id of the SUBSCRIBE packet will be returned, or 0 on error.

```c
uint16_t aws_mqtt_client_connection_unsubscribe(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic_filter,
    aws_mqtt_op_complete_fn *on_unsuback,
    void *on_unsuback_ud);
```
Unsubscribes from the topic filter given. `topic_filter` must persist until `on_unsuback` is called. The packet_id of
the UNSUBSCRIBE packet will be returned, or 0 on error.

```c
uint16_t aws_mqtt_client_connection_publish(
    struct aws_mqtt_client_connection *connection,
    const struct aws_byte_cursor *topic,
    enum aws_mqtt_qos qos,
    bool retain,
    const struct aws_byte_cursor *payload,
    aws_mqtt_op_complete_fn *on_complete,
    void *userdata);
```
Publish a payload to the topic specified. For QoS 0, `on_complete` will be called as soon as the packet is sent over
the wire. For QoS 1, as soon as PUBACK comes back. For QoS 2, PUBCOMP. `topic` and `payload` must persist until
`on_complete`.

```c
int aws_mqtt_client_connection_ping(struct aws_mqtt_client_connection *connection);
```
Sends a PINGREQ packet to the server. 

[aws-c-io]: https://github.com/awslabs/aws-c-io
