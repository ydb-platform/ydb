#pragma once
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/crt/http/HttpConnection.h>
#include <aws/crt/mqtt/Mqtt5Types.h>

namespace Aws
{
    namespace Crt
    {
        namespace Mqtt5
        {
            class ConnectPacket;
            class ConnAckPacket;
            class DisconnectPacket;
            class Mqtt5Client;
            class Mqtt5ClientOptions;
            class NegotiatedSettings;
            class PublishResult;
            class PublishPacket;
            class PubAckPacket;
            class SubscribePacket;
            class SubAckPacket;
            class UnsubscribePacket;
            class UnSubAckPacket;

            struct AWS_CRT_CPP_API ReconnectOptions
            {
                /**
                 * Controls how the reconnect delay is modified in order to smooth out the distribution of reconnection
                 * attempt timepoints for a large set of reconnecting clients.
                 */
                JitterMode m_reconnectMode;

                /**
                 * Minimum amount of time to wait to reconnect after a disconnect.  Exponential backoff is performed
                 * with jitter after each connection failure.
                 */
                uint64_t m_minReconnectDelayMs;

                /**
                 * Maximum amount of time to wait to reconnect after a disconnect.  Exponential backoff is performed
                 * with jitter after each connection failure.
                 */
                uint64_t m_maxReconnectDelayMs;

                /**
                 * Amount of time that must elapse with an established connection before the reconnect delay is reset to
                 * the minimum. This helps alleviate bandwidth-waste in fast reconnect cycles due to permission failures
                 * on operations.
                 */
                uint64_t m_minConnectedTimeToResetReconnectDelayMs;
            };

            /* Simple statistics about the current state of the client's queue of operations */
            struct AWS_CRT_CPP_API Mqtt5ClientOperationStatistics
            {
                /*
                 * total number of operations submitted to the client that have not yet been completed.  Unacked
                 * operations are a subset of this.
                 */
                uint64_t incompleteOperationCount;

                /*
                 * total packet size of operations submitted to the client that have not yet been completed.  Unacked
                 * operations are a subset of this.
                 */
                uint64_t incompleteOperationSize;

                /*
                 * total number of operations that have been sent to the server and are waiting for a corresponding ACK
                 * before they can be completed.
                 */
                uint64_t unackedOperationCount;

                /*
                 * total packet size of operations that have been sent to the server and are waiting for a corresponding
                 * ACK before they can be completed.
                 */
                uint64_t unackedOperationSize;
            };

            /**
             * The data returned when AttemptingConnect is invoked in the LifecycleEvents callback.
             * Currently empty, but may be used in the future for passing additional data.
             */
            struct AWS_CRT_CPP_API OnAttemptingConnectEventData
            {
                OnAttemptingConnectEventData() {}
            };

            /**
             * The data returned when OnConnectionFailure is invoked in the LifecycleEvents callback.
             */
            struct AWS_CRT_CPP_API OnConnectionFailureEventData
            {
                OnConnectionFailureEventData() : errorCode(AWS_ERROR_SUCCESS), connAckPacket(nullptr) {}

                int errorCode;
                std::shared_ptr<ConnAckPacket> connAckPacket;
            };

            /**
             * The data returned when OnConnectionSuccess is invoked in the LifecycleEvents callback.
             */
            struct AWS_CRT_CPP_API OnConnectionSuccessEventData
            {
                OnConnectionSuccessEventData() : connAckPacket(nullptr), negotiatedSettings(nullptr) {}

                std::shared_ptr<ConnAckPacket> connAckPacket;
                std::shared_ptr<NegotiatedSettings> negotiatedSettings;
            };

            /**
             * The data returned when OnDisconnect is invoked in the LifecycleEvents callback.
             */
            struct AWS_CRT_CPP_API OnDisconnectionEventData
            {
                OnDisconnectionEventData() : errorCode(AWS_ERROR_SUCCESS), disconnectPacket(nullptr) {}

                int errorCode;
                std::shared_ptr<DisconnectPacket> disconnectPacket;
            };

            /**
             * The data returned when OnStopped is invoked in the LifecycleEvents callback.
             * Currently empty, but may be used in the future for passing additional data.
             */
            struct AWS_CRT_CPP_API OnStoppedEventData
            {
                OnStoppedEventData() {}
            };

            /**
             * The data returned when a publish is made to a topic the MQTT5 client is subscribed to.
             */
            struct AWS_CRT_CPP_API PublishReceivedEventData
            {
                PublishReceivedEventData() : publishPacket(nullptr) {}
                std::shared_ptr<PublishPacket> publishPacket;
            };

            /**
             * Type signature of the callback invoked when connection succeed
             * Mandatory event fields: client, connack_data, settings
             */
            using OnConnectionSuccessHandler = std::function<void(Mqtt5Client &, const OnConnectionSuccessEventData &)>;

            /**
             * Type signature of the callback invoked when connection failed
             */
            using OnConnectionFailureHandler = std::function<void(Mqtt5Client &, const OnConnectionFailureEventData &)>;

            /**
             * Type signature of the callback invoked when the internal connection is shutdown
             */
            using OnDisconnectionHandler = std::function<void(Mqtt5Client &, const OnDisconnectionEventData &)>;

            /**
             * Type signature of the callback invoked when attempting connect to client
             * Mandatory event fields: client
             */
            using OnAttemptingConnectHandler = std::function<void(Mqtt5Client &, const OnAttemptingConnectEventData &)>;

            /**
             * Type signature of the callback invoked when client connection stopped
             * Mandatory event fields: client
             */
            using OnStoppedHandler = std::function<void(Mqtt5Client &, const OnStoppedEventData &)>;

            /**
             * Type signature of the callback invoked when a Disconnection Comlete
             *
             */
            using OnDisconnectCompletionHandler = std::function<void(std::shared_ptr<Mqtt5Client>, int)>;

            /**
             * Type signature of the callback invoked when a Publish Complete
             */
            using OnPublishCompletionHandler =
                std::function<void(std::shared_ptr<Mqtt5Client>, int, std::shared_ptr<PublishResult>)>;

            /**
             * Type signature of the callback invoked when a Subscribe Complete
             */
            using OnSubscribeCompletionHandler =
                std::function<void(std::shared_ptr<Mqtt5Client>, int, std::shared_ptr<SubAckPacket>)>;

            /**
             * Type signature of the callback invoked when a Unsubscribe Complete
             */
            using OnUnsubscribeCompletionHandler =
                std::function<void(std::shared_ptr<Mqtt5Client>, int, std::shared_ptr<UnSubAckPacket>)>;

            /**
             * Type signature of the callback invoked when a PacketPublish message received (OnMessageHandler)
             */
            using OnPublishReceivedHandler = std::function<void(Mqtt5Client &, const PublishReceivedEventData &)>;

            /**
             * Callback for users to invoke upon completion of, presumably asynchronous, OnWebSocketHandshakeIntercept
             * callback's initiated process.
             */
            using OnWebSocketHandshakeInterceptComplete =
                std::function<void(const std::shared_ptr<Http::HttpRequest> &, int)>;

            /**
             * Invoked during websocket handshake to give users opportunity to transform an http request for purposes
             * such as signing/authorization etc... Returning from this function does not continue the websocket
             * handshake since some work flows may be asynchronous. To accommodate that, onComplete must be invoked upon
             * completion of the signing process.
             */
            using OnWebSocketHandshakeIntercept =
                std::function<void(std::shared_ptr<Http::HttpRequest>, const OnWebSocketHandshakeInterceptComplete &)>;

            /**
             * An MQTT5 client. This is a move-only type. Unless otherwise specified,
             * all function arguments need only to live through the duration of the
             * function call.
             */
            class AWS_CRT_CPP_API Mqtt5Client final : public std::enable_shared_from_this<Mqtt5Client>
            {
              public:
                /**
                 * Factory function for mqtt5 client
                 *
                 * @param options: Mqtt5 Client Options
                 * @param allocator allocator to use
                 * @return a new mqtt5 client
                 */
                static std::shared_ptr<Mqtt5Client> NewMqtt5Client(
                    const Mqtt5ClientOptions &options,
                    Allocator *allocator = ApiAllocator()) noexcept;

                /**
                 * Get shared poitner of the Mqtt5Client. Mqtt5Client is inherited to enable_shared_from_this to help
                 * with memory safety.
                 *
                 * @return shared_ptr for the Mqtt5Client
                 */
                std::shared_ptr<Mqtt5Client> getptr() { return shared_from_this(); }

                /**
                 * @return true if the instance is in a valid state, false otherwise.
                 */
                operator bool() const noexcept;

                /**
                 * @return the value of the last aws error encountered by operations on this instance.
                 */
                int LastError() const noexcept;

                /**
                 * Notifies the MQTT5 client that you want it to attempt to connect to the configured endpoint.
                 * The client will attempt to stay connected using the properties of the reconnect-related parameters
                 * from the client configuration.
                 *
                 * @return bool: true if operation succeed, otherwise false.
                 */
                bool Start() const noexcept;

                /**
                 * Notifies the MQTT5 client that you want it to transition to the stopped state, disconnecting any
                 * existing connection and stopping subsequent reconnect attempts.
                 *
                 * @return bool: true if operation succeed, otherwise false
                 */
                bool Stop() noexcept;

                /**
                 * Notifies the MQTT5 client that you want it to transition to the stopped state, disconnecting any
                 * existing connection and stopping subsequent reconnect attempts.
                 *
                 * @param disconnectOptions (optional) properties of a DISCONNECT packet to send as part of the shutdown
                 * process
                 *
                 * @return bool: true if operation succeed, otherwise false
                 */
                bool Stop(std::shared_ptr<DisconnectPacket> disconnectOptions) noexcept;

                /**
                 * Tells the client to attempt to send a PUBLISH packet
                 *
                 * @param publishOptions: packet PUBLISH to send to the server
                 * @param onPublishCompletionCallback: callback on publish complete, default to NULL
                 *
                 * @return true if the publish operation succeed otherwise false
                 */
                bool Publish(
                    std::shared_ptr<PublishPacket> publishOptions,
                    OnPublishCompletionHandler onPublishCompletionCallback = NULL) noexcept;

                /**
                 * Tells the client to attempt to subscribe to one or more topic filters.
                 *
                 * @param subscribeOptions: SUBSCRIBE packet to send to the server
                 * @param onSubscribeCompletionCallback: callback on subscribe complete, default to NULL
                 *
                 * @return true if the subscription operation succeed otherwise false
                 */
                bool Subscribe(
                    std::shared_ptr<SubscribePacket> subscribeOptions,
                    OnSubscribeCompletionHandler onSubscribeCompletionCallback = NULL) noexcept;

                /**
                 * Tells the client to attempt to unsubscribe to one or more topic filters.
                 *
                 * @param unsubscribeOptions: UNSUBSCRIBE packet to send to the server
                 * @param onUnsubscribeCompletionCallback: callback on unsubscribe complete, default to NULL
                 *
                 * @return true if the unsubscription operation succeed otherwise false
                 */
                bool Unsubscribe(
                    std::shared_ptr<UnsubscribePacket> unsubscribeOptions,
                    OnUnsubscribeCompletionHandler onUnsubscribeCompletionCallback = NULL) noexcept;

                /**
                 * Get the statistics about the current state of the client's queue of operations
                 *
                 * @return Mqtt5ClientOperationStatistics
                 */
                const Mqtt5ClientOperationStatistics &GetOperationStatistics() noexcept;

                virtual ~Mqtt5Client();

              private:
                Mqtt5Client(const Mqtt5ClientOptions &options, Allocator *allocator = ApiAllocator()) noexcept;

                /* Static Callbacks */
                static void s_publishCompletionCallback(
                    enum aws_mqtt5_packet_type packet_type,
                    const void *packet,
                    int error_code,
                    void *complete_ctx);

                static void s_subscribeCompletionCallback(
                    const struct aws_mqtt5_packet_suback_view *puback,
                    int error_code,
                    void *complete_ctx);

                static void s_unsubscribeCompletionCallback(
                    const struct aws_mqtt5_packet_unsuback_view *puback,
                    int error_code,
                    void *complete_ctx);

                static void s_lifeCycleEventCallback(const aws_mqtt5_client_lifecycle_event *event);

                static void s_publishReceivedCallback(const aws_mqtt5_packet_publish_view *publish, void *user_data);

                static void s_onWebsocketHandshake(
                    aws_http_message *rawRequest,
                    void *user_data,
                    aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
                    void *complete_ctx);

                static void s_clientTerminationCompletion(void *complete_ctx);

                /* The handler is set by clientoptions */
                OnWebSocketHandshakeIntercept websocketInterceptor;
                /**
                 * Callback handler trigged when client successfully establishes an MQTT connection
                 */
                OnConnectionSuccessHandler onConnectionSuccess;

                /**
                 * Callback handler trigged when client fails to establish an MQTT connection
                 */
                OnConnectionFailureHandler onConnectionFailure;

                /**
                 * Callback handler trigged when client's current MQTT connection is closed
                 */
                OnDisconnectionHandler onDisconnection;

                /**
                 * Callback handler trigged when client reaches the "Stopped" state
                 */
                OnStoppedHandler onStopped;

                /**
                 * Callback handler trigged when client begins an attempt to connect to the remote endpoint.
                 */
                OnAttemptingConnectHandler onAttemptingConnect;

                /**
                 * Callback handler trigged when an MQTT PUBLISH packet is received by the client
                 */
                OnPublishReceivedHandler onPublishReceived;
                aws_mqtt5_client *m_client;
                Allocator *m_allocator;

                Mqtt5ClientOperationStatistics m_operationStatistics;
                std::condition_variable m_terminationCondition;
                std::mutex m_terminationMutex;
                bool m_terminationPredicate = false;
            };

            /**
             * Configuration interface for mqtt5 clients
             */
            class AWS_CRT_CPP_API Mqtt5ClientOptions final
            {

                friend class Mqtt5Client;

              public:
                /**
                 * Default constructior of Mqtt5ClientOptions
                 */
                Mqtt5ClientOptions(Crt::Allocator *allocator = ApiAllocator()) noexcept;

                /**
                 * Sets host to connect to.
                 *
                 * @param hostname endpoint to connect to
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withHostName(Crt::String hostname);

                /**
                 * Set port to connect to
                 *
                 * @param port port to connect to
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withPort(uint16_t port) noexcept;

                /**
                 * Set booststrap for mqtt5 client
                 *
                 * @param bootStrap bootstrap used for mqtt5 client. The default ClientBootstrap see
                 * Aws::Crt::ApiHandle::GetOrCreateStaticDefaultClientBootstrap.
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withBootstrap(Io::ClientBootstrap *bootStrap) noexcept;

                /**
                 * Sets the aws socket options
                 *
                 * @param socketOptions  Io::SocketOptions used to setup socket
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withSocketOptions(Io::SocketOptions socketOptions) noexcept;

                /**
                 * Sets the tls connection options
                 *
                 * @param tslOptions  Io::TlsConnectionOptions
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withTlsConnectionOptions(const Io::TlsConnectionOptions &tslOptions) noexcept;

                /**
                 * Sets http proxy options.
                 *
                 * @param proxyOptions http proxy configuration for connection establishment
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withHttpProxyOptions(
                    const Crt::Http::HttpClientConnectionProxyOptions &proxyOptions) noexcept;

                /**
                 * Sets mqtt5 connection options
                 *
                 * @param packetConnect package connection options
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withConnectOptions(std::shared_ptr<ConnectPacket> packetConnect) noexcept;

                /**
                 * Sets session behavior. Overrides how the MQTT5 client should behave with respect to MQTT sessions.
                 *
                 * @param sessionBehavior
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withSessionBehavior(ClientSessionBehaviorType sessionBehavior) noexcept;

                /**
                 * Sets client extended validation and flow control, additional controls for client behavior with
                 * respect to operation validation and flow control; these checks go beyond the base MQTT5 spec to
                 * respect limits of specific MQTT brokers.
                 *
                 * @param clientExtendedValidationAndFlowControl
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withClientExtendedValidationAndFlowControl(
                    ClientExtendedValidationAndFlowControl clientExtendedValidationAndFlowControl) noexcept;

                /**
                 * Sets OfflineQueueBehavior, controls how disconnects affect the queued and in-progress operations
                 * tracked by the client.  Also controls how new operations are handled while the client is not
                 * connected.  In particular, if the client is not connected, then any operation that would be failed
                 * on disconnect (according to these rules) will also be rejected.
                 *
                 * @param offlineQueueBehavior
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withOfflineQueueBehavior(
                    ClientOperationQueueBehaviorType offlineQueueBehavior) noexcept;

                /**
                 * Sets ReconnectOptions. Reconnect options, includes retryJitterMode, min reconnect delay time and
                 * max reconnect delay time and reset reconnect delay time
                 *
                 * @param reconnectOptions
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withReconnectOptions(ReconnectOptions reconnectOptions) noexcept;

                /**
                 * Sets ping timeout (ms). Time interval to wait after sending a PINGREQ for a PINGRESP to arrive.
                 * If one does not arrive, the client will close the current connection.
                 *
                 * @param pingTimeoutMs
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withPingTimeoutMs(uint32_t pingTimeoutMs) noexcept;

                /**
                 * Sets Connack Timeout (ms). Time interval to wait after sending a CONNECT request for a CONNACK
                 * to arrive.  If one does not arrive, the connection will be shut down.
                 *
                 * @param connackTimeoutMs
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withConnackTimeoutMs(uint32_t connackTimeoutMs) noexcept;

                /**
                 * Sets Operation Timeout(Seconds). Time interval to wait for an ack after sending a QoS 1+ PUBLISH,
                 * SUBSCRIBE, or UNSUBSCRIBE before failing the operation.
                 *
                 * @param ackTimeoutSeconds
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withAckTimeoutSeconds(uint32_t ackTimeoutSeconds) noexcept;

                /**
                 * Sets callback for transform HTTP request.
                 * This callback allows a custom transformation of the HTTP request that acts as the websocket
                 * handshake. Websockets will be used if this is set to a valid transformation callback.  To use
                 * websockets but not perform a transformation, just set this as a trivial completion callback.  If
                 * undefined, the connection will be made with direct MQTT.
                 *
                 * @param callback
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withWebsocketHandshakeTransformCallback(
                    OnWebSocketHandshakeIntercept callback) noexcept;

                /**
                 * Sets callback trigged when client successfully establishes an MQTT connection
                 *
                 * @param callback
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withClientConnectionSuccessCallback(OnConnectionSuccessHandler callback) noexcept;

                /**
                 * Sets callback trigged when client fails to establish an MQTT connection
                 *
                 * @param callback
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withClientConnectionFailureCallback(OnConnectionFailureHandler callback) noexcept;

                /**
                 * Sets callback trigged when client's current MQTT connection is closed
                 *
                 * @param callback
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withClientDisconnectionCallback(OnDisconnectionHandler callback) noexcept;

                /**
                 * Sets callback trigged when client reaches the "Stopped" state
                 *
                 * @param callback
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withClientStoppedCallback(OnStoppedHandler callback) noexcept;

                /**
                 * Sets callback trigged when client begins an attempt to connect to the remote endpoint.
                 *
                 * @param callback
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withClientAttemptingConnectCallback(OnAttemptingConnectHandler callback) noexcept;

                /**
                 * Sets callback trigged when a PUBLISH packet is received by the client
                 *
                 * @param callback
                 *
                 * @return this option object
                 */
                Mqtt5ClientOptions &withPublishReceivedCallback(OnPublishReceivedHandler callback) noexcept;

                /**
                 * Initializes the C aws_mqtt5_client_options from Mqtt5ClientOptions. For internal use
                 *
                 * @param raw_options - output parameter containing low level client options to be passed to the C
                 * interface
                 *
                 */
                bool initializeRawOptions(aws_mqtt5_client_options &raw_options) const noexcept;

                virtual ~Mqtt5ClientOptions();
                Mqtt5ClientOptions(const Mqtt5ClientOptions &) = delete;
                Mqtt5ClientOptions(Mqtt5ClientOptions &&) = delete;
                Mqtt5ClientOptions &operator=(const Mqtt5ClientOptions &) = delete;
                Mqtt5ClientOptions &operator=(Mqtt5ClientOptions &&) = delete;

              private:
                /**
                 * This callback allows a custom transformation of the HTTP request that acts as the websocket
                 * handshake. Websockets will be used if this is set to a valid transformation callback.  To use
                 * websockets but not perform a transformation, just set this as a trivial completion callback.  If
                 * undefined, the connection will be made with direct MQTT.
                 */
                OnWebSocketHandshakeIntercept websocketHandshakeTransform;

                /**
                 * Callback handler trigged when client successfully establishes an MQTT connection
                 */
                OnConnectionSuccessHandler onConnectionSuccess;

                /**
                 * Callback handler trigged when client fails to establish an MQTT connection
                 */
                OnConnectionFailureHandler onConnectionFailure;

                /**
                 * Callback handler trigged when client's current MQTT connection is closed
                 */
                OnDisconnectionHandler onDisconnection;

                /**
                 * Callback handler trigged when client reaches the "Stopped" state
                 *
                 * @param Mqtt5Client: The shared client
                 */
                OnStoppedHandler onStopped;

                /**
                 * Callback handler trigged when client begins an attempt to connect to the remote endpoint.
                 *
                 * @param Mqtt5Client: The shared client
                 */
                OnAttemptingConnectHandler onAttemptingConnect;

                /**
                 * Callback handler trigged when an MQTT PUBLISH packet is received by the client
                 *
                 * @param Mqtt5Client: The shared client
                 * @param PublishPacket: received Publish Packet
                 */
                OnPublishReceivedHandler onPublishReceived;

                /**
                 * Host name of the MQTT server to connect to.
                 */
                Crt::String m_hostName;

                /**
                 * Network port of the MQTT server to connect to.
                 */
                uint16_t m_port;

                /**
                 * Client bootstrap to use.  In almost all cases, this can be left undefined.
                 */
                Io::ClientBootstrap *m_bootstrap;

                /**
                 * Controls socket properties of the underlying MQTT connections made by the client.  Leave undefined to
                 * use defaults (no TCP keep alive, 10 second socket timeout).
                 */
                Crt::Io::SocketOptions m_socketOptions;

                /**
                 * TLS context for secure socket connections.
                 * If undefined, then a plaintext connection will be used.
                 */
                Crt::Optional<Crt::Io::TlsConnectionOptions> m_tlsConnectionOptions;

                /**
                 * Configures (tunneling) HTTP proxy usage when establishing MQTT connections
                 */
                Crt::Optional<Crt::Http::HttpClientConnectionProxyOptions> m_proxyOptions;

                /**
                 * All configurable options with respect to the CONNECT packet sent by the client, including the will.
                 * These connect properties will be used for every connection attempt made by the client.
                 */
                std::shared_ptr<ConnectPacket> m_connectOptions;

                /**
                 * Controls how the MQTT5 client should behave with respect to MQTT sessions.
                 */
                ClientSessionBehaviorType m_sessionBehavior;

                /**
                 * Additional controls for client behavior with respect to operation validation and flow control; these
                 * checks go beyond the base MQTT5 spec to respect limits of specific MQTT brokers.
                 */
                ClientExtendedValidationAndFlowControl m_extendedValidationAndFlowControlOptions;

                /**
                 * Controls how disconnects affect the queued and in-progress operations tracked by the client.  Also
                 * controls how new operations are handled while the client is not connected.  In particular, if the
                 * client is not connected, then any operation that would be failed on disconnect (according to these
                 * rules) will also be rejected.
                 */
                ClientOperationQueueBehaviorType m_offlineQueueBehavior;

                /**
                 * Reconnect options, includes retryJitterMode, min reconnect delay time and max reconnect delay time
                 */
                ReconnectOptions m_reconnectionOptions;

                /**
                 * Time interval to wait after sending a PINGREQ for a PINGRESP to arrive.  If one does not arrive, the
                 * client will close the current connection.
                 */
                uint32_t m_pingTimeoutMs;

                /**
                 * Time interval to wait after sending a CONNECT request for a CONNACK to arrive.  If one does not
                 * arrive, the connection will be shut down.
                 */
                uint32_t m_connackTimeoutMs;

                /**
                 * Time interval to wait for an ack after sending a QoS 1+ PUBLISH, SUBSCRIBE, or UNSUBSCRIBE before
                 * failing the operation.
                 */
                uint32_t m_ackTimeoutSec;

                /* Underlying Parameters */
                Crt::Allocator *m_allocator;
                aws_http_proxy_options m_httpProxyOptionsStorage;
                aws_mqtt5_packet_connect_view m_packetConnectViewStorage;
            };

        } // namespace Mqtt5
    }     // namespace Crt
} // namespace Aws
