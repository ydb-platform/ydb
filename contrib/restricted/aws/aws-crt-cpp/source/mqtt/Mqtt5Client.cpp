/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/crt/mqtt/Mqtt5Client.h>
#include <aws/crt/mqtt/Mqtt5Packets.h>

#include <aws/crt/Api.h>
#include <aws/crt/StlAllocator.h>
#include <aws/crt/http/HttpProxyStrategy.h>
#include <aws/crt/http/HttpRequestResponse.h>
#include <aws/crt/io/Bootstrap.h>
#include <aws/iot/MqttClient.h>

#include <utility>

namespace Aws
{
    namespace Crt
    {
        namespace Mqtt5
        {
            struct PubAckCallbackData : public std::enable_shared_from_this<PubAckCallbackData>
            {
                PubAckCallbackData(Allocator *alloc = ApiAllocator()) : client(nullptr), allocator(alloc) {}

                std::shared_ptr<Mqtt5Client> client;
                OnPublishCompletionHandler onPublishCompletion;
                Allocator *allocator;
            };

            struct SubAckCallbackData
            {
                SubAckCallbackData(Allocator *alloc = ApiAllocator()) : client(nullptr), allocator(alloc) {}

                std::shared_ptr<Mqtt5Client> client;
                OnSubscribeCompletionHandler onSubscribeCompletion;
                Allocator *allocator;
            };

            struct UnSubAckCallbackData
            {
                UnSubAckCallbackData(Allocator *alloc = ApiAllocator()) : client(nullptr), allocator(alloc) {}

                std::shared_ptr<Mqtt5Client> client;
                OnUnsubscribeCompletionHandler onUnsubscribeCompletion;
                Allocator *allocator;
            };

            void Mqtt5Client::s_lifeCycleEventCallback(const struct aws_mqtt5_client_lifecycle_event *event)
            {
                Mqtt5Client *client = reinterpret_cast<Mqtt5Client *>(event->user_data);
                switch (event->event_type)
                {
                    case AWS_MQTT5_CLET_STOPPED:
                        AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "Lifecycle event: Client Stopped!");
                        if (client->onStopped)
                        {
                            OnStoppedEventData eventData;
                            client->onStopped(*client, eventData);
                        }
                        break;

                    case AWS_MQTT5_CLET_ATTEMPTING_CONNECT:
                        AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "Lifecycle event: Attempting Connect!");
                        if (client->onAttemptingConnect)
                        {
                            OnAttemptingConnectEventData eventData;
                            client->onAttemptingConnect(*client, eventData);
                        }
                        break;

                    case AWS_MQTT5_CLET_CONNECTION_FAILURE:
                        AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "Lifecycle event: Connection Failure!");
                        AWS_LOGF_INFO(
                            AWS_LS_MQTT5_CLIENT,
                            "  Error Code: %d(%s)",
                            event->error_code,
                            aws_error_debug_str(event->error_code));
                        if (client->onConnectionFailure)
                        {
                            OnConnectionFailureEventData eventData;
                            eventData.errorCode = event->error_code;
                            std::shared_ptr<ConnAckPacket> packet = nullptr;
                            if (event->connack_data != NULL)
                            {
                                packet = Aws::Crt::MakeShared<ConnAckPacket>(
                                    client->m_allocator, *event->connack_data, client->m_allocator);
                                eventData.connAckPacket = packet;
                            }
                            client->onConnectionFailure(*client, eventData);
                        }
                        break;

                    case AWS_MQTT5_CLET_CONNECTION_SUCCESS:
                        AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "Lifecycle event: Connection Success!");
                        if (client->onConnectionSuccess)
                        {
                            OnConnectionSuccessEventData eventData;

                            std::shared_ptr<ConnAckPacket> packet = nullptr;
                            if (event->connack_data != NULL)
                            {
                                packet = Aws::Crt::MakeShared<ConnAckPacket>(ApiAllocator(), *event->connack_data);
                            }

                            std::shared_ptr<NegotiatedSettings> neg_settings = nullptr;
                            if (event->settings != NULL)
                            {
                                neg_settings =
                                    Aws::Crt::MakeShared<NegotiatedSettings>(ApiAllocator(), *event->settings);
                            }

                            eventData.connAckPacket = packet;
                            eventData.negotiatedSettings = neg_settings;
                            client->onConnectionSuccess(*client, eventData);
                        }
                        break;

                    case AWS_MQTT5_CLET_DISCONNECTION:
                        AWS_LOGF_INFO(
                            AWS_LS_MQTT5_CLIENT,
                            "  Error Code: %d(%s)",
                            event->error_code,
                            aws_error_debug_str(event->error_code));
                        if (client->onDisconnection)
                        {
                            OnDisconnectionEventData eventData;
                            std::shared_ptr<DisconnectPacket> disconnection = nullptr;
                            if (event->disconnect_data != nullptr)
                            {
                                disconnection = Aws::Crt::MakeShared<DisconnectPacket>(
                                    client->m_allocator, *event->disconnect_data, client->m_allocator);
                            }
                            eventData.errorCode = event->error_code;
                            eventData.disconnectPacket = disconnection;
                            client->onDisconnection(*client, eventData);
                        }
                        break;
                }
            }

            void Mqtt5Client::s_publishReceivedCallback(
                const struct aws_mqtt5_packet_publish_view *publish,
                void *user_data)
            {
                AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "on publish recieved callback");
                Mqtt5Client *client = reinterpret_cast<Mqtt5Client *>(user_data);
                if (client != nullptr && client->onPublishReceived != nullptr)
                {
                    if (publish != NULL)
                    {
                        std::shared_ptr<PublishPacket> packet =
                            std::make_shared<PublishPacket>(*publish, client->m_allocator);
                        PublishReceivedEventData eventData;
                        eventData.publishPacket = packet;
                        client->onPublishReceived(*client, eventData);
                    }
                    else
                    {
                        AWS_LOGF_ERROR(AWS_LS_MQTT5_CLIENT, "Failed to access Publish packet view.");
                    }
                }
            }

            void Mqtt5Client::s_publishCompletionCallback(
                enum aws_mqtt5_packet_type packet_type,
                const void *publshCompletionPacket,
                int error_code,
                void *complete_ctx)
            {
                AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "Publish completion callback triggered.");
                auto callbackData = reinterpret_cast<PubAckCallbackData *>(complete_ctx);

                if (callbackData)
                {
                    std::shared_ptr<PublishResult> publish = nullptr;
                    switch (packet_type)
                    {
                        case aws_mqtt5_packet_type::AWS_MQTT5_PT_PUBACK:
                        {
                            if (publshCompletionPacket != NULL)
                            {
                                std::shared_ptr<PubAckPacket> packet = std::make_shared<PubAckPacket>(
                                    *(aws_mqtt5_packet_puback_view *)publshCompletionPacket, callbackData->allocator);
                                publish = std::make_shared<PublishResult>(std::move(packet));
                            }
                            else // This should never happened.
                            {
                                AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "The PubAck Packet is invalid.");
                                publish = std::make_shared<PublishResult>(AWS_ERROR_INVALID_ARGUMENT);
                            }
                            break;
                        }
                        case aws_mqtt5_packet_type::AWS_MQTT5_PT_NONE:
                        {
                            publish = std::make_shared<PublishResult>(error_code);
                            break;
                        }
                        default: // Invalid packet type
                        {
                            AWS_LOGF_INFO(AWS_LS_MQTT5_CLIENT, "Invalid Packet Type.");
                            publish = std::make_shared<PublishResult>(AWS_ERROR_INVALID_ARGUMENT);
                            break;
                        }
                    }
                    if (callbackData->onPublishCompletion != NULL)
                    {
                        callbackData->onPublishCompletion(callbackData->client, error_code, publish);
                    }

                    Crt::Delete(callbackData, callbackData->allocator);
                }
            }

            void Mqtt5Client::s_onWebsocketHandshake(
                struct aws_http_message *rawRequest,
                void *user_data,
                aws_mqtt5_transform_websocket_handshake_complete_fn *complete_fn,
                void *complete_ctx)
            {
                auto client = reinterpret_cast<Mqtt5Client *>(user_data);

                Allocator *allocator = client->m_allocator;
                // we have to do this because of private constructors.
                auto toSeat =
                    reinterpret_cast<Http::HttpRequest *>(aws_mem_acquire(allocator, sizeof(Http::HttpRequest)));
                toSeat = new (toSeat) Http::HttpRequest(allocator, rawRequest);

                std::shared_ptr<Http::HttpRequest> request = std::shared_ptr<Http::HttpRequest>(
                    toSeat, [allocator](Http::HttpRequest *ptr) { Crt::Delete(ptr, allocator); });

                auto onInterceptComplete =
                    [complete_fn,
                     complete_ctx](const std::shared_ptr<Http::HttpRequest> &transformedRequest, int errorCode) {
                        complete_fn(transformedRequest->GetUnderlyingMessage(), errorCode, complete_ctx);
                    };

                client->websocketInterceptor(request, onInterceptComplete);
            }

            void Mqtt5Client::s_clientTerminationCompletion(void *complete_ctx)
            {
                Mqtt5Client *client = reinterpret_cast<Mqtt5Client *>(complete_ctx);
                std::unique_lock<std::mutex> lock(client->m_terminationMutex);
                client->m_terminationPredicate = true;
                client->m_terminationCondition.notify_all();
            }

            void Mqtt5Client::s_subscribeCompletionCallback(
                const aws_mqtt5_packet_suback_view *suback,
                int error_code,
                void *complete_ctx)
            {
                SubAckCallbackData *callbackData = reinterpret_cast<SubAckCallbackData *>(complete_ctx);
                AWS_ASSERT(callbackData != nullptr);

                std::shared_ptr<SubAckPacket> packet = nullptr;
                if (suback != nullptr)
                {
                    packet = std::make_shared<SubAckPacket>(*suback, callbackData->allocator);
                }

                if (error_code != 0)
                {
                    AWS_LOGF_INFO(
                        AWS_LS_MQTT5_CLIENT,
                        "SubscribeCompletion Failed with Error Code: %d(%s)",
                        error_code,
                        aws_error_debug_str(error_code));
                }

                if (callbackData->onSubscribeCompletion)
                {
                    callbackData->onSubscribeCompletion(callbackData->client, error_code, packet);
                }
                Crt::Delete(callbackData, callbackData->allocator);
            }

            void Mqtt5Client::s_unsubscribeCompletionCallback(
                const aws_mqtt5_packet_unsuback_view *unsuback,
                int error_code,
                void *complete_ctx)
            {
                UnSubAckCallbackData *callbackData = reinterpret_cast<UnSubAckCallbackData *>(complete_ctx);
                AWS_ASSERT(callbackData != nullptr);

                std::shared_ptr<UnSubAckPacket> packet = nullptr;
                if (unsuback != nullptr)
                {
                    packet = std::make_shared<UnSubAckPacket>(*unsuback, callbackData->allocator);
                }

                if (error_code != 0)
                {
                    AWS_LOGF_INFO(
                        AWS_LS_MQTT5_CLIENT,
                        "UnsubscribeCompletion Failed with Error Code: %d(%s)",
                        error_code,
                        aws_error_debug_str(error_code));
                }

                if (callbackData->onUnsubscribeCompletion != NULL)
                {
                    callbackData->onUnsubscribeCompletion(callbackData->client, error_code, packet);
                }

                Crt::Delete(callbackData, callbackData->allocator);
            }

            Mqtt5Client::Mqtt5Client(const Mqtt5ClientOptions &options, Allocator *allocator) noexcept
                : m_client(nullptr), m_allocator(allocator)
            {
                aws_mqtt5_client_options clientOptions;

                options.initializeRawOptions(clientOptions);

                /* Setup Callbacks */
                if (options.websocketHandshakeTransform)
                {
                    this->websocketInterceptor = options.websocketHandshakeTransform;
                    clientOptions.websocket_handshake_transform = &Mqtt5Client::s_onWebsocketHandshake;
                    clientOptions.websocket_handshake_transform_user_data = this;
                }

                if (options.onConnectionFailure)
                {
                    this->onConnectionFailure = options.onConnectionFailure;
                }

                if (options.onConnectionSuccess)
                {
                    this->onConnectionSuccess = options.onConnectionSuccess;
                }

                if (options.onDisconnection)
                {
                    this->onDisconnection = options.onDisconnection;
                }

                if (options.onPublishReceived)
                {
                    this->onPublishReceived = options.onPublishReceived;
                }

                if (options.onStopped)
                {
                    this->onStopped = options.onStopped;
                }

                if (options.onAttemptingConnect)
                {
                    this->onAttemptingConnect = options.onAttemptingConnect;
                }

                clientOptions.publish_received_handler_user_data = this;
                clientOptions.publish_received_handler = &Mqtt5Client::s_publishReceivedCallback;

                clientOptions.lifecycle_event_handler = &Mqtt5Client::s_lifeCycleEventCallback;
                clientOptions.lifecycle_event_handler_user_data = this;

                clientOptions.client_termination_handler = &Mqtt5Client::s_clientTerminationCompletion;
                clientOptions.client_termination_handler_user_data = this;

                m_client = aws_mqtt5_client_new(allocator, &clientOptions);
            }

            Mqtt5Client::~Mqtt5Client()
            {
                if (m_client != nullptr)
                {
                    aws_mqtt5_client_release(m_client);
                    std::unique_lock<std::mutex> lock(m_terminationMutex);
                    m_terminationCondition.wait(lock, [this] { return m_terminationPredicate == true; });
                    m_client = nullptr;
                }
            }

            std::shared_ptr<Mqtt5Client> Mqtt5Client::NewMqtt5Client(
                const Mqtt5ClientOptions &options,
                Allocator *allocator) noexcept
            {
                /* Copied from MqttClient.cpp:ln754 */
                // As the constructor is private, make share would not work here. We do make_share manually.
                Mqtt5Client *toSeat = reinterpret_cast<Mqtt5Client *>(aws_mem_acquire(allocator, sizeof(Mqtt5Client)));
                if (!toSeat)
                {
                    return nullptr;
                }

                toSeat = new (toSeat) Mqtt5Client(options, allocator);
                return std::shared_ptr<Mqtt5Client>(
                    toSeat, [allocator](Mqtt5Client *client) { Crt::Delete(client, allocator); });
            }

            Mqtt5Client::operator bool() const noexcept { return m_client != nullptr; }

            int Mqtt5Client::LastError() const noexcept { return aws_last_error(); }

            bool Mqtt5Client::Start() const noexcept { return aws_mqtt5_client_start(m_client) == AWS_OP_SUCCESS; }

            bool Mqtt5Client::Stop() noexcept { return aws_mqtt5_client_stop(m_client, NULL, NULL) == AWS_OP_SUCCESS; }

            bool Mqtt5Client::Stop(std::shared_ptr<DisconnectPacket> disconnectOptions) noexcept
            {
                if (disconnectOptions == nullptr)
                {
                    return Stop();
                }

                aws_mqtt5_packet_disconnect_view disconnect_packet;
                AWS_ZERO_STRUCT(disconnect_packet);
                if (disconnectOptions->initializeRawOptions(disconnect_packet) == false)
                {
                    return false;
                }
                return aws_mqtt5_client_stop(m_client, &disconnect_packet, NULL) == AWS_OP_SUCCESS;
            }

            bool Mqtt5Client::Publish(
                std::shared_ptr<PublishPacket> publishOptions,
                OnPublishCompletionHandler onPublishCmpletionCallback) noexcept
            {
                if (publishOptions == nullptr)
                {
                    return false;
                }

                aws_mqtt5_packet_publish_view publish;
                publishOptions->initializeRawOptions(publish);

                PubAckCallbackData *pubCallbackData = Aws::Crt::New<PubAckCallbackData>(m_allocator);

                pubCallbackData->client = this->getptr();
                pubCallbackData->allocator = m_allocator;
                pubCallbackData->onPublishCompletion = onPublishCmpletionCallback;

                aws_mqtt5_publish_completion_options options;

                options.completion_callback = Mqtt5Client::s_publishCompletionCallback;
                options.completion_user_data = pubCallbackData;

                int result = aws_mqtt5_client_publish(m_client, &publish, &options);
                if (result != AWS_OP_SUCCESS)
                {
                    Crt::Delete(pubCallbackData, pubCallbackData->allocator);
                    return false;
                }
                return true;
            }

            bool Mqtt5Client::Subscribe(
                std::shared_ptr<SubscribePacket> subscribeOptions,
                OnSubscribeCompletionHandler onSubscribeCompletionCallback) noexcept
            {
                if (subscribeOptions == nullptr)
                {
                    return false;
                }
                /* Setup packet_subscribe */
                aws_mqtt5_packet_subscribe_view subscribe;

                subscribeOptions->initializeRawOptions(subscribe);

                /* Setup subscription Completion callback*/
                SubAckCallbackData *subCallbackData = Aws::Crt::New<SubAckCallbackData>(m_allocator);

                subCallbackData->client = this->getptr();
                subCallbackData->allocator = m_allocator;
                subCallbackData->onSubscribeCompletion = onSubscribeCompletionCallback;

                aws_mqtt5_subscribe_completion_options options;

                options.completion_callback = Mqtt5Client::s_subscribeCompletionCallback;
                options.completion_user_data = subCallbackData;

                /* Subscribe to topic */
                int result = aws_mqtt5_client_subscribe(m_client, &subscribe, &options);
                if (result != AWS_OP_SUCCESS)
                {
                    Crt::Delete(subCallbackData, subCallbackData->allocator);
                    return false;
                }
                return result == AWS_OP_SUCCESS;
            }

            bool Mqtt5Client::Unsubscribe(
                std::shared_ptr<UnsubscribePacket> unsubscribeOptions,
                OnUnsubscribeCompletionHandler onUnsubscribeCompletionCallback) noexcept
            {
                if (unsubscribeOptions == nullptr)
                {
                    return false;
                }

                aws_mqtt5_packet_unsubscribe_view unsubscribe;
                unsubscribeOptions->initializeRawOptions(unsubscribe);

                UnSubAckCallbackData *unSubCallbackData = Aws::Crt::New<UnSubAckCallbackData>(m_allocator);

                unSubCallbackData->client = this->getptr();
                unSubCallbackData->allocator = m_allocator;
                unSubCallbackData->onUnsubscribeCompletion = onUnsubscribeCompletionCallback;

                aws_mqtt5_unsubscribe_completion_options options;

                options.completion_callback = Mqtt5Client::s_unsubscribeCompletionCallback;
                options.completion_user_data = unSubCallbackData;

                int result = aws_mqtt5_client_unsubscribe(m_client, &unsubscribe, &options);
                if (result != AWS_OP_SUCCESS)
                {
                    Crt::Delete(unSubCallbackData, unSubCallbackData->allocator);
                    return false;
                }
                return result == AWS_OP_SUCCESS;
            }

            const Mqtt5ClientOperationStatistics &Mqtt5Client::GetOperationStatistics() noexcept
            {
                aws_mqtt5_client_operation_statistics m_operationStatisticsNative = {0, 0, 0, 0};
                if (m_client != nullptr)
                {
                    aws_mqtt5_client_get_stats(m_client, &m_operationStatisticsNative);
                    m_operationStatistics.incompleteOperationCount =
                        m_operationStatisticsNative.incomplete_operation_count;
                    m_operationStatistics.incompleteOperationSize =
                        m_operationStatisticsNative.incomplete_operation_size;
                    m_operationStatistics.unackedOperationCount = m_operationStatisticsNative.unacked_operation_count;
                    m_operationStatistics.unackedOperationSize = m_operationStatisticsNative.unacked_operation_size;
                }
                return m_operationStatistics;
            }

            /*****************************************************
             *
             * Mqtt5ClientOptions
             *
             *****************************************************/

            /**
             * Mqtt5ClientOptions
             */
            Mqtt5ClientOptions::Mqtt5ClientOptions(Crt::Allocator *allocator) noexcept
                : m_bootstrap(nullptr), m_sessionBehavior(ClientSessionBehaviorType::AWS_MQTT5_CSBT_DEFAULT),
                  m_extendedValidationAndFlowControlOptions(AWS_MQTT5_EVAFCO_AWS_IOT_CORE_DEFAULTS),
                  m_offlineQueueBehavior(AWS_MQTT5_COQBT_DEFAULT),
                  m_reconnectionOptions({AWS_EXPONENTIAL_BACKOFF_JITTER_DEFAULT, 0, 0, 0}), m_pingTimeoutMs(0),
                  m_connackTimeoutMs(0), m_ackTimeoutSec(0), m_allocator(allocator)
            {
                m_socketOptions.SetSocketType(Io::SocketType::Stream);
                AWS_ZERO_STRUCT(m_packetConnectViewStorage);
                AWS_ZERO_STRUCT(m_httpProxyOptionsStorage);
            }

            bool Mqtt5ClientOptions::initializeRawOptions(aws_mqtt5_client_options &raw_options) const noexcept
            {
                AWS_ZERO_STRUCT(raw_options);

                raw_options.host_name = ByteCursorFromString(m_hostName);
                raw_options.port = m_port;

                if (m_bootstrap == nullptr)
                {
                    raw_options.bootstrap = ApiHandle::GetOrCreateStaticDefaultClientBootstrap()->GetUnderlyingHandle();
                }
                else
                {
                    raw_options.bootstrap = m_bootstrap->GetUnderlyingHandle();
                }
                raw_options.socket_options = &m_socketOptions.GetImpl();
                if (m_tlsConnectionOptions.has_value())
                {
                    raw_options.tls_options = m_tlsConnectionOptions.value().GetUnderlyingHandle();
                }

                if (m_proxyOptions.has_value())
                {
                    raw_options.http_proxy_options = &m_httpProxyOptionsStorage;
                }

                raw_options.connect_options = &m_packetConnectViewStorage;
                raw_options.session_behavior = m_sessionBehavior;
                raw_options.extended_validation_and_flow_control_options = m_extendedValidationAndFlowControlOptions;
                raw_options.offline_queue_behavior = m_offlineQueueBehavior;
                raw_options.retry_jitter_mode = m_reconnectionOptions.m_reconnectMode;
                raw_options.max_reconnect_delay_ms = m_reconnectionOptions.m_maxReconnectDelayMs;
                raw_options.min_reconnect_delay_ms = m_reconnectionOptions.m_minReconnectDelayMs;
                raw_options.min_connected_time_to_reset_reconnect_delay_ms =
                    m_reconnectionOptions.m_minConnectedTimeToResetReconnectDelayMs;
                raw_options.ping_timeout_ms = m_pingTimeoutMs;
                raw_options.connack_timeout_ms = m_connackTimeoutMs;
                raw_options.ack_timeout_seconds = m_ackTimeoutSec;

                return true;
            }

            Mqtt5ClientOptions::~Mqtt5ClientOptions() {}

            Mqtt5ClientOptions &Mqtt5ClientOptions::withHostName(Crt::String hostname)
            {
                m_hostName = std::move(hostname);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withPort(uint16_t port) noexcept
            {
                m_port = port;
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withBootstrap(Io::ClientBootstrap *bootStrap) noexcept
            {
                m_bootstrap = bootStrap;
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withSocketOptions(Io::SocketOptions socketOptions) noexcept
            {
                m_socketOptions = std::move(socketOptions);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withTlsConnectionOptions(
                const Io::TlsConnectionOptions &tslOptions) noexcept
            {
                m_tlsConnectionOptions = tslOptions;
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withHttpProxyOptions(
                const Crt::Http::HttpClientConnectionProxyOptions &proxyOptions) noexcept
            {
                m_proxyOptions = proxyOptions;
                m_proxyOptions->InitializeRawProxyOptions(m_httpProxyOptionsStorage);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withConnectOptions(
                std::shared_ptr<ConnectPacket> packetConnect) noexcept
            {
                m_connectOptions = packetConnect;
                m_connectOptions->initializeRawOptions(m_packetConnectViewStorage, m_allocator);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withSessionBehavior(
                ClientSessionBehaviorType sessionBehavior) noexcept
            {
                m_sessionBehavior = sessionBehavior;
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withClientExtendedValidationAndFlowControl(
                ClientExtendedValidationAndFlowControl clientExtendedValidationAndFlowControl) noexcept
            {
                m_extendedValidationAndFlowControlOptions = clientExtendedValidationAndFlowControl;
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withOfflineQueueBehavior(
                ClientOperationQueueBehaviorType offlineQueueBehavior) noexcept
            {
                m_offlineQueueBehavior = offlineQueueBehavior;
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withReconnectOptions(ReconnectOptions reconnectOptions) noexcept
            {
                m_reconnectionOptions = reconnectOptions;

                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withPingTimeoutMs(uint32_t pingTimeoutMs) noexcept
            {
                m_pingTimeoutMs = pingTimeoutMs;
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withConnackTimeoutMs(uint32_t connackTimeoutMs) noexcept
            {
                m_connackTimeoutMs = connackTimeoutMs;
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withAckTimeoutSeconds(uint32_t ackTimeoutSeconds) noexcept
            {
                m_ackTimeoutSec = ackTimeoutSeconds;
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withWebsocketHandshakeTransformCallback(
                OnWebSocketHandshakeIntercept callback) noexcept
            {
                websocketHandshakeTransform = std::move(callback);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withClientConnectionSuccessCallback(
                OnConnectionSuccessHandler callback) noexcept
            {
                onConnectionSuccess = std::move(callback);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withClientConnectionFailureCallback(
                OnConnectionFailureHandler callback) noexcept
            {
                onConnectionFailure = std::move(callback);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withClientDisconnectionCallback(
                OnDisconnectionHandler callback) noexcept
            {
                onDisconnection = std::move(callback);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withClientStoppedCallback(OnStoppedHandler callback) noexcept
            {
                onStopped = std::move(callback);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withClientAttemptingConnectCallback(
                OnAttemptingConnectHandler callback) noexcept
            {
                onAttemptingConnect = std::move(callback);
                return *this;
            }

            Mqtt5ClientOptions &Mqtt5ClientOptions::withPublishReceivedCallback(
                OnPublishReceivedHandler callback) noexcept
            {
                onPublishReceived = std::move(callback);
                return *this;
            }

        } // namespace Mqtt5
    }     // namespace Crt
} // namespace Aws
