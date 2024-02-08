#pragma once
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/mqtt/v5/mqtt5_client.h>
#include <aws/mqtt/v5/mqtt5_types.h>

namespace Aws
{
    namespace Crt
    {
        namespace Mqtt5
        {
            /**
             * MQTT message delivery quality of service.
             *
             * Enum values match [MQTT5
             * spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901234) encoding values.
             */
            using QOS = aws_mqtt5_qos;

            /**
             * Server return code for connect attempts.
             *
             * Enum values match [MQTT5
             * spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079) encoding values.
             */
            using ConnectReasonCode = aws_mqtt5_connect_reason_code;

            /**
             * Reason code inside DISCONNECT packets.  Helps determine why a connection was terminated.
             *
             * Enum values match [MQTT5
             * spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901208) encoding values.
             */
            using DisconnectReasonCode = aws_mqtt5_disconnect_reason_code;

            /**
             * Reason code inside PUBACK packets
             *
             * Data model of an [MQTT5
             * PUBACK](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901121) packet
             */
            using PubAckReasonCode = aws_mqtt5_puback_reason_code;

            /**
             * Reason code inside PUBACK packets that indicates the result of the associated PUBLISH request.
             *
             * Enum values match [MQTT5
             * spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901124) encoding values.
             */
            using SubAckReasonCode = aws_mqtt5_suback_reason_code;

            /**
             * Reason codes inside UNSUBACK packet payloads that specify the results for each topic filter in the
             * associated UNSUBSCRIBE packet.
             *
             * Enum values match [MQTT5
             * spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901194) encoding values.
             */
            using UnSubAckReasonCode = aws_mqtt5_unsuback_reason_code;

            /**
             * Controls how the MQTT5 client should behave with respect to MQTT sessions.
             */
            using ClientSessionBehaviorType = aws_mqtt5_client_session_behavior_type;

            /**
             * Additional controls for client behavior with respect to operation validation and flow control; these
             * checks go beyond the MQTT5 spec to respect limits of specific MQTT brokers.
             */
            using ClientExtendedValidationAndFlowControl = aws_mqtt5_extended_validation_and_flow_control_options;

            /**
             * Controls how disconnects affect the queued and in-progress operations tracked by the client.  Also
             * controls how operations are handled while the client is not connected.  In particular, if the client is
             * not connected, then any operation that would be failed on disconnect (according to these rules) will be
             * rejected.
             */
            using ClientOperationQueueBehaviorType = aws_mqtt5_client_operation_queue_behavior_type;

            /**
             * Controls how the reconnect delay is modified in order to smooth out the distribution of reconnection
             * attempt timepoints for a large set of reconnecting clients.
             *
             * See [Exponential Backoff and
             * Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)
             */
            using JitterMode = aws_exponential_backoff_jitter_mode;

            /**
             * Optional property describing a PUBLISH payload's format.
             *
             * Enum values match [MQTT5
             * spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901111) encoding values.
             */
            using PayloadFormatIndicator = aws_mqtt5_payload_format_indicator;

            /**
             * Configures how retained messages should be handled when subscribing with a topic filter that matches
             * topics with associated retained messages.
             *
             * Enum values match [MQTT5
             * spec](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901169) encoding values.
             */
            using RetainHandlingType = aws_mqtt5_retain_handling_type;

            /**
             * Type of mqtt packet.
             * Enum values match mqtt spec encoding values.
             *
             * https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901022
             */
            using PacketType = aws_mqtt5_packet_type;

        } // namespace Mqtt5

    } // namespace Crt
} // namespace Aws
