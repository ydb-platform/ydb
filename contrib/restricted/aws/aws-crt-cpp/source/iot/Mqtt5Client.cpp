/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/crt/Api.h>
#include <aws/crt/auth/Credentials.h>
#include <aws/crt/auth/Sigv4Signing.h>
#include <aws/crt/http/HttpRequestResponse.h>
#include <aws/crt/mqtt/Mqtt5Packets.h>

#include <aws/iot/Mqtt5Client.h>

#if !BYO_CRYPTO

namespace Aws
{
    namespace Iot
    {
        static Crt::String AddToUsernameParameter(
            Crt::String currentUsername,
            Crt::String parameterValue,
            Crt::String parameterPreText)
        {
            Crt::String return_string = currentUsername;
            if (return_string.find("?") != Crt::String::npos)
            {
                return_string += "&";
            }
            else
            {
                return_string += "?";
            }

            if (parameterValue.find(parameterPreText) != Crt::String::npos)
            {
                return return_string + parameterValue;
            }
            else
            {
                return return_string + parameterPreText + parameterValue;
            }
        }

        static bool buildMqtt5FinalUsername(
            Crt::Optional<Mqtt5CustomAuthConfig> customAuthConfig,
            Crt::String &username)
        {
            if (customAuthConfig.has_value())
            {
                /* If we're using token-signing authentication, then all token properties must be set */
                bool usingSigning = false;
                if (customAuthConfig->GetTokenValue().has_value() || customAuthConfig->GetTokenKeyName().has_value() ||
                    customAuthConfig->GetTokenSignature().has_value())
                {
                    usingSigning = true;
                    if (!customAuthConfig->GetTokenValue().has_value() ||
                        !customAuthConfig->GetTokenKeyName().has_value() ||
                        !customAuthConfig->GetTokenSignature().has_value())
                    {
                        return false;
                    }
                }
                Crt::String usernameString = "";

                if (!customAuthConfig->GetUsername().has_value())
                {
                    if (!username.empty())
                    {
                        usernameString += username;
                    }
                }
                else
                {
                    usernameString += customAuthConfig->GetUsername().value();
                }

                if (customAuthConfig->GetAuthorizerName().has_value())
                {
                    usernameString = AddToUsernameParameter(
                        usernameString, customAuthConfig->GetAuthorizerName().value(), "x-amz-customauthorizer-name=");
                }
                if (usingSigning)
                {
                    usernameString = AddToUsernameParameter(
                        usernameString,
                        customAuthConfig->GetTokenValue().value(),
                        customAuthConfig->GetTokenKeyName().value() + "=");
                    usernameString = AddToUsernameParameter(
                        usernameString,
                        customAuthConfig->GetTokenSignature().value(),
                        "x-amz-customauthorizer-signature=");
                }

                username = usernameString;
            }
            return true;
        }

        /*****************************************************
         *
         * Mqtt5ClientOptionsBuilder
         *
         *****************************************************/

        Mqtt5ClientBuilder::Mqtt5ClientBuilder(Crt::Allocator *allocator) noexcept
            : m_allocator(allocator), m_port(0), m_lastError(0), m_enableMetricsCollection(true)
        {
            m_options = new Crt::Mqtt5::Mqtt5ClientOptions(allocator);
        }

        Mqtt5ClientBuilder::Mqtt5ClientBuilder(int error, Crt::Allocator *allocator) noexcept
            : m_allocator(allocator), m_options(nullptr), m_lastError(error)
        {
        }

        Mqtt5ClientBuilder *Mqtt5ClientBuilder::NewMqtt5ClientBuilderWithMtlsFromPath(
            const Crt::String hostName,
            const char *certPath,
            const char *pkeyPath,
            Crt::Allocator *allocator) noexcept
        {
            Mqtt5ClientBuilder *result = new Mqtt5ClientBuilder(allocator);
            result->m_tlsConnectionOptions =
                Crt::Io::TlsContextOptions::InitClientWithMtls(certPath, pkeyPath, allocator);
            if (!result->m_tlsConnectionOptions.value())
            {
                result->m_lastError = result->m_tlsConnectionOptions->LastError();
                return result;
            }
            result->withHostName(hostName);
            return result;
        }

        Mqtt5ClientBuilder *Mqtt5ClientBuilder::NewMqtt5ClientBuilderWithMtlsFromMemory(
            const Crt::String hostName,
            const Crt::ByteCursor &cert,
            const Crt::ByteCursor &pkey,
            Crt::Allocator *allocator) noexcept
        {
            Mqtt5ClientBuilder *result = new Mqtt5ClientBuilder(allocator);
            result->m_tlsConnectionOptions = Crt::Io::TlsContextOptions::InitClientWithMtls(cert, pkey, allocator);
            if (!result->m_tlsConnectionOptions.value())
            {
                result->m_lastError = result->m_tlsConnectionOptions->LastError();
                return result;
            }
            result->withHostName(hostName);
            return result;
        }

        Mqtt5ClientBuilder *Mqtt5ClientBuilder::NewMqtt5ClientBuilderWithMtlsPkcs11(
            const Crt::String hostName,
            const Crt::Io::TlsContextPkcs11Options &pkcs11Options,
            Crt::Allocator *allocator) noexcept
        {
            Mqtt5ClientBuilder *result = new Mqtt5ClientBuilder(allocator);
            result->m_tlsConnectionOptions =
                Crt::Io::TlsContextOptions::InitClientWithMtlsPkcs11(pkcs11Options, allocator);
            if (!result->m_tlsConnectionOptions.value())
            {
                result->m_lastError = result->m_tlsConnectionOptions->LastError();
                return result;
            }
            result->withHostName(hostName);
            return result;
        }

        Mqtt5ClientBuilder *Mqtt5ClientBuilder::NewMqtt5ClientBuilderWithWindowsCertStorePath(
            const Crt::String hostName,
            const char *windowsCertStorePath,
            Crt::Allocator *allocator) noexcept
        {
            Mqtt5ClientBuilder *result = new Mqtt5ClientBuilder(allocator);
            result->m_tlsConnectionOptions =
                Crt::Io::TlsContextOptions::InitClientWithMtlsSystemPath(windowsCertStorePath, allocator);
            if (!result->m_tlsConnectionOptions.value())
            {
                result->m_lastError = result->m_tlsConnectionOptions->LastError();
                return result;
            }
            result->withHostName(hostName);
            return result;
        }

        Mqtt5ClientBuilder *Mqtt5ClientBuilder::NewMqtt5ClientBuilderWithWebsocket(
            const Crt::String hostName,
            const WebsocketConfig &config,
            Crt::Allocator *allocator) noexcept
        {
            Mqtt5ClientBuilder *result = new Mqtt5ClientBuilder(allocator);
            result->m_tlsConnectionOptions = Crt::Io::TlsContextOptions::InitDefaultClient();
            result->withHostName(hostName);
            result->m_websocketConfig = config;
            return result;
        }

        Mqtt5ClientBuilder *Mqtt5ClientBuilder::NewMqtt5ClientBuilderWithCustomAuthorizer(
            const Crt::String hostName,
            const Mqtt5CustomAuthConfig &customAuthConfig,
            Crt::Allocator *allocator) noexcept
        {
            Mqtt5ClientBuilder *result = new Mqtt5ClientBuilder(allocator);
            result->m_tlsConnectionOptions = Crt::Io::TlsContextOptions::InitDefaultClient();
            result->withHostName(hostName);
            result->WithCustomAuthorizer(customAuthConfig);
            return result;
        }

        Mqtt5ClientBuilder *Mqtt5ClientBuilder::NewMqtt5ClientBuilderWithCustomAuthorizerWebsocket(
            const Crt::String hostName,
            const Mqtt5CustomAuthConfig &customAuthConfig,
            const WebsocketConfig &config,
            Crt::Allocator *allocator) noexcept
        {
            Mqtt5ClientBuilder *result = new Mqtt5ClientBuilder(allocator);
            result->m_tlsConnectionOptions = Crt::Io::TlsContextOptions::InitDefaultClient();
            result->withHostName(hostName);
            result->m_websocketConfig = config;
            result->WithCustomAuthorizer(customAuthConfig);
            return result;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withHostName(const Crt::String hostName)
        {
            m_options->withHostName(hostName);
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withPort(uint16_t port) noexcept
        {
            m_port = port;
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::WithCertificateAuthority(const char *caPath) noexcept
        {
            if (m_tlsConnectionOptions)
            {
                if (!m_tlsConnectionOptions->OverrideDefaultTrustStore(nullptr, caPath))
                {
                    m_lastError = m_tlsConnectionOptions->LastError();
                }
            }
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::WithCertificateAuthority(const Crt::ByteCursor &cert) noexcept
        {
            if (m_tlsConnectionOptions)
            {
                if (!m_tlsConnectionOptions->OverrideDefaultTrustStore(cert))
                {
                    m_lastError = m_tlsConnectionOptions->LastError();
                }
            }
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withHttpProxyOptions(
            const Crt::Http::HttpClientConnectionProxyOptions &proxyOptions) noexcept
        {
            m_proxyOptions = proxyOptions;
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::WithCustomAuthorizer(const Iot::Mqtt5CustomAuthConfig &config) noexcept
        {
            m_customAuthConfig = config;
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withConnectOptions(
            std::shared_ptr<ConnectPacket> packetConnect) noexcept
        {
            m_connectOptions = packetConnect;
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withSessionBehavior(ClientSessionBehaviorType sessionBehavior) noexcept
        {
            m_options->withSessionBehavior(sessionBehavior);
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withClientExtendedValidationAndFlowControl(
            ClientExtendedValidationAndFlowControl clientExtendedValidationAndFlowControl) noexcept
        {
            m_options->withClientExtendedValidationAndFlowControl(clientExtendedValidationAndFlowControl);
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withOfflineQueueBehavior(
            ClientOperationQueueBehaviorType operationQueueBehavior) noexcept
        {
            m_options->withAckTimeoutSeconds(operationQueueBehavior);
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withReconnectOptions(ReconnectOptions reconnectOptions) noexcept
        {
            m_options->withReconnectOptions(reconnectOptions);
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withPingTimeoutMs(uint32_t pingTimeoutMs) noexcept
        {
            m_options->withPingTimeoutMs(pingTimeoutMs);
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withConnackTimeoutMs(uint32_t connackTimeoutMs) noexcept
        {
            m_options->withConnackTimeoutMs(connackTimeoutMs);
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withAckTimeoutSeconds(uint32_t ackTimeoutSeconds) noexcept
        {
            m_options->withAckTimeoutSeconds(ackTimeoutSeconds);
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::WithSdkName(const Crt::String &sdkName)
        {
            m_sdkName = sdkName;
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::WithSdkVersion(const Crt::String &sdkVersion)
        {
            m_sdkVersion = sdkVersion;
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withClientConnectionSuccessCallback(
            OnConnectionSuccessHandler callback) noexcept
        {
            m_options->withClientConnectionSuccessCallback(std::move(callback));
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withClientConnectionFailureCallback(
            OnConnectionFailureHandler callback) noexcept
        {
            m_options->withClientConnectionFailureCallback(std::move(callback));
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withClientDisconnectionCallback(
            OnDisconnectionHandler callback) noexcept
        {
            m_options->withClientDisconnectionCallback(std::move(callback));
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withClientStoppedCallback(OnStoppedHandler callback) noexcept
        {
            m_options->withClientStoppedCallback(std::move(callback));
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withClientAttemptingConnectCallback(
            OnAttemptingConnectHandler callback) noexcept
        {
            m_options->withClientAttemptingConnectCallback(std::move(callback));
            return *this;
        }

        Mqtt5ClientBuilder &Mqtt5ClientBuilder::withPublishReceivedCallback(OnPublishReceivedHandler callback) noexcept
        {
            m_options->withPublishReceivedCallback(std::move(callback));
            return *this;
        }

        std::shared_ptr<Mqtt5Client> Mqtt5ClientBuilder::Build() noexcept
        {
            if (m_lastError != 0)
            {
                return nullptr;
            }

            uint16_t port = m_port;

            if (!port) // port is default to 0
            {
                if (m_websocketConfig || Crt::Io::TlsContextOptions::IsAlpnSupported())
                {
                    port = 443;
                }
                else
                {
                    port = 8883;
                }
            }

            if (port == 443 && !m_websocketConfig && Crt::Io::TlsContextOptions::IsAlpnSupported() &&
                !m_customAuthConfig.has_value())
            {
                if (!m_tlsConnectionOptions->SetAlpnList("x-amzn-mqtt-ca"))
                {
                    return nullptr;
                }
            }

            if (m_customAuthConfig.has_value())
            {
                if (port != 443)
                {
                    AWS_LOGF_WARN(
                        AWS_LS_MQTT_GENERAL,
                        "Attempting to connect to authorizer with unsupported port. Port is not 443...");
                }
                if (!m_websocketConfig)
                {
                    if (!m_tlsConnectionOptions->SetAlpnList("mqtt"))
                    {
                        return nullptr;
                    }
                }
            }

            // add metrics string to username (if metrics enabled)
            if (m_enableMetricsCollection || m_customAuthConfig.has_value())
            {
                Crt::String username = "";
                if (m_connectOptions != nullptr)
                {
                    if (m_connectOptions->getUsername().has_value())
                        username = m_connectOptions->getUsername().value();
                }
                else
                {
                    m_connectOptions = std::make_shared<ConnectPacket>(m_allocator);
                }

                if (m_customAuthConfig.has_value())
                {
                    if (!buildMqtt5FinalUsername(m_customAuthConfig, username))
                    {
                        AWS_LOGF_ERROR(
                            AWS_LS_MQTT5_CLIENT,
                            "Failed to setup CustomAuthorizerConfig, please check if the parameters are set "
                            "correctly.");
                        return nullptr;
                    }
                    if (m_customAuthConfig->GetPassword().has_value())
                    {
                        m_connectOptions->withPassword(m_customAuthConfig->GetPassword().value());
                    }
                }

                if (m_enableMetricsCollection)
                {
                    username = AddToUsernameParameter(username, "SDK", m_sdkName);
                    username = AddToUsernameParameter(username, "Version", m_sdkName);
                }
                m_connectOptions->withUserName(username);
            }

            auto tlsContext =
                Crt::Io::TlsContext(m_tlsConnectionOptions.value(), Crt::Io::TlsMode::CLIENT, m_allocator);
            if (!tlsContext)
            {
                return nullptr;
            }

            m_options->withPort(port).withTlsConnectionOptions(tlsContext.NewConnectionOptions());

            if (m_connectOptions != nullptr)
            {
                m_options->withConnectOptions(m_connectOptions);
            }

            if (m_websocketConfig.has_value())
            {
                auto websocketConfig = m_websocketConfig.value();
                auto signerTransform = [websocketConfig](
                                           std::shared_ptr<Crt::Http::HttpRequest> req,
                                           const Crt::Mqtt::OnWebSocketHandshakeInterceptComplete &onComplete) {
                    // it is only a very happy coincidence that these function signatures match. This is the callback
                    // for signing to be complete. It invokes the callback for websocket handshake to be complete.
                    auto signingComplete =
                        [onComplete](const std::shared_ptr<Aws::Crt::Http::HttpRequest> &req1, int errorCode) {
                            onComplete(req1, errorCode);
                        };

                    auto signerConfig = websocketConfig.CreateSigningConfigCb();

                    websocketConfig.Signer->SignRequest(req, *signerConfig, signingComplete);
                };

                m_options->withWebsocketHandshakeTransformCallback(signerTransform);
                bool useWebsocketProxyOptions =
                    m_websocketConfig->ProxyOptions.has_value() && !m_proxyOptions.has_value();
                if (useWebsocketProxyOptions)
                {
                    m_options->withHttpProxyOptions(m_websocketConfig->ProxyOptions.value());
                }
                else if (m_proxyOptions.has_value())
                {
                    m_options->withHttpProxyOptions(m_proxyOptions.value());
                }
            }

            return Crt::Mqtt5::Mqtt5Client::NewMqtt5Client(*m_options, m_allocator);
        }

        Aws::Iot::Mqtt5CustomAuthConfig::Mqtt5CustomAuthConfig(Crt::Allocator *allocator) noexcept
            : m_allocator(allocator)
        {
            AWS_ZERO_STRUCT(m_passwordStorage);
        }

        Aws::Iot::Mqtt5CustomAuthConfig::~Mqtt5CustomAuthConfig() { aws_byte_buf_clean_up(&m_passwordStorage); }

        Aws::Iot::Mqtt5CustomAuthConfig::Mqtt5CustomAuthConfig(const Mqtt5CustomAuthConfig &rhs)
        {
            if (&rhs != this)
            {
                m_allocator = rhs.m_allocator;
                if (rhs.m_authorizerName.has_value())
                {
                    m_authorizerName = rhs.m_authorizerName.value();
                }
                if (rhs.m_tokenKeyName.has_value())
                {
                    m_tokenKeyName = rhs.m_tokenKeyName.value();
                }
                if (rhs.m_tokenSignature.has_value())
                {
                    m_tokenSignature = rhs.m_tokenSignature.value();
                }
                if (rhs.m_tokenValue.has_value())
                {
                    m_tokenValue = rhs.m_tokenValue.value();
                }
                if (rhs.m_username.has_value())
                {
                    m_username = rhs.m_username.value();
                }
                if (rhs.m_password.has_value())
                {
                    AWS_ZERO_STRUCT(m_passwordStorage);
                    aws_byte_buf_init_copy_from_cursor(&m_passwordStorage, m_allocator, rhs.m_password.value());
                    m_password = aws_byte_cursor_from_buf(&m_passwordStorage);
                }
            }
        }

        Mqtt5CustomAuthConfig &Aws::Iot::Mqtt5CustomAuthConfig::operator=(const Mqtt5CustomAuthConfig &rhs)
        {
            if (&rhs != this)
            {
                m_allocator = rhs.m_allocator;
                if (rhs.m_authorizerName.has_value())
                {
                    m_authorizerName = rhs.m_authorizerName.value();
                }
                if (rhs.m_tokenKeyName.has_value())
                {
                    m_tokenKeyName = rhs.m_tokenKeyName.value();
                }
                if (rhs.m_tokenSignature.has_value())
                {
                    m_tokenSignature = rhs.m_tokenSignature.value();
                }
                if (rhs.m_tokenValue.has_value())
                {
                    m_tokenValue = rhs.m_tokenValue.value();
                }
                if (rhs.m_username.has_value())
                {
                    m_username = rhs.m_username.value();
                }
                if (rhs.m_password.has_value())
                {
                    aws_byte_buf_clean_up(&m_passwordStorage);
                    AWS_ZERO_STRUCT(m_passwordStorage);
                    aws_byte_buf_init_copy_from_cursor(&m_passwordStorage, m_allocator, rhs.m_password.value());
                    m_password = aws_byte_cursor_from_buf(&m_passwordStorage);
                }
            }
            return *this;
        }

        const Crt::Optional<Crt::String> &Mqtt5CustomAuthConfig::GetAuthorizerName() { return m_authorizerName; }

        const Crt::Optional<Crt::String> &Mqtt5CustomAuthConfig::GetUsername() { return m_username; }

        const Crt::Optional<Crt::ByteCursor> &Mqtt5CustomAuthConfig::GetPassword() { return m_password; }

        const Crt::Optional<Crt::String> &Mqtt5CustomAuthConfig::GetTokenKeyName() { return m_tokenKeyName; }

        const Crt::Optional<Crt::String> &Mqtt5CustomAuthConfig::GetTokenValue() { return m_tokenValue; }

        const Crt::Optional<Crt::String> &Mqtt5CustomAuthConfig::GetTokenSignature() { return m_tokenSignature; }

        Mqtt5CustomAuthConfig &Aws::Iot::Mqtt5CustomAuthConfig::WithAuthorizerName(Crt::String authName)
        {
            m_authorizerName = std::move(authName);
            return *this;
        }

        Mqtt5CustomAuthConfig &Aws::Iot::Mqtt5CustomAuthConfig::WithUsername(Crt::String username)
        {
            m_username = std::move(username);
            return *this;
        }

        Mqtt5CustomAuthConfig &Aws::Iot::Mqtt5CustomAuthConfig::WithPassword(Crt::ByteCursor password)
        {
            aws_byte_buf_clean_up(&m_passwordStorage);
            AWS_ZERO_STRUCT(m_passwordStorage);
            aws_byte_buf_init_copy_from_cursor(&m_passwordStorage, m_allocator, password);
            m_password = aws_byte_cursor_from_buf(&m_passwordStorage);
            return *this;
        }

        Mqtt5CustomAuthConfig &Aws::Iot::Mqtt5CustomAuthConfig::WithTokenKeyName(Crt::String tokenKeyName)
        {
            m_tokenKeyName = std::move(tokenKeyName);
            return *this;
        }

        Mqtt5CustomAuthConfig &Aws::Iot::Mqtt5CustomAuthConfig::WithTokenValue(Crt::String tokenValue)
        {
            m_tokenValue = std::move(tokenValue);
            return *this;
        }

        Mqtt5CustomAuthConfig &Aws::Iot::Mqtt5CustomAuthConfig::WithTokenSignature(Crt::String tokenSignature)
        {
            m_tokenSignature = std::move(tokenSignature);
            return *this;
        }

    } // namespace Iot
} // namespace Aws

#endif // !BYO_CRYPTO
