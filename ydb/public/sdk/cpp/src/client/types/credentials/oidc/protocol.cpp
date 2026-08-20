#include "protocol.h"

#include <library/cpp/http/misc/httpcodes.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/uri/uri.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

#include <algorithm>
#include <cctype>
#include <string_view>

namespace NYdb::inline Dev::NOidc {

    namespace {

        constexpr std::string_view OPENID_SCOPE = "openid";

        struct TParsedEndpoint {
            std::string Host;
            ui16 Port = 0;
            std::string Request;
        };

        std::string NormalizeIssuer(std::string issuer) {
            while (!issuer.empty() && issuer.back() == '/') {
                issuer.pop_back();
            }
            if (issuer.empty()) {
                throw std::invalid_argument("OIDC issuer is empty");
            }
            return issuer;
        }

        TParsedEndpoint ParseEndpoint(const std::string& value, std::string_view name) {
            NUri::TUri uri;
            if (uri.Parse(value, NUri::TFeature::FeaturesAll) != NUri::TUri::TState::EParsed::ParsedOK) {
                throw std::invalid_argument(TStringBuilder() << "Failed to parse OIDC " << name);
            }
            if (uri.GetScheme() != NUri::TScheme::SchemeHTTPS || uri.IsNull(NUri::TUri::FieldHost)) {
                throw std::invalid_argument(TStringBuilder() << "OIDC " << name << " must use HTTPS and contain a host");
            }

            TParsedEndpoint result;
            result.Host = TStringBuilder() << "https://" << uri.GetHost();
            result.Port = uri.IsNull(NUri::TUri::FieldPort) ? 443 : FromString<ui16>(uri.GetField(NUri::TUri::FieldPort));

            TStringBuilder request;
            request << uri.GetField(NUri::TUri::FieldPath);
            if (request.empty()) {
                request << '/';
            }
            if (!uri.IsNull(NUri::TUri::FieldQuery)) {
                request << '?' << uri.GetField(NUri::TUri::FieldQuery);
            }
            result.Request = std::move(request);
            return result;
        }

        NJson::TJsonValue ParseJson(TStringStream& input, std::string_view context) {
            NJson::TJsonValue result;
            try {
                NJson::ReadJsonTree(&input, &result, true);
                result.GetMapSafe();
            } catch (const std::exception& error) {
                throw TOidcException(TStringBuilder() << "Invalid OIDC " << context << " response: " << error.what());
            }
            return result;
        }

        const NJson::TJsonValue& RequiredField(const NJson::TJsonValue::TMapType& object, std::string_view name) {
            const auto it = object.find(std::string(name));
            if (it == object.end()) {
                throw TOidcException(TStringBuilder() << "OIDC response has no field '" << name << "'");
            }
            return it->second;
        }

        std::optional<std::string> OptionalString(const NJson::TJsonValue::TMapType& object, std::string_view name) {
            const auto it = object.find(std::string(name));
            if (it == object.end()) {
                return std::nullopt;
            }
            return it->second.GetString();
        }

        std::optional<long long> OptionalInteger(const NJson::TJsonValue::TMapType& object, std::string_view name) {
            const auto it = object.find(std::string(name));
            if (it == object.end()) {
                return std::nullopt;
            }
            return it->second.GetIntegerRobust();
        }

        TOidcException ParseOAuthError(TKeepAliveHttpClient::THttpCode statusCode, TStringStream& input) {
            TStringBuilder message;
            message << "OIDC endpoint returned " << HttpCodeStrEx(statusCode);
            std::string oauthError;
            try {
                const auto json = ParseJson(input, "error");
                const auto& object = json.GetMapSafe();
                if (const auto error = OptionalString(object, "error"); error.has_value()) {
                    oauthError = error.value();
                    message << ", error: " << oauthError;
                }
                if (const auto description = OptionalString(object, "error_description"); description.has_value()) {
                    message << ", description: " << description.value();
                }
                if (const auto uri = OptionalString(object, "error_uri"); uri.has_value()) {
                    message << ", error_uri: " << uri.value();
                }
            } catch (const std::exception&) {
                message << ", response is not valid OAuth error JSON";
            }
            return TOidcException(message, statusCode, std::move(oauthError));
        }

        std::string JoinScope(const std::vector<std::string>& scopes) {
            TStringBuilder result;
            result << OPENID_SCOPE;
            for (const auto& scope : scopes) {
                if (scope.empty()) {
                    throw std::invalid_argument("OIDC scope is empty");
                }
                if (scope == OPENID_SCOPE) {
                    continue;
                }
                result << ' ' << scope;
            }
            return result;
        }

        TKeepAliveHttpClient::THeaders ClientAuthenticationHeaders(
            const std::string& clientId,
            const std::string& clientSecret)
        {
            if (clientId.empty() || clientSecret.empty()) {
                throw std::invalid_argument("OIDC client id and client secret are required");
            }
            TKeepAliveHttpClient::THeaders headers;
            headers["Authorization"] = "Basic " + Base64Encode(CGIEscapeRet(clientId) + ":" + CGIEscapeRet(clientSecret));
            return headers;
        }

    } // namespace

    TOidcException::TOidcException(
        std::string message,
        TKeepAliveHttpClient::THttpCode httpCode,
        std::string oauthError)
        : std::runtime_error(std::move(message))
        , HttpCode(httpCode)
        , OAuthError(std::move(oauthError))
    {
    }

    TOidcProtocolClient::TOidcProtocolClient(std::string issuer)
        : Issuer_(NormalizeIssuer(std::move(issuer)))
    {
        ParseEndpoint(Issuer_, "issuer");
    }

    const std::string& TOidcProtocolClient::GetIssuer() const {
        return Issuer_;
    }

    bool TOidcProtocolClient::IsRetryable(const std::exception& error) {
        if (const auto* oidcError = dynamic_cast<const TOidcException*>(&error)) {
            return oidcError->HttpCode == HTTP_REQUEST_TIME_OUT || oidcError->HttpCode == HTTP_AUTHENTICATION_TIMEOUT || oidcError->HttpCode == HTTP_TOO_MANY_REQUESTS || (oidcError->HttpCode >= 500 && oidcError->HttpCode < 600);
        }
        return dynamic_cast<const TSystemError*>(&error) != nullptr;
    }

    const TDiscoveryDocument& TOidcProtocolClient::GetDiscoveryDocument() {
        if (Discovery_.has_value()) {
            return Discovery_.value();
        }

        const TParsedEndpoint endpoint = ParseEndpoint(Issuer_ + "/.well-known/openid-configuration", "discovery endpoint");
        TKeepAliveHttpClient client(TString(endpoint.Host), endpoint.Port, SOCKET_TIMEOUT, CONNECT_TIMEOUT);
        TStringStream response;
        TKeepAliveHttpClient::THeaders headers;
        headers["Accept"] = "application/json";
        const auto statusCode = client.DoGet(endpoint.Request, &response, headers);
        if (statusCode != HTTP_OK) {
            throw TOidcException(
                TStringBuilder() << "OIDC discovery endpoint returned " << HttpCodeStrEx(statusCode),
                statusCode);
        }

        const auto json = ParseJson(response, "discovery");
        const auto& object = json.GetMapSafe();
        if (NormalizeIssuer(RequiredField(object, "issuer").GetString()) != Issuer_) {
            throw TOidcException("OIDC discovery issuer does not match configured issuer");
        }

        TDiscoveryDocument document;
        document.TokenEndpoint = RequiredField(object, "token_endpoint").GetString();
        ParseEndpoint(document.TokenEndpoint, "token endpoint");
        if (const auto deviceEndpoint = OptionalString(object, "device_authorization_endpoint"); deviceEndpoint.has_value()) {
            ParseEndpoint(deviceEndpoint.value(), "device authorization endpoint");
            document.DeviceAuthorizationEndpoint = deviceEndpoint.value();
        }
        Discovery_ = std::move(document);
        return Discovery_.value();
    }

    TOidcTokenSet TOidcProtocolClient::ClientCredentialsGrant(const TClientOidcConfig& config) {
        const std::string scope = JoinScope(config.Scope_);

        TCgiParameters form;
        form.emplace("grant_type", "client_credentials");
        form.emplace("scope", scope);
        return RequestTokens(form, ClientAuthenticationHeaders(config.ClientId_, config.ClientSecret_), std::nullopt);
    }

    TOidcTokenSet TOidcProtocolClient::RefreshToken(
        const TOidcToken& refreshToken,
        const std::optional<TOidcToken>& previousRefreshToken,
        const std::string& clientId,
        const std::string& clientSecret)
    {
        TCgiParameters form;
        form.emplace("grant_type", "refresh_token");
        form.emplace("refresh_token", refreshToken.Token);
        TKeepAliveHttpClient::THeaders headers;
        if (clientSecret.empty()) {
            form.emplace("client_id", clientId);
        } else {
            headers = ClientAuthenticationHeaders(clientId, clientSecret);
        }
        return RequestTokens(form, std::move(headers), previousRefreshToken);
    }

    TDeviceAuthorizationResponse TOidcProtocolClient::StartDeviceAuthorization(const TDeviceOidcConfig& config) {
        const auto& discovery = GetDiscoveryDocument();
        if (!discovery.DeviceAuthorizationEndpoint.has_value()) {
            throw TOidcException("OIDC discovery response has no device_authorization_endpoint");
        }

        const std::string scope = JoinScope(config.Scope_);

        TCgiParameters form;
        form.emplace("client_id", config.ClientId_);
        form.emplace("scope", scope);

        const TParsedEndpoint endpoint = ParseEndpoint(
            discovery.DeviceAuthorizationEndpoint.value(),
            "device authorization endpoint");
        TKeepAliveHttpClient client(TString(endpoint.Host), endpoint.Port, SOCKET_TIMEOUT, CONNECT_TIMEOUT);
        TStringStream response;
        TKeepAliveHttpClient::THeaders headers;
        headers["Content-Type"] = "application/x-www-form-urlencoded";
        headers["Accept"] = "application/json";
        const auto statusCode = client.DoPost(endpoint.Request, form.Print(), &response, headers);
        if (statusCode != HTTP_OK) {
            throw ParseOAuthError(statusCode, response);
        }

        const auto json = ParseJson(response, "device authorization");
        const auto& object = json.GetMapSafe();
        const auto expiresIn = RequiredField(object, "expires_in").GetIntegerRobust();
        if (expiresIn <= 0) {
            throw TOidcException("OIDC device authorization expires_in must be positive");
        }

        long long interval = DEFAULT_DEVICE_POLL_INTERVAL.Seconds();
        if (const auto it = object.find("interval"); it != object.end()) {
            interval = it->second.GetIntegerRobust();
            if (interval <= 0) {
                throw TOidcException("OIDC device authorization interval must be positive");
            }
        }

        TDeviceAuthorizationResponse result;
        result.DeviceCode = RequiredField(object, "device_code").GetString();
        result.UserInfo.UserCode = RequiredField(object, "user_code").GetString();
        result.UserInfo.VerificationUri = RequiredField(object, "verification_uri").GetString();
        result.UserInfo.VerificationUriComplete = OptionalString(object, "verification_uri_complete");
        result.UserInfo.ExpiresAt = TInstant::Now() + TDuration::Seconds(expiresIn);
        result.Interval = TDuration::Seconds(interval);
        if (result.DeviceCode.empty() || result.UserInfo.UserCode.empty() || result.UserInfo.VerificationUri.empty()) {
            throw TOidcException("OIDC device authorization response contains an empty required field");
        }
        return result;
    }

    TOidcTokenSet TOidcProtocolClient::PollDeviceToken(const TDeviceOidcConfig& config, const std::string& deviceCode) {
        TCgiParameters form;
        form.emplace("grant_type", "urn:ietf:params:oauth:grant-type:device_code");
        form.emplace("device_code", deviceCode);
        form.emplace("client_id", config.ClientId_);
        return RequestTokens(form, {}, std::nullopt);
    }

    TOidcTokenSet TOidcProtocolClient::RequestTokens(
        const TCgiParameters& form,
        TKeepAliveHttpClient::THeaders headers,
        const std::optional<TOidcToken>& previousRefreshToken)
    {
        const TParsedEndpoint endpoint = ParseEndpoint(GetDiscoveryDocument().TokenEndpoint, "token endpoint");
        TKeepAliveHttpClient client(TString(endpoint.Host), endpoint.Port, SOCKET_TIMEOUT, CONNECT_TIMEOUT);
        TStringStream response;
        headers["Content-Type"] = "application/x-www-form-urlencoded";
        headers["Accept"] = "application/json";
        const auto statusCode = client.DoPost(endpoint.Request, form.Print(), &response, headers);
        if (statusCode != HTTP_OK) {
            throw ParseOAuthError(statusCode, response);
        }

        const auto json = ParseJson(response, "token");
        const auto& object = json.GetMapSafe();
        std::string tokenType = RequiredField(object, "token_type").GetString();
        std::ranges::transform(tokenType, tokenType.begin(), [](unsigned char value) {
            return std::tolower(value);
        });
        if (tokenType != "bearer") {
            throw TOidcException("OIDC token_type is not Bearer");
        }

        const auto expiresIn = RequiredField(object, "expires_in").GetIntegerRobust();
        if (expiresIn <= 0) {
            throw TOidcException("OIDC token expires_in must be positive");
        }

        const TInstant now = TInstant::Now();
        TOidcTokenSet result;
        result.AccessToken.Token = RequiredField(object, "access_token").GetString();
        result.AccessToken.ExpiresAt = now + TDuration::Seconds(expiresIn);
        if (result.AccessToken.Token.empty()) {
            throw TOidcException("OIDC access_token is empty");
        }

        if (const auto refreshToken = OptionalString(object, "refresh_token"); refreshToken.has_value() && !refreshToken->empty()) {
            TOidcToken token{.Token = refreshToken.value()};
            if (const auto refreshExpiresIn = OptionalInteger(object, "refresh_expires_in"); refreshExpiresIn.has_value()) {
                if (refreshExpiresIn.value() <= 0) {
                    throw TOidcException("OIDC refresh_expires_in must be positive");
                }
                token.ExpiresAt = now + TDuration::Seconds(refreshExpiresIn.value());
            } else {
                token.ExpiresAt = now + TDuration::Seconds(expiresIn) + REFRESH_SKEW;
            }
            result.RefreshToken = std::move(token);
        } else {
            result.RefreshToken = previousRefreshToken;
        }
        return result;
    }

} // namespace NYdb::inline Dev::NOidc
