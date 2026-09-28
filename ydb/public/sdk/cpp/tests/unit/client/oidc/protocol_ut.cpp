#include "test_server.h"

#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/protocol.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/testing/unittest/registar.h>

#include <future>
#include <limits>

using namespace NYdb;
using namespace NYdb::NOidc;
using namespace NYdb::NOidc::NPrivate;

namespace {

NJson::TJsonValue Json(const TString& text);
NJson::TJsonValue Metadata(const TOidcTestServer& server);
NJson::TJsonValue DeviceResponse(const TOidcTestServer& server);
TOidcConfig DeviceConfig(const TOidcTestServer& server);
std::string Jwt(const TString& payload);

NJson::TJsonValue Json(const TString& text) {
    NJson::TJsonValue result;
    UNIT_ASSERT(NJson::ReadJsonTree(text, &result));
    return result;
}

NJson::TJsonValue Metadata(const TOidcTestServer& server) {
    NJson::TJsonValue result;
    result["issuer"] = server.Issuer();
    result["token_endpoint"] = server.Issuer() + "/token";
    result["device_authorization_endpoint"] = server.Issuer() + "/device";
    return result;
}

NJson::TJsonValue DeviceResponse(const TOidcTestServer& server) {
    auto result = Json(R"({"device_code":"private-code","user_code":"ABCD","expires_in":600})");
    result["verification_uri"] = server.Issuer() + "/verify";
    return result;
}

TOidcConfig DeviceConfig(const TOidcTestServer& server) {
    auto config = server.ClientConfig().Acceptor(std::make_shared<TTestAcceptor>());
    config.FlowConfig = TDeviceOidcConfig{"public-client", {"read"}};
    return config;
}

std::string Jwt(const TString& payload) {
    return "e30." + std::string(Base64EncodeUrl(payload)) + ".signature";
}

} // namespace

Y_UNIT_TEST_SUITE(TOidcProtocol) {
Y_UNIT_TEST(DiscoveryAndTokenRequestsIncludePortInHost) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"access","token_type":"Bearer","expires_in":600})", HTTP_OK);
    const auto config = server.ClientConfig();
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    UNIT_ASSERT_VALUES_EQUAL(protocol.ClientGrant().AccessToken.Token, "access");
    // Issuer is https://localhost:<allocated port>/realm.
    const auto expectedHost = config.Issuer.substr(8, config.Issuer.size() - 8 - 6);
    const auto requests = server.HostHeaders();
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
    for (const auto& hosts : requests) {
        UNIT_ASSERT_VALUES_EQUAL(hosts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(hosts.front(), expectedHost);
    }
}

Y_UNIT_TEST(RejectsInvalidDurations) {
    for (const TString& value : {"-1", "1.5", "true", "null", "\"10\"", "18446744073709551615"}) {
        UNIT_ASSERT_EXCEPTION(Seconds(Json(value), "expires_in", false), TError);
    }
    UNIT_ASSERT_EXCEPTION(Seconds(Json("0"), "expires_in", false), TError);
    UNIT_ASSERT_VALUES_EQUAL(Seconds(Json("0"), "refresh_expires_in", true), 0);
    const ui64 maximum = std::numeric_limits<i64>::max() / 2 / 1'000'000;
    UNIT_ASSERT_VALUES_EQUAL(Seconds(NJson::TJsonValue(maximum), "expires_in", false), maximum);
    UNIT_ASSERT_EXCEPTION(Seconds(NJson::TJsonValue(maximum + 1), "expires_in", false), TError);
}

Y_UNIT_TEST(JwtExpiryHandlesMissingAndInvalidPayloads) {
    for (const auto& token : {std::string("header.payload"), std::string("header.!.signature"),
             Jwt("[]"), Jwt("{}"), Jwt("null"), Jwt("not-json"),
             "header." + std::string(1024 * 1024, 'a') + ".signature"}) {
        UNIT_ASSERT(!JwtExpiry(token).has_value());
    }
    UNIT_ASSERT_VALUES_EQUAL(*JwtExpiry(Jwt(R"({"exp":0})")), TInstant::Zero());
    UNIT_ASSERT_VALUES_EQUAL(*JwtExpiry(Jwt(R"({"exp":-1})")), TInstant::Zero());
    for (const auto& payload : {R"({"exp":"tomorrow"})", R"({"exp":null})",
             R"({"exp":true})", R"({"exp":18446744073709551615})"}) {
        UNIT_ASSERT(!JwtExpiry(Jwt(payload)).has_value());
    }
}

Y_UNIT_TEST(UrlErrorsIdentifyIssuerOrEndpoint) {
    for (const bool issuer : {false, true}) {
        const std::string role = issuer ? "issuer" : "endpoint";
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseUrl("https://user:secret@example.com", issuer),
            std::invalid_argument, "invalid " + role + " URL");
        UNIT_ASSERT_EXCEPTION_CONTAINS(ParseUrl("http://example.com", issuer),
            std::invalid_argument, role + " requires HTTPS");
    }
}

Y_UNIT_TEST(DiscoveryMismatchReportsIssuerIdentifiers) {
    TOidcTestServer server;
    auto metadata = Metadata(server);
    metadata["issuer"] = server.Issuer() + "/";
    server.SetDiscoveryReply(NJson::WriteJson(metadata, false), HTTP_OK);
    const auto config = server.ClientConfig();
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    UNIT_ASSERT_EXCEPTION_CONTAINS(protocol.ClientGrant(), TError,
        "configured '" + config.Issuer + "', advertised '" + config.Issuer + "/'");
    UNIT_ASSERT(server.Requests().empty());
}

Y_UNIT_TEST(InvalidAdvertisedIssuerDoesNotLeakSecrets) {
    for (const auto& issuer : {"https://user:private-token@example.com",
             "https://example.com?token=private-token", "https://example.com/#private-token"}) {
        TOidcTestServer server;
        auto metadata = Metadata(server);
        metadata["issuer"] = issuer;
        server.SetDiscoveryReply(NJson::WriteJson(metadata, false), HTTP_OK);
        const auto config = server.ClientConfig();
        NThreading::TCancellationTokenSource cancellation;
        TProtocol protocol(config, cancellation.Token());
        try {
            protocol.ClientGrant();
            UNIT_FAIL("expected an invalid advertised issuer error");
        } catch (const TError& error) {
            const std::string message = error.what();
            UNIT_ASSERT_STRING_CONTAINS(message, "discovery issuer mismatch: invalid advertised issuer URL");
            UNIT_ASSERT(message.find("private-token") == std::string::npos);
        }
        UNIT_ASSERT(server.Requests().empty());
    }
}

Y_UNIT_TEST(RejectsInvalidDiscoveryFields) {
    for (const auto& field : {"issuer", "token_endpoint"}) {
        for (const TString& value : {"null", "false", "\"\""}) {
            TOidcTestServer server;
            auto metadata = Metadata(server);
            metadata[field] = Json(value);
            server.SetDiscoveryReply(NJson::WriteJson(metadata, false), HTTP_OK);
            const auto config = server.ClientConfig();
            NThreading::TCancellationTokenSource cancellation;
            TProtocol protocol(config, cancellation.Token());
            UNIT_ASSERT_EXCEPTION(protocol.ClientGrant(), TError);
            UNIT_ASSERT(server.Requests().empty());
        }
    }
    TOidcTestServer server;
    auto metadata = Metadata(server);
    metadata["issuer"] = "https://another-issuer.example";
    server.SetDiscoveryReply(NJson::WriteJson(metadata, false), HTTP_OK);
    const auto config = server.ClientConfig();
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    UNIT_ASSERT_EXCEPTION_CONTAINS(protocol.ClientGrant(), TError, "issuer mismatch");
    UNIT_ASSERT(server.Requests().empty());
}

Y_UNIT_TEST(DiscoveryRejectsUnsupportedAuthenticationMethods) {
    for (const TString& methods : {"null", "\"client_secret_basic\"", "[42]", "[]", "[\"client_secret_post\"]"}) {
        TOidcTestServer server;
        auto metadata = Metadata(server);
        metadata["token_endpoint_auth_methods_supported"] = Json(methods);
        server.SetDiscoveryReply(NJson::WriteJson(metadata, false), HTTP_OK);
        const auto config = server.ClientConfig();
        NThreading::TCancellationTokenSource cancellation;
        TProtocol protocol(config, cancellation.Token());
        UNIT_ASSERT_EXCEPTION(protocol.ClientGrant(), TError);
        UNIT_ASSERT(server.Requests().empty());
    }
}

Y_UNIT_TEST(DiscoveryAcceptsBasicAuthAndTrailingIssuerSlash) {
    TOidcTestServer server;
    auto config = server.ClientConfig();
    config.Issuer += "/";
    auto metadata = Metadata(server);
    metadata["issuer"] = config.Issuer;
    metadata["token_endpoint_auth_methods_supported"] = Json("[\"client_secret_post\",\"client_secret_basic\"]");
    server.SetDiscoveryReply(NJson::WriteJson(metadata, false), HTTP_OK);
    server.Enqueue(R"({"access_token":"access","token_type":"Bearer","expires_in":600})", HTTP_OK);
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    UNIT_ASSERT_VALUES_EQUAL(protocol.ClientGrant().AccessToken.Token, "access");
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 1);
}

Y_UNIT_TEST(RejectsMissingAndInvalidTokenFields) {
    for (const auto& field : {"access_token", "token_type", "refresh_token"}) {
        for (const TString& value : {"null", "42", "\"\"", "\"bad token\"", "\"bad\\u007ftoken\""}) {
            TOidcTestServer server;
            auto response = Json(R"({"access_token":"access","token_type":"Bearer","expires_in":600})");
            response[field] = Json(value);
            server.Enqueue(NJson::WriteJson(response, false), HTTP_OK);
            const auto config = server.ClientConfig();
            NThreading::TCancellationTokenSource cancellation;
            TProtocol protocol(config, cancellation.Token());
            UNIT_ASSERT_EXCEPTION(protocol.ClientGrant(), TError);
        }
    }
    for (const TString& response : {"{}", "[]", R"({"token_type":"Bearer","expires_in":600})"}) {
        TOidcTestServer server;
        server.Enqueue(response, HTTP_OK);
        const auto config = server.ClientConfig();
        NThreading::TCancellationTokenSource cancellation;
        TProtocol protocol(config, cancellation.Token());
        UNIT_ASSERT_EXCEPTION(protocol.ClientGrant(), TError);
    }
}

Y_UNIT_TEST(AccessTokenCanUseJwtExpiry) {
    TOidcTestServer server;
    const auto expiry = TInstant::Now() + TDuration::Hours(1);
    NJson::TJsonValue payload;
    payload["exp"] = expiry.Seconds();
    const auto token = Jwt(NJson::WriteJson(payload, false));
    auto response = Json(R"({"token_type":"Bearer"})");
    response["access_token"] = token;
    server.Enqueue(NJson::WriteJson(response, false), HTTP_OK);
    const auto config = server.ClientConfig();
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    const auto result = protocol.ClientGrant();
    UNIT_ASSERT_VALUES_EQUAL(result.AccessToken.Token, token);
    UNIT_ASSERT(result.AccessToken.ExpiresAt.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result.AccessToken.ExpiresAt->Seconds(), expiry.Seconds());
}

Y_UNIT_TEST(RefreshCanRemoveExpiryAndRotateToken) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"first","token_type":"Bearer","expires_in":600,"refresh_expires_in":0})", HTTP_OK);
    server.Enqueue(R"({"access_token":"second","token_type":"Bearer","expires_in":600,"refresh_token":"rotated","refresh_expires_in":1200})", HTTP_OK);
    const auto config = server.ClientConfig();
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    const auto first = protocol.Refresh({"refresh", TInstant::Now() + TDuration::Minutes(1)});
    UNIT_ASSERT(first.RefreshToken.has_value());
    UNIT_ASSERT_VALUES_EQUAL(first.RefreshToken->Token, "refresh");
    UNIT_ASSERT(!first.RefreshToken->ExpiresAt.has_value());
    const auto second = protocol.Refresh(*first.RefreshToken);
    UNIT_ASSERT(second.RefreshToken.has_value() && second.RefreshToken->ExpiresAt.has_value());
    UNIT_ASSERT_VALUES_EQUAL(second.RefreshToken->Token, "rotated");
    UNIT_ASSERT(second.RefreshToken->IsValid(TInstant::Now()));
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 1);
}

Y_UNIT_TEST(HttpErrorsAreClassifiedWithoutExposingResponse) {
    for (const auto status : {HTTP_BAD_REQUEST, HTTP_UNAUTHORIZED, HTTP_REQUEST_TIME_OUT, HTTP_TOO_MANY_REQUESTS,
             HTTP_INTERNAL_SERVER_ERROR, HTTP_BAD_GATEWAY, HTTP_SERVICE_UNAVAILABLE, HTTP_GATEWAY_TIME_OUT}) {
        for (const TString& body : {"private-response", "{}", R"({"error":42})", R"({"error":"private-error"})", R"({"error":"invalid_client"})"}) {
            TOidcTestServer server;
            server.Enqueue(body, status);
            const auto config = server.ClientConfig();
            NThreading::TCancellationTokenSource cancellation;
            TProtocol protocol(config, cancellation.Token());
            try {
                protocol.ClientGrant();
                UNIT_FAIL("Expected HTTP error");
            } catch (const TError& error) {
                UNIT_ASSERT_VALUES_EQUAL(error.Retryable, status != HTTP_BAD_REQUEST && status != HTTP_UNAUTHORIZED);
                UNIT_ASSERT_VALUES_EQUAL(error.Code, body.Contains("invalid_client") ? "invalid_client" : "");
                UNIT_ASSERT(!TString(error.what()).Contains("private"));
            }
        }
    }
}

Y_UNIT_TEST(RejectsOversizedResponse) {
    TOidcTestServer server;
    server.Enqueue(TString(1024 * 1024 + 1, 'x'), HTTP_OK);
    const auto config = server.ClientConfig();
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    UNIT_ASSERT_EXCEPTION_CONTAINS(protocol.ClientGrant(), TError, "response exceeds size limit");
}

Y_UNIT_TEST(DeviceRequiresAcceptorAndDiscoveryEndpoint) {
    TOidcTestServer server;
    auto config = DeviceConfig(server);
    config.Acceptor(nullptr);
    NThreading::TCancellationTokenSource cancellation;
    TProtocol withoutAcceptor(config, cancellation.Token());
    UNIT_ASSERT_EXCEPTION_CONTAINS(withoutAcceptor.DeviceGrant([](TDuration) { return true; }), TError, "auth acceptor");
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
    config.Acceptor(std::make_shared<TTestAcceptor>());
    auto metadata = Metadata(server);
    metadata.EraseValue("device_authorization_endpoint");
    server.SetDiscoveryReply(NJson::WriteJson(metadata, false), HTTP_OK);
    TProtocol withoutEndpoint(config, cancellation.Token());
    UNIT_ASSERT_EXCEPTION_CONTAINS(withoutEndpoint.DeviceGrant([](TDuration) { return true; }), TError, "device_authorization_endpoint");
    UNIT_ASSERT(server.Requests().empty());
}

Y_UNIT_TEST(DeviceRejectsMissingExpiryAndInvalidVerificationLinks) {
    for (const auto& field : {"expires_in", "verification_uri", "verification_uri_complete", "device_code", "user_code"}) {
        TOidcTestServer server;
        auto response = DeviceResponse(server);
        if (TString(field).StartsWith("verification_uri")) {
            response[field] = "http://insecure.example";
        } else {
            response.EraseValue(field);
        }
        server.Enqueue(NJson::WriteJson(response, false), HTTP_OK);
        const auto config = DeviceConfig(server);
        NThreading::TCancellationTokenSource cancellation;
        TProtocol protocol(config, cancellation.Token());
        UNIT_ASSERT_EXCEPTION(protocol.DeviceGrant([](TDuration) { return true; }), std::exception);
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
    }
}

Y_UNIT_TEST(DevicePollingHandlesPendingSlowDownAndTransientErrors) {
    TOidcTestServer server;
    server.Enqueue(NJson::WriteJson(DeviceResponse(server), false), HTTP_OK);
    server.Enqueue(R"({"error":"authorization_pending"})", HTTP_BAD_REQUEST);
    server.Enqueue(R"({"error":"slow_down"})", HTTP_BAD_REQUEST);
    server.Enqueue(R"({"error":"temporarily_unavailable"})", HTTP_SERVICE_UNAVAILABLE);
    server.Enqueue(R"({"access_token":"device-access","token_type":"Bearer","expires_in":600})", HTTP_OK);
    const auto config = DeviceConfig(server);
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    std::vector<TDuration> delays;
    const auto result = protocol.DeviceGrant([&](TDuration delay) {
        delays.push_back(delay);
        return true;
    });
    UNIT_ASSERT_VALUES_EQUAL(result.AccessToken.Token, "device-access");
    UNIT_ASSERT_VALUES_EQUAL(delays.size(), 4);
    UNIT_ASSERT_VALUES_EQUAL(delays[0], TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(delays[1], TDuration::Seconds(5));
    UNIT_ASSERT_VALUES_EQUAL(delays[2], TDuration::Seconds(10));
    UNIT_ASSERT_VALUES_EQUAL(delays[3], TDuration::Seconds(20));
    const auto requests = server.Requests();
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 5);
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("scope"), "read openid");
    for (size_t i = 1; i < requests.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(requests[i].Form.Get("client_id"), "public-client");
        UNIT_ASSERT(requests[i].Authorization.empty());
    }
}

Y_UNIT_TEST(DeviceDenialIsTerminal) {
    TOidcTestServer server;
    server.Enqueue(NJson::WriteJson(DeviceResponse(server), false), HTTP_OK);
    server.Enqueue(R"({"error":"access_denied"})", HTTP_BAD_REQUEST);
    const auto config = DeviceConfig(server);
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    UNIT_ASSERT_EXCEPTION_CONTAINS(protocol.DeviceGrant([](TDuration) { return true; }), TError, "access_denied");
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 2);
}

Y_UNIT_TEST(CancellationWaitsForPendingHttpRequest) {
    TOidcTestServer server;
    auto gate = NThreading::NewPromise<void>();
    server.BlockTokenRepliesUntil(gate.GetFuture());
    server.Enqueue(R"({"access_token":"late","token_type":"Bearer","expires_in":600})", HTTP_OK);
    const auto config = server.ClientConfig();
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    auto result = std::async(std::launch::async, [&] { return protocol.ClientGrant(); });
    const bool requested = server.WaitRequests(1);
    cancellation.Cancel();
    const bool stillRunning = result.wait_for(std::chrono::milliseconds(100)) == std::future_status::timeout;
    gate.TrySetValue();
    UNIT_ASSERT(requested);
    UNIT_ASSERT(stillRunning);
    UNIT_ASSERT_EXCEPTION(result.get(), std::exception);
}
Y_UNIT_TEST(DiscoveryIssuerComparisonPreservesTrailingSlash) {
    TOidcTestServer server;
    auto config = server.ClientConfig();
    config.Issuer += "/";
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    UNIT_ASSERT_EXCEPTION_CONTAINS(protocol.ClientGrant(), TError, "issuer mismatch");
    UNIT_ASSERT(server.Requests().empty());
}

Y_UNIT_TEST(DeviceAcceptsOpaqueTokenWithoutLifetime) {
    TOidcTestServer server;
    server.Enqueue(NJson::WriteJson(DeviceResponse(server), false), HTTP_OK);
    server.Enqueue(R"({"access_token":"opaque","token_type":"Bearer","refresh_token":"refresh"})", HTTP_OK);
    const auto config = DeviceConfig(server);
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    const auto result = protocol.DeviceGrant([](TDuration) { return true; });
    UNIT_ASSERT_VALUES_EQUAL(result.AccessToken.Token, "opaque");
    UNIT_ASSERT(!result.AccessToken.ExpiresAt.has_value());
    UNIT_ASSERT(result.RefreshToken.has_value());
    UNIT_ASSERT_VALUES_EQUAL(result.RefreshToken->Token, "refresh");
}

Y_UNIT_TEST(DeviceDoesNotPollAfterWaitOvershootsExpiry) {
    TOidcTestServer server;
    auto response = DeviceResponse(server);
    response["expires_in"] = 1;
    server.Enqueue(NJson::WriteJson(response, false), HTTP_OK);
    auto acceptor = std::make_shared<TTestAcceptor>();
    const auto config = DeviceConfig(server).Acceptor(acceptor);
    NThreading::TCancellationTokenSource cancellation;
    TProtocol protocol(config, cancellation.Token());
    UNIT_ASSERT_EXCEPTION_CONTAINS(protocol.DeviceGrant([&](TDuration) {
        NThreading::NewPromise<void>().GetFuture().Wait(acceptor->Wait().ExpiresAt + TDuration::MilliSeconds(1));
        return true;
    }), TError, "device authorization expired");
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
}

}
