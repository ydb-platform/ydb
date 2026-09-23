#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include "test_server.h"
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/static_provider.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/client_provider.h>
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/device_provider.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <future>

using namespace NYdb;
using namespace NYdb::NOidc;
using NYdb::NOidc::NPrivate::GetOidcClientIdentity;

namespace {

class TThrowingOidcFacility: public TQueuedOidcFacility {
public:
    void PostToResponseQueue(TPostTaskCb&& callback) override;
};

class TFailingOidcCacher: public TMemoryTokenCacher {
public:
    TFailingOidcCacher(bool failRead, bool failWrite);

    std::optional<TTokenCache> Read() const override;
    void Write(const TTokenCache& cache) override;

private:
    const bool FailRead;
    const bool FailWrite;
};

class TGatedOidcAcceptor: public TTestAcceptor {
public:
    void Accept(const TDeviceAuthInfo& info) override;

    NThreading::TPromise<void> Release = NThreading::NewPromise<void>();
    NThreading::TPromise<void> Finished = NThreading::NewPromise<void>();
};

void TThrowingOidcFacility::PostToResponseQueue(TPostTaskCb&&) {
    throw std::runtime_error("response queue unavailable");
}

TFailingOidcCacher::TFailingOidcCacher(bool failRead, bool failWrite)
    : FailRead(failRead)
    , FailWrite(failWrite)
{
}

std::optional<TTokenCache> TFailingOidcCacher::Read() const {
    if (FailRead) {
        throw std::runtime_error("cache read failed: private-token");
    }
    return TMemoryTokenCacher::Read();
}

void TFailingOidcCacher::Write(const TTokenCache& cache) {
    if (FailWrite) {
        throw std::runtime_error("cache write failed: private-token");
    }
    TMemoryTokenCacher::Write(cache);
}

void TGatedOidcAcceptor::Accept(const TDeviceAuthInfo& info) {
    TTestAcceptor::Accept(info);
    Release.GetFuture().Wait();
    Finished.TrySetValue();
}

} // namespace

Y_UNIT_TEST_SUITE(TOidcCredentials) {
Y_UNIT_TEST(FactoryReturnsConcreteProviders) {
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"cached", std::nullopt}, std::nullopt});
    config.Cacher(cache);
    auto facility = std::make_shared<TQueuedOidcFacility>();
    config.FlowConfig = TStaticOidcConfig{.AccessToken = "opaque"};
    auto provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    UNIT_ASSERT(std::dynamic_pointer_cast<NOidc::NPrivate::TStaticProvider>(provider) != nullptr);
    config.FlowConfig = TClientOidcConfig{"client", "secret", {}};
    provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    UNIT_ASSERT(std::dynamic_pointer_cast<NOidc::NPrivate::TClientProvider>(provider) != nullptr);
    config.FlowConfig = TDeviceOidcConfig{"client", {}};
    provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    UNIT_ASSERT(std::dynamic_pointer_cast<NOidc::NPrivate::TDeviceProvider>(provider) != nullptr);
}

Y_UNIT_TEST(BearerTicketDoesNotChangeCachedToken) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{
        .AccessToken = "opaque-access",
        .ExpiresAt = TInstant::Now() + TDuration::Hours(1),
    };
    config.Cacher(cache);
    auto provider = CreateOidcProviderFactory(config)->CreateProvider();
    auto pending = provider->GetAuthInfoAsync();
    const bool wasPending = !pending.IsReady();
    cache->Release.TrySetValue();

    UNIT_ASSERT(wasPending);
    UNIT_ASSERT_VALUES_EQUAL(pending.GetValueSync(), "Bearer opaque-access");
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer opaque-access");
    const auto stored = cache->Read();
    UNIT_ASSERT(stored.has_value());
    UNIT_ASSERT(!stored->RefreshToken.has_value());
    UNIT_ASSERT_VALUES_EQUAL(stored->AccessToken.Token, "opaque-access");
}

Y_UNIT_TEST(LiveFacilityDiscardCompletesPendingFuture) {
    auto cacher = std::make_shared<TGatedOidcCacher>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{.AccessToken = "opaque"};
    config.Cacher(cacher);
    auto facility = std::make_shared<TQueuedOidcFacility>();
    auto provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    UNIT_ASSERT(cacher->Entered.GetFuture().Wait(TDuration::Seconds(1)));
    auto pending = provider->GetAuthInfoAsync();
    cacher->Release.TrySetValue();
    UNIT_ASSERT(facility->WaitForTask());
    auto reentered = NThreading::NewPromise<void>();
    pending.Subscribe([facility, reentered](const auto&) mutable {
        facility->RunTasks();
        reentered.TrySetValue();
    });
    facility->DiscardTasks();
    UNIT_ASSERT(reentered.GetFuture().Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(pending.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT_EXCEPTION(pending.GetValueSync(), std::exception);
}

Y_UNIT_TEST(ThrowingDiscardSubscriberDoesNotTerminateWorker) {
    auto cacher = std::make_shared<TGatedOidcCacher>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{.AccessToken = "opaque"};
    config.Cacher(cacher);
    auto facility = std::make_shared<TQueuedOidcFacility>();
    auto provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    UNIT_ASSERT(cacher->Entered.GetFuture().Wait(TDuration::Seconds(1)));
    auto pending = provider->GetAuthInfoAsync();
    cacher->Release.TrySetValue();
    UNIT_ASSERT(facility->WaitForTask());
    auto reentered = NThreading::NewPromise<void>();
    pending.Subscribe([facility, reentered](const auto&) mutable {
        facility->RunTasks();
        reentered.TrySetValue();
        throw std::runtime_error("subscriber failure");
    });
    facility->DiscardTasks();
    UNIT_ASSERT(reentered.GetFuture().Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(pending.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT_EXCEPTION(pending.GetValueSync(), std::exception);
}

Y_UNIT_TEST(ThrowingSuccessSubscriberDoesNotInterruptResponseQueue) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{.AccessToken = "opaque"};
    config.Cacher(cache);
    auto facility = std::make_shared<TQueuedOidcFacility>();
    auto provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    auto pending = provider->GetAuthInfoAsync();
    pending.Subscribe([](const auto&) {
        throw std::runtime_error("subscriber failure");
    });
    cache->Release.TrySetValue();
    UNIT_ASSERT(facility->WaitForTask());
    bool nextTaskRan = false;
    facility->PostToResponseQueue([&nextTaskRan] { nextTaskRan = true; });
    UNIT_ASSERT_NO_EXCEPTION(facility->RunTasks());
    UNIT_ASSERT(nextTaskRan);
    UNIT_ASSERT_VALUES_EQUAL(pending.GetValueSync(), "Bearer opaque");
}

Y_UNIT_TEST(CacheFailuresDoNotPreventClientAuthentication) {
    for (const bool failRead : {false, true}) {
        for (const bool failWrite : {false, true}) {
            TOidcTestServer server;
            server.Enqueue(R"({"access_token":"access","token_type":"Bearer","expires_in":600})", HTTP_OK);
            auto cache = std::make_shared<TFailingOidcCacher>(failRead, failWrite);
            auto provider = CreateOidcProviderFactory(server.ClientConfig().Cacher(cache))->CreateProvider();
            UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer access");
            UNIT_ASSERT(provider->IsValid());
            UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
            if (!failRead && !failWrite) {
                UNIT_ASSERT_VALUES_EQUAL(cache->Read()->AccessToken.Token, "access");
            }
        }
    }
}

Y_UNIT_TEST(CacheWriteFailureDoesNotPreventStaticAuthentication) {
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{.AccessToken = "opaque"};
    config.Cacher(std::make_shared<TFailingOidcCacher>(true, true));
    auto provider = CreateOidcProviderFactory(config)->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer opaque");
}

Y_UNIT_TEST(CacheFailuresDoNotDiscardDeviceAuthorization) {
    TOidcTestServer server;
    server.Enqueue(TString("{\"device_code\":\"private-device\",\"user_code\":\"ABCD\",\"verification_uri\":\"") +
        server.Issuer() + "/verify\",\"expires_in\":60,\"interval\":1}", HTTP_OK);
    server.Enqueue(R"({"access_token":"user-access","token_type":"Bearer","expires_in":600})", HTTP_OK);
    auto acceptor = std::make_shared<TTestAcceptor>();
    auto config = server.ClientConfig().Acceptor(acceptor).Cacher(std::make_shared<TFailingOidcCacher>(true, true));
    config.FlowConfig = TDeviceOidcConfig{"public-client", {}};
    auto provider = CreateOidcProviderFactory(config)->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer user-access");
    UNIT_ASSERT_VALUES_EQUAL(acceptor->Wait().UserCode, "ABCD");
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 2);
}

Y_UNIT_TEST(MalformedJwtExpiryDoesNotRejectAccessToken) {
    const auto token = "e30." + std::string(Base64EncodeUrl(R"({"exp":"unknown"})")) + ".signature";
    for (const bool useStatic : {false, true}) {
        TOidcTestServer server;
        auto config = server.ClientConfig();
        if (useStatic) {
            config.FlowConfig = TStaticOidcConfig{.AccessToken = token};
        } else {
            server.Enqueue(TString("{\"access_token\":\"") + token + "\",\"token_type\":\"Bearer\"}", HTTP_OK);
        }
        auto provider = CreateOidcProviderFactory(config)->CreateProvider();
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer " + token);
        UNIT_ASSERT(provider->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), useStatic ? 0 : 1);
    }
}

Y_UNIT_TEST(CustomHooksIsolateFactoryIdentity) {
    for (const TFlowConfig& flow : {
             TFlowConfig{TStaticOidcConfig{.AccessToken = "opaque"}},
             TFlowConfig{TClientOidcConfig{"client", "secret", {}}}}) {
        TOidcConfig config;
        config.Issuer = "https://issuer.example";
        config.FlowConfig = flow;
        const auto identity = GetOidcClientIdentity(config);
        UNIT_ASSERT_VALUES_EQUAL(CreateOidcProviderFactory(config)->GetClientIdentity(),
            CreateOidcProviderFactory(config)->GetClientIdentity());
        for (const bool useCacher : {false, true}) {
            auto first = config;
            auto second = config;
            if (useCacher) {
                first.Cacher(std::make_shared<TMemoryTokenCacher>());
                second.Cacher(std::make_shared<TMemoryTokenCacher>());
            } else {
                first.Acceptor(std::make_shared<TTestAcceptor>());
                second.Acceptor(std::make_shared<TTestAcceptor>());
            }
            const auto firstFactory = CreateOidcProviderFactory(first);
            const auto firstIdentity = firstFactory->GetClientIdentity();
            UNIT_ASSERT(firstIdentity != CreateOidcProviderFactory(second)->GetClientIdentity());
            UNIT_ASSERT_VALUES_EQUAL(firstIdentity, firstFactory->GetClientIdentity());
            UNIT_ASSERT_VALUES_EQUAL(GetOidcClientIdentity(first), identity);
        }
    }
}

Y_UNIT_TEST(ClientRetriesTransientError) {
    TOidcTestServer server;
    auto replyGate = NThreading::NewPromise<void>();
    server.BlockTokenRepliesUntil(replyGate.GetFuture());
    server.Enqueue(R"({"error":"temporarily_unavailable"})", HTTP_SERVICE_UNAVAILABLE);
    server.Enqueue(R"({"access_token":"retried","token_type":"Bearer","expires_in":600})", HTTP_OK);
    auto facility = std::make_shared<TQueuedOidcFacility>();
    auto provider = CreateOidcProviderFactory(server.ClientConfig())->CreateProvider(facility);
    auto token = provider->GetAuthInfoAsync();
    replyGate.TrySetValue();
    UNIT_ASSERT(facility->WaitForTask());
    facility->RunTasks();
    UNIT_ASSERT(token.Wait(TDuration::Seconds(5)));
    UNIT_ASSERT_EXCEPTION_CONTAINS(token.GetValueSync(), std::exception, "503");
    if (!provider->GetAuthInfoAsync().HasValue()) {
        UNIT_ASSERT(facility->WaitForTask());
        facility->RunTasks();
    }
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer retried");
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 2);
}

Y_UNIT_TEST(JwtExpiryIsOnlyASchedulingHint) {
    using NYdb::NOidc::NPrivate::JwtExpiry;
    const std::string token = "e30." + std::string(Base64EncodeUrl(R"({"exp":2000000000})")) + ".signature";
    UNIT_ASSERT(JwtExpiry(token).has_value());
    UNIT_ASSERT_VALUES_EQUAL(*JwtExpiry(token), TInstant::Seconds(2000000000));
    UNIT_ASSERT(!JwtExpiry("opaque").has_value());
    UNIT_ASSERT(!JwtExpiry("e30.invalid.signature").has_value());
}

Y_UNIT_TEST(IndependentStaticFactories) {
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    TStaticOidcConfig first;
    first.AccessToken = "first-opaque-token";
    config.FlowConfig = first;
    auto firstFactory = CreateOidcProviderFactory(config);
    first.AccessToken = "second-opaque-token";
    config.FlowConfig = first;
    auto secondFactory = CreateOidcProviderFactory(config);
    UNIT_ASSERT_VALUES_EQUAL(firstFactory->CreateProvider()->GetAuthInfo(), "Bearer first-opaque-token");
    UNIT_ASSERT_VALUES_EQUAL(secondFactory->CreateProvider()->GetAuthInfo(), "Bearer second-opaque-token");
    UNIT_ASSERT(firstFactory->GetClientIdentity() != secondFactory->GetClientIdentity());
}

Y_UNIT_TEST(ClientIdentityDoesNotExposeCredentials) {
    const std::string clientSecret = "private-client-secret";
    const std::string staticToken = "private-static-access-token";
    const std::string cachedAccessToken = "private-cached-access-token";
    const std::string refreshToken = "private-cached-refresh-token";
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{cachedAccessToken, std::nullopt}, TOAuthToken{refreshToken, std::nullopt}});

    for (const TFlowConfig& flow : {
             TFlowConfig{TStaticOidcConfig{staticToken, std::nullopt}},
             TFlowConfig{TClientOidcConfig{"client", clientSecret, {"read"}}},
             TFlowConfig{TDeviceOidcConfig{"client", {"read"}}}}) {
        TOidcConfig config;
        config.Issuer = "https://issuer.example";
        config.FlowConfig = flow;
        config.Cacher(cache);
        const auto factory = CreateOidcProviderFactory(config);
        for (const auto& identity : {GetOidcClientIdentity(config), factory->GetClientIdentity()}) {
            for (const auto& secret : {clientSecret, staticToken, cachedAccessToken, refreshToken}) {
                UNIT_ASSERT(identity.find(secret) == std::string::npos);
            }
        }
    }
}

Y_UNIT_TEST(RejectsMissingClientSecret) {
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    TClientOidcConfig flow;
    flow.ClientId = "client";
    config.FlowConfig = flow;
    UNIT_ASSERT_EXCEPTION(CreateOidcProviderFactory(config), std::invalid_argument);
}

Y_UNIT_TEST(RejectsInsecureIssuer) {
    TOidcTestServer server;
    auto config = server.ClientConfig();
    config.Issuer = "http://issuer.example";
    UNIT_ASSERT_EXCEPTION(CreateOidcProviderFactory(config), std::invalid_argument);
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
}

Y_UNIT_TEST(UrlComponentsPreservePortAndQuery) {
    using NOidc::NPrivate::ParseUrl;
    const auto endpoint = ParseUrl("https://issuer.example:8443/token?scope=a%2Bb&x=1", false);
    UNIT_ASSERT_VALUES_EQUAL(endpoint.GetPort(), 8443);
    UNIT_ASSERT_VALUES_EQUAL(endpoint.PrintS(NUri::TUri::FlagScheme | NUri::TUri::FlagHost | NUri::TUri::FlagHostAscii), "https://issuer.example");
    UNIT_ASSERT_VALUES_EQUAL(endpoint.PrintS(NUri::TUri::FlagPath | NUri::TUri::FlagQuery), "/token?scope=a%2Bb&x=1");
    const auto root = ParseUrl("https://issuer.example", true);
    UNIT_ASSERT_VALUES_EQUAL(root.GetPort(), 443);
    UNIT_ASSERT_VALUES_EQUAL(root.PrintS(NUri::TUri::FlagPath | NUri::TUri::FlagQuery), "/");
    const auto query = ParseUrl("https://issuer.example?code=abc", false);
    UNIT_ASSERT_VALUES_EQUAL(query.PrintS(NUri::TUri::FlagPath | NUri::TUri::FlagQuery), "/?code=abc");
}

Y_UNIT_TEST(UrlValidationRejectsInvalidOidcEndpoints) {
    using NOidc::NPrivate::ParseUrl;
    for (const char* url : {
             "", "http://issuer.example", "https:///", "https://user:pass@issuer.example/token",
             "https://issuer.example/token#fragment", "https://issuer.example/a b",
             "https://issuer.example:65536/token"}) {
        UNIT_ASSERT_EXCEPTION(ParseUrl(url, false), std::invalid_argument);
    }
    UNIT_ASSERT_EXCEPTION(ParseUrl("https://issuer.example?query=value", true), std::invalid_argument);
}

Y_UNIT_TEST(ClientGrantEncodesForm) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"access","token_type":"Bearer","expires_in":600})", HTTP_OK);
    auto factory = CreateOidcProviderFactory(server.ClientConfig());
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
    UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer access");
    const auto requests = server.Requests();
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Method, "POST");
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("grant_type"), "client_credentials");
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Basic " + Base64Encode("client:secret+%2B%26"));
    UNIT_ASSERT(!requests[0].Form.Has("client_secret"));
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("scope"), "read write openid");
}

Y_UNIT_TEST(ClientGrantIncludesOpenidOnce) {
    const std::vector<std::pair<std::vector<std::string>, TString>> cases = {
        {{}, "openid"},
        {{"openid"}, "openid"},
        {{"profile", "openid"}, "profile openid"},
    };
    for (const auto& [scopes, expected] : cases) {
        TOidcTestServer server;
        server.Enqueue(R"({"access_token":"access","token_type":"Bearer","expires_in":600})", HTTP_OK);
        auto config = server.ClientConfig();
        std::get<TClientOidcConfig>(config.FlowConfig).Scopes = scopes;
        auto provider = CreateOidcProviderFactory(config)->CreateProvider();
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer access");
        UNIT_ASSERT_VALUES_EQUAL(server.Requests()[0].Form.Get("scope"), expected);
    }
}

Y_UNIT_TEST(ClientSecretBasic) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"access","token_type":"bEaReR","expires_in":600})", HTTP_OK);
    auto config = server.ClientConfig();
    auto& flow = std::get<TClientOidcConfig>(config.FlowConfig);
    flow.ClientId = "client:+ %";
    flow.ClientSecret = "secret:/?#[]@!$&'()*+,;= %~_-.Я";
    auto factory = CreateOidcProviderFactory(config);
    UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer access");
    const auto requests = server.Requests();
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Basic " + Base64Encode(
        "client%3A%2B+%25:secret%3A%2F%3F%23%5B%5D%40%21%24%26%27%28%29%2A%2B%2C%3B%3D+%25~_-.%D0%AF"));
    UNIT_ASSERT(!requests[0].Form.Has("client_secret"));
}

Y_UNIT_TEST(ReusesCachedAccessWithoutDiscovery) {
    TOidcTestServer server;
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"cached", TInstant::Now() + TDuration::Hours(1)}, std::nullopt});
    auto config = server.ClientConfig().Cacher(cache);
    auto factory = CreateOidcProviderFactory(config);
    UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer cached");
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
}

Y_UNIT_TEST(RefreshesExpiredCacheAndRetainsRefreshToken) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"fresh","token_type":"Bearer","expires_in":600})", HTTP_OK);
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"expired", TInstant::Seconds(1)}, TOAuthToken{"refresh", std::nullopt}});
    auto config = server.ClientConfig().Cacher(cache);
    auto factory = CreateOidcProviderFactory(config);
    UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer fresh");
    const auto requests = server.Requests();
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("grant_type"), "refresh_token");
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("refresh_token"), "refresh");
    const auto stored = cache->Read();
    UNIT_ASSERT(stored.has_value());
    UNIT_ASSERT(stored->RefreshToken.has_value());
    UNIT_ASSERT_VALUES_EQUAL(stored->RefreshToken->Token, "refresh");
}

Y_UNIT_TEST(RetainedRefreshTokenReceivesUpdatedExpiry) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"fresh","token_type":"Bearer","expires_in":600,"refresh_expires_in":1200})", HTTP_OK);
    auto cache = std::make_shared<TMemoryTokenCacher>();
    const auto now = TInstant::Now();
    cache->Write({{"expired", TInstant::Seconds(1)}, TOAuthToken{"refresh", now + TDuration::Seconds(60)}});
    auto factory = CreateOidcProviderFactory(server.ClientConfig().Cacher(cache));
    UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer fresh");
    const auto stored = cache->Read();
    UNIT_ASSERT(stored.has_value() && stored->RefreshToken.has_value() && stored->RefreshToken->ExpiresAt.has_value());
    UNIT_ASSERT(*stored->RefreshToken->ExpiresAt >= now + TDuration::Seconds(1200));
}

Y_UNIT_TEST(InvalidRefreshFallsBackToClientGrant) {
    TOidcTestServer server;
    server.Enqueue(R"({"error":"invalid_grant"})", HTTP_BAD_REQUEST);
    server.Enqueue(R"({"access_token":"fresh","token_type":"Bearer","expires_in":600,"refresh_token":"rotated"})", HTTP_OK);
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"expired", TInstant::Seconds(1)}, TOAuthToken{"refresh", std::nullopt}});
    auto config = server.ClientConfig().Cacher(cache);
    auto factory = CreateOidcProviderFactory(config);
    UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer fresh");
    const auto requests = server.Requests();
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(requests[1].Form.Get("grant_type"), "client_credentials");
    UNIT_ASSERT_VALUES_EQUAL(cache->Read()->RefreshToken->Token, "rotated");
}

Y_UNIT_TEST(ExpiredStaticTokenDoesNotContactIssuer) {
    TOidcTestServer server;
    auto config = server.ClientConfig();
    config.FlowConfig = TStaticOidcConfig{"expired", TInstant::Seconds(1)};
    auto provider = CreateOidcProviderFactory(config)->CreateProvider();
    auto result = provider->GetAuthInfoAsync();
    UNIT_ASSERT(result.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(result.HasException());
    UNIT_ASSERT(!provider->IsValid());
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
    UNIT_ASSERT(server.Requests().empty());
}

Y_UNIT_TEST(RejectsMalformedResponsesWithoutLeakingSecrets) {
    for (const TString& body : {
             TString("secret-response-is-not-json"),
             TString(R"({"access_token":"secret-access","token_type":"unsupported-secret","expires_in":600})"),
             TString(R"({"access_token":"secret-access","token_type":"Bearer","expires_in":0})")}) {
        TOidcTestServer server;
        server.Enqueue(body, HTTP_OK);
        auto factory = CreateOidcProviderFactory(server.ClientConfig());
        try {
            factory->CreateProvider()->GetAuthInfo();
            UNIT_FAIL("Expected invalid token response");
        } catch (const std::exception& error) {
            UNIT_ASSERT(!TString(error.what()).Contains("secret"));
        }
    }
}

Y_UNIT_TEST(ConcurrentRequestsShareInitialGrant) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"access","token_type":"Bearer","expires_in":600})", HTTP_OK);
    auto factory = CreateOidcProviderFactory(server.ClientConfig());
    auto provider = factory->CreateProvider();
    std::vector<std::future<std::string>> requests;
    for (size_t i = 0; i < 8; ++i) {
        requests.push_back(std::async(std::launch::async, [provider] { return provider->GetAuthInfo(); }));
    }
    for (auto& request : requests) {
        UNIT_ASSERT_VALUES_EQUAL(request.get(), "Bearer access");
    }
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
}

Y_UNIT_TEST(DeviceAuthorization) {
    TOidcTestServer server;
    server.Enqueue(TString("{\"device_code\":\"private-device\",\"user_code\":\"ABCD\",\"verification_uri\":\"") + server.Issuer() + "/verify\",\"verification_uri_complete\":\"" + server.Issuer() + "/verify?user_code=ABCD\",\"expires_in\":60,\"interval\":1}", HTTP_OK);
    server.Enqueue(R"({"access_token":"user-access","token_type":"Bearer","expires_in":600,"refresh_token":"user-refresh"})", HTTP_OK);
    auto acceptor = std::make_shared<TTestAcceptor>();
    auto cache = std::make_shared<TMemoryTokenCacher>();
    auto config = server.ClientConfig().Acceptor(acceptor).Cacher(cache);
    config.FlowConfig = TDeviceOidcConfig{"public-client", {"openid"}};
    auto factory = CreateOidcProviderFactory(config);
    auto provider = factory->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer user-access");
    const auto stored = cache->Read();
    UNIT_ASSERT(stored.has_value() && stored->RefreshToken.has_value());
    UNIT_ASSERT_VALUES_EQUAL(stored->AccessToken.Token, "user-access");
    UNIT_ASSERT_VALUES_EQUAL(stored->RefreshToken->Token, "user-refresh");
    const auto info = acceptor->Wait();
    UNIT_ASSERT_VALUES_EQUAL(info.UserCode, "ABCD");
    UNIT_ASSERT_VALUES_EQUAL(info.VerificationUrl, server.Issuer() + "/verify");
    UNIT_ASSERT(info.VerificationUrlComplete.has_value());
    UNIT_ASSERT_VALUES_EQUAL(info.VerificationUrlComplete.value(), server.Issuer() + "/verify?user_code=ABCD");
    const auto requests = server.Requests();
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("scope"), "openid");
    UNIT_ASSERT_VALUES_EQUAL(requests[1].Form.Get("grant_type"), "urn:ietf:params:oauth:grant-type:device_code");
    UNIT_ASSERT_VALUES_EQUAL(requests[1].Form.Get("device_code"), "private-device");
}

Y_UNIT_TEST(StoppingDeviceWaitCompletesPendingFuture) {
    TOidcTestServer server;
    server.Enqueue(TString("{\"device_code\":\"private-device\",\"user_code\":\"ABCD\",\"verification_uri\":\"") + server.Issuer() + "/verify\",\"expires_in\":600,\"interval\":300}", HTTP_OK);
    auto acceptor = std::make_shared<TTestAcceptor>();
    auto config = server.ClientConfig().Acceptor(acceptor);
    config.FlowConfig = TDeviceOidcConfig{"public-client", {}};
    auto factory = CreateOidcProviderFactory(config);
    auto facility = CreateSimpleCoreFacility();
    auto provider = factory->CreateProvider(facility);
    const auto pending = provider->GetAuthInfoAsync();
    UNIT_ASSERT(!acceptor->Wait().VerificationUrlComplete.has_value());
    UNIT_ASSERT_VALUES_EQUAL(server.Requests()[0].Form.Get("scope"), "openid");
    provider.reset();
    UNIT_ASSERT(pending.Wait(TDuration::Seconds(2)));
    UNIT_ASSERT(pending.HasException());
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
}

Y_UNIT_TEST(DeviceWithoutAcceptorCanUseCache) {
    TOidcTestServer server;
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"cached-user", TInstant::Now() + TDuration::Hours(1)}, std::nullopt});
    auto config = server.ClientConfig().Cacher(cache);
    config.FlowConfig = TDeviceOidcConfig{"public-client", {}};
    auto factory = CreateOidcProviderFactory(config);
    UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer cached-user");
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
}

Y_UNIT_TEST(ExpiredFacilityCompletesPendingFuture) {
    TOidcTestServer server;
    auto factory = CreateOidcProviderFactory(server.ClientConfig());
    auto provider = factory->CreateProvider(std::weak_ptr<ICoreFacility>{});
    auto pending = provider->GetAuthInfoAsync();
    UNIT_ASSERT(pending.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(pending.HasException());
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
}

Y_UNIT_TEST(StaticTokenExpiryNeverLeavesPendingRequest) {
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    TStaticOidcConfig flow;
    flow.AccessToken = "static";
    flow.ExpiresAt = TInstant::Now() + TDuration::MilliSeconds(100);
    config.FlowConfig = flow;
    auto factory = CreateOidcProviderFactory(config);
    auto provider = factory->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer static");
    // A real expiration boundary is the behavior under test. Wait for that
    // known deadline rather than estimating background worker progress.
    NThreading::NewPromise<void>().GetFuture().Wait(*flow.ExpiresAt + TDuration::MilliSeconds(1));
    auto expired = provider->GetAuthInfoAsync();
    UNIT_ASSERT(expired.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(expired.HasException());
}

Y_UNIT_TEST(DevicePollingPolicy) {
    using namespace NOidc::NPrivate;
    TDevicePolling polling;
    const auto now = TInstant::Seconds(1'000);
    polling.Deadline = now + TDuration::Seconds(20);
    UNIT_ASSERT_VALUES_EQUAL(polling.NextDelay(now), TDuration::Seconds(5));
    polling.HandleError(TError("pending", false, "authorization_pending"));
    UNIT_ASSERT_VALUES_EQUAL(polling.NextDelay(now), TDuration::Seconds(5));
    polling.HandleError(TError("slow", false, "slow_down"));
    UNIT_ASSERT_VALUES_EQUAL(polling.NextDelay(now), TDuration::Seconds(10));
    polling.HandleError(TError("transport", true, {}));
    UNIT_ASSERT_VALUES_EQUAL(polling.NextDelay(now), TDuration::Seconds(20));
    UNIT_ASSERT_VALUES_EQUAL(polling.NextDelay(now + TDuration::Seconds(19)), TDuration::Seconds(1));
    UNIT_ASSERT_EXCEPTION(polling.NextDelay(polling.Deadline), TError);
    UNIT_ASSERT_EXCEPTION(polling.HandleError(TError("denied", false, "access_denied")), TError);
}

Y_UNIT_TEST(EquivalentFactoriesHaveStableIdentity) {
    for (const TFlowConfig& flow : {
             TFlowConfig{TStaticOidcConfig{.AccessToken = "opaque"}},
             TFlowConfig{TClientOidcConfig{"client", "secret", {"read"}}},
             TFlowConfig{TDeviceOidcConfig{"client", {"read"}}}}) {
        TOidcConfig config;
        config.Issuer = "https://issuer.example";
        config.FlowConfig = flow;
        for (const bool useHooks : {false, true}) {
            if (useHooks) {
                config.Cacher(std::make_shared<TMemoryTokenCacher>());
                config.Acceptor(std::make_shared<TTestAcceptor>());
            }
            UNIT_ASSERT_VALUES_EQUAL(CreateOidcProviderFactory(config)->GetClientIdentity(),
                CreateOidcProviderFactory(config)->GetClientIdentity());
        }
    }
}

Y_UNIT_TEST(DeviceFactoriesDoNotShareUserIdentity) {
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TDeviceOidcConfig{"public-client", {"openid"}};
    auto alice = std::make_shared<TMemoryTokenCacher>();
    auto bob = std::make_shared<TMemoryTokenCacher>();
    auto aliceFactory = CreateOidcProviderFactory(config.Cacher(alice));
    auto bobFactory = CreateOidcProviderFactory(config.Cacher(bob));
    UNIT_ASSERT(aliceFactory->GetClientIdentity() != bobFactory->GetClientIdentity());
    UNIT_ASSERT_VALUES_EQUAL(aliceFactory->GetClientIdentity(), aliceFactory->GetClientIdentity());
}

Y_UNIT_TEST(StaticTokenReplacesCachedCredentials) {
    TOidcTestServer server;
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"cached", TInstant::Now() + TDuration::Hours(1)}, TOAuthToken{"refresh", std::nullopt}});
    auto config = server.ClientConfig().Cacher(cache);
    const auto expiry = TInstant::Now() + TDuration::Seconds(1);
    config.FlowConfig = TStaticOidcConfig{"initial", expiry};
    auto provider = CreateOidcProviderFactory(config)->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer initial");
    const auto stored = cache->Read();
    UNIT_ASSERT(stored.has_value());
    UNIT_ASSERT_VALUES_EQUAL(stored->AccessToken.Token, "initial");
    UNIT_ASSERT(stored->AccessToken.ExpiresAt.has_value());
    UNIT_ASSERT_VALUES_EQUAL(*stored->AccessToken.ExpiresAt, expiry);
    UNIT_ASSERT(!stored->RefreshToken.has_value());
    NThreading::NewPromise<void>().GetFuture().Wait(expiry + TDuration::MilliSeconds(1));
    auto result = provider->GetAuthInfoAsync();
    UNIT_ASSERT(result.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(result.HasException());
    UNIT_ASSERT(!provider->IsValid());
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
    UNIT_ASSERT(server.Requests().empty());
}

Y_UNIT_TEST(ClientRefreshPersistsTokensForNextProvider) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"fresh","token_type":"Bearer","expires_in":600,"refresh_token":"rotated"})", HTTP_OK);
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"expired", TInstant::Seconds(1)}, TOAuthToken{"initial-refresh", std::nullopt}});
    const auto config = server.ClientConfig().Cacher(cache);
    {
        auto provider = CreateOidcProviderFactory(config)->CreateProvider();
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer fresh");
    }
    const auto stored = cache->Read();
    UNIT_ASSERT(stored.has_value() && stored->RefreshToken.has_value());
    UNIT_ASSERT_VALUES_EQUAL(stored->AccessToken.Token, "fresh");
    UNIT_ASSERT_VALUES_EQUAL(stored->RefreshToken->Token, "rotated");
    auto provider = CreateOidcProviderFactory(config)->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer fresh");
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 1);
    const auto requests = server.Requests();
    UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("grant_type"), "refresh_token");
    UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("refresh_token"), "initial-refresh");
}

Y_UNIT_TEST(StandaloneDestructionWaitsForResponseCallback) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"access","token_type":"Bearer","expires_in":600})", HTTP_OK);
    auto replyGate = NThreading::NewPromise<void>();
    server.BlockTokenRepliesUntil(replyGate.GetFuture());
    auto factory = CreateOidcProviderFactory(server.ClientConfig());
    auto provider = factory->CreateProvider();
    auto entered = NThreading::NewPromise<void>();
    auto release = NThreading::NewPromise<void>();
    provider->GetAuthInfoAsync().Subscribe([entered, release](const auto&) mutable {
        entered.TrySetValue();
        release.GetFuture().Wait();
    });
    replyGate.TrySetValue();
    const bool callbackStarted = entered.GetFuture().Wait(TDuration::Seconds(5));
    auto stopped = std::async(std::launch::async,
        [provider = std::move(provider), factory = std::move(factory)]() mutable {
            factory.reset();
            provider.reset();
        });
    const bool completed = stopped.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready;
    release.TrySetValue();
    stopped.get();
    UNIT_ASSERT(callbackStarted);
    UNIT_ASSERT(!completed);
}

Y_UNIT_TEST(DiscardedCompletionAllowsExternalDestruction) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{.AccessToken = "opaque"};
    config.Cacher(cache);
    auto facility = std::make_shared<TQueuedOidcFacility>();
    auto provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    auto pending = provider->GetAuthInfoAsync();
    auto finished = NThreading::NewPromise<void>();
    pending.Subscribe([finished](const auto&) mutable {
        finished.TrySetValue();
    });
    cache->Release.TrySetValue();
    UNIT_ASSERT(facility->WaitForTask());
    facility->DiscardTasks();
    UNIT_ASSERT(finished.GetFuture().Wait(TDuration::Seconds(5)));
    provider.reset();
    UNIT_ASSERT(pending.HasException());
}

Y_UNIT_TEST(DestructionJoinsWorkerUntilAcceptorReturns) {
    TOidcTestServer server;
    server.Enqueue(TString("{\"device_code\":\"private-device\",\"user_code\":\"ABCD\",\"verification_uri\":\"") +
        server.Issuer() + "/verify\",\"expires_in\":60,\"interval\":1}", HTTP_OK);
    auto acceptor = std::make_shared<TGatedOidcAcceptor>();
    auto config = server.ClientConfig().Acceptor(acceptor);
    config.FlowConfig = TDeviceOidcConfig{"public-client", {}};
    auto provider = CreateOidcProviderFactory(config)->CreateProvider();
    auto pending = provider->GetAuthInfoAsync();
    acceptor->Wait();
    auto stopped = std::async(std::launch::async, [provider = std::move(provider)]() mutable {
        provider.reset();
    });
    const bool cancelled = pending.Wait(TDuration::Seconds(5));
    const bool completed = stopped.wait_for(std::chrono::milliseconds(100)) == std::future_status::ready;
    acceptor->Release.TrySetValue();
    stopped.get();
    UNIT_ASSERT(cancelled);
    UNIT_ASSERT(!completed);
    UNIT_ASSERT(acceptor->Finished.GetFuture().HasValue());
    UNIT_ASSERT(pending.HasException());
}

Y_UNIT_TEST(StandaloneSubscriberCanWaitForAnotherProvider) {
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{.AccessToken = "opaque"};
    auto firstCache = std::make_shared<TGatedOidcCacher>();
    auto secondCache = std::make_shared<TGatedOidcCacher>();
    auto first = CreateOidcProviderFactory(config.Cacher(firstCache))->CreateProvider();
    auto second = CreateOidcProviderFactory(config.Cacher(secondCache))->CreateProvider();
    auto entered = NThreading::NewPromise<void>();
    auto finished = NThreading::NewPromise<bool>();
    first->GetAuthInfoAsync().Subscribe([second, entered, finished](const auto&) mutable {
        auto pending = second->GetAuthInfoAsync();
        entered.TrySetValue();
        finished.TrySetValue(pending.Wait(TDuration::Seconds(2)));
    });
    firstCache->Release.TrySetValue();
    const bool callbackStarted = entered.GetFuture().Wait(TDuration::Seconds(5));
    secondCache->Release.TrySetValue();
    UNIT_ASSERT(callbackStarted);
    UNIT_ASSERT(finished.GetFuture().Wait(TDuration::Seconds(5)));
    UNIT_ASSERT(finished.GetFuture().GetValue());
}

Y_UNIT_TEST(DeviceRejectsStreamingTokenReceivedAfterExpiry) {
    TOidcTestServer server;
    server.Enqueue(TString("{\"device_code\":\"code\",\"user_code\":\"ABCD\",\"verification_uri\":\"") +
        server.Issuer() + "/verify\",\"expires_in\":2,\"interval\":1}", HTTP_OK);
    server.Enqueue(R"({"access_token":"late","token_type":"Bearer","expires_in":600})", HTTP_OK);
    server.SetTokenReplyDelay(TDuration::MilliSeconds(100));
    auto config = server.ClientConfig().Acceptor(std::make_shared<TTestAcceptor>());
    config.FlowConfig = TDeviceOidcConfig{"public-client", {}};
    auto provider = CreateOidcProviderFactory(config)->CreateProvider();
    auto pending = provider->GetAuthInfoAsync();
    const bool requested = server.WaitRequests(2);
    const bool completed = pending.Wait(TDuration::Seconds(10));
    provider.reset();
    UNIT_ASSERT(requested);
    UNIT_ASSERT(completed);
    UNIT_ASSERT_EXCEPTION_CONTAINS(pending.GetValueSync(), std::exception, "device authorization expired");
}

Y_UNIT_TEST(DeviceTokenRequestUsesRemainingLifetime) {
    auto gate = NThreading::NewPromise<void>();
    // Request deadline is exercised via the device deadline, which must
    // bound a token request even when the socket timeout is much longer.
    // The device code expires while the server holds its token response.
    TOidcTestServer deviceServer;
    deviceServer.Enqueue(TString("{\"device_code\":\"code\",\"user_code\":\"ABCD\",\"verification_uri\":\"") + deviceServer.Issuer() + "/verify\",\"expires_in\":2,\"interval\":1}", HTTP_OK);
    deviceServer.Enqueue(R"({"access_token":"late","token_type":"Bearer","expires_in":600})", HTTP_OK);
    deviceServer.BlockTokenRepliesUntil(gate.GetFuture());
    auto config = deviceServer.ClientConfig();
    config.FlowConfig = TDeviceOidcConfig{"public-client", {}};
    config.Acceptor(std::make_shared<TTestAcceptor>());
    auto factory = CreateOidcProviderFactory(config);
    auto provider = factory->CreateProvider();
    auto result = provider->GetAuthInfoAsync();
    const bool completed = result.Wait(TDuration::Seconds(4));
    gate.TrySetValue();
    UNIT_ASSERT(completed);
    UNIT_ASSERT(result.HasException());
}

Y_UNIT_TEST(DiscardedDeliveryCompletesOnFacilityDestruction) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    auto facility = std::make_shared<TQueuedOidcFacility>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    TStaticOidcConfig flow;
    flow.AccessToken = "static";
    config.FlowConfig = flow;
    config.Cacher(cache);
    auto factory = CreateOidcProviderFactory(config);
    auto provider = factory->CreateProvider(facility);
    auto pending = provider->GetAuthInfoAsync();
    cache->Release.TrySetValue();
    UNIT_ASSERT(facility->WaitForTask());
    facility.reset();
    UNIT_ASSERT(pending.Wait(TDuration::Seconds(1)));
    UNIT_ASSERT(pending.HasException());
}

Y_UNIT_TEST(QueuedDeliveryNeverReturnsExpiredToken) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    auto facility = std::make_shared<TQueuedOidcFacility>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    TStaticOidcConfig flow;
    flow.AccessToken = "static";
    flow.ExpiresAt = TInstant::Now() + TDuration::Seconds(1);
    config.FlowConfig = flow;
    config.Cacher(cache);
    auto provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    auto pending = provider->GetAuthInfoAsync();
    cache->Release.TrySetValue();
    UNIT_ASSERT(facility->WaitForTask());
    NThreading::NewPromise<void>().GetFuture().Wait(*flow.ExpiresAt + TDuration::MilliSeconds(1));
    facility->RunTasks();
    UNIT_ASSERT(pending.HasException());
}
Y_UNIT_TEST(ResponseQueueFailureCompletesPendingFuture) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    auto facility = std::make_shared<TThrowingOidcFacility>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{"access", std::nullopt};
    config.Cacher(cache);
    auto provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    auto pending = provider->GetAuthInfoAsync();
    cache->Release.TrySetValue();
    UNIT_ASSERT(pending.Wait(TDuration::Seconds(5)));
    UNIT_ASSERT_EXCEPTION_CONTAINS(pending.GetValueSync(), std::exception, "response queue unavailable");
}

Y_UNIT_TEST(RejectsMissingRequiredCredentialsAndInvalidScopes) {
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    for (const TFlowConfig& flow : {
             TFlowConfig{TStaticOidcConfig{}},
             TFlowConfig{TClientOidcConfig{"", "secret", {}}},
             TFlowConfig{TDeviceOidcConfig{"", {}}}}) {
        config.FlowConfig = flow;
        UNIT_ASSERT_EXCEPTION(CreateOidcProviderFactory(config), std::invalid_argument);
    }
    for (const std::string& scope : {"", "read write", "read\twrite", "read\nwrite", "read\"", "read\\", "read\x7f", "профиль"}) {
        config.FlowConfig = TClientOidcConfig{"client", "secret", {scope}};
        UNIT_ASSERT_EXCEPTION(CreateOidcProviderFactory(config), std::invalid_argument);
    }
}

Y_UNIT_TEST(ClientIdentityNormalizesScopesButDistinguishesCredentials) {
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TClientOidcConfig{"client", "secret", {"write", "read", "read"}};
    const auto identity = GetOidcClientIdentity(config);
    auto& client = std::get<TClientOidcConfig>(config.FlowConfig);
    client.Scopes = {"read", "write"};
    UNIT_ASSERT_VALUES_EQUAL(GetOidcClientIdentity(config), identity);
    client.ClientSecret = "other-secret";
    UNIT_ASSERT(GetOidcClientIdentity(config) != identity);
    config.FlowConfig = TStaticOidcConfig{"token", std::nullopt};
    const auto staticIdentity = GetOidcClientIdentity(config);
    std::get<TStaticOidcConfig>(config.FlowConfig).ExpiresAt = TInstant::Seconds(100);
    UNIT_ASSERT(GetOidcClientIdentity(config) != staticIdentity);
    const auto factory = CreateOidcProviderFactory(config);
    UNIT_ASSERT(factory->CreateProvider() == factory->CreateProvider());
}

Y_UNIT_TEST(CachedTokenWithoutExpiryNeedsNoRefresh) {
    TOidcTestServer server;
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"cached", std::nullopt}, std::nullopt});
    auto provider = CreateOidcProviderFactory(server.ClientConfig().Cacher(cache))->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer cached");
    UNIT_ASSERT(provider->IsValid());
    UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
}

Y_UNIT_TEST(UnknownCachedLifetimeTriggersRefresh) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"fresh","token_type":"Bearer","expires_in":600})", HTTP_OK);
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"unknown-expiry", std::nullopt}, TOAuthToken{"refresh", std::nullopt}});
    auto provider = CreateOidcProviderFactory(server.ClientConfig().Cacher(cache))->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer fresh");
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(server.Requests()[0].Form.Get("grant_type"), "refresh_token");
}

Y_UNIT_TEST(ExpiredRefreshTokenTriggersNewClientGrant) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"fresh","token_type":"Bearer","expires_in":600})", HTTP_OK);
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"expired", TInstant::Seconds(1)}, TOAuthToken{"expired-refresh", TInstant::Seconds(1)}});
    auto provider = CreateOidcProviderFactory(server.ClientConfig().Cacher(cache))->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer fresh");
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(server.Requests()[0].Form.Get("grant_type"), "client_credentials");
}

Y_UNIT_TEST(TerminalRefreshErrorDoesNotStartNewGrant) {
    TOidcTestServer server;
    server.Enqueue(R"({"error":"invalid_client"})", HTTP_UNAUTHORIZED);
    auto cache = std::make_shared<TMemoryTokenCacher>();
    cache->Write({{"expired", TInstant::Seconds(1)}, TOAuthToken{"refresh", std::nullopt}});
    auto provider = CreateOidcProviderFactory(server.ClientConfig().Cacher(cache))->CreateProvider();
    auto result = provider->GetAuthInfoAsync();
    UNIT_ASSERT(result.Wait(TDuration::Seconds(5)));
    UNIT_ASSERT_EXCEPTION_CONTAINS(result.GetValueSync(), std::exception, "invalid_client");
    UNIT_ASSERT(!provider->IsValid());
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
}

Y_UNIT_TEST(StopDuringCacheWriteDoesNotPublishToken) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    auto facility = std::make_shared<TQueuedOidcFacility>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{"access", std::nullopt};
    config.Cacher(cache);
    auto provider = std::make_shared<NOidc::NPrivate::TStaticProvider>(config, facility);
    auto pending = provider->GetAuthInfoAsync();
    const bool entered = cache->Entered.GetFuture().Wait(TDuration::Seconds(5));
    auto stopped = std::async(std::launch::async, [provider] { provider->Stop(); });
    const bool cancelled = pending.Wait(TDuration::Seconds(5));
    cache->Release.TrySetValue();
    stopped.get();
    UNIT_ASSERT(entered);
    UNIT_ASSERT(cancelled);
    UNIT_ASSERT(pending.HasException());
    UNIT_ASSERT(provider->GetAuthInfoAsync().HasException());
    UNIT_ASSERT(!provider->IsValid());
}

Y_UNIT_TEST(FacilityExpiredDuringCacheWriteCompletesWithError) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    auto facility = std::make_shared<TQueuedOidcFacility>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{"access", std::nullopt};
    config.Cacher(cache);
    auto provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
    const auto pending = provider->GetAuthInfoAsync();
    const bool entered = cache->Entered.GetFuture().Wait(TDuration::Seconds(5));
    facility.reset();
    cache->Release.TrySetValue();
    UNIT_ASSERT(entered);
    UNIT_ASSERT(pending.Wait(TDuration::Seconds(5)));
    UNIT_ASSERT(pending.HasException());
    UNIT_ASSERT(!provider->IsValid());
}

Y_UNIT_TEST(StopIgnoresThrowingPendingSubscriber) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    auto facility = std::make_shared<TQueuedOidcFacility>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{"access", std::nullopt};
    config.Cacher(cache);
    NOidc::NPrivate::TStaticProvider provider(config, facility);
    auto pending = provider.GetAuthInfoAsync();
    pending.Subscribe([](const auto&) { throw std::runtime_error("subscriber failure"); });
    auto stopped = std::async(std::launch::async, [&] { provider.Stop(); });
    const bool cancelled = pending.Wait(TDuration::Seconds(5));
    cache->Release.TrySetValue();
    UNIT_ASSERT_NO_EXCEPTION(stopped.get());
    UNIT_ASSERT(cancelled);
    UNIT_ASSERT(pending.HasException());
    UNIT_ASSERT(!provider.IsValid());
}

Y_UNIT_TEST(StopIgnoresThrowingQueuedSubscriber) {
    auto cache = std::make_shared<TGatedOidcCacher>();
    TOidcConfig config;
    config.Issuer = "https://issuer.example";
    config.FlowConfig = TStaticOidcConfig{"access", std::nullopt};
    config.Cacher(cache);
    auto facility = std::make_shared<TQueuedOidcFacility>();
    NOidc::NPrivate::TStaticProvider provider(config, facility);
    auto pending = provider.GetAuthInfoAsync();
    pending.Subscribe([](const auto&) { throw std::runtime_error("subscriber failure"); });
    cache->Release.TrySetValue();
    const bool queued = facility->WaitForTask();
    std::exception_ptr stopError;
    try {
        provider.Stop();
    } catch (...) {
        stopError = std::current_exception();
    }
    UNIT_ASSERT(queued);
    UNIT_ASSERT(stopError == nullptr);
    UNIT_ASSERT(pending.HasException());
    UNIT_ASSERT_NO_EXCEPTION(facility->RunTasks());
}

Y_UNIT_TEST(TransientInvalidGrantRetriesRefreshWithoutStartingAnotherFlow) {
    for (const bool device : {false, true}) {
        TOidcTestServer server;
        auto replyGate = NThreading::NewPromise<void>();
        server.BlockTokenRepliesUntil(replyGate.GetFuture());
        server.Enqueue(R"({"error":"invalid_grant"})", HTTP_SERVICE_UNAVAILABLE);
        server.Enqueue(R"({"access_token":"refreshed","token_type":"Bearer","expires_in":600})", HTTP_OK);
        auto cache = std::make_shared<TMemoryTokenCacher>();
        cache->Write({{"expired", TInstant::Seconds(1)}, TOAuthToken{"refresh", std::nullopt}});
        auto config = server.ClientConfig().Cacher(cache);
        if (device) {
            config.FlowConfig = TDeviceOidcConfig{"client", {}};
        }
        auto facility = std::make_shared<TQueuedOidcFacility>();
        auto provider = CreateOidcProviderFactory(config)->CreateProvider(facility);
        auto firstAttempt = provider->GetAuthInfoAsync();
        replyGate.TrySetValue();
        UNIT_ASSERT(facility->WaitForTask());
        facility->RunTasks();
        UNIT_ASSERT(firstAttempt.HasException());
        if (!provider->GetAuthInfoAsync().HasValue()) {
            UNIT_ASSERT(facility->WaitForTask());
            facility->RunTasks();
        }
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer refreshed");
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 2);
        for (const auto& request : requests) {
            UNIT_ASSERT_VALUES_EQUAL(request.Form.Get("grant_type"), "refresh_token");
            UNIT_ASSERT_VALUES_EQUAL(request.Form.Get("refresh_token"), "refresh");
        }
    }
}

Y_UNIT_TEST(PersistentOutageSettlesPendingCredentials) {
    TOidcTestServer server;
    for (size_t i = 0; i < 10; ++i) {
        server.Enqueue(R"({"error":"temporarily_unavailable"})", HTTP_SERVICE_UNAVAILABLE);
    }
    auto provider = CreateOidcProviderFactory(server.ClientConfig())->CreateProvider();
    auto pending = provider->GetAuthInfoAsync();
    UNIT_ASSERT(pending.Wait(TDuration::Seconds(2)));
    UNIT_ASSERT_EXCEPTION_CONTAINS(pending.GetValueSync(), std::exception, "503");
    UNIT_ASSERT_EXCEPTION_CONTAINS(provider->GetAuthInfo(), std::exception, "503");
    UNIT_ASSERT(!provider->IsValid());
}

Y_UNIT_TEST(ShutdownJoinsWorkerDuringTlsHandshake) {
    TOidcTestServer server;
    auto gate = NThreading::NewPromise<void>();
    server.BlockTlsHandshakeUntil(gate.GetFuture());
    auto provider = CreateOidcProviderFactory(server.ClientConfig())->CreateProvider();
    auto pending = provider->GetAuthInfoAsync();
    const bool started = server.WaitForTlsHandshake();
    auto stopped = std::async(std::launch::async, [provider = std::move(provider)]() mutable {
        provider.reset();
    });
    const bool completed = stopped.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    gate.TrySetValue();
    stopped.get();
    UNIT_ASSERT(started);
    UNIT_ASSERT(!completed);
    UNIT_ASSERT(pending.HasException());
}

Y_UNIT_TEST(ClientAcceptsOpaqueTokenWithoutLifetime) {
    TOidcTestServer server;
    server.Enqueue(R"({"access_token":"opaque","token_type":"Bearer"})", HTTP_OK);
    auto cache = std::make_shared<TMemoryTokenCacher>();
    auto provider = CreateOidcProviderFactory(server.ClientConfig().Cacher(cache))->CreateProvider();
    UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer opaque");
    UNIT_ASSERT(provider->IsValid());
    const auto stored = cache->Read();
    UNIT_ASSERT(stored.has_value() && !stored->AccessToken.ExpiresAt.has_value());
    UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 1);
}

} // Y_UNIT_TEST_SUITE(TOidcCredentials)
