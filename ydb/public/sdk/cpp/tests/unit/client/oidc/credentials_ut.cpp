#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/core_facility/core_facility.h>

#include "test_server.h"
#include <ydb/public/sdk/cpp/src/client/types/credentials/oidc/private.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <future>

using namespace NYdb;

Y_UNIT_TEST_SUITE(TOidcCredentials) {
    Y_UNIT_TEST(BearerTicketDoesNotChangeCachedToken) {
        auto cache = std::make_shared<TGatedOidcCacher>();
        TOidcConfig config;
        config.Issuer = "https://issuer.example";
        config.FlowConfig = TStaticOidcConfig{
            .AccessToken = "opaque-access",
            .RefreshToken = "opaque-refresh",
            .ClientId = "client",
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
        UNIT_ASSERT(stored && stored->RefreshToken);
        UNIT_ASSERT_VALUES_EQUAL(stored->AccessToken.Token, "opaque-access");
        UNIT_ASSERT_VALUES_EQUAL(stored->RefreshToken->Token, "opaque-refresh");
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

    Y_UNIT_TEST(ClientRetriesTransientError) {
        TOidcTestServer server;
        server.Enqueue(R"({"error":"temporarily_unavailable"})", HTTP_SERVICE_UNAVAILABLE);
        server.Enqueue(R"({"access_token":"retried","token_type":"Bearer","expires_in":600})", HTTP_OK);
        auto provider = CreateOidcProviderFactory(server.ClientConfig())->CreateProvider();
        auto token = provider->GetAuthInfoAsync();
        UNIT_ASSERT(token.Wait(TDuration::Seconds(5)));
        UNIT_ASSERT_VALUES_EQUAL(token.GetValueSync(), "Bearer retried");
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 2);
    }

    Y_UNIT_TEST(JwtExpiryIsOnlyASchedulingHint) {
        using NYdb::NOidc::NPrivate::JwtExpiry;
        const std::string token = "e30." + std::string(Base64EncodeUrl(R"({"exp":2000000000})")) + ".signature";
        UNIT_ASSERT(JwtExpiry(token));
        UNIT_ASSERT_VALUES_EQUAL(*JwtExpiry(token), TInstant::Seconds(2000000000));
        UNIT_ASSERT(!JwtExpiry("opaque"));
        UNIT_ASSERT(!JwtExpiry("e30.invalid.signature"));
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

    Y_UNIT_TEST(RejectsMissingClientSecret) {
        TOidcConfig config;
        config.Issuer = "https://issuer.example";
        TClientOidcConfig flow;
        flow.ClientId = "client";
        config.FlowConfig = flow;
        UNIT_ASSERT_EXCEPTION(CreateOidcProviderFactory(config), std::invalid_argument);
    }

    Y_UNIT_TEST(RejectsInsecureIssuerByDefault) {
        TOidcTestServer server;
        auto config = server.ClientConfig();
        config.AllowInsecureHttp(false);
        UNIT_ASSERT_EXCEPTION(CreateOidcProviderFactory(config), std::invalid_argument);
        UNIT_ASSERT_VALUES_EQUAL(server.DiscoveryCount(), 0);
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
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("client_id"), "client");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("client_secret"), "secret +&");
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("scope"), "read write");
    }

    Y_UNIT_TEST(ClientSecretBasic) {
        TOidcTestServer server;
        server.Enqueue(R"({"access_token":"access","token_type":"bearer","expires_in":600})", HTTP_OK);
        auto config = server.ClientConfig();
        config.TokenEndpointAuthMethod("client_secret_basic");
        auto factory = CreateOidcProviderFactory(config);
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer access");
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Authorization, "Basic " + Base64Encode("client:secret+%2B%26"));
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
        UNIT_ASSERT(stored && stored->RefreshToken && stored->RefreshToken->ExpiresAt);
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

    Y_UNIT_TEST(StaticRefreshIncludesClientId) {
        TOidcTestServer server;
        server.Enqueue(R"({"access_token":"fresh","token_type":"Bearer","expires_in":600})", HTTP_OK);
        auto config = server.ClientConfig();
        TStaticOidcConfig flow;
        flow.AccessToken = "expired";
        flow.ExpiresAt = TInstant::Seconds(1);
        flow.RefreshToken = "refresh";
        flow.ClientId = "public-client";
        config.FlowConfig = flow;
        auto factory = CreateOidcProviderFactory(config);
        UNIT_ASSERT_VALUES_EQUAL(factory->CreateProvider()->GetAuthInfo(), "Bearer fresh");
        const auto requests = server.Requests();
        UNIT_ASSERT_VALUES_EQUAL(requests[0].Form.Get("client_id"), "public-client");
    }

    Y_UNIT_TEST(RejectsMalformedResponsesWithoutLeakingSecrets) {
        for (const TString& body : {
                 TString("secret-response-is-not-json"),
                 TString(R"({"access_token":"secret-access","token_type":"unsupported-secret","expires_in":600})"),
                 TString(R"({"access_token":"secret-access","token_type":"Bearer","expires_in":0})"),
                 TString(R"({"access_token":"secret-access","token_type":"Bearer"})")}) {
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
        server.Enqueue(TString("{\"device_code\":\"private-device\",\"user_code\":\"ABCD\",\"verification_uri\":\"") + server.Issuer() + "/verify\",\"expires_in\":60,\"interval\":1}", HTTP_OK);
        server.Enqueue(R"({"access_token":"user-access","token_type":"Bearer","expires_in":600,"refresh_token":"user-refresh"})", HTTP_OK);
        auto acceptor = std::make_shared<TTestAcceptor>();
        auto config = server.ClientConfig().Acceptor(acceptor);
        config.FlowConfig = TDeviceOidcConfig{"public-client", {"openid"}};
        auto factory = CreateOidcProviderFactory(config);
        auto provider = factory->CreateProvider();
        UNIT_ASSERT_VALUES_EQUAL(acceptor->Wait().UserCode, "ABCD");
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer user-access");
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
        acceptor->Wait();
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

    Y_UNIT_TEST(StaticRefreshReadsLatestSharedCache) {
        TOidcTestServer server;
        server.Enqueue(R"({"access_token":"unnecessary-grant","token_type":"Bearer","expires_in":600})", HTTP_OK);
        auto cache = std::make_shared<TMemoryTokenCacher>();
        auto config = server.ClientConfig().Cacher(cache);
        TStaticOidcConfig flow;
        flow.AccessToken = "initial";
        flow.RefreshToken = "initial-refresh";
        flow.ClientId = "client";
        flow.ExpiresAt = TInstant::Now() + TDuration::Seconds(1);
        config.FlowConfig = flow;
        auto factory = CreateOidcProviderFactory(config);
        auto provider = factory->CreateProvider();
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer initial");
        cache->Write({{"another-process", TInstant::Now() + TDuration::Hours(1)}, TOAuthToken{"rotated", std::nullopt}});
        NThreading::NewPromise<void>().GetFuture().Wait(*flow.ExpiresAt + TDuration::MilliSeconds(1));
        UNIT_ASSERT_VALUES_EQUAL(provider->GetAuthInfo(), "Bearer another-process");
        UNIT_ASSERT_VALUES_EQUAL(server.Requests().size(), 0);
    }

    Y_UNIT_TEST(StandaloneCompletionCanReleaseLastOwner) {
        TOidcTestServer server;
        server.Enqueue(R"({"access_token":"access","token_type":"Bearer","expires_in":600})", HTTP_OK);
        auto replyGate = NThreading::NewPromise<void>();
        server.BlockTokenRepliesUntil(replyGate.GetFuture());
        auto factory = CreateOidcProviderFactory(server.ClientConfig());
        auto provider = factory->CreateProvider();
        auto future = provider->GetAuthInfoAsync();
        auto entered = NThreading::NewPromise<void>();
        auto release = NThreading::NewPromise<void>();
        auto finished = NThreading::NewPromise<void>();
        auto subscribed = NThreading::NewPromise<void>();
        // Subscribe on another thread because an already ready future invokes
        // the callback inline. The gate forces all external owners to be gone.
        auto subscriber = std::async(std::launch::async,
                                     [future, factory = std::move(factory), provider = std::move(provider), entered, release, finished, subscribed]() mutable {
                                         future.Subscribe([factory = std::move(factory), provider = std::move(provider), entered, release, finished](const auto&) mutable {
                                             entered.TrySetValue();
                                             release.GetFuture().Wait();
                                             provider.reset();
                                             factory.reset();
                                             finished.TrySetValue();
                                         });
                                         subscribed.TrySetValue();
                                     });
        subscribed.GetFuture().Wait();
        replyGate.TrySetValue();
        UNIT_ASSERT(entered.GetFuture().Wait(TDuration::Seconds(10)));
        release.TrySetValue();
        UNIT_ASSERT(finished.GetFuture().Wait(TDuration::Seconds(10)));
        subscriber.get();
    }

    Y_UNIT_TEST(HttpRequestHasAbsoluteDeadline) {
        auto gate = NThreading::NewPromise<void>();
        // Request deadline is exercised via the device deadline, which must
        // bound a token request even when the socket timeout is much longer.
        // The device code expires while the server holds its token response.
        TOidcTestServer deviceServer;
        deviceServer.Enqueue(TString("{\"device_code\":\"code\",\"user_code\":\"ABCD\",\"verification_uri\":\"") + deviceServer.Issuer() + "/verify\",\"expires_in\":2,\"interval\":1}", HTTP_OK);
        deviceServer.Enqueue(R"({"access_token":"late","token_type":"Bearer","expires_in":600})", HTTP_OK);
        deviceServer.BlockTokenRepliesUntil(gate.GetFuture());
        auto config = deviceServer.ClientConfig();
        config.SocketTimeout(TDuration::Seconds(10)).ConnectTimeout(TDuration::MilliSeconds(100));
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
} // Y_UNIT_TEST_SUITE(TOidcCredentials)
