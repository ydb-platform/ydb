#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_http_mock_server.h>

#include <gtest/gtest.h>

#include <util/datetime/base.h>

#include <atomic>
#include <thread>
#include <vector>

using namespace NYdb;
using namespace NYdb::NTest;

TEST(IamCredentialsProvider, ExpiryFieldSupport) {
    TMetadataServer server;
    server.SetStrictMode(false);
    auto expiry = TInstant::Now() + TDuration::Hours(12);
    server.SetResponse(HTTP_OK, MakeTokenResponseWithExpiry("expiry-token", expiry));

    TIamHost params = MakeMetadataParams(server.Port);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), "expiry-token");

    int countBefore = server.GetRequestCount();
    provider->GetAuthInfo();
    EXPECT_EQ(server.GetRequestCount(), countBefore);
}

TEST(IamCredentialsProvider, NoExpiryFieldFallback) {
    TMetadataServer server;
    server.SetStrictMode(false);
    server.SetResponse(HTTP_OK, MakeTokenResponseNoExpiry("no-expiry-token"));

    TIamHost params = MakeMetadataParams(server.Port);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), "no-expiry-token");

    int countBefore = server.GetRequestCount();
    provider->GetAuthInfo();
    EXPECT_EQ(server.GetRequestCount(), countBefore);
}

TEST(IamCredentialsProvider, ServerError) {
    TMetadataServer server;
    server.SetStrictMode(false);
    server.SetResponse(HTTP_INTERNAL_SERVER_ERROR, "");

    TIamHost params = MakeMetadataParams(server.Port);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    EXPECT_THROW(provider->GetAuthInfo(), yexception);
}

TEST(IamCredentialsProvider, GracePeriodOnRefreshError) {
    TMetadataServer server;
    server.SetStrictMode(false);
    server.SetResponse(HTTP_OK, MakeTokenResponse("old-token", 3600));

    TIamHost params = MakeMetadataParams(server.Port);
    params.RefreshPeriod = TDuration::MilliSeconds(100);

    auto provider = CreateIamCredentialsProviderFactory(params)->CreateProvider();
    EXPECT_EQ(provider->GetAuthInfo(), "old-token");

    int countBeforeRefresh = server.GetRequestCount();
    Sleep(TDuration::MilliSeconds(150));

    server.SetResponse(HTTP_INTERNAL_SERVER_ERROR, "");
    EXPECT_EQ(provider->GetAuthInfo(), "old-token");
    EXPECT_GT(server.GetRequestCount(), countBeforeRefresh);
}

TEST(IamCredentialsProvider, ThrowAfterTokenExpiredOnRefreshError) {
    TMetadataServer server;
    server.SetStrictMode(false);
    server.SetResponse(HTTP_OK, MakeTokenResponse("old-token", 1));

    TIamHost params = MakeMetadataParams(server.Port);
    params.RefreshPeriod = TDuration::MilliSeconds(100);

    auto provider = CreateIamCredentialsProviderFactory(params)->CreateProvider();
    EXPECT_EQ(provider->GetAuthInfo(), "old-token");

    Sleep(TDuration::MilliSeconds(150));
    server.SetResponse(HTTP_INTERNAL_SERVER_ERROR, "");
    EXPECT_EQ(provider->GetAuthInfo(), "old-token");

    Sleep(TDuration::Seconds(1));
    EXPECT_THROW(provider->GetAuthInfo(), yexception);
}

TEST(IamCredentialsProvider, RecoveryAfterRefreshError) {
    TMetadataServer server;
    server.SetStrictMode(false);
    server.SetResponse(HTTP_OK, MakeTokenResponse("token-1", 3600));

    TIamHost params = MakeMetadataParams(server.Port);
    params.RefreshPeriod = TDuration::MilliSeconds(100);

    auto provider = CreateIamCredentialsProviderFactory(params)->CreateProvider();
    EXPECT_EQ(provider->GetAuthInfo(), "token-1");

    Sleep(TDuration::MilliSeconds(150));
    server.SetResponse(HTTP_INTERNAL_SERVER_ERROR, "");
    EXPECT_EQ(provider->GetAuthInfo(), "token-1");

    server.SetResponse(HTTP_OK, MakeTokenResponse("token-2", 3600));
    int countBeforeRecovery = server.GetRequestCount();
    Sleep(TDuration::MilliSeconds(150));

    EXPECT_EQ(provider->GetAuthInfo(), "token-2");
    EXPECT_GT(server.GetRequestCount(), countBeforeRecovery);
}

TEST(IamCredentialsProvider, ConcurrentAccess) {
    TMetadataServer server;
    server.SetStrictMode(false);
    server.SetResponse(HTTP_OK, MakeTokenResponse("concurrent-token", 3600));

    TIamHost params = MakeMetadataParams(server.Port);
    params.RefreshPeriod = TDuration::MilliSeconds(1);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    constexpr int NUM_THREADS = 8;
    constexpr int ITERATIONS = 100;

    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);
    std::atomic<int> errors{0};

    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back([&]() {
            for (int j = 0; j < ITERATIONS; ++j) {
                try {
                    auto token = provider->GetAuthInfo();
                    if (token != "concurrent-token") {
                        errors.fetch_add(1);
                    }
                } catch (...) {
                    errors.fetch_add(1);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    EXPECT_EQ(errors.load(), 0);
}
