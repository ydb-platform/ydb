#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>

#include <ydb/public/sdk/cpp/tests/integration/iam/helpers/iam_http_mock_server.h>

#include <gtest/gtest.h>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

using namespace NYdb;
using namespace NYdb::NTest;

TEST(IamCredentialsProvider, ExpiryFieldSupport) {
    TMetadataServer server;
    auto expiry = TInstant::Now() + TDuration::Hours(12);
    server.SetResponse(HTTP_OK, MakeTokenResponseWithExpiry("expiry-token", expiry));

    TIamHost params = MakeMetadataParams(server.Port);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), "expiry-token");

    // Second call should not trigger refresh (token is still valid)
    int countBefore = server.GetRequestCount();
    provider->GetAuthInfo();
    EXPECT_EQ(server.GetRequestCount(), countBefore);
}

TEST(IamCredentialsProvider, NoExpiryFieldFallback) {
    TMetadataServer server;
    server.SetResponse(HTTP_OK, MakeTokenResponseNoExpiry("no-expiry-token"));

    TIamHost params = MakeMetadataParams(server.Port);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    // Token should be saved even without expires_in/expiry
    EXPECT_EQ(provider->GetAuthInfo(), "no-expiry-token");

    // Should not immediately refresh (fallback interval should be > 0)
    int countBefore = server.GetRequestCount();
    provider->GetAuthInfo();
    EXPECT_EQ(server.GetRequestCount(), countBefore);
}

TEST(IamCredentialsProvider, ServerError) {
    TMetadataServer server;
    server.SetResponse(HTTP_INTERNAL_SERVER_ERROR, "");

    TIamHost params = MakeMetadataParams(server.Port);

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    // Constructor GetTicket() fails, token should be empty
    EXPECT_EQ(provider->GetAuthInfo(), "");
}

TEST(IamCredentialsProvider, ConcurrentAccess) {
    TMetadataServer server;
    server.SetResponse(HTTP_OK, MakeTokenResponse("concurrent-token", 3600));

    TIamHost params = MakeMetadataParams(server.Port);
    params.RefreshPeriod = TDuration::MilliSeconds(1); // Force frequent refreshes

    auto factory = CreateIamCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    constexpr int NUM_THREADS = 8;
    constexpr int ITERATIONS = 100;

    std::vector<std::unique_ptr<std::thread>> threads;
    std::atomic<int> errors{0};

    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.push_back(std::make_unique<std::thread>([&]() {
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
        }));
    }

    for (auto& t : threads) {
        t->join();
    }

    EXPECT_EQ(errors.load(), 0);
}
