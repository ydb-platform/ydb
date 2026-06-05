#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/core_facility/simple_core_facility.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/helpers/iam_grpc_mock_server.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/helpers/iam_test_keys.h>

#include <ydb/public/api/client/yc_public/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/client/yc_public/iam/iam_token_service.pb.h>

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

using namespace NYdb;
using namespace NYdb::NTest;
using namespace yandex::cloud::iam::v1;

TEST(GrpcIamCredentialsProvider, TeardownWhileIamCreatePendingCompletes) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    TIamOAuth params = MakeOAuthParams(server.Endpoint());
    // Short client deadline: completion arrives with DEADLINE_EXCEEDED while the server handler is still
    // blocked waiting for Release() (in-flight work on the server side vs client teardown).
    params.RequestTimeout = TDuration::MilliSeconds(400);

    auto work = [&params] {
        auto facility = std::make_shared<TSimpleCoreFacility>();
        TIamOAuthCredentialsProvider<CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService> provider(
            params,
            facility);
        (void)provider;
    };

    std::future<void> done = std::async(std::launch::async, work);

    ASSERT_TRUE(iamService.WaitUntilRpcEntered(std::chrono::seconds(5)))
        << "server should have accepted the IAM Create call";

    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "provider destructor must finish while an IAM Create is still blocked on the server "
           "(in-flight RPC vs channel teardown).";
    done.get();

    iamService.Release();
    server.Stop();
}

TEST(GrpcIamCredentialsProvider, TeardownWhileIamCreatePendingCompletesViaFactoryWrapper) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    TIamOAuth params = MakeOAuthParams(server.Endpoint());
    params.RequestTimeout = TDuration::MilliSeconds(400);

    auto factory = std::make_shared<TIamOAuthCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(params);

    auto work = [&factory] {
        auto provider = factory->CreateProvider();
        (void)provider;
    };

    std::future<void> done = std::async(std::launch::async, work);

    ASSERT_TRUE(iamService.WaitUntilRpcEntered(std::chrono::seconds(5)))
        << "server should have accepted the IAM Create call";

    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "factory wrapper teardown must finish while an IAM Create is still blocked on the server";
    done.get();

    iamService.Release();
    server.Stop();
}

// Regression test for backward compatibility of the deprecated no-arg
// ICredentialsProviderFactory::CreateProvider() entry point on IAM factories. After commit
// 0140ad8 ("fix sdk: fixed self thread join in iam cred provider") this entry point was
// throwing "Not supported" and broke all out-of-tree callers that didn't yet plumb an
// ICoreFacility. The factory must spin up a private facility transparently.
TEST(GrpcIamCredentialsProvider, NoArgCreateProviderBackwardCompat) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    iamService.Release();

    TIamOAuth params = MakeOAuthParams(server.Endpoint());

    auto factory = std::make_shared<TIamOAuthCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(params);

    auto work = [&factory]() -> std::string {
        auto provider = factory->CreateProvider();
        return provider->GetAuthInfo();
    };

    std::future<std::string> done = std::async(std::launch::async, work);
    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "no-arg CreateProvider() path must produce a token and tear down cleanly";
    EXPECT_EQ(done.get(), "released-token");

    server.Stop();
}

// Regression test for the deprecated no-arg CreateProvider() on the JWT factory. Mirrors the
// OAuth counterpart above. The standalone path spins up a private TSimpleCoreFacility behind
// TOwningFacilityCredentialsProvider; this verifies the JWT specialization wires it up
// correctly and tears down without aborting.
TEST(GrpcIamCredentialsProvider, NoArgCreateProviderBackwardCompatJwt) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    iamService.Release();

    auto factory = std::make_shared<TIamJwtCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(MakeJwtParams(server.Endpoint()));

    auto work = [&factory]() -> std::string {
        auto provider = factory->CreateProvider();
        return provider->GetAuthInfo();
    };

    std::future<std::string> done = std::async(std::launch::async, work);
    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "no-arg CreateProvider() path must produce a token and tear down cleanly";
    EXPECT_EQ(done.get(), "released-token");

    server.Stop();
}

namespace {

class TSlowBlockingAuthProvider final : public ICredentialsProvider {
public:
    std::string GetAuthInfo() const override {
        std::unique_lock lock(Mutex_);
        if (++CallCount_ > 1) {
            BlockedCv_.wait(lock, [this] { return Released_; });
        }
        return "slow-auth-token";
    }

    bool IsValid() const override {
        return true;
    }

    void Release() {
        {
            std::lock_guard lock(Mutex_);
            Released_ = true;
        }
        BlockedCv_.notify_all();
    }

    bool WaitUntilBlocked(std::chrono::milliseconds timeout) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            std::lock_guard lock(Mutex_);
            if (CallCount_ > 1 && !Released_) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        std::lock_guard lock(Mutex_);
        return CallCount_ > 1 && !Released_;
    }

private:
    mutable std::mutex Mutex_;
    mutable std::condition_variable BlockedCv_;
    mutable int CallCount_ = 0;
    bool Released_ = false;
};

} // namespace

// Regression: Stop() must not hang when FillContext is blocked inside AuthTokenProvider_->GetAuthInfo().
TEST(GrpcIamCredentialsProvider, StopDuringFillContextDoesNotHang) {
    TBlockingIamTokenService iamService;
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    iamService.Release();

    auto authProvider = std::make_shared<TSlowBlockingAuthProvider>();

    TIamOAuth params = MakeOAuthParams(server.Endpoint());
    params.RefreshPeriod = TDuration::MilliSeconds(50);

    std::shared_ptr<TGrpcIamCredentialsProvider<CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>> provider;
    auto facility = std::make_shared<TSimpleCoreFacility>();

    provider = std::make_shared<TGrpcIamCredentialsProvider<CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(
        params,
        [token = params.OAuthToken](CreateIamTokenRequest& req) {
            req.set_yandex_passport_oauth_token(TStringType{token});
        },
        [](IamTokenService::Stub* stub, grpc::ClientContext* context, const CreateIamTokenRequest* request,
           CreateIamTokenResponse* response, std::function<void(grpc::Status)> cb) {
            stub->async()->Create(context, request, response, std::move(cb));
        },
        facility,
        authProvider);

    ASSERT_TRUE(authProvider->WaitUntilBlocked(std::chrono::seconds(10)))
        << "FillContext should block inside slow AuthTokenProvider during refresh";

    std::future<void> stopDone = std::async(std::launch::async,
        [provider = std::move(provider)]() mutable {
            provider.reset();
        });

    ASSERT_EQ(stopDone.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "provider destructor (Stop()) must complete while FillContext is blocked in GetAuthInfo()";
    stopDone.get();

    authProvider->Release();
    facility.reset();

    server.Stop();
}
