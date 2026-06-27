#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/core_facility/simple_core_facility.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_grpc_mock_server.h>

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
    TBlockingIamReleaseGuard releaseGuard(iamService);
    TIamGrpcServer server(&iamService);
    ASSERT_TRUE(server.Start());

    TIamOAuth params = MakeOAuthParams(server.Endpoint());
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
    TBlockingIamReleaseGuard releaseGuard(iamService);
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

TEST(GrpcIamCredentialsProvider, StopDuringFillContextDoesNotHang) {
    TIamTokenServiceStub iamStub;
    iamStub.SetResponseToken("unit-test-iam-token");
    TIamGrpcServer server(&iamStub);
    ASSERT_TRUE(server.Start());

    auto authProvider = std::make_shared<TSlowBlockingAuthProvider>();

    TIamOAuth params = MakeOAuthParams(server.Endpoint());
    params.RefreshPeriod = TDuration::MilliSeconds(50);
    params.RequestTimeout = TDuration::MilliSeconds(400);

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

    struct TAuthReleaseGuard {
        std::shared_ptr<TSlowBlockingAuthProvider> Provider;
        ~TAuthReleaseGuard() {
            if (Provider) {
                Provider->Release();
            }
        }
    } authReleaseGuard{authProvider};

    ASSERT_TRUE(authProvider->WaitUntilBlocked(std::chrono::seconds(30)))
        << "FillContext should block inside slow AuthTokenProvider during refresh";

    std::future<void> stopDone = std::async(std::launch::async,
        [provider = std::move(provider)]() mutable {
            provider.reset();
        });

    ASSERT_EQ(stopDone.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "provider destructor (Stop()) must complete while FillContext is blocked in GetAuthInfo()";
    stopDone.get();

    server.Stop();
}
