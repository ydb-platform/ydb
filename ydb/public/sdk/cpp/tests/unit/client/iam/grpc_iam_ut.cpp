#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/src/client/types/core_facility/simple_core_facility.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_grpc_mock_server.h>

#include <ydb/public/api/client/yc_public/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/client/yc_public/iam/iam_token_service.pb.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <util/generic/yexception.h>

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

    auto facility = std::make_shared<TSimpleCoreFacility>();
    auto provider = std::make_shared<TIamOAuthCredentialsProvider<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(params, facility);
    auto authInfo = provider->GetAuthInfoAsync();
    ASSERT_FALSE(authInfo.IsReady());

    ASSERT_TRUE(iamService.WaitUntilRpcEntered(std::chrono::seconds(5)))
        << "server should have accepted the IAM Create call";

    std::future<void> done = std::async(std::launch::async, [provider = std::move(provider)]() mutable {
        provider.reset();
    });

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

    auto provider = factory->CreateProvider();

    ASSERT_TRUE(iamService.WaitUntilRpcEntered(std::chrono::seconds(5)))
        << "server should have accepted the IAM Create call";

    std::future<void> done = std::async(std::launch::async, [provider = std::move(provider)]() mutable {
        provider.reset();
    });

    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "factory wrapper teardown must finish while an IAM Create is still blocked on the server";
    done.get();

    iamService.Release();
    server.Stop();
}

TEST(GrpcIamCredentialsProviderFactory, NoArgProvidersAreCachedAcrossFactoryInstances) {
    TIamTokenServiceStub iamStub;
    iamStub.SetResponseToken("unit-test-iam-token");
    TIamGrpcServer server(&iamStub);
    ASSERT_TRUE(server.Start());

    using TOAuthFactory = TIamOAuthCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>;
    const auto oauthParams = MakeOAuthParams(server.Endpoint());

    std::vector<std::future<TCredentialsProviderPtr>> oauthProviders;
    for (size_t i = 0; i < 8; ++i) {
        auto factory = std::make_shared<TOAuthFactory>(oauthParams);
        oauthProviders.emplace_back(std::async(std::launch::async, [factory] {
            return factory->CreateProvider();
        }));
    }

    auto firstOAuthProvider = oauthProviders.front().get();
    for (size_t i = 1; i < oauthProviders.size(); ++i) {
        EXPECT_EQ(firstOAuthProvider, oauthProviders[i].get());
    }
    EXPECT_EQ(firstOAuthProvider->GetAuthInfo(), "unit-test-iam-token");
    EXPECT_EQ(iamStub.GetRequestCount(), 1);

    using TJwtFactory = TIamJwtCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>;
    const auto jwtParams = MakeJwtParams(server.Endpoint());
    auto firstJwtProvider = std::make_shared<TJwtFactory>(jwtParams)->CreateProvider();
    auto secondJwtProvider = std::make_shared<TJwtFactory>(jwtParams)->CreateProvider();

    EXPECT_EQ(firstJwtProvider, secondJwtProvider);
    EXPECT_EQ(firstJwtProvider->GetAuthInfo(), "unit-test-iam-token");
    EXPECT_EQ(iamStub.GetRequestCount(), 2);

    auto differentOAuthParams = oauthParams;
    differentOAuthParams.OAuthToken = "different-oauth-token";
    auto differentOAuthProvider = std::make_shared<TOAuthFactory>(differentOAuthParams)->CreateProvider();
    EXPECT_NE(firstOAuthProvider, differentOAuthProvider);
    EXPECT_EQ(differentOAuthProvider->GetAuthInfo(), "unit-test-iam-token");
    EXPECT_EQ(iamStub.GetRequestCount(), 3);

    server.Stop();
}

TEST(GrpcIamCredentialsProviderFactory, ReadyCallbackCanReleaseNoArgProvider) {
    TIamTokenServiceStub iamStub;
    iamStub.SetResponseToken("unit-test-iam-token");
    TIamGrpcServer server(&iamStub);
    ASSERT_TRUE(server.Start());

    auto provider = std::make_shared<TIamOAuthCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(
            MakeOAuthParams(server.Endpoint()))->CreateProvider();
    auto released = std::make_shared<std::promise<void>>();
    auto authInfo = provider->GetAuthInfoAsync();
    ASSERT_TRUE(authInfo.IsReady());
    authInfo.Subscribe(
        [provider = std::move(provider), released](const auto&) mutable {
            provider.reset();
            released->set_value();
        });

    ASSERT_EQ(released->get_future().wait_for(std::chrono::seconds(10)), std::future_status::ready);
    server.Stop();
}

namespace {

class TSlowBlockingAuthProvider final : public ICredentialsProvider {
public:
    std::string GetAuthInfo() const override {
        std::unique_lock lock(Mutex_);
        if (++CallCount_ > 1) {
            BlockedCv_.notify_all();
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
        std::unique_lock lock(Mutex_);
        return BlockedCv_.wait_for(lock, timeout, [this] { return CallCount_ > 1; });
    }

private:
    mutable std::mutex Mutex_;
    mutable std::condition_variable BlockedCv_;
    mutable int CallCount_ = 0;
    bool Released_ = false;
};

class TDeferredAuthProvider final : public ICredentialsProvider {
public:
    TDeferredAuthProvider()
        : AuthInfo_(NThreading::NewPromise<std::string>())
    {}

    std::string GetAuthInfo() const override {
        return AuthInfo_.GetFuture().GetValueSync();
    }

    NThreading::TFuture<std::string> GetAuthInfoAsync() const override {
        ++CallCount_;
        return AuthInfo_.GetFuture();
    }

    bool IsValid() const override {
        return true;
    }

    int CallCount() const {
        return CallCount_.load();
    }

    void SetReady() {
        AuthInfo_.SetValue("auth-token");
    }

private:
    mutable std::atomic<int> CallCount_{0};
    NThreading::TPromise<std::string> AuthInfo_;
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

TEST(GrpcIamCredentialsProvider, WaitsForNestedAuthWithoutPollingIt) {
    TIamTokenServiceStub iamStub;
    iamStub.SetResponseToken("unit-test-iam-token");
    TIamGrpcServer server(&iamStub);
    ASSERT_TRUE(server.Start());

    auto authProvider = std::make_shared<TDeferredAuthProvider>();

    TIamOAuth params = MakeOAuthParams(server.Endpoint());
    auto facility = std::make_shared<TSimpleCoreFacility>();

    TGrpcIamCredentialsProvider<CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService> provider(
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

    auto future = provider.GetAuthInfoAsync();
    for (int i = 0; i < 100 && authProvider->CallCount() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_EQ(authProvider->CallCount(), 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(authProvider->CallCount(), 1);

    authProvider->SetReady();
    ASSERT_TRUE(future.Wait(TDuration::Seconds(10)));
    EXPECT_EQ(future.GetValue(), "unit-test-iam-token");
    EXPECT_EQ(iamStub.GetRequestCount(), 1);

    server.Stop();
}

TEST(GrpcIamCredentialsProvider, RetriesTransientFailure) {
    TIamTokenServiceStub iamStub;
    iamStub.SetResponseToken("unit-test-iam-token");
    iamStub.SetStatus(grpc::Status(grpc::StatusCode::UNAVAILABLE, "retry"));
    TIamGrpcServer server(&iamStub);
    ASSERT_TRUE(server.Start());

    auto facility = std::make_shared<TSimpleCoreFacility>();
    auto provider = std::make_shared<TIamOAuthCredentialsProvider<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(
            MakeOAuthParams(server.Endpoint()), facility);
    auto future = provider->GetAuthInfoAsync();
    for (int i = 0; i < 1000 && iamStub.GetRequestCount() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_GT(iamStub.GetRequestCount(), 0);
    ASSERT_FALSE(future.IsReady());

    iamStub.SetStatus(grpc::Status::OK);
    ASSERT_TRUE(future.Wait(TDuration::Seconds(10)));
    EXPECT_EQ(future.GetValue(), "unit-test-iam-token");
    EXPECT_GT(iamStub.GetRequestCount(), 1);

    server.Stop();
}

TEST(GrpcIamCredentialsProvider, DoesNotRetryTerminalFailure) {
    TIamTokenServiceStub iamStub;
    iamStub.SetStatus(grpc::Status(grpc::StatusCode::PERMISSION_DENIED, "terminal"));
    TIamGrpcServer server(&iamStub);
    ASSERT_TRUE(server.Start());

    auto facility = std::make_shared<TSimpleCoreFacility>();
    auto provider = std::make_shared<TIamOAuthCredentialsProvider<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(
            MakeOAuthParams(server.Endpoint()), facility);
    auto future = provider->GetAuthInfoAsync();
    ASSERT_TRUE(future.Wait(TDuration::Seconds(10)));
    EXPECT_THROW(future.GetValue(), yexception);
    const int requests = iamStub.GetRequestCount();
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(iamStub.GetRequestCount(), requests);

    server.Stop();
}
