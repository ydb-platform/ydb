#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/iam.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_grpc_mock_server.h>

#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.pb.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <exception>
#include <future>
#include <memory>
#include <string>
#include <thread>

using namespace NYdb;
using namespace NYdb::NTest;
using namespace yandex::cloud::priv::iam::v1;

namespace {

// Implements both Create (used by the nested JWT provider) and CreateForService (used by the
// outer service provider). Returns distinct tokens so the outer one is observable via GetAuthInfo().
class TIamServiceStub final : public IamTokenService::Service {
public:
    grpc::Status Create(
        grpc::ServerContext*,
        const CreateIamTokenRequest*,
        CreateIamTokenResponse* response) override
    {
        ++CreateRequestCount_;
        response->set_iam_token("inner-jwt-token");
        response->mutable_expires_at()->set_seconds(4102444800);
        return grpc::Status::OK;
    }

    grpc::Status CreateForService(
        grpc::ServerContext*,
        const CreateIamTokenForServiceRequest*,
        CreateIamTokenResponse* response) override
    {
        ++CreateForServiceRequestCount_;
        response->set_iam_token("outer-service-token");
        response->mutable_expires_at()->set_seconds(4102444800);
        return grpc::Status::OK;
    }

    int GetCreateRequestCount() const {
        return CreateRequestCount_.load();
    }

    int GetCreateForServiceRequestCount() const {
        return CreateForServiceRequestCount_.load();
    }

private:
    std::atomic<int> CreateRequestCount_{0};
    std::atomic<int> CreateForServiceRequestCount_{0};
};

using TJwtFactory = TIamJwtCredentialsProviderFactory<
    CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>;

using TOAuthFactory = TIamOAuthCredentialsProviderFactory<
    CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>;

TIamServiceParams MakeServiceParams(TCredentialsProviderFactoryPtr nestedFactory) {
    TIamServiceParams params;
    params.ServiceId = "unit-test-service";
    params.MicroserviceId = "unit-test-microservice";
    params.ResourceId = "unit-test-resource";
    params.ResourceType = "unit-test-resource-type";
    params.TargetServiceAccountId = "unit-test-target";
    params.SystemServiceAccountCredentials = std::move(nestedFactory);
    return params;
}

class TFlakyIamServiceStub final : public IamTokenService::Service {
public:
    grpc::Status Create(
        grpc::ServerContext*,
        const CreateIamTokenRequest*,
        CreateIamTokenResponse* response) override
    {
        if (Attempts_.fetch_add(1) == 0) {
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "transient failure");
        }
        response->set_iam_token("oauth-token");
        response->mutable_expires_at()->set_seconds(4102444800);
        return grpc::Status::OK;
    }

    size_t Attempts() const {
        return Attempts_.load();
    }

private:
    std::atomic_size_t Attempts_ = 0;
};

class TFailingCoreFacility final : public ICoreFacility {
public:
    void AddPeriodicTask(TPeriodicCb&& callback, TDeadline::Duration) override {
        NYdb::NIssue::TIssues issues;
        callback(std::move(issues), EStatus::CLIENT_CANCELLED);
    }

    void PostToResponseQueue(TPostTaskCb&& callback) override {
        callback();
    }
};

using TTestOAuthFactory = TIamOAuthCredentialsProviderFactory<
    CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>;

TTestOAuthFactory MakeOAuthFactory() {
    TIamOAuth params;
    params.Endpoint = "localhost:1";
    params.EnableSsl = false;
    params.OAuthToken = "token";
    return TTestOAuthFactory(params);
}

} // namespace

TEST(IamCredentialsProviderIdentity, IdentityIsValueBased) {
    const auto jwtParams = MakeJwtParams("iam.example:443");
    const auto firstJwtFactory = std::make_shared<TJwtFactory>(jwtParams);
    const auto secondJwtFactory = std::make_shared<TJwtFactory>(jwtParams);
    const auto jwtIdentity = firstJwtFactory->GetClientIdentity();
    EXPECT_EQ(jwtIdentity, secondJwtFactory->GetClientIdentity());
    auto differentJwtParams = jwtParams;
    differentJwtParams.JwtParams.KeyId = "different-key";
    EXPECT_NE(jwtIdentity, TJwtFactory(differentJwtParams).GetClientIdentity());

    const auto oauthParams = MakeOAuthParams("iam.example:443");
    const auto firstOAuthFactory = std::make_shared<TOAuthFactory>(oauthParams);
    const auto secondOAuthFactory = std::make_shared<TOAuthFactory>(oauthParams);
    const auto oauthIdentity = firstOAuthFactory->GetClientIdentity();
    EXPECT_EQ(oauthIdentity, secondOAuthFactory->GetClientIdentity());
    auto differentOAuthParams = oauthParams;
    differentOAuthParams.OAuthToken = "different-token";
    EXPECT_NE(oauthIdentity, TOAuthFactory(differentOAuthParams).GetClientIdentity());

    auto firstServiceParams = MakeServiceParams(firstJwtFactory);
    const auto firstServiceFactory = CreateIamServiceCredentialsProviderFactory(firstServiceParams);
    const auto secondServiceFactory = CreateIamServiceCredentialsProviderFactory(
        MakeServiceParams(secondJwtFactory));
    const auto serviceIdentity = firstServiceFactory->GetClientIdentity();
    EXPECT_EQ(serviceIdentity, secondServiceFactory->GetClientIdentity());
    firstServiceParams.Endpoint = "different-iam.example:443";
    EXPECT_NE(
        serviceIdentity,
        CreateIamServiceCredentialsProviderFactory(firstServiceParams)->GetClientIdentity());
}

TEST(IamServiceCredentialsProvider, NoArgProviderIsCachedAcrossFactoryInstances) {
    TIamServiceStub stub;
    TIamGrpcServer server(&stub);
    ASSERT_TRUE(server.Start());

    const auto jwtParams = MakeJwtParams(server.Endpoint());
    auto firstServiceParams = MakeServiceParams(std::make_shared<TJwtFactory>(jwtParams));
    firstServiceParams.Endpoint = server.Endpoint();
    firstServiceParams.EnableSsl = false;
    firstServiceParams.RequestTimeout = TDuration::Seconds(5);

    auto secondServiceParams = MakeServiceParams(std::make_shared<TJwtFactory>(jwtParams));
    secondServiceParams.Endpoint = server.Endpoint();
    secondServiceParams.EnableSsl = false;
    secondServiceParams.RequestTimeout = TDuration::Seconds(5);

    auto firstProvider = CreateIamServiceCredentialsProviderFactory(firstServiceParams)->CreateProvider();
    auto secondProvider = CreateIamServiceCredentialsProviderFactory(secondServiceParams)->CreateProvider();

    EXPECT_EQ(firstProvider, secondProvider);
    EXPECT_EQ(firstProvider->GetAuthInfo(), "outer-service-token");
    EXPECT_EQ(stub.GetCreateRequestCount(), 1);
    EXPECT_EQ(stub.GetCreateForServiceRequestCount(), 1);

    secondServiceParams.TargetServiceAccountId = "another-target";
    auto differentProvider = CreateIamServiceCredentialsProviderFactory(secondServiceParams)->CreateProvider();
    EXPECT_NE(firstProvider, differentProvider);
    EXPECT_EQ(differentProvider->GetAuthInfo(), "outer-service-token");
    EXPECT_EQ(stub.GetCreateRequestCount(), 1);
    EXPECT_EQ(stub.GetCreateForServiceRequestCount(), 2);

    server.Stop();
}

TEST(IamCredentialsProvider, AuthInfoFailsWithExpiredFacility) {
    auto provider = MakeOAuthFactory().CreateProvider(std::weak_ptr<ICoreFacility>{});
    auto future = provider->GetAuthInfoAsync();

    ASSERT_TRUE(future.IsReady());
    EXPECT_THROW(future.GetValue(), std::exception);
}

TEST(IamCredentialsProvider, AuthInfoFailsWhenPeriodicTaskIsRejected) {
    auto facility = std::make_shared<TFailingCoreFacility>();
    auto provider = MakeOAuthFactory().CreateProvider(std::weak_ptr<ICoreFacility>(facility));
    auto future = provider->GetAuthInfoAsync();

    ASSERT_TRUE(future.IsReady());
    EXPECT_THROW(future.GetValue(), std::exception);
}

TEST(IamCredentialsProvider, AuthInfoRetriesTransientIamFailure) {
    TFlakyIamServiceStub stub;
    TIamGrpcServer server(&stub);
    ASSERT_TRUE(server.Start());

    TIamOAuth params;
    params.Endpoint = server.Endpoint();
    params.EnableSsl = false;
    params.OAuthToken = "token";
    params.RequestTimeout = TDuration::Seconds(2);

    auto provider = TTestOAuthFactory(params).CreateProvider();
    auto future = provider->GetAuthInfoAsync();
    ASSERT_TRUE(future.Wait(TDuration::Seconds(10)));

    EXPECT_EQ(future.GetValue(), "oauth-token");
    EXPECT_GE(stub.Attempts(), 2u);

    server.Stop();
}

// Regression test for the deprecated no-arg CreateProvider() on the IAM service-account
// factory with a nested gRPC JWT auth provider. Before the fix, both providers shared a single
// TSimpleCoreFacility, each registered a periodic refresh task, and TSimpleCoreFacility's
// single-task invariant tripped Y_ABORT_UNLESS and killed the process. The fix gives the
// nested auth provider its own facility (via a recursive no-arg CreateProvider()), so the two
// periodic tasks land on separate facilities.
TEST(IamServiceCredentialsProvider, NoArgCreateProviderWithGrpcInnerCreds) {
    TIamServiceStub stub;
    TIamGrpcServer server(&stub);
    ASSERT_TRUE(server.Start());

    auto jwtParams = MakeJwtParams(server.Endpoint());
    auto jwtFactory = std::make_shared<TIamJwtCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(jwtParams);

    TIamServiceParams serviceParams;
    serviceParams.Endpoint = server.Endpoint();
    serviceParams.EnableSsl = false;
    serviceParams.RefreshPeriod = TDuration::Hours(1);
    serviceParams.RequestTimeout = TDuration::Seconds(5);
    serviceParams.ServiceId = "unit-test-service";
    serviceParams.MicroserviceId = "unit-test-microservice";
    serviceParams.ResourceId = "unit-test-resource";
    serviceParams.ResourceType = "unit-test-resource-type";
    serviceParams.TargetServiceAccountId = "unit-test-target";
    serviceParams.SystemServiceAccountCredentials = jwtFactory;

    auto serviceFactory = CreateIamServiceCredentialsProviderFactory(serviceParams);

    auto work = [&serviceFactory]() -> std::string {
        auto provider = serviceFactory->CreateProvider();
        return provider->GetAuthInfo();
    };

    std::future<std::string> done = std::async(std::launch::async, work);
    ASSERT_EQ(done.wait_for(std::chrono::seconds(20)), std::future_status::ready)
        << "no-arg CreateProvider() with nested gRPC JWT auth must not abort and must produce a token";
    EXPECT_EQ(done.get(), "outer-service-token");

    server.Stop();
}

// Regression: MakeRequestFiller must capture TIamServiceParams by value so a temporary factory
// can be destroyed while the provider still handles periodic refresh ticks.
TEST(IamServiceCredentialsProvider, NoArgCreateProviderTemporaryFactory) {
    TIamServiceStub stub;
    TIamGrpcServer server(&stub);
    ASSERT_TRUE(server.Start());

    auto jwtParams = MakeJwtParams(server.Endpoint());
    auto jwtFactory = std::make_shared<TIamJwtCredentialsProviderFactory<
        CreateIamTokenRequest, CreateIamTokenResponse, IamTokenService>>(jwtParams);

    TIamServiceParams serviceParams;
    serviceParams.Endpoint = server.Endpoint();
    serviceParams.EnableSsl = false;
    serviceParams.RefreshPeriod = TDuration::MilliSeconds(100);
    serviceParams.RequestTimeout = TDuration::Seconds(5);
    serviceParams.ServiceId = "unit-test-service";
    serviceParams.MicroserviceId = "unit-test-microservice";
    serviceParams.ResourceId = "unit-test-resource";
    serviceParams.ResourceType = "unit-test-resource-type";
    serviceParams.TargetServiceAccountId = "unit-test-target";
    serviceParams.SystemServiceAccountCredentials = jwtFactory;

    auto provider = CreateIamServiceCredentialsProviderFactory(serviceParams)->CreateProvider();
    EXPECT_EQ(provider->GetAuthInfo(), "outer-service-token");

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    EXPECT_EQ(provider->GetAuthInfo(), "outer-service-token");

    server.Stop();
}
