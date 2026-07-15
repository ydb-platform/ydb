#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/common/generic_provider.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam_private/iam.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_grpc_mock_server.h>

#include <ydb/public/api/client/yc_private/iam/iam_token_service.grpc.pb.h>
#include <ydb/public/api/client/yc_private/iam/iam_token_service.pb.h>

#include <gtest/gtest.h>

#include <chrono>
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
        response->set_iam_token("inner-jwt-token");
        response->mutable_expires_at()->set_seconds(4102444800);
        return grpc::Status::OK;
    }

    grpc::Status CreateForService(
        grpc::ServerContext*,
        const CreateIamTokenForServiceRequest*,
        CreateIamTokenResponse* response) override
    {
        response->set_iam_token("outer-service-token");
        response->mutable_expires_at()->set_seconds(4102444800);
        return grpc::Status::OK;
    }
};

} // namespace

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
