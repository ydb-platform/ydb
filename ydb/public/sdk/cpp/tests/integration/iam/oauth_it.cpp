#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_grpc_mock_server.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/iam_test_fixture.h>

#include <ydb/public/sdk/cpp/src/client/types/core_facility/simple_core_facility.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NTest;

namespace {

constexpr const char* kOAuthToken = "unit-test-oauth-token";

class TOAuthIamFixture : public ::testing::Test {
protected:
    void SetUp() override {
        Stub_.SetResponseToken(kMockRootBuiltinToken);
        ASSERT_TRUE(Server_.Start());
    }

    TIamTokenServiceStub Stub_;
    TIamGrpcServer Server_{&Stub_};
};

} // namespace

TEST_F(TOAuthIamFixture, OAuth_NoArgCreateProvider) {
    auto factory = CreateIamOAuthCredentialsProviderFactory(
        MakeOAuthParams(Server_.Endpoint(), kOAuthToken));

    auto provider = factory->CreateProvider();
    EXPECT_EQ(provider->GetAuthInfo(), kMockRootBuiltinToken);
    ASSERT_TRUE(Stub_.HasLastRequest());
    EXPECT_EQ(Stub_.GetLastRequest().yandex_passport_oauth_token(), kOAuthToken);
}

TEST_F(TOAuthIamFixture, OAuth_DriverUsesMockIamToken) {
    auto factory = CreateIamOAuthCredentialsProviderFactory(
        MakeOAuthParams(Server_.Endpoint(), kOAuthToken));

    TDriver driver(MakeDriverConfig(factory));
    RunSelect1ExpectSuccess(driver);

    EXPECT_GE(Stub_.GetRequestCount(), 1);
    ASSERT_TRUE(Stub_.HasLastRequest());
    EXPECT_EQ(Stub_.GetLastRequest().yandex_passport_oauth_token(), kOAuthToken);
}

TEST_F(TOAuthIamFixture, OAuth_RefreshUsesCachedToken) {
    auto factory = CreateIamOAuthCredentialsProviderFactory(
        MakeOAuthParams(Server_.Endpoint(), kOAuthToken));

    auto provider = factory->CreateProvider();
    EXPECT_EQ(provider->GetAuthInfo(), kMockRootBuiltinToken);

    const int countBefore = Stub_.GetRequestCount();
    EXPECT_EQ(provider->GetAuthInfo(), kMockRootBuiltinToken);
    EXPECT_EQ(Stub_.GetRequestCount(), countBefore);
}

TEST(OAuth_WithFacility, ReturnsTokenFromMock) {
    TIamTokenServiceStub stub;
    stub.SetResponseToken(kMockRootBuiltinToken);
    TIamGrpcServer server(&stub);
    ASSERT_TRUE(server.Start());

    auto factory = CreateIamOAuthCredentialsProviderFactory(
        MakeOAuthParams(server.Endpoint(), kOAuthToken));
    auto facility = std::make_shared<TSimpleCoreFacility>();
    auto provider = factory->CreateProvider(facility);

    EXPECT_EQ(provider->GetAuthInfo(), kMockRootBuiltinToken);
    ASSERT_TRUE(stub.HasLastRequest());
    EXPECT_EQ(stub.GetLastRequest().yandex_passport_oauth_token(), kOAuthToken);
}
