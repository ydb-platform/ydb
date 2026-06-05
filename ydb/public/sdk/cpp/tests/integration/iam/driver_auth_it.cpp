#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_grpc_mock_server.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_http_assertions.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_http_mock_server.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/iam_test_fixture.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NTest;

namespace {

constexpr const char* kInvalidToken = "invalid-e2e-token";
constexpr const char* kMockIamToken = "root@builtin";
constexpr const char* kOAuthToken = "unit-test-oauth-token";

void AssertAuthFailureWithInvalidToken(TCredentialsProviderFactoryPtr factory) {
    TDriver driver(MakeDriverConfig(std::move(factory)));
    const auto status = RunSelect1Status(driver);
    ASSERT_FALSE(status.IsSuccess())
        << "Expected auth failure with invalid token, but query succeeded. "
        << "Recipe must set YDB_ENFORCE_USER_TOKEN_REQUIREMENT=true.";
    EXPECT_TRUE(IsAuthError(status)) << status.GetIssues().ToString();
}

} // namespace

TEST(DriverAuth, InvalidStaticTokenRejected) {
    AssertAuthFailureWithInvalidToken(CreateOAuthCredentialsProviderFactory(kInvalidToken));
}

TEST(DriverAuth, MockOAuthTokenAccepted) {
    TIamTokenServiceStub stub;
    stub.SetResponseToken(kMockIamToken);
    TIamGrpcServer server(&stub);
    ASSERT_TRUE(server.Start());

    auto factory = CreateIamOAuthCredentialsProviderFactory(
        MakeOAuthParams(server.Endpoint(), kOAuthToken));
    TDriver driver(MakeDriverConfig(factory));
    RunSelect1ExpectSuccess(driver);

    EXPECT_GE(stub.GetRequestCount(), 1);
    ASSERT_TRUE(stub.HasLastRequest());
    EXPECT_EQ(stub.GetLastRequest().yandex_passport_oauth_token(), kOAuthToken);
}

TEST(DriverAuth, MetadataInvalidTokenRejected) {
    TMetadataServer server;
    server.SetResponse(HTTP_OK, MakeTokenResponse(kInvalidToken, 3600));

    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(server.Port));
    TDriver driver(MakeDriverConfig(std::move(factory)));
    const auto status = RunSelect1Status(driver);
    ASSERT_FALSE(status.IsSuccess())
        << "Expected auth failure with invalid metadata token, but query succeeded.";
    EXPECT_TRUE(IsAuthError(status)) << status.GetIssues().ToString();

    EXPECT_GE(server.GetRequestCount(), 1);
    AssertMetadataRequestShape(server);
}

TEST(DriverAuth, MockMetadataTokenAccepted) {
    TMetadataServer server;
    server.SetResponse(HTTP_OK, MakeTokenResponse(kMockIamToken, 3600));

    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(server.Port));
    TDriver driver(MakeDriverConfig(factory));
    RunSelect1ExpectSuccess(driver);

    EXPECT_GE(server.GetRequestCount(), 1);
    AssertMetadataRequestShape(server);
}
