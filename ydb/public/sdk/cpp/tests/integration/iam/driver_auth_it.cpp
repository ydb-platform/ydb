// Auth-enforcement negative control only; positive driver wiring lives in oauth/jwt/metadata *_it.cpp.
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_http_assertions.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_http_mock_server.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/iam_test_fixture.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NTest;

namespace {

constexpr const char* kInvalidToken = "invalid-e2e-token";

} // namespace

TEST(DriverAuth, InvalidStaticTokenRejected) {
    AssertAuthFailure(
        CreateOAuthCredentialsProviderFactory(kInvalidToken),
        "invalid static token");
}

TEST(DriverAuth, MetadataInvalidTokenRejected) {
    TMetadataServer server;
    server.SetResponse(HTTP_OK, MakeTokenResponse(kInvalidToken, 3600));

    AssertAuthFailure(
        CreateIamCredentialsProviderFactory(MakeMetadataParams(server.Port)),
        "invalid metadata token");

    EXPECT_GE(server.GetRequestCount(), 1);
    AssertMetadataRequestShape(server);
}
