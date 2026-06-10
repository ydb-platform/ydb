#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_http_assertions.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_http_mock_server.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/iam_test_fixture.h>

#include <ydb/public/sdk/cpp/src/client/types/core_facility/simple_core_facility.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NTest;

namespace {

class TMetadataFixture : public ::testing::Test {
protected:
    void SetUp() override {
        Server_.SetResponse(HTTP_OK, MakeTokenResponse(kMockRootBuiltinToken, 3600));
    }

    TMetadataServer Server_;
};

} // namespace

TEST_F(TMetadataFixture, Metadata_NoArgCreateProvider) {
    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(Server_.Port));
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), kMockRootBuiltinToken);
    EXPECT_GE(Server_.GetRequestCount(), 1);
    AssertMetadataRequestShape(Server_);
}

TEST_F(TMetadataFixture, Metadata_ViaFacilityDefault) {
    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(Server_.Port));
    auto facility = std::make_shared<TSimpleCoreFacility>();
    auto provider = factory->CreateProvider(facility);

    EXPECT_EQ(provider->GetAuthInfo(), kMockRootBuiltinToken);
    EXPECT_GE(Server_.GetRequestCount(), 1);
    AssertMetadataRequestShape(Server_);
}

TEST_F(TMetadataFixture, Metadata_DriverUsesMockIamToken) {
    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(Server_.Port));
    TDriver driver(MakeDriverConfig(factory));
    RunSelect1ExpectSuccess(driver);

    EXPECT_GE(Server_.GetRequestCount(), 1);
    AssertMetadataRequestShape(Server_);
}

TEST_F(TMetadataFixture, Metadata_PreflightThenDriver) {
    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(Server_.Port));

    // Mirrors CreateFromEnvironment metadata preflight in helpers.cpp.
    auto preflightProvider = factory->CreateProvider();
    EXPECT_EQ(preflightProvider->GetAuthInfo(), kMockRootBuiltinToken);

    const int countAfterPreflight = Server_.GetRequestCount();
    AssertMetadataRequestShape(Server_);

    TDriver driver(MakeDriverConfig(factory));
    RunSelect1ExpectSuccess(driver);

    EXPECT_GT(Server_.GetRequestCount(), countAfterPreflight);
    AssertMetadataRequestShape(Server_);
}
