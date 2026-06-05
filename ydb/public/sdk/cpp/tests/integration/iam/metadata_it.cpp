#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/helpers/iam_http_mock_server.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/iam_test_fixture.h>

#include <ydb/public/sdk/cpp/src/client/types/core_facility/simple_core_facility.h>

#include <library/cpp/testing/gtest/gtest.h>

using namespace NYdb;
using namespace NYdb::NTest;

namespace {

constexpr const char* kMockMetadataToken = "root@builtin";

class TMetadataFixture : public ::testing::Test {
protected:
    void SetUp() override {
        Server_.SetResponse(HTTP_OK, MakeTokenResponse(kMockMetadataToken, 3600));
    }

    TMetadataServer Server_;
};

} // namespace

TEST_F(TMetadataFixture, Metadata_NoArgCreateProvider) {
    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(Server_.Port));
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), kMockMetadataToken);
    EXPECT_GE(Server_.GetRequestCount(), 1);
}

TEST_F(TMetadataFixture, Metadata_ViaFacilityDefault) {
    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(Server_.Port));
    auto facility = std::make_shared<TSimpleCoreFacility>();
    auto provider = factory->CreateProvider(facility);

    EXPECT_EQ(provider->GetAuthInfo(), kMockMetadataToken);
    EXPECT_GE(Server_.GetRequestCount(), 1);
}

TEST_F(TMetadataFixture, Metadata_DriverPath) {
    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(Server_.Port));
    TDriver driver(MakeDriverConfig(factory));
    RunSelect1(driver);

    EXPECT_GE(Server_.GetRequestCount(), 1);
}

TEST_F(TMetadataFixture, Metadata_PreflightThenDriver) {
    auto factory = CreateIamCredentialsProviderFactory(MakeMetadataParams(Server_.Port));

    // Mirrors CreateFromEnvironment metadata preflight in helpers.cpp.
    auto preflightProvider = factory->CreateProvider();
    EXPECT_EQ(preflightProvider->GetAuthInfo(), kMockMetadataToken);

    const int countAfterPreflight = Server_.GetRequestCount();
    TDriver driver(MakeDriverConfig(factory));
    RunSelect1(driver);

    EXPECT_GT(Server_.GetRequestCount(), countAfterPreflight);
}
