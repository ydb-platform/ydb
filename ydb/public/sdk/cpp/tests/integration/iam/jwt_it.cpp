#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/helpers/iam_grpc_mock_server.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/iam_test_fixture.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/helpers/iam_test_keys.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

using namespace NYdb;
using namespace NYdb::NTest;

namespace {

constexpr const char* kMockIamToken = "root@builtin";

class TJwtIamFixture : public ::testing::Test {
protected:
    void SetUp() override {
        Stub_.SetResponseToken(kMockIamToken);
        ASSERT_TRUE(Server_.Start());
    }

    TIamTokenServiceStub Stub_;
    TIamGrpcServer Server_{&Stub_};
};

} // namespace

TEST_F(TJwtIamFixture, JwtParams_NoArgCreateProvider) {
    TIamJwtContent params;
    params.Endpoint = Server_.Endpoint();
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    params.RequestTimeout = TDuration::Seconds(5);
    params.JwtContent = MakeJwtKeyFileContent();

    auto factory = CreateIamJwtParamsCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), kMockIamToken);
    ASSERT_TRUE(Stub_.HasLastRequest());
    EXPECT_FALSE(Stub_.GetLastRequest().jwt().empty());
}

TEST_F(TJwtIamFixture, JwtParams_DriverFacilityPath) {
    TIamJwtContent params;
    params.Endpoint = Server_.Endpoint();
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    params.RequestTimeout = TDuration::Seconds(5);
    params.JwtContent = MakeJwtKeyFileContent();

    auto factory = CreateIamJwtParamsCredentialsProviderFactory(params);
    TDriver driver(MakeDriverConfig(factory));
    RunSelect1(driver);

    EXPECT_EQ(Stub_.GetRequestCount(), 1);
    ASSERT_TRUE(Stub_.HasLastRequest());
    EXPECT_FALSE(Stub_.GetLastRequest().jwt().empty());
}

TEST_F(TJwtIamFixture, JwtFile_NoArgCreateProvider) {
    TTempDir tempDir;
    const TString keyPath = tempDir.Path() / "sa-key.json";
    {
        TFileOutput out(keyPath);
        out.Write(MakeJwtKeyFileContent());
    }

    TIamJwtFilename params;
    params.Endpoint = Server_.Endpoint();
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    params.RequestTimeout = TDuration::Seconds(5);
    params.JwtFilename = keyPath;

    auto factory = CreateIamJwtFileCredentialsProviderFactory(params);
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), kMockIamToken);
    ASSERT_TRUE(Stub_.HasLastRequest());
    EXPECT_FALSE(Stub_.GetLastRequest().jwt().empty());
}

TEST_F(TJwtIamFixture, JwtFile_DriverFacilityPath) {
    TTempDir tempDir;
    const TString keyPath = tempDir.Path() / "sa-key.json";
    {
        TFileOutput out(keyPath);
        out.Write(MakeJwtKeyFileContent());
    }

    TIamJwtFilename params;
    params.Endpoint = Server_.Endpoint();
    params.EnableSsl = false;
    params.RefreshPeriod = TDuration::Hours(1);
    params.RequestTimeout = TDuration::Seconds(5);
    params.JwtFilename = keyPath;

    auto factory = CreateIamJwtFileCredentialsProviderFactory(params);
    TDriver driver(MakeDriverConfig(factory));
    RunSelect1(driver);

    EXPECT_EQ(Stub_.GetRequestCount(), 1);
    ASSERT_TRUE(Stub_.HasLastRequest());
    EXPECT_FALSE(Stub_.GetLastRequest().jwt().empty());
}
