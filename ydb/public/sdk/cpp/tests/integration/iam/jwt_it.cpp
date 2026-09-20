#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/iam/iam.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_grpc_mock_server.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_jwt_assertions.h>
#include <ydb/public/sdk/cpp/tests/common/iam_mocks/iam_test_keys.h>
#include <ydb/public/sdk/cpp/tests/integration/iam/iam_test_fixture.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>

using namespace NYdb;
using namespace NYdb::NTest;

namespace {

class TJwtIamFixture : public ::testing::Test {
protected:
    void SetUp() override {
        Stub_.SetResponseToken(kMockRootBuiltinToken);
        ASSERT_TRUE(Server_.Start());
    }

    TIamJwtContent MakeJwtContentParams() const {
        TIamJwtContent params;
        params.Endpoint = Server_.Endpoint();
        params.EnableSsl = false;
        params.RefreshPeriod = TDuration::Hours(1);
        params.RequestTimeout = TDuration::Seconds(5);
        params.JwtContent = MakeJwtKeyFileContent();
        return params;
    }

    TIamJwtFilename MakeJwtFileParams(const TString& keyPath) const {
        TIamJwtFilename params;
        params.Endpoint = Server_.Endpoint();
        params.EnableSsl = false;
        params.RefreshPeriod = TDuration::Hours(1);
        params.RequestTimeout = TDuration::Seconds(5);
        params.JwtFilename = keyPath;
        return params;
    }

    TIamTokenServiceStub Stub_;
    TIamGrpcServer Server_{&Stub_};
};

TString WriteJwtKeyToTempFile(TTempDir& tempDir) {
    const TString keyPath = tempDir.Path() / "sa-key.json";
    TFileOutput out(keyPath);
    out.Write(MakeJwtKeyFileContent());
    return keyPath;
}

} // namespace

TEST_F(TJwtIamFixture, JwtParams_NoArgCreateProvider) {
    auto factory = CreateIamJwtParamsCredentialsProviderFactory(MakeJwtContentParams());
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), kMockRootBuiltinToken);
    ASSERT_TRUE(Stub_.HasLastRequest());
    AssertIamJwt(Stub_.GetLastRequest().jwt());
}

TEST_F(TJwtIamFixture, JwtParams_DriverUsesMockIamToken) {
    auto factory = CreateIamJwtParamsCredentialsProviderFactory(MakeJwtContentParams());
    TDriver driver(MakeDriverConfig(factory));
    RunSelect1ExpectSuccess(driver);

    EXPECT_GE(Stub_.GetRequestCount(), 1);
    ASSERT_TRUE(Stub_.HasLastRequest());
    AssertIamJwt(Stub_.GetLastRequest().jwt());
}

TEST_F(TJwtIamFixture, JwtFile_NoArgCreateProvider) {
    TTempDir tempDir;
    const TString keyPath = WriteJwtKeyToTempFile(tempDir);

    auto factory = CreateIamJwtFileCredentialsProviderFactory(MakeJwtFileParams(keyPath));
    auto provider = factory->CreateProvider();

    EXPECT_EQ(provider->GetAuthInfo(), kMockRootBuiltinToken);
    ASSERT_TRUE(Stub_.HasLastRequest());
    AssertIamJwt(Stub_.GetLastRequest().jwt());
}

TEST_F(TJwtIamFixture, JwtFile_DriverUsesMockIamToken) {
    TTempDir tempDir;
    const TString keyPath = WriteJwtKeyToTempFile(tempDir);

    auto factory = CreateIamJwtFileCredentialsProviderFactory(MakeJwtFileParams(keyPath));
    TDriver driver(MakeDriverConfig(factory));
    RunSelect1ExpectSuccess(driver);

    EXPECT_GE(Stub_.GetRequestCount(), 1);
    ASSERT_TRUE(Stub_.HasLastRequest());
    AssertIamJwt(Stub_.GetLastRequest().jwt());
}
