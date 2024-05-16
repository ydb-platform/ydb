#include <yt/yt/client/api/options.h>

#include <library/cpp/testing/common/scope.h>
#include <library/cpp/testing/gtest/gtest.h>

#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>

#include <util/stream/file.h>

#include <util/system/env.h>

namespace NYT::NClient::NCache {

////////////////////////////////////////////////////////////////////////////////

TEST(TClientOptionsTest, TokenFromFile)
{
    TTempDir tmpDir;
    MakeDirIfNotExist(tmpDir.Name() + "/.yt");
    {
        TFileOutput token(tmpDir.Name() + "/.yt/token");
        token.Write(" AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA \n");
    }
    NTesting::TScopedEnvironment envGuard{{
        {"HOME", tmpDir.Name()},
        {"YT_TOKEN", ""},
        {"YT_TOKEN_PATH", ""},
    }};
    const auto clientOptions = NApi::GetClientOpsFromEnv();
    EXPECT_TRUE(clientOptions.Token);
    EXPECT_EQ("AAAA-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", *clientOptions.Token);
}

TEST(TClientOptionsTest, TokenFromYtTokenPath)
{
    TTempDir tmpDir;
    const TString tokenPath = tmpDir.Name() + "/token";
    {
        TFileOutput token(tokenPath);
        token.Write(" BBBB-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA \n");
    }
    NTesting::TScopedEnvironment envGuard{{
        {"YT_TOKEN", ""},
        {"YT_TOKEN_PATH", tokenPath},
    }};
    const auto clientOptions = NApi::GetClientOpsFromEnv();
    EXPECT_TRUE(clientOptions.Token);
    EXPECT_EQ("BBBB-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", *clientOptions.Token);
}

TEST(TClientOptionsTest, TokenFromEnv)
{
    NTesting::TScopedEnvironment tokenGuard("YT_TOKEN", "BBBB-BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB");
    const auto& clientOptions = NApi::GetClientOpsFromEnv();
    EXPECT_TRUE(clientOptions.Token);
    EXPECT_EQ("BBBB-BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB", *clientOptions.Token);
}

TEST(TClientOptionsTest, UserFromEnv)
{
    NTesting::TScopedEnvironment envGuard{{
        {"YT_TOKEN", "BBBB-BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
        {"YT_USER", "yt_test_user"},
    }};
    const auto& clientOptions = NApi::GetClientOpsFromEnv();
    EXPECT_TRUE(clientOptions.User);
    EXPECT_EQ("yt_test_user", *clientOptions.User);
}

TEST(TClientOptionsTest, AllowEmptyUser)
{
    NTesting::TScopedEnvironment envGuard{{
        {"YT_TOKEN", "BBBB-BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB"},
        {"YT_USER", ""},
    }};
    const auto& clientOptions = NApi::GetClientOpsFromEnv();
    EXPECT_TRUE(!clientOptions.User);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NCache
