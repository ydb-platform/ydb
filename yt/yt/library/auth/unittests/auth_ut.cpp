#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/library/auth/auth.h>

#include <yt/yt/core/misc/finally.h>

#include <util/folder/tempdir.h>

#include <util/stream/file.h>

#include <util/system/env.h>
#include <util/system/fs.h>
#include <util/system/tempfile.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

TEST(Auth, ValidateToken)
{
    EXPECT_NO_THROW(ValidateToken("ABACABA-3289-ABCDEF"));
    EXPECT_NO_THROW(ValidateToken("\x21-ohh-\x7e"));
    EXPECT_NO_THROW(ValidateToken(""));

    EXPECT_THROW_THAT(ValidateToken("\x20"), ::testing::HasSubstr("Incorrect token character"));
    EXPECT_THROW_THAT(ValidateToken("ABACABA-\x20"), ::testing::HasSubstr("Incorrect token character"));
    EXPECT_THROW_THAT(ValidateToken("ABACABA-\x7f"), ::testing::HasSubstr("Incorrect token character"));
}

TEST(Auth, LoadTokenFromFile)
{
    TTempFileHandle tempFile;
    {
        TOFStream os(tempFile);
        os << "Some-happy-token";
    }
    auto token = LoadTokenFromFile(tempFile.Name());
    EXPECT_EQ(token, "Some-happy-token");

    EXPECT_EQ(LoadTokenFromFile("/a/b/c/I_HOPE_YOU_DONT_EXIST"), std::nullopt);

    {
        TOFStream os(tempFile);
        os << "Unhappy-\x20-token";
    }
    EXPECT_THROW_THAT(LoadTokenFromFile(tempFile.Name()), ::testing::HasSubstr("Incorrect token character"));
}

TEST(Auth, LoadToken)
{
    auto oldYtToken = GetEnv("YT_TOKEN");
    auto oldYtSecureVaultToken = GetEnv("YT_SECURE_VAULT_YT_TOKEN");
    auto oldYtTokenPath = GetEnv("YT_TOKEN_PATH");
    auto oldHome = GetEnv("HOME");

    auto guard = Finally([&] {
        SetEnv("YT_TOKEN", oldYtToken);
        SetEnv("YT_SECURE_VAULT_YT_TOKEN", oldYtSecureVaultToken);
        SetEnv("YT_TOKEN_PATH", oldYtTokenPath);
        SetEnv("HOME", oldHome);
    });

    SetEnv("YT_TOKEN", "Token-from-YT_TOKEN");
    SetEnv("YT_SECURE_VAULT_YT_TOKEN", "Token-from-YT_SECURE_VAULT_YT_TOKEN");

    TTempFileHandle tokenPath;
    {
        TOFStream os(tokenPath);
        os << "Token-from-YT_TOKEN_PATH";
    }
    SetEnv("YT_TOKEN_PATH", tokenPath.Name());

    TTempDir home;
    auto dotYt = home.Path().Child(".yt");
    ASSERT_TRUE(NFs::MakeDirectory(dotYt.GetPath()));
    TTempFile dotYtToken(dotYt.Child("token"));
    {
        TOFStream os(dotYtToken.Name());
        os << "Token-from-HOME";
    }
    SetEnv("HOME", home.Path());

    EXPECT_EQ(LoadToken(), "Token-from-YT_TOKEN");
    SetEnv("YT_TOKEN", "");
    EXPECT_EQ(LoadToken(), "Token-from-YT_SECURE_VAULT_YT_TOKEN");
    SetEnv("YT_SECURE_VAULT_YT_TOKEN", "");
    EXPECT_EQ(LoadToken(), "Token-from-YT_TOKEN_PATH");
    SetEnv("YT_TOKEN_PATH", "");
    EXPECT_EQ(LoadToken(), "Token-from-HOME");

    TTempDir emptyHome;
    SetEnv("HOME", emptyHome.Path());
    EXPECT_EQ(LoadToken(), std::nullopt);

    SetEnv("YT_TOKEN", "Invalid-token-\x01");
    EXPECT_THROW_THAT(LoadToken(), ::testing::HasSubstr("Incorrect token character"));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
