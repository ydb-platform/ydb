#include <ydb/public/lib/ydb_cli/common/oidc.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/fs.h>

namespace NYdb::NConsoleClient {

    Y_UNIT_TEST_SUITE(TOidcCliTest) {
        Y_UNIT_TEST(FileCacheRoundTripPreservesExpiry) {
            TTempDir tempDir;
            const TString cachePath = tempDir.Path() / "tokens.json";
            auto cache = CreateOidcFileTokenCache(cachePath);

            const TInstant accessExpiry = TInstant::ParseIso8601("2200-01-01T00:00:00Z");
            const TInstant refreshExpiry = TInstant::ParseIso8601("2200-01-02T00:00:00Z");
            TOidcTokenSet tokens;
            tokens.AccessToken = TOidcToken{.Token = "access", .ExpiresAt = accessExpiry};
            tokens.RefreshToken = TOidcToken{.Token = "refresh", .ExpiresAt = refreshExpiry};
            cache->Write(tokens);

            const auto restored = cache->Read();
            UNIT_ASSERT(restored.has_value());
            UNIT_ASSERT_VALUES_EQUAL(restored->AccessToken.Token, "access");
            UNIT_ASSERT_VALUES_EQUAL(restored->AccessToken.ExpiresAt.value(), accessExpiry);
            UNIT_ASSERT_VALUES_EQUAL(restored->RefreshToken->Token, "refresh");
            UNIT_ASSERT_VALUES_EQUAL(restored->RefreshToken->ExpiresAt.value(), refreshExpiry);
#ifndef _win32_
            UNIT_ASSERT_VALUES_EQUAL(TFileStat(cachePath).Mode & (S_IRWXU | S_IRWXG | S_IRWXO), S_IRUSR | S_IWUSR);
#endif
        }

        Y_UNIT_TEST(RelativeCachePathResolvedFromConfigDirectory) {
            TTempDir tempDir;
            const TString configPath = tempDir.Path() / "oidc.yaml";
            TUnbufferedFileOutput(configPath).Write(R"yaml(
issuer: https://idp.example
cache_path: cache/tokens.json
static_credentials:
  access_token: access
)yaml");

            TOidcConfig config = ReadOidcConfig(configPath);
            UNIT_ASSERT(config.TokenCache_ != nullptr);
            config.TokenCache_->Write(TOidcTokenSet{.AccessToken = TOidcToken{.Token = "cached"}});
            UNIT_ASSERT(TFsPath(tempDir.Path() / "cache/tokens.json").Exists());
        }

#ifndef _win32_
        Y_UNIT_TEST(InsecureCachePermissionsRejected) {
            TTempDir tempDir;
            const TString cachePath = tempDir.Path() / "tokens.json";
            TUnbufferedFileOutput(cachePath).Write(R"json({"version":1,"access_token":"access"})json");
            UNIT_ASSERT_VALUES_EQUAL(Chmod(cachePath.c_str(), S_IRUSR | S_IWUSR | S_IRGRP), 0);

            auto cache = CreateOidcFileTokenCache(cachePath);
            UNIT_ASSERT_EXCEPTION(cache->Read(), yexception);
        }
#endif
    } // Y_UNIT_TEST_SUITE(TOidcCliTest)

} // namespace NYdb::NConsoleClient
