#include <ydb/public/lib/ydb_cli/common/oidc_token_cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>
#include <util/system/sysstat.h>

#include <atomic>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

using namespace NYdb;
using namespace NYdb::NOidc;
using namespace NYdb::NConsoleClient;

namespace {

std::string WriteFile(const TFsPath& path, const std::string& contents);
TTokenCache MakeCache(std::string access, std::string refresh);

std::string WriteFile(const TFsPath& path, const std::string& contents) {
    TFileOutput(path.GetPath()).Write(contents);
    return path.GetPath();
}

TTokenCache MakeCache(std::string access, std::string refresh) {
    TTokenCache result{
        .AccessToken = {
            .Token = std::move(access),
            .ExpiresAt = TInstant::Seconds(2'000'000'000),
        },
    };
    if (!refresh.empty()) {
        result.RefreshToken = TOAuthToken{
            .Token = std::move(refresh),
            .ExpiresAt = TInstant::Seconds(2'100'000'000),
        };
    }
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(TOidcFileTokenCache) {
    Y_UNIT_TEST(ReusesTokensAfterRestart) {
        TTempDir dir;
        const auto path = (dir.Path() / "tokens.json").GetPath();
        CreateFileTokenCacher(path, "identity-a")->Write(MakeCache("access-a", "refresh-a"));

        const auto cached = CreateFileTokenCacher(path, "identity-a")->Read();
        UNIT_ASSERT(cached.has_value());
        UNIT_ASSERT_VALUES_EQUAL(cached->AccessToken.Token, "access-a");
        UNIT_ASSERT(cached->AccessToken.ExpiresAt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*cached->AccessToken.ExpiresAt, TInstant::Seconds(2'000'000'000));
        UNIT_ASSERT(cached->RefreshToken.has_value());
        UNIT_ASSERT_VALUES_EQUAL(cached->RefreshToken->Token, "refresh-a");
        UNIT_ASSERT(cached->RefreshToken->ExpiresAt.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*cached->RefreshToken->ExpiresAt, TInstant::Seconds(2'100'000'000));
    }

    Y_UNIT_TEST(MissingCorruptOversizedAndUnsupportedCachesAreMisses) {
        TTempDir dir;
        const auto path = dir.Path() / "tokens.json";
        auto cacher = CreateFileTokenCacher(path.GetPath(), "identity-a");
        UNIT_ASSERT(!cacher->Read().has_value());

        WriteFile(path, "{broken json");
        UNIT_ASSERT(!cacher->Read().has_value());
        WriteFile(path, R"({"version":99,"identity":"identity-a"})");
        UNIT_ASSERT(!cacher->Read().has_value());
        WriteFile(path, std::string(1024 * 1024 + 1, 'x'));
        UNIT_ASSERT(!cacher->Read().has_value());
    }

    Y_UNIT_TEST(ExpiryOverflowIsACacheMiss) {
        TTempDir dir;
        const auto path = dir.Path() / "tokens.json";
        WriteFile(path, R"({"version":1,"identity":"identity-a","access_token":{"token":"access","expires_at":18446744073710}})");
        UNIT_ASSERT(!CreateFileTokenCacher(path.GetPath(), "identity-a")->Read().has_value());
    }

    Y_UNIT_TEST(ReadIoErrorIsNotReportedAsMiss) {
        TTempDir dir;
        auto cacher = CreateFileTokenCacher(dir.Path().GetPath(), "identity-a");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "regular file");
    }

#if defined(_unix_)
    Y_UNIT_TEST(RejectsFifoWithoutBlocking) {
        TTempDir dir;
        const auto path = (dir.Path() / "tokens.pipe").GetPath();
        UNIT_ASSERT_VALUES_EQUAL(mkfifo(path.c_str(), S_IRUSR | S_IWUSR), 0);

        auto cacher = CreateFileTokenCacher(path, "identity-a");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "regular file");
    }
#endif

    Y_UNIT_TEST(RejectsCacheLargerThanReadLimitOnWrite) {
        TTempDir dir;
        auto cacher = CreateFileTokenCacher((dir.Path() / "tokens.json").GetPath(), "identity-a");
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            cacher->Write(MakeCache(std::string(1024 * 1024 + 1, 'x'), {})),
            std::runtime_error,
            "maximum size");
    }

    Y_UNIT_TEST(IdentityMismatchIsAMissAndRejectsOverwrite) {
        TTempDir dir;
        const auto path = (dir.Path() / "tokens.json").GetPath();
        CreateFileTokenCacher(path, "identity-a")->Write(MakeCache("access-a", {}));

        auto other = CreateFileTokenCacher(path, "identity-b");
        UNIT_ASSERT(!other->Read().has_value());
        UNIT_ASSERT_EXCEPTION_CONTAINS(other->Write(MakeCache("access-b", {})), std::runtime_error, "identity");
        UNIT_ASSERT_VALUES_EQUAL(CreateFileTokenCacher(path, "identity-a")->Read()->AccessToken.Token, "access-a");
    }

    Y_UNIT_TEST(AtomicReplacementNeverExposesPartialJson) {
        TTempDir dir;
        const auto path = (dir.Path() / "tokens.json").GetPath();
        auto writer = CreateFileTokenCacher(path, "identity-a");
        auto reader = CreateFileTokenCacher(path, "identity-a");
        writer->Write(MakeCache("access-a", {}));

        std::atomic<bool> stop = false;
        std::atomic<bool> invalid = false;
        std::thread reading([&] {
            while (!stop.load()) {
                const auto value = reader->Read();
                if (!value.has_value() || (value->AccessToken.Token != "access-a" && value->AccessToken.Token != "access-b")) {
                    invalid = true;
                    break;
                }
            }
        });
        for (size_t i = 0; i < 100; ++i) {
            writer->Write(MakeCache(i % 2 ? "access-a" : "access-b", {}));
        }
        stop = true;
        reading.join();
        UNIT_ASSERT(!invalid.load());
    }

    Y_UNIT_TEST(CreatesOwnerOnlyCacheFile) {
        TTempDir dir;
        const auto path = (dir.Path() / "tokens.json").GetPath();
        CreateFileTokenCacher(path, "identity-a")->Write(MakeCache("access", {}));

#if defined(_unix_)
        const TFileStat stat(path);
        UNIT_ASSERT_VALUES_EQUAL(stat.Mode & (S_IRWXU | S_IRWXG | S_IRWXO), S_IRUSR | S_IWUSR);
#endif
    }

#if defined(_unix_)
    Y_UNIT_TEST(RestrictsPermissionsWhenReplacingCache) {
        TTempDir dir;
        const auto path = dir.Path() / "tokens.json";
        auto cacher = CreateFileTokenCacher(path.GetPath(), "identity-a");
        cacher->Write(MakeCache("old-access", {}));
        UNIT_ASSERT_VALUES_EQUAL(Chmod(path.GetPath().c_str(), S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH), 0);

        cacher->Write(MakeCache("new-access", {}));

        const TFileStat stat(path);
        UNIT_ASSERT_VALUES_EQUAL(stat.Mode & (S_IRWXU | S_IRWXG | S_IRWXO), S_IRUSR | S_IWUSR);
        UNIT_ASSERT_VALUES_EQUAL(cacher->Read()->AccessToken.Token, "new-access");
    }

    Y_UNIT_TEST(RejectsDanglingCacheSymlink) {
        TTempDir dir;
        const auto target = dir.Path() / "missing.json";
        const auto link = dir.Path() / "tokens.json";
        UNIT_ASSERT(NFs::SymLink(target.GetPath(), link.GetPath()));

        auto cacher = CreateFileTokenCacher(link.GetPath(), "identity-a");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "symlink");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Write(MakeCache("access", {})), std::runtime_error, "symlink");
        UNIT_ASSERT(!target.Exists());
        UNIT_ASSERT(link.IsSymlink());
    }

    Y_UNIT_TEST(RejectsCacheSymlink) {
        TTempDir dir;
        const auto target = dir.Path() / "target.json";
        const auto link = dir.Path() / "tokens.json";
        WriteFile(target, "unchanged");
        UNIT_ASSERT(NFs::SymLink(target.GetPath(), link.GetPath()));

        auto cacher = CreateFileTokenCacher(link.GetPath(), "identity-a");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "symlink");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Write(MakeCache("access", {})), std::runtime_error, "symlink");
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(target.GetPath()).ReadAll(), "unchanged");
    }
#endif
} // Y_UNIT_TEST_SUITE(TOidcFileTokenCache)
