#include <ydb/public/lib/ydb_cli/common/oidc_token_cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/scope.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/system/file.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>
#include <util/system/sysstat.h>

#include <atomic>
#include <exception>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#if defined(_unix_)
#include <unistd.h>
#endif

using namespace NYdb;
using namespace NYdb::NOidc;
using namespace NYdb::NConsoleClient;

namespace {

std::string WriteFile(const TFsPath& path, const std::string& contents);
TTokenCache MakeCache(std::string access, std::string refresh);

std::string WriteFile(const TFsPath& path, const std::string& contents) {
    if (!path.Exists()) {
        CreateFileTokenCacher(path.GetPath(), "fixture")->Write(MakeCache("fixture", ""));
    }
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

    Y_UNIT_TEST(RejectsEmptyCachePathAndIdentity) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateFileTokenCacher("", "identity"), std::invalid_argument, "path");
        UNIT_ASSERT_EXCEPTION_CONTAINS(CreateFileTokenCacher("tokens.json", ""), std::invalid_argument, "identity");
    }

    Y_UNIT_TEST(MalformedDocumentsAreSilentCacheMisses) {
        TTempDir dir;
        const auto path = dir.Path() / "tokens.json";
        std::vector<std::string> diagnostics;
        const auto cacher = CreateFileTokenCacher(path.GetPath(), "identity",
            [&](const std::string& message) { diagnostics.push_back(message); });
        for (const std::string& document : {
                "", "[]", "null", "{}",
                R"({"version":-1,"identity":"identity","access_token":{"token":"access"}})",
                R"({"version":"1","identity":"identity","access_token":{"token":"access"}})",
                R"({"version":1,"access_token":{"token":"access"}})",
                R"({"version":1,"identity":42,"access_token":{"token":"access"}})",
                R"({"version":1,"identity":"identity"})",
                R"({"version":1,"identity":"identity","access_token":null})",
                R"({"version":1,"identity":"identity","access_token":{}})",
                R"({"version":1,"identity":"identity","access_token":{"token":1}})",
                R"({"version":1,"identity":"identity","access_token":{"token":""}})",
                R"({"version":1,"identity":"identity","access_token":{"token":"access","expires_at":-1}})",
                R"({"version":1,"identity":"identity","access_token":{"token":"access","expires_at":true}})",
                R"({"version":1,"identity":"identity","access_token":{"token":"access"},"refresh_token":{}})",
                R"({"version":1,"identity":"identity","access_token":{"token":"access"},"refresh_token":{"token":"refresh","expires_at":"tomorrow"}})",
                R"({"nested":[[[[[[[[[[]]]]]]]]]]})"})
        {
            WriteFile(path, document);
            UNIT_ASSERT_C(!cacher->Read().has_value(), document);
            UNIT_ASSERT(diagnostics.empty());
        }
        // A corrupt cache must not prevent storing a fresh token.
        cacher->Write(MakeCache("fresh-access", "fresh-refresh"));
        UNIT_ASSERT_VALUES_EQUAL(cacher->Read()->AccessToken.Token, "fresh-access");
    }

    Y_UNIT_TEST(RoundTripsTokensWithoutExpiry) {
        TTempDir dir;
        const auto path = (dir.Path() / "tokens.json").GetPath();
        const TTokenCache cache{
            .AccessToken = {.Token = "access"},
            .RefreshToken = TOAuthToken{.Token = "refresh"},
        };
        CreateFileTokenCacher(path, "identity")->Write(cache);
        const auto restored = CreateFileTokenCacher(path, "identity")->Read();
        UNIT_ASSERT(restored.has_value());
        UNIT_ASSERT(!restored->AccessToken.ExpiresAt.has_value());
        UNIT_ASSERT(restored->RefreshToken.has_value());
        UNIT_ASSERT_VALUES_EQUAL(restored->RefreshToken->Token, "refresh");
        UNIT_ASSERT(!restored->RefreshToken->ExpiresAt.has_value());
    }

    Y_UNIT_TEST(InvalidTokensDoNotReplaceExistingCache) {
        TTempDir dir;
        const auto path = dir.Path() / "tokens.json";
        std::vector<std::string> diagnostics;
        const auto cacher = CreateFileTokenCacher(path.GetPath(), "identity",
            [&](const std::string& message) { diagnostics.push_back(message); });
        cacher->Write(MakeCache("original", "refresh"));
        const TString original = TFileInput(path).ReadAll();
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Write(MakeCache("", "secret-refresh")),
            std::invalid_argument, "access token must not be empty");
        auto invalid = MakeCache("secret-access", "refresh");
        invalid.RefreshToken->Token.clear();
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Write(invalid),
            std::invalid_argument, "refresh token must not be empty");
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), original);
        UNIT_ASSERT_VALUES_EQUAL(diagnostics.size(), 2);
        for (const auto& message : diagnostics) {
            UNIT_ASSERT(message.find("secret-") == std::string::npos);
        }
    }

    Y_UNIT_TEST(DiagnosticFailurePreservesOriginalError) {
        TTempDir dir;
        const auto cacher = CreateFileTokenCacher(dir.Path().GetPath(), "identity",
            [](const std::string&) { throw std::runtime_error("diagnostic failed"); });
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "regular file");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Write(MakeCache("access", "")), std::runtime_error, "regular file");
    }

    Y_UNIT_TEST(NonDirectoryParentIsAnIoError) {
        TTempDir dir;
        const auto parent = dir.Path() / "file";
        WriteFile(parent, "unchanged");
        const auto cacher = CreateFileTokenCacher((parent / "tokens.json").GetPath(), "identity");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "failed to open file");
        UNIT_ASSERT_VALUES_EQUAL(TFileInput(parent).ReadAll(), "unchanged");
    }

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
    // These tests require POSIX permissions and named FIFOs. On Windows,
    // Chmod only changes the read-only attribute; it cannot deny file reads.
    Y_UNIT_TEST(PermissionErrorsPreserveCacheAndCleanTemporaryFiles) {
        // Root bypasses Unix mode bits, so it cannot exercise EACCES this way.
        if (geteuid() == 0) {
            return;
        }
        TTempDir dir;
        const auto path = dir.Path() / "tokens.json";
        const auto cacher = CreateFileTokenCacher(path.GetPath(), "identity");
        cacher->Write(MakeCache("original", "refresh"));
        Y_DEFER {
            Chmod(dir.Name().c_str(), S_IRWXU);
            Chmod(path.GetPath().c_str(), S_IRUSR | S_IWUSR);
        };
        UNIT_ASSERT_VALUES_EQUAL(Chmod(path.GetPath().c_str(), 0), 0);
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "permission denied");
        UNIT_ASSERT_VALUES_EQUAL(Chmod(path.GetPath().c_str(), S_IRUSR | S_IWUSR), 0);
        UNIT_ASSERT_VALUES_EQUAL(Chmod(dir.Name().c_str(), S_IRUSR | S_IXUSR), 0);
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Write(MakeCache("new", "refresh")),
            std::runtime_error, "failed to write file");
        UNIT_ASSERT_VALUES_EQUAL(cacher->Read()->AccessToken.Token, "original");
        TVector<TString> files;
        dir.Path().ListNames(files);
        UNIT_ASSERT_VALUES_EQUAL(files.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(files.front(), "tokens.json");
    }

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

    Y_UNIT_TEST(ReportsWriteFailureBeforeRethrowingWithoutTokens) {
        TTempDir dir;
        std::vector<std::string> diagnostics;
        auto cacher = CreateFileTokenCacher(
            (dir.Path() / "missing" / "tokens.json").GetPath(), "identity-a",
            [&](const std::string& message) { diagnostics.push_back(message); });
        UNIT_ASSERT(!cacher->Read().has_value());
        UNIT_ASSERT(diagnostics.empty());

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            cacher->Write(MakeCache("secret-access", "secret-refresh")),
            std::runtime_error, "parent directory");
        UNIT_ASSERT_VALUES_EQUAL(diagnostics.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(diagnostics.front(), "parent directory");
        UNIT_ASSERT(diagnostics.front().find("secret-access") == std::string::npos);
        UNIT_ASSERT(diagnostics.front().find("secret-refresh") == std::string::npos);
    }

    Y_UNIT_TEST(ReportsReadFailureBeforeRethrowing) {
        TTempDir dir;
        std::vector<std::string> diagnostics;
        auto cacher = CreateFileTokenCacher(
            dir.Path().GetPath(), "identity-a",
            [&](const std::string& message) { diagnostics.push_back(message); });

        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "regular file");
        UNIT_ASSERT_VALUES_EQUAL(diagnostics.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(diagnostics.front(), "regular file");
    }

    Y_UNIT_TEST(ReportsIdentityCollisionWithoutIdentityOrTokens) {
        TTempDir dir;
        const auto path = (dir.Path() / "tokens.json").GetPath();
        CreateFileTokenCacher(path, "private-identity-a")->Write(MakeCache("secret-access-a", "secret-refresh-a"));
        std::vector<std::string> diagnostics;
        auto cacher = CreateFileTokenCacher(
            path, "private-identity-b",
            [&](const std::string& message) { diagnostics.push_back(message); });

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            cacher->Write(MakeCache("secret-access-b", "secret-refresh-b")), std::runtime_error, "identity");
        UNIT_ASSERT_VALUES_EQUAL(diagnostics.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(diagnostics.front(), "separate cache path");
        UNIT_ASSERT(diagnostics.front().find("private-identity") == std::string::npos);
        UNIT_ASSERT(diagnostics.front().find("secret-access") == std::string::npos);
        UNIT_ASSERT(diagnostics.front().find("secret-refresh") == std::string::npos);
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
    Y_UNIT_TEST(RejectsCacheAccessibleToOtherAccounts) {
        TTempDir dir;
        const auto path = dir.Path() / "tokens.json";
        auto cacher = CreateFileTokenCacher(path.GetPath(), "identity-a");
        cacher->Write(MakeCache("old-access", {}));
        const auto original = TFileInput(path).ReadAll();
        for (const auto extraMode : {S_IRGRP, S_IWGRP, S_IXGRP, S_IROTH, S_IWOTH, S_IXOTH}) {
            UNIT_ASSERT_VALUES_EQUAL(Chmod(path.GetPath().c_str(), S_IRUSR | S_IWUSR | extraMode), 0);
            UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "permissions");
            UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Write(MakeCache("new-access", {})), std::runtime_error, "permissions");
            UNIT_ASSERT_VALUES_EQUAL(TFileInput(path).ReadAll(), original);
        }
        UNIT_ASSERT_VALUES_EQUAL(Chmod(path.GetPath().c_str(), S_IRUSR | S_IWUSR), 0);
        UNIT_ASSERT_VALUES_EQUAL(cacher->Read()->AccessToken.Token, "old-access");
        cacher->Write(MakeCache("new-access", {}));
        UNIT_ASSERT_VALUES_EQUAL(cacher->Read()->AccessToken.Token, "new-access");
    }

    Y_UNIT_TEST(RejectsCacheOwnedByAnotherAccount) {
        // Only root can create a file owned by a different account.
        if (geteuid() != 0) {
            return;
        }
        TTempDir dir;
        const auto path = dir.Path() / "tokens.json";
        auto cacher = CreateFileTokenCacher(path.GetPath(), "identity-a");
        cacher->Write(MakeCache("foreign-access", {}));
        UNIT_ASSERT_VALUES_EQUAL(chown(path.GetPath().c_str(), 1, -1), 0);
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Read(), std::runtime_error, "owner");
        UNIT_ASSERT_EXCEPTION_CONTAINS(cacher->Write(MakeCache("new-access", {})), std::runtime_error, "owner");
    }

    Y_UNIT_TEST(ConcurrentReplacementNeverFollowsSymlink) {
        TTempDir dir;
        const auto path = dir.Path() / "tokens.json";
        const auto target = dir.Path() / "target.json";
        const auto replacement = dir.Path() / "replacement";
        const auto temporary = dir.Path() / "swapping";
        CreateFileTokenCacher(path.GetPath(), "identity-a")->Write(MakeCache("regular-access", {}));
        CreateFileTokenCacher(target.GetPath(), "identity-a")->Write(MakeCache("symlink-access", {}));
        UNIT_ASSERT(NFs::SymLink(target.GetPath(), replacement.GetPath()));
        auto cacher = CreateFileTokenCacher(path.GetPath(), "identity-a", [](const std::string&) {});

        std::exception_ptr replacementError;
        std::jthread replacing([&](std::stop_token stop) {
            try {
                while (!stop.stop_requested()) {
                    path.RenameTo(temporary);
                    replacement.RenameTo(path);
                    temporary.RenameTo(replacement);
                }
            } catch (...) {
                replacementError = std::current_exception();
            }
        });
        for (size_t i = 0; i < 10000; ++i) {
            std::optional<TTokenCache> cached;
            try {
                cached = cacher->Read();
            } catch (const std::runtime_error& error) {
                UNIT_ASSERT_STRING_CONTAINS(error.what(), "symlink");
            }
            if (cached.has_value()) {
                UNIT_ASSERT_VALUES_EQUAL(cached->AccessToken.Token, "regular-access");
            }
        }
        replacing.request_stop();
        replacing.join();
        if (replacementError != nullptr) {
            std::rethrow_exception(replacementError);
        }
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
