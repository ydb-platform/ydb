#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/oidc/token_cache.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/yexception.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/flock.h>
#include <util/system/fs.h>
#include <util/system/fstat.h>
#include <util/system/platform.h>
#include <util/system/sysstat.h>
#include <util/system/tempfile.h>

#include <cerrno>
#include <cstdint>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(_win_)
    #include <aclapi.h>
    #include <windows.h>
#elif defined(_unix_)
    #include <fcntl.h>
    #include <unistd.h>
#endif

namespace NYdb::inline Dev {
namespace {

constexpr ui64 CacheVersion = 1;
constexpr i64 MaxCacheSize = 1024 * 1024;
constexpr ui64 MaxJsonDepth = 8;

[[noreturn]] void ThrowCacheError(const std::string& path, const std::string& problem) {
    throw std::runtime_error("OIDC token cache '" + path + "': " + problem);
}

void RejectSymlink(const TFsPath& path) {
    if (path.IsSymlink()) {
        ThrowCacheError(path.GetPath(), "refusing to access a symlink");
    }
}

bool IsNotFoundError(int error) {
#if defined(_win_)
    return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND;
#else
    return error == ENOENT;
#endif
}

bool IsSymlinkOpenError(int error) {
#if defined(_unix_)
    return error == ELOOP;
#else
    return false;
#endif
}

#if defined(_win_)
struct TCloseHandle {
    void operator()(void* value) const noexcept {
        if (value) {
            CloseHandle(static_cast<HANDLE>(value));
        }
    }
};

struct TLocalFree {
    void operator()(void* value) const noexcept {
        if (value) {
            LocalFree(value);
        }
    }
};

class TOwnerOnlySecurity {
public:
    explicit TOwnerOnlySecurity(const std::string& path) {
        HANDLE processToken = nullptr;
        if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &processToken)) {
            ThrowCacheError(path, "failed to determine file owner");
        }
        ProcessToken_.reset(processToken);

        DWORD tokenInfoSize = 0;
        GetTokenInformation(processToken, TokenUser, nullptr, 0, &tokenInfoSize);
        if (!tokenInfoSize || GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
            ThrowCacheError(path, "failed to determine file owner");
        }
        TokenInfo_.resize(tokenInfoSize);
        if (!GetTokenInformation(processToken, TokenUser, TokenInfo_.data(), tokenInfoSize, &tokenInfoSize)) {
            ThrowCacheError(path, "failed to determine file owner");
        }
        const auto* tokenUser = reinterpret_cast<const TOKEN_USER*>(TokenInfo_.data());

        EXPLICIT_ACCESSA access{};
        access.grfAccessPermissions = FILE_ALL_ACCESS;
        access.grfAccessMode = SET_ACCESS;
        access.grfInheritance = NO_INHERITANCE;
        access.Trustee.TrusteeForm = TRUSTEE_IS_SID;
        access.Trustee.TrusteeType = TRUSTEE_IS_USER;
        access.Trustee.ptstrName = static_cast<LPSTR>(tokenUser->User.Sid);

        PACL acl = nullptr;
        const DWORD aclError = SetEntriesInAclA(1, &access, nullptr, &acl);
        Acl_.reset(acl);
        if (aclError != ERROR_SUCCESS) {
            ThrowCacheError(path, "failed to create owner-only permissions");
        }
        if (!InitializeSecurityDescriptor(&Descriptor_, SECURITY_DESCRIPTOR_REVISION) ||
            !SetSecurityDescriptorDacl(&Descriptor_, true, acl, false) ||
            !SetSecurityDescriptorControl(&Descriptor_, SE_DACL_PROTECTED, SE_DACL_PROTECTED))
        {
            ThrowCacheError(path, "failed to create owner-only security descriptor");
        }
        Attributes_.nLength = sizeof(Attributes_);
        Attributes_.lpSecurityDescriptor = &Descriptor_;
        Attributes_.bInheritHandle = false;
    }

    SECURITY_ATTRIBUTES* GetAttributes() {
        return &Attributes_;
    }

    PACL GetAcl() const {
        return static_cast<PACL>(Acl_.get());
    }

private:
    std::unique_ptr<void, TCloseHandle> ProcessToken_;
    std::vector<unsigned char> TokenInfo_;
    std::unique_ptr<void, TLocalFree> Acl_;
    SECURITY_DESCRIPTOR Descriptor_{};
    SECURITY_ATTRIBUTES Attributes_{};
};

std::filesystem::path ToWindowsPath(const std::string& path) {
    return std::filesystem::path(
        std::u8string_view(reinterpret_cast<const char8_t*>(path.data()), path.size()));
}
#endif

void ValidateRegularHandle(const TFileHandle& file, const std::string& path) {
    const TFileStat stat(file);
    if (stat.IsSymlink()) {
        ThrowCacheError(path, "refusing to access a symlink");
    }
    if (!stat.IsFile()) {
        ThrowCacheError(path, "path is not a regular file");
    }
}

TFileHandle OpenReadOnlyNoFollow(const std::string& path) {
#if defined(_win_)
    const auto windowsPath = ToWindowsPath(path);
    TFileHandle result(CreateFileW(
        windowsPath.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        nullptr,
        OPEN_EXISTING,
        FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN,
        nullptr));
#elif defined(_unix_)
    int handle;
    do {
        handle = open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    } while (handle < 0 && errno == EINTR);
    TFileHandle result(handle);
#else
    TFileHandle result;
#endif
    if (result.IsOpen()) {
        ValidateRegularHandle(result, path);
    }
    return result;
}

TFileHandle CreateOwnerOnlyFile(const std::string& path) {
#if defined(_win_)
    TOwnerOnlySecurity security(path);
    const auto windowsPath = ToWindowsPath(path);
    TFileHandle result(CreateFileW(
        windowsPath.c_str(),
        GENERIC_WRITE | WRITE_DAC,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        security.GetAttributes(),
        CREATE_NEW,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
        nullptr));
#elif defined(_unix_)
    int handle;
    do {
        handle = open(path.c_str(), O_CREAT | O_EXCL | O_WRONLY | O_CLOEXEC | O_NOFOLLOW, S_IRUSR | S_IWUSR);
    } while (handle < 0 && errno == EINTR);
    TFileHandle result(handle);
#else
    TFileHandle result;
#endif
    if (result.IsOpen()) {
        ValidateRegularHandle(result, path);
    }
    return result;
}

void ProtectOwnerOnly(FHANDLE handle, const std::string& path) {
#if defined(_win_)
    TOwnerOnlySecurity security(path);
    const DWORD securityError = SetSecurityInfo(
        static_cast<HANDLE>(handle),
        SE_FILE_OBJECT,
        DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
        nullptr,
        nullptr,
        security.GetAcl(),
        nullptr);
    if (securityError != ERROR_SUCCESS) {
        ThrowCacheError(path, "failed to set owner-only permissions");
    }
#elif defined(_unix_)
    if (fchmod(handle, S_IRUSR | S_IWUSR) != 0) {
        ThrowCacheError(path, "failed to set owner-only permissions");
    }
#else
    ThrowCacheError(path, "owner-only permissions are unsupported on this platform");
#endif
}

TFileHandle OpenLockFile(const std::string& path) {
#if defined(_win_)
    TOwnerOnlySecurity security(path);
    const auto windowsPath = ToWindowsPath(path);
    TFileHandle result(CreateFileW(
        windowsPath.c_str(),
        GENERIC_READ | GENERIC_WRITE | WRITE_DAC,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        security.GetAttributes(),
        OPEN_ALWAYS,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
        nullptr));
#elif defined(_unix_)
    int handle;
    do {
        handle = open(path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, S_IRUSR | S_IWUSR);
    } while (handle < 0 && errno == EINTR);
    TFileHandle result(handle);
#else
    TFileHandle result;
#endif
    if (result.IsOpen()) {
        ValidateRegularHandle(result, path);
        ProtectOwnerOnly(result, path);
    }
    return result;
}

std::optional<std::string> ReadCacheFile(const TFsPath& path) {
    ClearLastSystemError();
    TFileHandle handle = OpenReadOnlyNoFollow(path.GetPath());
    if (!handle.IsOpen()) {
        const int error = LastSystemError();
        if (IsNotFoundError(error)) {
            return std::nullopt;
        }
        if (IsSymlinkOpenError(error)) {
            ThrowCacheError(path.GetPath(), "refusing to access a symlink");
        }
        ThrowCacheError(path.GetPath(), "failed to open file");
    }

    try {
        TFile file(handle.Release(), path.GetPath());
        const i64 size = file.GetLength();
        if (size < 0) {
            ThrowCacheError(path.GetPath(), "failed to determine file size");
        }
        if (size > MaxCacheSize) {
            return std::string{};
        }
        std::string data(static_cast<size_t>(size), '\0');
        if (size) {
            file.Load(data.data(), data.size());
        }
        return data;
    } catch (const std::runtime_error&) {
        throw;
    } catch (const std::exception&) {
        ThrowCacheError(path.GetPath(), "failed to read file");
    }
}

std::optional<ui64> ReadUnsigned(const NJson::TJsonValue& value) {
    if (value.IsUInteger()) {
        return value.GetUIntegerSafe();
    }
    if (value.IsInteger()) {
        const auto integer = value.GetIntegerSafe();
        if (integer >= 0) {
            return static_cast<ui64>(integer);
        }
    }
    return std::nullopt;
}

std::optional<TOAuthToken> ParseToken(const NJson::TJsonValue& value) {
    if (!value.IsMap()) {
        return std::nullopt;
    }
    const auto& map = value.GetMapSafe();
    const auto* token = map.FindPtr("token");
    if (!token || !token->IsString() || token->GetStringSafe().empty()) {
        return std::nullopt;
    }

    TOAuthToken result{.Token = std::string(token->GetStringSafe())};
    if (const auto* expiry = map.FindPtr("expires_at")) {
        const auto seconds = ReadUnsigned(*expiry);
        if (!seconds || *seconds > TInstant::Max().Seconds()) {
            return std::nullopt;
        }
        result.ExpiresAt = TInstant::Seconds(*seconds);
    }
    return result;
}

struct TCacheDocument {
    std::string Identity;
    TTokenCache Cache;
};

std::optional<TCacheDocument> ParseDocument(const std::string& data) {
    try {
        NJson::TJsonValue root;
        NJson::TJsonReaderConfig config;
        config.MaxDepth = MaxJsonDepth;
        if (!NJson::ReadJsonTree(data, &config, &root) || !root.IsMap()) {
            return std::nullopt;
        }
        const auto& map = root.GetMapSafe();
        const auto* version = map.FindPtr("version");
        const auto parsedVersion = version ? ReadUnsigned(*version) : std::nullopt;
        const auto* identity = map.FindPtr("identity");
        const auto* access = map.FindPtr("access_token");
        if (!parsedVersion || *parsedVersion != CacheVersion || !identity || !identity->IsString() || !access) {
            return std::nullopt;
        }
        const auto accessToken = ParseToken(*access);
        if (!accessToken) {
            return std::nullopt;
        }

        TCacheDocument result{
            .Identity = std::string(identity->GetStringSafe()),
            .Cache = {.AccessToken = *accessToken},
        };
        if (const auto* refresh = map.FindPtr("refresh_token")) {
            result.Cache.RefreshToken = ParseToken(*refresh);
            if (!result.Cache.RefreshToken) {
                return std::nullopt;
            }
        }
        return result;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

NJson::TJsonValue SerializeToken(const TOAuthToken& token) {
    NJson::TJsonValue result(NJson::JSON_MAP);
    result.InsertValue("token", token.Token);
    if (token.ExpiresAt) {
        result.InsertValue("expires_at", static_cast<unsigned long long>(token.ExpiresAt->Seconds()));
    }
    return result;
}

std::string SerializeDocument(const std::string& identity, const TTokenCache& cache) {
    NJson::TJsonValue root(NJson::JSON_MAP);
    root.InsertValue("version", static_cast<unsigned long long>(CacheVersion));
    root.InsertValue("identity", identity);
    root.InsertValue("access_token", SerializeToken(cache.AccessToken));
    if (cache.RefreshToken) {
        root.InsertValue("refresh_token", SerializeToken(*cache.RefreshToken));
    }
    return std::string(NJson::WriteJson(root, false, true));
}

class TFileTokenCacheLock final: public ITokenCacheLock {
public:
    explicit TFileTokenCacheLock(TFileHandle file)
        : File_(std::move(file))
    {
    }

private:
    TFileHandle File_;
};

class TFileTokenCacher final: public ITokenCacher, public ILockingTokenCacher {
public:
    TFileTokenCacher(std::string path, std::string identity)
        : Path_(std::move(path))
        , Identity_(std::move(identity))
    {
    }

    std::optional<TTokenCache> Read() const override {
        const auto data = ReadCacheFile(Path_);
        if (!data) {
            return std::nullopt;
        }
        const auto document = ParseDocument(*data);
        if (!document || document->Identity != Identity_) {
            return std::nullopt;
        }
        return document->Cache;
    }

    void Write(const TTokenCache& cache) override {
        if (cache.AccessToken.Token.empty()) {
            throw std::invalid_argument("OIDC token cache access token must not be empty");
        }
        if (cache.RefreshToken && cache.RefreshToken->Token.empty()) {
            throw std::invalid_argument("OIDC token cache refresh token must not be empty");
        }

        const auto current = ReadCacheFile(Path_);
        if (current) {
            const auto document = ParseDocument(*current);
            if (document && document->Identity != Identity_) {
                ThrowCacheError(Path_.GetPath(), "identity differs; use a separate cache path");
            }
        }

        const auto parent = Path_.Parent();
        if (!parent.IsDirectory()) {
            ThrowCacheError(Path_.GetPath(), "parent directory does not exist");
        }
        RejectSymlink(Path_);

        const TFsPath temporary = parent / (".oidc-token-cache-" + CreateGuidAsString());
        TTempFile cleanup(temporary.GetPath());
        try {
            const auto data = SerializeDocument(Identity_, cache);
            if (data.size() > static_cast<size_t>(MaxCacheSize)) {
                ThrowCacheError(Path_.GetPath(), "serialized data exceeds maximum size");
            }
            TFileHandle handle = CreateOwnerOnlyFile(temporary.GetPath());
            if (!handle.IsOpen()) {
                ThrowCacheError(Path_.GetPath(), "failed to create temporary file");
            }
            TFile file(handle.Release(), temporary.GetPath());
            file.Write(data.data(), data.size());
            file.Flush();
            file.Close();
            RejectSymlink(Path_);
            if (!NFs::Rename(temporary.GetPath(), Path_.GetPath())) {
                ThrowCacheError(Path_.GetPath(), "failed to atomically replace file");
            }
        } catch (const std::runtime_error&) {
            throw;
        } catch (const std::exception&) {
            ThrowCacheError(Path_.GetPath(), "failed to write file");
        }
    }

    std::unique_ptr<ITokenCacheLock> TryLock() override {
        const TFsPath lockPath(Path_.GetPath() + ".lock");
        TFileHandle file = OpenLockFile(lockPath.GetPath());
        if (!file.IsOpen()) {
            ThrowCacheError(lockPath.GetPath(), "failed to open lock file");
        }
        if (file.Flock(LOCK_EX | LOCK_NB) != 0) {
            const int error = errno;
            if (error == EWOULDBLOCK || error == EAGAIN) {
                return nullptr;
            }
            ThrowCacheError(lockPath.GetPath(), "failed to acquire lock");
        }
        return std::make_unique<TFileTokenCacheLock>(std::move(file));
    }

private:
    TFsPath Path_;
    std::string Identity_;
};

} // namespace

std::shared_ptr<ITokenCacher> CreateFileTokenCacher(
    const std::string& cacheFilePath,
    const std::string& identity)
{
    if (cacheFilePath.empty()) {
        throw std::invalid_argument("OIDC token cache path must not be empty");
    }
    if (identity.empty()) {
        throw std::invalid_argument("OIDC token cache identity must not be empty");
    }
    return std::make_shared<TFileTokenCacher>(cacheFilePath, identity);
}

} // namespace NYdb::inline Dev
