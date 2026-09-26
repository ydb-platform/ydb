#include "oidc_token_cache.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/mutex.h>
#include <util/system/fstat.h>
#include <util/system/platform.h>
#include <util/system/tempfile.h>

#include <cerrno>
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
#endif

namespace NYdb::NConsoleClient {
namespace {

constexpr ui64 CacheVersion = 1;
constexpr i64 MaxCacheSize = 1024 * 1024;
constexpr ui64 MaxJsonDepth = 8;

struct TCacheDocument {
    std::string Identity;
    NOidc::TTokenCache Cache;
};

[[noreturn]] void ThrowCacheError(const std::string& path, const std::string& problem);
void RejectSymlink(const TFsPath& path);
bool IsNotFoundError(int error);
void ValidateRegularFile(const TFileStat& stat, const std::string& path);
TFile CreateOwnerOnlyFile(const TFsPath& path);
std::optional<std::string> ReadCacheFile(const TFsPath& path);
std::optional<ui64> ReadUnsigned(const NJson::TJsonValue& value);
std::optional<NOidc::TOAuthToken> ParseToken(const NJson::TJsonValue& value);
std::optional<TCacheDocument> ParseDocument(const std::string& data);
NJson::TJsonValue SerializeToken(const NOidc::TOAuthToken& token);
std::string SerializeDocument(const std::string& identity, const NOidc::TTokenCache& cache);

#if defined(_win_)
std::filesystem::path ToWindowsPath(const std::string& path);
#endif

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

#if defined(_win_)
struct TLocalFree {
    void operator()(void* value) const noexcept;
};

class TOwnerOnlySecurity {
public:
    explicit TOwnerOnlySecurity(const std::string& path);
    SECURITY_ATTRIBUTES* GetAttributes();

private:
    TFileHandle ProcessToken_;
    std::vector<unsigned char> TokenInfo_;
    std::unique_ptr<void, TLocalFree> Acl_;
    SECURITY_DESCRIPTOR Descriptor_{};
    SECURITY_ATTRIBUTES Attributes_{};
};

void TLocalFree::operator()(void* value) const noexcept {
    if (value != nullptr) {
        LocalFree(value);
    }
}

TOwnerOnlySecurity::TOwnerOnlySecurity(const std::string& path) {
    HANDLE processToken = nullptr;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &processToken)) {
        ThrowCacheError(path, "failed to determine file owner");
    }
    ProcessToken_ = TFileHandle(processToken);

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

SECURITY_ATTRIBUTES* TOwnerOnlySecurity::GetAttributes() {
    return &Attributes_;
}

std::filesystem::path ToWindowsPath(const std::string& path) {
    return std::filesystem::path(
        std::u8string_view(reinterpret_cast<const char8_t*>(path.data()), path.size()));
}
#endif

void ValidateRegularFile(const TFileStat& stat, const std::string& path) {
    if (stat.IsSymlink()) {
        ThrowCacheError(path, "refusing to access a symlink");
    }
    if (!stat.IsFile()) {
        ThrowCacheError(path, "path is not a regular file");
    }
}

TFile CreateOwnerOnlyFile(const TFsPath& path) {
#if defined(_win_)
    // TFile's ARUser/AWUser flags do not restrict the Windows DACL.
    TOwnerOnlySecurity security(path.GetPath());
    const auto windowsPath = ToWindowsPath(path.GetPath());
    TFileHandle handle(CreateFileW(
        windowsPath.c_str(),
        GENERIC_WRITE | WRITE_DAC,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        security.GetAttributes(),
        CREATE_NEW,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT,
        nullptr));
    if (!handle.IsOpen()) {
        ThrowCacheError(path.GetPath(), "failed to create temporary file");
    }
    return TFile(handle.Release(), path.GetPath());
#else
    return TFile(path.GetPath(), CreateNew | WrOnly | CloseOnExec | ARUser | AWUser);
#endif
}

std::optional<std::string> ReadCacheFile(const TFsPath& path) {
    ClearLastSystemError();
    const TFileStat stat(path, true);
    if (stat.IsNull()) {
        const int error = LastSystemError();
        if (IsNotFoundError(error)) {
            return std::nullopt;
        }
        ThrowCacheError(path.GetPath(), "failed to inspect file");
    }
    // A single process owns the cache. Check before opening to reject FIFOs
    // without blocking; concurrent replacement is limited to regular files.
    ValidateRegularFile(stat, path.GetPath());

    try {
        TFile file(path.GetPath(), OpenExisting | RdOnly | CloseOnExec | Seq);
        ValidateRegularFile(TFileStat(file), path.GetPath());
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

std::optional<NOidc::TOAuthToken> ParseToken(const NJson::TJsonValue& value) {
    if (!value.IsMap()) {
        return std::nullopt;
    }
    const auto& map = value.GetMapSafe();
    const auto* token = map.FindPtr("token");
    if (token == nullptr || !token->IsString() || token->GetStringSafe().empty()) {
        return std::nullopt;
    }

    NOidc::TOAuthToken result{.Token = std::string(token->GetStringSafe())};
    if (const auto* expiry = map.FindPtr("expires_at"); expiry != nullptr) {
        const auto seconds = ReadUnsigned(*expiry);
        if (!seconds.has_value() || *seconds > TInstant::Max().Seconds()) {
            return std::nullopt;
        }
        result.ExpiresAt = TInstant::Seconds(*seconds);
    }
    return result;
}

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
        const auto parsedVersion = version != nullptr ? ReadUnsigned(*version) : std::nullopt;
        const auto* identity = map.FindPtr("identity");
        const auto* access = map.FindPtr("access_token");
        if (!parsedVersion.has_value() || *parsedVersion != CacheVersion || identity == nullptr || !identity->IsString() || access == nullptr) {
            return std::nullopt;
        }
        const auto accessToken = ParseToken(*access);
        if (!accessToken.has_value()) {
            return std::nullopt;
        }

        TCacheDocument result{
            .Identity = std::string(identity->GetStringSafe()),
            .Cache = {.AccessToken = *accessToken},
        };
        if (const auto* refresh = map.FindPtr("refresh_token"); refresh != nullptr) {
            result.Cache.RefreshToken = ParseToken(*refresh);
            if (!result.Cache.RefreshToken.has_value()) {
                return std::nullopt;
            }
        }
        return result;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

NJson::TJsonValue SerializeToken(const NOidc::TOAuthToken& token) {
    NJson::TJsonValue result(NJson::JSON_MAP);
    result.InsertValue("token", token.Token);
    if (token.ExpiresAt.has_value()) {
        result.InsertValue("expires_at", static_cast<unsigned long long>(token.ExpiresAt->Seconds()));
    }
    return result;
}

std::string SerializeDocument(const std::string& identity, const NOidc::TTokenCache& cache) {
    NJson::TJsonValue root(NJson::JSON_MAP);
    root.InsertValue("version", static_cast<unsigned long long>(CacheVersion));
    root.InsertValue("identity", identity);
    root.InsertValue("access_token", SerializeToken(cache.AccessToken));
    if (cache.RefreshToken.has_value()) {
        root.InsertValue("refresh_token", SerializeToken(*cache.RefreshToken));
    }
    return std::string(NJson::WriteJson(root, false, true));
}

class TFileTokenCacher final: public NOidc::ITokenCacher {
public:
    TFileTokenCacher(std::string path, std::string identity);

    std::optional<NOidc::TTokenCache> Read() const override;
    void Write(const NOidc::TTokenCache& cache) override;

private:
    TFsPath Path_;
    std::string Identity_;
    mutable TMutex Mutex_;
};

TFileTokenCacher::TFileTokenCacher(std::string path, std::string identity)
    : Path_(std::move(path))
    , Identity_(std::move(identity))
{
}

std::optional<NOidc::TTokenCache> TFileTokenCacher::Read() const {
    with_lock (Mutex_) {
        const auto data = ReadCacheFile(Path_);
        if (!data.has_value()) {
            return std::nullopt;
        }
        const auto document = ParseDocument(*data);
        if (!document.has_value() || document->Identity != Identity_) {
            return std::nullopt;
        }
        return document->Cache;
    }
}

void TFileTokenCacher::Write(const NOidc::TTokenCache& cache) {
    with_lock (Mutex_) {
        if (cache.AccessToken.Token.empty()) {
            throw std::invalid_argument("OIDC token cache access token must not be empty");
        }
        if (cache.RefreshToken.has_value() && cache.RefreshToken->Token.empty()) {
            throw std::invalid_argument("OIDC token cache refresh token must not be empty");
        }

        const auto current = ReadCacheFile(Path_);
        if (current.has_value()) {
            const auto document = ParseDocument(*current);
            if (document.has_value() && document->Identity != Identity_) {
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
            TFile file = CreateOwnerOnlyFile(temporary);
            file.Write(data.data(), data.size());
            file.Flush();
            file.Close();
            RejectSymlink(Path_);
            temporary.RenameTo(Path_);
        } catch (const std::runtime_error&) {
            throw;
        } catch (const std::exception&) {
            ThrowCacheError(Path_.GetPath(), "failed to write file");
        }
    }
}

} // namespace

std::shared_ptr<NOidc::ITokenCacher> CreateFileTokenCacher(
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

} // namespace NYdb::NConsoleClient
