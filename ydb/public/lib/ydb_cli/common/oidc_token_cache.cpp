#include "oidc_token_cache.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/stream/output.h>
#include <util/system/error.h>
#include <util/system/file.h>
#include <util/system/mutex.h>
#include <util/system/fstat.h>
#include <util/system/platform.h>
#include <util/system/tempfile.h>
#include <util/system/sysstat.h>

#include <cerrno>
#include <filesystem>
#include <optional>
#include <memory>
#include <stdexcept>
#include <system_error>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#if defined(_win_)
    #include <util/system/fs_win.h>

    #include <aclapi.h>
    #include <windows.h>
#else
    #include <fcntl.h>
    #include <unistd.h>
#endif

namespace NYdb::NConsoleClient {
namespace {

constexpr ui64 CacheVersion = 1;
constexpr i64 MaxCacheSize = 1024 * 1024;
constexpr ui64 MaxJsonDepth = 8;

// Only these errors are safe to include in CLI diagnostics. Other exceptions
// can originate in JSON or I/O helpers and must not expose their messages.
class TCacheError: public std::runtime_error {
public:
    explicit TCacheError(const std::string& message);
};

struct TCacheDocument {
    std::string Identity;
    NOidc::TTokenCache Cache;
};

[[noreturn]] void ThrowCacheError(const std::string& path, const std::string& problem);
void RejectSymlink(const TFsPath& path);
bool IsNotFoundError(int error);
TFileHandle OpenCacheHandle(const TFsPath& path);
void ValidateRegularFile(const TFileStat& stat, const std::string& path);
void ValidateCachePermissions(const TFile& file, const std::string& path);
TFile CreateOwnerOnlyFile(const TFsPath& path);
std::optional<TFile> OpenCacheFile(const TFsPath& path);
std::optional<std::string> ReadCacheFile(const TFsPath& path);
std::optional<ui64> ReadUnsigned(const NJson::TJsonValue& value);
std::optional<NOidc::TOAuthToken> ParseToken(const NJson::TJsonValue& value);
std::optional<TCacheDocument> ParseDocument(const std::string& data);
NJson::TJsonValue SerializeToken(const NOidc::TOAuthToken& token);
std::string SerializeDocument(const std::string& identity, const NOidc::TTokenCache& cache);

TCacheError::TCacheError(const std::string& message)
    : std::runtime_error(message)
{
}

[[noreturn]] void ThrowCacheError(const std::string& path, const std::string& problem) {
    throw TCacheError("OIDC token cache '" + path + "': " + problem);
}

void RejectSymlink(const TFsPath& path) {
    if (path.IsSymlink()) {
        ThrowCacheError(path.GetPath(), "refusing to access a symlink");
    }
}

void ValidateRegularFile(const TFileStat& stat, const std::string& path) {
    if (stat.IsNull()) {
        ThrowCacheError(path, "failed to inspect opened file");
    }
    if (stat.IsSymlink()) {
        ThrowCacheError(path, "refusing to access a symlink");
    }
    if (!stat.IsFile()) {
        ThrowCacheError(path, "path is not a regular file");
    }
}

// TFile does not expose no-follow/nonblocking open modes or Windows ACLs.
// Keep these operations and account lookup platform-specific; use util for
// handle lifetime, metadata, I/O, temporary-file cleanup and atomic replacement.
#if defined(_win_)
std::vector<unsigned char> GetCurrentUser(const std::string& path);

class TOwnerOnlySecurity {
public:
    explicit TOwnerOnlySecurity(const std::string& path);
    SECURITY_ATTRIBUTES* GetAttributes();

private:
    std::vector<unsigned char> TokenInfo_;
    std::unique_ptr<void, decltype(&LocalFree)> Acl_{nullptr, &LocalFree};
    SECURITY_DESCRIPTOR Descriptor_{};
    SECURITY_ATTRIBUTES Attributes_{};
};

std::vector<unsigned char> GetCurrentUser(const std::string& path) {
    HANDLE processToken = nullptr;
    if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &processToken)) {
        ThrowCacheError(path, "failed to determine file owner");
    }
    const TFileHandle tokenHandle(processToken);

    DWORD tokenInfoSize = 0;
    GetTokenInformation(processToken, TokenUser, nullptr, 0, &tokenInfoSize);
    if (tokenInfoSize == 0 || GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
        ThrowCacheError(path, "failed to determine file owner");
    }
    std::vector<unsigned char> tokenInfo(tokenInfoSize);
    if (!GetTokenInformation(processToken, TokenUser, tokenInfo.data(), tokenInfoSize, &tokenInfoSize)) {
        ThrowCacheError(path, "failed to determine file owner");
    }
    return tokenInfo;
}

TOwnerOnlySecurity::TOwnerOnlySecurity(const std::string& path)
    : TokenInfo_(GetCurrentUser(path))
{
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
        !SetSecurityDescriptorOwner(&Descriptor_, tokenUser->User.Sid, false) ||
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

void ValidateCachePermissions(const TFile& file, const std::string& path) {
    const auto tokenInfo = GetCurrentUser(path);
    const auto* tokenUser = reinterpret_cast<const TOKEN_USER*>(tokenInfo.data());
    PSID owner = nullptr;
    PACL acl = nullptr;
    PSECURITY_DESCRIPTOR descriptor = nullptr;
    const DWORD error = GetSecurityInfo(file.GetHandle(), SE_FILE_OBJECT,
        OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
        &owner, nullptr, &acl, nullptr, &descriptor);
    const std::unique_ptr<void, decltype(&LocalFree)> cleanup(descriptor, &LocalFree);
    if (error != ERROR_SUCCESS) {
        ThrowCacheError(path, "failed to inspect file permissions");
    }
    if (owner == nullptr || !EqualSid(owner, tokenUser->User.Sid)) {
        ThrowCacheError(path, "file owner is not the current account");
    }
    // A null DACL grants access to everyone. Only owner allow-ACEs are accepted;
    // deny entries cannot grant access, and inherit-only entries do not apply here.
    if (acl == nullptr || !IsValidAcl(acl)) {
        ThrowCacheError(path, "file permissions must allow access only to the owner");
    }
    for (DWORD i = 0; i < acl->AceCount; ++i) {
        void* entry = nullptr;
        if (!GetAce(acl, i, &entry)) {
            ThrowCacheError(path, "failed to inspect file permissions");
        }
        const auto* header = static_cast<const ACE_HEADER*>(entry);
        if ((header->AceFlags & INHERIT_ONLY_ACE) != 0 || header->AceType == ACCESS_DENIED_ACE_TYPE) {
            continue;
        }
        if (header->AceType != ACCESS_ALLOWED_ACE_TYPE ||
            !EqualSid(&static_cast<ACCESS_ALLOWED_ACE*>(entry)->SidStart, tokenUser->User.Sid))
        {
            ThrowCacheError(path, "file permissions must allow access only to the owner");
        }
    }
}

bool IsNotFoundError(int error) {
    return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND;
}

TFileHandle OpenCacheHandle(const TFsPath& path) {
    TFileHandle handle(NFsPrivate::CreateFileWithUtf8Name(
        path.GetPath(), GENERIC_READ,
        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
        OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_BACKUP_SEMANTICS, false));
    if (!handle.IsOpen()) {
        return handle;
    }
    BY_HANDLE_FILE_INFORMATION information{};
    if (!GetFileInformationByHandle(handle, &information)) {
        ThrowCacheError(path.GetPath(), "failed to inspect opened file");
    }
    if (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) {
        ThrowCacheError(path.GetPath(), "refusing to access a symlink or reparse point");
    }
    if (GetFileType(handle) != FILE_TYPE_DISK) {
        ThrowCacheError(path.GetPath(), "path is not a regular file");
    }
    return handle;
}

TFile CreateOwnerOnlyFile(const TFsPath& path) {
    // TFile's ARUser/AWUser flags do not restrict the Windows DACL.
    TOwnerOnlySecurity security(path.GetPath());
    const auto& utf8Path = path.GetPath();
    const std::filesystem::path windowsPath(
        std::u8string_view(reinterpret_cast<const char8_t*>(utf8Path.data()), utf8Path.size()));
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
}
#else
void ValidateCachePermissions(const TFile& file, const std::string& path) {
    const TFileStat stat(file);
    if (stat.IsNull()) {
        ThrowCacheError(path, "failed to inspect file permissions");
    }
    if (stat.Uid != geteuid()) {
        ThrowCacheError(path, "file owner is not the current account");
    }
    if ((stat.Mode & (S_IRWXG | S_IRWXO)) != 0) {
        ThrowCacheError(path, "file permissions must allow access only to the owner (chmod 600)");
    }
}

bool IsNotFoundError(int error) {
    return error == ENOENT;
}

TFileHandle OpenCacheHandle(const TFsPath& path) {
    int descriptor;
    do {
        descriptor = open(path.GetPath().c_str(), O_RDONLY | O_NOFOLLOW | O_NONBLOCK | O_CLOEXEC);
    } while (descriptor < 0 && errno == EINTR);
    if (descriptor < 0 && errno == ELOOP) {
        ThrowCacheError(path.GetPath(), "refusing to access a symlink");
    }
    return TFileHandle(descriptor);
}

TFile CreateOwnerOnlyFile(const TFsPath& path) {
    return TFile(path.GetPath(), CreateNew | WrOnly | CloseOnExec | ARUser | AWUser);
}
#endif

std::optional<TFile> OpenCacheFile(const TFsPath& path) {
    auto handle = OpenCacheHandle(path);
    if (!handle.IsOpen()) {
        const int error = LastSystemError();
        if (IsNotFoundError(error)) {
            return std::nullopt;
        }
        const std::error_code code(error, std::system_category());
        if (code == std::errc::permission_denied || code == std::errc::operation_not_permitted) {
            ThrowCacheError(path.GetPath(), "permission denied when opening file");
        }
        ThrowCacheError(path.GetPath(), "failed to open file");
    }
    TFile file(handle.Release(), path.GetPath());
    ValidateRegularFile(TFileStat(file), path.GetPath());
    ValidateCachePermissions(file, path.GetPath());
    return file;
}

std::optional<std::string> ReadCacheFile(const TFsPath& path) {
    try {
        auto file = OpenCacheFile(path);
        if (!file.has_value()) {
            return std::nullopt;
        }
        const i64 size = file->GetLength();
        if (size < 0) {
            ThrowCacheError(path.GetPath(), "failed to determine file size");
        }
        if (size > MaxCacheSize) {
            return std::string{};
        }
        std::string data(static_cast<size_t>(size), '\0');
        if (size) {
            file->Load(data.data(), data.size());
        }
        return data;
    } catch (const TCacheError&) {
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
    TFileTokenCacher(std::string path, std::string identity, std::function<void(const std::string&)> diagnostic);

    std::optional<NOidc::TTokenCache> Read() const override;
    void Write(const NOidc::TTokenCache& cache) override;

private:
    void ReportError(const std::string& message) const noexcept;

    TFsPath Path_;
    std::string Identity_;
    std::function<void(const std::string&)> Diagnostic_;
    mutable TMutex Mutex_;
};

TFileTokenCacher::TFileTokenCacher(std::string path, std::string identity, std::function<void(const std::string&)> diagnostic)
    : Path_(std::move(path))
    , Identity_(std::move(identity))
    , Diagnostic_(std::move(diagnostic))
{
}

std::optional<NOidc::TTokenCache> TFileTokenCacher::Read() const {
    try {
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
    } catch (const TCacheError& error) {
        ReportError(error.what());
        throw;
    } catch (const std::exception&) {
        ReportError("OIDC token cache '" + Path_.GetPath() + "': failed to read file");
        throw;
    }
}

void TFileTokenCacher::Write(const NOidc::TTokenCache& cache) {
    try {
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
            // Cleanup covers successful writes and exception unwinding. SIGKILL or
            // power loss can leave an owner-only temporary file containing tokens;
            // such files require manual removal. Do not sweep siblings: another
            // CLI process may be writing a different cache in this directory.
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
            } catch (const TCacheError&) {
                throw;
            } catch (const std::exception&) {
                ThrowCacheError(Path_.GetPath(), "failed to write file");
            }
        }
    } catch (const TCacheError& error) {
        ReportError(error.what());
        throw;
    } catch (const std::exception&) {
        ReportError("OIDC token cache '" + Path_.GetPath() + "': failed to write file");
        throw;
    }
}

void TFileTokenCacher::ReportError(const std::string& message) const noexcept {
    try {
        Diagnostic_(message);
    } catch (...) {
        // A diagnostic sink must not replace the original cache error.
    }
}

} // namespace

std::shared_ptr<NOidc::ITokenCacher> CreateFileTokenCacher(
    const std::string& cacheFilePath,
    const std::string& identity)
{
    return CreateFileTokenCacher(cacheFilePath, identity, [](const std::string& message) {
        Cerr << "Warning: " << message << Endl;
    });
}

std::shared_ptr<NOidc::ITokenCacher> CreateFileTokenCacher(
    const std::string& cacheFilePath,
    const std::string& identity,
    std::function<void(const std::string&)> diagnostic)
{
    if (cacheFilePath.empty()) {
        throw std::invalid_argument("OIDC token cache path must not be empty");
    }
    if (identity.empty()) {
        throw std::invalid_argument("OIDC token cache identity must not be empty");
    }
    return std::make_shared<TFileTokenCacher>(cacheFilePath, identity, std::move(diagnostic));
}

} // namespace NYdb::NConsoleClient
