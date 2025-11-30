#include "local_paths.h"

#include <util/folder/dirut.h>
#include <util/generic/guid.h>
#if defined(_win32_)
#include <initializer_list>
#endif

#include <util/generic/maybe.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/stream/output.h>
#include <util/system/env.h>
#include <util/system/file.h>
#include <util/system/sysstat.h>
#include <util/system/user.h>

namespace NYdb::NConsoleClient::NLocalPaths {

#if defined(_win32_)
const TString YdbBinaryName = "ydb.exe";
#else
const TString YdbBinaryName = "ydb";
#endif

constexpr int DIR_MODE_PRIVATE = S_IRUSR | S_IWUSR | S_IXUSR; // rwx------
constexpr int DIR_MODE_CACHE = S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH; // rwxr-xr-x
constexpr int FILE_MODE_PRIVATE = S_IRUSR | S_IWUSR; // rw-------

TFsPath GetHomePath() {
#if defined(_win32_)
    if (auto home = TryGetEnv("USERPROFILE")) {
        if (!home->empty()) {
            return TFsPath(*home).Fix();
        }
    }
#endif
    return TFsPath(GetHomeDir()).Fix();
}

namespace {

TMaybe<TFsPath> GetEnvPath(const char* envName) {
    if (auto value = TryGetEnv(envName)) {
        if (!value->empty()) {
            return TFsPath(*value).Fix();
        }
    }
    return Nothing();
}

void EnsureDir(const TFsPath& path, int mode) {
    if (path.Exists()) {
        return;
    }
#if defined(_win32_)
    Y_UNUSED(mode);
    path.MkDirs();
#else
    if (mode > 0) {
        path.MkDirs(mode);
    } else {
        path.MkDirs();
    }
#endif
}

void WriteStringAtomically(const TFsPath& path, const TString& data, int mode) {
    EnsureDir(path.Parent(), DIR_MODE_PRIVATE);

    TString tmpName = TStringBuilder() << '.' << path.GetName() << ".tmp-" << CreateGuidAsString();
    TFsPath tmpPath = path.Parent().Child(tmpName);

    try {
        {
            TFileOutput out(tmpPath);
            out << data;
            out.Finish();
        }
#if !defined(_win32_)
        if (mode > 0) {
            Chmod(tmpPath.GetPath().c_str(), mode);
        }
#endif
        tmpPath.RenameTo(path);
    } catch (...) {
        tmpPath.DeleteIfExists();
        throw;
    }
}

TString ReadFileIfExists(const TFsPath& path) {
    if (!path.Exists()) {
        return TString();
    }
    TFileInput input(path);
    return input.ReadAll();
}

bool IsDirEmpty(const TFsPath& path) {
    if (!path.Exists()) {
        return true;
    }
    TVector<TString> children;
    path.ListNames(children);
    return children.empty();
}

#if defined(_win32_)
TFsPath ResolveWindowsDir(const char* overrideEnv, const char* envName, std::initializer_list<TString> fallbackSuffixes) {
    if (auto overridePath = GetEnvPath(overrideEnv)) {
        return *overridePath;
    }
    if (auto envPath = GetEnvPath(envName)) {
        TFsPath path = *envPath;
        for (const auto& suffix : fallbackSuffixes) {
            path = path.Child(suffix);
        }
        return path.Fix();
    }
    TFsPath path = GetHomePath();
    for (const auto& suffix : fallbackSuffixes) {
        path = path.Child(suffix);
    }
    return path.Fix();
}
#else
TFsPath ResolveUnixXdgDir(const char* overrideEnv, const char* xdgEnv, const TString& fallbackSuffix) {
    if (auto overridePath = GetEnvPath(overrideEnv)) {
        return *overridePath;
    }
    TString base;
    if (auto xdgPath = GetEnvPath(xdgEnv)) {
        base = xdgPath->GetPath();
    } else {
        base = TStringBuilder() << GetHomeDir() << fallbackSuffix;
    }
    return TFsPath(base).Child("ydb").Fix();
}
#endif

void MoveFilePreservingContents(const TFsPath& src, const TFsPath& dst, int mode, bool logMove = true) {
    EnsureDir(dst.Parent(), DIR_MODE_PRIVATE);
    try {
        src.RenameTo(dst);
#if !defined(_win32_)
        if (mode > 0) {
            Chmod(dst.GetPath().c_str(), mode);
        }
#endif
        if (logMove) {
            Cerr << "Migrated legacy file " << src.GetPath() << " to " << dst.GetPath() << Endl;
        }
        return;
    } catch (...) {
        // fall through to copy
    }
    TString data = ReadFileIfExists(src);
    WriteStringAtomically(dst, data, mode);
    try {
        src.DeleteIfExists();
    } catch (const yexception& e) {
        Cerr << "Failed to delete legacy file " << src.GetPath() << " after copy: " << e.what() << Endl;
    }
    if (logMove) {
        Cerr << "Migrated legacy file " << src.GetPath() << " to " << dst.GetPath()
            << " by copy (rename failed)" << Endl;
    }
}

void MoveImportProgress(const TFsPath& targetDir) {
    TFsPath legacyDir = GetHomePath().Child(".config").Child("ydb").Child("import_progress").Fix();
    EnsureDir(targetDir, DIR_MODE_PRIVATE);
    if (!legacyDir.Exists()) {
        return;
    }
    TVector<TFsPath> children;
    legacyDir.List(children);
    bool movedAny = false;
    size_t movedCount = 0;
    for (const auto& child : children) {
        if (!child.IsFile()) {
            continue;
        }
        TFsPath dst = targetDir.Child(child.GetName());
        TFsPath src = child;
        MoveFilePreservingContents(src, dst, FILE_MODE_PRIVATE, false);
        movedAny = true;
        ++movedCount;
    }
    if (IsDirEmpty(legacyDir)) {
        legacyDir.DeleteIfExists();
    }
    if (movedAny) {
        Cerr << "Migrated " << movedCount << " legacy import progress files from "
            << legacyDir.GetPath() << " to " << targetDir.GetPath() << Endl;
    }
}

} // namespace

void DeleteDirIfEmpty(const TFsPath& path) {
    if (path.Exists() && IsDirEmpty(path)) {
        try {
            path.DeleteIfExists();
            Cerr << "Removed empty legacy directory " << path.GetPath() << Endl;
        } catch (const yexception& e) {
            Cerr << "Failed to remove legacy directory " << path.GetPath() << ": " << e.what() << Endl;
        }
    }
}

TFsPath GetConfigDir() {
#if defined(_win32_)
    TFsPath dir = ResolveWindowsDir("YDB_CONFIG_DIR", "APPDATA", {"ydb"});
#else
    TFsPath dir = ResolveUnixXdgDir("YDB_CONFIG_DIR", "XDG_CONFIG_HOME", "/.config");
#endif
    EnsureDir(dir, DIR_MODE_PRIVATE);
    return dir;
}

TFsPath GetStateDir() {
#if defined(_win32_)
    TFsPath dir = ResolveWindowsDir("YDB_STATE_DIR", "LOCALAPPDATA", {"ydb", "State"});
#else
    TFsPath dir = ResolveUnixXdgDir("YDB_STATE_DIR", "XDG_STATE_HOME", "/.local/state");
#endif
    EnsureDir(dir, DIR_MODE_PRIVATE);
    return dir;
}

TFsPath GetCacheDir() {
#if defined(_win32_)
    TFsPath dir = ResolveWindowsDir("YDB_CACHE_DIR", "LOCALAPPDATA", {"ydb", "Cache"});
#else
    TFsPath dir = ResolveUnixXdgDir("YDB_CACHE_DIR", "XDG_CACHE_HOME", "/.cache");
#endif
    EnsureDir(dir, DIR_MODE_CACHE);
    return dir;
}

TFsPath GetCanonicalBinaryPath() {
#if defined(_win32_)
    TFsPath base;
    if (auto localAppData = GetEnvPath("LOCALAPPDATA")) {
        base = *localAppData;
    } else {
        base = GetHomePath().Child("AppData").Child("Local");
    }
    return base.Child("Programs").Child("ydb").Child(YdbBinaryName).Fix();
#else
    return GetHomePath().Child(".local").Child("bin").Child(YdbBinaryName).Fix();
#endif
}

TFsPath GetLegacyBinaryPath() {
    return GetHomePath().Child("ydb").Child("bin").Child(YdbBinaryName).Fix();
}

TFsPath GetLegacyPathHelperScript() {
    return GetHomePath().Child("ydb").Child("path.bash.inc").Fix();
}

TFsPath GetProfilesFile() {
    TFsPath filePath = GetConfigDir().Child("profiles.yaml");
    if (!filePath.Exists()) {
        TFsPath legacyDir = GetHomePath().Child("ydb").Child("config");
        TFsPath legacy = legacyDir.Child("config.yaml");
        if (legacy.Exists()) {
            MoveFilePreservingContents(legacy, filePath, FILE_MODE_PRIVATE);
            DeleteDirIfEmpty(legacyDir);
            DeleteDirIfEmpty(GetHomePath().Child("ydb"));
        }
    }
    return filePath;
}

TFsPath GetImportProgressDir() {
    TFsPath dir = GetStateDir().Child("import_progress");
    if (!dir.Exists()) {
        MoveImportProgress(dir);
    }
    return dir;
}

TFsPath GetUpdateStateFile() {
    TFsPath dir = GetStateDir().Child("update");
    EnsureDir(dir, DIR_MODE_PRIVATE);
    TFsPath file = dir.Child("state.json");
    if (!file.Exists()) {
        TFsPath legacyDir = GetHomePath().Child("ydb").Child("bin");
        TFsPath legacy = legacyDir.Child("config.json").Fix();
        if (legacy.Exists()) {
            MoveFilePreservingContents(legacy, file, FILE_MODE_PRIVATE);
            DeleteDirIfEmpty(legacyDir);
            DeleteDirIfEmpty(GetHomePath().Child("ydb"));
        }
    }
    return file;
}

TFsPath GetUpdateCacheDir() {
    TFsPath dir = GetCacheDir().Child("update");
    EnsureDir(dir, DIR_MODE_CACHE);
    return dir;
}

TFsPath GetHistoryFile() {
    TFsPath stateDir = GetStateDir();
    TFsPath target = stateDir.Child("interactive_history");
    if (!target.Exists()) {
        TFsPath legacy = GetHomePath().Child(".ydb_history");
        if (legacy.Exists()) {
            MoveFilePreservingContents(legacy, target, FILE_MODE_PRIVATE);
        }
    }
    return target;
}

}

