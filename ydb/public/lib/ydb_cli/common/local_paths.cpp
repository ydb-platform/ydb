#include "local_paths.h"

#include <initializer_list>

#include <util/folder/dirut.h>
#include <util/folder/iterator.h>
#include <util/generic/guid.h>
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

namespace {

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

TMaybe<TFsPath> GetEnvPath(const char* envName) {
    if (auto value = TryGetEnv(envName)) {
        if (!value->empty()) {
            return TFsPath(*value).Fix();
        }
    }
    return Nothing();
}

void EnsureDir(const TFsPath& path, int mode) {
    const bool needCreate = !path.Exists();
    if (needCreate) {
        path.MkDirs();
    }
#if !defined(_win32_)
    if (needCreate && mode > 0) {
        Chmod(path.GetPath().c_str(), mode);
    }
#else
    Y_UNUSED(mode);
#endif
}

void WriteStringAtomically(const TFsPath& path, const TString& data, int mode) {
    EnsureDir(path.Parent(), S_IRUSR | S_IWUSR | S_IXUSR);

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

TFsPath GetLegacyRoot() {
    return GetHomePath().Child("ydb");
}

TFsPath GetLegacyProfilesFile() {
    return GetLegacyRoot().Child("config").Child("config.yaml");
}

TFsPath GetLegacyImportProgressDir() {
#if defined(_win32_)
    if (auto appData = GetEnvPath("APPDATA")) {
        return appData->Child("ydb").Child("import_progress");
    }
    return GetHomePath().Child("AppData").Child("Roaming").Child("ydb").Child("import_progress");
#else
    TString base;
    if (auto xdg = GetEnvPath("XDG_CONFIG_HOME")) {
        base = xdg->GetPath();
    } else {
        base = TStringBuilder() << GetHomeDir() << "/.config";
    }
    return TFsPath(base).Child("ydb").Child("import_progress").Fix();
#endif
}

TFsPath GetLegacyUpdateStateFile() {
    return GetLegacyRoot().Child("bin").Child("config.json").Fix();
}

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

void CopyFileContents(const TFsPath& src, const TFsPath& dst, int mode) {
    TFileInput in(src);
    TString data = in.ReadAll();
    WriteStringAtomically(dst, data, mode);
}

void MoveFilePreservingContents(const TFsPath& src, const TFsPath& dst, int mode) {
    EnsureDir(dst.Parent(), S_IRUSR | S_IWUSR | S_IXUSR);
    try {
        src.RenameTo(dst);
        Cerr << "Migrated legacy file " << src.GetPath() << " to " << dst.GetPath() << Endl;
        return;
    } catch (...) {
        // fall through to copy
    }
    TString data = ReadFileIfExists(src);
    WriteStringAtomically(dst, data, mode);
    src.DeleteIfExists();
    Cerr << "Migrated legacy file " << src.GetPath() << " to " << dst.GetPath() << Endl;
}

void MigrateProfiles(const TFsPath& target) {
    if (target.Exists()) {
        return;
    }
    TFsPath legacy = GetLegacyProfilesFile();
    if (!legacy.Exists()) {
        return;
    }
    MoveFilePreservingContents(legacy, target, S_IRUSR | S_IWUSR);
}

void MoveImportProgress(const TFsPath& targetDir) {
    if (targetDir.Exists() && !IsDirEmpty(targetDir)) {
        return;
    }
    TFsPath legacyDir = GetLegacyImportProgressDir();
    if (!legacyDir.Exists()) {
        EnsureDir(targetDir, S_IRUSR | S_IWUSR | S_IXUSR);
        return;
    }
    EnsureDir(targetDir, S_IRUSR | S_IWUSR | S_IXUSR);
    TDirIterator it(legacyDir.GetPath());
    bool movedAny = false;
    while (it.Next()) {
        if (it->fts_info != FTS_F) {
            continue;
        }
        const TString name(it->fts_name);
        TFsPath src = legacyDir.Child(name);
        TFsPath dst = targetDir.Child(name);

        if (dst.Exists()) {
            TFileStat srcStat(src);
            TFileStat dstStat(dst);
            if (srcStat.MTime <= dstStat.MTime) {
                src.DeleteIfExists();
                continue;
            }
        }

        CopyFileContents(src, dst, S_IRUSR | S_IWUSR);
        src.DeleteIfExists();
        movedAny = true;
    }
    if (IsDirEmpty(legacyDir)) {
        legacyDir.DeleteIfExists();
    }
    if (movedAny) {
        Cerr << "Migrated legacy import progress files from " << legacyDir.GetPath()
            << " to " << targetDir.GetPath() << Endl;
    }
}

void MigrateUpdateState(const TFsPath& target) {
    if (target.Exists()) {
        return;
    }
    TFsPath legacy = GetLegacyUpdateStateFile();
    if (!legacy.Exists()) {
        return;
    }
    MoveFilePreservingContents(legacy, target, S_IRUSR | S_IWUSR);
}

}

TFsPath GetConfigDir() {
#if defined(_win32_)
    TFsPath dir = ResolveWindowsDir("YDB_CONFIG_DIR", "APPDATA", {"ydb"});
#else
    TFsPath dir = ResolveUnixXdgDir("YDB_CONFIG_DIR", "XDG_CONFIG_HOME", "/.config");
#endif
    EnsureDir(dir, S_IRUSR | S_IWUSR | S_IXUSR);
    return dir;
}

TFsPath GetStateDir() {
#if defined(_win32_)
    TFsPath dir = ResolveWindowsDir("YDB_STATE_DIR", "LOCALAPPDATA", {"ydb", "State"});
#else
    TFsPath dir = ResolveUnixXdgDir("YDB_STATE_DIR", "XDG_STATE_HOME", "/.local/state");
#endif
    EnsureDir(dir, S_IRUSR | S_IWUSR | S_IXUSR);
    return dir;
}

TFsPath GetCacheDir() {
#if defined(_win32_)
    TFsPath dir = ResolveWindowsDir("YDB_CACHE_DIR", "LOCALAPPDATA", {"ydb", "Cache"});
#else
    TFsPath dir = ResolveUnixXdgDir("YDB_CACHE_DIR", "XDG_CACHE_HOME", "/.cache");
#endif
    EnsureDir(dir, S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
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
    return GetLegacyRoot().Child("bin").Child(YdbBinaryName).Fix();
}

TFsPath GetLegacyPathHelperScript() {
    return GetLegacyRoot().Child("path.bash.inc").Fix();
}

TFsPath GetProfilesFile() {
    TFsPath filePath = GetConfigDir().Child("profiles.yaml");
    MigrateProfiles(filePath);
    return filePath;
}

TFsPath GetImportProgressDir() {
    TFsPath dir = GetStateDir().Child("import_progress");
    MoveImportProgress(dir);
    return dir;
}

TFsPath GetUpdateStateFile() {
    TFsPath dir = GetStateDir().Child("update");
    EnsureDir(dir, S_IRUSR | S_IWUSR | S_IXUSR);
    TFsPath file = dir.Child("state.json");
    MigrateUpdateState(file);
    return file;
}

TFsPath GetUpdateCacheDir() {
    TFsPath dir = GetCacheDir().Child("update");
    EnsureDir(dir, S_IRUSR | S_IWUSR | S_IXUSR | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH);
    return dir;
}

}

