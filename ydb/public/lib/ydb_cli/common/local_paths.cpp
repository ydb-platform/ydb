#include "local_paths.h"

#include <util/folder/dirut.h>
#include <util/string/builder.h>
#include <util/system/env.h>

namespace NYdb::NConsoleClient::NLocalPaths {

namespace {

constexpr int DIR_MODE_PRIVATE = S_IRUSR | S_IWUSR | S_IXUSR; // rwx------

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

TMaybe<TFsPath> GetEnvPath(const char* envName) {
    if (auto value = TryGetEnv(envName)) {
        if (!value->empty()) {
            return TFsPath(*value).Fix();
        }
    }
    return Nothing();
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
    TFsPath path = GetHomeDir();
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

TFsPath GetStateDir() {
#if defined(_win32_)
    TFsPath dir = ResolveWindowsDir("YDB_STATE_DIR", "LOCALAPPDATA", {"ydb", "State"});
#else
    TFsPath dir = ResolveUnixXdgDir("YDB_STATE_DIR", "XDG_STATE_HOME", "/.local/state");
#endif
    EnsureDir(dir, DIR_MODE_PRIVATE);
    return dir;
}

} // anonymous namespace

TFsPath GetAiHistoryFile() {
    TFsPath stateDir = GetStateDir();
    TFsPath target = stateDir.Child("ai_history");
    return target;
}

} // namespace NYdb::NConsoleClient::NLocalPaths
