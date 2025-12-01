#include "ydb_updater.h"

#include <library/cpp/json/writer/json.h>
#include <library/cpp/resource/resource.h>
#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/generic/vector.h>
#include <util/generic/scope.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/string/split.h>
#include <util/string/strip.h>
#include <util/system/env.h>
#include <util/system/execpath.h>
#include <util/system/shellcommand.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>

#include "local_paths.h"

#ifndef _win32_
#include <sys/utsname.h>
#endif
namespace NYdb {
namespace NConsoleClient {

TString GetOsArchitecture() {
#if defined(_win32_)
    return "amd64";
#else
    struct utsname uts;
    uname(&uts);
    TString machine = uts.machine;
    if (machine == "arm64" || machine == "aarch64") {
        return "arm64";
    } else {
        return "amd64";
    }
#endif
}

namespace {
#if defined(_darwin_)
    const TString osVersion = "darwin";
#elif defined(_win32_)
    const TString osVersion = "windows";
#else
    const TString osVersion = "linux";
#endif
    const TString osArch = GetOsArchitecture();

    bool DeletePathIfExistsSafe(const TFsPath& path) {
        if (!path.Exists()) {
            return false;
        }
        try {
            path.DeleteIfExists();
            return true;
        } catch (const yexception& e) {
            Cerr << "Failed to delete " << path.GetPath() << ": " << e.what() << Endl;
            return false;
        }
    }

#if !defined(_win32_)
    TFsPath GetHomePath() {
        TFsPath home(GetHomeDir());
        home.Fix();
        return home;
    }

    TFsPath MakeHomeFile(const TString& name) {
        return GetHomePath().Child(name);
    }

    bool ReadFileContent(const TFsPath& path, TString& out) {
        if (!path.Exists()) {
            return false;
        }
        TFileInput in(path);
        out = in.ReadAll();
        return true;
    }

    void WriteFileContent(const TFsPath& path, const TString& data) {
        TFsPath parent = path.Parent();
        if (!parent.Exists()) {
            parent.MkDirs();
        }
        TFileOutput out(path);
        out << data;
        out.Finish();
    }

    bool ShouldRemoveLegacyLine(const TString& line) {
        return line.Contains("ydb/path.bash.inc")
            || line.Contains("ydb/bin")
            || line.Contains("The next line updates PATH for YDB CLI");
    }

    bool CleanupLegacyRcFile(const TFsPath& file) {
        TString content;
        if (!ReadFileContent(file, content)) {
            return false;
        }
        TString cleaned;
        cleaned.reserve(content.size());
        bool changed = false;
        size_t pos = 0;
        while (pos <= content.size()) {
            size_t next = content.find('\n', pos);
            TString line = content.substr(pos, next == TString::npos ? TString::npos : next - pos);
            if (!line.empty() || next != TString::npos) {
                if (ShouldRemoveLegacyLine(line)) {
                    changed = true;
                } else {
                    if (!cleaned.empty()) {
                        cleaned.push_back('\n');
                    }
                    cleaned.append(line);
                }
            }
            if (next == TString::npos) {
                break;
            }
            pos = next + 1;
        }
        if (content.EndsWith('\n')) {
            cleaned.push_back('\n');
        }
        if (!changed) {
            return false;
        }
        WriteFileContent(file, cleaned);
        Cerr << "Removed legacy PATH entries from " << file.GetPath() << Endl;
        return true;
    }

    bool EnsureLocalBinBlock(const TFsPath& file) {
        TString content;
        if (ReadFileContent(file, content) && content.Contains(".local/bin")) {
            return false;
        }
        TString block =
            "# YDB CLI: append ~/.local/bin to PATH\n"
            "if ! echo \"${PATH}\" | tr ':' '\\n' | grep -Fq \"${HOME}/.local/bin\"; then\n"
            "    export PATH=\"${HOME}/.local/bin:${PATH}\"\n"
            "fi\n";
        if (!content.empty() && !content.EndsWith('\n')) {
            content.push_back('\n');
        }
        content += block;
        WriteFileContent(file, content);
        Cerr << "Added ~/.local/bin PATH export to " << file.GetPath() << Endl;
        return true;
    }

    void UpdateUnixPathReferences() {
        TFsPath helper = NLocalPaths::GetLegacyPathHelperScript();
        if (!helper.Exists()) {
            return;
        }
        if (DeletePathIfExistsSafe(helper)) {
            Cerr << "Removed legacy PATH helper script " << helper.GetPath() << Endl;
            NLocalPaths::DeleteDirIfEmpty(helper.Parent());
        }
        const TVector<TFsPath> filesToClean = {
            MakeHomeFile(".zprofile"),
            MakeHomeFile(".zshrc"),
            MakeHomeFile(".bash_profile"),
            MakeHomeFile(".bashrc"),
            MakeHomeFile(".profile")
        };
        for (const auto& file : filesToClean) {
            CleanupLegacyRcFile(file);
        }
        const char* shell = getenv("SHELL");
        TVector<TFsPath> filesToUpdate;
        if (shell && TString(shell).Contains("zsh")) {
            filesToUpdate.push_back(MakeHomeFile(".zprofile"));
        } else {
            filesToUpdate.push_back(MakeHomeFile(".bash_profile"));
            filesToUpdate.push_back(MakeHomeFile(".bashrc"));
        }
        for (const auto& file : filesToUpdate) {
            EnsureLocalBinBlock(file);
        }
        NColorizer::TColors warnColors = NConsoleClient::AutoColors(Cerr);
        Cerr << warnColors.RedColor()
            << "PATH updated. Restart your shell (exec -l $SHELL) or re-source your profile files to apply changes."
            << warnColors.OldColor() << Endl;
    }
#else

TString EscapeForPowerShellSingleQuotes(const TString& value) {
    TString result;
    result.reserve(value.size());
    for (const char ch : value) {
        if (ch == '\'') {
            result.append("''");
        } else {
            result.push_back(ch);
        }
    }
    return result;
}

bool PersistWindowsPath(const TString& newPath) {
    TString escaped = EscapeForPowerShellSingleQuotes(newPath);
    TShellCommand cmd(TStringBuilder()
        << "powershell -NoLogo -NoProfile -Command \"[Environment]::SetEnvironmentVariable('Path','"
        << escaped << "','User')\"");
    cmd.Run().Wait();
    return cmd.GetExitCode() == 0;
}

    void UpdateWindowsPathReferences() {
        TString legacyEntry = NLocalPaths::GetLegacyBinaryPath().Parent().GetPath();
        TString canonicalEntry = NLocalPaths::GetCanonicalBinaryPath().Parent().GetPath();
        TVector<TString> elements = StringSplitter(GetEnv("PATH")).Split(';').ToList<TString>();
        bool replaced = false;
        bool hasCanonical = false;
        for (TString& element : elements) {
            TString trimmed = StripString(element);
            if (trimmed.empty()) {
                continue;
            }
            if (trimmed == legacyEntry) {
                element = canonicalEntry;
                replaced = true;
                hasCanonical = true;
            } else if (trimmed == canonicalEntry) {
                hasCanonical = true;
            }
        }
        if (replaced) {
            TString newPath = JoinSeq(";", elements);
            SetEnv("PATH", newPath);
            bool persisted = PersistWindowsPath(newPath);
            NColorizer::TColors warn = NConsoleClient::AutoColors(Cerr);
            if (persisted) {
                Cerr << warn.RedColor()
                    << "Binary location has changed: replaced \"" << legacyEntry << "\" with \"" << canonicalEntry
                    << "\" in user PATH. Restart your shell to apply changes and continue using 'ydb'."
                    << warn.OldColor() << Endl;
            } else {
                Cerr << warn.RedColor()
                    << "Updated PATH only for current session; failed to persist changes. "
                    << "Add \"" << canonicalEntry << "\" to your PATH manually and restart the shell."
                    << warn.OldColor() << Endl;
            }
            return;
        }
        if (hasCanonical) {
            return;
        }
        Cerr << "Add \"" << canonicalEntry << "\" to your PATH so that 'ydb' is available after restart. "
            << "For example, run: setx PATH \"%PATH%;" << canonicalEntry << "\" and restart your shell." << Endl;
    }
#endif

    void EnsureParentDirExists(const TFsPath& path) {
        TFsPath parent = path.Parent();
        if (!parent.Exists()) {
            parent.MkDirs();
        }
    }

    TFsPath CreateCacheDownloadPath() {
        TFsPath dir = NLocalPaths::GetUpdateCacheDir();
#if defined(_win32_)
        TFsPath path = dir.Child(TStringBuilder() << "ydb-update-" << CreateGuidAsString() << ".exe");
#else
        TFsPath path = dir.Child(TStringBuilder() << "ydb-update-" << CreateGuidAsString());
#endif
        path.Fix();
        EnsureParentDirExists(path);
        return path;
    }

    TFsPath CreateStagingBinaryPath(const TFsPath& binaryPath) {
        TFsPath staging = binaryPath.Parent().Child(
            TStringBuilder() << '.' << binaryPath.GetName() << ".tmp-" << CreateGuidAsString());
        return staging.Fix();
    }

    void CopyBinaryFile(const TFsPath& src, const TFsPath& dst) {
        TFileInput input(src);
        TFileOutput output(dst);
        static const size_t BufferSize = 1 << 16;
        char buffer[BufferSize];
        size_t read = 0;
        while ((read = input.Read(buffer, BufferSize)) > 0) {
            output.Write(buffer, read);
        }
        output.Finish();
    }

    TFsPath GetCurrentBinaryPath() {
        TFsPath current(GetExecPath());
        current.Fix();
        return current;
    }

    void RemoveLegacyBinaryFile(const TFsPath& legacyBinary) {
        if (!legacyBinary.Exists()) {
            return;
        }
        const TFsPath legacyBinDir = legacyBinary.Parent();
        const TFsPath legacyRootDir = legacyBinDir.Parent();
        const TFsPath legacyInstallDir = legacyRootDir.Child("install");
        auto cleanupLegacyDirs = [&]() {
            NLocalPaths::DeleteDirIfEmpty(legacyBinDir);
            NLocalPaths::DeleteDirIfEmpty(legacyInstallDir);
            NLocalPaths::DeleteDirIfEmpty(legacyRootDir);
        };
#if defined(_win32_)
        TFsPath legacyBackup = legacyBinDir.Child(TStringBuilder() << NLocalPaths::YdbBinaryName << "_old");
        DeletePathIfExistsSafe(legacyBackup);
        TFsPath canonicalBackup = NLocalPaths::GetCanonicalBinaryPath().Parent().Child(
            TStringBuilder() << NLocalPaths::YdbBinaryName << "_old").Fix();
        DeletePathIfExistsSafe(canonicalBackup);
        try {
            EnsureParentDirExists(canonicalBackup);
            legacyBinary.RenameTo(canonicalBackup);
            Cerr << "Legacy binary moved to canonical backup " << canonicalBackup.GetPath() << Endl;
        } catch (const yexception& e) {
            Cerr << "Failed to move legacy binary to canonical backup: " << e.what() << Endl;
            Cerr << "Legacy binary remains at " << legacyBinary.GetPath() << ". Delete it manually after exiting the CLI." << Endl;
            return;
        }
        cleanupLegacyDirs();
#else
        if (DeletePathIfExistsSafe(legacyBinary)) {
            Cerr << "Removed legacy binary " << legacyBinary.GetPath() << Endl;
            cleanupLegacyDirs();
        }
#endif
    }
}

TYdbUpdater::TYdbUpdater(std::string storageUrl)
    : MyVersion(StripString(NResource::Find(TStringBuf(VersionResourceName))))
    , StorageUrl(storageUrl)
{
    LoadConfig();
}

TYdbUpdater::~TYdbUpdater() {
    if (ConfigChanged) {
        SaveConfig();
    }
}

int TYdbUpdater::Update(bool forceUpdate) {
    if (GetLatestVersion()) {
        if (MyVersion == LatestVersion && !forceUpdate) {
            Cerr << "Current version: \"" << MyVersion << "\". Latest version available: \"" << LatestVersion
                << "\". No need to update. Use '--force' option to update anyway." << Endl;
            return EXIT_FAILURE;
        }
    } else {
        return EXIT_FAILURE;
    }

    TFsPath tmpPathToBinary = CreateCacheDownloadPath();
    TString downloadUrl = TStringBuilder() << StorageUrl << '/' << LatestVersion << '/' << osVersion
        << '/' << osArch << '/' << NLocalPaths::YdbBinaryName;
    downloadUrl = "https://storage.yandexcloud.net/yandexcloud-ydb/testing/test/ydb";
    Cout << "Downloading binary from url " << downloadUrl << Endl;
    TShellCommand curlCmd(TStringBuilder() << "curl --connect-timeout 60 " << downloadUrl << " -o " << tmpPathToBinary.GetPath());
    curlCmd.Run().Wait();
    if (curlCmd.GetExitCode() != 0) {
        Cerr << "Failed to download from url \"" << downloadUrl << "\". " << curlCmd.GetError() << Endl;
        return EXIT_FAILURE;
    }
    Cout << "Downloaded to " << tmpPathToBinary.GetPath() << Endl;

#ifndef _win32_
    int chmodResult = Chmod(tmpPathToBinary.GetPath().data(), MODE0777);
    if (chmodResult != 0) {
        Cerr << "Couldn't make binary \"" << tmpPathToBinary.GetPath() << "\" executable. Error code: "
            << chmodResult << Endl;
    }
#endif

    // Check new binary
    Cout << "Checking downloaded binary by calling 'version' command..." << Endl;
    TShellCommand checkCmd(TStringBuilder() << tmpPathToBinary.GetPath() << " version");
    checkCmd.Run().Wait();
    if (checkCmd.GetExitCode() != 0) {
        Cerr << "Failed to check downloaded binary. " << checkCmd.GetError() << Endl;
        DeletePathIfExistsSafe(tmpPathToBinary);
        return EXIT_FAILURE;
    }
    Cout << checkCmd.GetOutput();

    TFsPath currentBinary = GetCurrentBinaryPath();
    TFsPath canonicalBinary = NLocalPaths::GetCanonicalBinaryPath();
    TFsPath legacyBinary = NLocalPaths::GetLegacyBinaryPath();
    const bool runningFromLegacy = (currentBinary == legacyBinary);
    TFsPath targetBinaryPath = runningFromLegacy ? canonicalBinary : currentBinary;
    EnsureParentDirExists(targetBinaryPath);
    TFsPath stagingBinary = CreateStagingBinaryPath(targetBinaryPath);
    try {
        CopyBinaryFile(tmpPathToBinary, stagingBinary);
#ifndef _win32_
        int chmodResult = Chmod(stagingBinary.GetPath().data(), MODE0777);
        if (chmodResult != 0) {
            Cerr << "Couldn't make binary \"" << stagingBinary.GetPath() << "\" executable. Error code: "
                << chmodResult << Endl;
        }
#endif
    } catch (const yexception& ex) {
        Cerr << "Failed to prepare new binary: " << ex.what() << Endl;
        DeletePathIfExistsSafe(stagingBinary);
        DeletePathIfExistsSafe(tmpPathToBinary);
        return EXIT_FAILURE;
    }
    if (!runningFromLegacy) {
#ifdef _win32_
        TFsPath binaryNameOld(TStringBuilder() << targetBinaryPath.GetPath() << "_old");
    binaryNameOld.Fix();
    DeletePathIfExistsSafe(binaryNameOld);
        targetBinaryPath.RenameTo(binaryNameOld);
    Cout << "Old binary renamed to " << binaryNameOld.GetPath() << Endl;
#else
        DeletePathIfExistsSafe(targetBinaryPath);
    Cout << "Old binary removed" << Endl;
#endif
    }

    stagingBinary.RenameTo(targetBinaryPath);
    Cout << "New binary installed to " << targetBinaryPath.GetPath() << Endl;
    DeletePathIfExistsSafe(tmpPathToBinary);

    if (runningFromLegacy) {
        RemoveLegacyBinaryFile(currentBinary);
#if defined(_win32_)
        UpdateWindowsPathReferences();
#else
        UpdateUnixPathReferences();
#endif
        Cout << "Legacy installation migrated to canonical path " << targetBinaryPath.GetPath() << Endl;
    }

    return EXIT_SUCCESS;
}

void TYdbUpdater::SetCheckVersion(bool value) {
    SetConfigValue("check_version", value);
}

template <typename T>
void TYdbUpdater::SetConfigValue(const TString& name, const T& value) {
    Config[name] = value;
    ConfigChanged = true;
}

void TYdbUpdater::LoadConfig() {
    TFsPath configFilePath = NLocalPaths::GetUpdateStateFile();
    configFilePath.Fix();
    try {
        if (configFilePath.Exists()) {
            TUnbufferedFileInput input(configFilePath);
            TString rawConfig = input.ReadAll();
            if (NJson::ReadJsonTree(rawConfig, &Config, /* throwOnError */ true)) {
                return;
            }
        }
    }
    catch (const yexception& e) {
        Cerr << "(!) Couldn't load config from file \"" << configFilePath.GetPath()
            << "\". Using default. " << e.what() << Endl;
    }
    Config = NJson::TJsonValue();
}

void TYdbUpdater::SaveConfig() {
    try {
        TFsPath configFilePath = NLocalPaths::GetUpdateStateFile();
        configFilePath.Fix();
        if (!configFilePath.Parent().Exists()) {
            configFilePath.Parent().MkDirs();
        }
        TFileOutput resultConfigFile(configFilePath);
        NJsonWriter::TBuf buf;
        buf.WriteJsonValue(&Config);
        resultConfigFile << buf.Str();
    }
    catch (const yexception& e) {
        Cerr << "(!) Couldn't save config to file. " << e.what() << Endl;
    }
}

bool TYdbUpdater::IsCheckEnabled() {
    if (Config.Has("check_version") && !Config["check_version"].GetBoolean()) {
        return false;
    }
    return true;
}

bool TYdbUpdater::IsTimeToCheckForUpdate() {
    if (Config.Has("last_check")) {
        TInstant lastCheck = TInstant::Seconds(Config["last_check"].GetUInteger());
        // Do not check for updates more than once every 24h
        if (lastCheck + TDuration::Days(1) > TInstant::Now()) {
            return false;
        }
    }
    return true;
}

bool TYdbUpdater::GetLatestVersion() {
    if (LatestVersion) {
        return true;
    }
    std::string versionUrl = StorageUrl + "/stable";
    TShellCommand curlCmd(TStringBuilder() << "curl --silent --max-time 10 " << versionUrl);
    curlCmd.Run().Wait();

    if (curlCmd.GetExitCode() == 0) {
        LatestVersion = StripString(curlCmd.GetOutput());
        SetConfigValue("last_check", TInstant::Now().Seconds());
        return true;
    }
    Cerr << "(!) Couldn't get latest version from url \"" << versionUrl << "\". " << curlCmd.GetError() << Endl
        << "You can disable further version checks with 'ydb version --disable-checks' command." << Endl;
    return false;
}

void TYdbUpdater::PrintUpdateMessageIfNeeded(bool forceVersionCheck) {
    if (forceVersionCheck) {
        Cerr << "Force checking if there is a newer version..." << Endl;
    } else if (!IsCheckEnabled() || !IsTimeToCheckForUpdate()) {
        return;
    }
    if (!GetLatestVersion()) {
        return;
    }
    if (MyVersion != LatestVersion) {
        NColorizer::TColors colors = NConsoleClient::AutoColors(Cerr);
        Cerr << colors.Green() << "(!) New version of YDB CLI is available. Current version: \"" << MyVersion
            << "\", Latest recommended version available: \"" << LatestVersion << "\". Run 'ydb update' command for update. "
            << "You can also disable further version checks with 'ydb version --disable-checks' command."
            << colors.OldColor() << Endl;
    } else if (forceVersionCheck) {
        NColorizer::TColors colors = NConsoleClient::AutoColors(Cerr);
        Cerr << colors.GreenColor() << "Current version is up to date"
            << colors.OldColor() << Endl;
    }
}

}
}
