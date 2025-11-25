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
#include <library/cpp/colorizer/output.h>
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

    TFsPath CreateCacheDownloadPath() {
        TFsPath dir = NLocalPaths::GetUpdateCacheDir();
        TFsPath path = dir.Child(TStringBuilder() << "ydb-update-" << CreateGuidAsString());
        path.Fix();
        EnsureParentDirExists(path.Fix());
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

    void EnsureParentDirExists(const TFsPath& path) {
        TFsPath parent = path.Parent();
        if (!parent.Exists()) {
            parent.MkDirs();
        }
    }

#if defined(_win32_)
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
#endif

    void UpdateLegacyPathReferences(const TFsPath& canonicalBinaryPath) {
        TFsPath canonicalDir = canonicalBinaryPath.Parent();
#if defined(_win32_)
        TString legacyEntry = NLocalPaths::GetLegacyBinaryPath().Parent().GetPath();
        TString canonicalEntry = canonicalDir.GetPath();
        TVector<TString> elements = StringSplitter(GetEnv("PATH")).Split(';').ToList<TString>();
        bool replaced = false;
        for (TString& element : elements) {
            TString trimmed = StripString(element);
            if (trimmed.empty()) {
                continue;
            }
            if (trimmed == legacyEntry) {
                element = canonicalEntry;
                replaced = true;
            }
        }
        if (replaced) {
            TString newPath = JoinSeq(";", elements);
            SetEnv("PATH", newPath);
            bool persisted = PersistWindowsPath(newPath);
            if (persisted) {
                Cerr << "Updated user PATH: replaced \"" << legacyEntry << "\" with \"" << canonicalEntry
                    << "\". Please restart your shell to apply changes." << Endl;
            } else {
                Cerr << "Updated PATH in current session, but failed to persist changes. "
                    << "Please update PATH manually to include " << canonicalEntry << Endl;
            }
        } else {
            Cerr << "Legacy PATH entry was not updated automatically. "
                << "Please ensure " << canonicalEntry << " is present in the PATH." << Endl;
        }
#else
        TFsPath helper = NLocalPaths::GetLegacyPathHelperScript();
        if (!helper.Exists()) {
            return;
        }
        try {
            TFsPath helperDir = helper.Parent();
            if (!helperDir.Exists()) {
                helperDir.MkDirs();
            }
            TFileOutput out(helper);
            out << "# Updated by ydb update: legacy path helper now points to canonical YDB CLI location" << Endl;
            out << "bin_path=\"" << canonicalDir.GetPath() << "\"" << Endl;
            out << "case \":${PATH}:\" in" << Endl;
            out << "    *:\"${bin_path}\":*) ;;" << Endl;
            out << "    *) export PATH=\"${bin_path}:${PATH}\" ;;" << Endl;
            out << "esac" << Endl;
            out.Finish();
            Cerr << "Updated legacy PATH helper script " << helper.GetPath()
                << " to use " << canonicalDir.GetPath()
                << ". Please restart your shell to apply changes." << Endl;
        } catch (const yexception& e) {
            Cerr << "Failed to update legacy PATH helper script: " << e.what() << Endl;
        }
#endif
    }

    void RemoveLegacyBinaryFile(const TFsPath& legacyBinary) {
        if (!legacyBinary.Exists()) {
            return;
        }
#if defined(_win32_)
        TFsPath backup(TStringBuilder() << legacyBinary.GetPath() << "_old");
        backup.Fix();
        backup.DeleteIfExists();
        try {
            legacyBinary.RenameTo(backup);
            Cerr << "Legacy binary moved to " << backup.GetPath() << ". You can delete it after closing current session." << Endl;
        } catch (const yexception& e) {
            if (!legacyBinary.DeleteIfExists()) {
                Cerr << "Failed to remove legacy binary " << legacyBinary.GetPath() << ": " << e.what() << Endl;
            } else {
                Cerr << "Removed legacy binary " << legacyBinary.GetPath() << Endl;
            }
        }
#else
        legacyBinary.DeleteIfExists();
        Cerr << "Removed legacy binary " << legacyBinary.GetPath() << Endl;
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
    const TString downloadUrl = TStringBuilder() << StorageUrl << '/' << LatestVersion << '/' << osVersion
        << '/' << osArch << '/' << NLocalPaths::YdbBinaryName;
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
        tmpPathToBinary.DeleteIfExists();
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
        stagingBinary.DeleteIfExists();
        tmpPathToBinary.DeleteIfExists();
        return EXIT_FAILURE;
    }
    if (!runningFromLegacy) {
#ifdef _win32_
        TFsPath binaryNameOld(TStringBuilder() << targetBinaryPath.GetPath() << "_old");
        binaryNameOld.Fix();
        binaryNameOld.DeleteIfExists();
        targetBinaryPath.RenameTo(binaryNameOld);
        Cout << "Old binary renamed to " << binaryNameOld.GetPath() << Endl;
#else
        targetBinaryPath.DeleteIfExists();
        Cout << "Old binary removed" << Endl;
#endif
    }

    stagingBinary.RenameTo(targetBinaryPath);
    Cout << "New binary installed to " << targetBinaryPath.GetPath() << Endl;
    tmpPathToBinary.DeleteIfExists();

    if (runningFromLegacy) {
        RemoveLegacyBinaryFile(currentBinary);
        UpdateLegacyPathReferences(targetBinaryPath);
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
