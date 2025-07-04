#include "ydb_updater.h"

#include <library/cpp/json/writer/json.h>
#include <library/cpp/resource/resource.h>
#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/path.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/strip.h>
#include <util/system/env.h>
#include <util/system/execpath.h>
#include <util/system/shellcommand.h>
#include <library/cpp/colorizer/output.h>

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
    const TString binaryName = "ydb";
    const TString homeDir = GetHomeDir();
#elif defined(_win32_)
    const TString osVersion = "windows";
    const TString binaryName = "ydb.exe";
    const TString homeDir = GetEnv("USERPROFILE");
#else
    const TString osVersion = "linux";
    const TString binaryName = "ydb";
    const TString homeDir = GetHomeDir();
#endif
    const TString osArch = GetOsArchitecture();
    const TString defaultConfigFile = TStringBuilder() << homeDir << "/ydb/bin/config.json";
    const TString defaultTempFile = TStringBuilder() << homeDir << "/ydb/install/" << binaryName;
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

    TFsPath tmpPathToBinary(defaultTempFile);
    tmpPathToBinary.Fix();
    TString corrPath = tmpPathToBinary.GetPath();
    if (!tmpPathToBinary.Parent().Exists()) {
        tmpPathToBinary.Parent().MkDirs();
    }
    const TString downloadUrl = TStringBuilder() << StorageUrl << '/' << LatestVersion << '/' << osVersion
        << '/' << osArch << '/' << binaryName;
    Cout << "Downloading binary from url " << downloadUrl << Endl;
    TShellCommand curlCmd(TStringBuilder() << "curl --max-time 60 " << downloadUrl << " -o " << tmpPathToBinary.GetPath());
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

    TFsPath fsPathToBinary(GetExecPath());
#ifdef _win32_
    TFsPath binaryNameOld(TStringBuilder() << fsPathToBinary.GetPath() << "_old");
    binaryNameOld.Fix();
    binaryNameOld.DeleteIfExists();
    fsPathToBinary.RenameTo(binaryNameOld);
    Cout << "Old binary renamed to " << binaryNameOld.GetPath() << Endl;
#else
    fsPathToBinary.DeleteIfExists();
    Cout << "Old binary removed" << Endl;
#endif
    tmpPathToBinary.RenameTo(fsPathToBinary);
    Cout << "New binary renamed to " << fsPathToBinary.GetPath() << Endl;

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
    TFsPath configFilePath(defaultConfigFile);
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
        TFsPath configFilePath(defaultConfigFile);
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
    Cerr << "(!) Couldn't get latest version from url \"" << versionUrl << "\". " << curlCmd.GetError() << Endl;
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
        NColorizer::TColors colors = NColorizer::AutoColors(Cerr);
        Cerr << colors.Green() << "(!) New version of YDB CLI is available. Current version: \"" << MyVersion
            << "\", Latest recommended version available: \"" << LatestVersion << "\". Run 'ydb update' command for update. "
            << "You can also disable further version checks with 'ydb version --disable-checks' command."
            << colors.OldColor() << Endl;
    } else if (forceVersionCheck) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cerr);
        Cerr << colors.GreenColor() << "Current version is up to date"
            << colors.OldColor() << Endl;
    }
}

}
}
