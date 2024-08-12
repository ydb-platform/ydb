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

namespace NYdb {
namespace NConsoleClient {

const char* VersionResourceName = "version.txt";

namespace {
#if defined(_darwin_)
    const TString OsVersion = "darwin";
    const TString BinaryName = "ydb";
    const TString HomeDir = GetHomeDir();
#elif defined(_win32_)
    const TString OsVersion = "windows";
    const TString BinaryName = "ydb.exe";
    const TString HomeDir = GetEnv("USERPROFILE");
#else
    const TString OsVersion = "linux";
    const TString BinaryName = "ydb";
    const TString HomeDir = GetHomeDir();
#endif
    const TString DefaultConfigFile = TStringBuilder() << HomeDir << "/ydb/bin/config.json";
    const TString DefaultTempFile = TStringBuilder() << HomeDir << "/ydb/install/" << BinaryName;
    const TString StorageUrl = "https://storage.yandexcloud.net/yandexcloud-ydb/release/";
    const TString VersionUrl = TStringBuilder() << StorageUrl << "stable";
}

TYdbUpdater::TYdbUpdater()
    : MyVersion(StripString(NResource::Find(TStringBuf(VersionResourceName))))
{
    LoadConfig();
}

TYdbUpdater::~TYdbUpdater() {
    if (ConfigChanged) {
        SaveConfig();
    }
}

bool TYdbUpdater::CheckIfUpdateNeeded(bool forceRequest) {
    if (!forceRequest && !IsCheckEnabled()) {
        return false;
    }
    if (!forceRequest && Config.Has("outdated") && Config["outdated"].GetBoolean()) {
        return true;
    }
    if (!forceRequest && !IsTimeToCheckForUpdate()) {
        return false;
    }

    SetConfigValue("last_check", TInstant::Now().Seconds());

    if (GetLatestVersion()) {
        bool isOutdated = MyVersion != LatestVersion;
        SetConfigValue("outdated", isOutdated);
        return isOutdated;
    }
    return false;
}

int TYdbUpdater::Update(bool forceUpdate) {
    if (!GetLatestVersion()) {
        return EXIT_FAILURE;
    }
    if (!CheckIfUpdateNeeded(/*forceRequest*/ true) && !forceUpdate) {
        Cerr << "Current version: \"" << MyVersion << "\". Latest version Available: \"" << LatestVersion
            << "\". No need to update. Use '--force' option to update anyway." << Endl;
        return EXIT_FAILURE;
    }

    TFsPath tmpPathToBinary(DefaultTempFile);
    tmpPathToBinary.Fix();
    TString corrPath = tmpPathToBinary.GetPath();
    if (!tmpPathToBinary.Parent().Exists()) {
        tmpPathToBinary.Parent().MkDirs();
    }
    const TString DownloadUrl = TStringBuilder() << StorageUrl << LatestVersion << "/" << OsVersion << "/amd64/"
        << BinaryName;
    Cout << "Downloading binary from url " << DownloadUrl << Endl;
    TShellCommand curlCmd(TStringBuilder() << "curl --max-time 60 " << DownloadUrl << " -o " << tmpPathToBinary.GetPath());
    curlCmd.Run().Wait();
    if (curlCmd.GetExitCode() != 0) {
        Cerr << "Failed to download from url \"" << DownloadUrl << "\". " << curlCmd.GetError() << Endl;
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

    SetConfigValue("outdated", false);
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
    TFsPath configFilePath(DefaultConfigFile);
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
        TFsPath configFilePath(DefaultConfigFile);
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

    TShellCommand curlCmd(TStringBuilder() << "curl --silent --max-time 10 " << VersionUrl);
    curlCmd.Run().Wait();

    if (curlCmd.GetExitCode() == 0) {
        LatestVersion = StripString(curlCmd.GetOutput());
        return true;
    }
    Cerr << "(!) Couldn't get latest version from url \"" << VersionUrl << "\". " << curlCmd.GetError() << Endl;
    return false;
}

}
}
