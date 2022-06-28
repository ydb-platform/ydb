#pragma once
#include <library/cpp/json/json_reader.h>

namespace NYdb {
namespace NConsoleClient {

extern const char* VersionResourceName;

class TYdbUpdater {
public:
    TYdbUpdater();
    ~TYdbUpdater();

    bool CheckIfUpdateNeeded(bool forceRequest = false);
    int Update(bool forceUpdate);
    void SetCheckVersion(bool value);

private:
    void LoadConfig();
    void SaveConfig();
    bool IsCheckEnabled();
    bool IsTimeToCheckForUpdate();
    bool IsVersionUpToDate();
    bool GetLatestVersion();

    template<typename T>
    void SetConfigValue(const TString& name, const T& value);

    NJson::TJsonValue Config;
    TString MyVersion;
    TString LatestVersion;
    bool ConfigChanged = false;
};

}
}
