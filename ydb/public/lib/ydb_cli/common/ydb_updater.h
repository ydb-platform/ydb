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

private:
    void LoadConfig();
    void SaveConfig();
    bool IsTimeToCheckForUpdate();
    bool IsVersionUpToDate();
    bool GetLatestVersion();

    NJson::TJsonValue Config;
    TString MyVersion;
    TString LatestVersion;
    bool ConfigChanged = false;
};

}
}
