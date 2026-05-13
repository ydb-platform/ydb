#include "build_info.h"

#include <util/string/builder.h>

namespace NYdb {
namespace NConsoleClient {

void AppendYdbCliBuildInfo(TDriverConfig& driverConfig, const TYdbCliBuildInfo& buildInfo, const TString& commandTag) {
    if (buildInfo.DistributionName.empty() || buildInfo.Version.empty()) {
        return;
    }
    TString distSegment = buildInfo.DistributionName + "/" + buildInfo.Version;
    driverConfig.AppendBuildInfo(std::string(distSegment));

    if (commandTag) {
        TString cmdSegment = TStringBuilder() << "ydb-cli-" << commandTag << "/" << buildInfo.Version;
        driverConfig.AppendBuildInfo(std::string(cmdSegment));
    }
}

}
}
