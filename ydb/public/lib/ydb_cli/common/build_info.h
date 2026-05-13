#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <util/generic/string.h>

namespace NYdb {
namespace NConsoleClient {

struct TYdbCliBuildInfo {
    TString DistributionName;
    TString Version;
};

// Appends distribution and (optionally) command build info segments to the driver config.
// commandTag: e.g. "import-file-csv", "interactive", or empty for no command segment.
void AppendYdbCliBuildInfo(TDriverConfig& driverConfig, const TYdbCliBuildInfo& buildInfo, const TString& commandTag);

}
}
