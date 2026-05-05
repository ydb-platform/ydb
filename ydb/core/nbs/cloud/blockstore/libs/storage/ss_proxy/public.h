#pragma once

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NYdb::NBS::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TSSProxyConfig
{
    int LogComponent = 0;
    ui32 PipeClientRetryCount = 0;
    TDuration PipeClientMinRetryTime;
    TDuration PipeClientMaxRetryTime;
    TString SchemeShardDir;
    TString PathDescriptionBackupFilePath;
    bool FallbackMode = false;
};

}   // namespace NYdb::NBS::NStorage
