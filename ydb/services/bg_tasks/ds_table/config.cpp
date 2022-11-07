#include "config.h"
#include <ydb/core/base/appdata.h>

namespace NKikimr::NBackgroundTasks {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TBackgroundTasksConfig& config) {
    EnabledFlag = config.GetEnabled();
    if (!RequestConfig.DeserializeFromProto(config.GetRequestConfig())) {
        return false;
    }
    PullPeriod = TDuration::Seconds(config.GetPullPeriodSeconds());
    PingPeriod = TDuration::Seconds(config.GetPingPeriodSeconds());
    PingCheckPeriod = TDuration::Seconds(config.GetPingCheckPeriodSeconds());
    MaxInFlight = config.GetMaxInFlight();
    InternalTablePath = config.GetInternalTablePath();
    return true;
}

TString TConfig::GetTablePath() const {
    return AppData()->TenantName + "/" + InternalTablePath;
}

}
