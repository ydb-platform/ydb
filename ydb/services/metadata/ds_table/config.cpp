#include "config.h"
#include <util/generic/ylimits.h>

namespace NKikimr::NMetadataProvider {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TMetadataProviderConfig& config) {
    EnabledFlag = config.GetEnabled();
    if (!RequestConfig.DeserializeFromProto(config.GetRequestConfig())) {
        return false;
    }
    RefreshPeriod = TDuration::Seconds(config.GetRefreshPeriodSeconds());
    return true;
}

}
