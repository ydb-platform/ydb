#include "config.h"
#include <util/generic/ylimits.h>

namespace NKikimr::NMetadataProvider {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TMetadataProviderConfig& config) {
    EnabledFlag = config.GetEnabled();
    RefreshPeriod = TDuration::Seconds(config.GetRefreshPeriodSeconds());
    RetryPeriodStart = TDuration::Seconds(config.GetRetryPeriodStartSeconds());
    RetryPeriodFinish = TDuration::Seconds(config.GetRetryPeriodFinishSeconds());
    if (RetryPeriodStart > RetryPeriodFinish) {
        Cerr << "incorrect metadata provider config start/finish periods";
        std::swap(RetryPeriodStart, RetryPeriodFinish);
        return false;
    }
    return true;
}

TDuration TConfig::GetRetryPeriod(const ui32 retry) const {
    const double kff = 1.0 / Max<double>(1.0, retry);
    return RetryPeriodStart * kff + RetryPeriodFinish * (1 - kff);
}

}
