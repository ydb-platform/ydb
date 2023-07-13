#include "config.h"

namespace NKikimr::NConveyor {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TConveyorConfig& config) {
    if (!config.HasEnabled()) {
        EnabledFlag = true;
    } else {
        EnabledFlag = config.GetEnabled();
    }
    if (config.HasQueueSizeLimit()) {
        QueueSizeLimit = config.GetQueueSizeLimit();
    }
    if (config.HasWorkersCount()) {
        WorkersCount = config.GetWorkersCount();
    }
    if (config.HasDefaultFractionOfThreadsCount()) {
        DefaultFractionOfThreadsCount = config.GetDefaultFractionOfThreadsCount();
    }
    return true;
}

ui32 TConfig::GetWorkersCountForConveyor(const ui32 poolThreadsCount) const {
    if (WorkersCount) {
        return *WorkersCount;
    } else if (DefaultFractionOfThreadsCount) {
        return Max<ui32>(1, *DefaultFractionOfThreadsCount * poolThreadsCount);
    } else {
        return poolThreadsCount;
    }
}

}
