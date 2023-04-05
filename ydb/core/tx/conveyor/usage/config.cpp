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
    return true;
}
}
