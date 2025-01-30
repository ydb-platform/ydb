#include "config.h"
#include <util/string/builder.h>

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
    if (config.HasWorkersCountDouble()) {
        WorkersCount = config.GetWorkersCountDouble();
    } else if (config.HasWorkersCount()) {
        WorkersCount = config.GetWorkersCount();
    }
    if (config.HasDefaultFractionOfThreadsCount()) {
        DefaultFractionOfThreadsCount = config.GetDefaultFractionOfThreadsCount();
    }
    return true;
}

ui32 TConfig::GetWorkersCountForConveyor(const ui32 poolThreadsCount) const {
    if (WorkersCount) {
        return std::ceil(*WorkersCount);
    } else if (DefaultFractionOfThreadsCount) {
        return Min<ui32>(poolThreadsCount, Max<ui32>(1, *DefaultFractionOfThreadsCount * poolThreadsCount));
    } else {
        return poolThreadsCount;
    }
}

TString TConfig::DebugString() const {
    TStringBuilder sb;
    if (WorkersCount) {
        sb << "WorkersCount=" << *WorkersCount << ";";
    }
    if (DefaultFractionOfThreadsCount) {
        sb << "DefaultFractionOfThreadsCount=" << *DefaultFractionOfThreadsCount << ";";
    }
    sb << "QueueSizeLimit=" << QueueSizeLimit << ";";
    sb << "Enabled=" << EnabledFlag << ";";
    return sb;
}

double TConfig::GetWorkerCPUUsage(const ui32 workerIdx) const {
    if (!WorkersCount) {
        return 1;
    }
    double wholePart;
    const double fractionalPart = std::modf(*WorkersCount, &wholePart);
    if (workerIdx + 1 <= wholePart) {
        return 1;
    } else {
        return fractionalPart;
    }
}

}
