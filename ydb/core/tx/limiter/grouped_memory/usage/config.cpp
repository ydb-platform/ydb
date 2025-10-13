#include "config.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TGroupedMemoryLimiterConfig& config) {
    CountBuckets = config.GetCountBuckets() ? config.GetCountBuckets() : 1;

    if (config.HasMemoryLimit()) {
        MemoryLimit = config.GetMemoryLimit() / CountBuckets;
    }

    if (config.HasHardMemoryLimit()) {
        HardMemoryLimit = config.GetHardMemoryLimit() / CountBuckets;
    }

    Enabled = config.GetEnabled();

    return true;
}

TString TConfig::DebugString() const {
    TStringBuilder sb;
    sb << "MemoryLimit=" << MemoryLimit.value_or(0)
       << ";HardMemoryLimit=" << HardMemoryLimit.value_or(0)
       << ";Enabled=" << Enabled
       << ";CountBuckets=" << CountBuckets
       << ";";
    return sb;
}

}
