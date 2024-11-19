#include "config.h"
#include <util/string/builder.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TGroupedMemoryLimiterConfig& config) {
    if (config.HasMemoryLimit()) {
        MemoryLimit = config.GetMemoryLimit();
    }
    Enabled = config.GetEnabled();
    return true;
}

TString TConfig::DebugString() const {
    TStringBuilder sb;
    sb << "MemoryLimit=" << MemoryLimit << ";Enabled=" << Enabled << ";";
    return sb;
}

}
