#include "config.h"

#include <util/string/builder.h>

namespace NKikimr::NPrioritiesQueue {

bool TConfig::DeserializeFromProto(const NKikimrConfig::TPrioritiesQueueConfig& config) {
    if (!config.HasEnabled()) {
        EnabledFlag = true;
    } else {
        EnabledFlag = config.GetEnabled();
    }
    if (config.HasLimit()) {
        Limit = config.GetLimit();
    }
    return true;
}

TString TConfig::DebugString() const {
    TStringBuilder sb;
    sb << "Limit=" << Limit << ";";
    sb << "Enabled=" << EnabledFlag << ";";
    return sb;
}

}   // namespace NKikimr::NPrioritiesQueue
