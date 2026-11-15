#include "tracing.h"

namespace NKikimr::NTracing {

TString TTimestampInfo::GetTextMode(EMode mode) {
    switch (mode) {
    case ModeDisabled:
        return "Disabled";
    case ModeAbsolute:
    case ModeDefault:
        return "Absolute";
    case ModeFirst:
        return "Relative to first";
    case ModePrevious:
        return "Relative to previous";
    default:
        return "Unknown mode";
    }
}

TString TTimestampInfo::GetTextPrecision(EPrecision precision) {
    switch (precision) {
    case PrecisionSeconds:
        return "Seconds";
    case PrecisionMilliseconds:
    case PrecisionDefault:
        return "Milliseconds";
    case PrecisionMicroseconds:
        return "Microseconds";
    default:
        return "Unknown precision";
    }
}

ITraceSignal* TSignalFactory::Create(const ITrace::TSerializedSignal& signal) {
    TLightReadGuard g{ Lock };
    auto it = Factorymap.find(signal.Type);
    if (it != Factorymap.end()) {
        return it->second->Create(signal.Signal);
    }
    return nullptr;
}

} // namespace NKikimr::NTracing
