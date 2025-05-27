#include "settings.h"

namespace NYql::NPushdown {

void TSettings::Enable(ui64 flagsMask, bool set) {
    if (set) {
        FeatureFlags |= flagsMask;
    } else {
        FeatureFlags &= ~flagsMask;
    }
}

void TSettings::EnableFunction(const TString& functionName) {
    EnabledFunctions.insert(functionName);
}

bool TSettings::IsEnabled(EFeatureFlag flagMask) const {
    return (FeatureFlags & flagMask) != 0;
}

bool TSettings::IsEnabledFunction(const TString& functionName) const {
    return EnabledFunctions.contains(functionName);
}

} // namespace NYql::NPushdown
