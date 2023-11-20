#include "settings.h"

namespace NYql::NPushdown {

void TSettings::Enable(ui64 flagsMask, bool set) {
    if (set) {
        FeatureFlags |= flagsMask;
    } else {
        FeatureFlags &= ~flagsMask;
    }
}

bool TSettings::IsEnabled(EFeatureFlag flagMask) const {
    return (FeatureFlags & flagMask) != 0;
}

} // namespace NYql::NPushdown
