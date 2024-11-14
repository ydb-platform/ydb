#include "settings.h"

namespace NYql::NPushdown {

void TSettings::Enable(ui64 flagsMask, bool set) {
    if (set) {
        FeatureFlags |= flagsMask;
    } else {
        FeatureFlags &= ~flagsMask;
    }
}

void TSettings::EnableUdf(const TString& udfName) {
    EnabledUdfs.insert(udfName);
}

bool TSettings::IsEnabled(EFeatureFlag flagMask) const {
    return (FeatureFlags & flagMask) != 0;
}

bool TSettings::IsEnabledUdf(const TString& udfName) const {
    return EnabledUdfs.contains(udfName);
}

} // namespace NYql::NPushdown
