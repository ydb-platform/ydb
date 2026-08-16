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

void TSettings::EnableMember(const TString& memberName) {
    EnabledMembers.insert(memberName);
}

bool TSettings::IsMemberEnabled(const TString& memberName) const {
    if (EnabledMembers.empty()) {
        return true;
    }
    return EnabledMembers.contains(memberName);
}

} // namespace NYql::NPushdown
