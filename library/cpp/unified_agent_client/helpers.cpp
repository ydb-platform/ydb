#include "helpers.h"

namespace NUnifiedAgent::NPrivate {
    bool IsUtf8(const THashMap<TString, TString>& meta) {
        for (const auto& p: meta) {
            if (!IsUtf(p.first) || !IsUtf(p.second)) {
                return false;
            }
        }
        return true;
    }
}
