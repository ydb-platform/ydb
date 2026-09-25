#pragma once

#include <util/stream/str.h>

namespace NKikimr {

    struct TCompactionPriority {
        double MaxRank = 0.0;
        bool EmergencyMode = false;

        bool operator>(const TCompactionPriority &other) const {
            if (EmergencyMode != other.EmergencyMode) {
                return EmergencyMode;
            }
            return MaxRank > other.MaxRank;
        }

        TString ToString() const {
            TStringStream str;
            str << "{MaxRank# " << MaxRank << " EmergencyMode# " << EmergencyMode << "}";
            return str.Str();
        }
    };

} // NKikimr
