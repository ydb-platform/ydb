#include "column_engine.h"

namespace NKikimr::NOlap {

TString TTiersInfo::GetDebugString() const {
    TStringBuilder sb;
    sb << "column=" << Column << ";";
    for (auto&& i : TierBorders) {
        sb << "tname=" << i.TierName << ";eborder=" << i.EvictBorder << ";";
    }
    return sb;
}

}
