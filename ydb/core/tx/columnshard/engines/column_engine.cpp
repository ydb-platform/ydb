#include "column_engine.h"
#include <util/stream/output.h>

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

template <>
void Out<NKikimr::NOlap::TColumnEngineChanges>(IOutputStream& out, TTypeTraits<NKikimr::NOlap::TColumnEngineChanges>::TFuncParam changes) {
    if (ui32 switched = changes.SwitchedPortions.size()) {
        out << "switch " << switched << " portions";
        for (auto& portionInfo : changes.SwitchedPortions) {
            out << portionInfo;
        }
        out << "; ";
    }
    if (ui32 added = changes.AppendedPortions.size()) {
        out << "add " << added << " portions";
        for (auto& portionInfo : changes.AppendedPortions) {
            out << portionInfo;
        }
        out << "; ";
    }
    if (ui32 moved = changes.PortionsToMove.size()) {
        out << "move " << moved << " portions";
        for (auto& [portionInfo, granule] : changes.PortionsToMove) {
            out << portionInfo << " (to " << granule << ")";
        }
        out << "; ";
    }
    if (ui32 evicted = changes.PortionsToEvict.size()) {
        out << "evict " << evicted << " portions";
        for (auto& [portionInfo, evictionFeatures] : changes.PortionsToEvict) {
            out << portionInfo << " (to " << evictionFeatures.TargetTierName << ")";
        }
        out << "; ";
    }
    if (ui32 dropped = changes.PortionsToDrop.size()) {
        out << "drop " << dropped << " portions";
        for (auto& portionInfo : changes.PortionsToDrop) {
            out << portionInfo;
        }
    }
}
