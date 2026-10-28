#pragma once

#include <ydb/library/actors/core/interconnect.h>

#include <algorithm>

namespace NKikimr {

// Numeric legacy aliases do not preserve partial-field semantics when converted
// by TNodeLocation. Require the current string fields in placement filters.
inline bool IsValidPDiskFilterLocation(const NActorsInterconnect::TNodeLocation& location) {
    return !location.HasDataCenterNum() && !location.HasRoomNum() && !location.HasRackNum()
        && !location.HasBodyNum() && !location.HasBody() && location.unknown_fields().empty();
}

// Every field supplied in the required location must be present in the actual one with the same value.
// Both item lists are sorted by (key, value), so this is a subset check.
inline bool MatchPDiskLocation(const NActors::TNodeLocation& actual, const NActors::TNodeLocation& required) {
    const auto& x = actual.GetItems();
    const auto& y = required.GetItems();
    return std::includes(x.begin(), x.end(), y.begin(), y.end());
}

} // namespace NKikimr
