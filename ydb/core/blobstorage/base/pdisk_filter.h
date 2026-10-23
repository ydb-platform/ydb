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

inline bool MatchPDiskLocation(const NActors::TNodeLocation& actual,
        const NActorsInterconnect::TNodeLocation& required) {
    if (!IsValidPDiskFilterLocation(required)) {
        return false;
    }
    const auto& items = actual.GetItems();
    auto match = [&](NActors::TNodeLocation::TKeys::E key, const TString& value) {
        const auto it = std::lower_bound(items.begin(), items.end(), key,
            [](const auto& item, auto key) { return item.first < key; });
        return it != items.end() && it->first == key && it->second == value;
    };
#define MATCH_LOCATION_FIELD(NAME) \
    if (required.Has##NAME() && !match(NActors::TNodeLocation::TKeys::NAME, required.Get##NAME())) { \
        return false; \
    }
    MATCH_LOCATION_FIELD(BridgePileName)
    MATCH_LOCATION_FIELD(DataCenter)
    MATCH_LOCATION_FIELD(Module)
    MATCH_LOCATION_FIELD(Rack)
    MATCH_LOCATION_FIELD(Unit)
#undef MATCH_LOCATION_FIELD
    return true;
}

} // namespace NKikimr
