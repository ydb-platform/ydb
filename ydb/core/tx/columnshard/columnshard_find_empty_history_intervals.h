#pragma once

#include "common/path_id.h"

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <compare>
#include <cstddef>
#include <map>
#include <optional>
#include <utility>
#include <vector>

namespace NKikimr::NColumnShard {

inline constexpr ui64 CutHistoryRequestLimit = 64;

struct THistoryIntervalKey {
    ui32 Channel = 0;
    ui32 From = 0;

    auto operator<=>(const THistoryIntervalKey&) const = default;
};

struct THistoryInterval {
    ui32 To = 0;
    ui32 Group = 0;
};

struct TEmptyHistoryIntervalsScan {
    std::map<THistoryIntervalKey, THistoryInterval> Intervals;
    std::vector<std::pair<TInternalPathId, ui64>> Portions;
    size_t Position = 0;
    size_t Pending = 0;
    NActors::TActorId PreparationActor;
    TInstant Started;
    std::optional<TInstant> Finished;
};

}   // namespace NKikimr::NColumnShard
