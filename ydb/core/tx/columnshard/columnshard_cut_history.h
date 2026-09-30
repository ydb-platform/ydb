#pragma once

#include "common/path_id.h"

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

namespace NKikimr::NColumnShard {

struct TCutHistoryInterval {
    ui32 Channel = 0;
    ui32 From = 0;
    ui32 To = 0;
    ui32 Group = 0;
    ui64 BlobReferences = 0;
    bool Attempted = false;
};

struct TCutHistoryScan {
    std::vector<TCutHistoryInterval> Intervals;
    std::vector<std::pair<TInternalPathId, ui64>> Portions;
    size_t Position = 0;
    size_t Pending = 0;
    NActors::TActorId PreparationActor;
    ui64 BootLastPortion = 0;
    std::pair<ui64, ui64> PreparationCursor{ 0, 0 };
    std::optional<std::pair<ui64, ui64>> PreparationMaxKey;
    bool PreparationPending = false;
    bool SavePending = false;
    bool RetryDelivery = false;
    TInstant Started;
    std::optional<TInstant> Finished;
};

}   // namespace NKikimr::NColumnShard
