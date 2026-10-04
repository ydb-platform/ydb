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

struct THistoryInterval {
    ui32 Channel = 0;
    ui32 From = 0;
    ui32 To = 0;
    ui32 Group = 0;
    // Set by the portion walk when a live blob of this tablet still sits in the interval.
    bool HasBlobs = false;
    // One request per interval per generation; cleared on undelivered so the next wakeup retries.
    bool Attempted = false;
    // Set once the request is durable in the journal; until then the interval is only recorded.
    bool ReadyToSend = false;
};

struct TUnusedHistoryScan {
    std::vector<THistoryInterval> Intervals;
    std::vector<std::pair<TInternalPathId, ui64>> Portions;
    size_t Position = 0;
    size_t Pending = 0;
    NActors::TActorId PreparationActor;
    bool SavePending = false;
    bool RetryDelivery = false;
    bool WaitingForGC = false;
    TInstant Started;
    std::optional<TInstant> Finished;
};

}   // namespace NKikimr::NColumnShard
