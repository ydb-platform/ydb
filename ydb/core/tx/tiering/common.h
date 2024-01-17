#pragma once
#include <ydb/core/base/events.h>

#include <ydb/library/accessor/accessor.h>

#include <ydb/library/actors/core/events.h>

namespace NKikimr::NColumnShard::NTiers {

enum EEvents {
    EvTierCleared = EventSpaceBegin(TKikimrEvents::ES_TIERING),
    EvSSFetchingResult,
    EvSSFetchingProblem,
    EvTimeout,
    EvTiersManagerReadyForUsage,
    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_TIERING), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_TIERING)");
}
