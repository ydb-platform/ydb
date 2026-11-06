#pragma once

#include "intervals/intervals.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/common/path_id.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>

#include <memory>
#include <optional>
#include <vector>

namespace NActors {
class IActor;
struct TActorContext;
}   // namespace NActors

namespace NKikimr {
class TTabletStorageInfo;
}

namespace NKikimr::NTabletFlatExecutor {
class ITransaction;
}

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
class ISnapshotSchema;
}   // namespace NKikimr::NOlap

namespace NKikimr::NColumnShard {

inline constexpr ui64 CutHistoryJournalLimit = 10;

bool IsCutHistoryEnabled();

class TCSCounters;
class TColumnShard;

NTabletFlatExecutor::ITransaction* CreateTxSaveCutHistoryRequests(
    TColumnShard* self, NFindEmptyHistoryIntervals::THistoryIntervals::TIntervals&& intervals, TInstant scanFinished);

struct TFindEmptyHistoryIntervalsContext {
    struct TPortion {
        TInternalPathId PathId;
        ui64 PortionId;
        std::shared_ptr<NOlap::ISnapshotSchema> Schema;
        ui64 AccessorMemory;
    };

    ui64 TabletId;
    NActors::TActorId TabletActorId;
    NFindEmptyHistoryIntervals::THistoryIntervals::TIntervals Intervals;
    std::vector<TPortion> Portions;
};

TFindEmptyHistoryIntervalsContext BuildFindEmptyHistoryIntervalsContext(const TTabletStorageInfo& storageInfo, ui32 generation,
    ui32 requiredBarrierGeneration, const NOlap::TColumnEngineForLogs* index, NActors::TActorId tabletActorId);

NActors::IActor* CreateFindEmptyHistoryIntervalsActor(TFindEmptyHistoryIntervalsContext&& context,
    const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& config, const TCSCounters& counters, TInstant started);

struct TEmptyHistoryIntervalsScanResult {
    NFindEmptyHistoryIntervals::THistoryIntervals::TIntervals Intervals;
    TInstant Started;
    TInstant Finished;
    bool Aborted = false;
};

struct TEvFindEmptyHistoryIntervals {
    enum EEv {
        EvContinue = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvAccessorsReady,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE));

    struct TEvContinue: NActors::TEventLocal<TEvContinue, EvContinue> {};

    struct TEvScanFinished: NActors::TEventLocal<TEvScanFinished, TEvColumnShard::EvFindEmptyHistoryIntervalsScanFinished> {
        TEmptyHistoryIntervalsScanResult Result;

        explicit TEvScanFinished(TEmptyHistoryIntervalsScanResult&& result)
            : Result(std::move(result))
        {
        }
    };
};

class TFindEmptyHistoryIntervals {
    NActors::TActorId ScanActor;

public:
    struct TStartContext {
        const TTabletStorageInfo& StorageInfo;
        ui32 Generation;
        const NOlap::TColumnEngineForLogs* Index;
        NActors::TActorId TabletActorId;
        const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& Config;
        const TCSCounters& Counters;
    };

    NActors::TActorId Start(ui32 completedBarrierGeneration, const TStartContext& context, const NActors::TActorContext& ctx);
    std::optional<TEmptyHistoryIntervalsScanResult> HandleFinish(
        TEvFindEmptyHistoryIntervals::TEvScanFinished::TPtr& ev, bool canCutHistory, const TCSCounters& counters);
};

}   // namespace NKikimr::NColumnShard
