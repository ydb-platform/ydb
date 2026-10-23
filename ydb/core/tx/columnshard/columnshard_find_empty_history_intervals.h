#pragma once

#include "columnshard.h"

#include "find_empty_history_intervals/intervals.h"

#include <ydb/core/protos/config.pb.h>
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

namespace NKikimr::NOlap {
class TColumnEngineForLogs;
class ISnapshotSchema;
}   // namespace NKikimr::NOlap

namespace NKikimr::NOlap::NResourceBroker::NSubscribe {
class TSubscriberCounters;
class TTaskContext;
}   // namespace NKikimr::NOlap::NResourceBroker::NSubscribe

namespace NKikimr::NColumnShard {

inline constexpr ui64 CutHistoryRequestLimit = 64;

class TCSCounters;

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

NActors::IActor* CreateFindEmptyHistoryIntervalsActor(TFindEmptyHistoryIntervalsContext&& context, NActors::TActorId resourceSubscriber,
    const NOlap::NResourceBroker::NSubscribe::TTaskContext& taskContext, const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& config,
    const TCSCounters& counters, TInstant started);

struct TEmptyHistoryIntervalsScanResult {
    NFindEmptyHistoryIntervals::THistoryIntervals::TIntervals Intervals;
    TInstant Started;
    TInstant Finished;
    bool Aborted = false;
};

struct TEvFindEmptyHistoryIntervals {
    struct TEvContinue: NActors::TEventLocal<TEvContinue, TEvColumnShard::EvFindEmptyHistoryIntervalsContinue> {};

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
        NActors::TActorId ResourceSubscriber;
        const std::shared_ptr<NOlap::NResourceBroker::NSubscribe::TSubscriberCounters>& SubscribeCounters;
        const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& Config;
        const TCSCounters& Counters;
    };

    bool IsFinished() const {
        return !ScanActor;
    }

    NActors::TActorId Start(ui32 completedBarrierGeneration, const TStartContext& context, const NActors::TActorContext& ctx);
    std::optional<TEmptyHistoryIntervalsScanResult> HandleFinish(
        TEvFindEmptyHistoryIntervals::TEvScanFinished::TPtr& ev, bool canCutHistory, const TCSCounters& counters);
};

}   // namespace NKikimr::NColumnShard
