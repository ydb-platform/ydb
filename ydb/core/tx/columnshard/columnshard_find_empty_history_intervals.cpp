#include "columnshard_impl.h"

#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NColumnShard {

void TColumnShard::OnFirstGC(ui32 completedBarrierGeneration, const TActorContext& ctx) {
    if (!IsCutHistoryEnabled()) {
        return;
    }
    FirstGCBarrierGeneration = completedBarrierGeneration;
    TryStartFindEmptyHistoryIntervals(ctx);
}

void TColumnShard::TryStartFindEmptyHistoryIntervals(const TActorContext& ctx) {
    if (!FirstGCBarrierGeneration || !HasIndex()) {
        return;
    }
    const auto& index = GetIndexAs<NOlap::TColumnEngineForLogs>();
    // the search walks the portions in memory: the blobs of a portion not loaded yet would look unused
    if (!index.AreAllPortionsLoaded(std::nullopt)) {
        return;
    }
    const ui32 completedBarrierGeneration = *std::exchange(FirstGCBarrierGeneration, std::nullopt);
    FindEmptyHistoryIntervals = std::make_unique<TFindEmptyHistoryIntervals>();
    const TFindEmptyHistoryIntervals::TStartContext context{ *Info(), Generation(), index, SelfId(), ColumnShardConfig->GetCutHistory(),
        Counters.GetCSCounters() };
    if (const auto actorId = FindEmptyHistoryIntervals->Start(completedBarrierGeneration, context, ctx)) {
        ActorsToStop.push_back(actorId);
    } else {
        FindEmptyHistoryIntervals.reset();
    }
}

void TColumnShard::Handle(TEvFindEmptyHistoryIntervals::TEvScanFinished::TPtr& ev, const TActorContext& ctx) {
    YDB_LOG_VERIFY(FindEmptyHistoryIntervals);
    if (auto result = FindEmptyHistoryIntervals->HandleFinish(ev, IsCutHistoryEnabled(), Counters.GetCSCounters())) {
        Execute(CreateTxSaveCutHistoryRequests(this, std::move(result->Intervals), result->Finished), ctx);
    } else {
        FindEmptyHistoryIntervals.reset();
    }
}

}   // namespace NKikimr::NColumnShard
