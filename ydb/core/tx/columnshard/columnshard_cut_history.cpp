#include "columnshard_impl.h"

#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/sessions.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NColumnShard {

class TColumnShard::TCutHistoryResultProcessor: public NOlap::IMetadataAccessorResultProcessor {
    TColumnShard* const Shard;

    void DoApplyResult(
        NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult>&& result, NOlap::TColumnEngineForLogs&) override {
        Shard->FinishCutHistoryBatch(result.GetValue());
    }

public:
    explicit TCutHistoryResultProcessor(TColumnShard* shard)
        : Shard(shard)
    {
    }
};

void TColumnShard::StartCutHistoryScan(const TActorContext& ctx) {
    if (!AppData()->FeatureFlags.GetEnableCutHistory() || !SharingSessionsManager->CanCutHistory() || CutHistoryScan) {
        return;
    }
    TCutHistoryScan scan;
    scan.Started = ctx.Now();
    for (ui32 channel = 2; channel < Info()->Channels.size(); ++channel) {
        const auto& history = Info()->Channels[channel].History;
        for (size_t i = 0; i + 1 < history.size(); ++i) {
            scan.Intervals.push_back({ channel, history[i].FromGeneration, history[i + 1].FromGeneration, history[i].GroupID });
        }
    }
    if (scan.Intervals.empty()) {
        return;
    }
    if (HasIndex()) {
        for (const auto& [pathId, granule] : GetIndexAs<NOlap::TColumnEngineForLogs>().GetTables()) {
            for (const auto& [portionId, _] : granule->GetPortions()) {
                scan.Portions.emplace_back(pathId, portionId);
            }
            for (const auto& [_, portion] : granule->GetInsertedPortions()) {
                scan.Portions.emplace_back(pathId, portion->GetPortionId());
            }
        }
    }
    Sort(scan.Portions);
    CutHistoryScan = std::move(scan);
    ctx.Schedule(TDuration::MilliSeconds(1), new TEvPrivate::TEvContinueCutHistory());
}

void TColumnShard::Handle(TEvPrivate::TEvContinueCutHistory::TPtr&, const TActorContext& ctx) {
    if (!CutHistoryScan || CutHistoryScan->Pending || CutHistoryScan->Finished) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        CutHistoryScan.reset();
        return;
    }
    auto& scan = *CutHistoryScan;
    if (scan.Position == scan.Portions.size()) {
        scan.Finished = ctx.Now();
        std::vector<std::pair<TInternalPathId, ui64>>().swap(scan.Portions);
        Counters.GetCSCounters().OnCutHistoryScanFinished(*scan.Finished - scan.Started);
        TryCutHistory(ctx);
        return;
    }
    const auto& index = GetIndexAs<NOlap::TColumnEngineForLogs>();
    auto request = std::make_shared<NOlap::TDataAccessorsRequest>(NOlap::NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
    const size_t end = Min(scan.Position + 32, scan.Portions.size());
    ui64 memory = 0;
    while (scan.Position < end && (request->IsEmpty() || memory < 8 * (1 << 20))) {
        const auto [pathId, portionId] = scan.Portions[scan.Position++];
        const auto granule = index.GetGranuleOptional(pathId);
        const auto portion = granule ? granule->GetPortionOptional(portionId, false) : nullptr;
        // In-memory absence follows cleanup Complete and publication of its GC bookkeeping.
        if (portion) {
            memory += portion->PredictAccessorsMemory(portion->GetSchema(index.GetVersionedIndex()));
            request->AddPortion(portion);
        }
    }
    if (request->IsEmpty()) {
        ctx.Schedule(TDuration::MilliSeconds(1), new TEvPrivate::TEvContinueCutHistory());
        return;
    }
    scan.Pending = request->GetSize();
    SubmitMetadataRequest(NOlap::TCSMetadataRequest(request, std::make_shared<TCutHistoryResultProcessor>(this)));
}

void TColumnShard::FinishCutHistoryBatch(const NOlap::TDataAccessorsResult& result) {
    if (!CutHistoryScan) {
        return;
    }
    auto& scan = *CutHistoryScan;
    if (!SharingSessionsManager->CanCutHistory() || result.HasErrors() || result.HasRemovedData() ||
        result.GetPortions().size() != scan.Pending) {
        CutHistoryScan.reset();
        return;
    }
    const auto& index = GetIndexAs<NOlap::TColumnEngineForLogs>();
    for (const auto& [_, accessor] : result.GetPortions()) {
        const auto blobs = accessor->GetBlobIdsByStorage(accessor->GetPortionInfo().GetSchema(index.GetVersionedIndex())->GetIndexInfo());
        const auto* defaults = blobs.FindPtr(NOlap::IStoragesManager::DefaultStorageId);
        if (!defaults) {
            continue;
        }
        for (const auto& blob : *defaults) {
            const auto& id = blob.GetLogoBlobId();
            if (id.TabletID() != TabletID()) {
                continue;
            }
            for (auto& interval : scan.Intervals) {
                if (id.Channel() == interval.Channel && interval.From <= id.Generation() && id.Generation() < interval.To) {
                    interval.NonEmpty = true;
                }
            }
        }
    }
    scan.Pending = 0;
    Schedule(TDuration::MilliSeconds(1), new TEvPrivate::TEvContinueCutHistory());
}

void TColumnShard::TryCutHistory(const TActorContext& ctx) {
    if (!CutHistoryScan || !CutHistoryScan->Finished) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        CutHistoryScan.reset();
        return;
    }
    const auto storage = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(StoragesManager->GetDefaultOperator());
    if (!storage || !LauncherID()) {
        return;
    }
    for (auto& interval : CutHistoryScan->Intervals) {
        if (interval.NonEmpty || interval.Sent || interval.Channel >= Info()->Channels.size()) {
            continue;
        }
        const auto& history = Info()->Channels[interval.Channel].History;
        auto entry = FindIf(history, [&](const auto& item) {
            return item.FromGeneration == interval.From;
        });
        if (entry == history.end() || entry->GroupID != interval.Group || entry + 1 == history.end() ||
            (entry + 1)->FromGeneration != interval.To) {
            continue;
        }
        if (!storage->CanCutHistory(interval.Channel, interval.From, interval.To)) {
            continue;
        }
        auto event = std::make_unique<TEvTablet::TEvCutTabletHistory>();
        event->Record.SetTabletID(TabletID());
        event->Record.SetChannel(interval.Channel);
        event->Record.SetFromGeneration(interval.From);
        event->Record.SetGroupID(interval.Group);
        if (RecentCutHistoryRequests.size() == 64) {
            RecentCutHistoryRequests.pop_front();
        }
        RecentCutHistoryRequests.emplace_back(TStringBuilder() << ctx.Now() << " recipient=" << LauncherID() << " toGeneration=" << interval.To
                                                               << " " << event->Record.ShortDebugString());
        interval.Sent = true;
        Counters.GetCSCounters().OnCutHistoryRequestSent(ctx.Now() - *CutHistoryScan->Finished);
        ctx.Send(LauncherID(), event.release());
    }
}

}   // namespace NKikimr::NColumnShard
