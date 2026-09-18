#include "columnshard_impl.h"

#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/sessions.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

#include <iterator>

namespace NKikimr::NColumnShard {
namespace {
constexpr size_t CutHistoryScanBatchSize = 32;
constexpr ui64 CutHistoryScanMemoryTarget = 8_MB;
constexpr TDuration CutHistoryContinuationDelay = TDuration::MilliSeconds(1);
}   // namespace

class TColumnShard::TTxSaveCutHistoryRequests: public TTransactionBase<TColumnShard> {
    const std::vector<TCutHistoryRequest> Requests;

public:
    TTxSaveCutHistoryRequests(TColumnShard* self, std::vector<TCutHistoryRequest>&& requests)
        : TBase(self)
        , Requests(std::move(requests))
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        using T = Schema::CutHistoryRequests;
        NIceDb::TNiceDb db(txc.DB);
        auto last = db.Table<T>().Reverse().Range().Select<T::Sequence>();
        if (!last.IsReady()) {
            return false;
        }
        ui64 sequence = last.EndOfSet() ? 0 : last.GetValue<T::Sequence>();
        for (const auto& request : Requests) {
            db.Table<T>().Key(++sequence).Update(NIceDb::TUpdate<T::TabletID>(request.TabletID), NIceDb::TUpdate<T::Channel>(request.Channel),
                NIceDb::TUpdate<T::FromGeneration>(request.FromGeneration), NIceDb::TUpdate<T::GroupID>(request.GroupID),
                NIceDb::TUpdate<T::TimestampUs>(request.Timestamp.MicroSeconds()), NIceDb::TUpdate<T::Recipient>(request.Recipient),
                NIceDb::TUpdate<T::ToGeneration>(request.ToGeneration), NIceDb::TUpdate<T::SendingGeneration>(request.SendingGeneration));
            if (sequence > CutHistoryRequestLimit) {
                db.Table<T>().Key(sequence - CutHistoryRequestLimit).Delete();
            }
        }
        return true;
    }

    void Complete(const TActorContext&) override {
    }
};

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
    for (ui32 channel = FirstDataChannel; channel < Info()->Channels.size(); ++channel) {
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
    ctx.Schedule(CutHistoryContinuationDelay, new TEvPrivate::TEvContinueCutHistory());
}

void TColumnShard::Handle(TEvPrivate::TEvContinueCutHistory::TPtr&, const TActorContext& ctx) {
    if (!CutHistoryScan || CutHistoryScan->Pending || CutHistoryScan->Finished) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        Counters.GetCSCounters().OnCutHistoryScanAborted();
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
    const size_t end = Min(scan.Position + CutHistoryScanBatchSize, scan.Portions.size());
    ui64 memory = 0;
    while (scan.Position < end && (request->IsEmpty() || memory < CutHistoryScanMemoryTarget)) {
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
        ctx.Schedule(CutHistoryContinuationDelay, new TEvPrivate::TEvContinueCutHistory());
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
        Counters.GetCSCounters().OnCutHistoryScanAborted();
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
                    ++interval.BlobReferences;
                }
            }
        }
    }
    scan.Pending = 0;
    Schedule(CutHistoryContinuationDelay, new TEvPrivate::TEvContinueCutHistory());
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
    std::vector<TCutHistoryRequest> requests;
    for (auto& interval : CutHistoryScan->Intervals) {
        if (interval.BlobReferences != 0 || interval.Sent || interval.Channel >= Info()->Channels.size()) {
            continue;
        }
        const auto& history = Info()->Channels[interval.Channel].History;
        auto entry = FindIf(history, [&](const auto& item) {
            return item.FromGeneration == interval.From;
        });
        if (entry == history.end() || entry->GroupID != interval.Group) {
            continue;
        }
        const auto nextEntry = std::next(entry);
        if (nextEntry == history.end() || nextEntry->FromGeneration != interval.To) {
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
        requests.push_back({ TabletID(), interval.Channel, interval.From, interval.Group, ctx.Now(), LauncherID(), interval.To, Generation() });
        interval.Sent = true;
        Counters.GetCSCounters().OnCutHistoryRequestSent(ctx.Now() - *CutHistoryScan->Finished);
        ctx.Send(LauncherID(), event.release());
    }
    if (!requests.empty()) {
        // A crash before this diagnostic transaction commits can lose the newest send attempts.
        Execute(new TTxSaveCutHistoryRequests(this, std::move(requests)), ctx);
    }
}

}   // namespace NKikimr::NColumnShard
