#include "columnshard_impl.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/sessions.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/random/random.h>

#include <iterator>
#include <utility>

namespace NKikimr::NColumnShard {
namespace {

void ScheduleUnusedHistoryContinuation(const NKikimrConfig::TColumnShardConfig& shardConfig, const TActorContext& ctx) {
    const auto& config = shardConfig.GetCutHistory();
    const ui64 jitter = config.GetContinuationJitterMs();
    const ui64 delay = Max<ui32>(1, config.GetContinuationDelayMs()) + (jitter ? RandomNumber<ui64>(jitter + 1) : 0);
    ctx.Schedule(TDuration::MilliSeconds(delay), new TEvPrivate::TEvContinueUnusedHistory());
}

THistoryInterval* FindHistoryInterval(std::vector<THistoryInterval>& intervals, const TLogoBlobID& id) {
    const auto next =
        UpperBoundBy(intervals.begin(), intervals.end(), std::pair<ui32, ui32>{ id.Channel(), id.Generation() }, [](const auto& interval) {
            return std::make_pair(interval.Channel, interval.From);
        });
    if (next != intervals.begin()) {
        auto& interval = *std::prev(next);
        if (id.Channel() == interval.Channel && id.Generation() < interval.To) {
            return &interval;
        }
    }
    return nullptr;
}

bool CanCutHistoryInterval(
    const TColumnShard& owner, const THistoryInterval& interval, const NOlap::TPendingGCBlobGenerations& pendingGenerations) {
    if (interval.HasBlobs || interval.Channel >= owner.Info()->Channels.size() || !owner.LauncherID()) {
        return false;
    }
    const auto& history = owner.Info()->Channels[interval.Channel].History;
    const auto entry = FindIf(history, [&](const auto& item) {
        return item.FromGeneration == interval.From;
    });
    if (entry == history.end() || entry->GroupID != interval.Group) {
        return false;
    }
    const auto next = std::next(entry);
    if (next == history.end() || next->FromGeneration != interval.To) {
        return false;
    }
    const auto storage =
        std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(owner.GetStoragesManager()->GetDefaultOperator());
    AFL_VERIFY(storage);
    return storage->CanCutHistory(pendingGenerations, interval.Channel, interval.From, interval.To);
}

class TUnusedHistoryPreparationActor: public NActors::TActorBootstrapped<TUnusedHistoryPreparationActor> {
    const TActorId Owner;
    std::vector<std::pair<TInternalPathId, ui64>> Portions;

public:
    TUnusedHistoryPreparationActor(const TActorId owner, std::vector<std::pair<TInternalPathId, ui64>>&& portions)
        : Owner(owner)
        , Portions(std::move(portions))
    {
    }

    void Bootstrap() {
        SortBy(Portions, [](const auto& address) {
            return std::make_pair(address.second, address.first);
        });
        Send(Owner, new TEvPrivate::TEvUnusedHistoryPortionsReady(std::move(Portions)));
        PassAway();
    }
};
}   // namespace

class TTxSaveCutHistoryRequests: public TTransactionBase<TColumnShard> {
    const std::vector<NKikimrTxColumnShard::TCutHistoryRequest> ReadyToSendRequests;

public:
    TTxSaveCutHistoryRequests(TColumnShard* self, std::vector<NKikimrTxColumnShard::TCutHistoryRequest>&& requests)
        : TBase(self)
        , ReadyToSendRequests(std::move(requests))
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
        for (const auto& request : ReadyToSendRequests) {
            db.Table<T>().Key(++sequence).Update(NIceDb::TUpdate<T::RequestProto>(request.SerializeAsString()));
            if (sequence > CutHistoryRequestLimit) {
                db.Table<T>().Key(sequence - CutHistoryRequestLimit).Delete();
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Self->UnusedHistoryScan || !Self->UnusedHistoryScan->Finished) {
            return;
        }
        Self->UnusedHistoryScan->SavePending = false;
        if (!Self->SharingSessionsManager->CanCutHistory()) {
            Self->UnusedHistoryScan.reset();
            return;
        }
        auto& scan = *Self->UnusedHistoryScan;
        for (const auto& request : ReadyToSendRequests) {
            const auto it = FindIf(scan.Intervals, [&](const auto& interval) {
                return interval.Channel == request.GetChannel() && interval.From == request.GetFromGeneration() &&
                       interval.To == request.GetToGeneration() && interval.Group == request.GetGroupID();
            });
            if (it != scan.Intervals.end()) {
                it->ReadyToSend = true;
            }
        }
        Self->TryCutHistory(ctx);
    }
};

class TUnusedHistoryResultProcessor: public NOlap::IMetadataAccessorResultProcessor {
    TColumnShard* const Owner;

    void DoApplyResult(
        NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult>&& result, NOlap::TColumnEngineForLogs&) override {
        Owner->FinishUnusedHistoryBatch(result.GetValue());
    }

public:
    explicit TUnusedHistoryResultProcessor(TColumnShard* owner)
        : Owner(owner)
    {
    }
};

void TColumnShard::InitUnusedHistoryScan() {
    if (!AppData()->FeatureFlags.GetEnableCutHistory() || !AppData()->FeatureFlags.GetEnableColumnshardCutHistory()) {
        return;
    }
    TUnusedHistoryScan scan;
    for (ui32 channel = FirstDataChannel; channel < Info()->Channels.size(); ++channel) {
        const auto& history = Info()->Channels[channel].History;
        for (size_t i = 0; i + 1 < history.size(); ++i) {
            AFL_VERIFY(history[i].FromGeneration < history[i + 1].FromGeneration)("channel", channel);
            if (history[i + 1].FromGeneration >= Generation()) {
                break;
            }
            scan.Intervals.push_back({ channel, history[i].FromGeneration, history[i + 1].FromGeneration, history[i].GroupID });
        }
    }
    if (scan.Intervals.empty()) {
        return;
    }
    UnusedHistoryScan = std::move(scan);
}

void TColumnShard::StartUnusedHistoryScan(const TActorContext& ctx) {
    if (!UnusedHistoryScan) {
        return;
    }
    if (!AppData()->FeatureFlags.GetEnableCutHistory() || !AppData()->FeatureFlags.GetEnableColumnshardCutHistory() ||
        !SharingSessionsManager->CanCutHistory()) {
        UnusedHistoryScan.reset();
        return;
    }
    const auto storage = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(StoragesManager->GetDefaultOperator());
    AFL_VERIFY(storage);
    auto& scan = *UnusedHistoryScan;
    EraseIf(scan.Intervals, [&](const auto& interval) {
        return storage->GetSharedBlobs()->HasBlobsInRange(interval.Channel, interval.From, interval.To);
    });
    if (scan.Intervals.empty()) {
        UnusedHistoryScan.reset();
        return;
    }
    scan.Started = ctx.Now();
    if (HasIndex()) {
        const auto& tables = GetIndexAs<NOlap::TColumnEngineForLogs>().GetTables();
        size_t portionCount = 0;
        for (const auto& [_, granule] : tables) {
            portionCount += granule->GetPortions().size() + granule->GetInsertedPortions().size();
        }
        std::vector<std::pair<TInternalPathId, ui64>> portions;
        portions.reserve(portionCount);
        for (const auto& [pathId, granule] : tables) {
            for (const auto& [portionId, _] : granule->GetPortions()) {
                portions.emplace_back(pathId, portionId);
            }
            for (const auto& [_, portion] : granule->GetInsertedPortions()) {
                portions.emplace_back(pathId, portion->GetPortionId());
            }
        }
        scan.PreparationActor =
            ctx.Register(new TUnusedHistoryPreparationActor(SelfId(), std::move(portions)), TMailboxType::HTSwap, AppDataVerified().BatchPoolId);
        ActorsToStop.push_back(scan.PreparationActor);
        return;
    }
    ScheduleUnusedHistoryContinuation(*ColumnShardConfig, ctx);
}

void TColumnShard::AbortUnusedHistoryScan() {
    if (UnusedHistoryScan->PreparationActor) {
        Send(UnusedHistoryScan->PreparationActor, new TEvents::TEvPoisonPill());
    }
    Counters.GetCSCounters().OnCutHistoryScanAborted();
    UnusedHistoryScan.reset();
}

void TColumnShard::Handle(TEvPrivate::TEvUnusedHistoryPortionsReady::TPtr& ev, const TActorContext& ctx) {
    if (!UnusedHistoryScan || !UnusedHistoryScan->PreparationActor || ev->Sender != UnusedHistoryScan->PreparationActor) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        AbortUnusedHistoryScan();
        return;
    }
    UnusedHistoryScan->PreparationActor = {};
    UnusedHistoryScan->Portions = std::move(ev->Get()->Portions);
    ScheduleUnusedHistoryContinuation(*ColumnShardConfig, ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvContinueUnusedHistory::TPtr&, const TActorContext& ctx) {
    if (!UnusedHistoryScan || UnusedHistoryScan->Pending || UnusedHistoryScan->Finished) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        AbortUnusedHistoryScan();
        return;
    }
    auto& scan = *UnusedHistoryScan;
    if (scan.PreparationActor) {
        return;
    }
    if (scan.Position == scan.Portions.size()) {
        scan.Finished = ctx.Now();
        std::vector<std::pair<TInternalPathId, ui64>>().swap(scan.Portions);
        Counters.GetCSCounters().OnCutHistoryScanFinished(*scan.Finished - scan.Started);
        TryCutHistory(ctx);
        return;
    }
    auto& index = MutableIndexAs<NOlap::TColumnEngineForLogs>();
    auto request = std::make_shared<NOlap::TDataAccessorsRequest>(NOlap::NGeneralCache::TPortionsMetadataCachePolicy::EConsumer::SCAN);
    const auto& config = ColumnShardConfig->GetCutHistory();
    const size_t end = scan.Position + Min<size_t>(Max<ui32>(1, config.GetScanBatchSize()), scan.Portions.size() - scan.Position);
    ui64 memory = 0;
    while (scan.Position < end && (request->IsEmpty() || memory < Max<ui64>(1, config.GetScanMemoryLimitBytes()))) {
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
        ScheduleUnusedHistoryContinuation(*ColumnShardConfig, ctx);
        return;
    }
    scan.Pending = request->GetSize();
    SubmitMetadataRequest(NOlap::TCSMetadataRequest(request, std::make_shared<TUnusedHistoryResultProcessor>(this)));
}

void TColumnShard::FinishUnusedHistoryBatch(const NOlap::TDataAccessorsResult& result) {
    if (!UnusedHistoryScan) {
        return;
    }
    auto& scan = *UnusedHistoryScan;
    if (!SharingSessionsManager->CanCutHistory() || result.HasErrors() || result.HasRemovedData() ||
        result.GetPortions().size() != scan.Pending) {
        AbortUnusedHistoryScan();
        return;
    }
    for (const auto& [_, accessor] : result.GetPortions()) {
        for (const auto& blob : accessor->GetBlobIds()) {
            const auto& id = blob.GetLogoBlobId();
            if (id.TabletID() != TabletID()) {
                continue;
            }
            if (auto* interval = FindHistoryInterval(scan.Intervals, id); interval && blob.GetDsGroup() == interval->Group) {
                interval->HasBlobs = true;
            }
        }
    }
    scan.Pending = 0;
    ScheduleUnusedHistoryContinuation(*ColumnShardConfig, TActivationContext::AsActorContext());
}

void TColumnShard::ResumePostponedCutHistory(const TActorContext& ctx) {
    if (UnusedHistoryScan && UnusedHistoryScan->WaitingForGC) {
        TryCutHistory(ctx);
    }
}

void TColumnShard::TryCutHistory(const TActorContext& ctx) {
    if (!UnusedHistoryScan || !UnusedHistoryScan->Finished || UnusedHistoryScan->SavePending) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        UnusedHistoryScan.reset();
        return;
    }
    const auto storage = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(StoragesManager->GetDefaultOperator());
    AFL_VERIFY(storage);
    UnusedHistoryScan->WaitingForGC = storage->HasGCInFlight();
    if (UnusedHistoryScan->WaitingForGC) {
        return;
    }
    NOlap::TPendingGCBlobGenerations pendingGenerations;
    if (AnyOf(UnusedHistoryScan->Intervals, [](const auto& interval) {
            return interval.ReadyToSend || (!interval.Attempted && !interval.HasBlobs);
        })) {
        pendingGenerations = storage->GetPendingGCBlobGenerations();
    }
    std::vector<NKikimrTxColumnShard::TCutHistoryRequest> requests;
    for (size_t i = 0; i < UnusedHistoryScan->Intervals.size(); ++i) {
        auto& interval = UnusedHistoryScan->Intervals[i];
        if (interval.Attempted && !interval.ReadyToSend) {
            continue;
        }
        interval.Attempted = true;
        const bool readyToSend = std::exchange(interval.ReadyToSend, false);
        if (!CanCutHistoryInterval(*this, interval, pendingGenerations)) {
            continue;
        }
        if (readyToSend) {
            auto event = std::make_unique<TEvTablet::TEvCutTabletHistory>();
            event->Record.SetTabletID(TabletID());
            event->Record.SetChannel(interval.Channel);
            event->Record.SetFromGeneration(interval.From);
            event->Record.SetGroupID(interval.Group);
            Counters.GetCSCounters().OnCutHistoryRequestSent(ctx.Now() - *UnusedHistoryScan->Finished);
            ctx.Send(LauncherID(), event.release(), IEventHandle::FlagTrackDelivery, i + 1);
            continue;
        }
        auto& request = requests.emplace_back();
        request.SetTabletID(TabletID());
        request.SetChannel(interval.Channel);
        request.SetFromGeneration(interval.From);
        request.SetGroupID(interval.Group);
        request.SetTimestampUs(ctx.Now().MicroSeconds());
        ActorIdToProto(LauncherID(), request.MutableRecipient());
        request.SetToGeneration(interval.To);
        request.SetSendingGeneration(Generation());
    }
    if (!requests.empty()) {
        UnusedHistoryScan->SavePending = true;
        Execute(new TTxSaveCutHistoryRequests(this, std::move(requests)), ctx);
    }
}

}   // namespace NKikimr::NColumnShard
