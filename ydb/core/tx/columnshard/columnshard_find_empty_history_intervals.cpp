#include "columnshard_impl.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
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

void ScheduleFindEmptyHistoryIntervalsContinuation(const NKikimrConfig::TColumnShardConfig& shardConfig, const TActorContext& ctx) {
    const auto& config = shardConfig.GetCutHistory();
    const ui64 jitter = config.GetContinuationJitterMs();
    const ui64 delay = Max<ui32>(1, config.GetContinuationDelayMs()) + (jitter ? RandomNumber<ui64>(jitter + 1) : 0);
    ctx.Schedule(TDuration::MilliSeconds(delay), new TEvPrivate::TEvContinueFindEmptyHistoryIntervals());
}

bool CanCutHistoryInterval(const TColumnShard& owner, const THistoryIntervalKey& key, const THistoryInterval& interval) {
    if (key.Channel >= owner.Info()->Channels.size() || !owner.LauncherID()) {
        return false;
    }
    const auto& history = owner.Info()->Channels[key.Channel].History;
    const auto entry = FindIf(history, [&](const auto& item) {
        return item.FromGeneration == key.From;
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
    return !storage->GetSharedBlobs()->HasBlobsInRange(key.Channel, key.From, interval.To);
}

class TFindEmptyHistoryIntervalsPreparationActor: public NActors::TActorBootstrapped<TFindEmptyHistoryIntervalsPreparationActor> {
    const TActorId Owner;
    std::vector<std::pair<TInternalPathId, ui64>> Portions;

public:
    TFindEmptyHistoryIntervalsPreparationActor(const TActorId owner, std::vector<std::pair<TInternalPathId, ui64>>&& portions)
        : Owner(owner)
        , Portions(std::move(portions))
    {
    }

    void Bootstrap() {
        SortBy(Portions, [](const auto& address) {
            return std::make_pair(address.second, address.first);
        });
        Send(Owner, new TEvPrivate::TEvFindEmptyHistoryIntervalsPortionsReady(std::move(Portions)));
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

    void Complete(const TActorContext& /*ctx*/) override {
        // Hive sending stays disabled until the pending-GC checks land.
        if (Self->SharingSessionsManager->CanCutHistory()) {
            for (const auto& request : ReadyToSendRequests) {
                auto event = std::make_unique<TEvTablet::TEvCutTabletHistory>();
                event->Record.SetTabletID(Self->TabletID());
                event->Record.SetChannel(request.GetChannel());
                event->Record.SetFromGeneration(request.GetFromGeneration());
                event->Record.SetGroupID(request.GetGroupID());
                // Self->Counters.GetCSCounters().OnCutHistoryRequestSent(ctx.Now() - *Self->EmptyHistoryIntervalsScan->Finished);
                // ctx.Send(Self->LauncherID(), event.release());
            }
        }
        Self->EmptyHistoryIntervalsScan.reset();
    }
};

class TFindEmptyHistoryIntervalsResultProcessor: public NOlap::IMetadataAccessorResultProcessor {
    TColumnShard* const Owner;

    void DoApplyResult(
        NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult>&& result, NOlap::TColumnEngineForLogs&) override {
        Owner->FinishFindEmptyHistoryIntervalsBatch(result.GetValue());
    }

public:
    explicit TFindEmptyHistoryIntervalsResultProcessor(TColumnShard* owner)
        : Owner(owner)
    {
    }
};

void TColumnShard::InitFindEmptyHistoryIntervals() {
    if (!AppData()->FeatureFlags.GetEnableCutHistory() || !AppData()->FeatureFlags.GetEnableColumnshardCutHistory()) {
        return;
    }
    TEmptyHistoryIntervalsScan scan;
    for (ui32 channel = FirstDataChannel; channel < Info()->Channels.size(); ++channel) {
        const auto& history = Info()->Channels[channel].History;
        for (size_t i = 0; i + 1 < history.size(); ++i) {
            AFL_VERIFY(history[i].FromGeneration < history[i + 1].FromGeneration)("channel", channel);
            if (history[i + 1].FromGeneration >= Generation()) {
                break;
            }
            scan.Intervals.emplace(THistoryIntervalKey{ channel, history[i].FromGeneration },
                THistoryInterval{ history[i + 1].FromGeneration, history[i].GroupID });
        }
    }
    if (scan.Intervals.empty()) {
        return;
    }
    EmptyHistoryIntervalsScan = std::move(scan);
}

void TColumnShard::StartFindEmptyHistoryIntervals(const TActorContext& ctx) {
    if (!EmptyHistoryIntervalsScan) {
        return;
    }
    if (!AppData()->FeatureFlags.GetEnableCutHistory() || !AppData()->FeatureFlags.GetEnableColumnshardCutHistory() ||
        !SharingSessionsManager->CanCutHistory()) {
        EmptyHistoryIntervalsScan.reset();
        return;
    }
    const auto storage = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(StoragesManager->GetDefaultOperator());
    AFL_VERIFY(storage);
    auto& scan = *EmptyHistoryIntervalsScan;
    std::erase_if(scan.Intervals, [&](const auto& entry) {
        const auto& [key, interval] = entry;
        return storage->GetSharedBlobs()->HasBlobsInRange(key.Channel, key.From, interval.To);
    });
    if (scan.Intervals.empty()) {
        EmptyHistoryIntervalsScan.reset();
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
        scan.PreparationActor = ctx.Register(
            new TFindEmptyHistoryIntervalsPreparationActor(SelfId(), std::move(portions)), TMailboxType::HTSwap, AppDataVerified().BatchPoolId);
        ActorsToStop.push_back(scan.PreparationActor);
        return;
    }
    ScheduleFindEmptyHistoryIntervalsContinuation(*ColumnShardConfig, ctx);
}

void TColumnShard::AbortFindEmptyHistoryIntervals() {
    if (EmptyHistoryIntervalsScan->PreparationActor) {
        Send(EmptyHistoryIntervalsScan->PreparationActor, new TEvents::TEvPoisonPill());
    }
    Counters.GetCSCounters().OnCutHistoryScanAborted();
    EmptyHistoryIntervalsScan.reset();
}

void TColumnShard::Handle(TEvPrivate::TEvFindEmptyHistoryIntervalsPortionsReady::TPtr& ev, const TActorContext& ctx) {
    if (!EmptyHistoryIntervalsScan || !EmptyHistoryIntervalsScan->PreparationActor ||
        ev->Sender != EmptyHistoryIntervalsScan->PreparationActor) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        AbortFindEmptyHistoryIntervals();
        return;
    }
    EmptyHistoryIntervalsScan->PreparationActor = {};
    EmptyHistoryIntervalsScan->Portions = std::move(ev->Get()->Portions);
    ScheduleFindEmptyHistoryIntervalsContinuation(*ColumnShardConfig, ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvContinueFindEmptyHistoryIntervals::TPtr&, const TActorContext& ctx) {
    if (!EmptyHistoryIntervalsScan || EmptyHistoryIntervalsScan->Pending || EmptyHistoryIntervalsScan->Finished) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        AbortFindEmptyHistoryIntervals();
        return;
    }
    auto& scan = *EmptyHistoryIntervalsScan;
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
        ScheduleFindEmptyHistoryIntervalsContinuation(*ColumnShardConfig, ctx);
        return;
    }
    scan.Pending = request->GetSize();
    SubmitMetadataRequest(NOlap::TCSMetadataRequest(request, std::make_shared<TFindEmptyHistoryIntervalsResultProcessor>(this)));
}

void TColumnShard::FinishFindEmptyHistoryIntervalsBatch(const NOlap::TDataAccessorsResult& result) {
    if (!EmptyHistoryIntervalsScan) {
        return;
    }
    auto& scan = *EmptyHistoryIntervalsScan;
    if (!SharingSessionsManager->CanCutHistory() || result.HasErrors() || result.HasRemovedData() ||
        result.GetPortions().size() != scan.Pending) {
        AbortFindEmptyHistoryIntervals();
        return;
    }
    const auto& ctx = TActivationContext::AsActorContext();
    for (const auto& [_, accessor] : result.GetPortions()) {
        for (const auto& blob : accessor->GetBlobIds()) {
            const auto& id = blob.GetLogoBlobId();
            if (id.TabletID() != TabletID()) {
                continue;
            }
            auto it = scan.Intervals.upper_bound({ id.Channel(), id.Generation() });
            if (it != scan.Intervals.begin()) {
                --it;
                const auto [key, interval] = *it;
                if (key.Channel == id.Channel() && id.Generation() < interval.To) {
                    if (blob.GetDsGroup() != interval.Group) {
                        Counters.GetCSCounters().OnCutHistoryBlobGroupMismatch();
                    }
                    scan.Intervals.erase(it);
                    if (scan.Intervals.empty()) {
                        Counters.GetCSCounters().OnCutHistoryScanFinished(ctx.Now() - scan.Started);
                        EmptyHistoryIntervalsScan.reset();
                        return;
                    }
                }
            }
        }
    }
    scan.Pending = 0;
    ScheduleFindEmptyHistoryIntervalsContinuation(*ColumnShardConfig, ctx);
}

void TColumnShard::TryCutHistory(const TActorContext& ctx) {
    if (!EmptyHistoryIntervalsScan || !EmptyHistoryIntervalsScan->Finished) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        EmptyHistoryIntervalsScan.reset();
        return;
    }
    std::vector<NKikimrTxColumnShard::TCutHistoryRequest> requests;
    for (const auto& [key, interval] : EmptyHistoryIntervalsScan->Intervals) {
        if (!CanCutHistoryInterval(*this, key, interval)) {
            continue;
        }
        auto& request = requests.emplace_back();
        request.SetTabletID(TabletID());
        request.SetChannel(key.Channel);
        request.SetFromGeneration(key.From);
        request.SetGroupID(interval.Group);
        request.SetTimestampUs(ctx.Now().MicroSeconds());
        ActorIdToProto(LauncherID(), request.MutableRecipient());
        request.SetToGeneration(interval.To);
        request.SetSendingGeneration(Generation());
    }
    EmptyHistoryIntervalsScan->Intervals.clear();
    if (!requests.empty()) {
        Counters.GetCSCounters().OnCuttableHistoryIntervalsFound(requests.size());
        Execute(new TTxSaveCutHistoryRequests(this, std::move(requests)), ctx);
    } else {
        EmptyHistoryIntervalsScan.reset();
    }
}

}   // namespace NKikimr::NColumnShard
