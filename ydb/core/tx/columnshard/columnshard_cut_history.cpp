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

namespace NKikimr::NColumnShard {
namespace {

class TCutHistoryPreparationActor: public NActors::TActorBootstrapped<TCutHistoryPreparationActor> {
    const TActorId Owner;
    std::vector<std::pair<TInternalPathId, ui64>> Portions;

    void Handle(TEvPrivate::TEvCutHistoryPortionsBatch::TPtr& ev) {
        if (ev->Sender != Owner) {
            return;
        }
        auto& chunk = *ev->Get();
        Portions.reserve(Portions.size() + chunk.Portions.size());
        std::move(chunk.Portions.begin(), chunk.Portions.end(), std::back_inserter(Portions));
        if (chunk.Finished) {
            SortBy(Portions, [](const auto& address) {
                return std::make_pair(address.second, address.first);
            });
            Send(Owner, new TEvPrivate::TEvCutHistoryPortionsReady(std::move(Portions)));
            PassAway();
        }
    }

    STRICT_STFUNC(StateWork, hFunc(TEvPrivate::TEvCutHistoryPortionsBatch, Handle) cFunc(TEvents::TEvPoisonPill::EventType, PassAway))

public:
    explicit TCutHistoryPreparationActor(const TActorId owner)
        : Owner(owner)
    {
    }

    void Bootstrap() {
        Become(&TThis::StateWork);
    }
};
}   // namespace

class TColumnShard::TTxPrepareCutHistory: public TTransactionBase<TColumnShard> {
    struct TPortions: Schema::IndexPortions {
        using Precharge = NIceDb::Schema::NoAutoPrecharge;
    };

    const TActorId PreparationActor;
    const ui64 BootLastPortion;
    const std::pair<ui64, ui64> StartCursor;
    const std::optional<std::pair<ui64, ui64>> StartMaxKey;
    std::pair<ui64, ui64> Cursor;
    std::optional<std::pair<ui64, ui64>> MaxKey;
    std::vector<std::pair<TInternalPathId, ui64>> Portions;
    bool Finished = false;

public:
    explicit TTxPrepareCutHistory(TColumnShard* self)
        : TBase(self)
        , PreparationActor(self->CutHistoryScan->PreparationActor)
        , BootLastPortion(self->CutHistoryScan->BootLastPortion)
        , StartCursor(self->CutHistoryScan->PreparationCursor)
        , StartMaxKey(self->CutHistoryScan->PreparationMaxKey)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Portions.clear();
        Cursor = StartCursor;
        MaxKey = StartMaxKey;
        Finished = false;
        NIceDb::TNiceDb db(txc.DB);
        if (!MaxKey) {
            auto last = db.Table<TPortions>().Reverse().Range().Select<TPortions::PathId, TPortions::PortionId>();
            if (!last.IsReady()) {
                return false;
            }
            if (last.EndOfSet()) {
                Finished = true;
                return true;
            }
            MaxKey = std::make_pair(last.GetValue<TPortions::PathId>(), last.GetValue<TPortions::PortionId>());
        }
        const size_t batchSize = Max<ui32>(1, Self->ColumnShardConfig->GetCutHistory().GetPreparationBatchSize());
        size_t visited = 0;
        while (visited < batchSize && !Finished) {
            auto rows = db.Table<TPortions>().GreaterOrEqual(Cursor.first, Cursor.second).Select<TPortions::PathId, TPortions::PortionId>();
            if (!rows.IsReady()) {
                return false;
            }
            while (!rows.EndOfSet() && visited < batchSize) {
                const std::pair<ui64, ui64> key{ rows.GetValue<TPortions::PathId>(), rows.GetValue<TPortions::PortionId>() };
                if (key > *MaxKey) {
                    Finished = true;
                    break;
                }
                ++visited;
                if (key.second <= BootLastPortion) {
                    Portions.emplace_back(TInternalPathId::FromRawValue(key.first), key.second);
                }
                if (key == *MaxKey) {
                    Finished = true;
                    break;
                }
                if (key.second > BootLastPortion || key.second == Max<ui64>()) {
                    if (key.first == Max<ui64>()) {
                        Finished = true;
                    } else {
                        Cursor = { key.first + 1, 0 };
                    }
                    break;
                }
                Cursor = { key.first, key.second + 1 };
                if (visited == batchSize) {
                    break;
                }
                if (!rows.Next()) {
                    return false;
                }
            }
            Finished |= rows.EndOfSet();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Self->CutHistoryScan || Self->CutHistoryScan->PreparationActor != PreparationActor) {
            return;
        }
        if (!Self->SharingSessionsManager->CanCutHistory()) {
            Self->AbortCutHistoryScan();
            return;
        }
        auto& scan = *Self->CutHistoryScan;
        // Read-only Complete follows prior cleanup commits and their GC bookkeeping publication.
        scan.PreparationCursor = Cursor;
        scan.PreparationMaxKey = MaxKey;
        scan.PreparationPending = Finished;
        ctx.Send(PreparationActor, new TEvPrivate::TEvCutHistoryPortionsBatch(std::move(Portions), Finished));
        if (!Finished) {
            Self->ScheduleCutHistoryContinuation(ctx);
        }
    }
};

class TColumnShard::TTxSaveCutHistoryRequests: public TTransactionBase<TColumnShard> {
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
        if (!Self->CutHistoryScan || !Self->CutHistoryScan->Finished) {
            return;
        }
        Self->CutHistoryScan->SavePending = false;
        if (!Self->SharingSessionsManager->CanCutHistory()) {
            Self->CutHistoryScan.reset();
            return;
        }
        const auto storage =
            std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(Self->StoragesManager->GetDefaultOperator());
        if (!storage || storage->GetStopped() || storage->HasGCInFlight()) {
            return;
        }
        const auto pendingGenerations = storage->GetPendingGCBlobGenerations();
        for (const auto& request : ReadyToSendRequests) {
            auto& scan = *Self->CutHistoryScan;
            const auto it = FindIf(scan.Intervals, [&](const auto& interval) {
                return interval.Channel == request.GetChannel() && interval.From == request.GetFromGeneration() &&
                       interval.To == request.GetToGeneration() && interval.Group == request.GetGroupID();
            });
            if (it == scan.Intervals.end() || !Self->CanCutHistoryInterval(*it, pendingGenerations)) {
                continue;
            }
            auto event = std::make_unique<TEvTablet::TEvCutTabletHistory>();
            event->Record.SetTabletID(request.GetTabletID());
            event->Record.SetChannel(request.GetChannel());
            event->Record.SetFromGeneration(request.GetFromGeneration());
            event->Record.SetGroupID(request.GetGroupID());
            Self->Counters.GetCSCounters().OnCutHistoryRequestSent(ctx.Now() - *scan.Finished);
            ctx.Send(Self->LauncherID(), event.release(), IEventHandle::FlagTrackDelivery, std::distance(scan.Intervals.begin(), it) + 1);
        }
    }
};

class TColumnShard::TCutHistoryResultProcessor: public NOlap::IMetadataAccessorResultProcessor {
    TColumnShard* const Owner;

    void DoApplyResult(
        NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult>&& result, NOlap::TColumnEngineForLogs&) override {
        Owner->FinishCutHistoryBatch(result.GetValue());
    }

public:
    explicit TCutHistoryResultProcessor(TColumnShard* owner)
        : Owner(owner)
    {
    }
};

void TColumnShard::ScheduleCutHistoryContinuation(const TActorContext& ctx) {
    const auto& config = ColumnShardConfig->GetCutHistory();
    const ui64 jitter = config.GetContinuationJitterMs();
    const ui64 delay = Max<ui32>(1, config.GetContinuationDelayMs()) + (jitter ? RandomNumber<ui64>(jitter + 1) : 0);
    ctx.Schedule(TDuration::MilliSeconds(delay), new TEvPrivate::TEvContinueCutHistory());
}

void TColumnShard::StartCutHistoryScan(const TActorContext& ctx) {
    if (!CutHistoryScan || CutHistoryScan->Started != TInstant::Zero()) {
        return;
    }
    if (!AppData()->FeatureFlags.GetEnableCutHistory() || !AppData()->FeatureFlags.GetEnableColumnshardCutHistory() ||
        !SharingSessionsManager->CanCutHistory()) {
        CutHistoryScan.reset();
        return;
    }
    const auto storage = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(StoragesManager->GetDefaultOperator());
    if (!storage || storage->GetStopped()) {
        CutHistoryScan.reset();
        return;
    }
    auto& scan = *CutHistoryScan;
    EraseIf(scan.Intervals, [&](const auto& interval) {
        return interval.BlobReferences || storage->GetSharedBlobs()->HasBlobsInRange(interval.Channel, interval.From, interval.To);
    });
    if (scan.Intervals.empty()) {
        CutHistoryScan.reset();
        return;
    }
    scan.Started = ctx.Now();
    if (HasIndex()) {
        scan.BootLastPortion = *MutableIndexAs<NOlap::TColumnEngineForLogs>().GetLastPortionPointer();
        scan.PreparationActor = ctx.Register(new TCutHistoryPreparationActor(SelfId()), TMailboxType::HTSwap, AppDataVerified().BatchPoolId);
        ActorsToStop.push_back(scan.PreparationActor);
    }
    ScheduleCutHistoryContinuation(ctx);
}

void TColumnShard::AbortCutHistoryScan() {
    if (CutHistoryScan->PreparationActor) {
        Send(CutHistoryScan->PreparationActor, new TEvents::TEvPoisonPill());
    }
    Counters.GetCSCounters().OnCutHistoryScanAborted();
    CutHistoryScan.reset();
}

void TColumnShard::Handle(TEvPrivate::TEvCutHistoryPortionsReady::TPtr& ev, const TActorContext& ctx) {
    if (!CutHistoryScan || !CutHistoryScan->PreparationActor || ev->Sender != CutHistoryScan->PreparationActor) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        AbortCutHistoryScan();
        return;
    }
    CutHistoryScan->PreparationActor = {};
    CutHistoryScan->PreparationPending = false;
    CutHistoryScan->Portions = std::move(ev->Get()->Portions);
    ScheduleCutHistoryContinuation(ctx);
}

void TColumnShard::Handle(TEvPrivate::TEvContinueCutHistory::TPtr&, const TActorContext& ctx) {
    if (!CutHistoryScan || CutHistoryScan->Pending || CutHistoryScan->Finished) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        AbortCutHistoryScan();
        return;
    }
    auto& scan = *CutHistoryScan;
    if (scan.PreparationActor) {
        if (!scan.PreparationPending) {
            scan.PreparationPending = true;
            Execute(new TTxPrepareCutHistory(this), ctx);
        }
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
        ScheduleCutHistoryContinuation(ctx);
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
        AbortCutHistoryScan();
        return;
    }
    for (const auto& [_, accessor] : result.GetPortions()) {
        for (const auto& blob : accessor->GetBlobIds()) {
            const auto& id = blob.GetLogoBlobId();
            if (id.TabletID() != TabletID()) {
                continue;
            }
            if (auto* interval = FindCutHistoryInterval(scan.Intervals, id); interval && blob.GetDsGroup() == interval->Group) {
                ++interval->BlobReferences;
            }
        }
    }
    scan.Pending = 0;
    ScheduleCutHistoryContinuation(TActivationContext::AsActorContext());
}

TColumnShard::TCutHistoryInterval* TColumnShard::FindCutHistoryInterval(std::vector<TCutHistoryInterval>& intervals, const TLogoBlobID& id) {
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

bool TColumnShard::CanCutHistoryInterval(const TCutHistoryInterval& interval, const NOlap::TPendingGCBlobGenerations& pendingGenerations) const {
    if (interval.BlobReferences || interval.Channel >= Info()->Channels.size() || !LauncherID()) {
        return false;
    }
    const auto& history = Info()->Channels[interval.Channel].History;
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
    const auto storage = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(StoragesManager->GetDefaultOperator());
    return storage && storage->CanCutHistory(pendingGenerations, interval.Channel, interval.From, interval.To);
}

void TColumnShard::TryCutHistory(const TActorContext& ctx) {
    if (!CutHistoryScan || !CutHistoryScan->Finished || CutHistoryScan->SavePending) {
        return;
    }
    if (!SharingSessionsManager->CanCutHistory()) {
        CutHistoryScan.reset();
        return;
    }
    const auto storage = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(StoragesManager->GetDefaultOperator());
    NOlap::TPendingGCBlobGenerations pendingGenerations;
    if (storage && !storage->GetStopped() && !storage->HasGCInFlight() && AnyOf(CutHistoryScan->Intervals, [](const auto& interval) {
            return !interval.Attempted && !interval.BlobReferences;
        })) {
        pendingGenerations = storage->GetPendingGCBlobGenerations();
    }
    std::vector<NKikimrTxColumnShard::TCutHistoryRequest> requests;
    for (auto& interval : CutHistoryScan->Intervals) {
        if (interval.Attempted) {
            continue;
        }
        interval.Attempted = true;
        if (!CanCutHistoryInterval(interval, pendingGenerations)) {
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
        CutHistoryScan->SavePending = true;
        Execute(new TTxSaveCutHistoryRequests(this, std::move(requests)), ctx);
    }
}

}   // namespace NKikimr::NColumnShard
