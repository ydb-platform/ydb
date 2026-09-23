#include "columnshard_impl.h"

#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/sessions.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

#include <iterator>

namespace NKikimr::NColumnShard {
namespace {
constexpr size_t CutHistoryPreparationBatchSize = 1024;
constexpr size_t CutHistoryScanBatchSize = 32;
constexpr ui64 CutHistoryScanMemoryTarget = 8_MB;
constexpr TDuration CutHistoryContinuationDelay = TDuration::MilliSeconds(1);

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
        size_t visited = 0;
        while (visited < CutHistoryPreparationBatchSize && !Finished) {
            auto rows = db.Table<TPortions>().GreaterOrEqual(Cursor.first, Cursor.second).Select<TPortions::PathId, TPortions::PortionId>();
            if (!rows.IsReady()) {
                return false;
            }
            while (!rows.EndOfSet() && visited < CutHistoryPreparationBatchSize) {
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
                if (visited == CutHistoryPreparationBatchSize) {
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
            ctx.Schedule(CutHistoryContinuationDelay, new TEvPrivate::TEvContinueCutHistory());
        }
    }
};

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
    TColumnShard* const Owner;
    const std::shared_ptr<const NOlap::TVersionedIndex> VersionedIndex;

    void DoApplyResult(
        NOlap::NResourceBroker::NSubscribe::TResourceContainer<NOlap::TDataAccessorsResult>&& result, NOlap::TColumnEngineForLogs&) override {
        Owner->FinishCutHistoryBatch(result.GetValue(), *VersionedIndex);
    }

public:
    TCutHistoryResultProcessor(TColumnShard* owner, const std::shared_ptr<const NOlap::TVersionedIndex>& versionedIndex)
        : Owner(owner)
        , VersionedIndex(versionedIndex)
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
        scan.BootLastPortion = *MutableIndexAs<NOlap::TColumnEngineForLogs>().GetLastPortionPointer();
        scan.PreparationActor = ctx.Register(new TCutHistoryPreparationActor(SelfId()), TMailboxType::HTSwap, AppDataVerified().BatchPoolId);
        ActorsToStop.push_back(scan.PreparationActor);
    }
    CutHistoryScan = std::move(scan);
    ctx.Schedule(CutHistoryContinuationDelay, new TEvPrivate::TEvContinueCutHistory());
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
    ctx.Schedule(CutHistoryContinuationDelay, new TEvPrivate::TEvContinueCutHistory());
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
    SubmitMetadataRequest(
        NOlap::TCSMetadataRequest(request, std::make_shared<TCutHistoryResultProcessor>(this, index.GetVersionedIndexReadonlyCopy())));
}

void TColumnShard::FinishCutHistoryBatch(const NOlap::TDataAccessorsResult& result, const NOlap::TVersionedIndex& versionedIndex) {
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
        const auto blobs = accessor->GetBlobIdsByStorage(accessor->GetPortionInfo().GetSchema(versionedIndex)->GetIndexInfo());
        const auto* defaults = blobs.FindPtr(NOlap::IStoragesManager::DefaultStorageId);
        if (!defaults) {
            continue;
        }
        for (const auto& blob : *defaults) {
            const auto& id = blob.GetLogoBlobId();
            if (id.TabletID() != TabletID()) {
                continue;
            }
            const auto nextInterval = UpperBoundBy(
                scan.Intervals.begin(), scan.Intervals.end(), std::pair<ui32, ui32>{ id.Channel(), id.Generation() }, [](const auto& interval) {
                    return std::make_pair(interval.Channel, interval.From);
                });
            if (nextInterval != scan.Intervals.begin()) {
                auto& interval = *std::prev(nextInterval);
                if (id.Channel() == interval.Channel && id.Generation() < interval.To) {
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
        // Requests are already sent; a crash before commit can omit them from the diagnostic journal.
        Execute(new TTxSaveCutHistoryRequests(this, std::move(requests)), ctx);
    }
}

}   // namespace NKikimr::NColumnShard
