#include "columnshard_find_empty_history_intervals.h"
#include "columnshard_impl.h"

#include "find_empty_history_intervals/portions.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/data_sharing/manager/sessions.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/random/random.h>

namespace NKikimr::NColumnShard {
namespace {

using TCachePolicy = NOlap::NGeneralCache::TPortionsMetadataCachePolicy;
using TAddress = TCachePolicy::TAddress;
using TCache = NGeneralCache::TServiceOperator<TCachePolicy>;
using TResourcesGuard = NOlap::NResourceBroker::NSubscribe::TResourcesGuard;
using TTaskContext = NOlap::NResourceBroker::NSubscribe::TTaskContext;

bool IsCutHistoryEnabled() {
    return AppData()->FeatureFlags.GetEnableCutHistory() && AppData()->FeatureFlags.GetEnableColumnshardCutHistory();
}

TDuration ContinuationDelay(const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& config) {
    const ui64 jitter = config.GetContinuationJitterMs();
    return TDuration::MilliSeconds(Max<ui32>(1, config.GetContinuationDelayMs()) + (jitter ? RandomNumber<ui64>(jitter + 1) : 0));
}

struct TEvResourcesReady: NActors::TEventLocal<TEvResourcesReady, TEvColumnShard::EvFindEmptyHistoryIntervalsResourcesReady> {
    std::shared_ptr<TResourcesGuard> Guard;

    explicit TEvResourcesReady(const std::shared_ptr<TResourcesGuard>& guard)
        : Guard(guard)
    {
    }
};

struct TEvAccessorsReady: NActors::TEventLocal<TEvAccessorsReady, TEvColumnShard::EvFindEmptyHistoryIntervalsAccessorsReady> {
    THashMap<TAddress, TCachePolicy::TObject> Accessors;
    std::shared_ptr<TResourcesGuard> Guard;
    bool Failed;

    TEvAccessorsReady(THashMap<TAddress, TCachePolicy::TObject>&& accessors, std::shared_ptr<TResourcesGuard>&& guard, bool failed)
        : Accessors(std::move(accessors))
        , Guard(std::move(guard))
        , Failed(failed)
    {
    }
};

class TMemoryRequest: public NOlap::NResourceBroker::NSubscribe::ITask {
    TActorId Owner;

    void DoOnAllocationSuccess(const std::shared_ptr<TResourcesGuard>& guard) override {
        TActivationContext::Send(Owner, std::make_unique<TEvResourcesReady>(guard));
    }

public:
    TMemoryRequest(TActorId owner, ui64 memory, const TTaskContext& context)
        : ITask(0, memory, "cut-history", context)
        , Owner(owner)
    {
    }
};

class TMetadataCallback: public TCache::ICallback {
    TActorId Owner;
    std::shared_ptr<TResourcesGuard> Guard;
    std::shared_ptr<TAtomicCounter> Aborted;

    bool DoIsAborted() const override {
        return Aborted->Val();
    }

    void DoOnResultReady(THashMap<TCachePolicy::TAddress, TCachePolicy::TObject>&& accessors, THashSet<TCachePolicy::TAddress>&& removed,
        NGeneralCache::NPublic::TErrorAddresses<TCachePolicy>&& errors) override {
        TActivationContext::Send(
            Owner, std::make_unique<TEvAccessorsReady>(std::move(accessors), std::move(Guard), !removed.empty() || errors.HasErrors()));
    }

public:
    TMetadataCallback(TActorId owner, std::shared_ptr<TResourcesGuard> guard, const std::shared_ptr<TAtomicCounter>& aborted)
        : Owner(owner)
        , Guard(std::move(guard))
        , Aborted(aborted)
    {
    }
};

class TFindEmptyHistoryIntervalsActor: public TActorBootstrapped<TFindEmptyHistoryIntervalsActor> {
    TActorId Owner;
    TActorId ResourceSubscriber;
    TTaskContext TaskContext;
    NKikimrConfig::TColumnShardConfig::TCutHistoryConfig Config;
    std::vector<TFindEmptyHistoryIntervalsContext::TPortion> Portions;
    NFindEmptyHistoryIntervals::TIntervalFilter Filter;
    THashMap<TAddress, NOlap::ISnapshotSchema::TPtr> BatchPortionSchemas;
    std::shared_ptr<TAtomicCounter> Aborted = std::make_shared<TAtomicCounter>(0);
    size_t Position = 0;
    TInstant Started;
    TCSCounters Counters;

    void Finish(bool aborted = false) {
        TEmptyHistoryIntervalsScanResult result;
        result.Intervals = Filter.ExtractIntervals();
        result.Started = Started;
        result.Finished = TActivationContext::Now();
        result.Aborted = aborted;
        Send(Owner, new TEvFindEmptyHistoryIntervals::TEvScanFinished(std::move(result)));
        PassAway();
    }

    void ScheduleNext() {
        Schedule(ContinuationDelay(Config), new TEvFindEmptyHistoryIntervals::TEvContinue());
    }

    void Handle(TEvFindEmptyHistoryIntervals::TEvContinue::TPtr&) {
        if (!IsCutHistoryEnabled()) {
            Finish(true);
            return;
        }
        if (Position == Portions.size() || Filter.Empty()) {
            Finish();
            return;
        }
        ui64 memory = 0;
        size_t end = Position + Min<size_t>(Max<ui32>(1, Config.GetScanBatchSize()), Portions.size() - Position);
        while (Position < end && (BatchPortionSchemas.empty() || memory < Max<ui64>(1, Config.GetScanMemoryLimitBytes()))) {
            auto& portion = Portions[Position++];
            memory += portion.AccessorMemory;
            AFL_VERIFY(BatchPortionSchemas.emplace(TAddress(Owner, NOlap::TPortionAddress(portion.PathId, portion.PortionId)), std::move(portion.Schema)).second);
        }
        NOlap::NResourceBroker::NSubscribe::ITask::StartResourceSubscription(
            ResourceSubscriber, std::make_shared<TMemoryRequest>(SelfId(), memory, TaskContext));
    }

    void Handle(TEvResourcesReady::TPtr& ev) {
        THashSet<TAddress> addresses;
        addresses.reserve(BatchPortionSchemas.size());
        for (const auto& [address, _] : BatchPortionSchemas) {
            addresses.emplace(address);
        }
        // SCAN labels metadata tracking only; resource-broker queues are managed separately.
        TCache::AskObjects(TCachePolicy::EConsumer::SCAN, std::move(addresses),
            std::make_shared<TMetadataCallback>(SelfId(), std::move(ev->Get()->Guard), Aborted));
    }

    void ExcludeBlob(const NOlap::TUnifiedBlobId& blob) {
        if (const auto group = Filter.Exclude(blob.GetLogoBlobId()); group && *group != blob.GetDsGroup()) {
            Counters.OnCutHistoryBlobGroupMismatch();
        }
    }

    void Handle(TEvAccessorsReady::TPtr& ev) {
        if (ev->Get()->Failed || ev->Get()->Accessors.size() != BatchPortionSchemas.size() || !IsCutHistoryEnabled()) {
            Finish(true);
            return;
        }
        for (const auto& [address, accessor] : ev->Get()->Accessors) {
            const auto* schema = BatchPortionSchemas.FindPtr(address);
            AFL_VERIFY(schema);
            NFindEmptyHistoryIntervals::VisitBlobStorageBlobs(*accessor, (*schema)->GetIndexInfo(), [this](const auto& blob) {
                ExcludeBlob(blob);
            });
        }
        BatchPortionSchemas.clear();
        if (Filter.Empty() || Position == Portions.size()) {
            Finish();
        } else {
            ScheduleNext();
        }
    }

    void PassAway() override {
        Aborted->Inc();
        TActorBootstrapped::PassAway();
    }

public:
    TFindEmptyHistoryIntervalsActor(TFindEmptyHistoryIntervalsContext&& context, TActorId resourceSubscriber, const TTaskContext& taskContext,
        const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& config, const TCSCounters& counters, TInstant started)
        : Owner(context.TabletActorId)
        , ResourceSubscriber(resourceSubscriber)
        , TaskContext(taskContext)
        , Config(config)
        , Portions(std::move(context.Portions))
        , Filter(context.TabletId, std::move(context.Intervals))
        , Started(started)
        , Counters(counters)
    {
    }

    void Bootstrap() {
        SortBy(Portions, [](const auto& portion) {
            return portion.PortionId;
        });
        Become(&TThis::StateWork);
        ScheduleNext();
    }

    STRICT_STFUNC(StateWork, hFunc(TEvFindEmptyHistoryIntervals::TEvContinue, Handle); hFunc(TEvResourcesReady, Handle);
                  hFunc(TEvAccessorsReady, Handle); cFunc(TEvents::TEvPoisonPill::EventType, PassAway);)
};

// Always restart these reads from the unmodified scan result after a transaction retry.
template <class TTable>
bool ExcludePersistedBlobs(NIceDb::TNiceDb& db, const NOlap::IBlobGroupSelector& selector, NFindEmptyHistoryIntervals::TIntervalFilter& filter) {
    if (filter.Empty()) {
        return true;
    }
    auto rows = db.Table<TTable>().Select();
    if (!rows.IsReady()) {
        return false;
    }
    while (!rows.EndOfSet()) {
        const auto serialized = rows.template GetValue<typename TTable::BlobId>();
        TString error;
        const auto blob = NOlap::TUnifiedBlobId::ParseFromString(serialized, &selector, error);
        AFL_VERIFY(blob.IsValid())("error", error)("blob", serialized);
        filter.Exclude(blob.GetLogoBlobId());
        if (filter.Empty()) {
            return true;
        }
        if (!rows.Next()) {
            return false;
        }
    }
    return true;
}

}   // namespace

class TTxSaveCutHistoryRequests: public TTransactionBase<TColumnShard> {
    const NFindEmptyHistoryIntervals::TIntervals Intervals;
    const TInstant ScanFinished;
    std::vector<NKikimrTxColumnShard::TCutHistoryRequest> Requests;
    NFindEmptyHistoryIntervals::TIntervals RemainingIntervals;

public:
    TTxSaveCutHistoryRequests(TColumnShard* self, NFindEmptyHistoryIntervals::TIntervals&& intervals, TInstant scanFinished)
        : TBase(self)
        , Intervals(std::move(intervals))
        , ScanFinished(scanFinished)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Self->FindEmptyHistoryIntervals.reset();
        Requests.clear();
        RemainingIntervals.clear();
        if (!Self->CanCutHistory()) {
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        NFindEmptyHistoryIntervals::TIntervalFilter filter(Self->TabletID(), Intervals);
        TBlobGroupSelector selector(Self->Info());
        if (!ExcludePersistedBlobs<Schema::BlobsToDelete>(db, selector, filter) ||
            !ExcludePersistedBlobs<Schema::BlobsToDeleteWT>(db, selector, filter)) {
            return false;
        }
        if (!filter.Empty()) {
            // CanCut rejects this boot after any sharing admission. Loaded links
            // are removed from the runtime manager only after the DB commit.
            Self->GetStoragesManager()
                ->GetSharedBlobsManager()
                ->GetStorageManagerVerified(NOlap::NBlobOperations::TGlobal::DefaultStorageId)
                ->VisitOwnBlobs([&](const TLogoBlobID& blob) {
                    filter.Exclude(blob);
                    return !filter.Empty();
                });
        }
        if (filter.Empty()) {
            return true;
        }
        using T = Schema::CutHistoryRequests;
        auto last = db.Table<T>().Reverse().Range().Select<T::Sequence>();
        if (!last.IsReady()) {
            return false;
        }
        ui64 sequence = last.EndOfSet() ? 0 : last.GetValue<T::Sequence>();
        RemainingIntervals = filter.ExtractIntervals();
        Requests.reserve(Min<ui64>(RemainingIntervals.size(), CutHistoryRequestLimit));
        // Every request sent by Complete must survive this transaction's journal pruning.
        while (!RemainingIntervals.empty() && Requests.size() < CutHistoryRequestLimit) {
            const auto& [key, interval] = *RemainingIntervals.begin();
            auto& request = Requests.emplace_back();
            request.SetTabletID(Self->TabletID());
            request.SetChannel(key.Channel);
            request.SetFromGeneration(key.From);
            request.SetToGeneration(interval.To);
            request.SetGroupID(interval.Group);
            request.SetTimestampUs(ctx.Now().MicroSeconds());
            request.SetSendingGeneration(Self->Generation());
            ActorIdToProto(Self->LauncherID(), request.MutableRecipient());
            db.Table<T>().Key(++sequence).Update(NIceDb::TUpdate<T::RequestProto>(request.SerializeAsString()));
            if (sequence > CutHistoryRequestLimit) {
                db.Table<T>().Key(sequence - CutHistoryRequestLimit).Delete();
            }
            RemainingIntervals.erase(RemainingIntervals.begin());
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Requests.empty()) {
            Self->Counters.GetCSCounters().OnCuttableHistoryIntervalsFound(Requests.size());
        }
        if (Self->CanCutHistory()) {
            for (const auto& request : Requests) {
                auto event = std::make_unique<TEvTablet::TEvCutTabletHistory>();
                event->Record.SetTabletID(Self->TabletID());
                event->Record.SetChannel(request.GetChannel());
                event->Record.SetFromGeneration(request.GetFromGeneration());
                event->Record.SetGroupID(request.GetGroupID());
                ctx.Send(Self->LauncherID(), event.release());
                Self->Counters.GetCSCounters().OnCutHistoryRequestSent(ctx.Now() - ScanFinished);
            }
            if (!RemainingIntervals.empty()) {
                Self->Execute(new TTxSaveCutHistoryRequests(Self, std::move(RemainingIntervals), ScanFinished), ctx);
            }
        }
    }
};

TFindEmptyHistoryIntervalsContext BuildFindEmptyHistoryIntervalsContext(const TTabletStorageInfo& storageInfo, ui32 generation,
    ui32 requiredBarrierGeneration, const NOlap::TColumnEngineForLogs* index, TActorId tabletActorId) {
    TFindEmptyHistoryIntervalsContext result;
    result.TabletId = storageInfo.TabletID;
    result.TabletActorId = tabletActorId;
    for (ui32 channel = FirstDataChannel; channel < storageInfo.Channels.size(); ++channel) {
        const auto& history = storageInfo.Channels[channel].History;
        for (size_t i = 0; i + 1 < history.size(); ++i) {
            AFL_VERIFY(history[i].FromGeneration < history[i + 1].FromGeneration)("channel", channel);
            const ui32 to = history[i + 1].FromGeneration;
            if (to >= generation || to > requiredBarrierGeneration) {
                break;
            }
            result.Intervals.emplace(NFindEmptyHistoryIntervals::THistoryIntervalKey{ channel, history[i].FromGeneration },
                NFindEmptyHistoryIntervals::THistoryInterval{ to, history[i].GroupID });
        }
    }
    if (result.Intervals.empty() || !index) {
        return result;
    }
    const auto add = [&](const auto& portion) {
        if (NFindEmptyHistoryIntervals::MayHaveBlobsInBlobStorage(*portion)) {
            const auto schema = portion->GetSchema(index->GetVersionedIndex());
            result.Portions.push_back({ portion->GetPathId(), portion->GetPortionId(), schema, portion->PredictAccessorsMemory(schema) });
        }
    };
    for (const auto& [_, granule] : index->GetTables()) {
        for (const auto& [_, portion] : granule->GetPortions()) {
            add(portion);
        }
        for (const auto& [_, portion] : granule->GetInsertedPortions()) {
            add(portion);
        }
    }
    return result;
}

IActor* CreateFindEmptyHistoryIntervalsActor(TFindEmptyHistoryIntervalsContext&& context, TActorId resourceSubscriber,
    const TTaskContext& taskContext, const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& config,
    const TCSCounters& counters, TInstant started) {
    return new TFindEmptyHistoryIntervalsActor(std::move(context), resourceSubscriber, taskContext, config, counters, started);
}

bool TColumnShard::CanCutHistory() const {
    return IsCutHistoryEnabled() && SharingSessionsManager->CanCutHistory();
}

TActorId TFindEmptyHistoryIntervals::Start(ui32 completedBarrierGeneration, const TStartContext& context, const TActorContext& ctx) {
    auto scanContext = BuildFindEmptyHistoryIntervalsContext(
        context.StorageInfo, context.Generation, completedBarrierGeneration, context.Index, context.TabletActorId);
    if (scanContext.Intervals.empty()) {
        return {};
    }
    const TTaskContext taskContext("CUT_HISTORY", context.SubscribeCounters);
    ScanActor = ctx.Register(CreateFindEmptyHistoryIntervalsActor(std::move(scanContext), context.ResourceSubscriber, taskContext,
                                 context.Config, context.Counters, ctx.Now()), TMailboxType::HTSwap, AppDataVerified().BatchPoolId);
    return ScanActor;
}

std::optional<TEmptyHistoryIntervalsScanResult> TFindEmptyHistoryIntervals::HandleFinish(
    TEvFindEmptyHistoryIntervals::TEvScanFinished::TPtr& ev, bool canCutHistory, const TCSCounters& counters) {
    if (!ScanActor || ev->Sender != ScanActor) {
        return {};
    }
    ScanActor = {};
    auto& result = ev->Get()->Result;
    if (result.Aborted || !canCutHistory) {
        counters.OnCutHistoryScanAborted();
        return {};
    }
    counters.OnCutHistoryScanFinished(result.Finished - result.Started);
    if (result.Intervals.empty()) {
        return {};
    }
    return std::move(result);
}

void TColumnShard::StartFindEmptyHistoryIntervals(ui32 completedBarrierGeneration, const TActorContext& ctx) {
    const TFindEmptyHistoryIntervals::TStartContext context{ *Info(), Generation(),
        HasIndex() ? &GetIndexAs<NOlap::TColumnEngineForLogs>() : nullptr, SelfId(), ResourceSubscribeActor, Counters.GetSubscribeCounters(),
        ColumnShardConfig->GetCutHistory(), Counters.GetCSCounters() };
    if (const auto actorId = FindEmptyHistoryIntervals->Start(completedBarrierGeneration, context, ctx)) {
        ActorsToStop.push_back(actorId);
    } else {
        FindEmptyHistoryIntervals.reset();
    }
}

void TColumnShard::Handle(TEvFindEmptyHistoryIntervals::TEvScanFinished::TPtr& ev, const TActorContext& ctx) {
    if (!FindEmptyHistoryIntervals) {
        return;
    }
    if (auto result = FindEmptyHistoryIntervals->HandleFinish(ev, CanCutHistory(), Counters.GetCSCounters())) {
        Execute(new TTxSaveCutHistoryRequests(this, std::move(result->Intervals), result->Finished), ctx);
    } else if (FindEmptyHistoryIntervals->IsFinished()) {
        FindEmptyHistoryIntervals.reset();
    }
}

}   // namespace NKikimr::NColumnShard
