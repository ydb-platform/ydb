#include "columnshard_find_empty_history_intervals.h"
#include "columnshard_impl.h"

#include "find_empty_history_intervals/portions.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/storage/granule/granule.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/random/random.h>

namespace NKikimr::NColumnShard {

bool IsCutHistoryEnabled() {
    return AppData()->FeatureFlags.GetEnableCutHistory() && AppData()->FeatureFlags.GetEnableColumnshardCutHistory();
}

namespace {

using TCachePolicy = NOlap::NGeneralCache::TPortionsMetadataCachePolicy;
using TAddress = TCachePolicy::TAddress;
using TCache = NGeneralCache::TServiceOperator<TCachePolicy>;

TDuration ComputeContinuationDelay(const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& config) {
    const ui64 jitter = config.GetContinuationJitterMs();
    return TDuration::MilliSeconds(Max<ui32>(1, config.GetContinuationDelayMs()) + (jitter ? RandomNumber<ui64>(jitter + 1) : 0));
}

struct TEvAccessorsReady: NActors::TEventLocal<TEvAccessorsReady, TEvColumnShard::EvFindEmptyHistoryIntervalsAccessorsReady> {
    THashMap<TAddress, TCachePolicy::TObject> Accessors;
    bool Failed;

    TEvAccessorsReady(THashMap<TAddress, TCachePolicy::TObject>&& accessors, bool failed)
        : Accessors(std::move(accessors))
        , Failed(failed)
    {
    }
};

class TMetadataCallback: public TCache::ICallback {
    TActorId Owner;
    std::shared_ptr<TAtomicCounter> Aborted;

    bool DoIsAborted() const override {
        return Aborted->Val();
    }

    void DoOnResultReady(THashMap<TCachePolicy::TAddress, TCachePolicy::TObject>&& accessors, THashSet<TCachePolicy::TAddress>&& removed,
        NGeneralCache::NPublic::TErrorAddresses<TCachePolicy>&& errors) override {
        TActivationContext::Send(Owner, std::make_unique<TEvAccessorsReady>(std::move(accessors), !removed.empty() || errors.HasErrors()));
    }

public:
    TMetadataCallback(TActorId owner, const std::shared_ptr<TAtomicCounter>& aborted)
        : Owner(owner)
        , Aborted(aborted)
    {
    }
};

class TFindEmptyHistoryIntervalsActor: public TActorBootstrapped<TFindEmptyHistoryIntervalsActor> {
    TActorId Owner;
    NKikimrConfig::TColumnShardConfig::TCutHistoryConfig Config;
    std::vector<TFindEmptyHistoryIntervalsContext::TPortion> Portions;
    NFindEmptyHistoryIntervals::THistoryIntervals HistoryIntervals;
    THashMap<TAddress, NOlap::ISnapshotSchema::TPtr> BatchPortionSchemas;
    std::shared_ptr<TAtomicCounter> Aborted = std::make_shared<TAtomicCounter>(0);
    size_t Position = 0;
    TInstant Started;
    TCSCounters Counters;

    void Finish(bool aborted = false) {
        TEmptyHistoryIntervalsScanResult result;
        result.Intervals = HistoryIntervals.ExtractIntervals();
        result.Started = Started;
        result.Finished = TActivationContext::Now();
        result.Aborted = aborted;
        Send(Owner, new TEvFindEmptyHistoryIntervals::TEvScanFinished(std::move(result)));
        PassAway();
    }

    void ScheduleNext() {
        Schedule(ComputeContinuationDelay(Config), new TEvFindEmptyHistoryIntervals::TEvContinue());
    }

    void Handle(TEvFindEmptyHistoryIntervals::TEvContinue::TPtr&) {
        if (Position == Portions.size() || HistoryIntervals.Empty()) {
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
        THashSet<TAddress> addresses;
        addresses.reserve(BatchPortionSchemas.size());
        for (const auto& [address, _] : BatchPortionSchemas) {
            addresses.emplace(address);
        }
        TCache::AskObjects(TCachePolicy::EConsumer::SCAN, std::move(addresses), std::make_shared<TMetadataCallback>(SelfId(), Aborted));
    }

    void Handle(TEvAccessorsReady::TPtr& ev) {
        if (ev->Get()->Failed || ev->Get()->Accessors.size() != BatchPortionSchemas.size() || !IsCutHistoryEnabled()) {
            Finish(true);
            return;
        }
        for (const auto& [address, accessor] : ev->Get()->Accessors) {
            const auto* schema = BatchPortionSchemas.FindPtr(address);
            AFL_VERIFY(schema);
            NFindEmptyHistoryIntervals::ExcludePortionBlobs(*accessor, (*schema)->GetIndexInfo(), [this](const auto& blob) {
                HistoryIntervals.Exclude(blob.GetLogoBlobId(), blob.GetDsGroup(), [this] {
                    Counters.OnCutHistoryBlobGroupMismatch();
                });
            });
        }
        BatchPortionSchemas.clear();
        if (HistoryIntervals.Empty() || Position == Portions.size()) {
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
    TFindEmptyHistoryIntervalsActor(TFindEmptyHistoryIntervalsContext&& context,
        const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& config, const TCSCounters& counters, TInstant started)
        : Owner(context.TabletActorId)
        , Config(config)
        , Portions(std::move(context.Portions))
        , HistoryIntervals(context.TabletId, std::move(context.Intervals))
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

    STRICT_STFUNC(StateWork, hFunc(TEvFindEmptyHistoryIntervals::TEvContinue, Handle);
                  hFunc(TEvAccessorsReady, Handle); cFunc(TEvents::TEvPoisonPill::EventType, PassAway);)
};

template <class TTable>
bool ExcludePersistedBlobs(
    NIceDb::TNiceDb& db, const NOlap::IBlobGroupSelector& selector, NFindEmptyHistoryIntervals::THistoryIntervals& historyIntervals) {
    if (historyIntervals.Empty()) {
        return true;
    }
    auto rows = [&] {
        if constexpr (requires { typename TTable::StorageId; }) {
            return db.Table<TTable>().Prefix(NOlap::NBlobOperations::TGlobal::DefaultStorageId).template Select<typename TTable::BlobId>();
        } else {
            return db.Table<TTable>().template Select<typename TTable::BlobId>();
        }
    }();
    if (!rows.IsReady()) {
        return false;
    }
    while (!rows.EndOfSet()) {
        const auto serialized = rows.template GetValue<typename TTable::BlobId>();
        TString error;
        const auto blob = NOlap::TUnifiedBlobId::ParseFromString(serialized, &selector, error);
        AFL_VERIFY(blob.IsValid())("error", error)("blob", serialized);
        historyIntervals.Exclude(blob.GetLogoBlobId());
        if (historyIntervals.Empty()) {
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
    enum class EReadStage {
        SharedBlobs,
        BorrowedBlobs,
        BlobsToDelete,
        BlobsToDeleteWT,
        Journal,
    };

    EReadStage ReadStage = EReadStage::SharedBlobs;
    NFindEmptyHistoryIntervals::THistoryIntervals HistoryIntervals;
    const TInstant ScanFinished;
    NKikimrTxColumnShard::TCutHistoryRequests Requests;

public:
    TTxSaveCutHistoryRequests(TColumnShard* self, NFindEmptyHistoryIntervals::THistoryIntervals::TIntervals&& intervals, TInstant scanFinished)
        : TBase(self)
        , HistoryIntervals(self->TabletID(), std::move(intervals))
        , ScanFinished(scanFinished)
    {
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        Self->FindEmptyHistoryIntervals.reset();
        if (!IsCutHistoryEnabled()) {
            return true;
        }
        NIceDb::TNiceDb db(txc.DB);
        TBlobGroupSelector selector(Self->Info());
        switch (ReadStage) {
            case EReadStage::SharedBlobs:
                if (!ExcludePersistedBlobs<Schema::SharedBlobIds>(db, selector, HistoryIntervals)) {
                    return false;
                }
                ReadStage = EReadStage::BorrowedBlobs;
                [[fallthrough]];
            case EReadStage::BorrowedBlobs:
                if (!ExcludePersistedBlobs<Schema::BorrowedBlobIds>(db, selector, HistoryIntervals)) {
                    return false;
                }
                ReadStage = EReadStage::BlobsToDelete;
                [[fallthrough]];
            case EReadStage::BlobsToDelete:
                if (!ExcludePersistedBlobs<Schema::BlobsToDelete>(db, selector, HistoryIntervals)) {
                    return false;
                }
                ReadStage = EReadStage::BlobsToDeleteWT;
                [[fallthrough]];
            case EReadStage::BlobsToDeleteWT:
                if (!ExcludePersistedBlobs<Schema::BlobsToDeleteWT>(db, selector, HistoryIntervals)) {
                    return false;
                }
                ReadStage = EReadStage::Journal;
                [[fallthrough]];
            case EReadStage::Journal:
                break;
        }
        if (HistoryIntervals.Empty()) {
            return true;
        }
        using T = Schema::CutHistoryRequests;
        auto last = db.Table<T>().Reverse().Range().Select<T::Sequence>();
        if (!last.IsReady()) {
            return false;
        }
        const ui64 sequence = last.EndOfSet() ? 1 : last.GetValue<T::Sequence>() + 1;
        for (const auto& [key, interval] : HistoryIntervals.GetIntervals()) {
            auto& request = *Requests.AddRequests();
            request.SetTabletID(Self->TabletID());
            request.SetChannel(key.Channel);
            request.SetFromGeneration(key.From);
            request.SetToGeneration(interval.To);
            request.SetGroupID(interval.Group);
            request.SetTimestampUs(ctx.Now().MicroSeconds());
            request.SetSendingGeneration(Self->Generation());
            ActorIdToProto(Self->LauncherID(), request.MutableRecipient());
        }
        db.Table<T>().Key(sequence).Update(NIceDb::TUpdate<T::RequestsProto>(Requests.SerializeAsString()));
        if (sequence > CutHistoryJournalLimit) {
            db.Table<T>().Key(sequence - CutHistoryJournalLimit).Delete();
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Requests.RequestsSize()) {
            Self->Counters.GetCSCounters().OnCuttableHistoryIntervalsFound(Requests.RequestsSize());
        }
        if (IsCutHistoryEnabled()) {
            for (const auto& request : Requests.GetRequests()) {
                auto event = std::make_unique<TEvTablet::TEvCutTabletHistory>();
                event->Record.SetTabletID(Self->TabletID());
                event->Record.SetChannel(request.GetChannel());
                event->Record.SetFromGeneration(request.GetFromGeneration());
                event->Record.SetGroupID(request.GetGroupID());
                ctx.Send(Self->LauncherID(), event.release());
                Self->Counters.GetCSCounters().OnCutHistoryRequestSent(ctx.Now() - ScanFinished);
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
        const auto schema = portion->GetSchema(index->GetVersionedIndex());
        if (portion->HasBlobsInBlobStorage(schema->GetIndexInfo())) {
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

IActor* CreateFindEmptyHistoryIntervalsActor(TFindEmptyHistoryIntervalsContext&& context,
    const NKikimrConfig::TColumnShardConfig::TCutHistoryConfig& config, const TCSCounters& counters, TInstant started) {
    return new TFindEmptyHistoryIntervalsActor(std::move(context), config, counters, started);
}

TActorId TFindEmptyHistoryIntervals::Start(ui32 completedBarrierGeneration, const TStartContext& context, const TActorContext& ctx) {
    auto scanContext = BuildFindEmptyHistoryIntervalsContext(
        context.StorageInfo, context.Generation, completedBarrierGeneration, context.Index, context.TabletActorId);
    if (scanContext.Intervals.empty()) {
        return {};
    }
    ScanActor = ctx.Register(CreateFindEmptyHistoryIntervalsActor(std::move(scanContext), context.Config, context.Counters, ctx.Now()),
        TMailboxType::HTSwap, AppDataVerified().BatchPoolId);
    return ScanActor;
}

std::optional<TEmptyHistoryIntervalsScanResult> TFindEmptyHistoryIntervals::HandleFinish(
    TEvFindEmptyHistoryIntervals::TEvScanFinished::TPtr& ev, bool canCutHistory, const TCSCounters& counters) {
    AFL_VERIFY(ScanActor && ev->Sender == ScanActor);
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
        HasIndex() ? &GetIndexAs<NOlap::TColumnEngineForLogs>() : nullptr, SelfId(), ColumnShardConfig->GetCutHistory(),
        Counters.GetCSCounters() };
    if (const auto actorId = FindEmptyHistoryIntervals->Start(completedBarrierGeneration, context, ctx)) {
        ActorsToStop.push_back(actorId);
    } else {
        FindEmptyHistoryIntervals.reset();
    }
}

void TColumnShard::Handle(TEvFindEmptyHistoryIntervals::TEvScanFinished::TPtr& ev, const TActorContext& ctx) {
    AFL_VERIFY(FindEmptyHistoryIntervals);
    if (auto result = FindEmptyHistoryIntervals->HandleFinish(ev, IsCutHistoryEnabled(), Counters.GetCSCounters())) {
        Execute(new TTxSaveCutHistoryRequests(this, std::move(result->Intervals), result->Finished), ctx);
    } else {
        FindEmptyHistoryIntervals.reset();
    }
}

}   // namespace NKikimr::NColumnShard
