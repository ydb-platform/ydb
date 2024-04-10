#pragma once
#include "defs.h"
#include "keyvalue.h"

#include "keyvalue_collector.h"
#include "keyvalue_scheme_flat.h"
#include "keyvalue_simple_db.h"
#include "keyvalue_simple_db_flat.h"
#include "keyvalue_state.h"

#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/public/lib/base/msgbus.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/protos/counters_keyvalue.pb.h>
#include <ydb/core/util/stlog.h>
#include <util/string/escape.h>

// Uncomment the following macro to enable consistency check before every transactions in TTxRequest
//#define KIKIMR_KEYVALUE_CONSISTENCY_CHECKS

namespace NKikimr {
namespace NKeyValue {

constexpr ui64 CollectorErrorInitialBackoffMs = 10;
constexpr ui64 CollectorErrorMaxBackoffMs = 5000;
constexpr ui64 CollectorMaxErrors = 20;
constexpr ui64 PeriodicRefreshMs = 15000;

class TKeyValueFlat : public TActor<TKeyValueFlat>, public NTabletFlatExecutor::TTabletExecutedFlat {
protected:
    struct TTxInit : public NTabletFlatExecutor::ITransaction {
        TActorId KeyValueActorId;
        TKeyValueFlat &Self;
        TVector<TLogoBlobID> TrashBeingCommitted;

        TTxInit(TActorId keyValueActorId, TKeyValueFlat &keyValueFlat)
            : KeyValueActorId(keyValueActorId)
            , Self(keyValueFlat)
        {}

        TTxType GetTxType() const override { return TXTYPE_INIT; }

        void ApplyLogBatching(const TActorContext &ctx, auto &alter) {
            if (NKikimr::AppData(ctx)->FeatureFlags.GetEnableKeyvalueLogBatching()) { 
                alter.SetExecutorAllowLogBatching(true);
                alter.SetExecutorLogFlushPeriod(TDuration::MicroSeconds(500));
            } else {
                alter.SetExecutorAllowLogBatching(false);
            }
        }

        bool Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) override {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << txc.Tablet << " TTxInit flat Execute");
            TSimpleDbFlat db(txc.DB, TrashBeingCommitted);
            if (txc.DB.GetScheme().GetTableInfo(TABLE_ID) == nullptr) {
                LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << txc.Tablet << " TTxInit flat BuildScheme");
                // Init the scheme
                auto &alter = txc.DB.Alter();
                alter.AddTable("kvtable", TABLE_ID);
                alter.AddColumn(TABLE_ID, "key", KEY_TAG, NScheme::TSmallBoundedString::TypeId, false);
                alter.AddColumnToKey(TABLE_ID, KEY_TAG);
                alter.AddColumn(TABLE_ID, "value", VALUE_TAG, NScheme::TString::TypeId, false);
                ApplyLogBatching(ctx, alter);
                Self.State.Clear();
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << txc.Tablet << " TTxInit flat ReadDb Tree");
                if (!LoadStateFromDB(Self.State, txc.DB)) {
                    return false;
                }
                if (Self.State.GetIsDamaged()) {
                    Self.Become(&TKeyValueFlat::StateBroken);
                    ctx.Send(Self.Tablet(), new TEvents::TEvPoisonPill);
                    return true;
                }
                Self.State.UpdateResourceMetrics(ctx);
                // auto &alter = txc.DB.Alter();
                // ApplyLogBatching(ctx, alter);
            }
            Self.State.InitExecute(Self.TabletID(), KeyValueActorId, txc.Generation, db, ctx, Self.Info());
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << txc.Tablet
                    << " TTxInit flat Execute returns true");
            return true;
        }

        static bool LoadStateFromDB(TKeyValueState& state, NTable::TDatabase& db) {
            state.Clear();
            // Just walk through the DB and read all the keys and values
            const std::array<ui32, 2> tags {{ KEY_TAG, VALUE_TAG }};
            auto mode = NTable::ELookup::GreaterOrEqualThan;
            auto iter = db.Iterate(TABLE_ID, {}, tags, mode);

            if (!db.Precharge(TABLE_ID, {}, {}, tags, 0, -1, -1))
                return false;

            while (iter->Next(NTable::ENext::Data) == NTable::EReady::Data) {
                const auto &row = iter->Row();

                TString key(row.Get(0).AsBuf());
                TString value(row.Get(1).AsBuf());

                state.Load(key, value);
                if (state.GetIsDamaged()) {
                    return true;
                }
            }

            return iter->Last() != NTable::EReady::Page;
        }

        void Complete(const TActorContext &ctx) override {
            Self.State.InitComplete(ctx, Self.Info());
            Self.State.PushTrashBeingCommitted(TrashBeingCommitted, ctx);
            Self.InitSchemeComplete(ctx);
            Self.CreatedHook(ctx);
        }
    };

    struct TTxRequest : public NTabletFlatExecutor::ITransaction {
        THolder<TIntermediate> Intermediate;
        TKeyValueFlat *Self;
        TVector<TLogoBlobID> TrashBeingCommitted;

        TTxRequest(THolder<TIntermediate> intermediate, TKeyValueFlat *keyValueFlat, NWilson::TTraceId &&traceId)
            : NTabletFlatExecutor::ITransaction(std::move(traceId))
            , Intermediate(std::move(intermediate))
            , Self(keyValueFlat)
        {
            Intermediate->Response.SetStatus(NMsgBusProxy::MSTATUS_UNKNOWN);
        }

        TTxType GetTxType() const override { return TXTYPE_REQUEST; }

        bool Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) override {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << txc.Tablet << " TTxRequest Execute");
            if (!CheckConsistency(txc)) {
                return false;
            }
            if (Self->State.GetIsDamaged()) {
                return true;
            }
            if (Intermediate->SetExecutorFastLogPolicy) {
                txc.DB.Alter().SetExecutorFastLogPolicy(Intermediate->SetExecutorFastLogPolicy->IsAllowed);
            }
            TSimpleDbFlat db(txc.DB, TrashBeingCommitted);
            Self->State.RequestExecute(Intermediate, db, ctx, Self->Info());
            return true;
        }

        void Complete(const TActorContext &ctx) override {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << Self->TabletID() << " TTxRequest Complete");
            Self->State.PushTrashBeingCommitted(TrashBeingCommitted, ctx);
            Self->State.RequestComplete(Intermediate, ctx, Self->Info());
        }

        bool CheckConsistency(NTabletFlatExecutor::TTransactionContext &txc) {
#ifdef KIKIMR_KEYVALUE_CONSISTENCY_CHECKS
            TKeyValueState state;
            if (!TTxInit::LoadStateFromDB(state, txc.DB)) {
                return false;
            }
            Y_ABORT_UNLESS(!state.IsDamaged());
            state.VerifyEqualIndex(Self->State);
            txc.DB.NoMoreReadsForTx();
            return true;
#else
            Y_UNUSED(txc);
            return true;
#endif
        }
    };

    struct TTxDropRefCountsOnError : NTabletFlatExecutor::ITransaction {
        std::deque<std::pair<TLogoBlobID, bool>> RefCountsIncr;
        TKeyValueFlat *Self;
        TVector<TLogoBlobID> TrashBeingCommitted;

        TTxDropRefCountsOnError(std::deque<std::pair<TLogoBlobID, bool>>&& refCountsIncr, TKeyValueFlat *self)
            : RefCountsIncr(std::move(refCountsIncr))
            , Self(self)
        {}

        TTxType GetTxType() const override { return TXTYPE_DROP_REF_COUNTS_ON_ERROR; }

        bool Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) override {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << Self->TabletID() << " TTxDropRefCountsOnError Execute");
            if (!Self->State.GetIsDamaged()) {
                TSimpleDbFlat db(txc.DB, TrashBeingCommitted);
                Self->State.DropRefCountsOnErrorInTx(std::move(RefCountsIncr), db, ctx);
            }
            return true;
        }

        void Complete(const TActorContext &ctx) override {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << Self->TabletID() << " TTxDropRefCountsOnError Complete");
            Self->State.PushTrashBeingCommitted(TrashBeingCommitted, ctx);
        }
    };

    struct TTxMonitoring : public NTabletFlatExecutor::ITransaction {
        const THolder<NMon::TEvRemoteHttpInfo> Event;
        const TActorId RespondTo;
        TKeyValueFlat *Self;

        TTxMonitoring(THolder<NMon::TEvRemoteHttpInfo> event, const TActorId &respondTo, TKeyValueFlat *keyValue)
            : Event(std::move(event))
            , RespondTo(respondTo)
            , Self(keyValue)
        {}

        bool Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) override {
            Y_UNUSED(txc);
            TStringStream str;
            THolder<IEventBase> response;
            TCgiParameters params(Event->Cgi());
            if (params.Has("section")) {
                const TString section = params.Get("section");
                NJson::TJsonValue json;
                if (section == "channelstat") {
                    Self->State.MonChannelStat(json);
                } else {
                    json["Error"] = "invalid json parameter value";
                }
                NJson::WriteJson(&str, &json);
                response = MakeHolder<NMon::TEvRemoteJsonInfoRes>(str.Str());
            } else {
                Self->State.RenderHTMLPage(str);
                response = MakeHolder<NMon::TEvRemoteHttpInfoRes>(str.Str());
            }
            ctx.Send(RespondTo, response.Release());
            return true;
        }

        void Complete(const TActorContext &ctx) override {
            Y_UNUSED(ctx);
        }
    };

    using TExecuteMethod = void (TKeyValueState::*)(ISimpleDb &db, const TActorContext &ctx);
    using TCompleteMethod = void (TKeyValueState::*)(const TActorContext &ctx, const TTabletStorageInfo *info);

    template <typename TDerived, TExecuteMethod ExecuteMethod, TCompleteMethod CompleteMethod>
    struct TTxUniversal : NTabletFlatExecutor::ITransaction {
        TKeyValueFlat *Self;
        TVector<TLogoBlobID> TrashBeingCommitted;

        TTxUniversal(TKeyValueFlat *keyValueFlat)
            : Self(keyValueFlat)
        {}

        bool Execute(NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx) override {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << txc.Tablet << ' ' << TDerived::Name << " Execute");
            TSimpleDbFlat db(txc.DB, TrashBeingCommitted);
            (Self->State.*ExecuteMethod)(db, ctx);
            return true;
        }

        void Complete(const TActorContext &ctx) override {
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << Self->TabletID()
                    << ' ' << TDerived::Name << " Complete");
            Self->State.PushTrashBeingCommitted(TrashBeingCommitted, ctx);
            (Self->State.*CompleteMethod)(ctx, Self->Info());
        }
    };

#ifdef KV_SIMPLE_TX
#error "KV_SIMPLE_TX already exist"
#else
#define KV_SIMPLE_TX(name) \
    struct TTx ## name : public TTxUniversal<TTx ## name, \
            &TKeyValueState:: name ## Execute, \
            &TKeyValueState:: name ## Complete> \
    { \
        static constexpr auto Name = "TTx" #name; \
        using TTxUniversal::TTxUniversal; \
    }
#endif

    KV_SIMPLE_TX(RegisterInitialGCCompletion);
    KV_SIMPLE_TX(CompleteGC);

    TKeyValueState State;
    TDeque<TAutoPtr<IEventHandle>> InitialEventsQueue;
    TActorId CollectorActorId;

    void OnDetach(const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID() << " OnDetach");
        HandleDie(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                << " OnTabletDead " << ev->Get()->ToString());
        HandleDie(ctx);
    }

    void DefaultSignalTabletActive(const TActorContext &) final {
        // must be empty
    }

    void OnActivateExecutor(const TActorContext &ctx) override {
        Executor()->RegisterExternalTabletCounters(State.TakeTabletCounters());
        State.SetupResourceMetrics(Executor()->GetResourceMetrics());
        ctx.Schedule(TDuration::MilliSeconds(PeriodicRefreshMs), new TEvKeyValue::TEvPeriodicRefresh);
        Execute(new TTxInit(ctx.SelfID, *this), ctx);
    }

    void Enqueue(STFUNC_SIG) override {
        SetActivityType(NKikimrServices::TActivity::KEYVALUE_ACTOR);
        ALOG_DEBUG(NKikimrServices::KEYVALUE,
                "KeyValue# " << TabletID()
                << " Enqueue, event type# " << (ui32)ev->GetTypeRewrite()
                << " event# " << ev->ToString());
        InitialEventsQueue.push_back(ev);
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // gRPC

    void Handle(TEvKeyValue::TEvRead::TPtr &ev) {
        State.OnEvReadRequest(ev, TActivationContext::AsActorContext(), Info());
    }

    void Handle(TEvKeyValue::TEvReadRange::TPtr &ev) {
        State.OnEvReadRangeRequest(ev, TActivationContext::AsActorContext(), Info());
    }

    void Handle(TEvKeyValue::TEvExecuteTransaction::TPtr &ev) {
        State.OnEvExecuteTransaction(ev, TActivationContext::AsActorContext(), Info());
    }

    void Handle(TEvKeyValue::TEvGetStorageChannelStatus::TPtr &ev) {
        State.OnEvGetStorageChannelStatus(ev, TActivationContext::AsActorContext(), Info());
    }

    void Handle(TEvKeyValue::TEvAcquireLock::TPtr &ev) {
        State.OnEvAcquireLock(ev, TActivationContext::AsActorContext(), Info());
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Online state
    void Handle(TEvKeyValue::TEvCompleteGC::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                << " Handle TEvCompleteGC " << ev->Get()->ToString());
        Y_ABORT_UNLESS(ev->Sender == (ev->Get()->Repeat ? ctx.SelfID : CollectorActorId));
        CollectorActorId = {};
        State.OnEvCompleteGC(ev->Get()->Repeat);
        Execute(new TTxCompleteGC(this), ctx);
    }

    void Handle(TEvKeyValue::TEvCollect::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                << " Handle TEvCollect " << ev->Get()->ToString());
        if (State.OnEvCollect(ctx)) {
            auto& operation = State.GetCollectOperation();
            STLOG(PRI_DEBUG, KEYVALUE_GC, KVC09, "TEvCollect", (TabletId, TabletID()),
                (Generation, Executor()->Generation()),
                (PerGenerationCounter, State.GetPerGenerationCounter()),
                (AdvanceBarrier, operation->AdvanceBarrier),
                (CollectGeneration, operation->Header.CollectGeneration),
                (CollectStep, operation->Header.CollectStep),
                (KeepCount, operation->Keep.size()),
                (DoNotKeepCount, operation->DoNotKeep.size()),
                (TrashGoingToCollectCount, operation->TrashGoingToCollect.size()),
                (TrashCount, State.GetTrashCount()));
            CollectorActorId = ctx.Register(CreateKeyValueCollector(ctx.SelfID, operation, Info(),
                Executor()->Generation(), State.GetPerGenerationCounter()));
            State.OnEvCollectDone(ctx);
        } else {
            LOG_ERROR_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                    << " Handle TEvCollect: PerGenerationCounter overflow prevention restart.");
            Become(&TThis::StateBroken);
            ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
        }
    }

    void CheckYellowChannels(TRequestStat& stat) {
        IExecutor* executor = Executor();
        if ((stat.YellowMoveChannels || stat.YellowStopChannels) && executor) {
            executor->OnYellowChannels(std::move(stat.YellowMoveChannels), std::move(stat.YellowStopChannels));
        }
    }

    void Handle(TEvKeyValue::TEvIntermediate::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                << " Handle TEvIntermediate " << ev->Get()->ToString());

        CheckYellowChannels(ev->Get()->Intermediate->Stat);

        State.OnEvIntermediate(*(ev->Get()->Intermediate), ctx);
        auto traceId = ev->Get()->Intermediate->Span.GetTraceId();
        Execute(new TTxRequest(std::move(ev->Get()->Intermediate), this, std::move(traceId)), ctx);
    }

    void Handle(TEvKeyValue::TEvNotify::TPtr &ev, const TActorContext &ctx) {
        TEvKeyValue::TEvNotify &event = *ev->Get();
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                << " Handle TEvNotify " << event.ToString());

        CheckYellowChannels(ev->Get()->Stat);
        State.OnRequestComplete(event.RequestUid, event.Generation, event.Step, ctx, Info(), event.Status, event.Stat);
        State.DropRefCountsOnError(event.RefCountsIncr, true, ctx);
        if (!event.RefCountsIncr.empty()) {
            Execute(new TTxDropRefCountsOnError(std::move(event.RefCountsIncr), this), ctx);
        }
    }

    void Handle(TEvBlobStorage::TEvCollectGarbageResult::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
            << " Handle TEvCollectGarbageResult Cookie# " << ev->Cookie <<  " Marker# KV52");

        if (ev->Cookie != (ui64)TKeyValueState::ECollectCookie::SoftInitial &&
                ev->Cookie != (ui64)TKeyValueState::ECollectCookie::Hard) {
            LOG_CRIT_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                << " received TEvCollectGarbageResult with unexpected Cookie# " << ev->Cookie);
            Send(SelfId(), new TKikimrEvents::TEvPoisonPill);
            return;
        }

        NKikimrProto::EReplyStatus status = ev->Get()->Status;
        if (status != NKikimrProto::OK) {
            LOG_ERROR_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                << " received not ok TEvCollectGarbageResult"
                << " Status# " << NKikimrProto::EReplyStatus_Name(status)
                << " ErrorReason# " << ev->Get()->ErrorReason);
            Send(SelfId(), new TKikimrEvents::TEvPoisonPill);
            return;
        }

        bool isLast = State.RegisterInitialCollectResult(ctx, Info());
        if (isLast) {
            Execute(new TTxRegisterInitialGCCompletion(this));
        }
    }

    void Handle(TEvKeyValue::TEvRequest::TPtr ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                << " Handle TEvRequest " << ev->Get()->ToString());
        UpdateTabletYellow();
        State.OnEvRequest(ev, ctx, Info());
    }

    void Handle(TEvKeyValue::TEvPeriodicRefresh::TPtr ev, const TActorContext &ctx) {
        Y_UNUSED(ev);
        LOG_TRACE_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID() << " Handle TEvPeriodicRefresh");
        ctx.Schedule(TDuration::MilliSeconds(PeriodicRefreshMs), new TEvKeyValue::TEvPeriodicRefresh);
        State.OnPeriodicRefresh(ctx);
    }

    void Handle(TChannelBalancer::TEvUpdateWeights::TPtr ev, const TActorContext& /*ctx*/) {
        State.OnUpdateWeights(ev);
    }

    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override {
        if (!Executor() || !Executor()->GetStats().IsActive)
            return false;

        if (!ev)
            return true;

        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID() << " Handle TEvRemoteHttpInfo: %s"
                << ev->Get()->Query.data());
        Execute(new TTxMonitoring(ev->Release(), ev->Sender, this), ctx);

        return true;
    }

    void Handle(TEvents::TEvPoisonPill::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID() << " Handle TEvents::TEvPoisonPill");
        Y_UNUSED(ev);
        Become(&TThis::StateBroken);
        ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
    }

    void RestoreActorActivity() {
        SetActivityType(NKikimrServices::TActivity::KEYVALUE_ACTOR);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KEYVALUE_ACTOR;
    }

    TKeyValueFlat(const TActorId &tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    {
        TAutoPtr<TTabletCountersBase> counters(
        new TProtobufTabletCounters<
                ESimpleCounters_descriptor,
                ECumulativeCounters_descriptor,
                EPercentileCounters_descriptor,
                ETxTypes_descriptor
            >());
        State.SetupTabletCounters(counters);
        State.Clear();
    }

    virtual void HandleDie(const TActorContext &ctx)
    {
        if (CollectorActorId) {
            ctx.Send(CollectorActorId, new TEvents::TEvPoisonPill);
        }
        State.Terminate(ctx);
        Die(ctx);
    }

    virtual void CreatedHook(const TActorContext &ctx)
    {
        SignalTabletActive(ctx);
    }

    virtual bool HandleHook(STFUNC_SIG)
    {
        Y_UNUSED(ev);
        return false;
    }

    STFUNC(StateInit) {
        RestoreActorActivity();
        ALOG_DEBUG(NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                << " StateInit flat event type# " << (ui32)ev->GetTypeRewrite()
                << " event# " << ev->ToString());
        StateInitImpl(ev, SelfId());
    }

    STFUNC(StateWork) {
        if (HandleHook(ev))
            return;
        RestoreActorActivity();
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKeyValue::TEvRead, Handle);
            hFunc(TEvKeyValue::TEvReadRange, Handle);
            hFunc(TEvKeyValue::TEvExecuteTransaction, Handle);
            hFunc(TEvKeyValue::TEvGetStorageChannelStatus, Handle);
            hFunc(TEvKeyValue::TEvAcquireLock, Handle);

            HFunc(TEvKeyValue::TEvCompleteGC, Handle);
            HFunc(TEvKeyValue::TEvCollect, Handle);
            HFunc(TEvKeyValue::TEvRequest, Handle);
            HFunc(TEvKeyValue::TEvIntermediate, Handle);
            HFunc(TEvKeyValue::TEvNotify, Handle);
            HFunc(TEvKeyValue::TEvPeriodicRefresh, Handle);
            HFunc(TChannelBalancer::TEvUpdateWeights, Handle);
            HFunc(TEvBlobStorage::TEvCollectGarbageResult, Handle);
            HFunc(TEvents::TEvPoisonPill, Handle);

            default:
                if (!HandleDefaultEvents(ev, SelfId())) {
                    ALOG_DEBUG(NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                            << " StateWork unexpected event type# " << (ui32)ev->GetTypeRewrite()
                            << " event# " << ev->ToString());
                }
                break;
        }
    }

    STFUNC(StateBroken) {
        RestoreActorActivity();
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvTabletDead, HandleTabletDead)

            default:
                ALOG_DEBUG(NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                        << " BrokenState unexpected event type# " << (ui32)ev->GetTypeRewrite()
                        << " event# " << ev->ToString());
                break;
        }
    }

    void InitSchemeComplete(const TActorContext &ctx) {
        Become(&TThis::StateWork);
        State.OnStateWork(ctx);
        UpdateTabletYellow();
        while (!InitialEventsQueue.empty()) {
            TAutoPtr<IEventHandle> &ev = InitialEventsQueue.front();
            LOG_DEBUG_S(ctx, NKikimrServices::KEYVALUE, "KeyValue# " << TabletID()
                    << " Dequeue, event type# " << (ui32)ev->GetTypeRewrite()
                    << " event# " << ev->ToString());
            ctx.ExecutorThread.Send(ev.Release());
            InitialEventsQueue.pop_front();
        }
        State.OnInitQueueEmpty(ctx);
    }

    void UpdateTabletYellow() {
        if (Executor()) {
            State.SetTabletYellowMove(Executor()->GetStats().IsAnyChannelYellowMove);
            State.SetTabletYellowStop(Executor()->GetStats().IsAnyChannelYellowStop);
        } else {
            State.SetTabletYellowMove(true);
            State.SetTabletYellowStop(true);
        }

    }

    bool ReassignChannelsEnabled() const override {
        return true;
    }
};


}// NKeyValue
}// NKikimr
