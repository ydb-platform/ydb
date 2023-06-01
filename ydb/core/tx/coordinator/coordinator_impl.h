#pragma once

#include "defs.h"
#include "coordinator.h"

#include <ydb/core/protos/counters_coordinator.pb.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/util/queue_oneone_inplace.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>

#include <util/generic/hash_set.h>
#include <util/stream/file.h>
#include <util/stream/zlib.h>

#include <algorithm>

namespace NKikimr {
namespace NFlatTxCoordinator {

typedef ui64 TStepId;
typedef ui64 TTabletId;
typedef ui64 TTxId;

struct TTransactionProposal {
    TActorId Proxy;

    TTxId TxId; // could be zero, then transaction is not saved a-priori and must be idempotent, with body provided
    TStepId MinStep; // not plan before (tablet would be not ready), could be zero
    TStepId MaxStep; // not plan after (tablet would trash tx body).

    struct TAffectedEntry {
        enum {
            AffectedRead = 1 << 0,
            AffectedWrite = 1 << 1,
        };

        TTabletId TabletId;
        ui64 AffectedFlags : 8;
        // reserved
    };

    TVector<TAffectedEntry> AffectedSet;

    TInstant AcceptMoment;
    bool IgnoreLowDiskSpace;

    TTransactionProposal(const TActorId &proxy, TTxId txId, TStepId minStep, TStepId maxStep, bool ignoreLowDiskSpace)
        : Proxy(proxy)
        , TxId(txId)
        , MinStep(minStep)
        , MaxStep(maxStep)
        , IgnoreLowDiskSpace(ignoreLowDiskSpace)
    {}
};

struct TCoordinatorStepConfirmations {
    struct TEntry {
        TTxId TxId;
        TActorId ProxyId;
        TEvTxProxy::TEvProposeTransactionStatus::EStatus Status;
        TStepId Step;

        TEntry(TTxId txid, const TActorId &proxyId, TEvTxProxy::TEvProposeTransactionStatus::EStatus status, TStepId step)
            : TxId(txid)
            , ProxyId(proxyId)
            , Status(status)
            , Step(step)
        {}
    };

    using TQ = TOneOneQueueInplace<TEntry *, 128>;

    const TAutoPtr<TQ, TQ::TPtrCleanDestructor> Queue;
    const TStepId Step;

    TCoordinatorStepConfirmations(TStepId step)
        : Queue(new TQ())
        , Step(step)
    {}
};

struct TMediatorStep {
    struct TTx {
        TTxId TxId;

        // todo: move to flat presentation (with buffer for all affected, instead of per-one)
        TVector<TTabletId> PushToAffected; // filtered one (only entries which belong to this mediator)

        ui64 Moderator;

        TTx(TTxId txId, TTabletId *affected, ui32 affectedSize, ui64 moderator)
            : TxId(txId)
            , PushToAffected(affected, affected + affectedSize)
            , Moderator(moderator)
        {
            Y_VERIFY(TxId != 0);
        }

        TTx(TTxId txId)
            : TxId(txId)
        {
            Y_VERIFY(TxId != 0);
        }
    };

    const TTabletId MediatorId;
    const TStepId Step;
    bool Confirmed;
    TVector<TTx> Transactions;

    TMediatorStep(TTabletId mediatorId, TStepId step)
        : MediatorId(mediatorId)
        , Step(step)
        , Confirmed(false)
    {}
};

struct TMediatorConfirmations {
    const TTabletId MediatorId;

    THashMap<TTxId, THashSet<TTabletId>> Acks;

    TMediatorConfirmations(TTabletId mediatorId)
        : MediatorId(mediatorId)
    {}
};

IActor* CreateTxCoordinatorMediatorQueue(const TActorId &owner, ui64 coordinator, ui64 mediator, ui64 coordinatorGeneration);

using NTabletFlatExecutor::TTabletExecutedFlat;
using NTabletFlatExecutor::ITransaction;
using NTabletFlatExecutor::TTransactionBase;
using NTabletFlatExecutor::TTransactionContext;

//#define COORDINATOR_LOG_TO_FILE

#ifdef COORDINATOR_LOG_TO_FILE
#define FLOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, priority, component, sampleBy, stream) \
do { \
    ::NActors::NLog::TSettings *mSettings = (::NActors::NLog::TSettings*)((actorCtxOrSystem).LoggerSettings()); \
    ::NActors::NLog::EPriority mPriority = (::NActors::NLog::EPriority)(priority); \
    ::NActors::NLog::EComponent mComponent = (::NActors::NLog::EComponent)(component); \
    if (mSettings && mSettings->Satisfies(mPriority, mComponent, sampleBy)) { \
        TStringBuilder logStringBuilder; \
        logStringBuilder << stream; \
        Self->DebugLog << (TString)logStringBuilder << Endl; \
    } \
} while(0) \
/**/
#else
#define FLOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, priority, component, sampleBy, stream) \
    LOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, priority, component, sampleBy, stream)
#endif

#define FLOG_LOG_S(actorCtxOrSystem, priority, component, stream) FLOG_LOG_S_SAMPLED_BY(actorCtxOrSystem, priority, component, 0ull, stream)
#define FLOG_DEBUG_S(actorCtxOrSystem, component, stream)  FLOG_LOG_S(actorCtxOrSystem, NActors::NLog::PRI_DEBUG, component, stream)


class TTxCoordinator : public TActor<TTxCoordinator>, public TTabletExecutedFlat {

    struct TEvPrivate {
        enum EEv {
            EvPlanTick = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvAcquireReadStepFlush,
            EvReadStepSubscribed,
            EvReadStepUnsubscribed,
            EvReadStepUpdated,
            EvPipeServerDisconnected,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvPlanTick : public TEventLocal<TEvPlanTick, EvPlanTick> {};

        struct TEvAcquireReadStepFlush : public TEventLocal<TEvAcquireReadStepFlush, EvAcquireReadStepFlush> {};

        // Coordinator to subscription manager, must be transactional, from Complete only!
        struct TEvReadStepSubscribed : public TEventLocal<TEvReadStepSubscribed, EvReadStepSubscribed> {
            const TActorId PipeServer;
            const TActorId InterconnectSession;
            const TActorId Sender;
            const ui64 Cookie;
            const ui64 SeqNo;
            const ui64 LastStep;
            const ui64 NextStep;

            TEvReadStepSubscribed(const TActorId& pipeServer, const TEvTxProxy::TEvSubscribeReadStep::TPtr &ev, ui64 lastStep, ui64 nextStep)
                : PipeServer(pipeServer)
                , InterconnectSession(ev->InterconnectSession)
                , Sender(ev->Sender)
                , Cookie(ev->Cookie)
                , SeqNo(ev->Get()->Record.GetSeqNo())
                , LastStep(lastStep)
                , NextStep(nextStep)
            { }
        };

        // Coordinator to subscription manager, must be transactional, from Complete only!
        struct TEvReadStepUnsubscribed : public TEventLocal<TEvReadStepUnsubscribed, EvReadStepUnsubscribed> {
            const TActorId Sender;
            const ui64 SeqNo;

            explicit TEvReadStepUnsubscribed(const TEvTxProxy::TEvUnsubscribeReadStep::TPtr &ev)
                : Sender(ev->Sender)
                , SeqNo(ev->Get()->Record.GetSeqNo())
            { }
        };

        // Coordinator to subscription manager, must be transactional, from Complete only!
        struct TEvReadStepUpdated : public TEventLocal<TEvReadStepUpdated, EvReadStepUpdated> {
            const ui64 NextStep;

            explicit TEvReadStepUpdated(ui64 nextStep)
                : NextStep(nextStep)
            { }
        };

        struct TEvPipeServerDisconnected : public TEventLocal<TEvPipeServerDisconnected, EvPipeServerDisconnected> {
            const TActorId ServerId;

            explicit TEvPipeServerDisconnected(const TActorId& serverId)
                : ServerId(serverId)
            { }
        };
    };

    struct TQueueType {
        typedef TOneOneQueueInplace<TTransactionProposal *, 512> TQ;

        struct TFlowEntry {
            ui16 Weight;
            ui16 Limit;

            TFlowEntry()
                : Weight(0)
                , Limit(0)
            {}
        };

        struct TSlot {
            TAutoPtr<TQ, TQ::TPtrCleanDestructor> Queue;
            ui64 QueueSize;

            TSlot()
                : Queue(new TQ())
                , QueueSize(0)
            {}
        };

        typedef TMap<TStepId, TSlot> TSlotQueue;

        TSlotQueue Low;
        TSlot RapidSlot; // slot for entries with schedule on 'right now' moment (actually - with min-schedule time in past).
        bool RapidFreeze;

        TAutoPtr<TQ, TQ::TPtrCleanDestructor> Unsorted;

        TSlot& LowSlot(TStepId step) {
            TMap<TStepId, TSlot>::iterator it = Low.find(step);
            if (it != Low.end())
                return it->second;
            std::pair<TMap<TStepId, TSlot>::iterator, bool> xit = Low.insert(TSlotQueue::value_type(step, TSlot()));
            TSlot &ret = xit.first->second;
            return ret;
        }

        TQueueType()
            : RapidFreeze(false)
        {}
    };

    struct TTxInit;
    struct TTxRestoreTransactions;
    struct TTxConfigure;
    struct TTxSchema;
    struct TTxUpgrade;
    struct TTxPlanStep;
    struct TTxRestartMediatorQueue;
    struct TTxMediatorConfirmations;
    struct TTxConsistencyCheck;
    struct TTxMonitoring;
    struct TTxStopGuard;
    struct TTxAcquireReadStep;
    struct TTxSubscribeReadStep;
    struct TTxUnsubscribeReadStep;

    class TReadStepSubscriptionManager;

    ITransaction* CreateTxInit();
    ITransaction* CreateTxRestoreTransactions();
    ITransaction* CreateTxConfigure(
            TActorId ackTo, ui64 version, ui64 resolution, const TVector<TTabletId> &mediators,
            const NKikimrSubDomains::TProcessingParams &config);
    ITransaction* CreateTxSchema();
    ITransaction* CreateTxUpgrade();
    ITransaction* CreateTxPlanStep(TStepId toStep, TVector<TQueueType::TSlot> &slots, bool rapid);
    ITransaction* CreateTxRestartMediatorQueue(TTabletId mediatorId, ui64 genCookie);
    ITransaction* CreateTxMediatorConfirmations(TAutoPtr<TMediatorConfirmations> &confirmations);
    ITransaction* CreateTxConsistencyCheck();
    ITransaction* CreateTxMonitoring(NMon::TEvRemoteHttpInfo::TPtr& ev);
    ITransaction* CreateTxStopGuard();

    struct TConfig {
        ui64 MediatorsVersion;
        TMediators::TPtr Mediators;
        TVector<TTabletId> Coordinators;

        ui64 PlanAhead;
        ui64 Resolution;
        ui64 RapidSlotFlushSize;

        bool Synthetic;

        TConfig()
            : MediatorsVersion(0)
            , PlanAhead(0)
            , Resolution(0)
            , RapidSlotFlushSize(0)
            , Synthetic(false)
        {}
    };

    struct TMediator {
        typedef TOneOneQueueInplace<TMediatorStep *, 32> TStepQueue;

        TActorId QueueActor;
        ui64 GenCookie;
        bool PushUpdates;

        TAutoPtr<TStepQueue, TStepQueue::TPtrCleanDestructor> Queue;

        TMediator()
            : GenCookie(0)
            , PushUpdates(false)
        {}
    };

    struct TTransaction {
        TStepId PlanOnStep;
        THashSet<TTabletId> AffectedSet;
        THashMap<TTabletId, THashSet<TTabletId>> UnconfirmedAffectedSet;

        TTransaction()
            : PlanOnStep(0)
        {}
    };

    struct TAcquireReadStepRequest {
        TActorId Sender;
        ui64 Cookie;

        TAcquireReadStepRequest(const TActorId& sender, ui64 cookie)
            : Sender(sender)
            , Cookie(cookie)
        { }
    };

    struct TVolatileState {
        ui64 LastPlanned = 0;
        ui64 LastSentStep = 0;
        ui64 LastAcquired = 0;
        ui64 LastEmptyStep = 0;
        TMonotonic LastEmptyPlanAt{ };

        TQueueType Queue;

        ui64 AcquireReadStepInFlight = 0;
        TMonotonic AcquireReadStepLast{ };
        NMetrics::TAverageValue<ui64> AcquireReadStepLatencyUs;
        TVector<TAcquireReadStepRequest> AcquireReadStepPending;
        bool AcquireReadStepFlushing = false;
        bool AcquireReadStepStarting = false;
    };

public:
    struct Schema : NIceDb::Schema {
        static const ui32 CurrentVersion;

        struct Transaction : Table<0> {
            struct ID : Column<0, NScheme::NTypeIds::Uint64> {}; // PK
            struct Plan : Column<1, NScheme::NTypeIds::Uint64> {};
            struct AffectedSet : Column<2, NScheme::NTypeIds::String> { using Type = TVector<TTabletId>; };

            using TKey = TableKey<ID>;
            using TColumns = TableColumns<ID, Plan, AffectedSet>;
        };

        struct AffectedSet : Table<4> {
            struct MediatorID : Column<1, NScheme::NTypeIds::Uint64> {};
            struct TransactionID : Column<2, Transaction::ID::ColumnType> {};
            struct DataShardID : Column<3, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<MediatorID, TransactionID, DataShardID>;
            using TColumns = TableColumns<MediatorID, TransactionID, DataShardID>;
        };

        struct State : Table<2> {
            enum EKeyType {
                KeyLastPlanned,
                DatabaseVersion,
                AcquireReadStepLast,
            };

            struct StateKey : Column<0, NScheme::NTypeIds::Uint64> { using Type = EKeyType; }; // PK
            struct StateValue : Column<1, NScheme::NTypeIds::Uint64> {};

            using TKey = TableKey<StateKey>;
            using TColumns = TableColumns<StateKey, StateValue>;
        };

        struct DomainConfiguration : Table<5> {
            struct Version : Column<1, NScheme::NTypeIds::Uint64> {};
            struct Mediators : Column<2, NScheme::NTypeIds::String> { using Type = TVector<TTabletId>; };
            struct Resolution : Column<3, NScheme::NTypeIds::Uint64> {};
            struct Config : Column<4, NScheme::NTypeIds::String> {};

            using TKey = TableKey<Version>;
            using TColumns = TableColumns<Version, Mediators, Resolution, Config>;
        };

        using TTables = SchemaTables<Transaction, AffectedSet, State, DomainConfiguration>;
    };

private:
    struct TCoordinatorMonCounters {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> Coordinator;

        ::NMonitoring::TDynamicCounters::TCounterPtr TxIn;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxPlanned;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxDeclined;
        ::NMonitoring::TDynamicCounters::TCounterPtr TxInFly;
        ::NMonitoring::TDynamicCounters::TCounterPtr StepsUncommited;
        ::NMonitoring::TDynamicCounters::TCounterPtr StepsInFly;

        ::NMonitoring::TDynamicCounters::TCounterPtr PlanTxCalls;
        ::NMonitoring::TDynamicCounters::TCounterPtr PlanTxOutdated;
        ::NMonitoring::TDynamicCounters::TCounterPtr PlanTxAccepted;

        ::NMonitoring::TDynamicCounters::TCounterPtr StepConsideredTx;
        ::NMonitoring::TDynamicCounters::TCounterPtr StepOutdatedTx;
        ::NMonitoring::TDynamicCounters::TCounterPtr StepPlannedDeclinedTx;
        ::NMonitoring::TDynamicCounters::TCounterPtr StepPlannedTx;
        ::NMonitoring::TDynamicCounters::TCounterPtr StepDeclinedNoSpaceTx;

        NMonitoring::THistogramPtr TxFromReceiveToPlan;
        NMonitoring::THistogramPtr TxPlanLatency;

        i64 CurrentTxInFly;
    };

    struct TLastStepSubscriber {
        TActorId PipeServer;
        TActorId InterconnectSession;
        ui64 SeqNo;
        ui64 Cookie;
    };

    struct TPipeServerData {
        // Subscriber -> Pointer into LastStepSubscribers
        THashMap<TActorId, TLastStepSubscriber*> LastStepSubscribers;
    };

    bool IcbRegistered = false;
    TControlWrapper EnableLeaderLeases;
    TControlWrapper MinLeaderLeaseDurationUs;

    TVolatileState VolatileState;
    TConfig Config;
    TCoordinatorMonCounters MonCounters;
    TTabletCountersBase* TabletCounters;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;
    THashMap<TActorId, TLastStepSubscriber> LastStepSubscribers;
    THashMap<TActorId, TPipeServerData> PipeServers;

    typedef THashMap<TTabletId, TMediator> TMediatorsIndex;
    TMediatorsIndex Mediators;

    typedef THashMap<TTxId, TTransaction> TTransactions;
    TTransactions Transactions;

    TActorId ReadStepSubscriptionManager;

    bool Stopping = false;

#ifdef COORDINATOR_LOG_TO_FILE
    // HACK
    TString DebugName;
    TFixedBufferFileOutput DebugLogFile;
    TZLibCompress DebugLog;
#endif

    void Die(const TActorContext &ctx) override {
        if (ReadStepSubscriptionManager) {
            ctx.Send(ReadStepSubscriptionManager, new TEvents::TEvPoison);
            ReadStepSubscriptionManager = { };
        }

        for (TMediatorsIndex::iterator it = Mediators.begin(), end = Mediators.end(); it != end; ++it) {
            TMediator &x = it->second;
            ctx.Send(x.QueueActor, new TEvents::TEvPoisonPill());
        }
        Mediators.clear();

        if (MonCounters.CurrentTxInFly && MonCounters.TxInFly)
            *MonCounters.TxInFly -= MonCounters.CurrentTxInFly;

        return TActor::Die(ctx);
    }

    void SendViaSession(const TActorId& sessionId, const TActorId& target, IEventBase* event, ui32 flags, ui64 cookie);

    void IcbRegister();
    bool ReadOnlyLeaseEnabled() override;
    TDuration ReadOnlyLeaseDuration() override;

    void IncCounter(NFlatTxCoordinator::ECumulativeCounters counter, ui64 num = 1) {
        TabletCounters->Cumulative()[counter].Increment(num);
    }

    void OnActivateExecutor(const TActorContext &ctx) override;

    void DefaultSignalTabletActive(const TActorContext &ctx) override {
        Y_UNUSED(ctx); //unactivate incomming tablet's pipes until StateSync
    }

    void OnDetach(const TActorContext &ctx) override {
        return Die(ctx);
    }

    void OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) override {
        Y_UNUSED(ev);
        return Die(ctx);
    }

    TMediator& Mediator(TTabletId mediatorId, const TActorContext &ctx) {
        TMediator &mediator = Mediators[mediatorId];
        if (!mediator.QueueActor)
            mediator.QueueActor = ctx.ExecutorThread.RegisterActor(CreateTxCoordinatorMediatorQueue(ctx.SelfID, TabletID(), mediatorId, Executor()->Generation()));
        return mediator;
    }

    void SendMediatorStep(TMediator &mediator, const TActorContext &ctx);

    void Handle(TEvTxProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx);
    void HandleEnqueue(TEvTxProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvTxProxy::TEvAcquireReadStep::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvPrivate::TEvAcquireReadStepFlush::TPtr &ev, const TActorContext &ctx);

    TActorId EnsureReadStepSubscriptionManager(const TActorContext &ctx);
    void Handle(TEvTxProxy::TEvSubscribeReadStep::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxProxy::TEvUnsubscribeReadStep::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvTxProxy::TEvSubscribeLastStep::TPtr &ev);
    void Handle(TEvTxProxy::TEvUnsubscribeLastStep::TPtr &ev);
    void NotifyUpdatedLastStep();
    void NotifyUpdatedLastStep(const TActorId& actorId, const TLastStepSubscriber& subscriber);

    void Handle(TEvPrivate::TEvPlanTick::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxCoordinator::TEvMediatorQueueStop::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxCoordinator::TEvMediatorQueueRestart::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxCoordinator::TEvMediatorQueueConfirmations::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvTxCoordinator::TEvCoordinatorConfirmPlan::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvSubDomain::TEvConfigure::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx);

    void DoConfiguration(const TEvSubDomain::TEvConfigure &ev, const TActorContext &ctx, const TActorId &ackTo = TActorId());

    void Sync(ui64 mediator, const TActorContext &ctx);
    void Sync(const TActorContext &ctx);

    void PlanTx(TAutoPtr<TTransactionProposal> &proposal, const TActorContext &ctx);

    void SchedulePlanTick(const TActorContext &ctx);
    bool RestoreMediatorInfo(TTabletId mediatorId, TVector<TAutoPtr<TMediatorStep>> &planned, TTransactionContext &txc, /*TKeyBuilder &kb, */THashMap<TTxId,TVector<TTabletId>> &pushToAffected) const;

    void TryInitMonCounters(const TActorContext &ctx);
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override;

    void OnTabletStop(TEvTablet::TEvTabletStop::TPtr &ev, const TActorContext &ctx) override;
    void OnStopGuardStarting(const TActorContext &ctx);
    void OnStopGuardComplete(const TActorContext &ctx);

    bool ReassignChannelsEnabled() const override {
        return true;
    }

    void MaybeFlushAcquireReadStep(const TActorContext &ctx);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COORDINATOR_ACTOR;
    }

    TTxCoordinator(TTabletStorageInfo *info, const TActorId &tablet);

    // no incomming pipes is allowed in StateInit
    STFUNC_TABLET_INIT(StateInit,
                HFunc(TEvents::TEvPoisonPill, Handle);
            )

    STFUNC_TABLET_DEF(StateSync,
                HFunc(TEvSubDomain::TEvConfigure, Handle);
                HFunc(TEvTxProxy::TEvProposeTransaction, HandleEnqueue);
                HFunc(TEvTxProxy::TEvAcquireReadStep, Handle);
                HFunc(TEvPrivate::TEvAcquireReadStepFlush, Handle);
                HFunc(TEvTxProxy::TEvSubscribeReadStep, Handle);
                HFunc(TEvTxProxy::TEvUnsubscribeReadStep, Handle);
                hFunc(TEvTxProxy::TEvSubscribeLastStep, Handle);
                hFunc(TEvTxProxy::TEvUnsubscribeLastStep, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
                HFunc(TEvTabletPipe::TEvServerConnected, Handle);
                HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            )

    STFUNC_TABLET_DEF(StateWork,
                HFunc(TEvSubDomain::TEvConfigure, Handle);
                HFunc(TEvTxProxy::TEvProposeTransaction, Handle);
                HFunc(TEvTxProxy::TEvAcquireReadStep, Handle);
                HFunc(TEvPrivate::TEvAcquireReadStepFlush, Handle);
                HFunc(TEvTxProxy::TEvSubscribeReadStep, Handle);
                HFunc(TEvTxProxy::TEvUnsubscribeReadStep, Handle);
                hFunc(TEvTxProxy::TEvSubscribeLastStep, Handle);
                hFunc(TEvTxProxy::TEvUnsubscribeLastStep, Handle);
                HFunc(TEvPrivate::TEvPlanTick, Handle);
                HFunc(TEvTxCoordinator::TEvMediatorQueueConfirmations, Handle);
                HFunc(TEvTxCoordinator::TEvMediatorQueueRestart, Handle);
                HFunc(TEvTxCoordinator::TEvMediatorQueueStop, Handle);
                HFunc(TEvTxCoordinator::TEvCoordinatorConfirmPlan, Handle);
                HFunc(TEvents::TEvPoisonPill, Handle);
                HFunc(TEvTabletPipe::TEvServerConnected, Handle);
                HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            )

   STFUNC_TABLET_IGN(StateBroken,)
};

}
}

