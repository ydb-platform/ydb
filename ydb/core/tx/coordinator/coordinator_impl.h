#pragma once

#include "defs.h"
#include "coordinator.h"

#include <ydb/core/protos/counters_coordinator.pb.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tx_processing.h>
#include <ydb/core/control/immediate_control_board_wrapper.h>
#include <ydb/core/tablet/tablet_counters.h>
#include <ydb/core/tablet/tablet_exception.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/coordinator/public/events.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tx/tx.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

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
    ui32 Flags;

    struct TAffectedEntry {
        TTabletId TabletId;
        ui64 AffectedFlags : 8;
        // reserved
    };

    TVector<TAffectedEntry> AffectedSet;

    TInstant AcceptMoment;
    bool IgnoreLowDiskSpace;

    TTransactionProposal(const TActorId &proxy, TTxId txId, TStepId minStep, TStepId maxStep, ui32 flags, bool ignoreLowDiskSpace)
        : Proxy(proxy)
        , TxId(txId)
        , MinStep(minStep)
        , MaxStep(maxStep)
        , Flags(flags)
        , IgnoreLowDiskSpace(ignoreLowDiskSpace)
    {}

    bool HasVolatileFlag() const {
        return (Flags & TEvTxProxy::TEvProposeTransaction::FlagVolatile) != 0;
    }
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

    std::deque<TEntry> Queue;
};

struct TMediatorStep {
    struct TTx {
        TTxId TxId;

        // todo: move to flat presentation (with buffer for all affected, instead of per-one)
        TVector<TTabletId> PushToAffected; // filtered one (only entries which belong to this mediator)

        TTx(TTxId txId, const TTabletId *affected, ui32 affectedSize)
            : TxId(txId)
            , PushToAffected(affected, affected + affectedSize)
        {
            Y_ABORT_UNLESS(TxId != 0);
        }

        TTx(TTxId txId)
            : TxId(txId)
        {
            Y_ABORT_UNLESS(TxId != 0);
        }
    };

    TTabletId MediatorId;
    TStepId Step;
    bool Confirmed = false;
    bool Volatile = false;

    TVector<TTx> Transactions;

    // Used by mediator queue to track acks
    size_t References = 0;

    TMediatorStep(TTabletId mediatorId, TStepId step)
        : MediatorId(mediatorId)
        , Step(step)
    {}

    void SerializeTo(TEvTxCoordinator::TEvCoordinatorStep *msg) const;
};

using TMediatorStepList = std::list<TMediatorStep>;

struct TMediatorConfirmations {
    const TTabletId MediatorId;

    THashMap<TTxId, THashSet<TTabletId>> Acks;

    TMediatorConfirmations(TTabletId mediatorId)
        : MediatorId(mediatorId)
    {}
};

struct TEvMediatorQueueStep : public TEventLocal<TEvMediatorQueueStep, TEvTxCoordinator::EvMediatorQueueStep> {
    TMediatorStepList Steps;

    TEvMediatorQueueStep() = default;

    void SpliceStep(TMediatorStepList& list, TMediatorStepList::iterator it) {
        Steps.splice(Steps.end(), list, it);
    }
};

struct TEvMediatorQueueRestart : public TEventLocal<TEvMediatorQueueRestart, TEvTxCoordinator::EvMediatorQueueRestart> {
    const ui64 MediatorId;
    const ui64 StartFrom;
    const ui64 GenCookie;

    TEvMediatorQueueRestart(ui64 mediatorId, ui64 startFrom, ui64 genCookie)
        : MediatorId(mediatorId)
        , StartFrom(startFrom)
        , GenCookie(genCookie)
    {}
};

struct TEvMediatorQueueStop : public TEventLocal<TEvMediatorQueueStop, TEvTxCoordinator::EvMediatorQueueStop> {
    const ui64 MediatorId;

    TEvMediatorQueueStop(ui64 mediatorId)
        : MediatorId(mediatorId)
    {}
};

struct TEvMediatorQueueConfirmations : public TEventLocal<TEvMediatorQueueConfirmations, TEvTxCoordinator::EvMediatorQueueConfirmations> {
    std::unique_ptr<NFlatTxCoordinator::TMediatorConfirmations> Confirmations;

    TEvMediatorQueueConfirmations(std::unique_ptr<NFlatTxCoordinator::TMediatorConfirmations> &&confirmations)
        : Confirmations(std::move(confirmations))
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

class TCoordinatorStateActor;

class TTxCoordinator : public TActor<TTxCoordinator>, public TTabletExecutedFlat {

    friend class TCoordinatorStateActor;

    struct TEvPrivate {
        enum EEv {
            EvPlanTick = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvAcquireReadStepFlush,
            EvReadStepSubscribed,
            EvReadStepUnsubscribed,
            EvReadStepUpdated,
            EvPipeServerDisconnected,
            EvRestoredProcessingParams,
            EvRestoredStateMissing,
            EvRestoredState,
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

        struct TEvPlanTick : public TEventLocal<TEvPlanTick, EvPlanTick> {
            const ui64 Step;

            explicit TEvPlanTick(ui64 step)
                : Step(step)
            { }
        };

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

        struct TEvRestoredProcessingParams : public TEventLocal<TEvRestoredProcessingParams, EvRestoredProcessingParams> {
            NKikimrSubDomains::TProcessingParams Config;

            explicit TEvRestoredProcessingParams(const NKikimrSubDomains::TProcessingParams& config)
                : Config(config)
            { }
        };

        struct TEvRestoredStateMissing : public TEventLocal<TEvRestoredStateMissing, EvRestoredStateMissing> {
            ui64 LastBlockedStep;

            explicit TEvRestoredStateMissing(ui64 lastBlockedStep)
                : LastBlockedStep(lastBlockedStep)
            { }
        };

        struct TEvRestoredState : public TEventLocal<TEvRestoredState, EvRestoredState> {
            NKikimrTxCoordinator::TEvCoordinatorStateResponse Record;

            explicit TEvRestoredState(NKikimrTxCoordinator::TEvCoordinatorStateResponse&& record)
                : Record(std::move(record))
            { }
        };
    };

    struct TQueueType {
        struct TFlowEntry {
            ui16 Weight;
            ui16 Limit;

            TFlowEntry()
                : Weight(0)
                , Limit(0)
            {}
        };

        using TQueue = std::deque<TTransactionProposal>;

        struct TSlot : public TQueue {
            using TQueue::TQueue;
        };

        typedef TMap<TStepId, TSlot> TSlotQueue;

        TSlotQueue Low;
        TSlot RapidSlot; // slot for entries with schedule on 'right now' moment (actually - with min-schedule time in past).

        std::optional<TSlot> Unsorted;

        TSlot& LowSlot(TStepId step) {
            return Low[step];
        }

        TStepId MinLowSlot() const {
            if (!Low.empty()) {
                return Low.begin()->first;
            }
            return 0;
        }

        TQueueType()
        {}
    };

    struct TTxInit;
    struct TTxRestoreTransactions;
    struct TTxConfigure;
    struct TTxSchema;
    struct TTxUpgrade;
    struct TTxPlanStep;
    struct TTxMediatorConfirmations;
    struct TTxConsistencyCheck;
    struct TTxMonitoring;
    struct TTxStopGuard;
    struct TTxAcquireReadStep;
    struct TTxSubscribeReadStep;
    struct TTxUnsubscribeReadStep;

    class TReadStepSubscriptionManager;
    class TRestoreProcessingParamsActor;

    ITransaction* CreateTxInit();
    ITransaction* CreateTxRestoreTransactions();
    ITransaction* CreateTxConfigure(
            TActorId ackTo, ui64 version, ui64 resolution, const TVector<TTabletId> &mediators,
            const NKikimrSubDomains::TProcessingParams &config);
    ITransaction* CreateTxSchema();
    ITransaction* CreateTxUpgrade();
    ITransaction* CreateTxPlanStep(TStepId toStep, std::deque<TQueueType::TSlot> &&slots);
    ITransaction* CreateTxRestartMediatorQueue(TTabletId mediatorId, ui64 genCookie);
    ITransaction* CreateTxMediatorConfirmations(std::unique_ptr<TMediatorConfirmations> &&confirmations);
    ITransaction* CreateTxConsistencyCheck();
    ITransaction* CreateTxMonitoring(NMon::TEvRemoteHttpInfo::TPtr& ev);
    ITransaction* CreateTxStopGuard();

    struct TConfig {
        ui64 Version = 0;
        TMediators::TPtr Mediators;
        TVector<TTabletId> Coordinators;

        ui64 Resolution = 1000;
        ui64 ReducedResolution = 1000;
        ui64 RapidSlotFlushSize = 1000;

        bool HaveProcessingParams = false;

        TConfig() {}
    };

    struct TMediator {
        TActorId QueueActor;
        TMediatorStepList Queue;
        bool Active = false;
    };

    struct TTransaction {
        TStepId PlanOnStep = 0;
        THashSet<TTabletId> AffectedSet;
        THashMap<TTabletId, THashSet<TTabletId>> UnconfirmedAffectedSet;
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
        ui64 LastConfirmedStep = 0;
        ui64 LastBlockedPending = 0;
        ui64 LastBlockedCommitted = 0;

        TQueueType Queue;

        ui64 AcquireReadStepInFlight = 0;
        TMonotonic AcquireReadStepLast{ };
        NMetrics::TAverageValue<ui64> AcquireReadStepLatencyUs;
        TVector<TAcquireReadStepRequest> AcquireReadStepPending;
        bool AcquireReadStepFlushing = false;
        bool AcquireReadStepStarting = false;

        // When true the state has been preserved by the state actor
        // Any changes will not be migrated to newer generations
        bool Preserved = false;
    };

public:
    struct Schema : NIceDb::Schema {
        static constexpr ui64 CurrentVersion = 1;

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
            enum EKeyType : ui64 {
                KeyLastPlanned = 0,
                DatabaseVersion = 1,
                AcquireReadStepLast = 2,
                LastBlockedActorX1 = 3,
                LastBlockedActorX2 = 4,
                LastBlockedStep = 5,
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

        template<class TCallback>
        static bool LoadState(NIceDb::TNiceDb& db, State::EKeyType key, TCallback&& callback) {
            auto rowset = db.Table<State>().Key(key).Select<State::StateValue>();

            if (!rowset.IsReady()) {
                return false;
            }

            if (rowset.IsValid()) {
                callback(rowset.GetValue<State::StateValue>());
            }

            return true;
        }

        static bool LoadState(NIceDb::TNiceDb& db, State::EKeyType key, std::optional<ui64>& out) {
            return LoadState(db, key, [&out](ui64 value) {
                out.emplace(value);
            });
        }

        static bool LoadState(NIceDb::TNiceDb& db, State::EKeyType key, ui64& out) {
            return LoadState(db, key, [&out](ui64 value) {
                out = value;
            });
        }

        static void SaveState(NIceDb::TNiceDb& db, State::EKeyType key, ui64 value) {
            db.Table<State>().Key(key).Update<State::StateValue>(value);
        }
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

    struct TSiblingState {
        ui64 CoordinatorId = 0;

        ui64 SeqNo = 0;
        bool Subscribed = false;
        bool Confirmed = false;

        TMonotonic NextAttempt;
        TDuration LastRetryDelay;
        size_t RetryAttempts = 0;
    };

    bool IcbRegistered = false;
    TControlWrapper EnableLeaderLeases;
    TControlWrapper MinLeaderLeaseDurationUs;
    TControlWrapper VolatilePlanLeaseMs;
    TControlWrapper PlanAheadTimeShiftMs;

    TVolatileState VolatileState;
    TConfig Config;
    TCoordinatorMonCounters MonCounters;
    TTabletCountersBase* TabletCounters;
    TAutoPtr<TTabletCountersBase> TabletCountersPtr;
    THashMap<TActorId, TLastStepSubscriber> LastStepSubscribers;
    THashMap<TActorId, TPipeServerData> PipeServers;
    THashMap<ui64, TSiblingState> Siblings;
    size_t SiblingsConfirmed = 0;

    TCoordinatorStateActor* CoordinatorStateActor = nullptr;
    TActorId CoordinatorStateActorId;
    TActorId RestoreStateActorId;
    TActorId PrevStateActorId;

    std::deque<ui64> PendingPlanTicks;
    std::set<ui64> PendingSiblingSteps;

    typedef THashMap<TTabletId, TMediator> TMediatorsIndex;
    TMediatorsIndex Mediators;

    typedef THashMap<TTxId, TTransaction> TTransactions;
    TTransactions Transactions;

    // Volatile transactions are not persistent
    // Separate hash map so we don't trip consistency checks
    TTransactions VolatileTransactions;

    TActorId ReadStepSubscriptionManager;

    TActorId RestoreProcessingParamsActor;

    bool Stopping = false;

#ifdef COORDINATOR_LOG_TO_FILE
    // HACK
    TString DebugName;
    TFixedBufferFileOutput DebugLogFile;
    TZLibCompress DebugLog;
#endif

    void Die(const TActorContext &ctx) override;

    void SendViaSession(const TActorId& sessionId, const TActorId& target, IEventBase* event, ui32 flags, ui64 cookie);

    void IcbRegister();
    bool ReadOnlyLeaseEnabled() override;
    TDuration ReadOnlyLeaseDuration() override;

    void SetCounter(NFlatTxCoordinator::ESimpleCounters counter, ui64 val) {
        TabletCounters->Simple()[counter].Set(val);
    }

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
    void SubscribeToSiblings();
    void SubscribeToSibling(TSiblingState& state);
    void UnsubscribeFromSiblings();
    void Handle(TEvTxProxy::TEvUpdatedLastStep::TPtr &ev);
    void Handle(TEvTxProxy::TEvRequirePlanSteps::TPtr &ev);
    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr &ev);

    void Handle(TEvPrivate::TEvPlanTick::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorQueueStop::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorQueueRestart::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvMediatorQueueConfirmations::TPtr &ev, const TActorContext &ctx);
    void Handle(TEvSubDomain::TEvConfigure::TPtr &ev, const TActorContext &ctx);

    void Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext& ctx);
    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx);

    void SendStepConfirmations(TCoordinatorStepConfirmations &confirmations, const TActorContext &ctx);
    void DoConfiguration(const TEvSubDomain::TEvConfigure &ev, const TActorContext &ctx, const TActorId &ackTo = TActorId());

    void Sync(ui64 mediator, const TActorContext &ctx);
    void Sync(const TActorContext &ctx);

    void PlanTx(TTransactionProposal &&proposal, const TActorContext &ctx);

    bool AllowReducedPlanResolution() const;
    void SchedulePlanTick();
    void SchedulePlanTickExact(ui64 next);
    void SchedulePlanTickAligned(ui64 next);
    ui64 AlignPlanStep(ui64 step);

    void TryInitMonCounters(const TActorContext &ctx);
    bool OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) override;

    void OnTabletStop(TEvTablet::TEvTabletStop::TPtr &ev, const TActorContext &ctx) override;
    void OnStopGuardStarting(const TActorContext &ctx);
    void OnStopGuardComplete(const TActorContext &ctx);

    bool ReassignChannelsEnabled() const override {
        return true;
    }

    void MaybeFlushAcquireReadStep(const TActorContext &ctx);

    // Attempts to restore missing processing params
    bool IsTabletInStaticDomain(TAppData *appData);
    void RestoreProcessingParams(const TActorContext &ctx);
    void Handle(TEvPrivate::TEvRestoredProcessingParams::TPtr &ev, const TActorContext &ctx);

    // State actor related methods
    bool StartStateActor();
    void ConfirmStateActorPersistent();
    void DetachStateActor();

    // Logic for restoring state from previous generation
    class TRestoreStateActor;
    void RestoreState(const TActorId& prevStateActorId, ui64 lastBlockedStep);
    void Handle(TEvPrivate::TEvRestoredStateMissing::TPtr& ev);
    void Handle(TEvPrivate::TEvRestoredState::TPtr& ev);

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COORDINATOR_ACTOR;
    }

    TTxCoordinator(TTabletStorageInfo *info, const TActorId &tablet);
    ~TTxCoordinator();

    // no incomming pipes is allowed in StateInit
    STFUNC_TABLET_INIT(StateInit,
                HFunc(TEvMediatorQueueConfirmations, Handle);
                HFunc(TEvMediatorQueueRestart, Handle);
                HFunc(TEvMediatorQueueStop, Handle);
                hFunc(TEvPrivate::TEvRestoredStateMissing, Handle);
                hFunc(TEvPrivate::TEvRestoredState, Handle);
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
                HFunc(TEvMediatorQueueConfirmations, Handle);
                HFunc(TEvMediatorQueueRestart, Handle);
                HFunc(TEvMediatorQueueStop, Handle);
                HFunc(TEvTabletPipe::TEvServerConnected, Handle);
                HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
                HFunc(TEvPrivate::TEvRestoredProcessingParams, Handle);
                hFunc(TEvPrivate::TEvRestoredStateMissing, Handle);
                hFunc(TEvPrivate::TEvRestoredState, Handle);
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
                HFunc(TEvMediatorQueueConfirmations, Handle);
                HFunc(TEvMediatorQueueRestart, Handle);
                HFunc(TEvMediatorQueueStop, Handle);
                HFunc(TEvTabletPipe::TEvServerConnected, Handle);
                HFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
                HFunc(TEvPrivate::TEvRestoredProcessingParams, Handle);
                hFunc(TEvTxProxy::TEvUpdatedLastStep, Handle);
                hFunc(TEvTxProxy::TEvRequirePlanSteps, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            )

   STFUNC_TABLET_IGN(StateBroken,)
};

}
}

