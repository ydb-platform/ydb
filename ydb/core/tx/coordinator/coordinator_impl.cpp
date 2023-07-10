#include "coordinator_impl.h"
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/tx.h>

#include <library/cpp/actors/interconnect/interconnect.h>

namespace NKikimr {
namespace NFlatTxCoordinator {

// When we need to plan a step far in the future don't schedule timer for more
// that this timeout. This is useful when coordinator starts on a node with
// incorrect wallclock time lagging in the past, and we expect eventual
// intervention where time actually jumps forward.
static constexpr TDuration MaxPlanTickDelay = TDuration::Seconds(30);

static void SendTransactionStatus(const TActorId &proxy, TEvTxProxy::TEvProposeTransactionStatus::EStatus status,
        ui64 txid, ui64 stepId, const TActorContext &ctx, ui64 tabletId) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << tabletId << " txid# " << txid
        << " step# " << stepId << " Status# " << status << " SEND to# " << proxy.ToString() << " Proxy marker# C1");
    ctx.Send(proxy, new TEvTxProxy::TEvProposeTransactionStatus(status, txid, stepId));
}

static TAutoPtr<TTransactionProposal> MakeTransactionProposal(TEvTxProxy::TEvProposeTransaction::TPtr &ev, ::NMonitoring::TDynamicCounters::TCounterPtr &counter) {
    const TActorId &sender = ev->Sender;
    const NKikimrTx::TEvProposeTransaction &record = ev->Get()->Record;

    const NKikimrTx::TProxyTransaction &txrec = record.GetTransaction();
    const ui64 txId = txrec.HasTxId() ? txrec.GetTxId() : 0;
    const ui64 minStep = txrec.HasMinStep() ? txrec.GetMinStep() : 0;
    const ui64 maxStep = txrec.HasMaxStep() ? txrec.GetMaxStep() : Max<ui64>();
    const bool ignoreLowDiskSpace = txrec.GetIgnoreLowDiskSpace();

    TAutoPtr<TTransactionProposal> proposal(new TTransactionProposal(sender, txId, minStep, maxStep, ignoreLowDiskSpace));
    proposal->AffectedSet.resize(txrec.AffectedSetSize());
    for (ui32 i = 0, e = txrec.AffectedSetSize(); i != e; ++i) {
        const auto &x = txrec.GetAffectedSet(i);
        auto &s = proposal->AffectedSet[i];
        s.TabletId = x.GetTabletId();

        Y_ASSERT(x.GetFlags() > 0 && x.GetFlags() <= 3);
        s.AffectedFlags = x.GetFlags();
    }

    counter->Inc();
    return proposal;
}

TTxCoordinator::TTxCoordinator(TTabletStorageInfo *info, const TActorId &tablet)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
    , EnableLeaderLeases(1, 0, 1)
    , MinLeaderLeaseDurationUs(250000, 1000, 5000000)
#ifdef COORDINATOR_LOG_TO_FILE
    , DebugName(Sprintf("/tmp/coordinator_db_log_%" PRIu64 ".%" PRIi32 ".%" PRIu64 ".gz", TabletID(), getpid(), tablet.LocalId()))
    , DebugLogFile(DebugName)
    , DebugLog(&DebugLogFile, ZLib::GZip, 1)
#endif
{
#ifdef COORDINATOR_LOG_TO_FILE
    // HACK
    Cerr << "Coordinator LOG will be dumped to " << DebugName << Endl;
#endif

    MonCounters.CurrentTxInFly = 0;
    TabletCountersPtr.Reset(new TProtobufTabletCounters<
        ESimpleCounters_descriptor,
        ECumulativeCounters_descriptor,
        EPercentileCounters_descriptor,
        ETxTypes_descriptor
    >());
    TabletCounters = TabletCountersPtr.Get();
}

void TTxCoordinator::PlanTx(TAutoPtr<TTransactionProposal> &proposal, const TActorContext &ctx) {
    proposal->AcceptMoment = ctx.Now();
    MonCounters.PlanTxCalls->Inc();

    if (proposal->MaxStep <= VolatileState.LastPlanned) {
        MonCounters.PlanTxOutdated->Inc();
        return SendTransactionStatus(proposal->Proxy
                                     , TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated
                                     , proposal->TxId, 0, ctx, TabletID());
    }

    if (Stopping) {
        return SendTransactionStatus(proposal->Proxy,
                TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting,
                proposal->TxId, 0, ctx, TabletID());
    }

    bool forRapidExecution = (proposal->MinStep <= VolatileState.LastPlanned);
    ui64 planStep = 0;

    // cycle for flow control
    for (ui64 cstep = (proposal->MinStep + Config.Resolution - 1) / Config.Resolution * Config.Resolution; /*no-op*/;cstep += Config.Resolution) {
        if (cstep >= proposal->MaxStep) {
            MonCounters.PlanTxOutdated->Inc();
            return SendTransactionStatus(proposal->Proxy,
                TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusOutdated, proposal->TxId, 0, ctx, TabletID());
        }

        if (forRapidExecution) {
            planStep = VolatileState.LastPlanned + 1;
            if ((planStep % Config.Resolution) == 0) // if point on border - do not use rapid slot
                forRapidExecution = false;
        } else {
            planStep = cstep;
        }

        break;
    }

    MonCounters.PlanTxAccepted->Inc();
    SendTransactionStatus(proposal->Proxy, TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusAccepted,
        proposal->TxId, planStep, ctx, TabletID());

    if (forRapidExecution) {
        TQueueType::TSlot &rapidSlot = VolatileState.Queue.RapidSlot;
        rapidSlot.Queue->Push(proposal.Release());
        ++rapidSlot.QueueSize;

        if (rapidSlot.QueueSize >= Config.RapidSlotFlushSize) {
            SchedulePlanTickExact(planStep);
        }

        // We may be sleeping at reduced resolution, try to wake up sooner
        SchedulePlanTickAligned(planStep);
    } else {
        TQueueType::TSlot &planSlot = VolatileState.Queue.LowSlot(planStep);
        planSlot.Queue->Push(proposal.Release());
        ++planSlot.QueueSize;

        // We may be sleeping at reduced resolution, try to wake up sooner
        SchedulePlanTickExact(planStep);
    }
}

void TTxCoordinator::Handle(TEvTxProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx) {
    TAutoPtr<TTransactionProposal> proposal = MakeTransactionProposal(ev, MonCounters.TxIn);
    LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << TabletID() << " txid# " << proposal->TxId
        << " HANDLE EvProposeTransaction marker# C0");
    PlanTx(proposal, ctx);
}

void TTxCoordinator::HandleEnqueue(TEvTxProxy::TEvProposeTransaction::TPtr &ev, const TActorContext &ctx) {
    TryInitMonCounters(ctx);

    TAutoPtr<TTransactionProposal> proposal = MakeTransactionProposal(ev, MonCounters.TxIn);
    LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << TabletID() << " txid# " << proposal->TxId
        << " HANDLE Enqueue EvProposeTransaction");

    if (Y_UNLIKELY(Stopping)) {
        return SendTransactionStatus(proposal->Proxy,
                TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting,
                proposal->TxId, 0, ctx, TabletID());
    }

    if (!VolatileState.Queue.Unsorted)
        VolatileState.Queue.Unsorted.Reset(new TQueueType::TQ());

    VolatileState.Queue.Unsorted->Push(proposal.Release());
}

bool TTxCoordinator::AllowReducedPlanResolution() const {
    return (
        !VolatileState.Queue.Unsorted &&
        VolatileState.LastEmptyStep &&
        VolatileState.LastEmptyStep == VolatileState.LastPlanned &&
        VolatileState.LastAcquired < VolatileState.LastEmptyStep &&
        Config.Coordinators.size() > 0 &&
        SiblingsConfirmed == Siblings.size());
}

void TTxCoordinator::SchedulePlanTick() {
    const ui64 resolution = Config.Resolution;
    const TInstant now = TAppData::TimeProvider->Now();
    const TMonotonic monotonic = AppData()->MonotonicTimeProvider->Now();

    // Step corresponding to current time
    ui64 current = now.MilliSeconds();

    // Next minimum step we would like to have
    ui64 next = (VolatileState.LastPlanned + 1 + resolution - 1) / resolution * resolution;

    if (AllowReducedPlanResolution()) {
        // We want to tick with reduced resolution when all siblings are confirmed
        ui64 reduced = (VolatileState.LastPlanned + 1 + Config.ReducedResolution - 1) / Config.ReducedResolution * Config.ReducedResolution;
        // Include transactions waiting in the queue, so we don't sleep for seconds when the next tx is in 10ms
        ui64 minWaiting = (VolatileState.Queue.MinLowSlot() + resolution - 1) / resolution * resolution;
        if (minWaiting && minWaiting < reduced) {
            reduced = minWaiting;
        }
        if (next < reduced) {
            next = reduced;
        }
    }

    if (!PendingSiblingSteps.empty()) {
        auto it = PendingSiblingSteps.begin();
        if (*it < next) {
            next = *it;
        }
    }

    // Adjust to the closest step that snaps to the resolution grid
    if (next < current) {
        current = current / resolution * resolution;
        if (next < current) {
            next = current;
        }
    }

    // Avoid scheduling more events if another is already pending
    if (!PendingPlanTicks.empty() && PendingPlanTicks.front() <= next) {
        return;
    }

    // Calculate a delay until we can plan the desired next step
    TDuration delay = Min(TInstant::MilliSeconds(next) - now, MaxPlanTickDelay);

    // Schedule using an absolute deadline so we don't wake up early due to stale timer
    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_COORDINATOR,
        "Coordinator# " << TabletID() << " scheduling step " << next << " in " << delay << " at " << (monotonic + delay));
    if (delay > TDuration::Zero()) {
        Schedule(monotonic + delay, new TEvPrivate::TEvPlanTick(next));
    } else {
        Send(SelfId(), new TEvPrivate::TEvPlanTick(next));
    }
    PendingPlanTicks.push_front(next);
}

void TTxCoordinator::SchedulePlanTickExact(ui64 next) {
    if (next <= VolatileState.LastPlanned) {
        return;
    }

    if (!PendingPlanTicks.empty() && PendingPlanTicks.front() <= next) {
        return;
    }

    const TInstant now = TAppData::TimeProvider->Now();
    const TMonotonic monotonic = AppData()->MonotonicTimeProvider->Now();

    TDuration delay = Min(TInstant::MilliSeconds(next) - now, MaxPlanTickDelay);

    LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_COORDINATOR,
        "Coordinator# " << TabletID() << " scheduling step " << next << " in " << delay << " at " << (monotonic + delay));
    if (delay > TDuration::Zero()) {
        Schedule(monotonic + delay, new TEvPrivate::TEvPlanTick(next));
    } else {
        Send(SelfId(), new TEvPrivate::TEvPlanTick(next));
    }
    PendingPlanTicks.push_front(next);
}

void TTxCoordinator::SchedulePlanTickAligned(ui64 next) {
    if (next <= VolatileState.LastPlanned) {
        return;
    }

    if (!PendingPlanTicks.empty() && PendingPlanTicks.front() <= next) {
        return;
    }

    const ui64 resolution = Config.Resolution;
    SchedulePlanTickExact((next + resolution - 1) / resolution * resolution);
}

void TTxCoordinator::Handle(TEvPrivate::TEvPlanTick::TPtr &ev, const TActorContext &ctx) {
    //LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << TabletID() << " HANDLE EvPlanTick LastPlanned " << VolatileState.LastPlanned);

    ui64 next = ev->Get()->Step;
    while (!PendingPlanTicks.empty() && PendingPlanTicks.front() <= next) {
        PendingPlanTicks.pop_front();
    }

    if (VolatileState.Queue.Unsorted) {
        while (TAutoPtr<TTransactionProposal> x = VolatileState.Queue.Unsorted->Pop())
            PlanTx(x, ctx);
        VolatileState.Queue.Unsorted.Destroy();
    }

    const ui64 resolution = Config.Resolution;
    const TInstant now = TAppData::TimeProvider->Now();

    // Check the step corresponding to current time
    ui64 current = now.MilliSeconds();
    if (current < next) {
        // We cannot plan this yet, schedule some other step
        return SchedulePlanTick();
    }

    // Snap to grid and prefer planning current time instead of some past
    current = current / resolution * resolution;
    if (next < current) {
        next = current;
    }

    if (next <= VolatileState.LastPlanned) {
        // This step has already been planned, schedule the next time
        return SchedulePlanTick();
    }

    TVector<TQueueType::TSlot> slots;

    if (VolatileState.Queue.RapidSlot.QueueSize) {
        if (slots.empty()) {
            slots.reserve(1000);
        }
        slots.push_back(VolatileState.Queue.RapidSlot);
        VolatileState.Queue.RapidSlot = TQueueType::TSlot();
    }

    while (!VolatileState.Queue.Low.empty()) {
        auto frontIt = VolatileState.Queue.Low.begin();
        if (frontIt->first > next)
            break;

        if (frontIt->second.QueueSize) {
            if (slots.empty()) {
                slots.reserve(1000);
            }
            slots.push_back(frontIt->second);
        }

        VolatileState.Queue.Low.erase(frontIt);
    }

    if (slots.empty()) {
        VolatileState.LastEmptyStep = next;
    }
    VolatileState.LastPlanned = next;

    NotifyUpdatedLastStep();
    Execute(CreateTxPlanStep(next, slots), ctx);
    SchedulePlanTick();
}

void TTxCoordinator::Handle(TEvTxCoordinator::TEvMediatorQueueConfirmations::TPtr &ev, const TActorContext &ctx) {
    TEvTxCoordinator::TEvMediatorQueueConfirmations *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << TabletID()
        << " HANDLE EvMediatorQueueConfirmations MediatorId# " << msg->Confirmations->MediatorId);
    Execute(CreateTxMediatorConfirmations(msg->Confirmations), ctx);
}

void TTxCoordinator::Handle(TEvTxCoordinator::TEvMediatorQueueStop::TPtr &ev, const TActorContext &ctx) {
    const TEvTxCoordinator::TEvMediatorQueueStop *msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << TabletID()
        << " HANDLE EvMediatorQueueStop MediatorId# " << msg->MediatorId);
    TMediator &mediator = Mediator(msg->MediatorId, ctx);
    mediator.PushUpdates = false;
    mediator.GenCookie = Max<ui64>();
    mediator.Queue.Destroy();
}

void TTxCoordinator::Handle(TEvTxCoordinator::TEvMediatorQueueRestart::TPtr &ev, const TActorContext &ctx) {
    const TEvTxCoordinator::TEvMediatorQueueRestart *msg = ev->Get();
    LOG_NOTICE_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << TabletID()
        << " HANDLE EvMediatorQueueRestart MediatorId# " << msg->MediatorId);

    TMediator &mediator = Mediator(msg->MediatorId, ctx);
    mediator.PushUpdates = false;
    mediator.GenCookie = msg->GenCookie;
    mediator.Queue.Reset(new TMediator::TStepQueue());
    Execute(CreateTxRestartMediatorQueue(msg->MediatorId, msg->GenCookie), ctx);
}

void TTxCoordinator::Handle(TEvTxCoordinator::TEvCoordinatorConfirmPlan::TPtr &ev, const TActorContext &ctx) {
    TAutoPtr<TCoordinatorStepConfirmations> confirmations = ev->Get()->Confirmations;
    while (TAutoPtr<TCoordinatorStepConfirmations::TEntry> x = confirmations->Queue->Pop()) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << TabletID() << " txid# " << x->TxId
            << " stepId# " << x->Step << " Status# " << x->Status
            << " SEND EvProposeTransactionStatus to# " << x->ProxyId.ToString() << " Proxy");
        ctx.Send(x->ProxyId, new TEvTxProxy::TEvProposeTransactionStatus(x->Status, x->TxId, x->Step));
    }
}

void TTxCoordinator::DoConfiguration(const TEvSubDomain::TEvConfigure &ev, const TActorContext &ctx, const TActorId &ackTo) {
    const TEvSubDomain::TEvConfigure::ProtoRecordType &record = ev.Record;

    if(0 == record.MediatorsSize()) {
        LOG_ERROR_S(ctx, NKikimrServices::TX_COORDINATOR
                     , "tablet# " << TabletID()
                    << " HANDLE EvCoordinatorConfiguration Version# " << record.GetVersion()
                    << " recive empty mediators set");
        Y_FAIL("empty mediators set");
        return;
    }

    TVector<TTabletId> mediators;
    mediators.reserve(record.MediatorsSize());

    for (auto id: record.GetMediators()) {
        Y_VERIFY(TabletID() != id, "found self id in mediators list");
        mediators.push_back(id);
    }

    Execute(CreateTxConfigure(ackTo, record.GetVersion(), record.GetPlanResolution(), mediators, record), ctx);
}

void TTxCoordinator::Handle(TEvSubDomain::TEvConfigure::TPtr &ev, const TActorContext &ctx) {
    const TEvSubDomain::TEvConfigure::ProtoRecordType &record = ev->Get()->Record;
    LOG_NOTICE_S(ctx, NKikimrServices::TX_COORDINATOR
                 , "tablet# " << TabletID()
                << " HANDLE TEvConfigure Version# " << record.GetVersion());

    DoConfiguration(*ev->Get(), ctx, ev->Sender);
}

void TTxCoordinator::Handle(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    Become(&TThis::StateBroken);
    ctx.Send(Tablet(), new TEvents::TEvPoisonPill);
}

void TTxCoordinator::Handle(TEvTabletPipe::TEvServerConnected::TPtr& ev, const TActorContext&) {
    auto res = PipeServers.emplace(
        std::piecewise_construct,
        std::forward_as_tuple(ev->Get()->ServerId),
        std::forward_as_tuple());
    Y_VERIFY_S(res.second, "Unexpected TEvServerConnected for " << ev->Get()->ServerId);
}

void TTxCoordinator::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev, const TActorContext& ctx) {
    auto it = PipeServers.find(ev->Get()->ServerId);
    Y_VERIFY_S(it != PipeServers.end(), "Unexpected TEvServerDisconnected for " << ev->Get()->ServerId);

    for (auto& pr : it->second.LastStepSubscribers) {
        LastStepSubscribers.erase(pr.first);
    }

    PipeServers.erase(it);

    if (ReadStepSubscriptionManager) {
        ctx.Send(ReadStepSubscriptionManager, new TEvPrivate::TEvPipeServerDisconnected(ev->Get()->ServerId));
    }
}

void TTxCoordinator::SendViaSession(const TActorId& sessionId, const TActorId& target, IEventBase* event, ui32 flags, ui64 cookie) {
    THolder<IEventHandle> ev = MakeHolder<IEventHandle>(target, SelfId(), event, flags, cookie);

    if (sessionId) {
        ev->Rewrite(TEvInterconnect::EvForward, sessionId);
    }

    TActivationContext::Send(ev.Release());
}

void TTxCoordinator::IcbRegister() {
    if (!IcbRegistered) {
        AppData()->Icb->RegisterSharedControl(EnableLeaderLeases, "CoordinatorControls.EnableLeaderLeases");
        AppData()->Icb->RegisterSharedControl(MinLeaderLeaseDurationUs, "CoordinatorControls.MinLeaderLeaseDurationUs");
        IcbRegistered = true;
    }
}

bool TTxCoordinator::ReadOnlyLeaseEnabled() {
    IcbRegister();
    ui64 value = EnableLeaderLeases;
    return value != 0;
}

TDuration TTxCoordinator::ReadOnlyLeaseDuration() {
    IcbRegister();
    ui64 value = MinLeaderLeaseDurationUs;
    return TDuration::MicroSeconds(value);
}

void TTxCoordinator::OnActivateExecutor(const TActorContext &ctx) {
    IcbRegister();
    TryInitMonCounters(ctx);
    Executor()->RegisterExternalTabletCounters(TabletCountersPtr);
    Execute(CreateTxSchema(), ctx);
}

void TTxCoordinator::TryInitMonCounters(const TActorContext &ctx) {
    if (MonCounters.Coordinator)
        return;

    auto &counters = AppData(ctx)->Counters;
    MonCounters.Coordinator = GetServiceCounters(counters, "coordinator");
    MonCounters.TxIn = MonCounters.Coordinator->GetCounter("TxIn", true);
    MonCounters.TxPlanned = MonCounters.Coordinator->GetCounter("TxPlanned", true);
    MonCounters.TxDeclined = MonCounters.Coordinator->GetCounter("TxDeclined", true);
    MonCounters.TxInFly = MonCounters.Coordinator->GetCounter("TxInFly", false);
    MonCounters.StepsUncommited = MonCounters.Coordinator->GetCounter("StepsUncommited", false);
    MonCounters.StepsInFly = MonCounters.Coordinator->GetCounter("StepsInFly", false);

    MonCounters.PlanTxCalls = MonCounters.Coordinator->GetCounter("PlanTx/Calls", true);
    MonCounters.PlanTxOutdated = MonCounters.Coordinator->GetCounter("PlanTx/Outdated", true);
    MonCounters.PlanTxAccepted = MonCounters.Coordinator->GetCounter("PlanTx/Accepted", true);

    MonCounters.StepConsideredTx = MonCounters.Coordinator->GetCounter("Step/ConsideredTx", true);
    MonCounters.StepOutdatedTx = MonCounters.Coordinator->GetCounter("Step/OutdatedTx", true);
    MonCounters.StepPlannedDeclinedTx = MonCounters.Coordinator->GetCounter("Step/PlannedDeclinedTx", true);
    MonCounters.StepPlannedTx = MonCounters.Coordinator->GetCounter("Step/PlannedTx", true);
    MonCounters.StepDeclinedNoSpaceTx = MonCounters.Coordinator->GetCounter("Step/DeclinedNoSpaceTx", true);

    MonCounters.TxFromReceiveToPlan = MonCounters.Coordinator->GetHistogram(
        "TxFromReceiveToPlanMs", NMonitoring::ExponentialHistogram(20, 2, 1));
    MonCounters.TxPlanLatency = MonCounters.Coordinator->GetHistogram(
        "TxPlanLatencyMs", NMonitoring::ExponentialHistogram(20, 2, 1));
}

void TTxCoordinator::SendMediatorStep(TMediator &mediator, const TActorContext &ctx) {
    while (TMediatorStep *step = mediator.Queue->Head()) {
        if (!step->Confirmed)
            return;

        TAutoPtr<TMediatorStep> extracted = mediator.Queue->Pop();
        for (const auto& tx: extracted->Transactions) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "Send from# " << TabletID()
                << " to mediator# " << extracted->MediatorId << ", step# " << extracted->Step
                << ", txid# " << tx.TxId << " marker# C2");
        }

        if (VolatileState.LastSentStep < extracted->Step) {
            VolatileState.LastSentStep = extracted->Step;
            if (ReadStepSubscriptionManager) {
                ctx.Send(ReadStepSubscriptionManager, new TEvPrivate::TEvReadStepUpdated(VolatileState.LastSentStep));
            }
        }
        ctx.Send(mediator.QueueActor, new TEvTxCoordinator::TEvMediatorQueueStep(mediator.GenCookie, extracted));
    }
}

bool TTxCoordinator::OnRenderAppHtmlPage(NMon::TEvRemoteHttpInfo::TPtr ev, const TActorContext &ctx) {
    if (!Executor() || !Executor()->GetStats().IsActive)
        return false;

    if (!ev)
        return true;

    Execute(CreateTxMonitoring(ev), ctx);
    return true;
}

void TTxCoordinator::OnTabletStop(TEvTablet::TEvTabletStop::TPtr &ev, const TActorContext &ctx) {
    const auto* msg = ev->Get();

    LOG_INFO_S(ctx, NKikimrServices::TX_COORDINATOR, "OnTabletStop: " << TabletID() << " reason = " << msg->GetReason());

    switch (msg->GetReason()) {
        case TEvTablet::TEvTabletStop::ReasonStop:
        case TEvTablet::TEvTabletStop::ReasonDemoted:
        case TEvTablet::TEvTabletStop::ReasonIsolated:
            // Keep trying to stop gracefully
            if (!Stopping) {
                Stopping = true;
                OnStopGuardStarting(ctx);
                Execute(CreateTxStopGuard(), ctx);
            }
            return;

        case TEvTablet::TEvTabletStop::ReasonUnknown:
        case TEvTablet::TEvTabletStop::ReasonStorageBlocked:
        case TEvTablet::TEvTabletStop::ReasonStorageFailure:
            // New commits are impossible, stop immediately
            break;
    }

    Stopping = true;
    return TTabletExecutedFlat::OnTabletStop(ev, ctx);
}

void TTxCoordinator::OnStopGuardStarting(const TActorContext &ctx) {
    auto processQueue = [&](auto &queue) {
        while (TAutoPtr<TTransactionProposal> proposal = queue.Pop()) {
            SendTransactionStatus(proposal->Proxy,
                    TEvTxProxy::TEvProposeTransactionStatus::EStatus::StatusRestarting,
                    proposal->TxId, 0, ctx, TabletID());
        }
    };

    if (VolatileState.Queue.Unsorted) {
        processQueue(*VolatileState.Queue.Unsorted);
        VolatileState.Queue.Unsorted.Destroy();
    }

    processQueue(*VolatileState.Queue.RapidSlot.Queue);

    for (auto &kv : VolatileState.Queue.Low) {
        processQueue(*kv.second.Queue);
    }
    VolatileState.Queue.Low.clear();
}

void TTxCoordinator::OnStopGuardComplete(const TActorContext &ctx) {
    // We have cleanly completed the last commit
    ctx.Send(Tablet(), new TEvTablet::TEvTabletStopped());
}

}

IActor* CreateFlatTxCoordinator(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NFlatTxCoordinator::TTxCoordinator(info, tablet);
}

}
