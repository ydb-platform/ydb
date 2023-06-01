#include "coordinator_impl.h"
#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <ydb/core/tablet/tablet_counters_protobuf.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/protos/services.pb.h>
#include <ydb/core/tx/tx.h>

#include <library/cpp/actors/interconnect/interconnect.h>

namespace NKikimr {
namespace NFlatTxCoordinator {

static constexpr TDuration MaxEmptyPlanDelay = TDuration::Seconds(1);

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

const ui32 TTxCoordinator::Schema::CurrentVersion = 1;

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

    Config.PlanAhead = 50;
    Config.Resolution = 1250;
    Config.RapidSlotFlushSize = 1000; // todo: something meaningful

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

        if (rapidSlot.QueueSize >= Config.RapidSlotFlushSize && !VolatileState.Queue.RapidFreeze) {
            TVector<TQueueType::TSlot> slots;
            slots.push_back(rapidSlot);
            rapidSlot = TQueueType::TSlot();
            VolatileState.LastPlanned = planStep;
            VolatileState.Queue.RapidFreeze = true;

            NotifyUpdatedLastStep();
            Execute(CreateTxPlanStep(planStep, slots, true), ctx);
        }
    } else {
        TQueueType::TSlot &planSlot = VolatileState.Queue.LowSlot(planStep);
        planSlot.Queue->Push(proposal.Release());
        ++planSlot.QueueSize;
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

void TTxCoordinator::Handle(TEvPrivate::TEvPlanTick::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    //LOG_DEBUG_S(ctx, NKikimrServices::TX_COORDINATOR, "tablet# " << TabletID() << " HANDLE EvPlanTick LastPlanned " << VolatileState.LastPlanned);

    if (VolatileState.Queue.Unsorted) {
        while (TAutoPtr<TTransactionProposal> x = VolatileState.Queue.Unsorted->Pop())
            PlanTx(x, ctx);
        VolatileState.Queue.Unsorted.Destroy();
    }

    // do work
    const ui64 resolution = Config.Resolution;
    const ui64 now = TAppData::TimeProvider->Now().MilliSeconds();
    const ui64 dirty = now + Config.PlanAhead;
    const ui64 next = (dirty + resolution - 1) / resolution * resolution;

    if (next <= VolatileState.LastPlanned) {
        return SchedulePlanTick(ctx);
    }

    TVector<TQueueType::TSlot> slots;
    slots.reserve(1000);

    if (VolatileState.Queue.RapidSlot.QueueSize) {
        slots.push_back(VolatileState.Queue.RapidSlot);
        VolatileState.Queue.RapidSlot = TQueueType::TSlot();
    }

    while (!VolatileState.Queue.Low.empty()) {
        auto frontIt = VolatileState.Queue.Low.begin();
        if (frontIt->first > next)
            break;

        if (frontIt->second.QueueSize)
            slots.push_back(frontIt->second);

        VolatileState.Queue.Low.erase(frontIt);
    }

    VolatileState.LastPlanned = next;
    SchedulePlanTick(ctx);

    // Check if we have an empty step
    if (slots.empty()) {
        auto monotonic = ctx.Monotonic();
        if (VolatileState.LastEmptyPlanAt &&
            VolatileState.LastAcquired < VolatileState.LastEmptyStep &&
            Config.Coordinators.size() == 1 &&
            (monotonic - VolatileState.LastEmptyPlanAt) < MaxEmptyPlanDelay)
        {
            // Avoid empty plan steps more than once a second for a lonely coordinator
            return;
        }
        VolatileState.LastEmptyStep = next;
        VolatileState.LastEmptyPlanAt = monotonic;
    } else {
        VolatileState.LastEmptyStep = 0;
        VolatileState.LastEmptyPlanAt = { };
    }

    NotifyUpdatedLastStep();
    Execute(CreateTxPlanStep(next, slots, false), ctx);
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

void TTxCoordinator::SchedulePlanTick(const TActorContext &ctx) {
    const ui64 planResolution = Config.Resolution;
    ctx.Schedule(TDuration::MilliSeconds(planResolution), new TEvPrivate::TEvPlanTick());
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
