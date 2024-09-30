#include "mediator_impl.h"

#include <ydb/core/engine/minikql/flat_local_tx_factory.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr {
namespace NTxMediator {

const ui32 TTxMediator::Schema::CurrentVersion = 0;

void TTxMediator::Die(const TActorContext &ctx) {
    if (!!ExecQueue) {
        ctx.Send(ExecQueue, new TEvents::TEvPoisonPill());
        ExecQueue = TActorId();
    }

    return TActor::Die(ctx);
}

void TTxMediator::OnActivateExecutor(const TActorContext &ctx) {
    Y_UNUSED(ctx);

    Become(&TThis::StateInit);
    Execute(CreateTxSchema(), ctx);
}

void TTxMediator::OnDetach(const TActorContext &ctx) {
    return Die(ctx);
}

void TTxMediator::OnTabletDead(TEvTablet::TEvTabletDead::TPtr &ev, const TActorContext &ctx) {
    Y_UNUSED(ev);
    return Die(ctx);
}

void TTxMediator::DefaultSignalTabletActive(const TActorContext &ctx) {
    Y_UNUSED(ctx); //unactivate incomming tablet's pipes until StateSync
}

ui64 TTxMediator::SubjectiveTime() {
    return TAppData::TimeProvider->Now().MilliSeconds();
}

void TTxMediator::InitSelfState(const TActorContext &ctx) {
    Y_ABORT_UNLESS(Config.Bukets);
    ExecQueue = ctx.ExecutorThread.RegisterActor(CreateTxMediatorExecQueue(ctx.SelfID, TabletID(), 1, Config.Bukets->Buckets()));
    Y_ABORT_UNLESS(!!ExecQueue);

    Y_ABORT_UNLESS(Config.CoordinatorSeletor);
    Y_ABORT_UNLESS(Config.CoordinatorSeletor->List().size());

    for (ui64 it: Config.CoordinatorSeletor->List()) {
        TCoordinatorInfo &x = VolatileState.Domain[it];
        Y_UNUSED(x);
    }

    ReplyEnqueuedSyncs(ctx);
    ProcessEnqueuedWatch(ctx);
}

void TTxMediator::ReplyEnqueuedSyncs(const TActorContext &ctx) {
    for (auto x: CoordinatorsSyncEnqueued) {
        ReplySync(x.first, x.second, ctx);
    }
    CoordinatorsSyncEnqueued.clear();
}

void TTxMediator::ProcessEnqueuedWatch(const TActorContext &ctx) {
    Y_UNUSED(ctx);
    for (auto &ev: EnqueuedWatch) {
        StateWork(ev);
    }
    EnqueuedWatch.clear();
}

void TTxMediator::ReplySync(const TActorId &sender, const NKikimrTx::TEvCoordinatorSync &record, const TActorContext &ctx) {
    Y_ABORT_UNLESS(record.GetMediatorID() == TabletID());

    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID()
        << " SEND EvCoordinatorSyncResult to# " << sender.ToString() << " Cookie# " << record.GetCookie()
        << " CompleteStep# " << VolatileState.CompleteStep
        << " LatestKnownStep# " << VolatileState.LatestKnownStep << " SubjectiveTime# " << SubjectiveTime()
        << " Coordinator# " << record.GetCoordinatorID());

    ctx.Send(sender,
        new TEvTxCoordinator::TEvCoordinatorSyncResult(
            record.GetCookie(),
            VolatileState.CompleteStep,
            VolatileState.LatestKnownStep,
            SubjectiveTime(),
            TabletID(),
            record.GetCoordinatorID()
        ));
}

void TTxMediator::HandleEnqueue(TEvTxCoordinator::TEvCoordinatorSync::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID() << " HANDLE Enqueue EvCoordinatorSync ");

    CoordinatorsSyncEnqueued[ev->Sender] = ev->Get()->Record;
}

void TTxMediator::HandleEnqueueWatch(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID() << " ENQUEUE Watch from# " << ev->Sender << " server# " << ev->Recipient);
    EnqueuedWatch.push_back(std::move(ev));
}

void TTxMediator::DoConfigure(const TEvSubDomain::TEvConfigure &ev, const TActorContext &ctx, const TActorId &ackTo) {
    const TEvSubDomain::TEvConfigure::ProtoRecordType &record = ev.Record;
    if (0 == record.CoordinatorsSize() || 0 == record.GetTimeCastBucketsPerMediator()) {
        LOG_ERROR_S(ctx, NKikimrServices::TX_MEDIATOR
                     , "tablet# " << TabletID() << " actor# " << SelfId()
                    << " Apply TEvMediatorConfiguration Version# " << record.GetVersion()
                    << " recive empty coordinators set");
        Y_ABORT("empty coordinators set");
        return;
    }

    TVector<TCoordinatorId> coordinators;
    coordinators.reserve(record.CoordinatorsSize());

    for (auto id: record.GetCoordinators()) {
        Y_ABORT_UNLESS(TabletID() != id, "found self id in coordinators list");
        coordinators.push_back(id);
    }

    Execute(CreateTxConfigure(ackTo, record.GetVersion(), coordinators, record.GetTimeCastBucketsPerMediator()), ctx);
}

void TTxMediator::Handle(TEvSubDomain::TEvConfigure::TPtr &ev, const TActorContext &ctx) {
    const TEvSubDomain::TEvConfigure::ProtoRecordType &record = ev->Get()->Record;
    LOG_NOTICE_S(ctx, NKikimrServices::TX_MEDIATOR
                 , "tablet# " << TabletID() << " actor# " << SelfId()
                << " HANDLE TEvMediatorConfiguration Version# " << record.GetVersion());

    DoConfigure(*ev->Get(), ctx, ev->Sender);
}

void TTxMediator::Handle(TEvTxCoordinator::TEvCoordinatorSync::TPtr &ev, const TActorContext &ctx) {
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID() << " HANDLE EvCoordinatorSync ");
    const NKikimrTx::TEvCoordinatorSync &record = ev->Get()->Record;
    ReplySync(ev->Sender, record, ctx);
}

void TTxMediator::ReplyStep(const TActorId &sender, NKikimrTx::TEvCoordinatorStepResult::EStatus status, const NKikimrTx::TEvCoordinatorStep &request, const TActorContext &ctx) {
//    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID() << " SEND EvCoordinatorStepResult"
//        << " to# " << sender.ToString() << " sender status# " << status << " step# " << request.GetStep()
//        << " CompleteStep# " << VolatileState.CompleteStep
//        << " LatestKnownStep# " << VolatileState.LatestKnownStep << " SubjectiveTime# " << SubjectiveTime()
//        << " Coordinator# " << request.GetCoordinatorID());
    ctx.Send(sender,
        new TEvTxCoordinator::TEvCoordinatorStepResult(
            status,
            request.GetStep(),
            VolatileState.CompleteStep,
            VolatileState.LatestKnownStep,
            SubjectiveTime(),
            TabletID(),
            request.GetCoordinatorID()
        ));
}

ui64 TTxMediator::FindProgressCandidate() {
    // in perfect case here could be priority queue, but who cares? (domain coordinators count is bounded and steps are not so frequent).
    ui64 candidate = Max<ui64>();
    for (TMap<ui64, TCoordinatorInfo>::iterator it = VolatileState.Domain.begin(), end = VolatileState.Domain.end(); it != end; ++it) {
        const TCoordinatorInfo &info = it->second;
        if (info.Queue.empty()) // could not progress w/o info from any domain coordinator
            return Max<ui64>();

        TCoordinatorStep *head = info.Queue.front().Get();
        if (head->Step < candidate)
            candidate = head->Step;
    }

    return candidate;
}

void TTxMediator::Progress(ui64 to, const TActorContext &ctx) {
    const ui64 from = VolatileState.CompleteStep;

    TAutoPtr<TMediateStep> mds(new TMediateStep(from, to));

    ui64 txCount = 0;
    for (TMap<ui64, TCoordinatorInfo>::iterator it = VolatileState.Domain.begin(), end = VolatileState.Domain.end(); it != end; ++it) {
        TCoordinatorInfo &info = it->second;
        Y_ABORT_UNLESS(!info.Queue.empty() && info.Queue.front()->Step >= to);

        if (info.Queue.front()->Step != to)
            continue;

        TAutoPtr<TCoordinatorStep> step = info.Queue.front();
        info.Queue.pop_front();
        info.KnownPrevStep = step->Step;

        txCount += step->Transactions.size();
        for (TTx &tx : step->Transactions)
            tx.AckTo = info.AckTo;

        mds->Steps.push_back(step);
    }

    if (txCount) {
        // push steps to execution
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID() << " SEND EvCommitStep to# " <<
            ExecQueue.ToString() << " ExecQueue " << mds->ToString() << " marker# M0");
    }
    ctx.Send(ExecQueue, new TEvTxMediator::TEvCommitStep(mds));

    VolatileState.CompleteStep = to;
}

void TTxMediator::CheckProgress(const TActorContext &ctx) {
    ui64 candidate = FindProgressCandidate();
    while (candidate != Max<ui64>()) {
        Progress(candidate, ctx);
        candidate = FindProgressCandidate();
    }
}

void TTxMediator::RequestLostAcks(const TActorId &sender, const NKikimrTx::TEvCoordinatorStep &request, const TActorContext &ctx) {
    TAutoPtr<TCoordinatorStep> step(new TCoordinatorStep(request));
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID() << " SEND EvRequestLostAcks to# "
        << ExecQueue.ToString() << " ExecQueue step " << step->ToString());
    ctx.Send(ExecQueue, new TEvTxMediator::TEvRequestLostAcks(step, sender));
}

void TTxMediator::ProcessDomainStep(const TActorId &sender, const NKikimrTx::TEvCoordinatorStep &request, ui64 coordinator, TCoordinatorInfo &info, const TActorContext &ctx) {
    Y_UNUSED(coordinator);
    if (request.GetActiveCoordinatorGeneration() != info.ActiveCoordinatorGeneration) {
        if (request.GetActiveCoordinatorGeneration() < info.ActiveCoordinatorGeneration) // outdated request
            return;
        else {
            info.ActiveCoordinatorGeneration = request.GetActiveCoordinatorGeneration();
            info.AckTo = sender;
        }
    }

    const TStepId step = request.GetStep();

    if (step <= VolatileState.CompleteStep) {
        return RequestLostAcks(sender, request, ctx);
    }

    if (!info.Queue.empty() && info.Queue.back()->Step >= step) {
        // it is supposed that we already knew that plan step from previous connections
        // all recieved plans are immutable and order is fixed so just skip it
        ReplyStep(sender, NKikimrTx::TEvCoordinatorStepResult::ACCEPTED_DOMAIN, request, ctx);
        return;
    }

    info.Queue.push_back(new TCoordinatorStep(request));

    if (VolatileState.LatestKnownStep < step)
        VolatileState.LatestKnownStep = step;

    ReplyStep(sender, NKikimrTx::TEvCoordinatorStepResult::ACCEPTED_DOMAIN, request, ctx);
    CheckProgress(ctx);
}

void TTxMediator::ProcessForeignStep(const TActorId &sender, const NKikimrTx::TEvCoordinatorStep &request, ui64 coordinator, TCoordinatorInfo &info, const TActorContext &ctx) {
    Y_UNUSED(sender);
    Y_UNUSED(request);
    Y_UNUSED(coordinator);
    Y_UNUSED(info);
    Y_UNUSED(ctx);

    Y_ABORT("TODO");
}

void TTxMediator::Handle(TEvTxCoordinator::TEvCoordinatorStep::TPtr &ev, const TActorContext &ctx) {
    const NKikimrTx::TEvCoordinatorStep &record = ev->Get()->Record;

    Y_ABORT_UNLESS(record.GetMediatorID() == TabletID());
    const ui64 coordinator = record.GetCoordinatorID();
    const ui64 step = record.GetStep();
    const ui64 transactionsCount = record.TransactionsSize();

    if (transactionsCount) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID()
            << " HANDLE EvCoordinatorStep coordinator# " << coordinator << " step# " << step);

        LOG_INFO(ctx, NKikimrServices::TX_MEDIATOR,
            "Coordinator step: Mediator [%" PRIu64 "],"
            " Coordinator [%" PRIu64 "],"
            " step# [%" PRIu64 "]"
            " transactions [%" PRIu64 "]",
            TabletID(), coordinator, step, transactionsCount);
    }

    // is domain?
    TMap<ui64, TCoordinatorInfo>::iterator domainIt = VolatileState.Domain.find(coordinator);
    if (domainIt != VolatileState.Domain.end())
        return ProcessDomainStep(ev->Sender, record, coordinator, domainIt->second, ctx);

    // is foreign?
    return ProcessForeignStep(ev->Sender, record, coordinator, VolatileState.Foreign[coordinator], ctx);
}

void TTxMediator::HandleForwardWatch(TAutoPtr<IEventHandle> &ev, const TActorContext &ctx) {
    if (!ConnectedServers.contains(ev->Recipient)) {
        // Server disconnected before this message could be processed
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID()
            << " IGNORE Watch from# " << ev->Sender << " server# " << ev->Recipient);
        return;
    }

    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID()
        << " FORWARD Watch from# " << ev->Sender << " to# " << ExecQueue.ToString() << " ExecQueue");
    // Preserve Recipient (server) and InterconnectSession
    ev->Rewrite(ev->GetTypeRewrite(), ExecQueue);
    ctx.ExecutorThread.Send(ev.Release());
}

void TTxMediator::Handle(TEvTabletPipe::TEvServerConnected::TPtr &ev, const TActorContext &ctx) {
    auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID() << " server# " << msg->ServerId << " connected");
    ConnectedServers.insert(msg->ServerId);
}

void TTxMediator::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr &ev, const TActorContext &ctx) {
    auto* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR, "tablet# " << TabletID() << " server# " << msg->ServerId << " disconnnected");
    ConnectedServers.erase(msg->ServerId);
    if (!!ExecQueue) {
        Send(ExecQueue, new TEvTxMediator::TEvServerDisconnected(msg->ServerId));
    }
}

TTxMediator::TTxMediator(TTabletStorageInfo *info, const TActorId &tablet)
    : TActor(&TThis::StateInit)
    , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
{}

}
}
