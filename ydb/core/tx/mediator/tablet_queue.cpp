#include "mediator_impl.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/tx_processing.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tablet/tablet_pipe_client_cache.h>

#include <util/string/builder.h>
#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NTxMediator {

class TTxMediatorTabletQueue : public TActor<TTxMediatorTabletQueue> {
    struct TStepEntry {
        const TStepId Step;
        ui64 RefCounter;

        TStepEntry(TStepId step)
            : Step(step)
            , RefCounter(0)
        {}
    };

    struct TTabletEntry {
        enum EState {
            StateInit,
            StateConnect,
            StateConnected,
        };

        struct TStep {
            TStepEntry * const StepRef;
            TVector<TTx> Transactions;

            TStep(TStepEntry *stepRef)
                : StepRef(stepRef)
            {}
        };

        typedef TOneOneQueueInplace<TStep *, 32> TQueueType;

        EState State;
        TAutoPtr<TQueueType, TQueueType::TPtrCleanDestructor> Queue;
        TMap<TStepId, TVector<TTx>> OutOfOrder; // todo: replace TVector<> with chunked queue to conserve copying

        TTabletEntry()
            : State(StateInit)
            , Queue(new TQueueType())
        {}

        void MergeOutOfOrder(TStep *x);
        void MergeToOutOfOrder(TStepId step, TVector<TTx> &update);
    };

    const TActorId Owner;
    const ui64 Mediator;
    const ui64 HashRange;
    const ui64 HashBucket;

    THashMap<TTabletId, TTabletEntry> PerTabletPlanQueue; // by tablet entries

    typedef TOneOneQueueInplace<TStepEntry *, 512> TStepCommitQueue;
    TAutoPtr<TStepCommitQueue, TStepCommitQueue::TPtrCleanDestructor> StepCommitQueue;

    TAutoPtr<NTabletPipe::IClientCache> Pipes;

    TStepId AcceptedStep;
    TStepId CommitedStep;
    TStepEntry *ActiveStep;

    THashSet<TActorId> TimecastWatches;
    ::NMonitoring::TDynamicCounters::TCounterPtr TimecastLagCounter;

    void SendToTablet(TTabletEntry::TStep *tabletStep, ui64 tablet, const TActorContext &ctx) {
        auto evx = new TEvTxProcessing::TEvPlanStep(tabletStep->StepRef->Step, Mediator, tablet);
        evx->Record.MutableTransactions()->Reserve(tabletStep->Transactions.size());
        for (const TTx &tx : tabletStep->Transactions) {
            NKikimrTx::TMediatorTransaction *x = evx->Record.AddTransactions();
            x->SetTxId(tx.TxId);
            if (tx.Moderator)
                x->SetModerator(tx.Moderator);
            ActorIdToProto(tx.AckTo, x->MutableAckTo());
            LOG_DEBUG(ctx, NKikimrServices::TX_MEDIATOR_PRIVATE, "Send from %" PRIu64 " to tablet %" PRIu64 ", step# %"
                PRIu64 ", txid# %" PRIu64 ", marker M5" PRIu64, Mediator, tablet, tabletStep->StepRef->Step, tx.TxId);
        }
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " SEND to# " << tablet << " " << evx->ToString());
        Pipes->Send(ctx, tablet, evx);
    }

    void CheckStepHead(const TActorContext &ctx) {
        Y_UNUSED(ctx);

        bool updateTimecast = false;
        while (TStepEntry *sx = StepCommitQueue->Head()) {
            if (sx->RefCounter != 0 || sx->Step > AcceptedStep)
                break;

            updateTimecast = true;
            CommitedStep = sx->Step;
            delete StepCommitQueue->Pop();
        }

        if (updateTimecast) {
            if (!TimecastLagCounter)
                TimecastLagCounter = GetServiceCounters(AppData(ctx)->Counters, "processing")->GetSubgroup("mediator", ToString(Mediator))->GetSubgroup("sensor", "TimecastLag")->GetNamedCounter("Bucket", ToString(HashBucket));
            *TimecastLagCounter = (AcceptedStep - CommitedStep);

            TEvMediatorTimecast::TEvUpdate evx;
            evx.Record.SetMediator(Mediator);
            evx.Record.SetBucket(HashBucket);
            evx.Record.SetTimeBarrier(CommitedStep);
            TAllocChunkSerializer serializer;
            const bool success = evx.SerializeToArcadiaStream(&serializer);
            Y_ABORT_UNLESS(success);
            TIntrusivePtr<TEventSerializedData> data = serializer.Release(evx.CreateSerializationInfo());

            // todo: we must throttle delivery
            const ui32 sendFlags = IEventHandle::FlagTrackDelivery;
            for (const TActorId &x : TimecastWatches) {
                LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
                    << " Mediator# " << Mediator << " SEND to# " << x.ToString() << " " << evx.ToString());
                ctx.ExecutorThread.Send(new IEventHandle(TEvMediatorTimecast::TEvUpdate::EventType, sendFlags, x, ctx.SelfID, data, 0));
            }
        }
    }

    void Handle(TEvTxMediator::TEvCommitTabletStep::TPtr &ev, const TActorContext &ctx) {
        TEvTxMediator::TEvCommitTabletStep *msg = ev->Get();
        const TStepId step = msg->Step;
        const TTabletId tablet = msg->TabletId;

        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString() << " marker# M4");

        TTabletEntry &tabletEntry = PerTabletPlanQueue[tablet];
        if (!ActiveStep) {
            ActiveStep = new TStepEntry(step);
            StepCommitQueue->Push(ActiveStep);
        }

        Y_ABORT_UNLESS(ActiveStep->Step == step);
        ++ActiveStep->RefCounter;

        TTabletEntry::TStep *tabletStep = new TTabletEntry::TStep(ActiveStep);
        tabletStep->Transactions.swap(msg->Transactions);
        tabletEntry.Queue->Push(tabletStep);

        switch (tabletEntry.State) {
        case TTabletEntry::StateInit:
            Pipes->Prepare(ctx, tablet);
            tabletEntry.State = TTabletEntry::StateConnect;
            break;
        case TTabletEntry::StateConnect:
            break;
        case TTabletEntry::StateConnected:
            SendToTablet(tabletStep, tablet, ctx);
            break;
        }
    }

    void Handle(TEvTxMediator::TEvOoOTabletStep::TPtr &ev, const TActorContext &ctx) {
        TEvTxMediator::TEvOoOTabletStep *msg = ev->Get();
        const TStepId step = msg->Step;
        const TTabletId tablet = msg->TabletId;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString());

        TTabletEntry &tabletEntry = PerTabletPlanQueue[tablet];
        TTabletEntry::TStep *headStep = tabletEntry.Queue->Head();
        if (!headStep || headStep->StepRef->Step > step) {
            AckOoO(tablet, step, msg->Transactions, ctx); // from already confirmed space, just reply right here
        } else { // not yet confirmed, save for later use
            tabletEntry.MergeToOutOfOrder(step, msg->Transactions);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr &ev, const TActorContext &ctx) {
        const TEvTabletPipe::TEvClientConnected *msg = ev->Get();
        const TTabletId tablet = msg->TabletId;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString());

        TTabletEntry &tabletEntry = PerTabletPlanQueue[tablet];
        Y_ABORT_UNLESS(tabletEntry.State == TTabletEntry::StateConnect);

        if (!Pipes->OnConnect(ev)) {
            if (msg->Dead) {
                LOG_WARN_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
                    << " Mediator# " << Mediator << " HANDLE TEvClientConnected(Dead=true)");
                while (TTabletEntry::TStep *sx = tabletEntry.Queue->Head()) {
                    tabletEntry.MergeOutOfOrder(sx);
                    AckOoO(tablet, sx->StepRef->Step, sx->Transactions, ctx);
                    --sx->StepRef->RefCounter;
                    delete tabletEntry.Queue->Pop();
                }
                PerTabletPlanQueue.erase(tablet);
                CheckStepHead(ctx);
                return;
            }

            Pipes->Prepare(ctx, tablet);
            return;
        }

        tabletEntry.State = TTabletEntry::StateConnected;
        TTabletEntry::TQueueType::TReadIterator it = tabletEntry.Queue->Iterator();
        while (TTabletEntry::TStep *sx = it.Next()) {
            tabletEntry.MergeOutOfOrder(sx);
            SendToTablet(sx, tablet, ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr &ev, const TActorContext &ctx) {
        const TEvTabletPipe::TEvClientDestroyed *msg = ev->Get();
        const TTabletId tablet = msg->TabletId;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString());

        Pipes->OnDisconnect(ev);

        TTabletEntry &tabletEntry = PerTabletPlanQueue[tablet];
        Y_ABORT_UNLESS(tabletEntry.State == TTabletEntry::StateConnected);

        // if connect to tablet lost and tablet is in no use - just forget connection
        if (tabletEntry.Queue->Head() == nullptr) {
            PerTabletPlanQueue.erase(tablet);
        } else { // if have smth in queue - request reconnect
            Pipes->Prepare(ctx, tablet);
            tabletEntry.State = TTabletEntry::StateConnect;
        }
    }

    void Handle(TEvTxMediator::TEvStepPlanComplete::TPtr &ev, const TActorContext &ctx) {
        const TEvTxMediator::TEvStepPlanComplete *msg = ev->Get();
        const TStepId step = msg->Step;
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << msg->ToString());

        if (ActiveStep)
            ActiveStep = nullptr;
        else
            StepCommitQueue->Push(new TStepEntry(step));

        AcceptedStep = step;
        CheckStepHead(ctx);
    }

    void Handle(TEvTxProcessing::TEvPlanStepAccepted::TPtr &ev, const TActorContext &ctx) {
        const NKikimrTx::TEvPlanStepAccepted &record = ev->Get()->Record;
        const TTabletId tablet = record.GetTabletId();
        const TStepId step = record.GetStep();
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << ev->Get()->ToString());

        TTabletEntry *tabletEntry = PerTabletPlanQueue.FindPtr(tablet);
        if (!tabletEntry)
            return;

        TTabletEntry::TStep *headStep = tabletEntry->Queue->Head();
        if (!headStep)
            return;

        // if non-head step confirmed - just skip and wait for head confirmation
        if (headStep->StepRef->Step != step)
            return;

        --headStep->StepRef->RefCounter;
        delete tabletEntry->Queue->Pop();

        // confirm out of order request (if any).
        const auto ooIt = tabletEntry->OutOfOrder.find(step);
        if (ooIt != tabletEntry->OutOfOrder.end()) {
            AckOoO(tablet, step, ooIt->second, ctx);
            tabletEntry->OutOfOrder.erase(ooIt);
        }

        CheckStepHead(ctx);
    }

    void Handle(TEvTxMediator::TEvWatchBucket::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE " << ev->Get()->ToString());
        const TActorId &source = ev->Get()->Source;
        TimecastWatches.insert(source);
    }

    void Handle(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
            << " Mediator# " << Mediator << " HANDLE TEvUndelivered");
        // for now every non-delivery is reason to drop watch
        TimecastWatches.erase(ev->Sender);
    }

    void AckOoO(TTabletId tablet, TStepId step, const TVector<TTx> &transactions, const TActorContext &ctx) {
        TMap<TActorId, TAutoPtr<TEvTxProcessing::TEvPlanStepAck>> acks;
        for (const TTx &tx : transactions) {
            TAutoPtr<TEvTxProcessing::TEvPlanStepAck> &ack = acks[tx.AckTo];
            if (!ack)
                ack = new TEvTxProcessing::TEvPlanStepAck(tablet, step, (const ui64 *)nullptr, (const ui64 *)nullptr);
            ack->Record.AddTxId(tx.TxId);
        }

        for (const auto &x : acks) {
            LOG_DEBUG_S(ctx, NKikimrServices::TX_MEDIATOR_TABLETQUEUE, "Actor# " << ctx.SelfID.ToString()
                << " Mediator# " << Mediator << " SEND to# " << x.first.ToString() << " " << x.second->ToString());
            ctx.Send(x.first, x.second.Release());
        }
    }

    void Die(const TActorContext &ctx) override {
        Pipes->Detach(ctx);
        Pipes.Destroy();

        TActor::Die(ctx);
    }

    static NTabletPipe::TClientConfig GetPipeClientConfig() {
        NTabletPipe::TClientConfig config;
        config.CheckAliveness = true;
        config.RetryPolicy = {
            .RetryLimitCount = 30,
            .MinRetryTime = TDuration::MilliSeconds(10),
            .MaxRetryTime = TDuration::MilliSeconds(500),
            .BackoffMultiplier = 2,
        };
        return config;
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_MEDIATOR_TABLET_QUEUE_ACTOR;
    }

    TTxMediatorTabletQueue(const TActorId &owner, ui64 mediator, ui64 hashRange, ui64 hashBucket)
        : TActor(&TThis::StateFunc)
        , Owner(owner)
        , Mediator(mediator)
        , HashRange(hashRange)
        , HashBucket(hashBucket)
        , StepCommitQueue(new TStepCommitQueue())
        , Pipes(NTabletPipe::CreateUnboundedClientCache(GetPipeClientConfig()))
        , AcceptedStep(0)
        , CommitedStep(0)
        , ActiveStep(nullptr)
    {
       Y_UNUSED(HashRange);
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProcessing::TEvPlanStepAccepted, Handle);
            HFunc(TEvTxMediator::TEvCommitTabletStep, Handle);
            HFunc(TEvTxMediator::TEvStepPlanComplete, Handle);
            HFunc(TEvTxMediator::TEvOoOTabletStep, Handle);
            HFunc(TEvTxMediator::TEvWatchBucket, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvents::TEvUndelivered, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }
};

TString yvector2str(const TVector<TTx>& v) {
    TStringStream stream;
    stream << '{';
    for (auto it = v.begin(); it != v.end(); ++it) {
        if (it != v.begin())
            stream << ',';
        stream << it->TxId;
    }
    stream << '}';
    return stream.Str();
}
//

void TTxMediatorTabletQueue::TTabletEntry::MergeOutOfOrder(TStep *sx) {
    const TStepId step = sx->StepRef->Step;
    const auto ox = OutOfOrder.find(step);
    if (ox != OutOfOrder.end()) {
        const TVector<TTx> &o = ox->second;
        Y_DEBUG_ABORT_UNLESS(
            IsSorted(sx->Transactions.begin(), sx->Transactions.end(), TTx::TCmpOrderId()),
            "%s",
            yvector2str(sx->Transactions).c_str()
        );
        Y_DEBUG_ABORT_UNLESS(IsSorted(o.begin(), o.end(), TTx::TCmpOrderId()), "%s", yvector2str(o).c_str());
        //
        // ok, now merge sorted arrays replacing ack-to
        TVector<TTx>::iterator planIt = sx->Transactions.begin();
        TVector<TTx>::iterator planEnd = sx->Transactions.end();
        TVector<TTx>::const_iterator oooIt = o.begin();
        TVector<TTx>::const_iterator oooEnd = o.end();
        while (oooIt != oooEnd && planIt != planEnd) {
            if (planIt->TxId < oooIt->TxId) {
                ++planIt;
            } else if (planIt->TxId == oooIt->TxId) {
                planIt->AckTo = oooIt->AckTo;
                ++oooIt;
                ++planIt;
            } else {
                Y_ABORT("Inconsistency: Plan TxId %" PRIu64 " > OutOfOrder TxId %" PRIu64, planIt->TxId, oooIt->TxId);
            }
        }
        OutOfOrder.erase(ox);
    }
}

void TTxMediatorTabletQueue::TTabletEntry::MergeToOutOfOrder(TStepId step, TVector<TTx> &update) {
    TVector<TTx> &current = OutOfOrder[step];
    if (current.empty()) {
        current.swap(update);
    } else {
        TVector<TTx> old;
        old.swap(current);
        Y_DEBUG_ABORT_UNLESS(IsSorted(old.begin(), old.end(), TTx::TCmpOrderId()), "%s", yvector2str(old).c_str());
        Y_DEBUG_ABORT_UNLESS(IsSorted(update.begin(), update.end(), TTx::TCmpOrderId()), "%s", yvector2str(update).c_str());
        //
        // now merge old with update
        TVector<TTx>::const_iterator oldIt = old.begin();
        TVector<TTx>::const_iterator oldEnd = old.end();
        TVector<TTx>::const_iterator updIt = update.begin();
        TVector<TTx>::const_iterator updEnd = update.end();

        while (oldIt != oldEnd && updIt != updEnd) {
            if (oldIt->TxId < updIt->TxId) {
                current.push_back(*oldIt);
                ++oldIt;
            } else if (updIt->TxId < oldIt->TxId) {
                current.push_back(*updIt);
                ++updIt;
            } else {
                current.push_back(*updIt);
                ++updIt;
                ++oldIt;
            }
        }

        // append tail
        current.insert(current.end(), oldIt, oldEnd);
        current.insert(current.end(), updIt, updEnd);
        Y_DEBUG_ABORT_UNLESS(IsSorted(current.begin(), current.end(), TTx::TCmpOrderId()), "%s", yvector2str(current).c_str());
        //
    }
}

}

IActor* CreateTxMediatorTabletQueue(const TActorId &owner, ui64 mediator, ui64 hashRange, ui64 hashBucket) {
    return new NTxMediator::TTxMediatorTabletQueue(owner, mediator, hashRange, hashBucket);
}

}
