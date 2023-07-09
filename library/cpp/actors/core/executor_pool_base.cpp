#include "actorsystem.h"
#include "actor.h"
#include "executor_pool_base.h"
#include "executor_thread.h"
#include "mailbox.h"
#include "probes.h"
#include <library/cpp/actors/util/datetime.h>

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    void DoActorInit(TActorSystem* sys, IActor* actor, const TActorId& self, const TActorId& owner) {
        actor->SelfActorId = self;
        actor->Registered(sys, owner);
    }

    TExecutorPoolBaseMailboxed::TExecutorPoolBaseMailboxed(ui32 poolId)
        : IExecutorPool(poolId)
        , ActorSystem(nullptr)
        , MailboxTable(new TMailboxTable)
    {}

    TExecutorPoolBaseMailboxed::~TExecutorPoolBaseMailboxed() {
        MailboxTable.Destroy();
    }

#if defined(ACTORSLIB_COLLECT_EXEC_STATS)
    void TExecutorPoolBaseMailboxed::RecalculateStuckActors(TExecutorThreadStats& stats) const {
        if (!ActorSystem || !ActorSystem->MonitorStuckActors()) {
            return;
        }

        const TMonotonic now = ActorSystem->Monotonic();

        std::vector<ui32> stuckActors;
        stuckActors.reserve(Actors.size());

        with_lock (StuckObserverMutex) {
            for (IActor *actor : Actors) {
                const TDuration delta = now - actor->LastReceiveTimestamp;
                if (delta > TDuration::Seconds(30)) {
                    stuckActors.push_back(actor->GetActivityType());
                }
            }
        }

        std::fill(stats.StuckActorsByActivity.begin(), stats.StuckActorsByActivity.end(), 0);
        for (ui32 activity : stuckActors) {
            ++stats.StuckActorsByActivity[activity];
        }
    }
#endif

    TExecutorPoolBase::TExecutorPoolBase(ui32 poolId, ui32 threads, TAffinity* affinity)
        : TExecutorPoolBaseMailboxed(poolId)
        , PoolThreads(threads)
        , ThreadsAffinity(affinity)
    {}

    TExecutorPoolBase::~TExecutorPoolBase() {
        while (Activations.Pop(0))
            ;
    }

    void TExecutorPoolBaseMailboxed::ReclaimMailbox(TMailboxType::EType mailboxType, ui32 hint, TWorkerId workerId, ui64 revolvingWriteCounter) {
        Y_UNUSED(workerId);
        MailboxTable->ReclaimMailbox(mailboxType, hint, revolvingWriteCounter);
    }

    TMailboxHeader *TExecutorPoolBaseMailboxed::ResolveMailbox(ui32 hint) {
        return MailboxTable->Get(hint);
    }

    ui64 TExecutorPoolBaseMailboxed::AllocateID() {
        return ActorSystem->AllocateIDSpace(1);
    }

    bool TExecutorPoolBaseMailboxed::Send(TAutoPtr<IEventHandle>& ev) {
        Y_VERIFY_DEBUG(ev->GetRecipientRewrite().PoolID() == PoolId);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        RelaxedStore(&ev->SendTime, (::NHPTimer::STime)GetCycleCountFast());
#endif
        return MailboxTable->SendTo(ev, this);
    }

    bool TExecutorPoolBaseMailboxed::SpecificSend(TAutoPtr<IEventHandle>& ev) {
        Y_VERIFY_DEBUG(ev->GetRecipientRewrite().PoolID() == PoolId);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        RelaxedStore(&ev->SendTime, (::NHPTimer::STime)GetCycleCountFast());
#endif
        return MailboxTable->SpecificSendTo(ev, this);
    }

    void TExecutorPoolBase::ScheduleActivation(ui32 activation) {
        ScheduleActivationEx(activation, AtomicIncrement(ActivationsRevolvingCounter));
    }

    Y_FORCE_INLINE bool IsAllowedToCapture(IExecutorPool *self) {
        if (TlsThreadContext->Pool != self || TlsThreadContext->CapturedType == ESendingType::Tail) {
            return false;
        }
        return TlsThreadContext->SendingType != ESendingType::Common;
    }

    void TExecutorPoolBase::SpecificScheduleActivation(ui32 activation) {
        if (IsAllowedToCapture(this)) {
            std::swap(TlsThreadContext->CapturedActivation, activation);
            TlsThreadContext->CapturedType = TlsThreadContext->SendingType;
        }
        if (activation) {
            ScheduleActivationEx(activation, AtomicIncrement(ActivationsRevolvingCounter));
        }
    }

    TActorId TExecutorPoolBaseMailboxed::Register(IActor* actor, TMailboxType::EType mailboxType, ui64 revolvingWriteCounter, const TActorId& parentId) {
        NHPTimer::STime hpstart = GetCycleCountFast();
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        ui32 at = actor->GetActivityType();
        Y_VERIFY_DEBUG(at < Stats.ActorsAliveByActivity.size());
        if (at >= Stats.MaxActivityType()) {
            at = TActorTypeOperator::GetActorActivityIncorrectIndex();
            Y_VERIFY(at < Stats.ActorsAliveByActivity.size());
        }
        AtomicIncrement(Stats.ActorsAliveByActivity[at]);
        if (ActorSystem->MonitorStuckActors()) {
            with_lock (StuckObserverMutex) {
                Y_VERIFY(actor->StuckIndex == Max<size_t>());
                actor->StuckIndex = Actors.size();
                Actors.push_back(actor);
            }
        }
#endif
        AtomicIncrement(ActorRegistrations);

        // first step - find good enough mailbox
        ui32 hint = 0;
        TMailboxHeader* mailbox = nullptr;

        if (revolvingWriteCounter == 0)
            revolvingWriteCounter = AtomicIncrement(RegisterRevolvingCounter);

        {
            ui32 hintBackoff = 0;

            while (hint == 0) {
                hint = MailboxTable->AllocateMailbox(mailboxType, ++revolvingWriteCounter);
                mailbox = MailboxTable->Get(hint);

                if (!mailbox->LockFromFree()) {
                    MailboxTable->ReclaimMailbox(mailboxType, hintBackoff, ++revolvingWriteCounter);
                    hintBackoff = hint;
                    hint = 0;
                }
            }

            MailboxTable->ReclaimMailbox(mailboxType, hintBackoff, ++revolvingWriteCounter);
        }

        const ui64 localActorId = AllocateID();

        // ok, got mailbox
        mailbox->AttachActor(localActorId, actor);

        // do init
        const TActorId actorId(ActorSystem->NodeId, PoolId, localActorId, hint);
        DoActorInit(ActorSystem, actor, actorId, parentId);

        // Once we unlock the mailbox the actor starts running and we cannot use the pointer any more
        actor = nullptr;

        switch (mailboxType) {
            case TMailboxType::Simple:
                UnlockFromExecution((TMailboxTable::TSimpleMailbox*)mailbox, this, false, hint, MaxWorkers, ++revolvingWriteCounter);
                break;
            case TMailboxType::Revolving:
                UnlockFromExecution((TMailboxTable::TRevolvingMailbox*)mailbox, this, false, hint, MaxWorkers, ++revolvingWriteCounter);
                break;
            case TMailboxType::HTSwap:
                UnlockFromExecution((TMailboxTable::THTSwapMailbox*)mailbox, this, false, hint, MaxWorkers, ++revolvingWriteCounter);
                break;
            case TMailboxType::ReadAsFilled:
                UnlockFromExecution((TMailboxTable::TReadAsFilledMailbox*)mailbox, this, false, hint, MaxWorkers, ++revolvingWriteCounter);
                break;
            case TMailboxType::TinyReadAsFilled:
                UnlockFromExecution((TMailboxTable::TTinyReadAsFilledMailbox*)mailbox, this, false, hint, MaxWorkers, ++revolvingWriteCounter);
                break;
            default:
                Y_FAIL();
        }

        NHPTimer::STime elapsed = GetCycleCountFast() - hpstart;
        if (elapsed > 1000000) {
            LWPROBE(SlowRegisterNew, PoolId, NHPTimer::GetSeconds(elapsed) * 1000.0);
        }

        return actorId;
    }

    TActorId TExecutorPoolBaseMailboxed::Register(IActor* actor, TMailboxHeader* mailbox, ui32 hint, const TActorId& parentId) {
        NHPTimer::STime hpstart = GetCycleCountFast();
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        ui32 at = actor->GetActivityType();
        if (at >= Stats.MaxActivityType())
            at = 0;
        AtomicIncrement(Stats.ActorsAliveByActivity[at]);
        if (ActorSystem->MonitorStuckActors()) {
            with_lock (StuckObserverMutex) {
                Y_VERIFY(actor->StuckIndex == Max<size_t>());
                actor->StuckIndex = Actors.size();
                Actors.push_back(actor);
            }
        }
#endif
        AtomicIncrement(ActorRegistrations);

        const ui64 localActorId = AllocateID();
        mailbox->AttachActor(localActorId, actor);

        const TActorId actorId(ActorSystem->NodeId, PoolId, localActorId, hint);
        DoActorInit(ActorSystem, actor, actorId, parentId);
        NHPTimer::STime elapsed = GetCycleCountFast() - hpstart;
        if (elapsed > 1000000) {
            LWPROBE(SlowRegisterAdd, PoolId, NHPTimer::GetSeconds(elapsed) * 1000.0);
        }

        return actorId;
    }

    TAffinity* TExecutorPoolBase::Affinity() const {
        return ThreadsAffinity.Get();
    }

    bool TExecutorPoolBaseMailboxed::Cleanup() {
        return MailboxTable->Cleanup();
    }

    ui32 TExecutorPoolBase::GetThreads() const {
        return PoolThreads;
    }
}
