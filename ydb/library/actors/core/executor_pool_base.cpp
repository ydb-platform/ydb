#include "actorsystem.h"
#include "actor.h"
#include "executor_pool_base.h"
#include "executor_pool_basic_feature_flags.h"
#include "executor_thread.h"
#include "mailbox.h"
#include "probes.h"
#include <ydb/library/actors/util/datetime.h>

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    void DoActorInit(TActorSystem* sys, IActor* actor, const TActorId& self, const TActorId& owner) {
        actor->SelfActorId = self;
        actor->DoActorInit();
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

        for (auto& u : stats.UsageByActivity) {
            u.fill(0);
        }

        auto accountUsage = [&](ui32 activityType, double usage) {
            Y_ABORT_UNLESS(0 <= usage);
            Y_ABORT_UNLESS(usage <= 1);
            int bin = Min<int>(9, usage * 10);
            ++stats.UsageByActivity[activityType][bin];
        };

        std::fill(stats.StuckActorsByActivity.begin(), stats.StuckActorsByActivity.end(), 0);

        with_lock (StuckObserverMutex) {
            for (size_t i = 0; i < Actors.size(); ++i) {
                IActor *actor = Actors[i];
                Y_ABORT_UNLESS(actor->StuckIndex == i);
                const TDuration delta = now - actor->LastReceiveTimestamp;
                if (delta > TDuration::Seconds(30)) {
                    ++stats.StuckActorsByActivity[actor->GetActivityType()];
                }
                accountUsage(actor->GetActivityType(), actor->GetUsage(GetCycleCountFast()));
            }
            for (const auto& [activityType, usage] : DeadActorsUsage) {
                accountUsage(activityType, usage);
            }
            DeadActorsUsage.clear();
        }
    }
#endif

    TExecutorPoolBase::TExecutorPoolBase(ui32 poolId, ui32 threads, TAffinity* affinity, bool useRingQueue)
        : TExecutorPoolBaseMailboxed(poolId)
        , PoolThreads(threads)
        , ThreadsAffinity(affinity)
    {
        if (useRingQueue) {
            Activations.emplace<TRingActivationQueue>(threads == 1);
        } else {
            Activations.emplace<TUnorderedCacheActivationQueue>();
        }
    }

    TExecutorPoolBase::~TExecutorPoolBase() {
        while (std::visit([](auto &x){return x.Pop(0);}, Activations))
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
        Y_DEBUG_ABORT_UNLESS(ev->GetRecipientRewrite().PoolID() == PoolId);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        RelaxedStore(&ev->SendTime, (::NHPTimer::STime)GetCycleCountFast());
#endif
        if (TlsThreadContext) {
            TlsThreadContext->IsCurrentRecipientAService = ev->Recipient.IsService();
        }
        return MailboxTable->SendTo(ev, this);
    }

    bool TExecutorPoolBaseMailboxed::SpecificSend(TAutoPtr<IEventHandle>& ev) {
        Y_DEBUG_ABORT_UNLESS(ev->GetRecipientRewrite().PoolID() == PoolId);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        RelaxedStore(&ev->SendTime, (::NHPTimer::STime)GetCycleCountFast());
#endif
        if (TlsThreadContext) {
            TlsThreadContext->IsCurrentRecipientAService = ev->Recipient.IsService();
        }
        return MailboxTable->SpecificSendTo(ev, this);
    }

    void TExecutorPoolBase::ScheduleActivation(ui32 activation) {
#ifdef RING_ACTIVATION_QUEUE
        ScheduleActivationEx(activation, 0);
#else
        ScheduleActivationEx(activation, AtomicIncrement(ActivationsRevolvingCounter));
#endif
    }

    Y_FORCE_INLINE bool IsAllowedToCapture(IExecutorPool *self) {
        if (TlsThreadContext->Pool != self || TlsThreadContext->CapturedType == ESendingType::Tail) {
            return false;
        }
        return TlsThreadContext->SendingType != ESendingType::Common;
    }

    Y_FORCE_INLINE bool IsTailSend(IExecutorPool *self) {
        return TlsThreadContext->Pool == self && TlsThreadContext->SendingType == ESendingType::Tail && TlsThreadContext->CapturedType != ESendingType::Tail;
    }

    void TExecutorPoolBase::SpecificScheduleActivation(ui32 activation) {
        if (NFeatures::IsCommon() && IsAllowedToCapture(this) || IsTailSend(this)) {
            std::swap(TlsThreadContext->CapturedActivation, activation);
            TlsThreadContext->CapturedType = TlsThreadContext->SendingType;
        }
        if (activation) {
#ifdef RING_ACTIVATION_QUEUE
        ScheduleActivationEx(activation, 0);
#else
        ScheduleActivationEx(activation, AtomicIncrement(ActivationsRevolvingCounter));
#endif
        }
    }

    TActorId TExecutorPoolBaseMailboxed::Register(IActor* actor, TMailboxType::EType mailboxType, ui64 revolvingWriteCounter, const TActorId& parentId) {
        NHPTimer::STime hpstart = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_REGISTER, false> activityGuard(hpstart);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        ui32 at = actor->GetActivityType();
        Y_DEBUG_ABORT_UNLESS(at < Stats.ActorsAliveByActivity.size());
        if (at >= Stats.MaxActivityType()) {
            at = TActorTypeOperator::GetActorActivityIncorrectIndex();
            Y_ABORT_UNLESS(at < Stats.ActorsAliveByActivity.size());
        }
        AtomicIncrement(Stats.ActorsAliveByActivity[at]);
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

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        if (ActorSystem->MonitorStuckActors()) {
            with_lock (StuckObserverMutex) {
                Y_ABORT_UNLESS(actor->StuckIndex == Max<size_t>());
                actor->StuckIndex = Actors.size();
                Actors.push_back(actor);
            }
        }
#endif

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
                Y_ABORT();
        }

        NHPTimer::STime elapsed = GetCycleCountFast() - hpstart;
        if (elapsed > 1000000) {
            LWPROBE(SlowRegisterNew, PoolId, NHPTimer::GetSeconds(elapsed) * 1000.0);
        }

        return actorId;
    }

    TActorId TExecutorPoolBaseMailboxed::Register(IActor* actor, TMailboxHeader* mailbox, ui32 hint, const TActorId& parentId) {
        NHPTimer::STime hpstart = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_REGISTER, false> activityGuard(hpstart);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        ui32 at = actor->GetActivityType();
        if (at >= Stats.MaxActivityType())
            at = 0;
        AtomicIncrement(Stats.ActorsAliveByActivity[at]);
#endif
        AtomicIncrement(ActorRegistrations);

        const ui64 localActorId = AllocateID();
        mailbox->AttachActor(localActorId, actor);

        const TActorId actorId(ActorSystem->NodeId, PoolId, localActorId, hint);
        DoActorInit(ActorSystem, actor, actorId, parentId);

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        if (ActorSystem->MonitorStuckActors()) {
            with_lock (StuckObserverMutex) {
                Y_ABORT_UNLESS(actor->StuckIndex == Max<size_t>());
                actor->StuckIndex = Actors.size();
                Actors.push_back(actor);
            }
        }
#endif

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
