#pragma once

#include "executor_pool_base.h"
#include "actor.h"
#include "activity_guard.h"
#include "mailbox.h"
#include "probes.h"

namespace NActors {
    LWTRACE_USING(ACTORLIB_PROVIDER);

    template<class TInitializer>
    TActorId TExecutorPoolBaseMailboxed::RegisterWithInitializer(IActor* actor, TMailboxCache& cache,
            ui64 revolvingWriteCounter, const TActorId& parentId, TInitializer&& initialize) {
        NHPTimer::STime hpstart = GetCycleCountFast();
        TInternalActorTypeGuard<EInternalActorSystemActivity::ACTOR_SYSTEM_REGISTER, false> activityGuard(hpstart);
#ifdef ACTORSLIB_COLLECT_EXEC_STATS
        ui32 at = actor->GetActivityType().GetIndex();
        Y_DEBUG_ABORT_UNLESS(at < Stats.ActorsAliveByActivity.size());
        if (at >= Stats.MaxActivityType()) {
            at = TActorTypeOperator::GetActorActivityIncorrectIndex();
            Y_ABORT_UNLESS(at < Stats.ActorsAliveByActivity.size());
        }
        AtomicIncrement(Stats.ActorsAliveByActivity[at]);
#endif
        AtomicIncrement(ActorRegistrations);

        TMailbox* mailbox = cache ? cache.Allocate() : MailboxTable->Allocate();

        // Free mailboxes are not executing, lock to a normal state
        mailbox->LockFromFree();

        initialize(mailbox, actor);

        const ui64 localActorId = AllocateID();
        mailbox->AttachActor(localActorId, actor);

        // do init
        const TActorId actorId(ActorSystem->NodeId, PoolId, localActorId, mailbox->Hint);
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

        mailbox->Unlock(this, GetCycleCountFast(), revolvingWriteCounter);

        NHPTimer::STime elapsed = GetCycleCountFast() - hpstart;
        if (elapsed > 1000000) {
            LWPROBE(SlowRegisterNew, PoolId, NHPTimer::GetSeconds(elapsed) * 1000.0);
        }

        return actorId;
    }

} // namespace NActors
