#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NActors {

    struct TActorLivenessCheckTarget {
        TActorId ActorId;
        ui64 Cookie = 0;
    };

    class TActorLivenessChecker
        : public TActorBootstrapped<TActorLivenessChecker>
    {
    public:
        explicit TActorLivenessChecker(TVector<TActorLivenessCheckTarget> targets);

        void Bootstrap();

        STFUNC(StateFunc);

    protected:
        // Hooks run synchronously in the checker actor context. OnFinish is
        // called exactly once after every target has produced a response.
        virtual void OnAlive(const TActorLivenessCheckTarget& target);
        virtual void OnDead(const TActorLivenessCheckTarget& target);
        virtual void OnUnsure(const TActorLivenessCheckTarget& target);
        virtual void OnFinish();

    private:
        void Handle(TEvents::TEvActorAlive::TPtr& ev);
        void Handle(TEvents::TEvActorDead::TPtr& ev);
        void Handle(TEvents::TEvActorLivenessUnsure::TPtr& ev);

        bool ExtractTarget(const TActorId& actorId, TActorLivenessCheckTarget& target);
        void CompleteIfDone();
        void Complete();

    private:
        THashMap<TActorId, ui64> PendingTargets;
    };

}
