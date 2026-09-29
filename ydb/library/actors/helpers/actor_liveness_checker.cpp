#include "actor_liveness_checker.h"

#include <ydb/library/actors/core/hfunc.h>

namespace NActors {
    TActorLivenessChecker::TActorLivenessChecker(TVector<TActorLivenessCheckTarget> targets) {
        for (const auto& target : targets) {
            PendingTargets.emplace(target.ActorId, target.Cookie);
        }
    }

    void TActorLivenessChecker::Bootstrap() {
        if (PendingTargets.empty()) {
            return Complete();
        }

        Become(&TThis::StateFunc);
        // Every local liveness probe eventually produces ActorAlive or ActorDead.
        // Remote probes currently produce ActorLivenessUnsure.
        for (const auto& [actorId, cookie] : PendingTargets) {
            SendActorLivenessCheck(actorId, cookie);
        }
    }

    STFUNC(TActorLivenessChecker::StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvents::TEvActorAlive, Handle)
            hFunc(TEvents::TEvActorDead, Handle)
            hFunc(TEvents::TEvActorLivenessUnsure, Handle)
        }
    }

    void TActorLivenessChecker::OnAlive(const TActorLivenessCheckTarget&) {
    }

    void TActorLivenessChecker::OnDead(const TActorLivenessCheckTarget&) {
    }

    void TActorLivenessChecker::OnUnsure(const TActorLivenessCheckTarget&) {
    }

    void TActorLivenessChecker::OnFinish() {
    }

    void TActorLivenessChecker::Handle(TEvents::TEvActorAlive::TPtr& ev) {
        TActorLivenessCheckTarget target;
        if (ExtractTarget(ev->Sender, target)) {
            OnAlive(target);
            CompleteIfDone();
        }
    }

    void TActorLivenessChecker::Handle(TEvents::TEvActorDead::TPtr& ev) {
        TActorLivenessCheckTarget target;
        if (ExtractTarget(ev->Sender, target)) {
            OnDead(target);
            CompleteIfDone();
        }
    }

    void TActorLivenessChecker::Handle(TEvents::TEvActorLivenessUnsure::TPtr& ev) {
        TActorLivenessCheckTarget target;
        if (ExtractTarget(ev->Sender, target)) {
            OnUnsure(target);
            CompleteIfDone();
        }
    }

    bool TActorLivenessChecker::ExtractTarget(
            const TActorId& actorId,
            TActorLivenessCheckTarget& target) {
        const auto it = PendingTargets.find(actorId);
        if (it == PendingTargets.end()) {
            return false;
        }

        target = {
            .ActorId = it->first,
            .Cookie = it->second,
        };
        PendingTargets.erase(it);
        return true;
    }

    void TActorLivenessChecker::CompleteIfDone() {
        if (PendingTargets.empty()) {
            Complete();
        }
    }

    void TActorLivenessChecker::Complete() {
        OnFinish();
        PassAway();
    }

}
