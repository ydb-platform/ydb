#include "actor_liveness_checker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash.h>

#include <utility>

namespace NActors {
    namespace {

        class TActorLivenessChecker
            : public TActorBootstrapped<TActorLivenessChecker>
        {
        public:
            TActorLivenessChecker(
                    TVector<TActorLivenessCheckTarget> targets,
                    TActorLivenessCheckerCallbacks callbacks)
                : Callbacks(std::move(callbacks))
            {
                for (const auto& target : targets) {
                    PendingTargets.emplace(target.ActorId, target.Cookie);
                }
            }

            void Bootstrap() {
                if (PendingTargets.empty()) {
                    return Complete();
                }

                Become(&TThis::StateFunc);
                // Every local liveness probe eventually produces ActorAlive or ActorDead.
                // Remote probes currently produce ActorLivenessUnsure.
                for (const auto& [actorId, cookie] : PendingTargets) {
                    Y_UNUSED(cookie);
                    TActivationContext::Send(new IEventHandle(
                        TEvents::TSystem::CheckActorLiveness,
                        TEvents::TEvCheckActorLiveness::RequestFlags,
                        actorId,
                        SelfId(),
                        nullptr,
                        0));
                }
            }

        private:
            STRICT_STFUNC(StateFunc,
                hFunc(TEvents::TEvActorAlive, Handle)
                hFunc(TEvents::TEvActorDead, Handle)
                hFunc(TEvents::TEvActorLivenessUnsure, Handle)
            )

            void Handle(TEvents::TEvActorAlive::TPtr& ev) {
                CompleteTarget(ev->Sender);
            }

            void Handle(TEvents::TEvActorDead::TPtr& ev) {
                if (const auto it = PendingTargets.find(ev->Sender); it != PendingTargets.end()) {
                    DeadTargets.push_back({
                        .ActorId = it->first,
                        .Cookie = it->second,
                    });
                    PendingTargets.erase(it);
                    if (Callbacks.OnActorDead) {
                        Callbacks.OnActorDead(DeadTargets.back());
                    }
                    CompleteIfDone();
                }
            }

            void Handle(TEvents::TEvActorLivenessUnsure::TPtr& ev) {
                CompleteTarget(ev->Sender);
            }

            void CompleteTarget(const TActorId& actorId) {
                if (PendingTargets.erase(actorId)) {
                    CompleteIfDone();
                }
            }

            void CompleteIfDone() {
                if (PendingTargets.empty()) {
                    Complete();
                }
            }

            void Complete() {
                if (Callbacks.OnComplete) {
                    Callbacks.OnComplete(DeadTargets);
                }
                PassAway();
            }

        private:
            const TActorLivenessCheckerCallbacks Callbacks;
            THashMap<TActorId, ui64> PendingTargets;
            TVector<TActorLivenessCheckTarget> DeadTargets;
        };

    }

    IActor* CreateActorLivenessChecker(
            TVector<TActorLivenessCheckTarget> targets,
            TActorLivenessCheckerCallbacks callbacks) {
        return new TActorLivenessChecker(std::move(targets), std::move(callbacks));
    }

}
