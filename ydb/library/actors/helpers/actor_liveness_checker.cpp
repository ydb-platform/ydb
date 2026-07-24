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
                    const TActorId& notify)
                : Notify(notify)
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
                        cookie));
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
                if (PendingTargets.erase(ev->Sender)) {
                    TActivationContext::Send(ev->Forward(Notify));
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
                Send(Notify, new TEvents::TEvGone);
                PassAway();
            }

        private:
            const TActorId Notify;
            THashMap<TActorId, ui64> PendingTargets;
        };

    }

    IActor* CreateActorLivenessChecker(
            TVector<TActorLivenessCheckTarget> targets,
            const TActorId& notify) {
        return new TActorLivenessChecker(std::move(targets), notify);
    }

}
