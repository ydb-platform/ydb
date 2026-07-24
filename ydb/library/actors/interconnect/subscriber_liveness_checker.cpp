#include "subscriber_liveness_checker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/hash_set.h>

namespace NActors {
    namespace {

        class TSubscriberLivenessChecker
            : public TActorBootstrapped<TSubscriberLivenessChecker>
        {
        public:
            TSubscriberLivenessChecker(
                    const TActorId& subscriptionOwner,
                    TVector<TActorId> subscribers)
                : SubscriptionOwner(subscriptionOwner)
                , PendingSubscribers(subscribers.begin(), subscribers.end())
            {}

            void Bootstrap() {
                if (PendingSubscribers.empty()) {
                    return PassAway();
                }

                Become(&TThis::StateFunc);
                // Every liveness probe is guaranteed to eventually produce exactly one response:
                // ActorAlive or ActorDead for a local target, and ActorLivenessUnsure for a remote one.
                // The checker can therefore wait for all responses without a timeout.
                for (const TActorId& subscriber : PendingSubscribers) {
                    TActivationContext::Send(new IEventHandle(
                        TEvents::TSystem::CheckActorLiveness,
                        TEvents::TEvCheckActorLiveness::RequestFlags,
                        subscriber,
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
                Complete(ev->Sender);
            }

            void Handle(TEvents::TEvActorDead::TPtr& ev) {
                if (PendingSubscribers.erase(ev->Sender)) {
                    // The IC session identifies the subscription by event sender.
                    TActivationContext::Send(new IEventHandle(
                        SubscriptionOwner,
                        ev->Sender,
                        new TEvents::TEvUnsubscribe));
                    PassAwayIfDone();
                }
            }

            void Handle(TEvents::TEvActorLivenessUnsure::TPtr& ev) {
                Complete(ev->Sender);
            }

            void Complete(const TActorId& subscriber) {
                if (PendingSubscribers.erase(subscriber)) {
                    PassAwayIfDone();
                }
            }

            void PassAwayIfDone() {
                if (PendingSubscribers.empty()) {
                    PassAway();
                }
            }

        private:
            const TActorId SubscriptionOwner;
            THashSet<TActorId> PendingSubscribers;
        };

    }

    IActor* CreateSubscriberLivenessChecker(
            const TActorId& subscriptionOwner,
            TVector<TActorId> subscribers) {
        return new TSubscriberLivenessChecker(subscriptionOwner, std::move(subscribers));
    }

}
