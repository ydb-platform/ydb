#include "subscriber_liveness_checker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NActors {
    namespace {

        TStringBuf FormatSubscriberActivityName(ui32 activityIndex) {
            return activityIndex == Max<ui32>() ? TStringBuf("manual") : GetActivityTypeName(activityIndex);
        }

        class TSubscriberLivenessChecker
            : public TActorBootstrapped<TSubscriberLivenessChecker>
        {
        public:
            TSubscriberLivenessChecker(
                    const TActorId& subscriptionOwner,
                    TVector<TSubscriberLivenessInfo> subscribers)
                : SubscriptionOwner(subscriptionOwner)
            {
                for (const auto& subscriber : subscribers) {
                    PendingSubscribers.emplace(subscriber.ActorId, subscriber.ActivityIndex);
                }
            }

            void Bootstrap() {
                if (PendingSubscribers.empty()) {
                    return PassAway();
                }

                Become(&TThis::StateFunc);
                // Every liveness probe is guaranteed to eventually produce exactly one response:
                // ActorAlive or ActorDead for a local target, and ActorLivenessUnsure for a remote one.
                // The checker can therefore wait for all responses without a timeout.
                for (const auto& [subscriber, activityIndex] : PendingSubscribers) {
                    Y_UNUSED(activityIndex);
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
                if (const auto it = PendingSubscribers.find(ev->Sender); it != PendingSubscribers.end()) {
                    ++LeakedSubscribersByActivity[it->second];
                    PendingSubscribers.erase(it);
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
                    LogLeakedSubscribers();
                    PassAway();
                }
            }

            void LogLeakedSubscribers() const {
                if (LeakedSubscribersByActivity.empty()) {
                    return;
                }

                TStringBuilder details;
                bool first = true;
                for (const auto& [activityIndex, count] : LeakedSubscribersByActivity) {
                    if (!first) {
                        details << ", ";
                    }
                    first = false;
                    details << "{activity# " << FormatSubscriberActivityName(activityIndex)
                        << " actors# " << count << '}';
                }
                LOG_WARN_S(*TlsActivationContext, NActorsServices::INTERCONNECT_SESSION,
                    "Subscriber liveness check found leaked subscriptions: " << details);
            }

        private:
            const TActorId SubscriptionOwner;
            THashMap<TActorId, ui32> PendingSubscribers;
            TMap<ui32, ui64> LeakedSubscribersByActivity;
        };

    }

    IActor* CreateSubscriberLivenessChecker(
            const TActorId& subscriptionOwner,
            TVector<TSubscriberLivenessInfo> subscribers) {
        return new TSubscriberLivenessChecker(subscriptionOwner, std::move(subscribers));
    }

}
