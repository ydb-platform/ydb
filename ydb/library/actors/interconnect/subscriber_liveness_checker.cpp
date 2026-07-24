#include "subscriber_liveness_checker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

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
                    TVector<TActorLivenessCheckTarget> subscribers)
                : SubscriptionOwner(subscriptionOwner)
                , Subscribers(std::move(subscribers))
            {
            }

            void Bootstrap() {
                if (Subscribers.empty()) {
                    return PassAway();
                }

                Become(&TThis::StateFunc);
                Checker = Register(CreateActorLivenessChecker(std::move(Subscribers), SelfId()));
            }

        private:
            STRICT_STFUNC(StateFunc,
                hFunc(TEvents::TEvActorDead, Handle)
                hFunc(TEvents::TEvGone, Handle)
            )

            void Handle(TEvents::TEvActorDead::TPtr& ev) {
                ++LeakedSubscribersByActivity[static_cast<ui32>(ev->Cookie)];
                // The IC session identifies the subscription by event sender.
                TActivationContext::Send(new IEventHandle(
                    SubscriptionOwner,
                    ev->Sender,
                    new TEvents::TEvUnsubscribe));
            }

            void Handle(TEvents::TEvGone::TPtr& ev) {
                if (ev->Sender != Checker) {
                    return;
                }
                LogLeakedSubscribers();
                PassAway();
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
            TVector<TActorLivenessCheckTarget> Subscribers;
            TActorId Checker;
            TMap<ui32, ui64> LeakedSubscribersByActivity;
        };

    }

    IActor* CreateSubscriberLivenessChecker(
            const TActorId& subscriptionOwner,
            TVector<TActorLivenessCheckTarget> subscribers) {
        return new TSubscriberLivenessChecker(subscriptionOwner, std::move(subscribers));
    }

}
