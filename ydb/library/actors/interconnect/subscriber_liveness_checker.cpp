#include "subscriber_liveness_checker.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <util/generic/map.h>
#include <util/string/builder.h>

#define YDB_LOG_THIS_FILE_COMPONENT NActorsServices::INTERCONNECT_SESSION

namespace NActors {
    namespace {

        TStringBuf FormatSubscriberActivityName(ui32 activityIndex) {
            return activityIndex == Max<ui32>() ? TStringBuf("manual") : GetActivityTypeName(activityIndex);
        }

        class TSubscriberLivenessChecker
            : public TActorLivenessChecker
        {
        public:
            TSubscriberLivenessChecker(
                    const TActorId& subscriptionOwner,
                    TVector<TActorLivenessCheckTarget> subscribers)
                : TActorLivenessChecker(std::move(subscribers))
                , SubscriptionOwner(subscriptionOwner)
            {
            }

        private:
            void OnDead(const TActorLivenessCheckTarget& target) override {
                ++LeakedSubscribersByActivity[static_cast<ui32>(target.Cookie)];
                // The IC session identifies the subscription by event sender.
                TActivationContext::Send(new IEventHandle(
                    SubscriptionOwner,
                    target.ActorId,
                    new TEvents::TEvUnsubscribe));
            }

            void OnFinish() override {
                LogLeakedSubscribers();
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
                YDB_LOG_WARN("Subscriber liveness check found leaked",
                    {"subscriptions", details});
            }

        private:
            const TActorId SubscriptionOwner;
            TMap<ui32, ui64> LeakedSubscribersByActivity;
        };

    }

    IActor* CreateSubscriberLivenessChecker(
            const TActorId& subscriptionOwner,
            TVector<TActorLivenessCheckTarget> subscribers) {
        return new TSubscriberLivenessChecker(subscriptionOwner, std::move(subscribers));
    }

}
