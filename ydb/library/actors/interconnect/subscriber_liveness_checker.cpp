#include "subscriber_liveness_checker.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/protos/services_common.pb.h>

#include <util/generic/map.h>
#include <util/string/builder.h>

namespace NActors {
    namespace {

        TStringBuf FormatSubscriberActivityName(ui32 activityIndex) {
            return activityIndex == Max<ui32>() ? TStringBuf("manual") : GetActivityTypeName(activityIndex);
        }

        void LogLeakedSubscribers(const TVector<TActorLivenessCheckTarget>& deadSubscribers) {
            if (deadSubscribers.empty()) {
                return;
            }

            TMap<ui32, ui64> leakedSubscribersByActivity;
            for (const auto& subscriber : deadSubscribers) {
                ++leakedSubscribersByActivity[static_cast<ui32>(subscriber.Cookie)];
            }

            TStringBuilder details;
            bool first = true;
            for (const auto& [activityIndex, count] : leakedSubscribersByActivity) {
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

    }

    void RegisterSubscriberLivenessChecker(
            const TActorId& subscriptionOwner,
            TVector<TActorLivenessCheckTarget> subscribers) {
        if (subscribers.empty()) {
            return;
        }

        TActorLivenessCheckerCallbacks callbacks{
            .OnActorDead = [subscriptionOwner](const TActorLivenessCheckTarget& subscriber) {
                // The IC session identifies the subscription by event sender.
                TActivationContext::Send(new IEventHandle(
                    subscriptionOwner,
                    subscriber.ActorId,
                    new TEvents::TEvUnsubscribe));
            },
            .OnComplete = LogLeakedSubscribers,
        };
        TActivationContext::Register(
            CreateActorLivenessChecker(std::move(subscribers), std::move(callbacks)),
            subscriptionOwner);
    }

}
