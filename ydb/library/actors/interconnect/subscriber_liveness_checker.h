#pragma once

#include <ydb/library/actors/helpers/actor_liveness_checker.h>

#include <util/generic/vector.h>

#include <utility>

namespace NActors {

    IActor* CreateSubscriberLivenessChecker(
        const TActorId& subscriptionOwner,
        TVector<TActorLivenessCheckTarget> subscribers);

    template <typename TSubscribers>
    void RegisterSubscriberLivenessChecker(
            const TActorId& subscriptionOwner,
            const TSubscribers& subscribers) {
        if (subscribers.empty()) {
            return;
        }

        TVector<TActorLivenessCheckTarget> subscriberInfos;
        subscriberInfos.reserve(subscribers.size());
        for (const auto& item : subscribers) {
            subscriberInfos.push_back({
                .ActorId = item.first,
                .Cookie = item.second.ActivityIndex,
            });
        }
        TActivationContext::Register(
            CreateSubscriberLivenessChecker(subscriptionOwner, std::move(subscriberInfos)),
            subscriptionOwner);
    }

}
