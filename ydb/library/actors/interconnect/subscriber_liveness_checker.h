#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/vector.h>

#include <utility>

namespace NActors {

    struct TSubscriberLivenessInfo {
        TActorId ActorId;
        ui32 ActivityIndex = Max<ui32>();
    };

    IActor* CreateSubscriberLivenessChecker(
        const TActorId& subscriptionOwner,
        TVector<TSubscriberLivenessInfo> subscribers);

    template <typename TSubscribers>
    void RegisterSubscriberLivenessChecker(
            const TActorId& subscriptionOwner,
            const TSubscribers& subscribers) {
        if (subscribers.empty()) {
            return;
        }

        TVector<TSubscriberLivenessInfo> subscriberInfos;
        subscriberInfos.reserve(subscribers.size());
        for (const auto& item : subscribers) {
            subscriberInfos.push_back({
                .ActorId = item.first,
                .ActivityIndex = item.second.ActivityIndex,
            });
        }
        TActivationContext::Register(
            CreateSubscriberLivenessChecker(subscriptionOwner, std::move(subscriberInfos)),
            subscriptionOwner);
    }

}
