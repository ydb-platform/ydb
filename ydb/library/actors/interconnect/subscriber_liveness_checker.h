#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/vector.h>

#include <utility>

namespace NActors {

    IActor* CreateSubscriberLivenessChecker(
        const TActorId& subscriptionOwner,
        TVector<TActorId> subscribers);

    template <typename TSubscribers>
    void RegisterSubscriberLivenessChecker(
            const TActorId& subscriptionOwner,
            const TSubscribers& subscribers) {
        if (subscribers.empty()) {
            return;
        }

        TVector<TActorId> subscriberIds;
        subscriberIds.reserve(subscribers.size());
        for (const auto& item : subscribers) {
            subscriberIds.push_back(item.first);
        }
        TActivationContext::Register(
            CreateSubscriberLivenessChecker(subscriptionOwner, std::move(subscriberIds)),
            subscriptionOwner);
    }

}
