#pragma once

#include <ydb/library/actors/core/actor.h>

#include <util/generic/vector.h>

namespace NActors {

    IActor* CreateSubscriberLivenessChecker(
        const TActorId& subscriptionOwner,
        TVector<TActorId> subscribers);

}
