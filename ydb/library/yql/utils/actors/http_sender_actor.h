#pragma once

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/retry/retry_policy.h>

namespace NYql::NDq {
    using THttpSenderRetryPolicy = IRetryPolicy<const NHttp::TEvHttpProxy::TEvHttpIncomingResponse*>;

    NActors::IActor* CreateHttpSenderActor(
        NActors::TActorId senderId,
        NActors::TActorId httpProxyId,
        const THttpSenderRetryPolicy::TPtr& retryPolicy);
} // NYql::NDq
