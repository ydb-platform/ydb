#pragma once
#include <ydb/library/actors/core/actor.h>
#include "http.h"

namespace NHttp {

struct TCachePolicy {
    TDuration TimeToExpire;
    TDuration TimeToRefresh;
    TDuration PaceToRefresh;
    bool KeepOnError = false;
    bool DiscardCache = false;
    TArrayRef<TString> HeadersToCacheKey;
    TArrayRef<TString> StatusesToRetry;
    ui32 RetriesCount = 0;

    TCachePolicy() = default;
};

using TGetCachePolicy = std::function<TCachePolicy(const THttpRequest*)>;

NActors::IActor* CreateHttpCache(const NActors::TActorId& httpProxyId, TGetCachePolicy cachePolicy);
NActors::IActor* CreateOutgoingHttpCache(const NActors::TActorId& httpProxyId, TGetCachePolicy cachePolicy);
NActors::IActor* CreateIncomingHttpCache(const NActors::TActorId& httpProxyId, TGetCachePolicy cachePolicy);
TCachePolicy GetDefaultCachePolicy(const THttpRequest* request, const TCachePolicy& policy = TCachePolicy());

}
