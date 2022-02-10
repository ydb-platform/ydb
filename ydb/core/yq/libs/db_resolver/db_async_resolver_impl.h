#pragma once
#include "db_async_resolver.h"
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>

namespace NYq {

using TEndpoint = TEvents::TEvEndpointResponse::TEndpoint;

class TDatabaseAsyncResolver : public IDatabaseAsyncResolver {
public:
    TDatabaseAsyncResolver(
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& recipient,
        const TString& ydbMvpEndpoint,
        const TString& mdbGateway,
        const bool mdbTransformHost
    );

    NThreading::TFuture<THashMap<std::pair<TString, DatabaseType>, TEndpoint>> ResolveIds(const TResolveParams& params) const override;
private:
    NActors::TActorSystem* ActorSystem;
    const NActors::TActorId Recipient;
    const TString YdbMvpEndpoint;
    const TString MdbGateway;
    const bool MdbTransformHost = false;
};

} // NYq
