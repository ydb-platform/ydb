#pragma once
#include "db_async_resolver.h"
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>

namespace NYq {

class TDatabaseAsyncResolverImpl : public IDatabaseAsyncResolver {
public:
    TDatabaseAsyncResolverImpl(
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& recipient,
        const TString& ydbMvpEndpoint,
        const TString& mdbGateway,
        bool mdbTransformHost = false,
        const TString& traceId = ""
    );

    NThreading::TFuture<TEvents::TDbResolverResponse> ResolveIds(
        const THashMap<std::pair<TString, DatabaseType>, TEvents::TDatabaseAuth>& ids) const override;
private:
    NActors::TActorSystem* ActorSystem;
    const NActors::TActorId Recipient;
    const TString YdbMvpEndpoint;
    const TString MdbGateway;
    const bool MdbTransformHost = false;
    const TString TraceId;
};

} // NYq
