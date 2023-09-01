#pragma once

#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/mdb_endpoint_generator.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>

namespace NFq {

class TDatabaseAsyncResolverImpl : public NYql::IDatabaseAsyncResolver {
public:
    TDatabaseAsyncResolverImpl(
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& recipient,
        const TString& ydbMvpEndpoint,
        const TString& mdbGateway,
        const NYql::IMdbEndpointGenerator::TPtr& endpointGenerator, 
        const TString& traceId = ""
    );

    NThreading::TFuture<NYql::TDatabaseResolverResponse> ResolveIds(const TDatabaseAuthMap& ids) const override;
private:
    NActors::TActorSystem* ActorSystem;
    const NActors::TActorId Recipient;
    const TString YdbMvpEndpoint;
    const TString MdbGateway;
    const NYql::IMdbEndpointGenerator::TPtr MdbEndpointGenerator;
    const TString TraceId;
};

} // NFq
