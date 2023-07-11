#pragma once

#include <ydb/core/fq/libs/events/events.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/mdb_host_transformer.h>
#include <ydb/library/yql/providers/dq/actors/actor_helpers.h>

namespace NFq {

class TDatabaseAsyncResolverImpl : public NYql::IDatabaseAsyncResolver {
public:
    TDatabaseAsyncResolverImpl(
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& recipient,
        const TString& ydbMvpEndpoint,
        const TString& mdbGateway,
        NYql::IMdbHostTransformer::TPtr&& mdbHostTransformer, 
        const TString& traceId = ""
    );

    NThreading::TFuture<NYql::TDatabaseResolverResponse> ResolveIds(const TDatabaseAuthMap& ids) const override;
private:
    NActors::TActorSystem* ActorSystem;
    const NActors::TActorId Recipient;
    const TString YdbMvpEndpoint;
    const TString MdbGateway;
    NYql::IMdbHostTransformer::TPtr MdbHostTransformer;
    const TString TraceId;
};

} // NFq
