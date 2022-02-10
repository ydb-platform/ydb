#pragma once
#include "db_async_resolver_impl.h"

namespace NYq {

class TDatabaseAsyncResolverWithMeta : public IDatabaseAsyncResolver {
public:
    TDatabaseAsyncResolverWithMeta(
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& recipient,
        const TString& ydbMvpEndpoint,
        const TString& mdbGateway,
        const bool mdbTransformHost,
        const TString& traceId,
        const TString& token,
        const THashMap<TString, TString>& accountIdSignatures,
        const THashMap<TString, YandexQuery::Connection>& connections
    );

    NThreading::TFuture<TEvents::TDbResolverResponse> ResolveIds(const TResolveParams& params) const override;

    TString GetTraceId() const;

    TString GetToken() const;

    const THashMap<TString, TString>& GetAccountIdSignatures() const;

    void TryAddDbIdToResolve(
        const bool isEndpoint,
        const TString& clusterName,
        const TString& dbId,
        const DatabaseType type,
        THashMap<std::pair<TString, DatabaseType>, TEvents::TDatabaseAuth>& databaseIds
    ) const;

private:
    TDatabaseAsyncResolver DbResolver;
    const TString TraceId;
    const TString Token;
    const THashMap<TString, TString> AccountIdSignatures;
    const THashMap<TString, YandexQuery::Connection> Connections;
};

} // NYq