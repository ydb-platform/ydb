#include "db_async_resolver_with_meta.h"
#include <ydb/core/yq/libs/common/database_token_builder.h>

namespace NYq {

    TDatabaseAsyncResolverWithMeta::TDatabaseAsyncResolverWithMeta(
        NActors::TActorSystem* actorSystem,
        const NActors::TActorId& recipient,
        const TString& ydbMvpEndpoint,
        const TString& mdbGateway,
        const bool mdbTransformHost,
        const TString& traceId,
        const TString& token,
        const THashMap<TString, TString>& accountIdSignatures,
        const THashMap<TString, YandexQuery::Connection>& connections)
        : DbResolver(actorSystem, recipient, ydbMvpEndpoint, mdbGateway, mdbTransformHost)
        , TraceId(traceId)
        , Token(token)
        , AccountIdSignatures(accountIdSignatures)
        , Connections(connections)
    {}

    NThreading::TFuture<TEvents::TDbResolverResponse> TDatabaseAsyncResolverWithMeta::ResolveIds(const TResolveParams& params) const {
        return DbResolver.ResolveIds(params);
    }

    TString TDatabaseAsyncResolverWithMeta::GetTraceId() const {
        return TraceId;
    }

    TString TDatabaseAsyncResolverWithMeta::GetToken() const {
        return Token;
    }

    const THashMap<TString, TString>& TDatabaseAsyncResolverWithMeta::GetAccountIdSignatures() const {
        return AccountIdSignatures;
    }

    void TDatabaseAsyncResolverWithMeta::TryAddDbIdToResolve(
        const bool isEndpoint,
        const TString& clusterName,
        const TString& dbId,
        const DatabaseType type,
        THashMap<std::pair<TString, DatabaseType>, TEvents::TDatabaseAuth>& databaseIds) const {
            if (isEndpoint) {
                return;
            }
            const auto iter = Connections.find(clusterName);
            if (iter == Connections.end()) {
                return;
            }
            const auto& conn = iter->second;
            const auto& setting = conn.content().setting();
            YandexQuery::IamAuth auth;
            switch (type) {
            case DatabaseType::Ydb:
                auth = setting.ydb_database().auth();
                break;
            case DatabaseType::ClickHouse:
                auth = setting.clickhouse_cluster().auth();
                break;
            case DatabaseType::DataStreams:
                auth = setting.data_streams().auth();
                break;
            default:
                return;
            }
            TryAddDatabaseToResolve(auth, dbId, type, Token, AccountIdSignatures, databaseIds);
        }

} // NYq

