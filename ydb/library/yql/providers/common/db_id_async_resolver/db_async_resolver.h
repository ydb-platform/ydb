#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

namespace NYql {

enum class EDatabaseType {
    Ydb,
    ClickHouse,
    DataStreams,
    ObjectStorage,
    PostgreSQL
};

inline EDatabaseType DatabaseTypeFromDataSourceKind(NConnector::NApi::EDataSourceKind dataSourceKind) {
    switch (dataSourceKind) {
        case NConnector::NApi::EDataSourceKind::POSTGRESQL:
            return EDatabaseType::PostgreSQL;
        case NConnector::NApi::EDataSourceKind::CLICKHOUSE:
            return EDatabaseType::ClickHouse;
        default:
            ythrow yexception() << "Unknown data source kind: " << NConnector::NApi::EDataSourceKind_Name(dataSourceKind);
    }
}

inline NConnector::NApi::EDataSourceKind DatabaseTypeToDataSourceKind(EDatabaseType databaseType) {
    switch (databaseType) {
        case EDatabaseType::PostgreSQL:
            return  NConnector::NApi::EDataSourceKind::POSTGRESQL;
        case EDatabaseType::ClickHouse:
            return  NConnector::NApi::EDataSourceKind::CLICKHOUSE;
        default:
            ythrow yexception() << "Unknown database type: " << ToString(databaseType);
    }
}

inline TString DatabaseTypeToMdbUrlPath(EDatabaseType databaseType) {
    auto dump = ToString(databaseType);
    dump.to_lower();

    switch (databaseType) {
        case EDatabaseType::ClickHouse:
            return dump;
        case EDatabaseType::PostgreSQL:
            return dump;
        default:
            ythrow yexception() << "Unsupported database type: " << ToString(databaseType);
    }
}

struct TDatabaseAuth {
    // Serialized token value used to access MDB API
    TString StructuredToken; 
    bool AddBearerToToken = false; 

    // This flag describes one's intention to connect managed database using secure or insecure sockets,
    // but it will work only for certain kinds of databases.
    // For more details look through the parser implementations here (not all of them rely on this flag): 
    // https://a.yandex-team.ru/arcadia/ydb/core/fq/libs/actors/database_resolver.cpp?rev=r12426855#L229
    bool UseTls = false; 

    bool operator==(const TDatabaseAuth& other) const {
        return std::tie(StructuredToken, AddBearerToToken, UseTls) == std::tie(other.StructuredToken, other.AddBearerToToken, other.UseTls);
    }

    bool operator!=(const TDatabaseAuth& other) const {
        return !(*this == other);
    }
};

struct TDatabaseResolverResponse {

    struct TDatabaseDescription {
        TString Endpoint;
        TString Host;
        ui32 Port = 0;
        TString Database;
        bool Secure = false;
    };

    // key - (database id, database type), value - resolved database params
    using TDatabaseDescriptionMap = THashMap<std::pair<TString, EDatabaseType>, TDatabaseDescription>;

    TDatabaseResolverResponse() = default;

    TDatabaseResolverResponse(
        TDatabaseDescriptionMap&& databaseDescriptionMap,
        bool success = false,
        const NYql::TIssues& issues = {})
        : DatabaseDescriptionMap(std::move(databaseDescriptionMap))
        , Success(success)
        , Issues(issues) {}

    TDatabaseDescriptionMap DatabaseDescriptionMap;
    bool Success = false;
    NYql::TIssues Issues;
};

class IDatabaseAsyncResolver {
public:
    using TPtr = std::shared_ptr<IDatabaseAsyncResolver>;

    using TDatabaseAuthMap = THashMap<std::pair<TString, EDatabaseType>, NYql::TDatabaseAuth>;

    virtual NThreading::TFuture<NYql::TDatabaseResolverResponse> ResolveIds(const TDatabaseAuthMap& ids) const = 0;

    virtual ~IDatabaseAsyncResolver() = default;
};

} // NYql
