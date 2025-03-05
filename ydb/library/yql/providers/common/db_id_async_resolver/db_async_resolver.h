#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NYql {

enum class EDatabaseType {
    Ydb,
    ClickHouse,
    DataStreams,
    ObjectStorage,
    PostgreSQL,
    YT,
    MySQL,
    Greenplum,
    MsSQLServer,
    Oracle,
    Logging,
    Solomon
};

inline EDatabaseType DatabaseTypeFromDataSourceKind(NYql::EGenericDataSourceKind dataSourceKind) {
    switch (dataSourceKind) {
        case NYql::EGenericDataSourceKind::POSTGRESQL:
            return EDatabaseType::PostgreSQL;
        case NYql::EGenericDataSourceKind::CLICKHOUSE:
            return EDatabaseType::ClickHouse;
        case NYql::EGenericDataSourceKind::YDB:
            return EDatabaseType::Ydb;
        case NYql::EGenericDataSourceKind::MYSQL:
            return EDatabaseType::MySQL;
        case NYql::EGenericDataSourceKind::GREENPLUM:
            return EDatabaseType::Greenplum;
        case NYql::EGenericDataSourceKind::MS_SQL_SERVER:
          return EDatabaseType::MsSQLServer;
        case NYql::EGenericDataSourceKind::ORACLE:
          return EDatabaseType::Oracle;
        case NYql::EGenericDataSourceKind::LOGGING:
          return EDatabaseType::Logging;
        default:
            ythrow yexception() << "Unknown data source kind: " << NYql::EGenericDataSourceKind_Name(dataSourceKind);
    }
}

inline NYql::EGenericDataSourceKind DatabaseTypeToDataSourceKind(EDatabaseType databaseType) {
    switch (databaseType) {
        case EDatabaseType::PostgreSQL:
            return  NYql::EGenericDataSourceKind::POSTGRESQL;
        case EDatabaseType::ClickHouse:
            return  NYql::EGenericDataSourceKind::CLICKHOUSE;
        case EDatabaseType::Ydb:
            return  NYql::EGenericDataSourceKind::YDB;
        case EDatabaseType::MySQL:
            return NYql::EGenericDataSourceKind::MYSQL;
        case EDatabaseType::Greenplum:
            return  NYql::EGenericDataSourceKind::GREENPLUM;
        case EDatabaseType::MsSQLServer:
            return NYql::EGenericDataSourceKind::MS_SQL_SERVER;
        case EDatabaseType::Oracle:
            return NYql::EGenericDataSourceKind::ORACLE;
        case EDatabaseType::Logging:
            return NYql::EGenericDataSourceKind::LOGGING;
        default:
            ythrow yexception() << "Unknown database type: " << ToString(databaseType);
    }
}

inline TString DatabaseTypeLowercase(EDatabaseType databaseType) {
    auto dump = ToString(databaseType);
    dump.to_lower();
    return dump;
}

// TODO: remove this function after /kikimr/yq/tests/control_plane_storage is moved to /ydb.
inline TString DatabaseTypeToMdbUrlPath(EDatabaseType databaseType) {
    return DatabaseTypeLowercase(databaseType);
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

    // For some of the data sources accessible via generic provider it's possible to specify the connection protocol.
    // This setting may impact the throughput.
    NYql::EGenericProtocol Protocol = NYql::EGenericProtocol::PROTOCOL_UNSPECIFIED;

    bool operator==(const TDatabaseAuth& other) const {
        return std::tie(StructuredToken, AddBearerToToken, UseTls, Protocol) == std::tie(other.StructuredToken, other.AddBearerToToken, other.UseTls, Protocol);
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

        TString ToDebugString() const {
            return TStringBuilder() << "endpoint=" << Endpoint
                                    << ", host=" << Host
                                    << ", port=" << Port
                                    << ", database=" << Database
                                    << ", secure=" << ToString(Secure);
        };
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
