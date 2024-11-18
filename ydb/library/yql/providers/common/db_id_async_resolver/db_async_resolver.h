#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

// FIXME: simplify this after YQ-3839 is completed
#if __has_include(<yql/essentials/providers/common/proto/connector/common/data_source.pb.h>)
    #error #include <yql/essentials/providers/common/proto/connector/common/data_source.pb.h>

    namespace NConnectorCommon = NYql::NConnector::NCommon;
#else
    #include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h>

    namespace NConnectorCommon = NYql::NConnector::NApi;
#endif

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
    Oracle
};

inline EDatabaseType DatabaseTypeFromDataSourceKind(NConnectorCommon::EDataSourceKind dataSourceKind) {
    switch (dataSourceKind) {
        case NConnectorCommon::EDataSourceKind::POSTGRESQL:
            return EDatabaseType::PostgreSQL;
        case NConnectorCommon::EDataSourceKind::CLICKHOUSE:
            return EDatabaseType::ClickHouse;
        case NConnectorCommon::EDataSourceKind::YDB:
            return EDatabaseType::Ydb;
        case NConnectorCommon::EDataSourceKind::MYSQL:
            return EDatabaseType::MySQL;
        case NConnectorCommon::EDataSourceKind::GREENPLUM:
            return EDatabaseType::Greenplum;
        case NConnectorCommon::EDataSourceKind::MS_SQL_SERVER:
          return EDatabaseType::MsSQLServer;
        case NConnectorCommon::EDataSourceKind::ORACLE:
          return EDatabaseType::Oracle;
        default:
            ythrow yexception() << "Unknown data source kind: " << NConnectorCommon::EDataSourceKind_Name(dataSourceKind);
    }
}

inline NConnectorCommon::EDataSourceKind DatabaseTypeToDataSourceKind(EDatabaseType databaseType) {
    switch (databaseType) {
        case EDatabaseType::PostgreSQL:
            return  NConnectorCommon::EDataSourceKind::POSTGRESQL;
        case EDatabaseType::ClickHouse:
            return  NConnectorCommon::EDataSourceKind::CLICKHOUSE;
        case EDatabaseType::Ydb:
            return  NConnectorCommon::EDataSourceKind::YDB;
        case EDatabaseType::MySQL:
            return NConnectorCommon::EDataSourceKind::MYSQL;
        case EDatabaseType::Greenplum:
            return  NConnectorCommon::EDataSourceKind::GREENPLUM;
        case EDatabaseType::MsSQLServer:
            return NConnectorCommon::EDataSourceKind::MS_SQL_SERVER;
        case EDatabaseType::Oracle:
            return NConnectorCommon::EDataSourceKind::ORACLE;
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
    NConnectorCommon::EProtocol Protocol = NConnectorCommon::EProtocol::PROTOCOL_UNSPECIFIED;

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
