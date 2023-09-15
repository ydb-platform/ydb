#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/string/builder.h>
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

inline EDatabaseType DataSourceKindToDatabaseType(NConnector::NApi::EDataSourceKind dataSourceKind) {
    switch (dataSourceKind) {
        case NConnector::NApi::EDataSourceKind::POSTGRESQL:
            return EDatabaseType::PostgreSQL;
        case NConnector::NApi::EDataSourceKind::CLICKHOUSE:
            return EDatabaseType::ClickHouse;
        default:
            ythrow yexception() << TStringBuf() << "Unknown data source kind: " << NConnector::NApi::EDataSourceKind_Name(dataSourceKind);
    }
}

inline TString DatabaseTypeToString(EDatabaseType databaseType) {
    switch (databaseType) {
        case EDatabaseType::ClickHouse:
            return "clickhouse";
        case EDatabaseType::PostgreSQL:
            return "postgresql";
        default:
            ythrow yexception() << TStringBuf() << "Unknown database type: " << int(databaseType);
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

    struct TEndpoint {
        std::tuple<TString, ui32> ParseHostPort() const {
            size_t pos = Endpoint.find(':');
            if (pos == TString::npos) {
                ythrow yexception() << TStringBuilder() << "Endpoint '" << Endpoint << "' contains no ':' separator";
            }

            auto host = Endpoint.substr(0, pos);
            auto port = static_cast<ui32>(std::stoi(Endpoint.substr(pos + 1)));
        
            return std::make_tuple(std::move(host), port);
        }

        TString Endpoint;
        TString Database;
        bool Secure = false;
    };

    using TDatabaseEndpointsMap = THashMap<std::pair<TString, EDatabaseType>, TEndpoint>;

    TDatabaseResolverResponse() = default;

    TDatabaseResolverResponse(
        TDatabaseEndpointsMap&& databaseId2Endpoint,
        bool success = false,
        const NYql::TIssues& issues = {})
        : DatabaseId2Endpoint(std::move(databaseId2Endpoint))
        , Success(success)
        , Issues(issues) {}

    TDatabaseEndpointsMap DatabaseId2Endpoint;
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
