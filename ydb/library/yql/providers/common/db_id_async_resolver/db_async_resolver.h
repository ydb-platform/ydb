#pragma once
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <library/cpp/threading/future/future.h>

namespace NYql {

enum class DatabaseType {
    Ydb,
    ClickHouse,
    DataStreams,
    ObjectStorage,
    Generic
};


struct TDatabaseAuth {
    TString StructuredToken;
    bool AddBearerToToken = false;

    bool operator==(const TDatabaseAuth& other) const {
        return std::tie(StructuredToken, AddBearerToToken) == std::tie(other.StructuredToken, other.AddBearerToToken);
    }
    bool operator!=(const TDatabaseAuth& other) const {
        return !(*this == other);
    }
};

struct TDbResolverResponse {
    struct TEndpoint {
        TString Endpoint;
        TString Database;
        bool Secure = false;
    };

    TDbResolverResponse() = default;

    TDbResolverResponse(
        THashMap<std::pair<TString, DatabaseType>, TEndpoint>&& databaseId2Endpoint,
        bool success = false,
        const NYql::TIssues& issues = {})
        : DatabaseId2Endpoint(std::move(databaseId2Endpoint))
        , Success(success)
        , Issues(issues) {}

    THashMap<std::pair<TString, DatabaseType>, TEndpoint> DatabaseId2Endpoint;
    bool Success = false;
    NYql::TIssues Issues;
};

class IDatabaseAsyncResolver {
public:
    using DatabaseIds = THashMap<std::pair<TString, DatabaseType>, NYql::TDatabaseAuth>;

    virtual NThreading::TFuture<NYql::TDbResolverResponse> ResolveIds(const DatabaseIds& ids) const = 0;

    virtual ~IDatabaseAsyncResolver() = default;
};

} // NYql
