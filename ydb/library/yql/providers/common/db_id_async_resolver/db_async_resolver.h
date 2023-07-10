#pragma once

#include <library/cpp/threading/future/future.h>
#include <util/string/builder.h>
#include <ydb/library/yql/public/issue/yql_issue.h>

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

    using TDatabaseEndpointsMap = THashMap<std::pair<TString, DatabaseType>, TEndpoint>;

    TDbResolverResponse() = default;

    TDbResolverResponse(
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
    using TDatabaseAuthMap = THashMap<std::pair<TString, DatabaseType>, NYql::TDatabaseAuth>;

    virtual NThreading::TFuture<NYql::TDbResolverResponse> ResolveIds(const TDatabaseAuthMap& ids) const = 0;

    virtual ~IDatabaseAsyncResolver() = default;
};

} // NYql
