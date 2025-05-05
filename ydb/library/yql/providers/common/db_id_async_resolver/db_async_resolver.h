#pragma once

#include "database_type.h"

#include <library/cpp/threading/future/future.h>
#include <util/string/builder.h>
#include <util/string/cast.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/public/issue/yql_issue.h>

namespace NYql {

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
