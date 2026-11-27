#pragma once

#include "meta_settings.h"

#include <ydb/mvp/core/core_ydb.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/event_local.h>

#include <util/generic/hash.h>

#include <memory>
#include <optional>

namespace NMVP {

struct TDatabaseHealth {
    TString Status;
    TInstant CheckedAt;
};

using TDatabaseHealthMap = THashMap<TString, TDatabaseHealth>;

TString HealthcheckOverall(TStringBuf status);
bool ParseClusterHealth(const NJson::TJsonValue& json, TInstant checkedAt,
                        TString& clusterStatus, TDatabaseHealthMap& databases);
TDatabaseHealthMap ParseDatabaseHealth(const NYdb::TResultSet& result);
void ApplyDatabaseHealth(NJson::TJsonValue& database, const TDatabaseHealthMap& statuses, TInstant now, TDuration ttl);

// Private to Meta's healthcheck actors. The query executor is also used by HTTP
// handlers so that table errors/timeouts can fall back to the viewer response.
struct TEvHealthQuery : NActors::TEventLocal<TEvHealthQuery, EventSpaceBegin(NActors::TEvents::ES_PRIVATE) + 700> {
    TString Query;
    NYdb::TParams Params;
    NYdb::NTable::TClientSettings ClientSettings;
    TDuration Timeout;

    TEvHealthQuery(TString query, NYdb::TParams params, NYdb::NTable::TClientSettings clientSettings,
                  TDuration timeout = TDuration::Seconds(10))
        : Query(std::move(query))
        , Params(std::move(params))
        , ClientSettings(std::move(clientSettings))
        , Timeout(timeout)
    {}
};

struct TEvHealthQueryResult : NActors::TEventLocal<TEvHealthQueryResult, TEvHealthQuery::EventType + 1> {
    std::optional<NYdb::NTable::TDataQueryResult> Result;

    TEvHealthQueryResult() = default;
    explicit TEvHealthQueryResult(NYdb::NTable::TDataQueryResult result)
        : Result(std::move(result))
    {}

    bool IsSuccess() const {
        return Result && Result->IsSuccess();
    }
};

NActors::IActor* CreateHealthQueryService(const TYdbLocation& location);

// Local-only access to the immutable snapshot held by the collector. HTTP
// callers must pass whoami authorization before sending this event.
struct TEvGetCachedHealth : NActors::TEventLocal<TEvGetCachedHealth, TEvHealthQuery::EventType + 2> {
    TString Cluster;
    TString Owner;
    TEvGetCachedHealth(TString cluster, TString owner)
        : Cluster(std::move(cluster)), Owner(std::move(owner)) {}
};

struct TEvCachedHealth : NActors::TEventLocal<TEvCachedHealth, TEvHealthQuery::EventType + 3> {
    std::shared_ptr<const TString> Body;
    TInstant CheckedAt;
};

NActors::IActor* CreateHealthMonitor(const NActors::TActorId& httpProxy, const NActors::TActorId& queryService,
                                   TString rootDomain, TString tokenName, TMetaSettings settings, TString localEndpoint);
TEvHealthQuery* MakeLoadDatabaseHealthQuery(const TYdbLocation& location, const TRequest& request);
TEvHealthQuery* MakeLoadDatabaseHealthQuery(const TYdbLocation& location, const TRequest& request,
                                           TInstant now, TDuration statusTtl);

} // namespace NMVP
