#include "meta_health.h"
#include "mvp.h"

#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/guid.h>
#include <util/generic/map.h>

#include <algorithm>

namespace NMVP {

using NActors::TActivationContext;

TString HealthcheckOverall(TStringBuf status) {
    // New cluster snapshots use StatusFlag; retain support for legacy rows.
    if (status == "GREY") {
        return "Grey";
    }
    if (status == "GREEN") {
        return "Green";
    }
    if (status == "BLUE") {
        return "Blue";
    }
    if (status == "YELLOW") {
        return "Yellow";
    }
    if (status == "ORANGE") {
        return "Orange";
    }
    if (status == "RED") {
        return "Red";
    }
    if (status == "GOOD") {
        return "Green";
    }
    if (status == "DEGRADED") {
        return "Yellow";
    }
    if (status == "MAINTENANCE_REQUIRED" || status == "EMERGENCY") {
        return "Red";
    }
    return {};
}

bool ParseClusterHealth(const NJson::TJsonValue& json, TInstant checkedAt,
                        TString& clusterStatus, TDatabaseHealthMap& databases) {
    if (!json.IsMap() || !json["database_status"].IsArray()) {
        return false;
    }
    clusterStatus = json["self_check_result"].GetString();
    if (clusterStatus != "GOOD" && clusterStatus != "DEGRADED"
        && clusterStatus != "MAINTENANCE_REQUIRED" && clusterStatus != "EMERGENCY") {
        return false;
    }
    for (const auto& database : json["database_status"].GetArray()) {
        const TString name = database["name"].GetString();
        // HC can include an unnamed entry for unassigned storage pools.
        if (name.empty()) {
            continue;
        }
        const TString status = database["overall"].GetString();
        if (status != "GREY" && status != "GREEN" && status != "BLUE"
            && status != "YELLOW" && status != "ORANGE" && status != "RED") {
            return false;
        }
        if (!databases.emplace(name, TDatabaseHealth{status, checkedAt}).second) {
            return false;
        }
    }
    return true;
}

TDatabaseHealthMap ParseDatabaseHealth(const NYdb::TResultSet& result) {
    TDatabaseHealthMap statuses;
    NYdb::TResultSetParser parser(result);
    while (parser.TryNextRow()) {
        const auto status = parser.ColumnParser("Status").GetOptionalUtf8();
        const auto time = parser.ColumnParser("CheckedAt").GetOptionalTimestamp();
        if (status && time && !HealthcheckOverall(*status).empty()) {
            statuses[TString(parser.ColumnParser("Database").GetUtf8())] = {TString(*status), *time};
        }
    }
    return statuses;
}

void ApplyDatabaseHealth(NJson::TJsonValue& database, const TDatabaseHealthMap& statuses, TInstant now, TDuration ttl) {
    auto it = statuses.find(database["Name"].GetStringRobust());
    if (it == statuses.end() || it->second.CheckedAt > now || now - it->second.CheckedAt >= ttl) {
        return;
    }
    if (TString overall = HealthcheckOverall(it->second.Status)) {
        database["Overall"] = std::move(overall);
    }
}

TEvHealthQuery* MakeLoadDatabaseHealthQuery(const TYdbLocation& location, const TRequest& request) {
    return MakeLoadDatabaseHealthQuery(location, request, TInstant::Now(), InstanceMVP->MetaSettings.Healthcheck.StatusTtl);
}

TEvHealthQuery* MakeLoadDatabaseHealthQuery(const TYdbLocation& location, const TRequest& request,
                                           TInstant now, TDuration statusTtl) {
    NYdb::TParamsBuilder params;
    params.AddParam("$cluster").Utf8(request.Parameters["cluster_name"]).Build();
    params.AddParam("$cutoff").Timestamp(now - statusTtl).Build();
    return new TEvHealthQuery(
        TStringBuilder() << "DECLARE $cluster AS Utf8; DECLARE $cutoff AS Timestamp; "
            << "SELECT Database, Status, CheckedAt FROM `" << location.RootDomain
            << "/ydb/DatabaseHealth.db` WHERE Cluster=$cluster AND CheckedAt > $cutoff;",
        params.Build(), TMVP::GetStrictMetaDatabaseClientSettings(request, location), TDuration::Seconds(2));
}

namespace {

class THealthQueryActor : public NActors::TActorBootstrapped<THealthQueryActor>, public THandlerActorYdb {
    const TYdbLocation& Location;
    NActors::TActorId Owner;
    ui64 Cookie;
    THolder<TEvHealthQuery> Query;
    std::shared_ptr<NYdb::NTable::TTableClient> Client;

public:
    THealthQueryActor(const TYdbLocation& location, TEvHealthQuery::TPtr& event)
        : Location(location)
        , Owner(event->Sender)
        , Cookie(event->Cookie)
        , Query(event->Release().Release())
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Schedule(Query->Timeout, new NActors::TEvents::TEvWakeup());
        Client = std::make_shared<NYdb::NTable::TTableClient>(Location.GetTableClient(Query->ClientSettings));
        Client->CreateSession(NYdb::NTable::TCreateSessionSettings().ClientTimeout(Query->Timeout)).Subscribe(
            [actorSystem = TActivationContext::ActorSystem(), actorId = SelfId(), client = Client]
            (NYdb::NTable::TAsyncCreateSessionResult result) {
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(result.ExtractValue()));
            });
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr& event) {
        const auto& result = event->Get()->Result;
        if (!result.IsSuccess()) {
            Fail();
            return;
        }
        auto session = result.GetSession();
        session.ExecuteDataQuery(Query->Query,
            NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
            Query->Params, NYdb::NTable::TExecDataQuerySettings().ClientTimeout(Query->Timeout).OperationTimeout(Query->Timeout))
            .Subscribe([actorSystem = TActivationContext::ActorSystem(), actorId = SelfId(), session, client = Client]
                       (NYdb::NTable::TAsyncDataQueryResult result) {
                actorSystem->Send(actorId, new TEvHealthQueryResult(result.ExtractValue()));
            });
    }

    void Handle(TEvHealthQueryResult::TPtr& event) {
        if (!event->Get()->IsSuccess()) {
            LOG_WARN_S(*NActors::TlsActivationContext, EService::MVP, "MetaHealth: table query failed");
        }
        Send(Owner, event->Release().Release(), 0, Cookie);
        PassAway();
    }

    void Fail() {
        LOG_WARN_S(*NActors::TlsActivationContext, EService::MVP, "MetaHealth: table request failed or timed out");
        Send(Owner, new TEvHealthQueryResult(), 0, Cookie);
        PassAway();
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            hFunc(TEvHealthQueryResult, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, Fail);
        }
    }
};

class THealthQueryService : public NActors::TActor<THealthQueryService> {
    const TYdbLocation& Location;

public:
    explicit THealthQueryService(const TYdbLocation& location)
        : TActor(&TThis::StateWork)
        , Location(location)
    {}

    void Handle(TEvHealthQuery::TPtr& event) {
        Register(new THealthQueryActor(Location, event));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHealthQuery, Handle);
        }
    }
};

class THealthMonitor : public NActors::TActorBootstrapped<THealthMonitor>, public THandlerActorYdb {
    static constexpr TDuration LeaseDuration = TDuration::Seconds(120);
    static constexpr TDuration LeasePeriod = TDuration::Seconds(30);

    struct TCluster {
        TString Balancer;
        TString TokenName;
        ui64 Generation = 0;
        TMonotonic NextCheck;
        TMonotonic NextLease;
        TMonotonic LeaseDeadline;
        ui64 LeaseQuery = 0;
        ui64 WriteQuery = 0;
        NHttp::THttpOutgoingRequestPtr Request;
        ui64 RequestGeneration = 0;
        TMonotonic RequestDeadline;
        bool RequestExpired = false;
        std::shared_ptr<const TString> PendingBody;
        TInstant PendingCheckedAt;
        std::shared_ptr<const TString> Body;
        TInstant CheckedAt;
    };

    enum class EQuery { Lease, Write };
    struct TQueryInfo {
        EQuery Kind;
        TString Cluster;
        ui64 Generation;
        TMonotonic StartedAt;
    };

    const NActors::TActorId HttpProxy;
    const NActors::TActorId QueryService;
    const TString RootDomain;
    const TString TokenName;
    const TMetaSettings Settings;
    const TString LocalEndpoint;
    const TString Owner = CreateGuidAsString();
    TMap<TString, TCluster> Clusters;
    THashMap<ui64, TQueryInfo> Queries;
    ui64 NextId = 0;
    ui64 NextGeneration = 0;
    bool LoadingClusters = false;
    TMonotonic NextClusterDiscovery;

public:
    THealthMonitor(NActors::TActorId httpProxy, NActors::TActorId queryService, TString rootDomain,
                   TString tokenName, TMetaSettings settings, TString localEndpoint)
        : HttpProxy(httpProxy)
        , QueryService(queryService)
        , RootDomain(std::move(rootDomain))
        , TokenName(std::move(tokenName))
        , Settings(std::move(settings))
        , LocalEndpoint(std::move(localEndpoint))
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        Wakeup();
    }

    TString Table(TStringBuf name) const {
        return TStringBuilder() << "`" << RootDomain << "/ydb/" << name << ".db`";
    }

    void Query(TString query, NYdb::TParams params, ui64 cookie = 0) {
        Send(QueryService, new TEvHealthQuery(std::move(query), std::move(params),
            NYdb::NTable::TClientSettings().Database(RootDomain).AuthToken(GetAuthHeaderValue(TokenName))), 0, cookie);
    }

    void LoseOwnership(TCluster& cluster) {
        cluster.LeaseDeadline = {};
        cluster.Body.reset();
        cluster.PendingBody.reset();
    }

    void Wakeup() {
        const auto now = TActivationContext::Monotonic();
        if (!LoadingClusters && now >= NextClusterDiscovery) {
            LoadingClusters = true;
            NYdb::TParamsBuilder params;
            params.AddParam("$service").Utf8(Settings.ServiceEndpoint).Build();
            Query(TStringBuilder() << "DECLARE $service AS Utf8; SELECT name, balancer, api_user_token, meta_endpoint FROM "
                << Table("MasterClusterExt") << " WHERE meta_endpoint=$service;", params.Build());
        }
        for (auto& [name, cluster] : Clusters) {
            if (cluster.LeaseDeadline <= now) {
                LoseOwnership(cluster);
            }
            if (cluster.Body && (cluster.CheckedAt > TActivationContext::Now()
                || TActivationContext::Now() - cluster.CheckedAt >= Settings.Healthcheck.StatusTtl)) {
                cluster.Body.reset();
            }
            if (cluster.Request && !cluster.RequestExpired && cluster.RequestDeadline <= now) {
                // The transport timeout is inactivity-based. Keep the slot
                // occupied until it finishes so a trickling response cannot
                // create overlapping requests. Late results are discarded.
                LOG_WARN_S(*NActors::TlsActivationContext, EService::MVP,
                    "MetaHealth: absolute HTTP deadline exceeded for cluster " << name);
                cluster.RequestExpired = true;
            }
        }
        Dispatch();
        Schedule(TDuration::Seconds(1), new NActors::TEvents::TEvWakeup());
    }

    void AddClusterParams(NYdb::TParamsBuilder& params, const TString& name, const TCluster& cluster) {
        params.AddParam("$cluster").Utf8(name).Build();
        params.AddParam("$owner").Utf8(Owner).Build();
        params.AddParam("$service").Utf8(Settings.ServiceEndpoint).Build();
        params.AddParam("$balancer").Utf8(cluster.Balancer).Build();
        params.AddParam("$token").Utf8(cluster.TokenName).Build();
    }

    TString Declarations() const {
        return "DECLARE $cluster AS Utf8; DECLARE $owner AS Utf8; DECLARE $service AS Utf8; "
               "DECLARE $balancer AS Utf8; DECLARE $token AS Utf8; ";
    }

    void Acquire(const TString& name, TCluster& cluster) {
        NYdb::TParamsBuilder params;
        AddClusterParams(params, name, cluster);
        params.AddParam("$forward").Utf8(LocalEndpoint).Build();
        const ui64 id = ++NextId;
        Queries.emplace(id, TQueryInfo{EQuery::Lease, name, cluster.Generation, TActivationContext::Monotonic()});
        cluster.LeaseQuery = id;
        Query(TStringBuilder() << Declarations() << "DECLARE $forward AS Utf8; "
            << "$eligible = SELECT c.name AS Cluster FROM " << Table("MasterClusterExt") << " AS c LEFT JOIN "
            << Table("ClusterHealth") << " AS l ON c.name=l.Cluster "
            << "WHERE c.name=$cluster AND c.meta_endpoint=$service AND c.balancer=$balancer "
            << "AND COALESCE(c.api_user_token, '')=$token "
            << "AND (l.Owner IS NULL OR l.Owner=$owner OR l.LastCheck IS NULL "
            << "OR l.LastCheck + Interval('PT120S') <= CurrentUtcTimestamp()); "
            << "UPSERT INTO " << Table("ClusterHealth")
            << " (Cluster, Owner, ForwardEndpoint, ServiceEndpoint, LastCheck) "
            << "SELECT Cluster, $owner AS Owner, $forward AS ForwardEndpoint, $service AS ServiceEndpoint, "
            << "CurrentUtcTimestamp() AS LastCheck FROM $eligible; "
            << "SELECT Cluster FROM $eligible;", params.Build(), id);
    }

    void Dispatch() {
        const auto now = TActivationContext::Monotonic();
        for (auto& [name, cluster] : Clusters) {
            if (!cluster.LeaseQuery && now >= cluster.NextLease) {
                Acquire(name, cluster);
            }
            if (cluster.LeaseDeadline <= now || cluster.Request || cluster.WriteQuery || now < cluster.NextCheck) {
                continue;
            }
            // No database filter: one complete cluster check, also retaining the
            // detailed issue log and hints consumed by healthcheck-proxy.
            const TString url = GetApiUrl(cluster.Balancer, TStringBuilder()
                << "/healthcheck?verbose=true&return_hints=true&timeout=" << Settings.Healthcheck.RequestTimeout.MilliSeconds());
            cluster.Request = NHttp::THttpOutgoingRequest::CreateRequestGet(url);
            const TString token = GetAuthHeaderValue(cluster.TokenName);
            if (url.StartsWith("https://") && !token.empty()) {
                cluster.Request->Set("Authorization", token);
            }
            cluster.Request->Set("Accept", "application/json");
            cluster.RequestGeneration = cluster.Generation;
            cluster.RequestExpired = false;
            cluster.NextCheck = now + Settings.Healthcheck.CheckPeriod;
            const TDuration timeout = Settings.Healthcheck.RequestTimeout
                + Settings.Healthcheck.RequestTimeout / 5 + TDuration::Seconds(1);
            cluster.RequestDeadline = now + timeout;
            auto* event = new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(cluster.Request, timeout);
            event->AllowConnectionReuse = true;
            Send(HttpProxy, event);
        }
    }

    void LoadClusters(const NYdb::TResultSet& result) {
        TMap<TString, TCluster> clusters;
        NYdb::TResultSetParser parser(result);
        while (parser.TryNextRow()) {
            TString name = ColumnValueToString(parser.GetValue("name"));
            TString balancer = ColumnValueToString(parser.GetValue("balancer"));
            TString token = ColumnValueToString(parser.GetValue("api_user_token"));
            TString service = ColumnValueToString(parser.GetValue("meta_endpoint"));
            if (name.empty() || balancer.empty() || service != Settings.ServiceEndpoint) {
                continue;
            }
            auto old = Clusters.find(name);
            auto& cluster = clusters[name];
            if (old != Clusters.end()) {
                cluster = std::move(old->second);
            }
            if (!cluster.Generation || cluster.Balancer != balancer || cluster.TokenName != token) {
                LoseOwnership(cluster);
                cluster.Balancer = std::move(balancer);
                cluster.TokenName = std::move(token);
                cluster.Generation = ++NextGeneration;
                cluster.NextLease = {};
                cluster.NextCheck = {};
                // Keep an old HTTP request/write draining before starting the
                // replacement. Generation checks discard their late results.
            }
        }
        Clusters = std::move(clusters);
    }

    void Handle(TEvHealthQueryResult::TPtr& event) {
        if (event->Cookie == 0) {
            LoadingClusters = false;
            NextClusterDiscovery = TActivationContext::Monotonic() + Settings.Healthcheck.DiscoveryPeriod;
            if (event->Get()->IsSuccess() && event->Get()->Result->GetResultSets().size() == 1) {
                try {
                    LoadClusters(event->Get()->Result->GetResultSet(0));
                } catch (const std::exception&) {
                    LOG_WARN_S(*NActors::TlsActivationContext, EService::MVP, "MetaHealth: invalid cluster registry");
                    // A partially read registry must never leave active owners.
                    Clusters.clear();
                }
            }
        } else {
            const auto query = Queries.find(event->Cookie);
            if (query == Queries.end()) {
                return;
            }
            const auto info = query->second;
            Queries.erase(query);
            const auto it = Clusters.find(info.Cluster);
            if (it == Clusters.end()) {
                return;
            }
            auto& cluster = it->second;
            if (info.Kind == EQuery::Lease) {
                if (cluster.LeaseQuery != event->Cookie) {
                    return;
                }
                cluster.LeaseQuery = 0;
            } else {
                if (cluster.WriteQuery != event->Cookie) {
                    return;
                }
                cluster.WriteQuery = 0;
            }
            if (cluster.Generation != info.Generation) {
                Dispatch();
                return;
            }
            bool applied = false;
            const bool success = event->Get()->IsSuccess() && event->Get()->Result->GetResultSets().size() == 1;
            if (success) {
                try {
                    NYdb::TResultSetParser parser(event->Get()->Result->GetResultSet(0));
                    applied = parser.TryNextRow();
                } catch (const std::exception&) {
                    applied = false;
                }
            }
            const auto now = TActivationContext::Monotonic();
            if (info.Kind == EQuery::Lease) {
                cluster.NextLease = now + LeasePeriod;
                if (applied && info.StartedAt + LeaseDuration > now) {
                    // Conservative local deadline measured from query dispatch,
                    // independent of clock skew between Meta and the database.
                    cluster.LeaseDeadline = info.StartedAt + LeaseDuration;
                } else if (success || cluster.LeaseDeadline <= now) {
                    LoseOwnership(cluster);
                }
            } else {
                if (applied && cluster.LeaseDeadline > now && cluster.PendingBody) {
                    cluster.Body = std::move(cluster.PendingBody);
                    cluster.CheckedAt = cluster.PendingCheckedAt;
                } else {
                    cluster.PendingBody.reset();
                    if (success && !applied) {
                        LoseOwnership(cluster);
                    }
                }
                // If collection was slower than the period, skip missed ticks.
                // Do not build a backlog or immediately retry failed writes.
                if (cluster.NextCheck <= now) {
                    cluster.NextCheck = now + Settings.Healthcheck.CheckPeriod;
                }
            }
        }
        Dispatch();
    }

    void Store(const TString& name, TCluster& cluster, const TString& status, const TDatabaseHealthMap& databases) {
        NYdb::TParamsBuilder params;
        AddClusterParams(params, name, cluster);
        params.AddParam("$status").Utf8(status).Build();
        params.AddParam("$time").Timestamp(cluster.PendingCheckedAt).Build();
        NYdb::TValueBuilder rows;
        if (databases.empty()) {
            rows.EmptyList(NYdb::TTypeBuilder().BeginStruct().AddMember("Database").Primitive(NYdb::EPrimitiveType::Utf8)
                .AddMember("Status").Primitive(NYdb::EPrimitiveType::Utf8).EndStruct().Build());
        } else {
            rows.BeginList();
            for (const auto& [database, health] : databases) {
                rows.AddListItem().BeginStruct().AddMember("Database").Utf8(database)
                    .AddMember("Status").Utf8(health.Status).EndStruct();
            }
            rows.EndList();
        }
        params.AddParam("$rows", rows.Build());
        const ui64 id = ++NextId;
        cluster.WriteQuery = id;
        Queries.emplace(id, TQueryInfo{EQuery::Write, name, cluster.Generation, TActivationContext::Monotonic()});
        Query(TStringBuilder() << Declarations()
            << "DECLARE $status AS Utf8; DECLARE $time AS Timestamp; "
            << "DECLARE $rows AS List<Struct<Database:Utf8,Status:Utf8>>; "
            << "$eligible = SELECT l.Cluster AS Cluster FROM " << Table("ClusterHealth") << " AS l INNER JOIN "
            << Table("MasterClusterExt") << " AS c ON c.name=l.Cluster "
            << "WHERE l.Cluster=$cluster AND l.Owner=$owner AND l.ServiceEndpoint=$service "
            << "AND l.LastCheck + Interval('PT120S') > CurrentUtcTimestamp() "
            << "AND c.meta_endpoint=$service AND c.balancer=$balancer AND COALESCE(c.api_user_token, '')=$token; "
            << "DELETE FROM " << Table("DatabaseHealth") << " WHERE Cluster=$cluster AND Cluster IN (SELECT Cluster FROM $eligible); "
            << "UPSERT INTO " << Table("DatabaseHealth") << " (Cluster, Database, Status, CheckedAt) "
            << "SELECT $cluster AS Cluster, Database, Status, $time AS CheckedAt FROM AS_TABLE($rows) "
            << "WHERE $cluster IN (SELECT Cluster FROM $eligible); "
            << "UPDATE " << Table("ClusterHealth") << " SET Status=$status, CheckedAt=$time WHERE Cluster IN (SELECT Cluster FROM $eligible); "
            << "SELECT Cluster FROM $eligible;", params.Build(), id);
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& event) {
        auto it = std::find_if(Clusters.begin(), Clusters.end(), [&](const auto& entry) {
            return entry.second.Request && entry.second.Request == event->Get()->Request;
        });
        if (it == Clusters.end()) {
            return;
        }
        auto& cluster = it->second;
        cluster.Request.Reset();
        const auto now = TActivationContext::Monotonic();
        if (cluster.RequestGeneration == cluster.Generation && cluster.LeaseDeadline > now && cluster.RequestDeadline > now) {
            NJson::TJsonValue json;
            TString status;
            TDatabaseHealthMap databases;
            const auto& response = event->Get()->Response;
            const auto checkedAt = TActivationContext::Now();
            if (event->Get()->Error.empty() && response && response->Status == "200"
                && NJson::ReadJsonTree(response->Body, &json)
                && ParseClusterHealth(json, checkedAt, status, databases)) {
                cluster.PendingBody = std::make_shared<const TString>(response->Body);
                cluster.PendingCheckedAt = checkedAt;
                Store(it->first, cluster, status, databases);
                return;
            }
            LOG_WARN_S(*NActors::TlsActivationContext, EService::MVP, "MetaHealth: check failed for cluster " << it->first);
        }
        cluster.NextCheck = Max(cluster.NextCheck, now + Settings.Healthcheck.CheckPeriod);
        Dispatch();
    }

    void Handle(TEvGetCachedHealth::TPtr& event) {
        auto* reply = new TEvCachedHealth();
        const auto cluster = Clusters.find(event->Get()->Cluster);
        const auto now = TActivationContext::Now();
        if (event->Get()->Owner == Owner && cluster != Clusters.end()
            && cluster->second.LeaseDeadline > TActivationContext::Monotonic()
            && cluster->second.Body && cluster->second.CheckedAt <= now
            && now - cluster->second.CheckedAt < Settings.Healthcheck.StatusTtl) {
            reply->Body = cluster->second.Body;
            reply->CheckedAt = cluster->second.CheckedAt;
        }
        Send(event->Sender, reply, 0, event->Cookie);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHealthQueryResult, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(TEvGetCachedHealth, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, Wakeup);
            cFunc(NActors::TEvents::TSystem::Poison, PassAway);
        }
    }
};

} // namespace

NActors::IActor* CreateHealthQueryService(const TYdbLocation& location) {
    return new THealthQueryService(location);
}

NActors::IActor* CreateHealthMonitor(const NActors::TActorId& httpProxy, const NActors::TActorId& queryService,
                                   TString rootDomain, TString tokenName, TMetaSettings settings, TString localEndpoint) {
    return new THealthMonitor(httpProxy, queryService, std::move(rootDomain), std::move(tokenName),
                              std::move(settings), std::move(localEndpoint));
}

} // namespace NMVP
