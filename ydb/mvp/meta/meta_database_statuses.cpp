#include "meta_database_statuses.h"
#include "meta_cluster_info.h"
#include "mvp.h"

#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/quote/quote.h>
#include <util/string/cast.h>

namespace NMVP {
namespace {

constexpr TDuration QUERY_TIMEOUT = TDuration::Seconds(2);
constexpr TDuration WHOAMI_TIMEOUT = TDuration::Seconds(5);
constexpr TDuration REQUEST_TIMEOUT = TDuration::Seconds(10);

class TDatabaseStatusesRequest : public NActors::TActorBootstrapped<TDatabaseStatusesRequest>, private THandlerActorYdb {
    enum EQuery : ui64 {
        ClusterInfo = 1,
        Statuses = 2,
        Lease = 3,
        Cached = 4,
    };

    const NActors::TActorId HttpProxy;
    const NActors::TActorId QueryService;
    const TYdbLocation& Location;
    const TDuration StatusTtl;
    const bool FullResponse;
    const NActors::TActorId Monitor;
    const TString ServiceEndpoint;
    const TString LocalEndpoint;
    TRequest Request;
    TString ClusterName;
    TString AssignedEndpoint;
    EQuery PendingQuery = ClusterInfo;
    NHttp::THttpOutgoingRequestPtr WhoamiRequest;
    NHttp::THttpOutgoingRequestPtr ForwardRequest;

public:
    TDatabaseStatusesRequest(const NActors::TActorId& httpProxy, const NActors::TActorId& queryService,
                             const TYdbLocation& location, TDuration statusTtl,
                             const NActors::TActorId& sender, NHttp::THttpIncomingRequestPtr request,
                             bool fullResponse, NActors::TActorId monitor, TString serviceEndpoint, TString localEndpoint)
        : HttpProxy(httpProxy)
        , QueryService(queryService)
        , Location(location)
        , StatusTtl(statusTtl)
        , FullResponse(fullResponse)
        , Monitor(monitor)
        , ServiceEndpoint(std::move(serviceEndpoint))
        , LocalEndpoint(std::move(localEndpoint))
        , Request(sender, std::move(request))
    {}

    void Bootstrap() {
        if (Request.Request->Method == "OPTIONS") {
            Reply("204", "No Content", {});
            return;
        }
        if (Request.Request->Method != "GET") {
            Fail("405", "Method Not Allowed");
            return;
        }
        ClusterName = Request.Parameters["cluster_name"];
        if (ClusterName.empty() && FullResponse) {
            ClusterName = Request.Parameters["name"];
        }
        if (ClusterName.empty()) {
            Fail("400", "Bad Request", "cluster_name is required");
            return;
        }
        Become(&TThis::StateWork);
        Schedule(FullResponse ? TDuration::Seconds(30) : REQUEST_TIMEOUT, new NActors::TEvents::TEvWakeup());
        Send(QueryService, new TEvHealthQuery(BuildClusterInfoQuery(Location.RootDomain),
            BuildClusterInfoQueryParams(ClusterName),
            TMVP::GetStrictMetaDatabaseClientSettings(Request, Location), QUERY_TIMEOUT), 0, ClusterInfo);
    }

    void Handle(TEvHealthQueryResult::TPtr& event) {
        // Replies come from the query executor's child actor, not the service.
        if (event->Cookie != PendingQuery || WhoamiRequest || ForwardRequest) {
            return;
        }
        if (!event->Get()->IsSuccess() || event->Get()->Result->GetResultSets().size() != 1) {
            Fail("503", "Service Unavailable");
            return;
        }
        try {
            const auto& result = event->Get()->Result->GetResultSet(0);
            if (PendingQuery == ClusterInfo) {
                THashMap<TString, TString> clusterInfo;
                if (!TryExtractClusterInfo(result, clusterInfo)) {
                    Fail("404", "Not Found");
                    return;
                }
                const auto balancer = clusterInfo.find("balancer");
                if (balancer == clusterInfo.end() || balancer->second.empty()) {
                    Fail("503", "Service Unavailable");
                    return;
                }
                AssignedEndpoint = clusterInfo["meta_endpoint"];
                RequestWhoami(balancer->second);
            } else if (PendingQuery == Statuses) {
                ReplyStatuses(ParseDatabaseHealth(result));
            } else if (PendingQuery == Lease) {
                NYdb::TResultSetParser parser(result);
                if (!parser.TryNextRow()) {
                    Fail("503", "Service Unavailable", "No active healthcheck owner");
                    return;
                }
                const TString owner = ColumnValueToString(parser.GetValue("Owner"));
                const TString endpoint = ColumnValueToString(parser.GetValue("ForwardEndpoint"));
                if (endpoint.empty() || owner.empty()) {
                    Fail("503", "Service Unavailable");
                } else if (endpoint != LocalEndpoint) {
                    Forward(endpoint);
                } else if (Monitor) {
                    PendingQuery = Cached;
                    Send(Monitor, new TEvGetCachedHealth(ClusterName, owner), 0, Cached);
                } else {
                    Fail("503", "Service Unavailable", "Local healthcheck collector is unavailable");
                }
            }
        } catch (const std::exception&) {
            Fail("503", "Service Unavailable");
        }
    }

    void RequestWhoami(const TString& balancer) {
        // Only the trusted cluster record selects the destination. No database
        // parameter: whoami must check cluster viewer access, not database access.
        WhoamiRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(GetApiUrl(balancer, "/whoami"));
        NHttp::THeaders headers(Request.Request->Headers);
        for (TStringBuf name : {"Authorization", "Cookie", "x-yacloud-subjecttoken", "x-request-id"}) {
            Request.ForwardHeader(headers, WhoamiRequest, name);
        }
        // OIDC, when present on this route, decides whether to exchange the
        // caller's credentials. Never authorize a caller with api_user_token.
        WhoamiRequest->Set("Accept", "application/json");
        WhoamiRequest->Set("Cache-Control", "no-cache, no-store");
        Send(HttpProxy, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(WhoamiRequest, WHOAMI_TIMEOUT));
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr& event) {
        const auto& incoming = *event->Get();
        if (ForwardRequest && incoming.Request == ForwardRequest) {
            ForwardRequest.Reset();
            if (!incoming.Error.empty() || !incoming.Response) {
                Fail("502", "Bad Gateway");
                return;
            }
            const auto& response = incoming.Response;
            const NHttp::THeaders headers(response->Headers);
            if (FullResponse && response->Status == "200") {
                TInstant checkedAt;
                TInstant expiresAt;
                if (!TInstant::TryParseIso8601(headers["X-Healthcheck-Checked-At"], checkedAt)
                    || !TInstant::TryParseIso8601(headers["X-Healthcheck-Expires-At"], expiresAt)) {
                    Fail("502", "Bad Gateway", "Forwarded healthcheck has no valid freshness metadata");
                    return;
                }
                const auto now = NActors::TActivationContext::Now();
                if (checkedAt > now || now >= expiresAt || now - checkedAt >= StatusTtl) {
                    Fail("503", "Service Unavailable", "Forwarded healthcheck cache has expired");
                    return;
                }
            }
            Reply(response->Status, response->Message, response->Body,
                headers["X-Healthcheck-Checked-At"], headers["X-Healthcheck-Expires-At"]);
            return;
        }
        if (!WhoamiRequest || incoming.Request != WhoamiRequest) {
            return;
        }
        WhoamiRequest.Reset();
        if (!incoming.Error.empty() || !incoming.Response) {
            Fail("502", "Bad Gateway");
            return;
        }
        if (incoming.Response->Status == "401") {
            Fail("401", "Unauthorized");
            return;
        }
        if (incoming.Response->Status == "403") {
            Fail("403", "Forbidden");
            return;
        }
        // Do not follow login redirects or treat an OIDC profile-only 200 as
        // permission to read statuses.
        NJson::TJsonValue whoami;
        if (incoming.Response->Status != "200"
            || !NJson::ReadJsonTree(incoming.Response->Body, &whoami)
            || !whoami.IsMap()) {
            Fail("502", "Bad Gateway");
            return;
        }
        const auto& allowed = whoami[FullResponse ? "IsMonitoringAllowed" : "IsViewerAllowed"];
        if (!allowed.IsBoolean() || !allowed.GetBoolean()) {
            Fail("403", "Forbidden");
            return;
        }

        // Only after authorization: neither ownership information nor cached
        // database names may be disclosed to an unauthorized caller.
        if (!ServiceEndpoint.empty() && !AssignedEndpoint.empty() && AssignedEndpoint != ServiceEndpoint) {
            Forward(AssignedEndpoint);
        } else if (FullResponse) {
            if (AssignedEndpoint.empty()) {
                Fail("503", "Service Unavailable", "Cluster has no assigned Meta service");
                return;
            }
            PendingQuery = Lease;
            NYdb::TParamsBuilder params;
            params.AddParam("$cluster").Utf8(ClusterName).Build();
            params.AddParam("$service").Utf8(AssignedEndpoint).Build();
            Send(QueryService, new TEvHealthQuery(
                TStringBuilder() << "DECLARE $cluster AS Utf8; DECLARE $service AS Utf8; "
                    << "SELECT Owner, ForwardEndpoint FROM `" << Location.RootDomain << "/ydb/ClusterHealth.db` "
                    << "WHERE Cluster=$cluster AND ServiceEndpoint=$service "
                    << "AND LastCheck + Interval('PT120S') > CurrentUtcTimestamp();",
                params.Build(), TMVP::GetStrictMetaDatabaseClientSettings(Request, Location), QUERY_TIMEOUT), 0, Lease);
        } else {
            PendingQuery = Statuses;
            Send(QueryService, MakeLoadDatabaseHealthQuery(Location, Request,
                NActors::TActivationContext::Now(), StatusTtl), 0, Statuses);
        }
    }

    void Forward(const TString& endpoint) {
        const NHttp::THeaders headers(Request.Request->Headers);
        const auto hops = FromStringWithDefault<ui32>(headers["X-Meta-Healthcheck-Hops"], 0);
        if (hops >= 2 || (!endpoint.StartsWith("https://") && !endpoint.StartsWith("http://"))) {
            Fail("503", "Service Unavailable", "Healthcheck forwarding is unavailable");
            return;
        }
        // Targets come only from the trusted cluster registry or ownership table.
        // A regional hop followed by one owner-pod hop is allowed, never a loop.
        TString base = endpoint;
        while (base.EndsWith("/")) {
            base.pop_back();
        }
        ForwardRequest = NHttp::THttpOutgoingRequest::CreateRequestGet(TStringBuilder() << base
            << (FullResponse ? "/meta/healthcheck" : "/meta/database_statuses")
            << "?cluster_name=" << CGIEscapeRet(ClusterName));
        for (TStringBuf name : {"Authorization", "Cookie", "x-yacloud-subjecttoken", "x-request-id"}) {
            Request.ForwardHeader(headers, ForwardRequest, name);
        }
        ForwardRequest->Set("X-Meta-Healthcheck-Hops", ToString(hops + 1));
        ForwardRequest->Set("Accept", "application/json");
        ForwardRequest->Set("Cache-Control", "no-cache, no-store");
        Send(HttpProxy, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(ForwardRequest, TDuration::Seconds(10)));
    }

    void Handle(TEvCachedHealth::TPtr& event) {
        if (event->Sender != Monitor || PendingQuery != Cached) {
            return;
        }
        const auto now = NActors::TActivationContext::Now();
        const auto& cached = *event->Get();
        if (!cached.Body || cached.CheckedAt > now || now - cached.CheckedAt >= StatusTtl) {
            Fail("503", "Service Unavailable", "Healthcheck cache is empty or expired");
            return;
        }
        Reply("200", "OK", *cached.Body, cached.CheckedAt.ToString(), (cached.CheckedAt + StatusTtl).ToString());
    }

    void ReplyStatuses(const TDatabaseHealthMap& statuses) {
        const auto now = NActors::TActivationContext::Now();
        NJson::TJsonValue body(NJson::JSON_MAP);
        body["cluster_name"] = Request.Parameters["cluster_name"];
        auto& databases = body["databases"];
        databases.SetType(NJson::JSON_ARRAY);
        for (const auto& [name, health] : statuses) {
            // Recheck freshness after the query: a status may expire in flight.
            if (health.CheckedAt > now || now - health.CheckedAt >= StatusTtl) {
                continue;
            }
            auto& database = databases.AppendValue(NJson::TJsonValue(NJson::JSON_MAP));
            database["Name"] = name;
            database["Overall"] = HealthcheckOverall(health.Status);
            database["HealthcheckStatus"] = health.Status;
            database["CheckedAt"] = health.CheckedAt.ToString();
            database["ExpiresAt"] = (health.CheckedAt + StatusTtl).ToString();
        }
        Reply("200", "OK", NJson::WriteJson(body, false));
    }

    void Reply(TStringBuf status, TStringBuf message, TStringBuf body, TStringBuf checkedAt = {}, TStringBuf expiresAt = {}) {
        NHttp::THeadersBuilder headers;
        SetCORS(Request.Request, &headers);
        headers.Set("Allow", "GET,OPTIONS");
        headers.Set("Access-Control-Allow-Methods", "GET,OPTIONS");
        headers.Set("Cache-Control", "no-store");
        headers.Set("Vary", "Authorization,Cookie,Origin");
        headers.Set("Access-Control-Expose-Headers", "X-Healthcheck-Checked-At,X-Healthcheck-Expires-At");
        if (!checkedAt.empty() && !expiresAt.empty()) {
            headers.Set("X-Healthcheck-Checked-At", checkedAt);
            headers.Set("X-Healthcheck-Expires-At", expiresAt);
        }
        if (!body.empty()) {
            headers.Set("Content-Type", "application/json; charset=utf-8");
        }
        Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(
            Request.Request->CreateResponse(status, message, headers, body)));
        PassAway();
    }

    void Fail(TStringBuf status, TStringBuf message, TStringBuf detail = {}) {
        NJson::TJsonValue body(NJson::JSON_MAP);
        body["error"] = TString(detail.empty() ? message : detail);
        Reply(status, message, NJson::WriteJson(body, false));
    }

    void Timeout() {
        Fail("504", "Gateway Timeout");
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHealthQueryResult, Handle);
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            hFunc(TEvCachedHealth, Handle);
            cFunc(NActors::TEvents::TSystem::Wakeup, Timeout);
        }
    }
};

class TDatabaseStatusesHandler : public NActors::TActor<TDatabaseStatusesHandler> {
    const NActors::TActorId HttpProxy;
    const NActors::TActorId QueryService;
    const TYdbLocation& Location;
    const TDuration StatusTtl;
    const bool FullResponse;
    const NActors::TActorId Monitor;
    const TString ServiceEndpoint;
    const TString LocalEndpoint;

public:
    TDatabaseStatusesHandler(const NActors::TActorId& httpProxy, const NActors::TActorId& queryService,
                            const TYdbLocation& location, TDuration statusTtl,
                            bool fullResponse = false, NActors::TActorId monitor = {},
                            TString serviceEndpoint = {}, TString localEndpoint = {})
        : TActor(&TThis::StateWork)
        , HttpProxy(httpProxy)
        , QueryService(queryService)
        , Location(location)
        , StatusTtl(statusTtl)
        , FullResponse(fullResponse)
        , Monitor(monitor)
        , ServiceEndpoint(std::move(serviceEndpoint))
        , LocalEndpoint(std::move(localEndpoint))
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& event) {
        Register(new TDatabaseStatusesRequest(HttpProxy, QueryService, Location, StatusTtl,
            event->Sender, event->Get()->Request, FullResponse, Monitor, ServiceEndpoint, LocalEndpoint));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace

NActors::IActor* CreateDatabaseStatusesHandler(const NActors::TActorId& httpProxy,
                                              const NActors::TActorId& queryService,
                                              const TYdbLocation& location,
                                              TMetaSettings::THealthcheckSettings settings, TString serviceEndpoint) {
    return new TDatabaseStatusesHandler(httpProxy, queryService, location, settings.StatusTtl,
        false, {}, std::move(serviceEndpoint));
}

NActors::IActor* CreateCachedHealthcheckHandler(const NActors::TActorId& httpProxy,
                                               const NActors::TActorId& queryService,
                                               const NActors::TActorId& monitor,
                                               const TYdbLocation& location,
                                               const TMetaSettings& settings, TString localEndpoint) {
    return new TDatabaseStatusesHandler(httpProxy, queryService, location, settings.Healthcheck.StatusTtl,
        true, monitor, settings.ServiceEndpoint, std::move(localEndpoint));
}

} // namespace NMVP
