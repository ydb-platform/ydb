#include "meta_health.h"
#include "mvp.h"
#include "meta_cp_databases.h"
#include "meta_database_statuses.h"
#include "ut/meta_test_runtime.h"

#include <ydb/public/api/protos/ydb_value.pb.h>

#include <util/generic/hash_set.h>

#include <tuple>

using namespace NActors;
using namespace NMVP;

namespace {

NYdb::NTable::TDataQueryResult QueryResult(TVector<NYdb::TResultSet> results = {}) {
    return NYdb::NTable::TDataQueryResult(NYdb::TStatus(NYdb::EStatus::SUCCESS, {}),
        std::move(results), std::nullopt, std::nullopt, false, std::nullopt);
}

NYdb::TResultSet ClusterList(std::initializer_list<TString> names, TStringBuf balancerSuffix = {},
                            TStringBuf serviceEndpoint = "https://meta.region") {
    Ydb::ResultSet result;
    for (const auto& name : {"name", "balancer", "api_user_token", "control_plane", "mvp_token", "meta_endpoint"}) {
        auto* column = result.add_columns();
        column->set_name(name);
        column->mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    for (const auto& name : names) {
        auto* row = result.add_rows();
        row->add_items()->set_text_value(name);
        const TString balancer = TStringBuilder() << "https://" << name << balancerSuffix << "/viewer/json";
        row->add_items()->set_text_value(balancer);
        row->add_items()->set_text_value("");
        row->add_items()->set_text_value("");
        row->add_items()->set_text_value("");
        row->add_items()->set_text_value(TString(serviceEndpoint));
    }
    return NYdb::TResultSet(std::move(result));
}

NYdb::TResultSet Applied(bool applied = true) {
    Ydb::ResultSet result;
    auto* column = result.add_columns();
    column->set_name("Cluster");
    column->mutable_type()->set_type_id(Ydb::Type::UTF8);
    if (applied) {
        result.add_rows()->add_items()->set_text_value("cluster");
    }
    return NYdb::TResultSet(std::move(result));
}

NYdb::TResultSet Lease(TStringBuf endpoint = "https://local:8789") {
    Ydb::ResultSet result;
    for (const auto& name : {"Owner", "ForwardEndpoint"}) {
        auto* column = result.add_columns();
        column->set_name(name);
        column->mutable_type()->set_type_id(Ydb::Type::UTF8);
    }
    auto* row = result.add_rows();
    row->add_items()->set_text_value("owner");
    row->add_items()->set_text_value(TString(endpoint));
    return NYdb::TResultSet(std::move(result));
}

const TString ClusterHealthBody = R"({"self_check_result":"MAINTENANCE_REQUIRED","database_status":[{"name":"/Root/a","overall":"ORANGE"},{"name":"/Root/b","overall":"GREEN"}],"issue_log":[{"type":"STORAGE_GROUP","level":4,"message":"details"}]})";

NYdb::TResultSet DatabaseStatuses(TInstant checkedAt) {
    Ydb::ResultSet result;
    auto* database = result.add_columns();
    database->set_name("Database");
    database->mutable_type()->set_type_id(Ydb::Type::UTF8);
    auto* status = result.add_columns();
    status->set_name("Status");
    status->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::UTF8);
    auto* time = result.add_columns();
    time->set_name("CheckedAt");
    time->mutable_type()->mutable_optional_type()->mutable_item()->set_type_id(Ydb::Type::TIMESTAMP);
    auto* row = result.add_rows();
    row->add_items()->set_text_value("/Root/a");
    row->add_items()->set_text_value("EMERGENCY");
    row->add_items()->set_uint64_value(checkedAt.MicroSeconds());
    return NYdb::TResultSet(std::move(result));
}

class TTestDatabasesActor : public THandlerActorMetaCpDatabasesGET {
public:
    using THandlerActorMetaCpDatabasesGET::THandlerActorMetaCpDatabasesGET;

    void RequestClusterInfo(const TActorContext& ctx) override {
        ctx.Send(SelfId(), new TEvPrivate::TEvDataQueryResult(QueryResult({ClusterList({"cluster"})})));
    }
};

struct TMonitorTest {
    TMvpTestRuntime Runtime;
    TActorId Http;
    TActorId Store;
    TActorId Monitor;
    TAutoPtr<IEventHandle> HttpHandle;
    TAutoPtr<IEventHandle> QueryHandle;
    NHttp::THttpOutgoingRequestPtr Request;
    TString Owner;

    TMonitorTest() {
        Runtime.Initialize();
        Runtime.UpdateCurrentTime(TInstant::Seconds(1000));
        Http = Runtime.AllocateEdgeActor();
        Store = Runtime.AllocateEdgeActor();
        TMetaSettings settings;
        settings.ServiceEndpoint = "https://meta.region";
        Monitor = Runtime.Register(CreateHealthMonitor(Http, Store, "/meta", "", settings, "https://local:8789"));
    }

    TEvHealthQuery* NextQuery() {
        auto* query = Runtime.GrabEdgeEvent<TEvHealthQuery>(QueryHandle, TDuration::Seconds(1));
        UNIT_ASSERT(query);
        return query;
    }

    void ReplyQuery(NYdb::NTable::TDataQueryResult result) {
        Runtime.Send(new IEventHandle(Monitor, Store, new TEvHealthQueryResult(std::move(result)), 0, QueryHandle->Cookie));
    }

    void DiscoverClusters(std::initializer_list<TString> clusters) {
        auto* query = NextQuery();
        UNIT_ASSERT_STRING_CONTAINS(query->Query, "MasterClusterExt.db");
        ReplyQuery(QueryResult({ClusterList(clusters)}));
    }

    NHttp::TEvHttpProxy::TEvHttpOutgoingRequest* NextHttp() {
        auto* event = Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(HttpHandle, TDuration::Seconds(1));
        UNIT_ASSERT(event);
        Request = event->Request;
        return event;
    }

    void ReplyHttp(TString body, TString error = {}) {
        ReplyHttpFor(Request, std::move(body), std::move(error));
    }

    void ReplyHttpFor(const NHttp::THttpOutgoingRequestPtr& request, TString body, TString error = {}) {
        NHttp::THttpIncomingResponsePtr response;
        if (error.empty()) {
            response = new NHttp::THttpIncomingResponse(request);
            EatWholeString(response, TStringBuilder()
                << "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: "
                << body.size() << "\r\n\r\n" << body);
        }
        // Deliberately no cookie: the real HTTP proxy does not forward it.
        Runtime.Send(new IEventHandle(Monitor, Http,
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, response, error)));
    }

    void AcquireCluster(bool applied = true) {
        auto* query = NextQuery();
        UNIT_ASSERT_STRING_CONTAINS(query->Query, "ClusterHealth.db");
        UNIT_ASSERT_STRING_CONTAINS(query->Query, "PT120S");
        UNIT_ASSERT_STRING_CONTAINS(query->Query, "c.meta_endpoint=$service");
        Owner = TString(NYdb::TValueParser(*query->Params.GetValue("$owner")).GetUtf8());
        ReplyQuery(QueryResult({Applied(applied)}));
    }

    std::shared_ptr<const TString> Cached(TString owner = {}) {
        Runtime.Send(new IEventHandle(Monitor, Store, new TEvGetCachedHealth("cluster", owner.empty() ? Owner : owner)));
        TAutoPtr<IEventHandle> handle;
        auto* result = Runtime.GrabEdgeEvent<TEvCachedHealth>(handle, TDuration::Seconds(1));
        UNIT_ASSERT(result);
        return result->Body;
    }

    void WakeAfter(TDuration duration) {
        Runtime.AdvanceCurrentTime(duration);
        Runtime.Send(new IEventHandle(Monitor, Store, new TEvents::TEvWakeup()));
    }

    void AssertNoQuery() {
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEvent<TEvHealthQuery>(handle, TDuration::MilliSeconds(1)));
    }

    void AssertNoHttp() {
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle, TDuration::MilliSeconds(1)));
    }
};

struct TStatusesTest {
    TMvpTestRuntime Runtime;
    const TYdbLocation Location{"meta", "meta", {}, "/meta"};
    TActorId Http;
    TActorId Store;
    TActorId QueryExecutor;
    TActorId Client;
    TActorId Handler;
    TActorId Cache;
    TActorId RequestActor;
    TAutoPtr<IEventHandle> Handle;
    ui64 QueryCookie = 0;
    NHttp::THttpOutgoingRequestPtr Whoami;

    explicit TStatusesTest(bool fullResponse = false) {
        Runtime.Initialize();
        Runtime.UpdateCurrentTime(TInstant::Seconds(1000));
        Http = Runtime.AllocateEdgeActor();
        Store = Runtime.AllocateEdgeActor();
        QueryExecutor = Runtime.AllocateEdgeActor();
        Client = Runtime.AllocateEdgeActor();
        Cache = Runtime.AllocateEdgeActor();
        TMetaSettings settings;
        settings.ServiceEndpoint = "https://meta.region";
        Handler = Runtime.Register(fullResponse
            ? CreateCachedHealthcheckHandler(Http, Store, Cache, Location, settings, "https://local:8789")
            : CreateDatabaseStatusesHandler(Http, Store, Location, {}, settings.ServiceEndpoint));
    }

    void Start(TStringBuf headers = {}, TStringBuf url = "/meta/database_statuses?cluster_name=cluster",
               TStringBuf method = "GET") {
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        EatWholeString(request, TStringBuilder() << method << " " << url
            << " HTTP/1.1\r\nHost: localhost\r\n" << headers << "\r\n");
        UNIT_ASSERT_EQUAL(request->Stage, NHttp::THttpIncomingRequest::EParseStage::Done);
        Runtime.Send(new IEventHandle(Handler, Client, new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request)));
    }

    TEvHealthQuery* NextQuery(TStringBuf table) {
        auto* query = Runtime.GrabEdgeEvent<TEvHealthQuery>(Handle, TDuration::Seconds(1));
        UNIT_ASSERT(query);
        UNIT_ASSERT_STRING_CONTAINS(query->Query, table);
        RequestActor = Handle->Sender;
        QueryCookie = Handle->Cookie;
        return query;
    }

    void ReplyQuery(NYdb::NTable::TDataQueryResult result) {
        // In production a short-lived executor, not Store, sends this reply.
        Runtime.Send(new IEventHandle(RequestActor, QueryExecutor,
            new TEvHealthQueryResult(std::move(result)), 0, QueryCookie));
    }

    void ResolveCluster() {
        auto* query = NextQuery("MasterClusterExt.db");
        auto param = query->Params.GetValue("$name");
        UNIT_ASSERT(param);
        UNIT_ASSERT_VALUES_EQUAL(NYdb::TValueParser(*param).GetUtf8(), "cluster");
        ReplyQuery(QueryResult({ClusterList({"cluster"})}));
    }

    void NextWhoami() {
        auto* outgoing = Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(Handle, TDuration::Seconds(1));
        UNIT_ASSERT(outgoing);
        Whoami = outgoing->Request;
        UNIT_ASSERT_VALUES_EQUAL(Whoami->Host, "cluster");
        UNIT_ASSERT_VALUES_EQUAL(Whoami->URL, "/viewer/json/whoami");
        UNIT_ASSERT_VALUES_EQUAL(outgoing->Timeout, TDuration::Seconds(5));
    }

    void ReplyWhoami(TStringBuf body, TStringBuf status = "200", TStringBuf error = {}) {
        NHttp::THttpIncomingResponsePtr response;
        if (error.empty()) {
            response = new NHttp::THttpIncomingResponse(Whoami);
            EatWholeString(response, TStringBuilder() << "HTTP/1.1 " << status
                << " Response\r\nContent-Type: application/json\r\nContent-Length: "
                << body.size() << "\r\n\r\n" << body);
        }
        Runtime.Send(new IEventHandle(RequestActor, Http,
            new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(Whoami, response, TString(error))));
    }

    NHttp::THttpOutgoingResponsePtr Response(TStringBuf status) {
        auto* result = Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(Handle, TDuration::Seconds(1));
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(result->Response->Status, status);
        UNIT_ASSERT_VALUES_EQUAL(NHttp::THeaders(result->Response->Headers)["Cache-Control"], "no-store");
        return result->Response;
    }

    void AssertNoQuery() {
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEvent<TEvHealthQuery>(handle, TDuration::MilliSeconds(1)));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(MetaHealth) {
    Y_UNIT_TEST(StatusesRequireWhoamiBeforeReadingHealthTable) {
        TStatusesTest test;
        test.Start("Authorization: Bearer caller-token\r\nCookie: oidc_session=caller-session\r\nx-request-id: request-1\r\n",
            "/meta/database_statuses?cluster_name=cluster&database=/DoNotForward");
        test.ResolveCluster();
        test.NextWhoami();
        const NHttp::THeaders headers(test.Whoami->Headers);
        UNIT_ASSERT_VALUES_EQUAL(headers["Authorization"], "Bearer caller-token");
        UNIT_ASSERT_VALUES_EQUAL(headers["Cookie"], "oidc_session=caller-session");
        UNIT_ASSERT_VALUES_EQUAL(headers["x-request-id"], "request-1");
        test.AssertNoQuery();

        test.ReplyWhoami(R"({"IsViewerAllowed":true,"UserSID":"caller","ExtendedInfo":{"private":"profile"}})");
        auto* query = test.NextQuery("DatabaseHealth.db");
        auto cluster = query->Params.GetValue("$cluster");
        UNIT_ASSERT(cluster);
        UNIT_ASSERT_VALUES_EQUAL(NYdb::TValueParser(*cluster).GetUtf8(), "cluster");
        auto cutoff = query->Params.GetValue("$cutoff");
        UNIT_ASSERT(cutoff);
        UNIT_ASSERT_VALUES_EQUAL(NYdb::TValueParser(*cutoff).GetTimestamp(),
            test.Runtime.GetCurrentTime() - TDuration::Seconds(120));
        const auto checkedAt = test.Runtime.GetCurrentTime();
        test.ReplyQuery(QueryResult({DatabaseStatuses(checkedAt)}));
        const auto response = test.Response("200");
        const auto json = ParseJson(response->Body);
        UNIT_ASSERT_VALUES_EQUAL(json["cluster_name"].GetString(), "cluster");
        UNIT_ASSERT_VALUES_EQUAL(json["databases"].GetArray().size(), 1);
        const auto& database = json["databases"][0];
        UNIT_ASSERT_VALUES_EQUAL(database["Name"].GetString(), "/Root/a");
        UNIT_ASSERT_VALUES_EQUAL(database["Overall"].GetString(), "Red");
        UNIT_ASSERT_VALUES_EQUAL(database["HealthcheckStatus"].GetString(), "EMERGENCY");
        UNIT_ASSERT_VALUES_EQUAL(database["CheckedAt"].GetString(), checkedAt.ToString());
        UNIT_ASSERT_VALUES_EQUAL(database["ExpiresAt"].GetString(), (checkedAt + TDuration::Seconds(120)).ToString());
        UNIT_ASSERT(!json.Has("UserSID"));
        UNIT_ASSERT(!json.Has("ExtendedInfo"));
    }

    Y_UNIT_TEST(StatusesForwardCookieOnlySessionToOidc) {
        TStatusesTest test;
        test.Start("Cookie: oidc_session=caller-session\r\n");
        test.ResolveCluster();
        test.NextWhoami();
        const NHttp::THeaders headers(test.Whoami->Headers);
        UNIT_ASSERT(headers["Authorization"].empty());
        UNIT_ASSERT_VALUES_EQUAL(headers["Cookie"], "oidc_session=caller-session");
        test.ReplyWhoami(R"({"IsViewerAllowed":true})");
        test.NextQuery("DatabaseHealth.db");
        test.ReplyQuery(QueryResult({DatabaseStatuses(test.Runtime.GetCurrentTime())}));
        test.Response("200");
    }

    Y_UNIT_TEST(StatusesFailClosedOnDeniedOrInvalidWhoami) {
        struct TCase {
            TStringBuf Status;
            TStringBuf Body;
            TStringBuf Error;
            TStringBuf ExpectedStatus;
        };
        for (const auto& scenario : {
                TCase{"200", R"({"IsViewerAllowed":false})", {}, "403"},
                TCase{"200", R"({"IsViewerAllowed":"true"})", {}, "403"},
                TCase{"200", R"({"IsViewerAllowed":1})", {}, "403"},
                TCase{"200", R"({"IsViewerAllowed":null})", {}, "403"},
                TCase{"200", R"({"UserSID":"profile-only","IsDatabaseAllowed":true})", {}, "403"},
                TCase{"200", R"({"ExtendedErrors":["viewer unavailable"],"UserSID":"oidc-user"})", {}, "403"},
                TCase{"200", "{}", {}, "403"},
                TCase{"200", "[]", {}, "502"},
                TCase{"200", "<html>Login</html>", {}, "502"},
                TCase{"200", "{invalid", {}, "502"},
                TCase{"401", R"({"IsViewerAllowed":true})", {}, "401"},
                TCase{"403", R"({"IsViewerAllowed":true})", {}, "403"},
                TCase{"302", R"({"IsViewerAllowed":true})", {}, "502"},
                TCase{"500", R"({"IsViewerAllowed":true})", {}, "502"},
                TCase{{}, {}, "Timeout", "502"}}) {
            TStatusesTest test;
            test.Start();
            test.ResolveCluster();
            test.NextWhoami();
            test.ReplyWhoami(scenario.Body, scenario.Status, scenario.Error);
            const auto response = test.Response(scenario.ExpectedStatus);
            UNIT_ASSERT(!ParseJson(response->Body).Has("databases"));
            test.AssertNoQuery();
        }
    }

    Y_UNIT_TEST(StatusesDoNotReuseAnotherCallersAuthorization) {
        TStatusesTest test;
        test.Start("Authorization: Bearer allowed-user\r\n");
        test.ResolveCluster();
        test.NextWhoami();
        test.ReplyWhoami(R"({"IsViewerAllowed":true})");
        test.NextQuery("DatabaseHealth.db");
        test.ReplyQuery(QueryResult({DatabaseStatuses(test.Runtime.GetCurrentTime())}));
        test.Response("200");

        test.Start("Authorization: Bearer denied-user\r\n");
        test.ResolveCluster();
        test.NextWhoami();
        UNIT_ASSERT_VALUES_EQUAL(NHttp::THeaders(test.Whoami->Headers)["Authorization"], "Bearer denied-user");
        test.ReplyWhoami(R"({"IsViewerAllowed":false})");
        test.Response("403");
        test.AssertNoQuery();
    }

    Y_UNIT_TEST(StatusesDropExpiredAndFutureRows) {
        for (ui32 scenario = 0; scenario != 3; ++scenario) {
            TStatusesTest test;
            test.Start();
            test.ResolveCluster();
            test.NextWhoami();
            test.ReplyWhoami(R"({"IsViewerAllowed":true})");
            test.NextQuery("DatabaseHealth.db");
            auto checkedAt = test.Runtime.GetCurrentTime();
            if (scenario == 0) {
                checkedAt -= TDuration::Seconds(120);
            } else if (scenario == 1) {
                checkedAt += TDuration::Seconds(1);
            } else {
                // Fresh at query start, expired at response assembly.
                checkedAt -= TDuration::Seconds(119);
                test.Runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            }
            test.ReplyQuery(QueryResult({DatabaseStatuses(checkedAt)}));
            UNIT_ASSERT(ParseJson(test.Response("200")->Body)["databases"].GetArray().empty());
        }
    }

    Y_UNIT_TEST(StatusesHandleQueryFailuresAndUnknownCluster) {
        for (ui32 scenario = 0; scenario != 4; ++scenario) {
            TStatusesTest test;
            test.Start();
            test.NextQuery("MasterClusterExt.db");
            if (scenario == 0) {
                test.ReplyQuery(QueryResult({ClusterList({})}));
                test.Response("404");
            } else if (scenario == 1) {
                // A successful SDK result without the expected result set.
                test.ReplyQuery(QueryResult());
                test.Response("503");
            } else {
                if (scenario == 3) {
                    test.ReplyQuery(QueryResult({ClusterList({"cluster"})}));
                    test.NextWhoami();
                    test.ReplyWhoami(R"({"IsViewerAllowed":true})");
                    test.NextQuery("DatabaseHealth.db");
                }
                test.Runtime.Send(new IEventHandle(test.RequestActor, test.QueryExecutor,
                    new TEvHealthQueryResult(), 0, test.QueryCookie));
                test.Response("503");
            }
            test.AssertNoQuery();
        }
    }

    Y_UNIT_TEST(StatusesHaveBoundedLifetimeAndIgnoreLateWhoami) {
        TStatusesTest test;
        test.Start();
        test.ResolveCluster();
        test.NextWhoami();
        test.Runtime.AdvanceCurrentTime(TDuration::Seconds(10));
        test.Runtime.Send(new IEventHandle(test.RequestActor, test.Client, new TEvents::TEvWakeup()));
        test.Response("504");
        test.ReplyWhoami(R"({"IsViewerAllowed":true})");
        test.AssertNoQuery();
    }

    Y_UNIT_TEST(StatusesValidateRequestWithoutQueryingTables) {
        for (const auto& [method, url, status] : TVector<std::tuple<TString, TString, TString>>{
                {"GET", "/meta/database_statuses", "400"},
                {"POST", "/meta/database_statuses?cluster_name=cluster", "405"},
                {"OPTIONS", "/meta/database_statuses?cluster_name=cluster", "204"}}) {
            TStatusesTest test;
            test.Start({}, url, method);
            test.Response(status);
            test.AssertNoQuery();
        }
    }

    Y_UNIT_TEST(DatabaseEndpointOverridesFreshHealthAndFallsBackOnErrorsOrExpiry) {
        for (ui32 scenario = 0; scenario != 4; ++scenario) {
            const char* argv[] = {"meta-test"};
            TMVP mvp(1, argv);
            mvp.MetaSettings.Healthcheck.Enabled = true;
            TMvpTestRuntime runtime;
            runtime.Initialize();
            runtime.UpdateCurrentTime(TInstant::Now());
            const auto sender = runtime.AllocateEdgeActor();
            const auto http = runtime.AllocateEdgeActor();
            const auto store = runtime.AllocateEdgeActor();
            mvp.HealthQueryServiceId = store;
            const TYdbLocation location("meta", "meta", {}, "/meta");
            const auto actor = runtime.Register(new TTestDatabasesActor(http, location, sender,
                BuildHttpRequest("/meta/cp_databases?cluster_name=cluster&database_name=/Root/a")));
            TAutoPtr<IEventHandle> handle;
            auto* query = runtime.GrabEdgeEvent<TEvHealthQuery>(handle);
            UNIT_ASSERT_STRING_CONTAINS(query->Query, "DatabaseHealth.db");
            UNIT_ASSERT_VALUES_EQUAL(query->Timeout, TDuration::Seconds(2));
            if (scenario == 1) {
                // The query executor uses the same result for timeout and
                // inability to create a session.
                runtime.Send(new IEventHandle(actor, store, new TEvHealthQueryResult()));
            } else {
                runtime.Send(new IEventHandle(actor, store, new TEvHealthQueryResult(QueryResult({
                    DatabaseStatuses(runtime.GetCurrentTime() - TDuration::Seconds(scenario == 2 ? 120 : 0))
                }))));
            }
            auto* outgoing = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(handle);
            UNIT_ASSERT_STRING_CONTAINS(outgoing->Request->URL, "/tenantinfo");
            if (scenario == 3) {
                // A result that was fresh when read can expire while viewer
                // is being queried. The merge must check freshness again.
                runtime.AdvanceCurrentTime(TDuration::Seconds(120));
            }
            const TString body = R"({"TenantInfo":[{"Name":"/Root/a","Overall":"Green"},{"Name":"/Root/b","Overall":"Yellow"}]})";
            NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(outgoing->Request);
            EatWholeString(response, TStringBuilder() << "HTTP/1.1 200 OK\r\nContent-Length: "
                << body.size() << "\r\n\r\n" << body);
            runtime.Send(new IEventHandle(actor, http,
                new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(outgoing->Request, response)));
            auto* reply = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(handle);
            UNIT_ASSERT_VALUES_EQUAL(reply->Response->Status, "200");
            const auto json = ParseJson(reply->Response->Body);
            UNIT_ASSERT_VALUES_EQUAL(json["databases"].GetArray().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(json["databases"][0]["Name"].GetString(), "/Root/a");
            UNIT_ASSERT_VALUES_EQUAL(json["databases"][0]["Overall"].GetString(), scenario == 0 ? "Red" : "Green");
            InstanceMVP = nullptr;
        }
    }

    Y_UNIT_TEST(MapsKnownStatusesOnly) {
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("GREY"), "Grey");
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("GREEN"), "Green");
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("BLUE"), "Blue");
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("YELLOW"), "Yellow");
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("ORANGE"), "Orange");
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("RED"), "Red");
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("GOOD"), "Green");
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("DEGRADED"), "Yellow");
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("MAINTENANCE_REQUIRED"), "Red");
        UNIT_ASSERT_VALUES_EQUAL(HealthcheckOverall("EMERGENCY"), "Red");
        UNIT_ASSERT(HealthcheckOverall("UNSPECIFIED").empty());
        UNIT_ASSERT(HealthcheckOverall("").empty());
    }

    Y_UNIT_TEST(OverridesOnlyFreshMatchingStatus) {
        const auto now = TInstant::Seconds(1000);
        const auto ttl = TDuration::Seconds(120);
        const auto initial = ParseJson(R"({"Name":"/Root/a","Overall":"Yellow","Nodes":[1]})");
        auto database = initial;
        ApplyDatabaseHealth(database, {{"/Root/a", {"GOOD", now - TDuration::Seconds(119)}}}, now, ttl);
        UNIT_ASSERT_VALUES_EQUAL(database["Overall"].GetString(), "Green");
        UNIT_ASSERT_VALUES_EQUAL(database["Nodes"], initial["Nodes"]);
        for (const auto& statuses : TVector<TDatabaseHealthMap>{
                {},
                {{"/Root/b", {"EMERGENCY", now}}},
                {{"/Root/a", {"EMERGENCY", now - ttl}}},
                {{"/Root/a", {"GOOD", now - ttl - TDuration::Seconds(1)}}},
                {{"/Root/a", {"UNKNOWN", now}}},
                {{"/Root/a", {"GOOD", now + TDuration::Seconds(1)}}}}) {
            database = initial;
            ApplyDatabaseHealth(database, statuses, now, ttl);
            UNIT_ASSERT_VALUES_EQUAL(database, initial);
        }
    }

    Y_UNIT_TEST(ClusterSnapshotContainsStatusesAndPreservesFullResponse) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AssertNoHttp(); // ownership must be confirmed first
        test.AcquireCluster();
        auto* request = test.NextHttp();
        UNIT_ASSERT_VALUES_EQUAL(request->Timeout, TDuration::Seconds(37));
        UNIT_ASSERT(request->AllowConnectionReuse);
        NHttp::TUrlParameters params(request->Request->URL);
        UNIT_ASSERT(!params.Has("database"));
        UNIT_ASSERT(!params.Has("max_level"));
        UNIT_ASSERT(!params.Has("merge_records"));
        UNIT_ASSERT_VALUES_EQUAL(params["verbose"], "true");
        UNIT_ASSERT_VALUES_EQUAL(params["return_hints"], "true");
        UNIT_ASSERT_VALUES_EQUAL(params["timeout"], "30000");
        test.AssertNoHttp();
        test.ReplyHttp(ClusterHealthBody);
        auto* query = test.NextQuery();
        UNIT_ASSERT_STRING_CONTAINS(query->Query, "DELETE FROM `/meta/ydb/DatabaseHealth.db`");
        UNIT_ASSERT_STRING_CONTAINS(query->Query, "l.Owner=$owner");
        UNIT_ASSERT_STRING_CONTAINS(query->Query, "l.LastCheck + Interval('PT120S') >");
        UNIT_ASSERT_STRING_CONTAINS(query->Query, "c.meta_endpoint=$service");
        UNIT_ASSERT_VALUES_EQUAL(NYdb::TValueParser(*query->Params.GetValue("$status")).GetUtf8(), "MAINTENANCE_REQUIRED");
        const auto rowValues = query->Params.GetValue("$rows");
        UNIT_ASSERT(rowValues);
        NYdb::TValueParser rows(*rowValues);
        rows.OpenList();
        THashMap<TString, TString> statuses;
        while (rows.TryNextListItem()) {
            rows.OpenStruct();
            TString database;
            TString status;
            while (rows.TryNextMember()) {
                if (rows.GetMemberName() == "Database") {
                    database = TString(rows.GetUtf8());
                } else if (rows.GetMemberName() == "Status") {
                    status = TString(rows.GetUtf8());
                }
            }
            statuses[database] = status;
            rows.CloseStruct();
        }
        rows.CloseList();
        UNIT_ASSERT_VALUES_EQUAL(statuses.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(statuses["/Root/a"], "ORANGE");
        UNIT_ASSERT_VALUES_EQUAL(statuses["/Root/b"], "GREEN");
        UNIT_ASSERT(!test.Cached()); // publish only after a fenced transaction
        test.ReplyQuery(QueryResult({Applied()}));
        UNIT_ASSERT_VALUES_EQUAL(*test.Cached(), ClusterHealthBody);
        UNIT_ASSERT(!test.Cached("another-owner"));
        test.AssertNoHttp();
    }

    Y_UNIT_TEST(ClusterSnapshotParsingRejectsMissingVerboseAndUnknownFlags) {
        TString status;
        TDatabaseHealthMap databases;
        UNIT_ASSERT(ParseClusterHealth(ParseJson(ClusterHealthBody), TInstant::Seconds(1000), status, databases));
        UNIT_ASSERT_VALUES_EQUAL(databases.size(), 2);
        for (const auto& body : {
            "{}",
            R"({"self_check_result":"GOOD"})",
            R"({"self_check_result":"UNKNOWN","database_status":[]})",
            R"({"self_check_result":"GOOD","database_status":[{"name":"/Root/a","overall":"UNKNOWN"}]})",
            R"({"self_check_result":"GOOD","database_status":[{"name":"/Root/a","overall":"GREEN"},{"name":"/Root/a","overall":"RED"}]})"}) {
            databases.clear();
            UNIT_ASSERT(!ParseClusterHealth(ParseJson(body), TInstant::Seconds(1000), status, databases));
        }
        databases.clear();
        UNIT_ASSERT(ParseClusterHealth(ParseJson(
            R"({"self_check_result":"GOOD","database_status":[{"overall":"GREEN"}]})"),
            TInstant::Seconds(1000), status, databases));
        UNIT_ASSERT(databases.empty()); // unnamed storage is not a database
    }

    Y_UNIT_TEST(OnlyAssignedClustersCanBeAcquired) {
        for (const auto& service : {"", "https://other.region"}) {
            TMonitorTest test;
            test.NextQuery();
            test.ReplyQuery(QueryResult({ClusterList({"cluster"}, {}, service)}));
            test.AssertNoQuery();
            test.AssertNoHttp();
        }
    }

    Y_UNIT_TEST(ReplicaWithoutLeaseDoesNotCollect) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster(false);
        test.AssertNoHttp();
        test.WakeAfter(TDuration::Seconds(30));
        test.AcquireCluster(); // the old owner expired in the database
        test.NextHttp();
        test.AssertNoHttp();
    }

    Y_UNIT_TEST(EachCollectorIncarnationHasADifferentOwner) {
        TString firstOwner;
        {
            TMonitorTest test;
            test.DiscoverClusters({"cluster"});
            test.AcquireCluster(false);
            firstOwner = test.Owner;
        }
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster(false);
        UNIT_ASSERT(!firstOwner.empty());
        UNIT_ASSERT(firstOwner != test.Owner);
    }

    Y_UNIT_TEST(PeriodIsFifteenSecondsFromStartAndWritesDoNotOverlap) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster();
        test.NextHttp();
        test.WakeAfter(TDuration::Seconds(5));
        test.ReplyHttp(ClusterHealthBody);
        test.NextQuery();
        test.AssertNoHttp();
        test.ReplyQuery(QueryResult({Applied()}));
        test.WakeAfter(TDuration::Seconds(9));
        test.AssertNoHttp();
        test.WakeAfter(TDuration::Seconds(1));
        test.NextHttp();
        test.AssertNoHttp();
    }

    Y_UNIT_TEST(SlowClusterDoesNotBlockOtherClustersOrDuplicateItsCheck) {
        TMonitorTest test;
        test.DiscoverClusters({"a", "b", "c"});
        TVector<NHttp::THttpOutgoingRequestPtr> requests;
        for (const auto& name : {"a", "b", "c"}) {
            test.AcquireCluster();
            test.NextHttp();
            UNIT_ASSERT_VALUES_EQUAL(test.Request->Host, name);
            requests.push_back(test.Request);
        }
        test.WakeAfter(TDuration::Seconds(15));
        test.AssertNoHttp();
        test.ReplyHttpFor(requests[1], ClusterHealthBody);
        test.NextQuery();
        test.ReplyQuery(QueryResult({Applied()}));
        test.WakeAfter(TDuration::Seconds(15));
        // Lease renewals do not wait for HC completion; a and c stay in flight.
        for (ui32 i = 0; i < 3; ++i) {
            test.AcquireCluster();
        }
        test.NextHttp();
        UNIT_ASSERT_VALUES_EQUAL(test.Request->Host, "b");
        test.AssertNoHttp();
    }

    Y_UNIT_TEST(FailedCheckKeepsPreviousSnapshotAndDoesNotWriteStatuses) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster();
        test.NextHttp();
        test.ReplyHttp(ClusterHealthBody);
        test.NextQuery();
        test.ReplyQuery(QueryResult({Applied()}));
        test.WakeAfter(TDuration::Seconds(15));
        test.NextHttp();
        test.ReplyHttp({}, "Timeout");
        test.AssertNoQuery();
        UNIT_ASSERT_VALUES_EQUAL(*test.Cached(), ClusterHealthBody);
    }

    Y_UNIT_TEST(UnknownAndMalformedResponsesNeverReplaceSnapshot) {
        for (const auto& body : {"invalid json", "{}", R"({"self_check_result":"GOOD"})"}) {
            TMonitorTest test;
            test.DiscoverClusters({"cluster"});
            test.AcquireCluster();
            test.NextHttp();
            test.ReplyHttp(body);
            test.AssertNoQuery();
            test.AssertNoHttp();
            UNIT_ASSERT(!test.Cached());
        }
    }

    Y_UNIT_TEST(FailedStatusWriteKeepsPreviousSnapshot) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster();
        test.NextHttp();
        test.ReplyHttp(ClusterHealthBody);
        test.NextQuery();
        test.ReplyQuery(QueryResult({Applied()}));
        test.WakeAfter(TDuration::Seconds(15));
        test.NextHttp();
        test.ReplyHttp(R"({"self_check_result":"GOOD","database_status":[]})");
        test.NextQuery();
        test.Runtime.Send(new IEventHandle(test.Monitor, test.Store,
            new TEvHealthQueryResult(), 0, test.QueryHandle->Cookie));
        UNIT_ASSERT_VALUES_EQUAL(*test.Cached(), ClusterHealthBody);
        test.AssertNoHttp();
    }

    Y_UNIT_TEST(FencedOutWriteIsNotPublished) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster();
        test.NextHttp();
        test.ReplyHttp(ClusterHealthBody);
        test.NextQuery();
        test.ReplyQuery(QueryResult({Applied(false)}));
        UNIT_ASSERT(!test.Cached());
        test.WakeAfter(TDuration::Seconds(15));
        test.AssertNoHttp();
    }

    Y_UNIT_TEST(LosingOwnershipDropsCacheAndRejectsLateHttpResponse) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster();
        test.NextHttp();
        test.ReplyHttp(ClusterHealthBody);
        test.NextQuery();
        test.ReplyQuery(QueryResult({Applied()}));
        test.WakeAfter(TDuration::Seconds(15));
        test.NextHttp();
        const auto late = test.Request;
        test.WakeAfter(TDuration::Seconds(15));
        test.AcquireCluster(false);
        UNIT_ASSERT(!test.Cached());
        test.ReplyHttpFor(late, ClusterHealthBody);
        test.AssertNoQuery();
        test.AssertNoHttp();
    }

    Y_UNIT_TEST(LeaseExpiryStopsCollectionEvenIfRenewalNeverReturns) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster();
        test.NextHttp();
        test.ReplyHttp(ClusterHealthBody);
        test.NextQuery();
        test.ReplyQuery(QueryResult({Applied()}));
        test.WakeAfter(TDuration::Seconds(121));
        // Registry refresh + lease renewal are in flight, but the expired
        // local lease must not permit a new HC or serving the old JSON.
        test.AssertNoHttp();
        UNIT_ASSERT(!test.Cached());
    }

    Y_UNIT_TEST(AbsoluteTimeoutIgnoresLateResponse) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster();
        test.NextHttp();
        const auto late = test.Request;
        test.WakeAfter(TDuration::Seconds(38));
        test.AcquireCluster(); // independent lease renewal
        test.WakeAfter(TDuration::Seconds(15));
        // A timed-out but still draining HTTP response must occupy its slot.
        test.AssertNoHttp();
        test.ReplyHttpFor(late, ClusterHealthBody);
        test.AssertNoQuery();
        test.AssertNoHttp();
        UNIT_ASSERT(!test.Cached());
        test.WakeAfter(TDuration::Seconds(15));
        test.NextHttp();
    }

    Y_UNIT_TEST(RegistryRemovalDiscardsLateResponse) {
        TMonitorTest test;
        test.DiscoverClusters({"cluster"});
        test.AcquireCluster();
        test.NextHttp();
        const auto late = test.Request;
        test.WakeAfter(TDuration::Seconds(61));
        test.DiscoverClusters({});
        test.ReplyHttpFor(late, ClusterHealthBody);
        UNIT_ASSERT(!test.Cached());
        test.AssertNoHttp();
    }

    Y_UNIT_TEST(FullResponseRequiresMonitoringPermissionNotJustViewer) {
        for (const auto& body : {R"({"IsViewerAllowed":true})",
                               R"({"IsViewerAllowed":true,"IsMonitoringAllowed":false})",
                               R"({"IsMonitoringAllowed":"true"})"}) {
            TStatusesTest test(true);
            test.Start({}, "/meta/healthcheck?name=cluster");
            test.ResolveCluster();
            test.NextWhoami();
            test.ReplyWhoami(body);
            test.Response("403");
            test.AssertNoQuery();
        }
    }

    Y_UNIT_TEST(FullResponseReturnsCachedBodyWithFreshnessHeaders) {
        TStatusesTest test(true);
        test.Start("Authorization: Bearer caller-token\r\n", "/meta/healthcheck?name=cluster");
        test.ResolveCluster();
        test.NextWhoami();
        test.ReplyWhoami(R"({"IsMonitoringAllowed":true})");
        test.NextQuery("ClusterHealth.db");
        test.ReplyQuery(QueryResult({Lease()}));
        auto* get = test.Runtime.GrabEdgeEvent<TEvGetCachedHealth>(test.Handle, TDuration::Seconds(1));
        UNIT_ASSERT(get);
        UNIT_ASSERT_VALUES_EQUAL(get->Cluster, "cluster");
        UNIT_ASSERT_VALUES_EQUAL(get->Owner, "owner");
        auto* cached = new TEvCachedHealth();
        cached->Body = std::make_shared<const TString>(ClusterHealthBody);
        cached->CheckedAt = test.Runtime.GetCurrentTime();
        test.Runtime.Send(new IEventHandle(test.RequestActor, test.Cache, cached));
        auto response = test.Response("200");
        UNIT_ASSERT_VALUES_EQUAL(response->Body, ClusterHealthBody);
        UNIT_ASSERT_VALUES_EQUAL(NHttp::THeaders(response->Headers)["X-Healthcheck-Checked-At"],
            test.Runtime.GetCurrentTime().ToString());
        test.AssertNoQuery();
    }

    Y_UNIT_TEST(EmptyAndExpiredFullCacheReturn503) {
        for (ui32 scenario = 0; scenario < 3; ++scenario) {
            TStatusesTest test(true);
            test.Start({}, "/meta/healthcheck?name=cluster");
            test.ResolveCluster();
            test.NextWhoami();
            test.ReplyWhoami(R"({"IsMonitoringAllowed":true})");
            test.NextQuery("ClusterHealth.db");
            test.ReplyQuery(QueryResult({Lease()}));
            UNIT_ASSERT(test.Runtime.GrabEdgeEvent<TEvGetCachedHealth>(test.Handle, TDuration::Seconds(1)));
            auto* cached = new TEvCachedHealth();
            if (scenario != 0) {
                cached->Body = std::make_shared<const TString>(ClusterHealthBody);
                cached->CheckedAt = scenario == 1
                    ? test.Runtime.GetCurrentTime() - TDuration::Seconds(120)
                    : test.Runtime.GetCurrentTime() + TDuration::Seconds(1);
            }
            test.Runtime.Send(new IEventHandle(test.RequestActor, test.Cache, cached));
            test.Response("503");
        }
    }

    Y_UNIT_TEST(FullResponseForwardsToOwnerWithoutSubstitutingCaller) {
        TStatusesTest test(true);
        test.Start("Authorization: Bearer caller-token\r\nCookie: session=user\r\n", "/meta/healthcheck?name=cluster");
        test.ResolveCluster();
        test.NextWhoami();
        test.ReplyWhoami(R"({"IsMonitoringAllowed":true})");
        test.NextQuery("ClusterHealth.db");
        test.ReplyQuery(QueryResult({Lease("https://another-pod:8789")}));
        auto* request = test.Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(test.Handle, TDuration::Seconds(1));
        UNIT_ASSERT(request);
        UNIT_ASSERT_VALUES_EQUAL(request->Request->Host, "another-pod:8789");
        UNIT_ASSERT_VALUES_EQUAL(request->Request->URL, "/meta/healthcheck?cluster_name=cluster");
        const NHttp::THeaders headers(request->Request->Headers);
        UNIT_ASSERT_VALUES_EQUAL(headers["Authorization"], "Bearer caller-token");
        UNIT_ASSERT_VALUES_EQUAL(headers["Cookie"], "session=user");
        UNIT_ASSERT_VALUES_EQUAL(headers["X-Meta-Healthcheck-Hops"], "1");
    }

    Y_UNIT_TEST(ForwardingLoopsAreBounded) {
        TStatusesTest test(true);
        test.Start("X-Meta-Healthcheck-Hops: 2\r\n", "/meta/healthcheck?name=cluster");
        test.ResolveCluster();
        test.NextWhoami();
        test.ReplyWhoami(R"({"IsMonitoringAllowed":true})");
        test.NextQuery("ClusterHealth.db");
        test.ReplyQuery(QueryResult({Lease("https://another-pod:8789")}));
        test.Response("503");
    }

    Y_UNIT_TEST(RegionalForwardingRequiresWhoamiAuthorization) {
        for (bool fullResponse : {false, true}) {
            for (bool allowed : {false, true}) {
                TStatusesTest test(fullResponse);
                test.Start("Authorization: Bearer caller-token\r\n",
                    fullResponse ? "/meta/healthcheck?name=cluster" : "/meta/database_statuses?cluster_name=cluster");
                test.NextQuery("MasterClusterExt.db");
                test.ReplyQuery(QueryResult({ClusterList({"cluster"}, {}, "https://other.region")}));
                test.NextWhoami();
                test.AssertNoQuery();
                test.ReplyWhoami(allowed ? R"({"IsViewerAllowed":true,"IsMonitoringAllowed":true})" : "{}");
                if (allowed) {
                    auto* forwarded = test.Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(
                        test.Handle, TDuration::Seconds(1));
                    UNIT_ASSERT(forwarded);
                    UNIT_ASSERT_VALUES_EQUAL(forwarded->Request->Host, "other.region");
                    UNIT_ASSERT_VALUES_EQUAL(NHttp::THeaders(forwarded->Request->Headers)["Authorization"], "Bearer caller-token");
                } else {
                    test.Response("403");
                    UNIT_ASSERT(!test.Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(
                        test.Handle, TDuration::MilliSeconds(1)));
                }
                test.AssertNoQuery();
            }
        }
    }

    Y_UNIT_TEST(ForwardedFullResponseMustStillBeFresh) {
        for (ui32 scenario = 0; scenario < 4; ++scenario) {
            TStatusesTest test(true);
            test.Start({}, "/meta/healthcheck?name=cluster");
            test.ResolveCluster();
            test.NextWhoami();
            test.ReplyWhoami(R"({"IsMonitoringAllowed":true})");
            test.NextQuery("ClusterHealth.db");
            test.ReplyQuery(QueryResult({Lease("https://another-pod:8789")}));
            auto* forwarded = test.Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingRequest>(
                test.Handle, TDuration::Seconds(1));
            UNIT_ASSERT(forwarded);
            const auto request = forwarded->Request;
            TStringBuilder responseText;
            responseText << "HTTP/1.1 200 OK\r\nContent-Length: " << ClusterHealthBody.size() << "\r\n";
            if (scenario != 1) {
                const auto checkedAt = scenario == 2
                    ? test.Runtime.GetCurrentTime() - TDuration::Seconds(119)
                    : test.Runtime.GetCurrentTime() + TDuration::Seconds(scenario == 3 ? 10 : 0);
                responseText << "X-Healthcheck-Checked-At: " << checkedAt.ToString() << "\r\n"
                    << "X-Healthcheck-Expires-At: " << (checkedAt + TDuration::Seconds(120)).ToString() << "\r\n";
            }
            responseText << "\r\n" << ClusterHealthBody;
            NHttp::THttpIncomingResponsePtr response = new NHttp::THttpIncomingResponse(request);
            EatWholeString(response, responseText);
            test.Runtime.AdvanceCurrentTime(TDuration::Seconds(2));
            test.Runtime.Send(new IEventHandle(test.RequestActor, test.Http,
                new NHttp::TEvHttpProxy::TEvHttpIncomingResponse(request, response)));
            auto result = test.Response(scenario == 0 ? "200" : scenario == 1 ? "502" : "503");
            if (scenario == 0) {
                UNIT_ASSERT_VALUES_EQUAL(result->Body, ClusterHealthBody);
            }
            test.AssertNoQuery();
        }
    }

    Y_UNIT_TEST(HealthcheckConfigurationDefaultsAndValidation) {
        NMvp::NMeta::TMetaConfig config;
        config.SetMetaApiEndpoint("grpc://meta:2135");
        config.SetMetaDatabase("/meta");
        auto settings = BuildMetaSettings(config, NMvp::yandex_v2);
        UNIT_ASSERT(!settings.Healthcheck.Enabled);
        UNIT_ASSERT_VALUES_EQUAL(settings.Healthcheck.StatusTtl, TDuration::Seconds(120));
        UNIT_ASSERT_VALUES_EQUAL(settings.Healthcheck.CheckPeriod, TDuration::Seconds(15));
        config.MutableHealthcheck()->SetEnabled(true);
        UNIT_ASSERT_EXCEPTION_CONTAINS(BuildMetaSettings(config, NMvp::yandex_v2), yexception, "requires service_endpoint");
        config.SetServiceEndpoint("https://meta.region");
        UNIT_ASSERT_EXCEPTION_CONTAINS(BuildMetaSettings(config, NMvp::yandex_v2), yexception, "requires meta_database_token_name");
        config.MutableHealthcheck()->SetCollect(false);
        UNIT_ASSERT_NO_EXCEPTION(BuildMetaSettings(config, NMvp::yandex_v2));
        config.MutableHealthcheck()->SetCheckPeriodSeconds(0);
        UNIT_ASSERT_EXCEPTION_CONTAINS(BuildMetaSettings(config, NMvp::yandex_v2), yexception, "must be positive");
    }
}
