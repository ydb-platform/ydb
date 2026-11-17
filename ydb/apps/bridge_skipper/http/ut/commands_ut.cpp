#include <ydb/apps/bridge_skipper/http/http.h>
#include <ydb/apps/bridge_skipper/runtime/events.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

#include <cstring>
#include <initializer_list>
#include <tuple>

namespace NBridgeSkipper {
namespace {

struct THttpFixture {
    NActors::TTestActorRuntimeBase Runtime;
    std::shared_ptr<TSnapshotStore> Store = std::make_shared<TSnapshotStore>();
    std::shared_ptr<TCommandAdmission> Admission = std::make_shared<TCommandAdmission>();
    NActors::TActorId Http;
    NActors::TActorId Sender;
    NActors::TActorId Controller;

    explicit THttpFixture(const THttpConfig& config = {}) {
        Runtime.Initialize();
        Store->Publish(std::make_shared<TSkipperSnapshot>());
        const auto proxy = Runtime.AllocateEdgeActor();
        Sender = Runtime.AllocateEdgeActor();
        Controller = Runtime.AllocateEdgeActor();
        Http = Runtime.Register(CreateHttpActor(Store, proxy, config, Admission));
        Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvRegisterHandler>(proxy);
    }

    void Request(TStringBuf method, TStringBuf path, TStringBuf headers, TStringBuf body = {}, TStringBuf type = "application/json") {
        const TString wire = TString(method) + " " + TString(path) + " HTTP/1.1\r\n" + TString(headers) + "Content-Type: "
            + TString(type) + "\r\nContent-Length: " + ::ToString(body.size()) + "\r\n\r\n" + TString(body);
        NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
        request->EnsureEnoughSpaceAvailable(wire.size());
        memcpy(request->Pos(), wire.data(), wire.size());
        request->Advance(wire.size());
        Runtime.Send(new NActors::IEventHandle(Http, Sender,
            new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request), 0, 39), 0, true);
    }

    void Post(TStringBuf path, TStringBuf body, TStringBuf type = "application/json") {
        Request("POST", path, "Host: localhost:8080\r\n", body, type);
    }

    TString Response(TStringBuf status) {
        const auto response = Runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(Sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, 39);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Response->Status, status);
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Response->Headers, "Cache-Control: no-store");
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Response->Headers, "X-Frame-Options: DENY");
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Response->Headers, "Content-Security-Policy: frame-ancestors 'none'");
        UNIT_ASSERT(!response->Get()->Response->Headers.Contains("Access-Control-Allow-Origin"));
        return TString(response->Get()->Response->Body);
    }
};

const TString ModeBody = R"({"mode":"observe","expected_boot_id":"boot","expected_mode_revision":"1","request_id":"off"})";

struct THttpSecurityFixture : THttpFixture {
    ui32 Forwarded = 0;
    NActors::TTestActorRuntimeBase::TEventObserverHolder Commands;

    explicit THttpSecurityFixture(const THttpConfig& config = {})
        : THttpFixture(config)
    {
        Admission->Open(Controller);
        Commands = Runtime.AddObserver<TEvSkipper::TEvCommand>([&](TEvSkipper::TEvCommand::TPtr& event) {
            UNIT_ASSERT_VALUES_EQUAL(event->Recipient, Controller);
            ++Forwarded;
            TCommandResult result;
            result.Body["accepted"] = true;
            Runtime.Send(new NActors::IEventHandle(event->Sender, Controller,
                new TEvSkipper::TEvCommandResult(std::move(result)), 0, event->Cookie));
            event.Reset();
        });
    }
};

THttpConfig HttpConfig(TStringBuf listen, std::initializer_list<TString> trusted = {}) {
    THttpConfig config;
    config.Listen = TString(listen);
    config.TrustedAuthorities.assign(trusted.begin(), trusted.end());
    return config;
}

} // namespace

Y_UNIT_TEST_SUITE(THttpCommands) {
    Y_UNIT_TEST(LogSearchMatchesTheExportedRecordEncoding) {
        TSkipperSnapshot snapshot;
        snapshot.Diagnostics = std::make_shared<TDiagnosticStore>("boot");
        snapshot.Diagnostics->Log("info", "message", TInstant::Seconds(1));
        const auto exported = RouteHttpRequest("GET", "/api/logs/export", snapshot,
            TMonotonic::Seconds(1), TInstant::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(exported.Status, 200);
        const TStringBuf record = TStringBuf(exported.Body).After('\n').Before('\n');
        // A compact exported prefix spans timestamp and cursor fields. The
        // browser searches the same record received from the API.
        const auto end = record.find(",\"level\"");
        UNIT_ASSERT(end != TStringBuf::npos);
        const TString query(record.SubStr(0, end));
        const auto filtered = snapshot.Diagnostics->ReadLogs({}, {}, query);
        UNIT_ASSERT_VALUES_EQUAL(filtered["rows"].GetArray().size(), 1);
    }

    Y_UNIT_TEST(ParserRejectsUnsupportedActionsBodiesAndNonDecimalRevisions) {
        THttpResponse error;
        UNIT_ASSERT(ParseCommand("/api/mode", "application/json; charset=utf-8", ModeBody, error));
        for (const TStringBuf body : {R"({"action":"remove","snapshot_revision":"b"})",
                R"({"action":"rejoin","snapshot_revision":"b"})", "[]", "{}", "{broken"})
        {
            UNIT_ASSERT(!ParseCommand("/api/plans", "application/json", body, error));
            UNIT_ASSERT_VALUES_EQUAL(error.Status, 400);
        }
        const auto emptyPile = ParseCommand("/api/plans", "application/json",
            R"({"action":"rejoin","target_pile":"","snapshot_revision":"b"})", error);
        UNIT_ASSERT(emptyPile && emptyPile->Target && emptyPile->Target->empty());
        UNIT_ASSERT(!ParseCommand("/api/mode", "text/plain", ModeBody, error));
        UNIT_ASSERT(!ParseCommand("/api/mode", "application/json", TString(65537, ' '), error));
        UNIT_ASSERT_VALUES_EQUAL(error.Status, 413);
        UNIT_ASSERT(!ParseCommand("/api/mode", "application/json",
            R"({"mode":"auto","expected_boot_id":"b","expected_mode_revision":1,"request_id":"x","reviewed_snapshot_revision":"b"})", error));
    }

    Y_UNIT_TEST(HttpAuthorityAndOriginAreCheckedBeforeAdmissionOrCommandParsing) {
        THttpSecurityFixture f;
        for (const auto& [path, body] : {
                std::pair<TStringBuf, TStringBuf>{"/api/mode", ModeBody},
                std::pair<TStringBuf, TStringBuf>{"/api/plans", R"({"action":"failover","snapshot_revision":"boot:1"})"},
                std::pair<TStringBuf, TStringBuf>{"/api/operations", R"({"plan_id":"boot:1","request_id":"authority-check"})"}})
        {
            f.Request("POST", path, "Host: evil.example:8080\r\nOrigin: http://evil.example:8080\r\n", body);
            UNIT_ASSERT_STRING_CONTAINS(f.Response("403"), "HOST_NOT_ALLOWED");
        }
        f.Request("GET", "/api/state", "Host: evil.example:8080\r\n");
        UNIT_ASSERT_STRING_CONTAINS(f.Response("403"), "HOST_NOT_ALLOWED");
        UNIT_ASSERT_VALUES_EQUAL(f.Forwarded, 0);

        THttpFixture closed;
        closed.Request("POST", "/api/mode", "Host: evil.example:8080\r\n", ModeBody);
        UNIT_ASSERT_STRING_CONTAINS(closed.Response("403"), "HOST_NOT_ALLOWED");
        closed.Request("POST", "/api/mode", "Host: localhost:8080\r\nOrigin: http://evil.example:8080\r\n", "not-json");
        UNIT_ASSERT_STRING_CONTAINS(closed.Response("403"), "ORIGIN_NOT_ALLOWED");
    }

    Y_UNIT_TEST(HttpAllowsLocalCliAndSameOriginBrowserAuthorities) {
        THttpSecurityFixture f;
        for (const auto headers : {
                "Host: localhost:8080\r\n",
                "Host: 127.0.0.1:8080\r\nOrigin: http://127.0.0.1:8080\r\n",
                "Host: LOCALHOST:08080\r\nOrigin: https://localhost:8080\r\n",
                "Host: [0:0:0:0:0:0:0:1]:8080\r\nOrigin: http://[::1]:8080\r\n",
                "Host: \tlocalhost:8080 \t\r\nOrigin: \thttp://localhost:8080 \t\r\n"})
        {
            f.Request("POST", "/api/mode", headers, ModeBody);
            UNIT_ASSERT_STRING_CONTAINS(f.Response("200"), "accepted");
        }
        UNIT_ASSERT_VALUES_EQUAL(f.Forwarded, 5);
    }

    Y_UNIT_TEST(HttpRejectsMissingDuplicateMalformedHostsAndOrigins) {
        THttpSecurityFixture f;
        for (const auto headers : {
                "",
                "Host: localhost:8080\r\nHost: evil.example:8080\r\n",
                "Host: localhost:8080\r\nhost: localhost:8080\r\n",
                "Host: localhost:8081\r\n",
                "Host: localhost\r\n",
                "Host: user@localhost:8080\r\n",
                "Host: localhost:8080/path\r\n",
                "Host: localhost:8080,evil.example\r\n",
                "Host: ::1:8080\r\n"})
        {
            f.Request("POST", "/api/mode", headers, ModeBody);
            UNIT_ASSERT_STRING_CONTAINS(f.Response("403"), "HOST_NOT_ALLOWED");
        }
        for (const auto origin : {"", "null", "http://evil.example:8080", "http://127.0.0.1:8080", "http://localhost:8081",
                "file://localhost:8080", "http://localhost:8080/path", "http://user@localhost:8080", "http://localhost:8080#fragment",
                "http://localhost:8080 http://evil.example:8080", "http://localhost:8080,http://evil.example:8080"})
        {
            f.Request("POST", "/api/mode", TString("Host: localhost:8080\r\nOrigin: ") + origin + "\r\n", ModeBody);
            UNIT_ASSERT_STRING_CONTAINS(f.Response("403"), "ORIGIN_NOT_ALLOWED");
        }
        f.Request("POST", "/api/mode", "Host: localhost:8080\r\nOrigin: http://localhost:8080\r\nOrigin: http://localhost:8080\r\n", ModeBody);
        UNIT_ASSERT_STRING_CONTAINS(f.Response("403"), "ORIGIN_NOT_ALLOWED");
        f.Request("POST", "/api/mode", "Host: evil.example:8080\r\nForwarded: host=localhost:8080;proto=https\r\nX-Forwarded-Host: localhost:8080\r\n", ModeBody);
        UNIT_ASSERT_STRING_CONTAINS(f.Response("403"), "HOST_NOT_ALLOWED");
        UNIT_ASSERT_VALUES_EQUAL(f.Forwarded, 0);
    }

    Y_UNIT_TEST(HttpPreservesConcreteDnsBindAndExplicitTlsProxyAuthorities) {
        THttpSecurityFixture f(HttpConfig("admin.example:8080", {"proxy.example", "proxy.example:8443", "[2001:db8::9]:8443", "192.0.2.10:8080"}));
        for (const auto headers : {
                "Host: ADMIN.EXAMPLE:8080\r\n",
                "Host: 192.0.2.10:8080\r\n",
                "Host: proxy.example\r\nOrigin: https://proxy.example\r\n",
                "Host: proxy.example\r\nOrigin: https://proxy.example:443\r\n",
                "Host: proxy.example:8443\r\nOrigin: https://proxy.example:8443\r\n",
                "Host: [2001:0db8:0:0:0:0:0:9]:8443\r\nOrigin: https://[2001:db8::9]:8443\r\n"})
        {
            f.Request("POST", "/api/mode", headers, ModeBody);
            UNIT_ASSERT_STRING_CONTAINS(f.Response("200"), "accepted");
        }
        f.Request("POST", "/api/mode", "Host: admin.example:8080\r\nOrigin: https://proxy.example:8443\r\n", ModeBody);
        UNIT_ASSERT_STRING_CONTAINS(f.Response("403"), "ORIGIN_NOT_ALLOWED");
        f.Request("GET", "/api/state", "Host: unconfigured.example:8080\r\n");
        UNIT_ASSERT_STRING_CONTAINS(f.Response("403"), "HOST_NOT_ALLOWED");
        UNIT_ASSERT_VALUES_EQUAL(f.Forwarded, 6);
    }

    Y_UNIT_TEST(HttpWildcardBindsAllowNumericAuthoritiesAtTheirListenPort) {
        for (const auto listen : {"0.0.0.0:8080", "[::]:8080"}) {
            THttpSecurityFixture f(HttpConfig(listen));
            for (const auto host : {"192.0.2.10:8080", "[2001:db8::1]:8080", "localhost:8080", "127.0.0.1:8080", "[::1]:8080"}) {
                f.Request("GET", "/api/state", TString("Host: ") + host + "\r\n");
                f.Response("200");
            }
            for (const auto host : {"evil.example:8080", "192.0.2.10:8081", "[2001:db8::1]:8081", "999.1.2.3:8080"}) {
                f.Request("GET", "/api/state", TString("Host: ") + host + "\r\n");
                UNIT_ASSERT_STRING_CONTAINS(f.Response("403"), "HOST_NOT_ALLOWED");
            }
        }
        THttpSecurityFixture concrete(HttpConfig("192.0.2.10:8080"));
        concrete.Request("GET", "/api/state", "Host: 192.0.2.10:8080\r\n");
        concrete.Response("200");
        concrete.Request("GET", "/api/state", "Host: 192.0.2.11:8080\r\n");
        UNIT_ASSERT_STRING_CONTAINS(concrete.Response("403"), "HOST_NOT_ALLOWED");
        THttpSecurityFixture ipv6(HttpConfig("[2001:0db8:0:0:0:0:0:10]:8080"));
        ipv6.Request("GET", "/api/state", "Host: [2001:db8::10]:8080\r\n");
        ipv6.Response("200");
        ipv6.Request("GET", "/api/state", "Host: [2001:db8::11]:8080\r\n");
        UNIT_ASSERT_STRING_CONTAINS(ipv6.Response("403"), "HOST_NOT_ALLOWED");
    }

    Y_UNIT_TEST(HttpDefaultPortsAndFramingHeadersCoverReadAndErrorResponses) {
        for (const auto listen : {"127.0.0.1:80", "127.0.0.1:443"}) {
            THttpSecurityFixture f(HttpConfig(listen));
            f.Request("GET", "/api/state", "Host: localhost\r\n");
            f.Response("200");
        }
        THttpFixture f;
        for (const auto& [method, path, status] : {
                std::tuple{"GET", "/", "200"},
                std::tuple{"HEAD", "/api/state", "200"},
                std::tuple{"GET", "/absent", "404"},
                std::tuple{"POST", "/absent", "405"}})
        {
            f.Request(method, path, "Host: localhost:8080\r\n");
            const auto body = f.Response(status);
            if (TStringBuf(method) == "HEAD") {
                UNIT_ASSERT(body.empty());
            }
        }
    }

    Y_UNIT_TEST(AdmissionAndSyntaxFailureNeverForwardCommands) {
        THttpFixture f;
        f.Post("/api/mode", ModeBody);
        UNIT_ASSERT_STRING_CONTAINS(f.Response("503"), "CONTROLLER_UNAVAILABLE");
        f.Admission->Open(f.Controller);
        f.Post("/api/plans", R"({"action":"remove","snapshot_revision":"b"})");
        UNIT_ASSERT_STRING_CONTAINS(f.Response("400"), "INVALID_COMMAND");
        f.Admission->Close();
        f.Post("/api/mode", ModeBody);
        UNIT_ASSERT_STRING_CONTAINS(f.Response("503"), "CONTROLLER_UNAVAILABLE");
    }

    Y_UNIT_TEST(ControllerRepliesPreserveCorrelationAndInternalErrorDetails) {
        THttpFixture f;
        f.Admission->Open(f.Controller);
        f.Post("/api/mode", ModeBody);
        const auto command = f.Runtime.GrabEdgeEvent<TEvSkipper::TEvCommand>(f.Controller);
        UNIT_ASSERT(command->Get()->Command.Mode == EMode::Observe);
        UNIT_ASSERT_VALUES_EQUAL(command->Cookie, 1);
        TCommandResult result;
        result.Status = 409;
        result.Body["error"] = "REJECTED_AT_GENERATION";
        result.Body["status"] = "INTERNAL_ERROR";
        f.Runtime.Send(new NActors::IEventHandle(command->Sender, f.Controller,
            new TEvSkipper::TEvCommandResult(result), 0, command->Cookie), 0, true);
        UNIT_ASSERT_STRING_CONTAINS(f.Response("409"), "INTERNAL_ERROR");
    }

    Y_UNIT_TEST(UndeliveredAndTimeoutHaveDistinctOutcomeContracts) {
        THttpFixture f;
        f.Admission->Open(f.Controller);
        f.Post("/api/mode", ModeBody);
        const auto command = f.Runtime.GrabEdgeEvent<TEvSkipper::TEvCommand>(f.Controller);
        f.Runtime.Send(new NActors::IEventHandle(command->Sender, {}, new NActors::TEvents::TEvWakeup()), 0, true);
        UNIT_ASSERT_STRING_CONTAINS(f.Response("504"), "COMMAND_OUTCOME_UNKNOWN");
        // Timeout ends the HTTP actor only, leaving the original command valid.
        TCommandResult result;
        f.Runtime.Send(new NActors::IEventHandle(command->Sender, f.Controller,
            new TEvSkipper::TEvCommandResult(result), 0, command->Cookie), 0, true);
        const auto missing = f.Runtime.Register(CreateHttpActor(f.Store, f.Runtime.AllocateEdgeActor(), THttpConfig{}));
        Y_UNUSED(missing);
        f.Admission->Open(NActors::TActorId(0, "missing"));
        f.Post("/api/mode", ModeBody);
        UNIT_ASSERT_STRING_CONTAINS(f.Response("503"), "CONTROLLER_UNAVAILABLE");
    }
}

} // namespace NBridgeSkipper
