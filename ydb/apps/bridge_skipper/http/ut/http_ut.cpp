#include "fixtures.h"

#include <ydb/apps/bridge_skipper/http/http.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>
#include <util/string/split.h>

#include <algorithm>
#include <cstring>
#include <tuple>

namespace NBridgeSkipper {
namespace {

NJson::TJsonValue Json(TStringBuf body) {
    NJson::TJsonValue value;
    NJson::ReadJsonTree(body, &value, true);
    return value;
}

TSkipperSnapshot Startup() {
    TSkipperSnapshot snapshot;
    snapshot.ClusterId = "cluster";
    snapshot.Selection.Blocker = EBlockerReason::NoView;
    snapshot.Decision.Kind = EDecisionKind::Blocked;
    snapshot.Decision.Blocker = EBlockerReason::NoView;
    snapshot.Decision.Reason = "NO_VIEW";
    return snapshot;
}

TSkipperSnapshot Full() {
    auto snapshot = Startup();
    snapshot.LastTickAt = TMonotonic::MilliSeconds(99750);
    TClusterView view;
    view.Generation = 9007199254740993ull;
    view.Piles = {{"", EPileState::Primary}, {"b", EPileState::Synchronized}};
    view.Pending = TPendingOperation{"ADD", TString(""), "waiting"};
    view.ConversionInProgress = true;
    view.Endpoint = "grpc://a:2135";
    view.SentAt = TMonotonic::MilliSeconds(98500);
    snapshot.Selection.View = view;
    snapshot.Selection.Blocker.reset();
    snapshot.GenerationHighWater = view.Generation + 1;
    snapshot.PersistentState.GenerationHighWater = view.Generation;
    TEndpointSnapshot endpoint;
    endpoint.Uri = view.Endpoint;
    endpoint.Pile = "";
    endpoint.Identity = EEndpointIdentity::Verified;
    endpoint.View = view;
    TSelfCheckReply reply;
    reply.Endpoint = endpoint.Uri;
    reply.ResponderPile = "";
    reply.SentAt = TMonotonic::Seconds(98);
    reply.Classification.Truncated = true;
    reply.Classification.SelfCheckResult = "DEGRADED";
    reply.Classification.Notes = {"PILE_BOARD RED pile=b: <redacted>"};
    reply.Classification.OmittedNotes = 3;
    endpoint.SelfCheck = reply;
    snapshot.Endpoints.push_back(endpoint);
    endpoint = {};
    endpoint.Uri = "grpc://ignored:2135";
    endpoint.Pile = "old";
    endpoint.Identity = EEndpointIdentity::Mismatch;
    endpoint.Ignored = true;
    endpoint.LastError = "ENDPOINT_PILE_MISMATCH";
    // Unsorted on purpose: the document sorts endpoints by URI.
    snapshot.Endpoints.insert(snapshot.Endpoints.begin(), endpoint);
    auto& primary = snapshot.Piles[""];
    primary.Verdict = {EPileVerdict::Healthy, {"BSC_CLEAN"}, false, true, TMonotonic::Seconds(98)};
    TConnectivity report;
    report.VantagePile = "";
    report.ReporterNodeId = 7;
    report.Generation = view.Generation;
    report.SentAt = TMonotonic::Seconds(99);
    report.Piles[""] = {3, 3, true, true, true};
    primary.Connectivity.push_back(report);
    auto& lost = snapshot.Piles["b"];
    lost.Verdict = {EPileVerdict::Lost, {"NO_NODE_QUORUM"}, true, false, TMonotonic::Seconds(99)};
    lost.DownSince = TMonotonic::Seconds(70);
    lost.Confirmed = true;
    snapshot.Decision.Kind = EDecisionKind::Ready;
    snapshot.Decision.Blocker.reset();
    snapshot.Decision.Reason = "confirmed failure";
    snapshot.Decision.Plan = TPlan{view.Generation, {{"b", EPileState::Disconnected}}, view.Endpoint};
    snapshot.Decision.CliCommand = "guarded CLI command";
    snapshot.PersistentState.PendingMutation = TPendingMutation{view.Generation, snapshot.Decision.Plan->Update};
    snapshot.StateWriteError = "fsync failed";
    TMutationInvocation invocation;
    invocation.Id = 1;
    invocation.SentAt = TMonotonic::Seconds(90);
    invocation.Plan = *snapshot.Decision.Plan;
    invocation.Status = "CLIENT_DEADLINE_EXCEEDED";
    invocation.Outcome = EMutationOutcome::Ambiguous;
    snapshot.LastMutation = invocation;
    return snapshot;
}

// The encoder preserves label order and emits optional trailing commas. Match
// series by label values so these tests do not impose its whitespace format.
double Sample(
    TStringBuf text,
    TStringBuf name,
    std::initializer_list<std::pair<TStringBuf, TStringBuf>> labels = {})
{
    for (const auto line : StringSplitter(text).Split('\n').SkipEmpty()) {
        const TStringBuf value = line.Token();
        if (!value.StartsWith(name) || value.size() <= name.size()
            || (value[name.size()] != ' ' && value[name.size()] != '{'))
        {
            continue;
        }
        bool matches = true;
        for (const auto& [key, label] : labels) {
            matches = matches && value.Contains(TString(key) + "=\"" + TString(label) + "\"");
        }
        if (matches) {
            return FromString<double>(value.SubStr(value.rfind(' ') + 1));
        }
    }
    UNIT_FAIL("Missing series " << name);
    return 0;
}

} // namespace

Y_UNIT_TEST_SUITE(TReadOnlyHttp) {
    Y_UNIT_TEST(StateJsonGoldenAndReadTimeAges) {
        const auto snapshot = Full();
        const auto rendered = RenderStateJson(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(rendered, NJson::WriteJson(Json(NFixtures::State), false, true));
        // Ages are measured at read time from the send times.
        const auto later = Json(RenderStateJson(snapshot, TMonotonic::Seconds(105), TInstant::Seconds(105)));
        UNIT_ASSERT_VALUES_EQUAL(later["loop_age_ms"].GetUInteger(), 5250);
        UNIT_ASSERT_VALUES_EQUAL(later["view"]["age_ms"].GetUInteger(), 6500);
        UNIT_ASSERT_VALUES_EQUAL(later["endpoints"][0]["selfcheck_age_ms"].GetUInteger(), 7000);
        // B5: stamps are monotonic; the wall clock only shifts the displayed times.
        const auto shifted = Json(RenderStateJson(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(1000)));
        UNIT_ASSERT_VALUES_EQUAL(shifted["now"].GetString(), TInstant::Seconds(1000).ToString());
        UNIT_ASSERT_VALUES_EQUAL(shifted["piles"][1]["down_since"].GetString(), TInstant::Seconds(970).ToString());
        UNIT_ASSERT_VALUES_EQUAL(shifted["last_mutation"]["sent_at"].GetString(), TInstant::Seconds(990).ToString());
        UNIT_ASSERT_VALUES_EQUAL(shifted["loop_age_ms"].GetUInteger(), 250);
        const auto startup = Json(RenderStateJson(Startup(), TMonotonic::Seconds(100), TInstant::Seconds(100)));
        UNIT_ASSERT(startup["view"].IsNull());
        UNIT_ASSERT(startup["loop_age_ms"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(startup["decision"]["blocker"].GetString(), "NO_VIEW");
    }

    Y_UNIT_TEST(MethodsAreRejectedBeforeRouting) {
        const auto snapshot = Full();
        for (const TStringBuf method : {"POST", "PUT", "DELETE", "PATCH", "OPTIONS", "get"}) {
            for (const TStringBuf path : {"/", "/api/state", "/metrics", "/api/mode", "/api/plans", "/unknown"}) {
                UNIT_ASSERT_VALUES_EQUAL(RouteHttpRequest(method, path, snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Status, 405);
            }
        }
        for (const TStringBuf path : {"/", "/api/operations", "/api/audit", "/api/logs", "/mon/state", "/metrics/"}) {
            UNIT_ASSERT_VALUES_EQUAL(RouteHttpRequest("GET", path, snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Status, 404);
        }
        const auto state = RouteHttpRequest("GET", "/api/state", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(state.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(state.ContentType, "application/json; charset=utf-8");
        UNIT_ASSERT_VALUES_EQUAL(state.Body, RenderStateJson(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)));
        const auto head = RouteHttpRequest("HEAD", "/api/state", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(head.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(head.ContentType, "application/json; charset=utf-8");
        UNIT_ASSERT(head.Body.empty());
        UNIT_ASSERT_VALUES_EQUAL(RouteHttpRequest("HEAD", "/unknown", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Status, 404);
        UNIT_ASSERT(RouteHttpRequest("HEAD", "/unknown", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Body.empty());
        const auto query = RouteHttpRequest("GET", "/api/state?x=1", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(query.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(query.Body, state.Body);
        UNIT_ASSERT_VALUES_EQUAL(RouteHttpRequest("GET", "/metrics?format=prometheus", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(RouteHttpRequest("GET", "/?x=1", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Status, 404);
        const auto metrics = RouteHttpRequest("GET", "/metrics", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(metrics.Status, 200);
        UNIT_ASSERT_VALUES_EQUAL(metrics.ContentType, "text/plain; version=0.0.4; charset=utf-8");
    }

    // The actor adds the headers, so no-store is checked through it.
    Y_UNIT_TEST(ActorRegistersCatchAllAndResponsesAreNoStore) {
        NActors::TTestActorRuntimeBase runtime(1, true);
        runtime.Initialize();
        auto store = std::make_shared<TSnapshotStore>();
        store->Publish(std::make_shared<const TSkipperSnapshot>(Startup()));
        const auto proxy = runtime.AllocateEdgeActor();
        const auto sender = runtime.AllocateEdgeActor();
        const auto actor = runtime.Register(CreateHttpActor(store, proxy));
        TAutoPtr<NActors::IEventHandle> registration;
        const auto* registered = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvRegisterHandler>(registration);
        UNIT_ASSERT_VALUES_EQUAL(registered->Path, "/");
        UNIT_ASSERT_VALUES_EQUAL(registered->Handler, actor);
        for (const auto& [method, path, status] : {
                std::tuple{"GET", "/api/state", "200"},
                std::tuple{"POST", "/unknown", "405"}})
        {
            const TString wire = TString(method) + " " + path + " HTTP/1.1\r\nHost: localhost\r\n\r\n";
            NHttp::THttpIncomingRequestPtr request = new NHttp::THttpIncomingRequest();
            request->EnsureEnoughSpaceAvailable(wire.size());
            memcpy(request->Pos(), wire.data(), wire.size());
            request->Advance(wire.size());
            runtime.Send(new NActors::IEventHandle(actor, sender,
                new NHttp::TEvHttpProxy::TEvHttpIncomingRequest(request)), 0, true);
            TAutoPtr<NActors::IEventHandle> event;
            const auto* response = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvHttpOutgoingResponse>(event);
            UNIT_ASSERT_VALUES_EQUAL(response->Response->Status, status);
            UNIT_ASSERT_STRING_CONTAINS(response->Response->Headers, "Cache-Control: no-store");
        }
    }
}

Y_UNIT_TEST_SUITE(TSnapshotMetrics) {
    Y_UNIT_TEST(ActualCountersAreScrapeInvariant) {
        auto snapshot = Full();
        snapshot.Counters.RpcTotal = {
            {{EYdbMethod::PlainState, "SUCCESS"}, 5},
            {{EYdbMethod::ConnectivityState, "UNAVAILABLE"}, 2},
            {{EYdbMethod::SelfCheck, "SDK_START_ERROR"}, 3},
            {{EYdbMethod::SelfCheck, "SDK_RESULT_ERROR"}, 4},
            {{EYdbMethod::SelfCheck, "INVALID_RPC_REQUEST"}, 6},
            {{EYdbMethod::Update, "TRANSPORT_UNAVAILABLE"}, 7},
            {{EYdbMethod::Update, "OTHER"}, 8}};
        snapshot.Counters.MutationTotal = {{EMutationOutcome::Success, 2}, {EMutationOutcome::GenerationMoved, 3},
            {EMutationOutcome::NotAppliedHere, 4}, {EMutationOutcome::Rejected, 5}, {EMutationOutcome::Ambiguous, 6}};
        snapshot.Counters.UnexplainedStateChanges = 12;
        for (unsigned scrape = 0; scrape < 3; ++scrape) {
            const auto metrics = RenderMetrics(snapshot, TMonotonic::Seconds(100 + scrape));
            UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_rpc_total", {{"method", "get_cluster_state"}, {"status", "SUCCESS"}}), 5);
            UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_rpc_total", {{"method", "get_cluster_state_connectivity"}, {"status", "UNAVAILABLE"}}), 2);
            UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_rpc_total", {{"method", "self_check"}, {"status", "SDK_START_ERROR"}}), 3);
            UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_rpc_total", {{"method", "self_check"}, {"status", "SDK_RESULT_ERROR"}}), 4);
            UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_rpc_total", {{"method", "self_check"}, {"status", "INVALID_RPC_REQUEST"}}), 6);
            UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_rpc_total", {{"method", "update_cluster_state"}, {"status", "TRANSPORT_UNAVAILABLE"}}), 7);
            UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_rpc_total", {{"method", "update_cluster_state"}, {"status", "OTHER"}}), 8);
            for (const auto& [outcome, count] : snapshot.Counters.MutationTotal) {
                UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_mutation_total", {{"outcome", ToString(outcome)}}), count);
            }
            UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_unexplained_state_change_total"), 12);
            UNIT_ASSERT(!metrics.Contains("grpc://"));
        }
        UNIT_ASSERT_VALUES_EQUAL(snapshot.Counters.RpcTotal.at({EYdbMethod::Update, "OTHER"}), 8);
    }

    Y_UNIT_TEST(FractionalAgesAdvanceAndMissingViewGaugesAreOmitted) {
        using ::ToString;

        auto snapshot = Full();
        const auto first = RenderMetrics(snapshot, TMonotonic::Seconds(100));
        UNIT_ASSERT_DOUBLES_EQUAL(Sample(first, "skipper_loop_age_seconds"), 0.25, 0.000001);
        UNIT_ASSERT_DOUBLES_EQUAL(Sample(first, "skipper_cluster_state_age_seconds"), 1.5, 0.000001);
        const auto later = RenderMetrics(snapshot, TMonotonic::Seconds(102));
        UNIT_ASSERT_DOUBLES_EQUAL(Sample(later, "skipper_loop_age_seconds"), 2.25, 0.000001);
        UNIT_ASSERT_DOUBLES_EQUAL(Sample(later, "skipper_cluster_state_age_seconds"), 3.5, 0.000001);
        const auto earlier = RenderMetrics(snapshot, TMonotonic::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(Sample(earlier, "skipper_loop_age_seconds"), 0);
        snapshot.Selection.View.reset();
        const auto missing = RenderMetrics(snapshot, TMonotonic::Seconds(100));
        UNIT_ASSERT(!missing.Contains("skipper_cluster_state_age_seconds"));
        UNIT_ASSERT(!missing.Contains("skipper_pile_verdict"));
        snapshot.LastTickAt = {};
        UNIT_ASSERT(!RenderMetrics(snapshot, TMonotonic::Seconds(100)).Contains("skipper_loop_age_seconds"));
    }

    Y_UNIT_TEST(OneHotSeriesTrackVerdictsAndRemovedPilesDisappear) {
        auto snapshot = Full();
        auto metrics = RenderMetrics(snapshot, TMonotonic::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_pile_verdict", {{"pile", "b"}, {"verdict", "LOST"}}), 1);
        UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_pile_verdict", {{"pile", "b"}, {"verdict", "HEALTHY"}}), 0);
        UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_decision", {{"kind", "READY"}}), 1);
        UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_pending_mutation"), 1);
        UNIT_ASSERT(!metrics.Contains("pile=\"old\""));
        snapshot.Piles["b"].Verdict.Verdict = EPileVerdict::Unknown;
        snapshot.Decision.Kind = EDecisionKind::Blocked;
        snapshot.Decision.Blocker = EBlockerReason::StateWriteFailed;
        metrics = RenderMetrics(snapshot, TMonotonic::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_pile_verdict", {{"pile", "b"}, {"verdict", "LOST"}}), 0);
        UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_pile_verdict", {{"pile", "b"}, {"verdict", "UNKNOWN"}}), 1);
        UNIT_ASSERT_VALUES_EQUAL(Sample(metrics, "skipper_blocked", {{"reason", "STATE_WRITE_FAILED"}}), 1);
        snapshot.Selection.View->Piles.erase("b");
        metrics = RenderMetrics(snapshot, TMonotonic::Seconds(100));
        UNIT_ASSERT(!metrics.Contains("pile=\"b\""));
    }

    Y_UNIT_TEST(EveryFamilyIsContiguous) {
        auto snapshot = Full();
        snapshot.Counters.RpcTotal = {{{EYdbMethod::PlainState, "SUCCESS"}, 1}, {{EYdbMethod::Update, "SUCCESS"}, 1}};
        // monlib writes each TYPE line once, so a family that reappears after
        // another family started has its samples under a foreign TYPE line.
        const auto metrics = RenderMetrics(snapshot, TMonotonic::Seconds(100));
        TString family;
        for (const auto line : StringSplitter(metrics).Split('\n').SkipEmpty()) {
            const TStringBuf text = line.Token();
            if (text.StartsWith("# TYPE ")) {
                family = TString(text.SubStr(7).Before(' '));
                continue;
            }
            const TStringBuf name = text.Head(std::min(text.find('{'), text.find(' ')));
            UNIT_ASSERT_C(name == family, text);
        }
    }

    Y_UNIT_TEST(PileLabelsAreEscapedByMonlib) {
        auto snapshot = Full();
        snapshot.Selection.View->Piles["quote\"slash\\line\n"] = EPileState::Inactive;
        snapshot.Piles["quote\"slash\\line\n"].Verdict.Verdict = EPileVerdict::Unknown;
        const auto metrics = RenderMetrics(snapshot, TMonotonic::Seconds(100));
        UNIT_ASSERT_STRING_CONTAINS(metrics, "pile=\"quote\\\"slash\\\\line\\n\"");
    }
}

} // namespace NBridgeSkipper
