#include <ydb/apps/bridge_skipper/http/http.h>
#include <ydb/apps/bridge_skipper/http/page.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/testing/unittest/registar.h>
#include <util/string/cast.h>
#include <util/string/split.h>
#include <util/string/subst.h>

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

TString ElementText(TStringBuf page, TStringBuf marker) {
    const size_t begin = page.find(marker);
    UNIT_ASSERT_C(begin != TStringBuf::npos, marker);
    const auto content = page.SubStr(begin + marker.size());
    TString text(content.Before('<'));
    SubstGlobal(text, "&quot;", "\"");
    SubstGlobal(text, "&#39;", "'");
    SubstGlobal(text, "&lt;", "<");
    SubstGlobal(text, "&gt;", ">");
    SubstGlobal(text, "&amp;", "&");
    return text;
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

TSkipperSnapshot RejectedGenerationSnapshot(bool lifecycleOnly = false) {
    auto snapshot = Full();
    snapshot.Mode = EMode::Manual;
    snapshot.PersistentState.Mode = EMode::Manual;
    snapshot.ControllerReady = true;
    snapshot.StateWriteError.clear();
    const auto generation = snapshot.Selection.View->Generation;
    snapshot.GenerationHighWater = generation;
    snapshot.Selection.View->Pending.reset();
    snapshot.Selection.View->ConversionInProgress = false;
    snapshot.PersistentState.PendingMutation->Origin = EDispatchOrigin::Manual;
    snapshot.LastMutation->Origin = EDispatchOrigin::Manual;
    snapshot.LastMutation->Status = "BAD_REQUEST";
    snapshot.LastMutation->Outcome = EMutationOutcome::Rejected;
    snapshot.PendingAttempts = 1;
    if (lifecycleOnly) {
        snapshot.LifecycleRejectedGeneration = generation;
        snapshot.PersistentState.PendingMutation->Kind = EOperationKind::TakeOffline;
        snapshot.PersistentState.PendingMutation->Update = {{"b", EPileState::Suspended}};
        snapshot.LastMutation->Kind = EOperationKind::TakeOffline;
        snapshot.LastMutation->Plan.Update = snapshot.PersistentState.PendingMutation->Update;
        snapshot.Decision = {};
        snapshot.Decision.Kind = EDecisionKind::Blocked;
        snapshot.Decision.Blocker = EBlockerReason::PendingMutation;
        snapshot.Decision.Reason = "PENDING_MUTATION";
    } else {
        snapshot.RejectedGeneration = generation;
        snapshot.Decision.Reason = "REJECTED_BY_SERVER";
    }
    TDecision refused;
    refused.Kind = EDecisionKind::Blocked;
    refused.Reason = "REJECTED_AT_GENERATION";
    for (const auto& [name, state] : snapshot.Selection.View->Piles) {
        (void)state;
        for (const auto kind : {EOperationKind::Failover, EOperationKind::Switchover, EOperationKind::TakeOffline, EOperationKind::Rejoin}) {
            snapshot.Actions[name][kind] = lifecycleOnly && kind == EOperationKind::Failover ? snapshot.Decision : refused;
        }
    }
    return snapshot;
}

void DropSelectedView(TSkipperSnapshot& snapshot) {
    // Mirror a controller tick with no fresh view: retain observation history,
    // but publish no current assessments, action map or Ready plan.
    snapshot.LastObservedView = snapshot.Selection.View;
    snapshot.Selection = {};
    snapshot.Selection.Blocker = EBlockerReason::NoView;
    snapshot.Decision = Startup().Decision;
    snapshot.Actions.clear();
    snapshot.LastKnownPiles = snapshot.Piles;
    snapshot.Piles.clear();
    snapshot.Participation.clear();
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
    Y_UNIT_TEST(StateJsonContractsAndReadTimeAges) {
        const auto snapshot = Full();
        const auto rendered = RenderStateJson(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        const auto state = Json(rendered);
        UNIT_ASSERT_VALUES_EQUAL(state["schema"].GetInteger(), 3);
        UNIT_ASSERT_VALUES_EQUAL(state["generation_high_water"].GetString(), "9007199254740994");
        UNIT_ASSERT_VALUES_EQUAL(state["view"]["generation"].GetString(), "9007199254740993");
        UNIT_ASSERT_VALUES_EQUAL(state["endpoints"][0]["uri"].GetString(), "grpc://a:2135");
        UNIT_ASSERT_VALUES_EQUAL(state["endpoints"][0]["selfcheck_mode"].GetString(), "BSC");
        UNIT_ASSERT_VALUES_EQUAL(state["piles"][0]["connectivity"][0]["in_data_quorum"].GetBoolean(), true);
        UNIT_ASSERT_VALUES_EQUAL(state["piles"][0]["connectivity"][0]["static_nodes"].GetUInteger(), 3);
        UNIT_ASSERT_VALUES_EQUAL(state["pending_mutation"]["update"]["b"].GetString(), "DISCONNECTED");
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

    Y_UNIT_TEST(PendingSummaryOnlyUsesTheMatchingTypedAttempt) {
        for (const TString mismatch : {"superset", "rejected_maintenance", "manual_prepare", "new_generation", "none"}) {
            auto snapshot = Full();
            snapshot.LastMutation->Status = "SUCCESS";
            snapshot.LastMutation->Outcome = EMutationOutcome::Success;
            if (mismatch == "superset") {
                snapshot.PersistentState.PendingMutation->Update["c"] = EPileState::Disconnected;
            } else if (mismatch == "rejected_maintenance") {
                snapshot.LastMutation->Kind = EOperationKind::TakeOffline;
                snapshot.LastMutation->Plan.Update = {{"b", EPileState::Suspended}};
                snapshot.LastMutation->Status = "BAD_REQUEST";
                snapshot.LastMutation->Outcome = EMutationOutcome::Rejected;
            } else if (mismatch == "manual_prepare") {
                snapshot.PersistentState.PendingMutation->Origin = EDispatchOrigin::Manual;
                snapshot.LastMutation->Status = "INTERNAL_ERROR";
                snapshot.LastMutation->Outcome = EMutationOutcome::Ambiguous;
            } else if (mismatch == "new_generation") {
                ++snapshot.PersistentState.PendingMutation->ExpectedGeneration;
            }
            const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
            UNIT_ASSERT_VALUES_EQUAL(state["last_mutation"]["outcome"].GetString(), ToString(*snapshot.LastMutation->Outcome));
            if (mismatch == "none") {
                UNIT_ASSERT_VALUES_EQUAL(state["pending_mutation"]["latest_status"].GetString(), "SUCCESS");
                UNIT_ASSERT(state["operation_message"].GetString().Contains("RPC SUCCESS"));
                UNIT_ASSERT(!state["pending_mutation"]["message"].GetString().Contains("Recorded but not sent"));
            } else {
                UNIT_ASSERT(state["pending_mutation"]["latest_status"].IsNull());
                UNIT_ASSERT(state["pending_mutation"]["latest_outcome"].IsNull());
                UNIT_ASSERT(!state["operation_message"].GetString().Contains("RPC SUCCESS"));
                UNIT_ASSERT(state["operation_message"].GetString().Contains("Current pending update"));
            }
        }
    }

    Y_UNIT_TEST(CurrentBootUnsentPendingDoesNotInventARestartDispatch) {
        auto snapshot = Full();
        snapshot.LastMutation.reset();
        snapshot.PendingAttempts = 0;
        snapshot.MayApplyElsewhere = false;
        const auto project = [&] {
            return BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        };
        const auto unsent = project();
        const auto& message = unsent["pending_mutation"]["message"].GetString();
        UNIT_ASSERT_STRING_CONTAINS(message, "not sent");
        UNIT_ASSERT(!message.Contains("before restart"));
        UNIT_ASSERT(!message.Contains("may still complete"));

        snapshot.MayApplyElsewhere = true;
        const auto restored = project();
        UNIT_ASSERT_STRING_CONTAINS(restored["pending_mutation"]["message"].GetString(), "may still complete");
        UNIT_ASSERT_STRING_CONTAINS(restored["pending_mutation"]["message"].GetString(), "before restart");

        snapshot.MayApplyElsewhere = false;
        snapshot.PendingAttempts = 1;
        const auto uncertain = project();
        UNIT_ASSERT_STRING_CONTAINS(uncertain["pending_mutation"]["message"].GetString(), "may still complete");
    }

    Y_UNIT_TEST(PresentationRetainsObservedRolesAndExplainsExpiry) {
        auto snapshot = Full();
        snapshot.BootAt = TMonotonic::Seconds(1);
        snapshot.BootUtc = TInstant::Seconds(1);
        snapshot.Mode = EMode::Observe;
        snapshot.LastObservedView = snapshot.Selection.View;
        snapshot.Selection.View.reset();
        const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(728), TInstant::Seconds(728));
        UNIT_ASSERT_VALUES_EQUAL(state["schema"].GetInteger(), 3);
        UNIT_ASSERT_VALUES_EQUAL(state["mode"].GetString(), "observe");
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][0]["state"].GetString(), "PRIMARY");
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][0]["assessment"].GetString(), "expired");
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][0]["evidence"]["age_ms"].GetUInteger(), 630000);
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][0]["evidence"]["expired_for_ms"].GetUInteger(), 600000);
        UNIT_ASSERT(state["incident"]["active"].GetBoolean());
        // A pending requested overlay must never become the observed roles.
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][1]["state"].GetString(), "SYNCHRONIZED");
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][1]["last_known_verdict"].GetString(), "LOST");
    }

    Y_UNIT_TEST(ServerPageContainsEscapedContentAndUtcWithoutJavascript) {
        auto snapshot = Full();
        snapshot.ClusterId = "cluster<script>&\"";
        snapshot.BootAt = TMonotonic::Seconds(1);
        snapshot.BootUtc = TInstant::Seconds(1);
        const auto page = RenderPage(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_STRING_CONTAINS(page, "cluster&lt;script&gt;&amp;&quot;");
        UNIT_ASSERT_STRING_CONTAINS(page, "PRIMARY");
        UNIT_ASSERT_STRING_CONTAINS(page, "SYNCHRONIZED");
        UNIT_ASSERT_STRING_CONTAINS(page, "9007199254740993");
        UNIT_ASSERT_STRING_CONTAINS(page, "UTC");
        UNIT_ASSERT_STRING_CONTAINS(page, "Empty pile name");
        UNIT_ASSERT_STRING_CONTAINS(page, "may still complete");
        UNIT_ASSERT(!page.Contains("cluster<script>"));
    }

    Y_UNIT_TEST(CanonicalPresentationTextMatchesServerRenderedDocument) {
        auto snapshot = Full();
        snapshot.Mode = EMode::Manual;
        const auto now = TMonotonic::Seconds(100);
        const auto wallNow = TInstant::Seconds(100);
        const auto state = Json(RenderStateJson(snapshot, now, wallNow));
        const auto page = RenderPage(snapshot, now, wallNow);
        UNIT_ASSERT_VALUES_EQUAL(state["mode_text"].GetString(), "Manual · one reviewed request per approval.");
        UNIT_ASSERT_VALUES_EQUAL(state["decision_text"].GetString(), state["pending_mutation"]["message"].GetString());
        UNIT_ASSERT_VALUES_EQUAL(ElementText(page, "data-testid=\"effective-mode\">"), state["mode_text"].GetString());
        UNIT_ASSERT_VALUES_EQUAL(ElementText(page, "<h2 id=\"decision-title\">"), state["decision_text"].GetString());
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][0]["health_text"].GetString(), "Healthy");
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][0]["evidence_age_text"].GetString(), "Last sample 2 s ago");
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][1]["health_text"].GetString(), "Lost · confirmed");
        UNIT_ASSERT_VALUES_EQUAL(state["cards"][1]["context_action"].GetString(), "failover");
        UNIT_ASSERT_VALUES_EQUAL(ElementText(page, "<p class=\"health\">"), state["cards"][0]["health_text"].GetString());
        UNIT_ASSERT_VALUES_EQUAL(ElementText(page, "<p class=\"evidence-age metadata\">"), state["cards"][0]["evidence_age_text"].GetString());
    }

    Y_UNIT_TEST(RejectedGenerationExplainsRefusalInEveryMode) {
        for (const auto mode : {EMode::Observe, EMode::Manual, EMode::Auto}) {
            auto snapshot = Full();
            snapshot.Mode = mode;
            snapshot.PersistentState.PendingMutation.reset();
            snapshot.LastMutation->Status = "BAD_REQUEST";
            snapshot.LastMutation->Outcome = EMutationOutcome::Rejected;
            const auto generation = snapshot.Selection.View->Generation;
            snapshot.RejectedGeneration = generation;
            const auto now = TMonotonic::Seconds(100);
            const auto wallNow = TInstant::Seconds(100);
            const auto state = BuildPresentation(snapshot, now, wallNow);
            const auto& text = state["decision_text"].GetString();
            UNIT_ASSERT_VALUES_EQUAL(state["decision"]["kind"].GetString(), "READY");
            UNIT_ASSERT_STRING_CONTAINS(text, "Refused");
            UNIT_ASSERT_STRING_CONTAINS(text, "no sends");
            UNIT_ASSERT_STRING_CONTAINS(text, ::ToString(generation));
            UNIT_ASSERT_C(text.Contains("newer generation") || text.Contains("restart"), text);
            UNIT_ASSERT_VALUES_EQUAL(state["dispatch_refusal"].GetString(), text);
            UNIT_ASSERT_STRING_CONTAINS(state["auto_activation_text"].GetString(), text);
            UNIT_ASSERT_VALUES_EQUAL(ElementText(RenderPage(snapshot, now, wallNow), "<h2 id=\"decision-title\">"), text);

            snapshot.RejectedGeneration = generation - 1;
            const auto unlatched = BuildPresentation(snapshot, now, wallNow);
            const TString expected = mode == EMode::Manual ? "Plan ready · awaiting operator execution."
                : mode == EMode::Auto ? "Automatic dispatch enabled; inspect actual send and retry status."
                : "Plan ready · not dispatched.";
            UNIT_ASSERT_VALUES_EQUAL(unlatched["decision_text"].GetString(), expected);
            UNIT_ASSERT(unlatched["dispatch_refusal"].IsNull());
            UNIT_ASSERT_STRING_CONTAINS(unlatched["auto_activation_text"].GetString(), "A Ready plan may be dispatched immediately.");

            snapshot.RejectedGeneration.reset();
            snapshot.LifecycleRejectedGeneration = generation;
            const auto lifecycleOnly = BuildPresentation(snapshot, now, wallNow);
            UNIT_ASSERT(lifecycleOnly["dispatch_refusal"].IsNull());
            UNIT_ASSERT_VALUES_EQUAL(lifecycleOnly["decision_text"].GetString(), expected);
            UNIT_ASSERT_STRING_CONTAINS(lifecycleOnly["auto_activation_text"].GetString(), "A Ready plan may be dispatched immediately.");
        }
    }

    Y_UNIT_TEST(ServerOperationDetailsContainPendingAttemptAndRetryTogether) {
        auto snapshot = Full();
        snapshot.BootAt = TMonotonic::Seconds(1);
        snapshot.BootUtc = TInstant::Seconds(1);
        snapshot.RetryAt = TMonotonic::Seconds(110);
        const auto now = TMonotonic::Seconds(100);
        const auto wallNow = TInstant::Seconds(100);
        const auto state = Json(RenderStateJson(snapshot, now, wallNow));
        const auto page = RenderPage(snapshot, now, wallNow);
        const auto details = Json(ElementText(page, "<pre id=\"operation-json\" class=\"exact-update\">"));
        UNIT_ASSERT_VALUES_EQUAL(details.GetMap().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(details["pending"]["update"]["b"].GetString(), "DISCONNECTED");
        UNIT_ASSERT_VALUES_EQUAL(details["last_attempt"]["status"].GetString(), "CLIENT_DEADLINE_EXCEEDED");
        UNIT_ASSERT_VALUES_EQUAL(details["next_retry_at"].GetString(), TInstant::Seconds(110).ToString());
        UNIT_ASSERT_VALUES_EQUAL(NJson::WriteJson(details, false, true), NJson::WriteJson(state["operation_details"], false, true));
        UNIT_ASSERT_VALUES_EQUAL(NJson::WriteJson(details["pending"], false, true), NJson::WriteJson(state["pending_mutation"], false, true));
        UNIT_ASSERT_VALUES_EQUAL(NJson::WriteJson(details["last_attempt"], false, true), NJson::WriteJson(state["last_mutation"], false, true));
    }

    Y_UNIT_TEST(AbsoluteEventTimestampDoesNotMoveWithWallClock) {
        auto snapshot = Full();
        snapshot.BootAt = TMonotonic::Seconds(1);
        snapshot.BootUtc = TInstant::Seconds(1);
        const auto a = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        const auto b = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(1000));
        UNIT_ASSERT_VALUES_EQUAL(a["piles"][1]["down_since"].GetString(), b["piles"][1]["down_since"].GetString());
        UNIT_ASSERT_VALUES_EQUAL(a["last_mutation"]["sent_at"].GetString(), b["last_mutation"]["sent_at"].GetString());
    }

    Y_UNIT_TEST(NeverSampledAssessmentIsUnavailableWhileRetainedEvidenceStillExpires) {
        auto snapshot = Startup();
        snapshot.EvidenceTtl = TDuration::Seconds(30);
        TClusterView view;
        view.Generation = 1;
        view.Piles = {{"a", EPileState::Primary}};
        view.SentAt = TMonotonic::Seconds(99);
        snapshot.Selection.View = view;
        snapshot.Selection.Blocker.reset();
        snapshot.Piles["a"].Verdict.Reasons = {"No conclusive BSC or connectivity verdict"};
        const auto now = TMonotonic::Seconds(100);
        const auto wallNow = TInstant::Seconds(100);
        const auto state = BuildPresentation(snapshot, now, wallNow);
        const auto& card = state["cards"][0];
        UNIT_ASSERT_VALUES_EQUAL(card["current_assessment"]["assessment"].GetString(), "unavailable");
        UNIT_ASSERT_VALUES_EQUAL(card["assessment"].GetString(), "unavailable");
        UNIT_ASSERT_VALUES_EQUAL(card["health_text"].GetString(), "Assessment unavailable");
        UNIT_ASSERT(card["evidence"]["age_ms"].IsNull());
        UNIT_ASSERT(!card["evidence"]["expired"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(ElementText(RenderPage(snapshot, now, wallNow), "<p class=\"health\">"),
            "Assessment unavailable");

        // An unavailable current sample may still select real historical evidence.
        auto& retained = snapshot.LastKnownPiles["a"];
        retained.Verdict = {EPileVerdict::Failed, {"Historical storage failure"}, false, false, TMonotonic::Seconds(60)};
        const auto historical = BuildPresentation(snapshot, now, wallNow);
        const auto& historicalCard = historical["cards"][0];
        UNIT_ASSERT_VALUES_EQUAL(historicalCard["current_assessment"]["assessment"].GetString(), "unavailable");
        UNIT_ASSERT_VALUES_EQUAL(historicalCard["last_known_assessment"]["assessment"].GetString(), "expired");
        UNIT_ASSERT_VALUES_EQUAL(historicalCard["assessment"].GetString(), "expired");
        UNIT_ASSERT_VALUES_EQUAL(historicalCard["evidence"]["expired_for_ms"].GetUInteger(), 10000);
        UNIT_ASSERT(historicalCard["incident"].GetBoolean());
    }

    Y_UNIT_TEST(CurrentUnknownDetailsStaySeparateFromLastKnownIncident) {
        auto snapshot = Full();
        snapshot.LastKnownPiles = snapshot.Piles;
        auto& current = snapshot.Piles["b"];
        current.Verdict.Verdict = EPileVerdict::Unknown;
        current.Verdict.Reasons = {"BSC_WARMUP"};
        current.Verdict.NewestEvidenceSentAt = TMonotonic{};
        const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        const auto& card = state["cards"][1];
        UNIT_ASSERT_VALUES_EQUAL(card["assessment"].GetString(), "unavailable");
        UNIT_ASSERT_VALUES_EQUAL(card["verdict"].GetString(), "UNKNOWN");
        UNIT_ASSERT_VALUES_EQUAL(card["reasons"][0].GetString(), "BSC_WARMUP");
        UNIT_ASSERT_VALUES_EQUAL(card["current_assessment"]["verdict"].GetString(), "UNKNOWN");
        UNIT_ASSERT_VALUES_EQUAL(card["last_known_assessment"]["verdict"].GetString(), "LOST");
        UNIT_ASSERT(card["incident"].GetBoolean());
    }

    Y_UNIT_TEST(CompactCardsReferenceSingleCurrentAndSelectedHistoricalSources) {
        auto snapshot = Full();
        auto& pile = snapshot.Piles["b"];
        pile.Verdict.Verdict = EPileVerdict::Failed;
        pile.Verdict.Reasons = {"Historical failed storage"};
        pile.Verdict.NewestEvidenceSentAt = TMonotonic::Seconds(97);
        TConnectivity report;
        report.Generation = snapshot.Selection.View->Generation;
        report.ReporterNodeId = 70;
        report.SentAt = TMonotonic::Seconds(97);
        report.Piles["b"] = {3, 0, false, true, true};
        pile.Connectivity = {report};
        snapshot.LastKnownPiles = snapshot.Piles;
        pile.Verdict = {EPileVerdict::Unknown, {"Current BSC warmup"}, false, false, TMonotonic{}};
        report.ReporterNodeId = 80;
        report.SentAt = TMonotonic::Seconds(99);
        pile.Connectivity = {report};
        TEndpointSnapshot endpoint;
        endpoint.Pile = "b";
        endpoint.Uri = "grpc://b-quoted\"<&>:2135";
        snapshot.Endpoints.push_back(endpoint);
        endpoint.Uri = "grpc://b-first:2135";
        snapshot.Endpoints.push_back(endpoint);

        const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        const auto& card = state["cards"][1];
        UNIT_ASSERT(card.GetMap().find("connectivity") == card.GetMap().end());
        UNIT_ASSERT(card.GetMap().find("endpoints") == card.GetMap().end());
        UNIT_ASSERT_VALUES_EQUAL(state["piles"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(state["last_known_piles"].GetArray().size(), 1);
        const auto& current = state["piles"][1];
        const auto& retained = state["last_known_piles"][0];
        UNIT_ASSERT_VALUES_EQUAL(current["name"].GetString(), "b");
        UNIT_ASSERT_VALUES_EQUAL(retained["name"].GetString(), "b");
        UNIT_ASSERT_VALUES_EQUAL(current["connectivity"][0]["reporter_node_id"].GetUInteger(), 80);
        UNIT_ASSERT_VALUES_EQUAL(retained["connectivity"][0]["reporter_node_id"].GetUInteger(), 70);
        UNIT_ASSERT_VALUES_EQUAL(retained["connectivity"][0]["evidence"]["age_ms"].GetUInteger(), 3000);
        for (const auto& [compact, raw] : {std::make_pair(card["current_assessment"], current),
                std::make_pair(card["last_known_assessment"], retained)})
        {
            UNIT_ASSERT_VALUES_EQUAL(compact.GetMap().size(), 8);
            for (const TStringBuf key : {"verdict", "reasons", "fallback_red", "self_quorum", "evidence", "assessment", "down_since", "confirmed"}) {
                UNIT_ASSERT_C(compact.GetMap().find(TString(key)) != compact.GetMap().end(), key);
                UNIT_ASSERT_VALUES_EQUAL(NJson::WriteJson(compact[TString(key)], false, true), NJson::WriteJson(raw[TString(key)], false, true));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(card["current_assessment"]["reasons"][0].GetString(), "Current BSC warmup");
        UNIT_ASSERT_VALUES_EQUAL(card["last_known_assessment"]["reasons"][0].GetString(), "Historical failed storage");
        UNIT_ASSERT(card["current_assessment"]["evidence"]["age_ms"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(card["last_known_assessment"]["evidence"]["age_ms"].GetUInteger(), 3000);
        UNIT_ASSERT_VALUES_EQUAL(card["endpoint_uris"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(card["endpoint_uris"][0].GetString(), "grpc://b-first:2135");
        UNIT_ASSERT_VALUES_EQUAL(card["endpoint_uris"][1].GetString(), "grpc://b-quoted\"<&>:2135");

        snapshot.Piles["b"] = snapshot.LastKnownPiles["b"];
        const auto known = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT(known["last_known_piles"].GetArray().empty());
        UNIT_ASSERT_VALUES_EQUAL(NJson::WriteJson(known["cards"][1]["current_assessment"], false, true),
            NJson::WriteJson(known["cards"][1]["last_known_assessment"], false, true));
    }

    Y_UNIT_TEST(RejoinWarmupPreservesRetainedFailureAcrossParticipationExclusion) {
        auto snapshot = Full();
        snapshot.PersistentState.PendingMutation.reset();
        snapshot.LastMutation.reset();
        snapshot.Decision.Kind = EDecisionKind::Stable;
        snapshot.Decision.Plan.reset();
        snapshot.Piles["b"].Verdict.Verdict = EPileVerdict::Failed;
        snapshot.Piles["b"].Verdict.Reasons = {"BSC_GROUP_RED"};
        snapshot.LastKnownPiles = snapshot.Piles;
        auto& current = snapshot.Piles["b"];
        current.Verdict = {EPileVerdict::Unknown, {"BSC_WARMUP"}, false, false, TMonotonic{}};
        current.Confirmed = false;
        current.Connectivity.clear();
        const auto check = [&](TStringBuf assessment, bool incident) {
            const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
            const auto& card = state["cards"][1];
            UNIT_ASSERT_VALUES_EQUAL(card["assessment"].GetString(), assessment);
            UNIT_ASSERT_VALUES_EQUAL(card["verdict"].GetString(), "UNKNOWN");
            UNIT_ASSERT_VALUES_EQUAL(card["current_assessment"]["verdict"].GetString(), "UNKNOWN");
            UNIT_ASSERT_VALUES_EQUAL(card["current_assessment"]["reasons"][0].GetString(), "BSC_WARMUP");
            UNIT_ASSERT_VALUES_EQUAL(card["last_known_verdict"].GetString(), "FAILED");
            UNIT_ASSERT_VALUES_EQUAL(card["last_known_assessment"]["verdict"].GetString(), "FAILED");
            UNIT_ASSERT_VALUES_EQUAL(card["incident"].GetBoolean(), incident);
            UNIT_ASSERT_VALUES_EQUAL(state["incident"]["active"].GetBoolean(), incident);
            return card["participating"];
        };

        snapshot.Selection.View->Piles["b"] = EPileState::Disconnected;
        check("not_assessed", false);

        ++snapshot.Selection.View->Generation;
        snapshot.Selection.View->Piles["b"] = EPileState::NotSynchronized;
        UNIT_ASSERT(check("unavailable", true).IsNull());

        snapshot.Participation["b"] = {false, TMonotonic::Seconds(99)};
        UNIT_ASSERT(!check("not_assessed", false).GetBoolean());

        snapshot.Participation["b"] = {true, TMonotonic::Seconds(99)};
        UNIT_ASSERT(check("unavailable", true).GetBoolean());
    }

    Y_UNIT_TEST(ExpiryBoundaryAndKnownAssessmentExclusion) {
        auto snapshot = Full();
        const auto expired = BuildPresentation(snapshot, TMonotonic::Seconds(128), TInstant::Seconds(128));
        UNIT_ASSERT_VALUES_EQUAL(expired["cards"][0]["assessment"].GetString(), "expired");
        UNIT_ASSERT_VALUES_EQUAL(expired["cards"][0]["evidence"]["expired_for_ms"].GetUInteger(), 0);
        snapshot.Selection.View->Piles["b"] = EPileState::NotSynchronized;
        auto unknown = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT(unknown["cards"][1]["participating"].IsNull());
        UNIT_ASSERT(unknown["cards"][1]["assessment"].GetString() != "not_assessed");
        TConnectivity report;
        report.Generation = snapshot.Selection.View->Generation;
        report.SentAt = TMonotonic::Seconds(99);
        report.Piles["b"] = {3, 0, false, false, false};
        snapshot.Piles["b"].Connectivity.push_back(report);
        const auto excluded = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(excluded["cards"][1]["assessment"].GetString(), "not_assessed");
        UNIT_ASSERT(!excluded["cards"][1]["participating"].GetBoolean());
    }

    Y_UNIT_TEST(BscProvenStageTwoParticipationIsNotHiddenByConnectivity) {
        for (const auto verdict : {EPileVerdict::Unknown, EPileVerdict::Failed}) {
            auto snapshot = Full();
            snapshot.Selection.View->Piles["b"] = EPileState::NotSynchronized;
            auto& pile = snapshot.Piles["b"];
            pile.Verdict.Verdict = verdict;
            pile.Confirmed = false;
            TConnectivity report;
            report.Generation = snapshot.Selection.View->Generation;
            report.SentAt = TMonotonic::Seconds(99);
            report.Piles["b"] = {3, 0, false, false, false};
            pile.Connectivity = {report};
            snapshot.Participation["b"] = {true, TMonotonic::Seconds(99)};
            auto state = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
            UNIT_ASSERT(state["cards"][1]["participating"].GetBoolean());
            UNIT_ASSERT(state["cards"][1]["assessment"].GetString() != "not_assessed");
            UNIT_ASSERT_VALUES_EQUAL(state["cards"][1]["incident"].GetBoolean(), verdict == EPileVerdict::Failed);
            snapshot.Participation["b"] = {false, TMonotonic::Seconds(99)};
            snapshot.Piles.erase("b");
            state = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
            UNIT_ASSERT_VALUES_EQUAL(state["cards"][1]["assessment"].GetString(), "not_assessed");
        }
    }

    Y_UNIT_TEST(ParticipationProofExpiresWithoutControllerTick) {
        for (const bool participates : {false, true}) {
            auto snapshot = Full();
            snapshot.EvidenceTtl = TDuration::Seconds(30);
            snapshot.Selection.View->SentAt = TMonotonic::Seconds(100);
            snapshot.Selection.View->Piles["b"] = EPileState::NotSynchronized;
            snapshot.Participation["b"] = {participates, TMonotonic::Seconds(80)};
            auto state = BuildPresentation(snapshot, TMonotonic::Seconds(109), TInstant::Seconds(109));
            UNIT_ASSERT_VALUES_EQUAL(state["cards"][1]["participating"].GetBoolean(), participates);
            state = BuildPresentation(snapshot, TMonotonic::Seconds(110), TInstant::Seconds(110));
            UNIT_ASSERT(state["cards"][1]["participating"].IsNull());
            UNIT_ASSERT(state["cards"][1]["assessment"].GetString() != "not_assessed");
            UNIT_ASSERT(state["cards"][1]["incident"].GetBoolean());
        }
    }

    Y_UNIT_TEST(IncidentActionUsesCanonicalFailoverAvailability) {
        auto snapshot = Full();
        snapshot.Mode = EMode::Manual;
        snapshot.ControllerReady = true;
        TDecision refused;
        refused.Kind = EDecisionKind::Blocked;
        refused.Reason = "REJECTED_AT_GENERATION";
        snapshot.Actions["b"][EOperationKind::Failover] = refused;
        const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(state["decision"]["kind"].GetString(), "READY");
        UNIT_ASSERT(!state["failover_action"]["available"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(state["failover_action"]["reason"].GetString(), "REJECTED_AT_GENERATION");
        const auto page = RenderPage(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_STRING_CONTAINS(page, "id=\"review-failover\" class=\"primary\" disabled");
        UNIT_ASSERT_STRING_CONTAINS(page, "aria-describedby=\"review-failover-reason\"");
    }

    Y_UNIT_TEST(RejectedFailoverActionsRetainReasonAtEvidenceBoundary) {
        auto snapshot = RejectedGenerationSnapshot();
        const auto boundary = snapshot.Selection.View->SentAt + snapshot.EvidenceTtl;
        const auto state = BuildPresentation(snapshot, boundary, TInstant::Seconds(129));
        UNIT_ASSERT(!state["cards"][1]["state_current"].GetBoolean());
        for (const TString action : {"failover", "switchover", "take_offline", "rejoin"}) {
            const auto& availability = state["cards"][1]["actions"][action];
            UNIT_ASSERT(!availability["available"].GetBoolean());
            UNIT_ASSERT_VALUES_EQUAL(availability["reason"].GetString(), "REJECTED_AT_GENERATION");
        }
        UNIT_ASSERT(!state["failover_action"]["available"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(state["failover_action"]["reason"].GetString(), "REJECTED_AT_GENERATION");

        // Freshness still withdraws an otherwise Ready action at the boundary.
        snapshot.RejectedGeneration.reset();
        snapshot.Actions["b"][EOperationKind::Failover] = snapshot.Decision;
        const auto readyExpired = BuildPresentation(snapshot, boundary, TInstant::Seconds(129));
        UNIT_ASSERT(!readyExpired["failover_action"]["available"].GetBoolean());
        UNIT_ASSERT_VALUES_EQUAL(readyExpired["failover_action"]["reason"].GetString(), "Fresh coherent observations required");
    }

    Y_UNIT_TEST(RejectedFailoverRefusalSurvivesMissingSelectedView) {
        for (const auto mode : {EMode::Observe, EMode::Manual, EMode::Auto}) {
            auto snapshot = RejectedGenerationSnapshot();
            snapshot.Mode = mode;
            const auto generation = *snapshot.RejectedGeneration;
            DropSelectedView(snapshot);
            const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(129), TInstant::Seconds(129));
            UNIT_ASSERT(state["view"].IsNull());
            UNIT_ASSERT_VALUES_EQUAL(state["last_observed_view"]["generation"].GetString(), ::ToString(generation));
            UNIT_ASSERT_VALUES_EQUAL(state["rejected_generation"].GetString(), ::ToString(generation));
            const auto& refusal = state["dispatch_refusal"].GetString();
            UNIT_ASSERT_STRING_CONTAINS(refusal, "Refused");
            UNIT_ASSERT_STRING_CONTAINS(refusal, ::ToString(generation));
            UNIT_ASSERT_STRING_CONTAINS(refusal, "no sends");
            UNIT_ASSERT_VALUES_EQUAL(state["decision_text"].GetString(), refusal);
            UNIT_ASSERT_STRING_CONTAINS(state["auto_activation_text"].GetString(), refusal);
            if (mode == EMode::Manual) {
                for (const TString action : {"failover", "switchover", "take_offline", "rejoin"}) {
                    const auto& availability = state["cards"][1]["actions"][action];
                    UNIT_ASSERT(!availability["available"].GetBoolean());
                    UNIT_ASSERT_VALUES_EQUAL(availability["reason"].GetString(), "REJECTED_AT_GENERATION");
                }
                UNIT_ASSERT(!state["failover_action"]["available"].GetBoolean());
                UNIT_ASSERT_VALUES_EQUAL(state["failover_action"]["reason"].GetString(), "REJECTED_AT_GENERATION");
            }
        }
    }

    Y_UNIT_TEST(NewerSelectedViewDoesNotInheritOldRejection) {
        auto snapshot = RejectedGenerationSnapshot();
        const auto generation = *snapshot.RejectedGeneration;
        snapshot.LastObservedView = snapshot.Selection.View;
        ++snapshot.Selection.View->Generation;
        snapshot.Selection.View->SentAt = TMonotonic::Seconds(129);
        snapshot.GenerationHighWater = generation + 1;
        snapshot.PersistentState.PendingMutation.reset();
        snapshot.Decision.Reason = "confirmed failure";
        snapshot.Decision.Plan->ExpectedGeneration = generation + 1;
        snapshot.Actions["b"][EOperationKind::Failover] = snapshot.Decision;
        // Even if old latch diagnostics remain, the selected generation takes
        // precedence over last-observed history when projecting the refusal.
        snapshot.LifecycleRejectedGeneration = generation;
        const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(130), TInstant::Seconds(130));
        UNIT_ASSERT_VALUES_EQUAL(state["view"]["generation"].GetString(), ::ToString(generation + 1));
        UNIT_ASSERT(state["dispatch_refusal"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(state["decision_text"].GetString(), "Plan ready · awaiting operator execution.");
        UNIT_ASSERT_STRING_CONTAINS(state["auto_activation_text"].GetString(), "A Ready plan may be dispatched immediately.");
        UNIT_ASSERT(state["failover_action"]["available"].GetBoolean());
    }

    Y_UNIT_TEST(LifecycleRejectionRemainsLocalAtEvidenceBoundary) {
        const auto snapshot = RejectedGenerationSnapshot(true);
        const auto boundary = snapshot.Selection.View->SentAt + snapshot.EvidenceTtl;
        const auto state = BuildPresentation(snapshot, boundary, TInstant::Seconds(129));
        for (const TString action : {"switchover", "take_offline", "rejoin"}) {
            const auto& availability = state["cards"][1]["actions"][action];
            UNIT_ASSERT(!availability["available"].GetBoolean());
            UNIT_ASSERT_VALUES_EQUAL(availability["reason"].GetString(), "REJECTED_AT_GENERATION");
        }
        UNIT_ASSERT(state["rejected_generation"].IsNull());
        UNIT_ASSERT(state["dispatch_refusal"].IsNull());
        UNIT_ASSERT_STRING_CONTAINS(state["auto_activation_text"].GetString(), "A Ready plan may be dispatched immediately.");
        UNIT_ASSERT(state["failover_action"]["reason"].GetString() != "REJECTED_AT_GENERATION");
    }

    Y_UNIT_TEST(LifecycleRejectionRemainsLocalWithoutSelectedView) {
        auto snapshot = RejectedGenerationSnapshot(true);
        const auto generation = *snapshot.LifecycleRejectedGeneration;
        DropSelectedView(snapshot);
        const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(129), TInstant::Seconds(129));
        UNIT_ASSERT(state["view"].IsNull());
        UNIT_ASSERT_VALUES_EQUAL(state["last_observed_view"]["generation"].GetString(), ::ToString(generation));
        UNIT_ASSERT_VALUES_EQUAL(state["lifecycle_rejected_generation"].GetString(), ::ToString(generation));
        for (const TString action : {"switchover", "take_offline", "rejoin"}) {
            const auto& availability = state["cards"][1]["actions"][action];
            UNIT_ASSERT(!availability["available"].GetBoolean());
            UNIT_ASSERT_VALUES_EQUAL(availability["reason"].GetString(), "REJECTED_AT_GENERATION");
        }
        UNIT_ASSERT(state["rejected_generation"].IsNull());
        UNIT_ASSERT(state["dispatch_refusal"].IsNull());
        UNIT_ASSERT_STRING_CONTAINS(state["auto_activation_text"].GetString(), "A Ready plan may be dispatched immediately.");
        UNIT_ASSERT(state["failover_action"]["reason"].GetString() != "REJECTED_AT_GENERATION");
    }

    Y_UNIT_TEST(DefaultLogProjectionUsesInfoWhilePreservingDebugCapture) {
        auto snapshot = Startup();
        snapshot.Diagnostics = std::make_shared<TDiagnosticStore>("debug-boot", "debug");
        snapshot.Diagnostics->Log("debug", "debug-capture-only-sentinel", TInstant::Seconds(98));
        snapshot.Diagnostics->Log("info", "info-default-visible-sentinel", TInstant::Seconds(99));
        const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(state["logs"]["rows"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(state["logs"]["rows"][0]["message"].GetString(), "info-default-visible-sentinel");
        UNIT_ASSERT_VALUES_EQUAL(state["logs"]["capture_level"].GetString(), "debug");
        UNIT_ASSERT(state["logs"]["debug_available"].GetBoolean());
        // Ring coverage counts describe all captured levels, before the default Info filter.
        UNIT_ASSERT_VALUES_EQUAL(state["logs"]["retained_count"].GetUInteger(), 2);
        UNIT_ASSERT_VALUES_EQUAL(state["logs"]["dropped_count"].GetUInteger(), 0);
        UNIT_ASSERT_VALUES_EQUAL(state["logs"]["total_count"].GetUInteger(), 2);
        UNIT_ASSERT_VALUES_EQUAL(state["logs"]["oldest_at"].GetString(), TInstant::Seconds(98).ToString());
        const auto captured = snapshot.Diagnostics->ReadLogs({}, "debug");
        UNIT_ASSERT_VALUES_EQUAL(captured["rows"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(captured["rows"][0]["message"].GetString(), "debug-capture-only-sentinel");
        const auto page = RenderPage(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_STRING_CONTAINS(page, "info-default-visible-sentinel");
        UNIT_ASSERT_STRING_CONTAINS(page, "id=\"log-retention\"");
        UNIT_ASSERT_STRING_CONTAINS(page, "1970-01-01T00:01:38.000000Z UTC");
        UNIT_ASSERT(!page.Contains("debug-capture-only-sentinel"));
    }

    Y_UNIT_TEST(HistoryGapAndOriginsUseCanonicalLabelsInJsonAndServerHtml) {
        auto snapshot = Startup();
        snapshot.Diagnostics = std::make_shared<TDiagnosticStore>("history");
        NJson::TJsonValue gap(NJson::JSON_MAP);
        gap["generation"] = "45";
        gap["before_generation"] = "42";
        gap["message"] = "Observed pile state transition";
        gap["origin"] = "external";
        gap["coverage_gap"] = true;
        gap["coverage_gap_text"] = "Generations 43–44 not observed.";
        snapshot.Diagnostics->History("view", std::move(gap), TInstant::Seconds(98));
        NJson::TJsonValue mode(NJson::JSON_MAP);
        mode["mode"] = "manual";
        mode["origin"] = "operator";
        snapshot.Diagnostics->History("mode", std::move(mode), TInstant::Seconds(99));
        NJson::TJsonValue decision(NJson::JSON_MAP);
        decision["reason"] = "CONFIRMED_DOWN: b";
        decision["origin"] = "skipper";
        snapshot.Diagnostics->History("decision", std::move(decision), TInstant::Seconds(100));

        const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        const auto& rows = state["history"]["rows"];
        UNIT_ASSERT_STRING_CONTAINS(rows[0]["summary"].GetString(), "Observed pile state transition");
        UNIT_ASSERT_STRING_CONTAINS(rows[0]["summary"].GetString(), "Generations 43–44 not observed.");
        UNIT_ASSERT_VALUES_EQUAL(rows[0]["origin_label"].GetString(), "External/unknown");
        UNIT_ASSERT_STRING_CONTAINS(rows[1]["summary"].GetString(), "manual");
        UNIT_ASSERT_VALUES_EQUAL(rows[1]["origin_label"].GetString(), "Operator");
        UNIT_ASSERT_STRING_CONTAINS(rows[2]["summary"].GetString(), "CONFIRMED_DOWN: b");
        UNIT_ASSERT_VALUES_EQUAL(rows[2]["origin_label"].GetString(), "Skipper");
        const auto page = RenderPage(snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        for (const auto& row : rows.GetArray()) {
            UNIT_ASSERT_STRING_CONTAINS(page, "<td>" + row["summary"].GetString()
                + "</td><td>" + row["origin_label"].GetString() + "</td>");
        }
    }

    Y_UNIT_TEST(FreshTailExcerptsDoNotReportLostDiagnosticCoverage) {
        auto snapshot = Startup();
        snapshot.Diagnostics = std::make_shared<TDiagnosticStore>("fresh");
        for (ui64 n = 1; n <= 4; ++n) {
            snapshot.Diagnostics->History("view", NJson::TJsonValue(NJson::JSON_MAP), TInstant::Seconds(n));
        }
        for (ui64 n = 1; n <= 101; ++n) {
            snapshot.Diagnostics->Log("info", "Fresh retained log", TInstant::Seconds(n));
        }
        const auto state = BuildPresentation(snapshot, TMonotonic::Seconds(102), TInstant::Seconds(102));
        UNIT_ASSERT_VALUES_EQUAL(state["history"]["rows"].GetArray().size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(state["logs"]["rows"].GetArray().size(), 100);
        for (const TStringBuf key : {"history", "logs"}) {
            const auto& stream = state[TString(key)];
            UNIT_ASSERT_VALUES_EQUAL(stream["dropped_count"].GetUInteger(), 0);
            UNIT_ASSERT(!stream["coverage_gap"].GetBoolean());
            UNIT_ASSERT(stream["tail_clipped"].GetBoolean());
        }
        const auto page = RenderPage(snapshot, TMonotonic::Seconds(102), TInstant::Seconds(102));
        UNIT_ASSERT_VALUES_EQUAL(ElementText(page, "<p id=\"history-coverage\" class=\"coverage\">"), "");
        UNIT_ASSERT_VALUES_EQUAL(ElementText(page, "<p id=\"log-coverage\" class=\"coverage\">"), "");
    }

    Y_UNIT_TEST(RetainedEvictionNoticesAreServerRendered) {
        auto snapshot = Startup();
        snapshot.Diagnostics = std::make_shared<TDiagnosticStore>("pruned");
        for (ui64 n = 1; n <= 1001; ++n) {
            snapshot.Diagnostics->History("view", NJson::TJsonValue(NJson::JSON_MAP), TInstant::Seconds(n));
        }
        for (ui64 n = 1; n <= 5001; ++n) {
            snapshot.Diagnostics->Log("info", "Retained log", TInstant::Seconds(n));
        }
        const auto now = TMonotonic::Seconds(5002);
        const auto wallNow = TInstant::Seconds(5002);
        const auto state = BuildPresentation(snapshot, now, wallNow);
        UNIT_ASSERT_VALUES_EQUAL(state["history"]["dropped_count"].GetUInteger(), 1);
        UNIT_ASSERT_VALUES_EQUAL(state["logs"]["dropped_count"].GetUInteger(), 1);
        const auto page = RenderPage(snapshot, now, wallNow);
        UNIT_ASSERT_STRING_CONTAINS(ElementText(page, "<p id=\"history-coverage\" class=\"coverage\">"), "coverage gap");
        UNIT_ASSERT_STRING_CONTAINS(ElementText(page, "<p id=\"log-coverage\" class=\"coverage\">"), "coverage gap");
    }

    Y_UNIT_TEST(MethodsAreRejectedBeforeRouting) {
        const auto snapshot = Full();
        for (const TStringBuf method : {"POST", "PUT", "DELETE", "PATCH", "OPTIONS", "get"}) {
            for (const TStringBuf path : {"/", "/api/state", "/metrics", "/api/mode", "/api/plans", "/unknown"}) {
                UNIT_ASSERT_VALUES_EQUAL(RouteHttpRequest(method, path, snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Status, 405);
            }
        }
        for (const TStringBuf path : {"/api/audit", "/mon/state", "/metrics/"}) {
            UNIT_ASSERT_VALUES_EQUAL(RouteHttpRequest("GET", path, snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Status, 404);
        }
        UNIT_ASSERT_VALUES_EQUAL(RouteHttpRequest("GET", "/api/operations", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Status, 405);
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
        UNIT_ASSERT_VALUES_EQUAL(RouteHttpRequest("GET", "/?x=1", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100)).Status, 200);
        const auto page = RouteHttpRequest("GET", "/", snapshot, TMonotonic::Seconds(100), TInstant::Seconds(100));
        UNIT_ASSERT_VALUES_EQUAL(page.ContentType, "text/html; charset=utf-8");
        UNIT_ASSERT_STRING_CONTAINS(page.Body, "Bridge Skipper");
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
        const auto actor = runtime.Register(CreateHttpActor(store, proxy, THttpConfig{}));
        TAutoPtr<NActors::IEventHandle> registration;
        const auto* registered = runtime.GrabEdgeEvent<NHttp::TEvHttpProxy::TEvRegisterHandler>(registration);
        UNIT_ASSERT_VALUES_EQUAL(registered->Path, "/");
        UNIT_ASSERT_VALUES_EQUAL(registered->Handler, actor);
        for (const auto& [method, path, status] : {
                std::tuple{"GET", "/api/state", "200"},
                std::tuple{"POST", "/unknown", "405"}})
        {
            const TString wire = TString(method) + " " + path + " HTTP/1.1\r\nHost: localhost:8080\r\n\r\n";
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
