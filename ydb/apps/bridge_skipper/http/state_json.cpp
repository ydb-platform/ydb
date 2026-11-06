#include "http.h"
#include "age.h"

#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <util/string/cast.h>

#include <algorithm>

namespace NBridgeSkipper {
namespace {

using TJson = NJson::TJsonValue;

TJson Object() {
    return TJson(NJson::JSON_MAP);
}

TJson Array() {
    return TJson(NJson::JSON_ARRAY);
}

TJson Timestamp(TMonotonic at, TMonotonic now, TInstant wallNow) {
    return at ? TJson((wallNow - (now - at)).ToString()) : TJson(NJson::JSON_NULL);
}

TJson Timestamp(std::optional<TMonotonic> at, TMonotonic now, TInstant wallNow) {
    return at ? Timestamp(*at, now, wallNow) : TJson(NJson::JSON_NULL);
}

TJson Age(TMonotonic at, TMonotonic now) {
    const auto age = AgeMilliseconds(at, now);
    return age ? TJson(*age) : TJson(NJson::JSON_NULL);
}

TJson Diagnostic(const TString& text) {
    return text.empty() ? TJson(NJson::JSON_NULL) : TJson(text);
}

template <typename T>
TJson Strings(const T& strings) {
    auto result = Array();
    for (const auto& text : strings) {
        result.AppendValue(text);
    }
    return result;
}

TJson View(const TClusterView& view, TMonotonic now) {
    auto result = Object();
    result["generation"] = ::ToString(view.Generation);
    result["piles"] = Array();
    for (const auto& [name, state] : view.Piles) {
        auto pile = Object();
        pile["name"] = name;
        pile["state"] = ToString(state);
        result["piles"].AppendValue(std::move(pile));
    }
    result["pending_operation"] = TJson(NJson::JSON_NULL);
    if (view.Pending) {
        auto pending = Object();
        pending["kind"] = view.Pending->Kind;
        pending["pile"] = view.Pending->Pile ? TJson(*view.Pending->Pile) : TJson(NJson::JSON_NULL);
        pending["error"] = Diagnostic(view.Pending->Error);
        result["pending_operation"] = std::move(pending);
    }
    result["conversion_in_progress"] = view.ConversionInProgress;
    result["selected_from"] = view.Endpoint;
    result["age_ms"] = Age(view.SentAt, now);
    return result;
}

TJson Endpoint(const TEndpointSnapshot& endpoint, TMonotonic now) {
    auto result = Object();
    result["uri"] = endpoint.Uri;
    result["pile"] = endpoint.Pile;
    result["identity"] = ToString(endpoint.Identity);
    result["ignored"] = endpoint.Ignored;
    result["state_generation"] = endpoint.View ? TJson(::ToString(endpoint.View->Generation)) : TJson(NJson::JSON_NULL);
    result["state_age_ms"] = endpoint.View ? Age(endpoint.View->SentAt, now) : TJson(NJson::JSON_NULL);
    // Every SelfCheck field is null until the endpoint returns a reply.
    for (const auto* key : {"selfcheck_mode", "selfcheck_age_ms", "truncated", "size_warning", "self_check_result",
            "notes", "omitted_notes"})
    {
        result[key] = TJson(NJson::JSON_NULL);
    }
    if (const auto& reply = endpoint.SelfCheck) {
        const auto& classification = reply->Classification;
        result["selfcheck_mode"] = classification.Mode == ESelfCheckMode::Bsc ? "BSC" : "FALLBACK";
        result["selfcheck_age_ms"] = Age(reply->SentAt, now);
        result["truncated"] = classification.Truncated;
        result["size_warning"] = classification.SizeWarning;
        result["self_check_result"] = classification.SelfCheckResult;
        result["notes"] = Strings(classification.Notes);
        result["omitted_notes"] = classification.OmittedNotes;
    }
    result["last_error"] = Diagnostic(endpoint.LastError);
    return result;
}

TJson Pile(const TString& name, const TPileSnapshot& pile, TMonotonic now, TInstant wallNow) {
    auto result = Object();
    result["name"] = name;
    result["verdict"] = ToString(pile.Verdict.Verdict);
    result["reasons"] = Strings(pile.Verdict.Reasons);
    result["fallback_red"] = pile.Verdict.FallbackRed;
    result["self_quorum"] = pile.Verdict.SelfQuorum;
    result["connectivity"] = Array();
    // The actor keeps only the valid reports that list this pile.
    for (const auto& report : pile.Connectivity) {
        const auto& connectivity = report.Piles.at(name);
        auto item = Object();
        item["vantage_pile"] = report.VantagePile;
        item["reporter_node_id"] = report.ReporterNodeId;
        item["has_node_quorum"] = connectivity.HasNodeQuorum;
        item["in_config_quorum"] = connectivity.InConfigQuorum;
        item["binding_in_progress"] = report.BindingInProgress;
        result["connectivity"].AppendValue(std::move(item));
    }
    result["down_since"] = Timestamp(pile.DownSince, now, wallNow);
    result["confirmed"] = pile.Confirmed;
    return result;
}

TJson Invocation(const TMutationInvocation& invocation, TMonotonic now, TInstant wallNow) {
    auto result = PlanJson(invocation.Plan);
    result["sent_at"] = Timestamp(invocation.SentAt, now, wallNow);
    result["status"] = invocation.Status ? TJson(*invocation.Status) : TJson(NJson::JSON_NULL);
    result["outcome"] = invocation.Outcome ? TJson(ToString(*invocation.Outcome)) : TJson(NJson::JSON_NULL);
    result["ambiguous_resends"] = invocation.AmbiguousResends;
    return result;
}

} // namespace

TString RenderStateJson(const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    auto result = Object();
    result["schema"] = 2;
    result["cluster_id"] = snapshot.ClusterId;
    result["mode"] = snapshot.Mode == EMode::Manual ? "manual" : "auto";
    result["now"] = wallNow.ToString();
    result["loop_age_ms"] = Age(snapshot.LastTickAt, now);
    result["view"] = snapshot.Selection.View ? View(*snapshot.Selection.View, now) : TJson(NJson::JSON_NULL);
    result["generation_high_water"] = ::ToString(snapshot.GenerationHighWater);
    result["endpoints"] = Array();
    std::vector<const TEndpointSnapshot*> endpoints;
    for (const auto& endpoint : snapshot.Endpoints) {
        endpoints.push_back(&endpoint);
    }
    std::stable_sort(endpoints.begin(), endpoints.end(), [](const auto* left, const auto* right) {
        return left->Uri < right->Uri;
    });
    for (const auto* endpoint : endpoints) {
        result["endpoints"].AppendValue(Endpoint(*endpoint, now));
    }
    result["piles"] = Array();
    for (const auto& [name, pile] : snapshot.Piles) {
        result["piles"].AppendValue(Pile(name, pile, now, wallNow));
    }
    auto decision = Object();
    decision["kind"] = ToString(snapshot.Decision.Kind);
    decision["reason"] = snapshot.Decision.Reason;
    decision["blocker"] = snapshot.Decision.Blocker ? TJson(ToString(*snapshot.Decision.Blocker)) : TJson(NJson::JSON_NULL);
    decision["plan"] = snapshot.Decision.Plan ? PlanJson(*snapshot.Decision.Plan) : TJson(NJson::JSON_NULL);
    decision["cli_command"] = Diagnostic(snapshot.Decision.CliCommand);
    result["decision"] = std::move(decision);
    result["pending_mutation"] = TJson(NJson::JSON_NULL);
    if (const auto& pending = snapshot.PersistentState.PendingMutation) {
        auto item = Object();
        item["expected_generation"] = ::ToString(pending->ExpectedGeneration);
        item["update"] = UpdateJson(pending->Update);
        result["pending_mutation"] = std::move(item);
    }
    result["state_write_error"] = Diagnostic(snapshot.StateWriteError);
    result["last_mutation"] = snapshot.LastMutation ? Invocation(*snapshot.LastMutation, now, wallNow) : TJson(NJson::JSON_NULL);
    return NJson::WriteJson(result, false, true);
}

} // namespace NBridgeSkipper
