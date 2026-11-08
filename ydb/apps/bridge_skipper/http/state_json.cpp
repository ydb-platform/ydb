#include "http.h"
#include "page.h"
#include "age.h"

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

TJson Null() {
    return TJson(NJson::JSON_NULL);
}

// The boot anchor maps an event's monotonic instant once, independent of later
// wall-clock corrections. Recorded history and invocations already carry UTC.
TJson Timestamp(TMonotonic at, const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    if (!at) {
        return Null();
    }
    if (snapshot.BootAt && snapshot.BootUtc) {
        return TJson((at >= snapshot.BootAt ? snapshot.BootUtc + (at - snapshot.BootAt)
            : snapshot.BootUtc - (snapshot.BootAt - at)).ToString());
    }
    return TJson((now >= at ? wallNow - (now - at) : wallNow).ToString());
}

TJson Age(TMonotonic at, TMonotonic now) {
    const auto age = AgeMilliseconds(at, now);
    return age ? TJson(*age) : Null();
}

TJson Diagnostic(const TString& text) {
    return text.empty() ? Null() : TJson(text);
}

template <typename T>
TJson Strings(const T& strings) {
    auto result = Array();
    for (const auto& text : strings) {
        result.AppendValue(text);
    }
    return result;
}

TJson Evidence(TMonotonic sent, const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    auto result = Object();
    result["age_ms"] = Age(sent, now);
    result["ttl_ms"] = snapshot.EvidenceTtl.MilliSeconds();
    result["sample_at"] = Timestamp(sent, snapshot, now, wallNow);
    result["expires_at"] = sent ? Timestamp(sent + snapshot.EvidenceTtl, snapshot, now, wallNow) : Null();
    const auto age = AgeMilliseconds(sent, now);
    result["expired"] = age && *age >= snapshot.EvidenceTtl.MilliSeconds();
    result["expired_for_ms"] = age ? TJson(*age >= snapshot.EvidenceTtl.MilliSeconds()
        ? *age - snapshot.EvidenceTtl.MilliSeconds() : 0) : Null();
    return result;
}

TJson View(const TClusterView& view, const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    auto result = Object();
    result["generation"] = ::ToString(view.Generation);
    result["piles"] = Array();
    for (const auto& [name, state] : view.Piles) {
        auto pile = Object();
        pile["name"] = name;
        pile["state"] = ToString(state);
        result["piles"].AppendValue(std::move(pile));
    }
    result["pending_operation"] = Null();
    if (view.Pending) {
        auto pending = Object();
        pending["kind"] = view.Pending->Kind;
        pending["pile"] = view.Pending->Pile ? TJson(*view.Pending->Pile) : Null();
        pending["error"] = Diagnostic(view.Pending->Error);
        result["pending_operation"] = std::move(pending);
    }
    result["conversion_in_progress"] = view.ConversionInProgress;
    result["selected_from"] = view.Endpoint;
    result["age_ms"] = Age(view.SentAt, now);
    result["sample_at"] = Timestamp(view.SentAt, snapshot, now, wallNow);
    result["evidence"] = Evidence(view.SentAt, snapshot, now, wallNow);
    result["evidence"]["source"] = "GetClusterState";
    return result;
}

TJson Endpoint(const TEndpointSnapshot& endpoint, const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    auto result = Object();
    result["uri"] = endpoint.Uri;
    result["pile"] = endpoint.Pile;
    result["identity"] = ToString(endpoint.Identity);
    result["ignored"] = endpoint.Ignored;
    result["state_generation"] = endpoint.View ? TJson(::ToString(endpoint.View->Generation)) : Null();
    result["state_age_ms"] = endpoint.View ? Age(endpoint.View->SentAt, now) : Null();
    for (const auto* key : {"selfcheck_mode", "selfcheck_age_ms", "truncated", "size_warning", "self_check_result",
            "notes", "omitted_notes", "selfcheck_evidence"})
    {
        result[key] = Null();
    }
    if (const auto& reply = endpoint.SelfCheck) {
        const auto& classification = reply->Classification;
        result["selfcheck_mode"] = classification.Mode == ESelfCheckMode::Bsc ? "BSC" : "FALLBACK";
        result["selfcheck_age_ms"] = Age(reply->SentAt, now);
        result["selfcheck_evidence"] = Evidence(reply->SentAt, snapshot, now, wallNow);
        result["selfcheck_evidence"]["source"] = "SelfCheck";
        result["truncated"] = classification.Truncated;
        result["size_warning"] = classification.SizeWarning;
        result["self_check_result"] = classification.SelfCheckResult;
        result["notes"] = Strings(classification.Notes);
        result["omitted_notes"] = classification.OmittedNotes;
    }
    result["last_error"] = Diagnostic(endpoint.LastError);
    return result;
}

TJson AssessmentMetadata(const TPileSnapshot& pile, const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    auto result = Object();
    result["verdict"] = ToString(pile.Verdict.Verdict);
    result["reasons"] = Strings(pile.Verdict.Reasons);
    result["fallback_red"] = pile.Verdict.FallbackRed;
    result["self_quorum"] = pile.Verdict.SelfQuorum;
    result["evidence"] = Evidence(pile.Verdict.NewestEvidenceSentAt, snapshot, now, wallNow);
    result["evidence"]["source"] = "controller_assessment";
    result["assessment"] = !pile.Verdict.NewestEvidenceSentAt ? "unavailable"
        : result["evidence"]["expired"].GetBoolean() ? "expired" : "available";
    result["down_since"] = pile.DownSince ? Timestamp(*pile.DownSince, snapshot, now, wallNow) : Null();
    result["confirmed"] = pile.Confirmed;
    return result;
}

TJson Pile(const TString& name, const TPileSnapshot& pile, const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    auto result = AssessmentMetadata(pile, snapshot, now, wallNow);
    result["name"] = name;
    result["connectivity"] = Array();
    for (const auto& report : pile.Connectivity) {
        const auto found = report.Piles.find(name);
        if (found == report.Piles.end()) {
            continue;
        }
        const auto& connectivity = found->second;
        auto item = Object();
        item["vantage_pile"] = report.VantagePile;
        item["reporter_node_id"] = report.ReporterNodeId;
        item["reporter_is_root"] = report.ReporterIsRoot;
        item["generation"] = ::ToString(report.Generation);
        item["age_ms"] = Age(report.SentAt, now);
        item["evidence"] = Evidence(report.SentAt, snapshot, now, wallNow);
        item["evidence"]["source"] = "GetClusterState connectivity";
        item["static_nodes"] = connectivity.StaticNodes;
        item["connected_static_nodes"] = connectivity.ConnectedStaticNodes;
        item["has_node_quorum"] = connectivity.HasNodeQuorum;
        item["in_config_quorum"] = connectivity.InConfigQuorum;
        item["in_data_quorum"] = connectivity.InDataQuorum;
        item["binding_in_progress"] = report.BindingInProgress;
        result["connectivity"].AppendValue(std::move(item));
    }
    return result;
}

TJson Decision(const TDecision& decision) {
    auto result = Object();
    result["kind"] = ToString(decision.Kind);
    result["reason"] = decision.Reason;
    result["blocker"] = decision.Blocker ? TJson(ToString(*decision.Blocker)) : Null();
    result["plan"] = decision.Plan ? PlanJson(*decision.Plan) : Null();
    result["cli_command"] = Diagnostic(decision.CliCommand);
    return result;
}

TJson Invocation(const TMutationInvocation& invocation, const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    auto result = PlanJson(invocation.Plan);
    result["id"] = snapshot.ControllerBootId + ":operation:" + ::ToString(invocation.Id);
    result["sent_at"] = invocation.SentUtc ? TJson(invocation.SentUtc.ToString())
        : Timestamp(invocation.SentAt, snapshot, now, wallNow);
    result["kind"] = ToString(invocation.Kind);
    result["origin"] = ToString(invocation.Origin);
    result["status"] = invocation.Status ? TJson(*invocation.Status) : Null();
    result["outcome"] = invocation.Outcome ? TJson(ToString(*invocation.Outcome)) : Null();
    result["ambiguous_resends"] = invocation.AmbiguousResends;
    return result;
}

bool Down(EPileVerdict verdict) {
    return verdict == EPileVerdict::Failed || verdict == EPileVerdict::Lost;
}

TString ActionLabel(EOperationKind kind) {
    switch (kind) {
        case EOperationKind::Failover: return "Review failover";
        case EOperationKind::Switchover: return "Switchover here";
        case EOperationKind::TakeOffline: return "Take offline";
        case EOperationKind::Rejoin: return "Rejoin";
    }
    return "Unavailable";
}

TString Relative(ui64 age) {
    const ui64 seconds = age / 1000;
    if (seconds < 60) {
        return ::ToString(seconds) + " s ago";
    }
    if (seconds < 3600) {
        return ::ToString(seconds / 60) + " min ago";
    }
    return ::ToString(seconds / 3600) + " h ago";
}

TString HealthText(const TJson& card) {
    const auto assessment = card["assessment"].GetString();
    if (assessment == "not_assessed") {
        return "Not assessed";
    }
    if (assessment == "expired") {
        const auto& age = card["evidence"]["expired_for_ms"];
        return age.IsUInteger() || age.IsInteger() ? "Health unknown · " + TString(age.GetUInteger() < 1000
            ? "Expired just now" : "expired " + Relative(age.GetUInteger())) : "Health unknown · Expiry time unavailable";
    }
    if (assessment != "available") {
        return "Assessment unavailable";
    }
    const auto verdict = card["verdict"].GetString();
    TString label = verdict == "HEALTHY" ? "Healthy" : verdict == "FAILED" ? "Failed"
        : verdict == "LOST" ? "Lost" : verdict == "REPLICATING" ? "Replicating" : "Unknown";
    if (verdict == "FAILED" || verdict == "LOST") {
        label += card["confirmed"].GetBoolean() ? " · confirmed" : " · Confirming failure";
    }
    return label;
}

TString ModeText(const TJson& state) {
    const auto mode = state["mode"].GetString();
    return mode == "manual" ? "Manual · one reviewed request per approval."
        : mode == "auto" ? "Auto · policy-approved failover enabled."
        : "Observe · collection continues; no new dispatch.";
}

TString DecisionText(const TJson& state) {
    if (!state["dispatch_refusal"].IsNull()) {
        return state["dispatch_refusal"].GetString();
    }
    if (!state["pending_mutation"].IsNull()) {
        return state["pending_mutation"]["message"].GetString();
    }
    const auto kind = state["decision"]["kind"].GetString();
    if (kind == "READY") {
        const auto mode = state["mode"].GetString();
        return mode == "manual" ? "Plan ready · awaiting operator execution."
            : mode == "auto" ? "Automatic dispatch enabled; inspect actual send and retry status."
            : "Plan ready · not dispatched.";
    }
    if (kind == "STABLE") {
        return "No failover proposed.";
    }
    if (kind == "CONFIRMING") {
        return "Confirming failure; waiting for qualifying evidence.";
    }
    if (kind == "BLOCKED") {
        return "Actions blocked: " + state["decision"]["reason"].GetString();
    }
    return "Automatic planning cannot decide; operator failover is outside this console.";
}

} // namespace

NJson::TJsonValue BuildPresentation(const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    const auto& pending = snapshot.PersistentState.PendingMutation;
    const bool matchingPendingAttempt = pending && snapshot.LastMutation
        && snapshot.LastMutation->Plan.ExpectedGeneration == pending->ExpectedGeneration
        && snapshot.LastMutation->Plan.Update == pending->Update
        && snapshot.LastMutation->Kind == pending->Kind
        && snapshot.LastMutation->Origin == pending->Origin;
    auto result = Object();
    result["schema"] = 3;
    result["cluster_id"] = snapshot.ClusterId;
    result["mode"] = ToString(snapshot.Mode);
    result["mode_text"] = ModeText(result);
    result["controller_boot_id"] = snapshot.ControllerBootId;
    result["mode_revision"] = ::ToString(snapshot.ModeRevision);
    result["snapshot_revision"] = snapshot.SnapshotRevision;
    result["controller_ready"] = snapshot.ControllerReady && !snapshot.Stopping;
    result["now"] = wallNow.ToString();
    result["started_at"] = snapshot.BootUtc ? TJson(snapshot.BootUtc.ToString()) : Null();
    result["loop_age_ms"] = Age(snapshot.LastTickAt, now);
    const auto& observed = snapshot.Selection.View ? snapshot.Selection.View : snapshot.LastObservedView;
    result["view"] = snapshot.Selection.View ? View(*snapshot.Selection.View, snapshot, now, wallNow) : Null();
    result["last_observed_view"] = observed ? View(*observed, snapshot, now, wallNow) : Null();
    result["generation_high_water"] = ::ToString(snapshot.GenerationHighWater);
    result["evidence_ttl_ms"] = snapshot.EvidenceTtl.MilliSeconds();
    const auto viewAge = observed ? AgeMilliseconds(observed->SentAt, now) : std::nullopt;
    result["state_age_text"] = !viewAge ? "State unavailable" : TString(*viewAge >= snapshot.EvidenceTtl.MilliSeconds()
        ? "State stale · " : snapshot.Selection.View ? "State: " : "Last state: ") + Relative(*viewAge);
    const auto loopAge = AgeMilliseconds(snapshot.LastTickAt, now);
    result["loop_age_text"] = loopAge ? "Loop: " + Relative(*loopAge) : "Controller loop unavailable";
    result["endpoints"] = Array();
    std::vector<const TEndpointSnapshot*> endpoints;
    for (const auto& endpoint : snapshot.Endpoints) {
        endpoints.push_back(&endpoint);
    }
    std::stable_sort(endpoints.begin(), endpoints.end(), [](const auto* left, const auto* right) {
        return left->Uri < right->Uri;
    });
    for (const auto* endpoint : endpoints) {
        result["endpoints"].AppendValue(Endpoint(*endpoint, snapshot, now, wallNow));
    }
    result["piles"] = Array();
    for (const auto& [name, pile] : snapshot.Piles) {
        result["piles"].AppendValue(Pile(name, pile, snapshot, now, wallNow));
    }
    result["last_known_piles"] = Array();
    result["decision"] = Decision(snapshot.Decision);
    result["pending_mutation"] = Null();
    if (pending) {
        auto item = Object();
        item["expected_generation"] = ::ToString(pending->ExpectedGeneration);
        item["update"] = UpdateJson(pending->Update);
        item["kind"] = ToString(pending->Kind);
        item["origin"] = ToString(pending->Origin);
        item["may_apply_elsewhere"] = snapshot.MayApplyElsewhere;
        item["attempt_count"] = snapshot.PendingAttempts;
        item["first_sent_at"] = snapshot.PendingFirstSendUtc ? TJson(snapshot.PendingFirstSendUtc->ToString()) : Null();
        item["last_sent_at"] = snapshot.PendingLastSendUtc ? TJson(snapshot.PendingLastSendUtc->ToString()) : Null();
        item["attempt_coverage"] = "current_boot_only";
        item["latest_status"] = matchingPendingAttempt && snapshot.LastMutation->Status ? TJson(*snapshot.LastMutation->Status) : Null();
        item["latest_outcome"] = matchingPendingAttempt && snapshot.LastMutation->Outcome ? TJson(ToString(*snapshot.LastMutation->Outcome)) : Null();
        TString message;
        if (matchingPendingAttempt) {
            message = "Request unresolved; an earlier attempt may still complete.";
        } else if (snapshot.PendingAttempts == 0 && !snapshot.MayApplyElsewhere) {
            message = "Recorded but not sent. No request has been handed off for this pending update.";
        } else if (snapshot.LastMutation) {
            message = "Current pending update differs from the latest recorded attempt. Earlier requests may still complete.";
        } else {
            message = "Recorded; no send in this process. Dispatch may have occurred before restart; request may still complete.";
        }
        if (!snapshot.StateWriteError.empty()) {
            message += " State write not confirmed durable; no new dispatch until saving succeeds.";
        }
        item["message"] = std::move(message);
        result["pending_mutation"] = std::move(item);
    }
    result["state_write_error"] = Diagnostic(snapshot.StateWriteError);
    result["save_warning"] = snapshot.StateWriteError.empty() ? Null() : TJson(
        "Saving failed: " + snapshot.StateWriteError + ". No dispatch while state writes fail. A restart before a successful save resumes whichever mode the state file holds, possibly the previous one.");
    result["last_mutation"] = snapshot.LastMutation ? Invocation(*snapshot.LastMutation, snapshot, now, wallNow) : Null();
    result["rejected_generation"] = snapshot.RejectedGeneration ? TJson(::ToString(*snapshot.RejectedGeneration)) : Null();
    result["lifecycle_rejected_generation"] = snapshot.LifecycleRejectedGeneration ? TJson(::ToString(*snapshot.LifecycleRejectedGeneration)) : Null();
    // Observation gaps do not clear process-local rejection latches.
    const bool rejectedFailover = observed && snapshot.RejectedGeneration == observed->Generation;
    const bool rejectedLifecycle = observed && snapshot.LifecycleRejectedGeneration == observed->Generation;
    result["dispatch_refusal"] = rejectedFailover
        ? TJson("Refused by server at generation " + ::ToString(*snapshot.RejectedGeneration)
            + "; no sends at this generation until a newer generation or restart.") : Null();
    result["auto_activation_text"] = "Auto dispatches policy-approved failover and guarded retries. "
        + (result["dispatch_refusal"].IsNull() ? TString("A Ready plan may be dispatched immediately.")
            : result["dispatch_refusal"].GetString());
    result["retry_at"] = snapshot.RetryAt ? Timestamp(*snapshot.RetryAt, snapshot, now, wallNow) : Null();
    result["decision_text"] = DecisionText(result);
    result["operation_details"] = Object();
    result["operation_details"]["pending"] = result["pending_mutation"];
    result["operation_details"]["last_attempt"] = result["last_mutation"];
    result["operation_details"]["next_retry_at"] = result["retry_at"];
    result["operation_message"] = Null();
    if (pending && snapshot.LastMutation && !matchingPendingAttempt) {
        result["operation_message"] = "Current pending update does not match the latest recorded attempt. See attempt details; earlier requests may still complete.";
    } else if (snapshot.LastMutation) {
        const auto& invocation = *snapshot.LastMutation;
        TString message = "Request handed off; awaiting RPC outcome. It may still complete.";
        if (invocation.Outcome) {
            switch (*invocation.Outcome) {
                case EMutationOutcome::Success:
                    message = "RPC SUCCESS; await observed-state verification. Success does not prove invocation causality or application readiness.";
                    break;
                case EMutationOutcome::GenerationMoved:
                    message = "Generation moved; obtain a new reviewed plan.";
                    break;
                case EMutationOutcome::NotAppliedHere:
                    message = "This attempt did not apply here. Earlier attempts may remain uncertain; Manual requires a new review.";
                    break;
                case EMutationOutcome::Rejected:
                    message = snapshot.MayApplyElsewhere ? "This attempt was rejected. An earlier attempt may still complete."
                        : "This attempt was rejected before distconf.";
                    break;
                case EMutationOutcome::Ambiguous:
                    message = "Outcome unknown; request may still complete.";
                    break;
            }
        }
        if (observed && observed->Generation > invocation.Plan.ExpectedGeneration) {
            message += " A newer generation was observed; matching state does not prove this invocation caused the change.";
        }
        result["operation_message"] = std::move(message);
    }
    result["retry_message"] = snapshot.Mode == EMode::Auto ? "Guarded automatic retry only when eligible."
        : "Not scheduled in Observe/Manual.";
    result["cards"] = Array();
    auto incident = Object();
    incident["active"] = false;
    incident["piles"] = Array();
    if (observed) {
        const bool viewFresh = snapshot.Selection.View && !snapshot.Selection.Blocker
            && AgeMilliseconds(observed->SentAt, now).value_or(snapshot.EvidenceTtl.MilliSeconds()) < snapshot.EvidenceTtl.MilliSeconds();
        const TPileParticipation noParticipation;
        const auto& evaluatedParticipation = viewFresh ? snapshot.Participation : noParticipation;
        for (const auto& [name, state] : observed->Piles) {
            const auto current = snapshot.Piles.find(name);
            const TPileSnapshot* assessment = current == snapshot.Piles.end() ? nullptr : &current->second;
            const auto retained = snapshot.LastKnownPiles.find(name);
            if ((!assessment || assessment->Verdict.Verdict == EPileVerdict::Unknown) && retained != snapshot.LastKnownPiles.end()) {
                assessment = &retained->second;
            }
            const auto currentMetadata = current != snapshot.Piles.end()
                ? AssessmentMetadata(current->second, snapshot, now, wallNow) : Null();
            const auto lastKnownMetadata = current != snapshot.Piles.end() && assessment == &current->second
                ? currentMetadata : assessment ? AssessmentMetadata(*assessment, snapshot, now, wallNow) : Null();
            if (assessment && (current == snapshot.Piles.end() || assessment != &current->second)) {
                result["last_known_piles"].AppendValue(Pile(name, *assessment, snapshot, now, wallNow));
            }
            auto card = Object();
            const auto& displayMetadata = currentMetadata.IsNull() ? lastKnownMetadata : currentMetadata;
            if (!displayMetadata.IsNull()) {
                for (const TStringBuf key : {"reasons", "fallback_red", "self_quorum", "down_since", "confirmed"}) {
                    card[TString(key)] = displayMetadata[TString(key)];
                }
            }
            card["current_assessment"] = currentMetadata;
            card["last_known_assessment"] = lastKnownMetadata;
            card["name"] = name;
            card["display_name"] = name.empty() ? "Empty pile name" : name;
            card["state"] = ToString(state);
            card["state_evidence"] = Evidence(observed->SentAt, snapshot, now, wallNow);
            card["state_current"] = viewFresh;
            card["last_known_verdict"] = assessment ? lastKnownMetadata["verdict"] : Null();
            card["evidence"] = assessment ? lastKnownMetadata["evidence"] : Evidence({}, snapshot, now, wallNow);
            card["evidence"]["source"] = "controller_assessment";
            const bool evidenceFresh = assessment && assessment->Verdict.NewestEvidenceSentAt
                && AgeMilliseconds(assessment->Verdict.NewestEvidenceSentAt, now).value_or(snapshot.EvidenceTtl.MilliSeconds()) < snapshot.EvidenceTtl.MilliSeconds();
            const bool assessed = viewFresh && evidenceFresh && current != snapshot.Piles.end()
                && current->second.Verdict.Verdict != EPileVerdict::Unknown;
            std::vector<TConnectivityReply> reports;
            if (viewFresh && current != snapshot.Piles.end()) {
                for (const auto& report : current->second.Connectivity) {
                    if (report.Generation == observed->Generation && report.SentAt
                        && AgeMilliseconds(report.SentAt, now).value_or(snapshot.EvidenceTtl.MilliSeconds()) < snapshot.EvidenceTtl.MilliSeconds())
                    {
                        // Display-only participation fallback does not select a receiving endpoint.
                        reports.push_back({{}, report});
                    }
                }
            }
            auto participating = EvaluatedParticipation(name, state, reports, evaluatedParticipation);
            const auto participationProof = evaluatedParticipation.find(name);
            card["participation_evidence"] = Evidence(participationProof != evaluatedParticipation.end()
                ? participationProof->second.SentAt : TMonotonic{}, snapshot, now, wallNow);
            card["participation_evidence"]["source"] = "controller_participation";
            if (state == EPileState::NotSynchronized && participationProof != evaluatedParticipation.end()
                && (card["participation_evidence"]["age_ms"].IsNull()
                    || card["participation_evidence"]["age_ms"].GetUInteger() >= snapshot.EvidenceTtl.MilliSeconds()))
            {
                participating.reset();
            }
            card["participating"] = participating ? TJson(*participating) : Null();
            const bool excluded = state == EPileState::Disconnected || state == EPileState::Inactive
                || (state == EPileState::NotSynchronized && viewFresh && participating && !*participating);
            card["assessment"] = excluded ? "not_assessed" : assessed ? "available"
                : assessment && assessment->Verdict.NewestEvidenceSentAt && !evidenceFresh ? "expired" : "unavailable";
            card["verdict"] = assessed ? ToString(current->second.Verdict.Verdict) : "UNKNOWN";
            card["incident"] = !excluded && assessment && Down(assessment->Verdict.Verdict);
            if (card["incident"].GetBoolean()) {
                incident["active"] = true;
                incident["piles"].AppendValue(name);
            }
            card["actions"] = Object();
            for (const auto action : {EOperationKind::Switchover, EOperationKind::Failover, EOperationKind::TakeOffline, EOperationKind::Rejoin}) {
                auto availability = Object();
                availability["label"] = ActionLabel(action);
                availability["available"] = false;
                availability["reason"] = "Assessment unavailable";
                // Without a selected view the actor publishes no action map.
                // Keep the known rejection visible without enabling any action.
                if (!snapshot.Selection.View && (rejectedFailover || (action != EOperationKind::Failover && rejectedLifecycle))) {
                    availability["reason"] = !snapshot.ControllerReady || snapshot.Stopping ? "Controller unavailable"
                        : snapshot.Mode != EMode::Manual ? "Enable Manual mode for reviewed execution" : "REJECTED_AT_GENERATION";
                }
                if (const auto pileActions = snapshot.Actions.find(name); pileActions != snapshot.Actions.end()) {
                    if (const auto found = pileActions->second.find(action); found != pileActions->second.end()) {
                        const bool ready = found->second.Kind == EDecisionKind::Ready && found->second.Plan;
                        availability["available"] = ready && snapshot.Mode == EMode::Manual && snapshot.ControllerReady && !snapshot.Stopping && viewFresh;
                        availability["reason"] = !snapshot.ControllerReady || snapshot.Stopping ? "Controller unavailable"
                            : snapshot.Mode != EMode::Manual ? "Enable Manual mode for reviewed execution"
                            : ready && !viewFresh ? "Fresh coherent observations required" : found->second.Reason;
                    }
                }
                if (action == EOperationKind::Failover && snapshot.Decision.Plan && !snapshot.Decision.Plan->Update.contains(name)) {
                    availability["available"] = false;
                    availability["reason"] = "Pile is not affected by a Ready failover plan";
                }
                card["actions"][ToString(action)] = std::move(availability);
            }
            card["proposed"] = Null();
            if (snapshot.Decision.Plan) {
                if (const auto entry = snapshot.Decision.Plan->Update.find(name); entry != snapshot.Decision.Plan->Update.end()) {
                    card["proposed"] = entry->second == EPileState::Disconnected ? "Proposed disconnect"
                        : entry->second == EPileState::Primary ? "Proposed new primary" : "Proposed role reset";
                }
            }
            card["context_action"] = !card["proposed"].IsNull() ? TJson("failover")
                : state == EPileState::Disconnected ? TJson("rejoin")
                : state == EPileState::Synchronized ? TJson("switchover") : Null();
            card["health_text"] = HealthText(card);
            const auto& evidenceAge = card["evidence"]["age_ms"];
            card["evidence_age_text"] = evidenceAge.IsNull() ? "Expiry time unavailable"
                : "Last sample " + Relative(evidenceAge.GetUInteger());
            card["endpoint_uris"] = Array();
            for (const auto& endpoint : result["endpoints"].GetArray()) {
                if (endpoint["pile"].GetString() == name) {
                    card["endpoint_uris"].AppendValue(endpoint["uri"]);
                }
            }
            result["cards"].AppendValue(std::move(card));
        }
    }
    auto failoverAction = Object();
    failoverAction["label"] = ActionLabel(EOperationKind::Failover);
    failoverAction["available"] = false;
    failoverAction["reason"] = "No Ready failover plan";
    if (snapshot.Decision.Plan || rejectedFailover) {
        for (const auto& card : result["cards"].GetArray()) {
            if (snapshot.Decision.Plan && !snapshot.Decision.Plan->Update.contains(card["name"].GetString())) {
                continue;
            }
            failoverAction = card["actions"][ToString(EOperationKind::Failover)];
            if (failoverAction["available"].GetBoolean()) {
                break;
            }
        }
    }
    result["failover_action"] = std::move(failoverAction);
    result["incident"] = std::move(incident);
    result["history"] = Object();
    result["history"]["rows"] = Array();
    result["logs"] = Object();
    result["logs"]["rows"] = Array();
    if (snapshot.Diagnostics) {
        result["history"] = snapshot.Diagnostics->ReadHistory({}, 3);
        result["logs"] = snapshot.Diagnostics->ReadLogs({}, "info", {}, {}, 100);
        result["logs"]["capture_level"] = snapshot.Diagnostics->CaptureLevel();
    }
    return result;
}

TString RenderStateJson(const TSkipperSnapshot& snapshot, TMonotonic now, TInstant wallNow) {
    return NJson::WriteJson(BuildPresentation(snapshot, now, wallNow), false, true);
}

} // namespace NBridgeSkipper
