#include "http.h"
#include "age.h"

#include <library/cpp/monlib/encode/prometheus/prometheus.h>
#include <util/stream/str.h>

namespace NBridgeSkipper {
namespace {

using TLabels = std::initializer_list<std::pair<TStringBuf, TStringBuf>>;

constexpr EPileVerdict Verdicts[] = {
    EPileVerdict::Healthy, EPileVerdict::Failed, EPileVerdict::Lost,
    EPileVerdict::Replicating, EPileVerdict::Unknown,
};

constexpr EDecisionKind Decisions[] = {
    EDecisionKind::Stable, EDecisionKind::Unknown, EDecisionKind::Confirming,
    EDecisionKind::Blocked, EDecisionKind::Ready,
};

constexpr EBlockerReason Blockers[] = {
    EBlockerReason::NoView, EBlockerReason::ViewConflict, EBlockerReason::StaleHistory,
    EBlockerReason::PendingMutation, EBlockerReason::NoCandidate,
    EBlockerReason::SuspendedPileLost, EBlockerReason::NoMutationEndpoint, EBlockerReason::StateWriteFailed,
};

constexpr EMutationOutcome Outcomes[] = {
    EMutationOutcome::Success, EMutationOutcome::GenerationMoved, EMutationOutcome::NotAppliedHere,
    EMutationOutcome::Rejected, EMutationOutcome::Ambiguous,
};

void Begin(
    NMonitoring::IMetricEncoder& encoder,
    NMonitoring::EMetricType type,
    TStringBuf name,
    TLabels labels)
{
    encoder.OnMetricBegin(type);
    encoder.OnLabelsBegin();
    encoder.OnLabel("sensor", name);
    for (const auto& [key, value] : labels) {
        encoder.OnLabel(key, value);
    }
    encoder.OnLabelsEnd();
}

void Gauge(NMonitoring::IMetricEncoder& encoder, TStringBuf name, double value, TLabels labels = {}) {
    Begin(encoder, NMonitoring::EMetricType::GAUGE, name, labels);
    encoder.OnDouble(TInstant::Zero(), value);
    encoder.OnMetricEnd();
}

void Counter(NMonitoring::IMetricEncoder& encoder, TStringBuf name, ui64 value, TLabels labels = {}) {
    Begin(encoder, NMonitoring::EMetricType::COUNTER, name, labels);
    encoder.OnUint64(TInstant::Zero(), value);
    encoder.OnMetricEnd();
}

template <typename K>
ui64 Count(const std::map<K, ui64>& counters, K key) {
    const auto found = counters.find(key);
    return found == counters.end() ? 0 : found->second;
}

} // namespace

TString RenderMetrics(const TSkipperSnapshot& snapshot, TMonotonic now) {
    // Every family is emitted contiguously: monlib writes its TYPE line once.
    TStringStream stream;
    auto encoder = NMonitoring::EncoderPrometheus(&stream);
    encoder->OnStreamBegin();
    if (const auto age = AgeMilliseconds(snapshot.LastTickAt, now)) {
        Gauge(*encoder, "skipper_loop_age_seconds", static_cast<double>(*age) / 1000);
    }
    if (const auto& view = snapshot.Selection.View) {
        if (const auto age = AgeMilliseconds(view->SentAt, now)) {
            Gauge(*encoder, "skipper_cluster_state_age_seconds", static_cast<double>(*age) / 1000);
        }
        for (const auto& [pile, state] : view->Piles) {
            (void)state;
            if (const auto found = snapshot.Piles.find(pile); found != snapshot.Piles.end()) {
                for (const auto candidate : Verdicts) {
                    Gauge(*encoder, "skipper_pile_verdict", found->second.Verdict.Verdict == candidate,
                        {{"pile", pile}, {"verdict", ToString(candidate)}});
                }
            }
        }
    }
    for (const auto kind : Decisions) {
        Gauge(*encoder, "skipper_decision", snapshot.Decision.Kind == kind, {{"kind", ToString(kind)}});
    }
    for (const auto reason : Blockers) {
        Gauge(*encoder, "skipper_blocked", snapshot.Decision.Kind == EDecisionKind::Blocked
            && snapshot.Decision.Blocker == reason, {{"reason", ToString(reason)}});
    }
    Gauge(*encoder, "skipper_pending_mutation", snapshot.PersistentState.PendingMutation.has_value());
    for (const auto outcome : Outcomes) {
        Counter(*encoder, "skipper_mutation_total", Count(snapshot.Counters.MutationTotal, outcome), {{"outcome", ToString(outcome)}});
    }
    Counter(*encoder, "skipper_unexplained_state_change_total", snapshot.Counters.UnexplainedStateChanges);
    // The actor records only closed status names (MetricStatus).
    for (const auto& [key, count] : snapshot.Counters.RpcTotal) {
        Counter(*encoder, "skipper_rpc_total", count, {{"method", ToString(key.first)}, {"status", key.second}});
    }
    encoder->OnStreamEnd();
    encoder->Close();
    return stream.Str();
}

} // namespace NBridgeSkipper
