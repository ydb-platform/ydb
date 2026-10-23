#include "metrics.h"
#include "wire.h"

namespace NFq::NWasmServices {
namespace {
constexpr const char* Kinds[] = { "http", "grpc", "internal" };
constexpr const char* Outcomes[] = { "success", "error", "timeout", "cancelled" };
constexpr const char* Reasons[] = { "local_operations", "local_bytes", "shared_operations", "shared_bytes", "closed", "deadline" };

NMonitoring::THistogramPtr TimeHistogram(NMonitoring::TDynamicCounterPtr counters, const char* name) {
    return counters->GetHistogram(name, NMonitoring::ExplicitHistogram({ 0, 1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000, 30000, 120000 }));
}
}   // namespace

TRequestMetrics::TSeries::TSeries(NMonitoring::TDynamicCounterPtr counters) {
    Started = counters->GetCounter("RequestsStarted", true);
    InFlight = counters->GetCounter("RequestsInFlight");
    Bytes = counters->GetCounter("ReservedBytes");
    CleanupPending = counters->GetCounter("CleanupPending");
    for (size_t i = 0; i < Completed.size(); ++i) {
        Completed[i] = counters->GetSubgroup("outcome", Outcomes[i])->GetCounter("RequestsCompleted", true);
    }
    for (size_t i = 0; i < Rejected.size(); ++i) {
        Rejected[i] = counters->GetSubgroup("reason", Reasons[i])->GetCounter("AdmissionRejected", true);
    }
    Duration = TimeHistogram(counters, "RequestDurationMs");
    QueueDelay = TimeHistogram(counters, "QueueDelayMs");
    CleanupDuration = TimeHistogram(counters, "CleanupDurationMs");
    RequestBytes = counters->GetHistogram("RequestPayloadBytes", NMonitoring::ExponentialHistogram(16, 2, 64));
    ResponseBytes = counters->GetHistogram("ResponsePayloadBytes", NMonitoring::ExponentialHistogram(16, 2, 64));
}

TRequestMetrics::TOperation::TOperation(std::shared_ptr<TSeries> series, ui64 bytes, std::optional<ui64> payload)
    : Series(std::move(series))
    , ReservedBytes(bytes)
{
    ++*Series->Started;
    ++*Series->InFlight;
    Series->Bytes->Add(bytes);
    if (payload) {
        Series->RequestBytes->Collect(*payload);
    }
}

void TRequestMetrics::TOperation::Complete(EMetricOutcome outcome, std::optional<ui64> payload) {
    std::lock_guard lock(Mutex);
    if (Finished) {
        return;
    }
    Finished = TMonotonic::Now();
    --*Series->InFlight;
    ++*Series->Completed[static_cast<size_t>(outcome)];
    ++*Series->CleanupPending;
    Series->Duration->Collect((*Finished - Started).MilliSeconds());
    if (payload) {
        Series->ResponseBytes->Collect(*payload);
    }
}

TRequestMetrics::TOperation::~TOperation() {
    // Hard shutdown may suppress owner callbacks, but still terminates accounting.
    Complete(EMetricOutcome::Cancelled);
    Series->Bytes->Sub(ReservedBytes);
    --*Series->CleanupPending;
    Series->CleanupDuration->Collect((TMonotonic::Now() - *Finished).MilliSeconds());
}

void TRequestMetrics::TOperation::Queued() {
    std::lock_guard lock(Mutex);
    if (!Finished && !QueuedAt) {
        QueuedAt = TMonotonic::Now();
    }
}

void TRequestMetrics::TOperation::Dispatched() {
    std::lock_guard lock(Mutex);
    if (QueuedAt && !Finished) {
        Series->QueueDelay->Collect((TMonotonic::Now() - *QueuedAt).MilliSeconds());
    }
    QueuedAt.reset();
}

TRequestMetrics::TRequestMetrics(NMonitoring::TDynamicCounterPtr counters, ui32 maxAliasesPerKind)
    : Counters(std::move(counters))
    , MaxAliases(maxAliasesPerKind)
{
    // Overflow/anonymous groups cannot be starved by user supplied aliases.
    for (size_t i = 0; i < Series.size(); ++i) {
        for (const char* alias : { "unlabeled", "other" }) {
            auto series = std::make_shared<TSeries>(Counters->GetSubgroup("protocol", Kinds[i])->GetSubgroup("call_alias", alias));
            series->Alias = alias;
            Series[i].emplace(alias, std::move(series));
        }
    }
}

std::shared_ptr<TRequestMetrics::TSeries> TRequestMetrics::Get(TStringBuf alias, EMetricKind kind) {
    const auto normalized = MonitoringAlias({ alias.data(), alias.size() });
    TString key = normalized.empty() ? TString("unlabeled") : TString(normalized.data(), normalized.size());
    std::lock_guard lock(Mutex);
    auto& map = Series[static_cast<size_t>(kind)];
    if (const auto it = map.find(key); it != map.end()) {
        return it->second;
    }
    if (map.size() - 2 >= MaxAliases) {
        return map.at("other");
    }
    auto series = std::make_shared<TSeries>(Counters->GetSubgroup("protocol", Kinds[static_cast<size_t>(kind)])->GetSubgroup("call_alias", key));
    series->Alias = key;
    return map.emplace(key, std::move(series)).first->second;
}

std::shared_ptr<TRequestMetrics::TOperation> TRequestMetrics::Start(
    TStringBuf alias, EMetricKind kind, ui64 bytes, std::optional<ui64> payload) {
    return std::make_shared<TOperation>(Get(alias, kind), bytes, payload);
}

void TRequestMetrics::Reject(const std::shared_ptr<TSeries>& series, EAdmissionReason reason) {
    ++*series->Rejected[static_cast<size_t>(reason)];
}
}   // namespace NFq::NWasmServices
