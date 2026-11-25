#pragma once

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/time_provider/monotonic.h>
#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

#include <array>
#include <memory>
#include <mutex>
#include <optional>

namespace NFq::NWasmServices {

enum class EMetricKind {
    Http,
    Grpc,
    Internal
};
enum class EMetricOutcome {
    Success,
    Error,
    Timeout,
    Cancelled
};
enum class EAdmissionReason {
    LocalOperations,
    LocalBytes,
    SharedOperations,
    SharedBytes,
    Closed,
    Deadline
};

// Shared by the executor and all operations. Labels are bounded for the entire
// registry lifetime, including completed calls; no query IDs, URLs or secrets.
class TRequestMetrics {
public:
    struct TSeries {
        NMonitoring::TDynamicCounters::TCounterPtr Started, InFlight, Bytes, CleanupPending;
        std::array<NMonitoring::TDynamicCounters::TCounterPtr, 4> Completed;
        std::array<NMonitoring::TDynamicCounters::TCounterPtr, 6> Rejected;
        NMonitoring::THistogramPtr Duration, QueueDelay, RequestBytes, ResponseBytes, CleanupDuration;
        explicit TSeries(NMonitoring::TDynamicCounterPtr counters);
    };

    // Declare before physical buffers/handles so it is destroyed after them.
    class TOperation {
    public:
        TOperation(std::shared_ptr<TSeries> series, ui64 reservedBytes, std::optional<ui64> payloadBytes);
        ~TOperation();
        void Complete(EMetricOutcome outcome, std::optional<ui64> payloadBytes = {});
        void Queued();
        void Dispatched();

    private:
        const std::shared_ptr<TSeries> Series;
        const ui64 ReservedBytes;
        const TMonotonic Started = TMonotonic::Now();
        std::mutex Mutex;
        std::optional<TMonotonic> Finished, QueuedAt;
    };

    explicit TRequestMetrics(NMonitoring::TDynamicCounterPtr counters, ui32 maxAliasesPerKind = 128);
    std::shared_ptr<TSeries> Get(TStringBuf alias, EMetricKind kind);
    std::shared_ptr<TOperation> Start(TStringBuf alias, EMetricKind kind, ui64 reservedBytes = 0, std::optional<ui64> payloadBytes = {});
    void Reject(const std::shared_ptr<TSeries>& series, EAdmissionReason reason);

private:
    const NMonitoring::TDynamicCounterPtr Counters;
    const ui32 MaxAliases;
    std::mutex Mutex;
    std::array<THashMap<TString, std::shared_ptr<TSeries>>, 3> Series;
};

}   // namespace NFq::NWasmServices
