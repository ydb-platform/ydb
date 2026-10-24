#pragma once

#include "metrics.h"

#include <ydb/library/actors/wilson/wilson_span.h>

#include <atomic>

namespace NFq::NWasmServices {

// Explicit immutable parent context, shared bounded budget per transform.
// Never stored in the shared executor or in guest memory.
struct TTraceContext {
    struct TBudget {
        std::atomic<ui32> Remaining;
        std::atomic<ui64> Dropped{ 0 };

        explicit TBudget(ui32 limit)
            : Remaining(limit)
        {
        }
    };

    std::shared_ptr<const NWilson::TTraceId> Parent;
    std::shared_ptr<TBudget> Budget;
    NActors::TActorSystem* System = nullptr;

    static TTraceContext Create(NWilson::TTraceId parent, NActors::TActorSystem* system, ui32 limit = 4096);
};

// Wilson spans are mutable and not thread safe. Cancellation can run off-pool;
// serialize all writes and logical completion, just like request metrics.
class TCallTrace {
public:
    static std::shared_ptr<TCallTrace> Start(const TTraceContext& context, const char* name);
    ~TCallTrace();
    TTraceContext Context() const;
    void Attribute(const char* key, NWilson::TAttributeValue value);
    void Complete(EMetricOutcome outcome, std::optional<ui64> payload = {}, int code = -1, int error = -1);
    void Queued();
    void Dispatched();

private:
    explicit TCallTrace(const TTraceContext& context, const char* name);
    const TTraceContext Parent;
    mutable std::mutex Mutex;
    NWilson::TSpan Span;
    std::optional<TMonotonic> QueuedAt;
    ui64 QueueDelayUs = 0;
};

}   // namespace NFq::NWasmServices
