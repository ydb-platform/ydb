#include "tracing.h"

namespace NFq::NWasmServices {
TTraceContext TTraceContext::Create(NWilson::TTraceId parent, NActors::TActorSystem* system, ui32 limit) {
    if (!parent) {
        return {};
    }
    return { std::make_shared<const NWilson::TTraceId>(std::move(parent)), std::make_shared<TBudget>(limit), system };
}

std::shared_ptr<TCallTrace> TCallTrace::Start(const TTraceContext& context, const char* name) {
    if (!context.Parent || !context.Parent->IsWilsonTrace() || context.Parent->GetVerbosity() < 1 || !context.Parent->GetTimeToLive() ||
        !context.Budget) {
        return {};
    }
    auto remaining = context.Budget->Remaining.load();
    while (remaining && !context.Budget->Remaining.compare_exchange_weak(remaining, remaining - 1)) {
    }
    if (!remaining) {
        ++context.Budget->Dropped;
        return {};
    }
    return std::shared_ptr<TCallTrace>(new TCallTrace(context, name));
}

TCallTrace::TCallTrace(const TTraceContext& context, const char* name)
    : Parent(context)
    , Span(1, context.Parent->Clone(), name, NWilson::EFlags::NONE, context.System)
{
}

TCallTrace::~TCallTrace() {
    Complete(EMetricOutcome::Cancelled);
}

TTraceContext TCallTrace::Context() const {
    std::lock_guard lock(Mutex);
    if (!Span) {
        return {};
    }
    return { std::make_shared<const NWilson::TTraceId>(Span.GetTraceId()), Parent.Budget, Parent.System };
}

void TCallTrace::Attribute(const char* key, NWilson::TAttributeValue value) {
    std::lock_guard lock(Mutex);
    if (Span) {
        Span.Attribute(key, std::move(value));
    }
}

void TCallTrace::Complete(EMetricOutcome outcome, std::optional<ui64> payload, int code, int error) {
    std::lock_guard lock(Mutex);
    if (!Span) {
        return;
    }
    static constexpr const char* names[] = { "success", "error", "timeout", "cancelled" };
    Span.Attribute("outcome", TString(names[static_cast<size_t>(outcome)]));
    Span.Attribute("queue_delay_us", i64(QueueDelayUs));
    if (payload) {
        Span.Attribute("response_payload_bytes", i64(*payload));
    }
    if (code >= 0) {
        Span.Attribute("response_code", i64(code));
    }
    if (error >= 0) {
        Span.Attribute("error_class", i64(error));
    }
    if (outcome == EMetricOutcome::Success) {
        Span.EndOk();
    } else {
        Span.EndError(names[static_cast<size_t>(outcome)]);
    }
}

void TCallTrace::Queued() {
    std::lock_guard lock(Mutex);
    if (Span && !QueuedAt) {
        QueuedAt = TMonotonic::Now();
    }
}

void TCallTrace::Dispatched() {
    std::lock_guard lock(Mutex);
    if (Span && QueuedAt) {
        QueueDelayUs += (TMonotonic::Now() - *QueuedAt).MicroSeconds();
    }
    QueuedAt.reset();
}
}   // namespace NFq::NWasmServices
