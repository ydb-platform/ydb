#include "retro_span.h"
#include "retro_tracing.h"

#include <util/random/random.h>

namespace NRetro {

/// global

TTraceId NewTraceId() {
    return RandomNumber<ui64>();
}

TSpanId NewSpanId() {
    return RandomNumber<ui64>();
}

void FillSpanId(const TFullSpanId& id, NRetroProto::TSpanId* proto) {
    proto->SetTraceId(id.TraceId);
    proto->SetSpanId(id.SpanId);
}

TFullSpanId SpanIdToFullSpanId(const NRetroProto::TSpanId& proto) {
    return TFullSpanId(proto.GetTraceId(), proto.GetSpanId());
}

/// TFullSpanId

TFullSpanId::TFullSpanId(TTraceId traceId, TSpanId spanId)
    : TraceId(traceId)
    , SpanId(spanId)
{}

/// TRetroSpan

TRetroSpan::TRetroSpan(TInstant start)
    : Id(NewTraceId(), NewSpanId())
    , ParentSpanId(std::nullopt)
    , StartTs(start) {
}

TRetroSpan::TRetroSpan(TFullSpanId parentId, TInstant start)
    : Id(parentId.TraceId, NewSpanId())
    , ParentSpanId(parentId.SpanId)
    , StartTs(start) {
}

TFullSpanId TRetroSpan::GetId() const {
    return Id;
}

std::optional<TSpanId> TRetroSpan::GetParentSpanId() const {
    return ParentSpanId;
}

std::vector<ui32> TRetroSpan::GetSubrequestNodeIds() const {
    return {};
}

TString TRetroSpan::GetName() const {
    return "Unknown_RetroSpan";
}

TInstant TRetroSpan::GetStart() const {
    return StartTs;
}

TInstant TRetroSpan::GetEnd() const {
    return EndTs;
}

void TRetroSpan::End(TInstant ts) {
    if (!std::exchange(Ended, true)) {
        EndTs = ts;
        WriteRetroSpan(*this);
    }
}

void TRetroSpan::FillWilsonSpanAttributes(NWilson::TSpan* span) const {
    Y_UNUSED(span);
}

TRetroSpan::TPtr TRetroSpan::FromRawData(ui32 type, const void* data) {
    switch (type) {
#define ALLOCATE_MINI_SPAN_OF_TYPE(type)                                    \
        case ERetroSpanType::type:                                          \
            return std::make_unique<TRetroSpan##type>(                      \
                        *reinterpret_cast<const TRetroSpan##type*>(data));

        ALLOCATE_MINI_SPAN_OF_TYPE(DSProxyRequest);
        ALLOCATE_MINI_SPAN_OF_TYPE(BackpressureInFlight);
        ALLOCATE_MINI_SPAN_OF_TYPE(VDiskLogPut);

#undef ALLOCATE_MINI_SPAN_OF_TYPE

        default:
            return nullptr;
    }
}

} // namespace NRetro
