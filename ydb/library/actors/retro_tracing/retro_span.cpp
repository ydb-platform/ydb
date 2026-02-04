#include "retro_span.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NRetroTracing {

TRetroSpan::TRetroSpan(ui32 type, ui32 size)
    : Type(type)
    , Size(size)
{}

ui32 TRetroSpan::GetType() const {
    return Type;
}

ui32 TRetroSpan::GetSize() const {
    return Size;
}

const void* TRetroSpan::GetData() const {
    return reinterpret_cast<const void*>(this);
}

void* TRetroSpan::GetDataMut() {
    return reinterpret_cast<void*>(this);
}

NWilson::TTraceId TRetroSpan::GetParentId() const {
    return NWilson::TTraceId(ParentId);
}

NWilson::TTraceId TRetroSpan::GetTraceId() const {
    return NWilson::TTraceId(SpanId);
}

void TRetroSpan::AttachToTrace(const NWilson::TTraceId& parentId) {
    ParentId = NWilson::TTraceId(parentId);
    SpanId = ParentId.Span(DefaultVerbosity);
}

TRetroSpan* TRetroSpan::Deserialize(const char* data) {
    const TRetroSpan* base = reinterpret_cast<const TRetroSpan*>(data);
    return TRetroSpan::DeserializeImpl(base->GetType(), base->GetSize(), data);
}

std::unique_ptr<TRetroSpan> TRetroSpan::DeserializeToUnique(const char* data) {
    return std::unique_ptr<TRetroSpan>(TRetroSpan::Deserialize(data));
}

std::shared_ptr<TRetroSpan> TRetroSpan::DeserializeToShared(const char* data) {
    return std::shared_ptr<TRetroSpan>(TRetroSpan::Deserialize(data));
}

void TRetroSpan::Serialize(void* destination) const {
    std::memcpy(destination, GetData(), GetSize());
}

std::unique_ptr<NWilson::TSpan> TRetroSpan::MakeWilsonSpan() {
    std::unique_ptr<NWilson::TSpan> res = std::make_unique<NWilson::TSpan>(
            NWilson::TSpan::ConstructTerminated(GetParentId(), GetTraceId(),
                    GetStartTs(), GetEndTs(), GetName()));
    return nullptr;
}

bool TRetroSpan::IsEnded() const {
    return EndTs != TInstant::Zero();
}

TInstant TRetroSpan::GetStartTs() const {
    return StartTs;
}

TInstant TRetroSpan::GetEndTs() const {
    return EndTs;
}

} // namespace NRetroTracing
