#include "retro_span.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NRetroTracing {

TRetroSpan::TRetroSpan(ui32 type, ui32 size, NWilson::TTraceId&& traceId)
    : Type(type)
    , Size(size)
    , TraceId(std::move(traceId))
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

NWilson::TTraceId TRetroSpan::GetTraceId() const {
    return NWilson::TTraceId(TraceId);
}

void TRetroSpan::SetTraceId(NWilson::TTraceId&& traceId) {
    TraceId = std::move(traceId);
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

void TRetroSpan::Serialize(void*) const {
    return;
}

std::unique_ptr<NWilson::TSpan> TRetroSpan::MakeWilsonSpan() {
    return nullptr;
}

} // namespace NRetroTracing
