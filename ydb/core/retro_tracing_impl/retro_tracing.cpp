#include "named_span.h"

#include <cstring>

#include <ydb/library/actors/retro_tracing/retro_span.h>

NRetroTracing::TRetroSpan* NRetroTracing::TRetroSpan::DeserializeImpl(
        ui32 type, ui32 size, const void* data) {
    switch (type) {
#define SPAN_TYPE(TSpanType, typeId)                                \
        case typeId: {                                              \
            TSpanType res;                                          \
            std::memcpy(reinterpret_cast<void*>(&res), data, size); \
            return new TSpanType(res);                              \
        }

        SPAN_TYPE(NKikimr::TNamedSpan, NKikimr::NamedSpan);

#undef SPAN_TYPE
        default:
            return nullptr;
    }
}
