#include <ydb/library/actors/interconnect/retro_tracing/spans.h>
#include <ydb/library/actors/retro_tracing/collector/retro_span_deserialization.h>
#include <ydb/library/actors/retro_tracing/span/typed_retro_span.h>
#include "test_spans.h"

#include <cstring>

namespace NRetroTracing {

TRetroSpan* DeserializeRetroSpanImpl(ui32 type, ui32 size, const void* data) {
    switch (TSpanTypeNamespace::Get(type)) {
        case TSpanTypeNamespace::INTERCONNECT:
            return NActors::DeserializeInterconnectRetroSpan(type, size, data);
        case TSpanTypeNamespace::USERSPACE: {
            switch (type) {
#define SPAN_TYPE(TSpanType, typeId)                                        \
                case typeId: {                                              \
                    TSpanType res;                                          \
                    std::memcpy(reinterpret_cast<void*>(&res), data, size); \
                    return new TSpanType(res);                              \
                }

                SPAN_TYPE(TTestSpan1, Test1);
                SPAN_TYPE(TTestSpan2, Test2);

#undef SPAN_TYPE
                default:
                    return nullptr;
            }
        }
        default:
            return nullptr;
    }
}

}
