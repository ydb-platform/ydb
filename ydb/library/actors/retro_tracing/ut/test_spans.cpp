#include <ydb/library/actors/retro_tracing/retro_span.h>
#include "test_spans.h"

namespace NRetroTracing {

TRetroSpan* TRetroSpan::DeserializeImpl(ui32 type, ui32/* size*/, const void* data) {

    switch (type) {
#define SPAN_TYPE(typeId, TSpanType)                                        \
        case typeId:                                                        \
            return new TSpanType(*reinterpret_cast<const TSpanType*>(data))

        SPAN_TYPE(Test1, TTestSpan1);
        SPAN_TYPE(Test2, TTestSpan2);

#undef SPAN_TYPE
        default:
            return nullptr;
    }
}

}
