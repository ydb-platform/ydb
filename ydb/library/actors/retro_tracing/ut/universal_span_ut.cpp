#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>

#include <ydb/library/actors/retro_tracing/universal_span.h>

#include "test_spans.h"

namespace NKikimr {

Y_UNIT_TEST_SUITE(UniversalSpan) {
    Y_UNIT_TEST(Serialization) {
        const ui32 bufferSize = 10_KB;
        const ui8 verbosity = 1;
        const ui32 ttl = Max<ui32>();
        char buffer[bufferSize];

        NWilson::TTraceId parentId = NWilson::TTraceId::NewTraceId(verbosity, ttl, true);

        {
            TUniversalSpan<TTestSpan1> span(verbosity, parentId, "Test1");
            const ui64 var1 = RandomNumber<ui64>();
            const ui32 var2 = RandomNumber<ui32>();
            NWilson::TTraceId spanTraceId = span.GetTraceId();
            UNIT_ASSERT(spanTraceId);
            span.GetRetroSpanPtr()->Var1 = var1;
            span.GetRetroSpanPtr()->Var2 = var2;
            span.GetRetroSpanPtr()->Serialize(buffer);

            std::unique_ptr<NRetroTracing::TRetroSpan> deserialized =
                    NRetroTracing::TRetroSpan::DeserializeToUnique(buffer);
            const TTestSpan1* deserializedPtr = deserialized->Cast<TTestSpan1>();
            UNIT_ASSERT(deserializedPtr);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var1, var1);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var2, var2);
            UNIT_ASSERT(spanTraceId == deserializedPtr->GetTraceId());
        }

        {
            TUniversalSpan<TTestSpan2> span(verbosity, parentId, "Test2");
            const ui8 var3 = RandomNumber<ui8>();
            span.GetRetroSpanPtr()->Var3 = var3;
            NWilson::TTraceId spanTraceId = span.GetTraceId();
            UNIT_ASSERT(spanTraceId);
            span.GetRetroSpanPtr()->Serialize(buffer);

            std::unique_ptr<NRetroTracing::TRetroSpan> deserialized =
                    NRetroTracing::TRetroSpan::DeserializeToUnique(buffer);
            const TTestSpan2* deserializedPtr = deserialized->Cast<TTestSpan2>();
            UNIT_ASSERT(deserializedPtr);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var3, var3);
            UNIT_ASSERT(spanTraceId == deserializedPtr->GetTraceId());
        }
    }
}

};
