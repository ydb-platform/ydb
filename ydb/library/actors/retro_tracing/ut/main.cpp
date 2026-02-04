#include <library/cpp/testing/unittest/registar.h>
#include <util/random/random.h>
#include <util/stream/null.h>

#include <ydb/library/actors/retro_tracing/universal_span.h>

#include "test_spans.h"

#define Ctest Cnull

namespace NKikimr {

Y_UNIT_TEST_SUITE(UniversalSpan) {
    Y_UNIT_TEST(RetroSpan) {
        const ui8 verbosity = 1;
        const ui32 ttl = Max<ui32>();

        NWilson::TTraceId parentId = NWilson::TTraceId::NewTraceId(verbosity, ttl, true);

        TUniversalSpan<TTestSpan1> span(verbosity, parentId, "Test1");
        UNIT_ASSERT(span.GetSpanType() == EUniversalSpanType::Retro);
        UNIT_ASSERT(span.GetTraceId() != NWilson::TTraceId{});
        UNIT_ASSERT(span.GetRetroSpanPtr()->GetName().find("TTestSpan1") != std::string::npos);
        Ctest << span.GetRetroSpanPtr()->GetName() << Endl;
    }

    Y_UNIT_TEST(Serialization) {
        const ui32 bufferSize = 10_KB;
        const ui8 verbosity = 1;
        const ui32 ttl = Max<ui32>();
        char buffer[bufferSize];

        NWilson::TTraceId parentId = NWilson::TTraceId::NewTraceId(verbosity, ttl, true);

        {
            TUniversalSpan<TTestSpan1> span(verbosity, parentId, "Test1");
            UNIT_ASSERT(span.GetSpanType() == EUniversalSpanType::Retro);
            TInstant start = span.GetRetroSpanPtr()->GetStartTs();

            NWilson::TTraceId spanTraceId = span.GetTraceId();
            UNIT_ASSERT(spanTraceId);

            const ui64 var1 = RandomNumber<ui64>();
            const ui32 var2 = RandomNumber<ui32>();
            span.GetRetroSpanPtr()->Var1 = var1;
            span.GetRetroSpanPtr()->Var2 = var2;

            Sleep(TDuration::Seconds(3));
            span.GetRetroSpanPtr()->End();
            TInstant end = span.GetRetroSpanPtr()->GetEndTs();

            UNIT_ASSERT(end - start > TDuration::Seconds(2));
            span.GetRetroSpanPtr()->Serialize(buffer);

            std::unique_ptr<NRetroTracing::TRetroSpan> deserialized =
                    NRetroTracing::TRetroSpan::DeserializeToUnique(buffer);
            const TTestSpan1* deserializedPtr = deserialized->Cast<TTestSpan1>();
            UNIT_ASSERT(deserializedPtr);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->GetStartTs(), start);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->GetEndTs(), end);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var1, var1);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var2, var2);
            UNIT_ASSERT(spanTraceId == deserializedPtr->GetTraceId());
        }

        {
            TUniversalSpan<TTestSpan2> span(verbosity, parentId, "Test2");
            UNIT_ASSERT(span.GetSpanType() == EUniversalSpanType::Retro);
            TInstant start = span.GetRetroSpanPtr()->GetStartTs();

            NWilson::TTraceId spanTraceId = span.GetTraceId();
            UNIT_ASSERT(spanTraceId);

            const ui8 var3 = RandomNumber<ui8>();
            span.GetRetroSpanPtr()->Var3 = var3;

            Sleep(TDuration::Seconds(3));
            span.GetRetroSpanPtr()->End();
            TInstant end = span.GetRetroSpanPtr()->GetEndTs();
            UNIT_ASSERT(end - start > TDuration::Seconds(2));

            span.GetRetroSpanPtr()->Serialize(buffer);

            std::unique_ptr<NRetroTracing::TRetroSpan> deserialized =
                    NRetroTracing::TRetroSpan::DeserializeToUnique(buffer);
            const TTestSpan2* deserializedPtr = deserialized->Cast<TTestSpan2>();
            UNIT_ASSERT(deserializedPtr);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->GetStartTs(), start);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->GetEndTs(), end);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var3, var3);
            UNIT_ASSERT(spanTraceId == deserializedPtr->GetTraceId());
        }
    }
}

};
