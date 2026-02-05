#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/random/entropy.h>
#include <util/random/random.h>
#include <util/stream/null.h>

#include <ydb/library/actors/retro_tracing/universal_span.h>
#include <ydb/library/actors/retro_tracing/span_buffer.h>

#include "test_spans.h"

#include <thread>

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

            const ui64 var = RandomNumber<ui64>();
            span.GetRetroSpanPtr()->Var = var;

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
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var, var);
            UNIT_ASSERT(spanTraceId == deserializedPtr->GetTraceId());
        }

        {
            TUniversalSpan<TTestSpan2> span(verbosity, parentId, "Test2");
            UNIT_ASSERT(span.GetSpanType() == EUniversalSpanType::Retro);
            TInstant start = span.GetRetroSpanPtr()->GetStartTs();

            NWilson::TTraceId spanTraceId = span.GetTraceId();
            UNIT_ASSERT(spanTraceId);

            const ui32 var1 = RandomNumber<ui32>();
            const ui64 var2 = RandomNumber<ui64>();
            span.GetRetroSpanPtr()->Var1 = var1;
            span.GetRetroSpanPtr()->Var2 = var2;

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
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var1, var1);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var2, var2);
            UNIT_ASSERT(spanTraceId == deserializedPtr->GetTraceId());
        }
    }
}

Y_UNIT_TEST_SUITE(SpanBuffer) {
    const ui8 Verbosity = 1;

    enum ETestParameters : ui64 {
        AUTO_END    = 0b01,
        MANUAL_END  = 0b10,
    };
    
    void TestSimple(ui64 parameters) {
        const ui32 ttl = Max<ui32>();
        NWilson::TTraceId parentId = NWilson::TTraceId::NewTraceId(Verbosity, ttl, true);
        ui64 var = RandomNumber<ui64>();

        if (parameters & AUTO_END) {
            TUniversalSpan<TTestSpan1> span(Verbosity, parentId, "Test", NWilson::EFlags::AUTO_END);
            span.GetRetroSpanPtr()->Var = var;
            if (parameters & MANUAL_END) {
                span.GetRetroSpanPtr()->End();
            }
        } else {
            TUniversalSpan<TTestSpan1> span(Verbosity, parentId, "Test");
            span.GetRetroSpanPtr()->Var = var;
            span.GetRetroSpanPtr()->End();
        }

        for (ui32 i = 0; i < 10; ++i) {
            std::vector<std::unique_ptr<NRetroTracing::TRetroSpan>> spans = NRetroTracing::GetSpansOfTrace(parentId);
            UNIT_ASSERT_VALUES_EQUAL(spans.size(), 1);
            const TTestSpan1* ptr = spans[0]->Cast<TTestSpan1>();
            UNIT_ASSERT(ptr);
            UNIT_ASSERT_VALUES_EQUAL(ptr->Var, var);
            UNIT_ASSERT_VALUES_UNEQUAL(ptr->GetStartTs(), TInstant::Zero());
            UNIT_ASSERT_VALUES_UNEQUAL(ptr->GetEndTs(), TInstant::Zero());

        }
        NRetroTracing::DropThreadLocalBuffer();
    }

    Y_UNIT_TEST(AutoEnd) {
        std::thread t(TestSimple, AUTO_END);
        t.join();
    }

    Y_UNIT_TEST(ManualEnd) {
        std::thread t(TestSimple, AUTO_END | MANUAL_END);
        t.join();
    }

    Y_UNIT_TEST(Simple) {
        std::thread t(TestSimple, 0);
        t.join();
    }

    constexpr static ui32 BufferSize = 10_MB;
    char Buffer[BufferSize];
    NWilson::TTraceId TraceId;

    ui64 GetValue(ui32 pos) {
        return *reinterpret_cast<ui64*>(Buffer + pos);
    }

    void SpanWriter(TInstant end) {
        while (TInstant::Now() < end) {
            ui32 pos = RandomNumber<ui32>(BufferSize - sizeof(ui64));
            ui64 val = GetValue(pos);
            TUniversalSpan<TTestSpan2> span(Verbosity, TraceId, "Test", NWilson::EFlags::AUTO_END);
            span.GetRetroSpanPtr()->Var1 = pos;
            span.GetRetroSpanPtr()->Var2 = val;
        }
    }

    void SpanReader(TInstant end) {
        while (TInstant::Now() < end) {
            auto spans = NRetroTracing::GetSpansOfTrace(TraceId);
            for (const std::unique_ptr<NRetroTracing::TRetroSpan>& span : spans) {
                const TTestSpan2* ptr = span->Cast<TTestSpan2>();
                UNIT_ASSERT(ptr);
                UNIT_ASSERT_VALUES_EQUAL(ptr->Var2, GetValue(ptr->Var1));
            }
        }
    }

    Y_UNIT_TEST(MultiThreaded) {
        const ui32 ttl = Max<ui32>();
        TraceId = NWilson::TTraceId::NewTraceId(Verbosity, ttl, true);
        EntropyPool().Read(Buffer, BufferSize);

        std::vector<std::thread> threads;
        
        ui32 readers = 2;
        ui32 writers = 10;

        TInstant start = TInstant::Now();
        TInstant end = start + TDuration::Seconds(10);

        for (ui32 i = 0; i < readers; ++i) {
            threads.emplace_back(SpanReader, end);
        }

        for (ui32 i = 0; i < writers; ++i) {
            threads.emplace_back(SpanWriter, end);
        }

        for (std::thread& thread : threads) {
            thread.join();
        }
    }
}

};
