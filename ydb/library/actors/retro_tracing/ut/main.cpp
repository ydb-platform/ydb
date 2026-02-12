#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
#include <util/random/entropy.h>
#include <util/random/random.h>
#include <util/stream/null.h>

#include <ydb/library/actors/retro_tracing/retro_collector.h>
#include <ydb/library/actors/retro_tracing/span_buffer.h>
#include <ydb/library/actors/retro_tracing/universal_span.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/wilson/test_util/fake_wilson_uploader.h>

#include "test_spans.h"

#include <thread>

#define Ctest Cnull

namespace NRetroTracing {

Y_UNIT_TEST_SUITE(UniversalSpan) {
    Y_UNIT_TEST(RetroSpan) {
        const ui8 verbosity = 1;
        const ui32 ttl = Max<ui32>();

        NWilson::TTraceId parentId = NWilson::TTraceId::NewTraceId(verbosity, ttl, true);

        TUniversalSpan<TTestSpan1> span(verbosity, NWilson::TTraceId(parentId), "Test1");
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
            TUniversalSpan<TTestSpan1> span(verbosity, NWilson::TTraceId(parentId), "Test1");
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

            std::unique_ptr<TRetroSpan> deserialized = TRetroSpan::DeserializeToUnique(buffer);
            const TTestSpan1* deserializedPtr = deserialized->Cast<TTestSpan1>();
            UNIT_ASSERT(deserializedPtr);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->GetStartTs(), start);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->GetEndTs(), end);
            UNIT_ASSERT_VALUES_EQUAL(deserializedPtr->Var, var);
            UNIT_ASSERT(spanTraceId == deserializedPtr->GetTraceId());
        }

        {
            TUniversalSpan<TTestSpan2> span(verbosity, NWilson::TTraceId(parentId), "Test2");
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

            std::unique_ptr<TRetroSpan> deserialized =
                    TRetroSpan::DeserializeToUnique(buffer);
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
            TUniversalSpan<TTestSpan1> span(Verbosity, NWilson::TTraceId(parentId), "Test",
                    NWilson::EFlags::AUTO_END);
            span.GetRetroSpanPtr()->Var = var;
            if (parameters & MANUAL_END) {
                span.GetRetroSpanPtr()->End();
            }
        } else {
            TUniversalSpan<TTestSpan1> span(Verbosity, NWilson::TTraceId(parentId), "Test");
            span.GetRetroSpanPtr()->Var = var;
            span.GetRetroSpanPtr()->End();
        }

        for (ui32 i = 0; i < 10; ++i) {
            std::vector<std::unique_ptr<TRetroSpan>> spans = GetSpansOfTrace(parentId);
            UNIT_ASSERT_VALUES_EQUAL(spans.size(), 1);
            const TTestSpan1* ptr = spans[0]->Cast<TTestSpan1>();
            UNIT_ASSERT(ptr);
            UNIT_ASSERT_VALUES_EQUAL(ptr->Var, var);
            UNIT_ASSERT_VALUES_UNEQUAL(ptr->GetStartTs(), TInstant::Zero());
            UNIT_ASSERT_VALUES_UNEQUAL(ptr->GetEndTs(), TInstant::Zero());

        }
        DropThreadLocalBuffer();
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

    constexpr static ui32 BufferSize = 1_KB;
    char Buffer[BufferSize];
    NWilson::TTraceId TraceId;

    ui64 GetValue(ui32 pos) {
        return *reinterpret_cast<ui64*>(Buffer + pos);
    }

    void SpanWriter(TInstant end) {
        while (TInstant::Now() < end) {
            ui32 pos = RandomNumber<ui32>(BufferSize - sizeof(ui64));
            ui64 val = GetValue(pos);
            TUniversalSpan<TTestSpan2> span(Verbosity, NWilson::TTraceId(TraceId), "Test",
                    NWilson::EFlags::AUTO_END);
            span.GetRetroSpanPtr()->Var1 = pos;
            span.GetRetroSpanPtr()->Var2 = val;
        }
    }

    void SpanReader(TInstant end) {
        while (TInstant::Now() < end) {
            auto spans = GetSpansOfTrace(TraceId);
            for (const std::unique_ptr<TRetroSpan>& span : spans) {
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

Y_UNIT_TEST_SUITE(RetroCollector) {
    const ui8 Verbosity = 1;

    class TTestActor : public NActors::TActorBootstrapped<TTestActor> {
    public:
        TTestActor(const NWilson::TTraceId& traceId, const NActors::TActorId& edgeActorId)
            : TraceId(traceId)
            , EdgeActorId(edgeActorId)
        {}

        void Bootstrap() {
            Foo(NWilson::TTraceId(TraceId));
            DemandTrace(TraceId);
            Sleep(TDuration::Seconds(1));
            Send(EdgeActorId, new NActors::TEvents::TEvGone);
            PassAway();
        }

        void Foo(NWilson::TTraceId&& traceId) {
            Ctest << "Foo()" << Endl;
            TUniversalSpan<TTestSpan1> span(Verbosity, std::move(traceId), "Name", NWilson::EFlags::AUTO_END);
            span.GetRetroSpanPtr()->Var = 111;
            Sleep(TDuration::MilliSeconds(100));

            for (ui32 i = 0; i < ChildSpans; ++i) {
                Bar(span.GetTraceId(), i);
            }
        }

        void Bar(NWilson::TTraceId&& traceId, ui32 idx) {
            Ctest << "Bar()" << Endl;
            TUniversalSpan<TTestSpan2> span(Verbosity, std::move(traceId), "Name", NWilson::EFlags::AUTO_END);
            span.GetRetroSpanPtr()->Var1 = idx;
            span.GetRetroSpanPtr()->Var2 = 999;
            Sleep(TDuration::MilliSeconds(20));
        }

    public:
        NWilson::TTraceId TraceId;
        NActors::TActorId EdgeActorId;

        constexpr static ui32 ChildSpans = 5;
    };

    Y_UNIT_TEST(Simple) {
        NActors::TTestActorRuntimeBase actorSystem(1);
        actorSystem.Initialize();

        { // register retro collector
            NActors::TActorId actorId = actorSystem.Register(CreateRetroCollector());
            actorSystem.RegisterService(MakeRetroCollectorId(), actorId);
        }

        NWilson::TFakeWilsonUploader* wilson = new NWilson::TFakeWilsonUploader;
        { // register fake wilson collector
            NActors::TActorId actorId = actorSystem.Register(wilson);
            actorSystem.RegisterService(NWilson::MakeWilsonUploaderId(), actorId);
        }

        const ui32 ttl = Max<ui32>();
        NWilson::TTraceId traceId = NWilson::TTraceId::NewTraceId(Verbosity, ttl, true);

        NActors::TActorId edge = actorSystem.AllocateEdgeActor();
        {
            actorSystem.Register(new TTestActor(traceId, edge));
        }

        actorSystem.GrabEdgeEvent<NActors::TEvents::TEvGone>(edge);

        ui32 numSpan1 = 0;
        ui32 numSpan2 = 0;
        ui64 rootSpanId = 0;
        std::unordered_set<ui64> childSpans = {};
        std::unordered_set<ui64> childSpanParents = {};
        for (const auto& span : wilson->Spans) {
            UNIT_ASSERT_VALUES_EQUAL(span.span_id().size(), sizeof(ui64));
            UNIT_ASSERT_VALUES_EQUAL(span.parent_span_id().size(), sizeof(ui64));
            ui64 spanId = *reinterpret_cast<const ui64*>(span.span_id().data());
            ui64 parentSpanId = *reinterpret_cast<const ui64*>(span.parent_span_id().data());
            UNIT_ASSERT_VALUES_UNEQUAL(spanId, 0);
            if (span.name().find("TTestSpan1") != std::string::npos) {
                ++numSpan1;
                rootSpanId = spanId;
            } else if (span.name().find("TTestSpan2") != std::string::npos) {
                ++numSpan2;
                childSpans.insert(spanId);
                childSpanParents.insert(parentSpanId);
            } else {
                UNIT_FAIL(span.name());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(numSpan1, 1);
        UNIT_ASSERT_VALUES_EQUAL(numSpan2, TTestActor::ChildSpans);
        UNIT_ASSERT_VALUES_UNEQUAL(rootSpanId, 0);
        UNIT_ASSERT_VALUES_EQUAL(childSpans.size(), numSpan2);
        UNIT_ASSERT_VALUES_EQUAL(childSpanParents.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(*childSpanParents.begin(), rootSpanId);
    }
}

};
