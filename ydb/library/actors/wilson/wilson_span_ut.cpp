#include "wilson_span.h"
#include "wilson_event_budget.h"
#include "wilson_uploader.h"

#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NWilson;

Y_UNIT_TEST_SUITE(TSpan) {
    Y_UNIT_TEST(ConstructTerminatedTimestamps) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        const auto edge = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeWilsonUploaderId(), edge);

        const auto parent = TTraceId::NewTraceId(15, 4095);
        constexpr ui64 start = 1'700'000'000'000'000'123;
        const struct {
            ui64 End;
            ui64 ExpectedEnd;
        } cases[] = {
            {start + 1'234, start + 1'234},
            {start - 1'234, start},
        };
        for (const auto& test : cases) {
            auto span = TSpan::ConstructTerminated(parent, parent.Span(15), start, test.End,
                                                   NTraceProto::Status::STATUS_CODE_OK, "Terminated", runtime.GetActorSystem(0));
            span.End();
            const auto event = runtime.GrabEdgeEvent<TEvWilson>(edge);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Span.start_time_unix_nano(), start);
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Span.end_time_unix_nano(), test.ExpectedEnd);
        }
    }

    Y_UNIT_TEST(EventBudgetGuardsBeforeBuildingAttributes) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        const auto edge = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeWilsonUploaderId(), edge);
        TSpanEventBudget budget;
        size_t builds = 0;
        auto attributes = [&] {
            ++builds;
            TKeyValueList result;
            result["state"] = TString("ready");
            return result;
        };
        TSpan disabled;
        UNIT_ASSERT(!budget.Event(disabled, 10, "state", attributes));
        TSpan span(6, TTraceId::NewTraceId(9, 4095), "Owner", EFlags::AUTO_END,
                   runtime.GetActorSystem(0));
        UNIT_ASSERT(!budget.Event(span, 10, "state", attributes));
        UNIT_ASSERT_VALUES_EQUAL(builds, 0u);
        UNIT_ASSERT(budget.Event(span, 9, "state", attributes));
        budget.Annotate(span);
        span.EndOk();
        UNIT_ASSERT(!budget.Event(span, 9, "late callback", attributes));
        UNIT_ASSERT_VALUES_EQUAL(builds, 1u);
        const auto event = runtime.GrabEdgeEvent<TEvWilson>(edge);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->Span.events_size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(event->Get()->Span.events(0).name(), "state");
    }

    Y_UNIT_TEST(EventBudgetPreservesTerminalReserveAndTruncation) {
        NActors::TTestActorRuntimeBase runtime;
        runtime.Initialize();
        const auto edge = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeWilsonUploaderId(), edge);
        TSpan span(6, TTraceId::NewTraceId(15, 4095), "Owner", EFlags::AUTO_END,
                   runtime.GetActorSystem(0));
        TSpanEventBudget budget(5, 2);
        size_t builds = 0;
        auto attributes = [&] {
            ++builds;
            return TKeyValueList{};
        };
        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT(budget.Event(span, 10, "progress", attributes));
        }
        UNIT_ASSERT(!budget.Event(span, 10, "omitted progress", attributes));
        UNIT_ASSERT(budget.Event(span, 10, "cancel", attributes, TSpanEventBudget::EKind::Terminal));
        UNIT_ASSERT(budget.Event(span, 10, "finish", attributes, TSpanEventBudget::EKind::Terminal));
        UNIT_ASSERT(!budget.Event(span, 10, "omitted terminal", attributes, TSpanEventBudget::EKind::Terminal));
        UNIT_ASSERT_VALUES_EQUAL(builds, 5u);
        budget.Annotate(span);
        span.EndOk();
        const auto event = runtime.GrabEdgeEvent<TEvWilson>(edge);
        const auto& result = event->Get()->Span;
        UNIT_ASSERT_VALUES_EQUAL(result.events_size(), 5);
        UNIT_ASSERT_VALUES_EQUAL(result.events(3).name(), "cancel");
        UNIT_ASSERT_VALUES_EQUAL(result.events(4).name(), "finish");
        ui64 previousMonotonic = 0;
        const i64 sequences[] = {1, 2, 3, 5, 6};
        for (int i = 0; i < result.events_size(); ++i) {
            bool sawSequence = false;
            bool sawMonotonic = false;
            for (const auto& attribute : result.events(i).attributes()) {
                if (attribute.key() == "ydb.event.sequence") {
                    UNIT_ASSERT_VALUES_EQUAL(attribute.value().int_value(), sequences[i]);
                    sawSequence = true;
                } else if (attribute.key() == "ydb.event.monotonic_us") {
                    const ui64 monotonic = attribute.value().int_value();
                    UNIT_ASSERT(monotonic >= previousMonotonic);
                    previousMonotonic = monotonic;
                    sawMonotonic = true;
                }
            }
            UNIT_ASSERT(sawSequence && sawMonotonic);
        }
        bool sawDropped = false;
        bool sawExhausted = false;
        for (const auto& attribute : result.attributes()) {
            if (attribute.key() == "ydb.trace.events_dropped") {
                UNIT_ASSERT_VALUES_EQUAL(attribute.value().int_value(), 2);
                sawDropped = true;
            } else if (attribute.key() == "ydb.trace.event_budget.exhausted") {
                UNIT_ASSERT(attribute.value().bool_value());
                sawExhausted = true;
            }
        }
        UNIT_ASSERT(sawDropped && sawExhausted);
    }

} // Y_UNIT_TEST_SUITE(TSpan)
