#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_program_builder.h>

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>

#include <library/cpp/threading/future/future.h>

namespace NKikimr::NMiniKQL {

Y_UNIT_TEST_SUITE(KqpStreamingAggregationRuntime) {
    Y_UNIT_TEST(InMemoryStateUsesMiniKqlAllocator) {
        constexpr ui64 keyCount = 65536;
        TKqpComputeContextBase computeCtx;
        TSetup<false> setup(GetKqpBaseComputeFactory(&computeCtx));
        TKqpProgramBuilder pb(*setup.Env, *setup.FunctionRegistry);
        const auto one = pb.NewDataLiteral<ui64>(1);
        const auto flow = pb.ToFlow(pb.ListFromRange(
            pb.NewDataLiteral<ui64>(0), pb.NewDataLiteral<ui64>(keyCount), one), {});
        const auto aggregation = pb.StreamingAggregation(flow,
            [&](TRuntimeNode item) { return pb.NewTuple({item}); },
            [&](TRuntimeNode) { return one; },
            [&](TRuntimeNode state, TRuntimeNode) { return pb.Add(state, one); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.Add(pb.Nth(key, 0), state); },
            pb.NewDataLiteral<NUdf::EDataSlot::String>(""));
        auto graph = setup.BuildGraph(pb.FromFlow(aggregation));
        const auto stream = graph->GetValue();
        const auto usedBefore = setup.Alloc.GetUsed();
        NUdf::TUnboxedValue item;
        for (ui64 i = 0; i < keyCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(stream.Fetch(item), NUdf::EFetchStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), i + 1);
        }

        // The input is lazy and outputs/states are embedded integers. Retained
        // map entries, rather than collected results, must account for this growth.
        const auto usedAfter = setup.Alloc.GetUsed();
        UNIT_ASSERT_C(usedAfter >= usedBefore + keyCount * sizeof(NUdf::TUnboxedValue),
            "State map storage is not charged to MiniKQL: before=" << usedBefore << ", after=" << usedAfter);
        UNIT_ASSERT_VALUES_EQUAL(stream.Fetch(item), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST(PatternIsNotCacheable) {
        for (const TStringBuf stateTablePath : {"", "/Root/state"}) {
            TKqpComputeContextBase computeCtx;
            TSetup<false> setup(GetKqpBaseComputeFactory(&computeCtx));
            TKqpProgramBuilder pb(*setup.Env, *setup.FunctionRegistry);
            const auto one = pb.NewDataLiteral<ui64>(1);
            const auto two = pb.NewDataLiteral<ui64>(2);
            const auto flow = pb.ToFlow(pb.AsList({one, one, two}), {});
            const auto aggregation = pb.StreamingAggregation(flow,
                [&](TRuntimeNode item) { return pb.NewTuple({item}); },
                [&](TRuntimeNode) { return one; },
                [&](TRuntimeNode state, TRuntimeNode) { return pb.Add(state, one); },
                [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewTuple({key, state}); },
                pb.NewDataLiteral<NUdf::EDataSlot::String>(stateTablePath));
            auto graph = setup.BuildGraph(pb.Collect(aggregation));
            UNIT_ASSERT(!setup.Pattern->GetSuitableForCache());

            // The in-memory backend still works without an actor or wakeup callback.
            if (stateTablePath.empty()) {
                const auto result = graph->GetValue();
                const auto iterator = result.GetListIterator();
                NUdf::TUnboxedValue item;
                for (const ui64 expected : {1, 2, 1}) {
                    UNIT_ASSERT(iterator.Next(item));
                    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).Get<ui64>(), expected);
                }
                UNIT_ASSERT(!iterator.Next(item));
            }
        }
    }

    Y_UNIT_TEST(WakeupCallbackOutlivesContext) {
        for (const bool readyBeforeSubscribe : {false, true}) {
            for (const bool failed : {false, true}) {
                auto promise = NThreading::NewPromise<void>();
                const auto complete = [&]() {
                    if (failed) {
                        promise.SetException("test failure");
                    } else {
                        promise.SetValue();
                    }
                };
                if (readyBeforeSubscribe) {
                    complete();
                }

                ui32 wakeups = 0;
                {
                    TKqpComputeContextBase computeCtx;
                    UNIT_ASSERT(!computeCtx.GetWakeupCallback());
                    computeCtx.SetWakeupCallback([&]() { ++wakeups; });
                    const auto wakeupCallback = computeCtx.GetWakeupCallback();
                    promise.GetFuture().Subscribe([wakeupCallback](const auto&) { wakeupCallback(); });
                    computeCtx.SetWakeupCallback({});
                    UNIT_ASSERT_VALUES_EQUAL(wakeups, readyBeforeSubscribe ? 1 : 0);
                }

                // The subscription owns the callback independently of the KQP context.
                if (!readyBeforeSubscribe) {
                    complete();
                }
                UNIT_ASSERT_VALUES_EQUAL(wakeups, 1);
            }
        }
    }
}

} // namespace NKikimr::NMiniKQL
