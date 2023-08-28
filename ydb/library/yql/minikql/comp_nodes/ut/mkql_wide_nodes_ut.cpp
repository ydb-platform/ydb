#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 18u
Y_UNIT_TEST_SUITE(TMiniKQLWideNodesTest) {
    // TDOD: fixme
#if 0
    Y_UNIT_TEST_LLVM(TestWideDiscard) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.Discard(pb.ExpandMap(pb.ToFlow(list), [](TRuntimeNode) { return TRuntimeNode::TList(); })));
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }
#endif

    Y_UNIT_TEST_LLVM(TestDiscard) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data0 = pb.NewDataLiteral<NUdf::EDataSlot::String>("000");
        const auto data1 = pb.NewDataLiteral<NUdf::EDataSlot::String>("100");
        const auto data2 = pb.NewDataLiteral<NUdf::EDataSlot::String>("200");
        const auto data3 = pb.NewDataLiteral<NUdf::EDataSlot::String>("300");
        const auto dataType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto list = pb.NewList(dataType, {data0, data1, data2, data3});

        const auto pgmReturn = pb.FromFlow(pb.Discard(pb.ToFlow(list)));
        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestTakeOverSource) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;


        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.Take(pb.Source(), pb.NewDataLiteral<ui64>(666ULL)), [&](TRuntimeNode::TList) { return pb.NewTuple({}); } ));

        const auto graph = setup.BuildGraph(pgmReturn);
        UNIT_ASSERT_VALUES_EQUAL(graph->GetValue().GetListLength(), 666ULL);
    }

    Y_UNIT_TEST_LLVM(TestSkipAndTake) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.ListFromRange(pb.NewDataLiteral<ui32>(100U), pb.NewDataLiteral<ui32>(666U), pb.NewDataLiteral<ui32>(3U));

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.Take(pb.Skip(pb.ExpandMap(pb.ToFlow(pb.Enumerate(list)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 1U), pb.Nth(item, 0U)}; }),
            pb.NewDataLiteral<ui64>(42ULL)), pb.NewDataLiteral<ui64>(4ULL)),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 42);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 226);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 43);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 229);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 44);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 232);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<ui64>(), 45);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<ui32>(), 235);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDoNotCalculateSkipped) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list = pb.ListFromRange(pb.NewDataLiteral<ui64>(100ULL), pb.NewDataLiteral<ui64>(135ULL), pb.NewDataLiteral<ui64>(5ULL));

        const auto trap = pb.NewDataLiteral<NUdf::EDataSlot::String>("IT'S A TRAP!");

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.Skip(pb.WideMap(pb.ExpandMap(pb.ToFlow(pb.Enumerate(list)),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 1U), pb.Nth(item, 0U)}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.Unwrap(pb.Div(items.front(), items.back()), trap, __FILE__, __LINE__, 0)}; }),
            pb.NewDataLiteral<ui64>(3ULL)),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 38ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 30ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 25ULL);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.template Get<ui64>(), 21ULL);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

}
#endif
}
}
