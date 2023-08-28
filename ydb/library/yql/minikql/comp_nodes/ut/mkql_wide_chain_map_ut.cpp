#include "mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 23u
Y_UNIT_TEST_SUITE(TMiniKQLWideChain1MapTest) {
    Y_UNIT_TEST_LLVM(TestThinLambda) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType)});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
            [&](TRuntimeNode::TList inputs) { return inputs; },
            [&](TRuntimeNode::TList, TRuntimeNode::TList outputs) { return outputs; }),
            [&](TRuntimeNode::TList) -> TRuntimeNode { return pb.NewTuple({}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSimpleSwap) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList inputs) { return inputs; },
            [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList { return {inputs.back(), outputs[1U], inputs.front()}; }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -2);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -3);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 3);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 4);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestSimpleChain) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewOptional(pb.NewDataLiteral<i32>(-5)), pb.NewOptional(pb.NewDataLiteral<i32>(-6))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-4)), pb.NewOptional(pb.NewDataLiteral<i32>(-7))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewOptional(pb.NewDataLiteral<i32>(-7)), pb.NewOptional(pb.NewDataLiteral<i32>(-8))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-9))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {pb.Add(inputs.front(), inputs[1U]), pb.NewEmptyOptional(dataType), pb.Sub(inputs.back(), inputs[1U])}; },
            [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList {
                return {pb.AggrAdd(outputs.back(), inputs[1U]), outputs.front(), pb.Decrement(outputs[1])};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(NUdf::EFetchStatus::Ok == iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -4);
        UNIT_ASSERT(!item.GetElement(1));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -1);
        UNIT_ASSERT(NUdf::EFetchStatus::Ok == iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -5);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -4);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(NUdf::EFetchStatus::Ok == iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -7);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -5);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -5);
        UNIT_ASSERT(NUdf::EFetchStatus::Ok == iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -5);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -7);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -6);
        UNIT_ASSERT(NUdf::EFetchStatus::Finish == iterator.Fetch(item));
        UNIT_ASSERT(NUdf::EFetchStatus::Finish == iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestAgrregateWithPrevious) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewOptional(pb.NewDataLiteral<i32>(5)), pb.NewEmptyOptional(dataType)});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {inputs[1U], inputs[2U], inputs[0U]}; },
            [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList {
                return {pb.AggrMin(inputs[0U], outputs[1U]), pb.AggrMax(inputs[1U], outputs[2U]), pb.AggrAdd(outputs[0U], inputs[2U])};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 1);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -2);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -2);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 5);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 2);
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestPasstroughtFieldSplitAsIs) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewOptional(pb.NewDataLiteral<i32>(-6)), pb.NewOptional(pb.NewDataLiteral<i32>(-5))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-4)), pb.NewOptional(pb.NewDataLiteral<i32>(-4))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewOptional(pb.NewDataLiteral<i32>(-7)), pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(0))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {inputs[1U], pb.Mul(inputs.front(), inputs.back()), inputs[1U]}; },
            [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList {
                return {inputs[1U], pb.Mul(outputs[1U], pb.Add(inputs.back(), inputs.front())), inputs[1U]};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -6);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -5);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -6);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -7);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 0);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), -7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 0);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestFieldBothWayPasstroughtAndArg) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), pb.NewOptional(pb.NewDataLiteral<i32>(-5)), pb.NewOptional(pb.NewDataLiteral<i32>(-6))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), pb.NewOptional(pb.NewDataLiteral<i32>(-4)), pb.NewOptional(pb.NewDataLiteral<i32>(-7))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), pb.NewOptional(pb.NewDataLiteral<i32>(-7)), pb.NewOptional(pb.NewDataLiteral<i32>(-8))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), pb.NewEmptyOptional(dataType), pb.NewOptional(pb.NewDataLiteral<i32>(-9))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {inputs[1U], pb.Sub(inputs.front(), inputs.back()), pb.Minus(inputs[1U])}; },
            [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList {
                return {inputs[1U], pb.Sub(outputs[1U], pb.Add(inputs.back(), inputs.front())), pb.Minus(inputs[1U])};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue().GetListIterator();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -5);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 7);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 5);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -4);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 12);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 4);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), -7);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 17);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 7);
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item.GetElement(0));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 22);
        UNIT_ASSERT(!item.GetElement(2));
        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    Y_UNIT_TEST_LLVM(TestDotCalculateUnusedField) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewOptionalType(pb.NewDataType(NUdf::TDataType<i32>::Id));
        const auto tupleType = pb.NewTupleType({dataType, dataType, dataType});
        const auto null = pb.NewEmptyOptional(dataType);

        const auto data1 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(1)), null, pb.NewOptional(pb.NewDataLiteral<i32>(-1))});
        const auto data2 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(2)), null, pb.NewOptional(pb.NewDataLiteral<i32>(-2))});
        const auto data3 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(3)), null, pb.NewOptional(pb.NewDataLiteral<i32>(-3))});
        const auto data4 = pb.NewTuple(tupleType, {pb.NewOptional(pb.NewDataLiteral<i32>(4)), null, pb.NewOptional(pb.NewDataLiteral<i32>(-4))});

        const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});

        const auto landmine = pb.NewDataLiteral<NUdf::EDataSlot::Utf8>("Veszély! Aknák!");

        const auto pgmReturn = pb.FromFlow(pb.NarrowMap(pb.WideChain1Map(pb.ExpandMap(pb.ToFlow(list),
            [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
            [&](TRuntimeNode::TList inputs) -> TRuntimeNode::TList { return {inputs.back(), pb.Unwrap(inputs[1U], landmine, __FILE__, __LINE__, 0), inputs.front()}; },
            [&](TRuntimeNode::TList inputs, TRuntimeNode::TList outputs) -> TRuntimeNode::TList {
                return {pb.Mul(outputs.front(), inputs.back()), pb.Unwrap(inputs[1U], landmine, __FILE__, __LINE__, 0), pb.Add(inputs.front(), outputs.back())};
            }),
            [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple({items.back(), items.front()}); }
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(NUdf::EFetchStatus::Ok == iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -1);
        UNIT_ASSERT(NUdf::EFetchStatus::Ok == iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 3);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 2);
        UNIT_ASSERT(NUdf::EFetchStatus::Ok == iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 6);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), -6);
        UNIT_ASSERT(NUdf::EFetchStatus::Ok == iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 10);
        UNIT_ASSERT_VALUES_EQUAL(item.GetElement(1).template Get<i32>(), 24);
        UNIT_ASSERT(NUdf::EFetchStatus::Finish == iterator.Fetch(item));
        UNIT_ASSERT(NUdf::EFetchStatus::Finish == iterator.Fetch(item));
    }
}
#endif
}
}

