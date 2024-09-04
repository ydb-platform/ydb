#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <arrow/compute/exec_internal.h>
#include <arrow/array/builder_primitive.h>

#include <ydb/library/yql/public/udf/udf_helpers.h>
#include <ydb/library/yql/public/udf/arrow/udf_arrow_helpers.h>

BEGIN_SIMPLE_ARROW_UDF(TInc, i32(i32)) {
    Y_UNUSED(valueBuilder);
    const i32 arg = args[0].Get<i32>();
    return NYql::NUdf::TUnboxedValuePod(arg + 1);
}

struct TIncKernelExec : public NYql::NUdf::TUnaryKernelExec<TIncKernelExec> {
    template <typename TSink>
    static void Process(const NYql::NUdf::IValueBuilder* valueBuilder, NYql::NUdf::TBlockItem arg, const TSink& sink) {
        Y_UNUSED(valueBuilder);
        sink(NYql::NUdf::TBlockItem(arg.As<i32>() + 1));
    }
};

END_SIMPLE_ARROW_UDF(TInc, TIncKernelExec::Do);

SIMPLE_MODULE(TBlockUTModule,
    TInc
)

namespace NKikimr {
namespace NMiniKQL {

namespace {
    arrow::Datum ExecuteOneKernel(const IArrowKernelComputationNode* kernelNode,
        const std::vector<arrow::Datum>& argDatums, arrow::compute::ExecContext& execContext) {
        const auto& kernel = kernelNode->GetArrowKernel();
        arrow::compute::KernelContext kernelContext(&execContext);
        std::unique_ptr<arrow::compute::KernelState> state;
        if (kernel.init) {
            state = ARROW_RESULT(kernel.init(&kernelContext, { &kernel, kernelNode->GetArgsDesc(), nullptr }));
            kernelContext.SetState(state.get());
        }

        auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
        ARROW_OK(executor->Init(&kernelContext, { &kernel, kernelNode->GetArgsDesc(), nullptr }));
        auto listener = std::make_shared<arrow::compute::detail::DatumAccumulator>();
        ARROW_OK(executor->Execute(argDatums, listener.get()));
        return executor->WrapResults(argDatums, listener->values());
    }

    void ExecuteAllKernels(std::vector<arrow::Datum>& datums, const TArrowKernelsTopology* topology, arrow::compute::ExecContext& execContext) {
        for (ui32 i = 0; i < topology->Items.size(); ++i) {
            std::vector<arrow::Datum> argDatums;
            argDatums.reserve(topology->Items[i].Inputs.size());
            for (auto j : topology->Items[i].Inputs) {
                argDatums.emplace_back(datums[j]);
            }

            arrow::Datum output = ExecuteOneKernel(topology->Items[i].Node.get(), argDatums, execContext);
            datums[i + topology->InputArgsCount] = output;
        }
    }
}

Y_UNIT_TEST_SUITE(TMiniKQLBlocksTest) {
Y_UNIT_TEST_LLVM(TestEmpty) {
    TSetup<LLVM> setup;
    auto& pb = *setup.PgmBuilder;

    const auto type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto list = pb.NewEmptyList(type);
    const auto sourceFlow = pb.ToFlow(list);
    const auto flowAfterBlocks = pb.FromBlocks(pb.ToBlocks(sourceFlow));
    const auto pgmReturn = pb.ForwardList(flowAfterBlocks);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestSimple) {
    static const size_t dataCount = 1000;
    TSetup<LLVM> setup;
    auto& pb = *setup.PgmBuilder;

    TRuntimeNode::TList data;
    data.reserve(dataCount);
    for (ui64 i = 0ULL; i < dataCount; ++i) {
        data.push_back(pb.NewDataLiteral(i));
    }
    const auto type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto list = pb.NewList(type, data);
    const auto sourceFlow = pb.ToFlow(list);
    const auto flowAfterBlocks = pb.FromBlocks(pb.ToBlocks(sourceFlow));
    const auto pgmReturn = pb.ForwardList(flowAfterBlocks);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    for (size_t i = 0; i < dataCount; ++i) {
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), i);
    }
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestWideToBlocks) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type, ui64Type});

    const auto data1 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(1), pb.NewDataLiteral<ui64>(10)});
    const auto data2 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(2), pb.NewDataLiteral<ui64>(20)});
    const auto data3 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(3), pb.NewDataLiteral<ui64>(30)});

    const auto list = pb.NewList(tupleType, {data1, data2, data3});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });
    const auto wideBlocksFlow = pb.WideToBlocks(wideFlow);
    const auto narrowBlocksFlow = pb.NarrowMap(wideBlocksFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return items[1];
    });
    const auto narrowFlow = pb.FromBlocks(narrowBlocksFlow);
    const auto pgmReturn = pb.ForwardList(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 10);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 20);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 30);

    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

namespace {
template<bool LLVM>
void TestChunked(bool withBlockExpand) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type   = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto boolType   = pb.NewDataType(NUdf::TDataType<bool>::Id);
    const auto stringType = pb.NewDataType(NUdf::EDataSlot::String);
    const auto utf8Type   = pb.NewDataType(NUdf::EDataSlot::Utf8);

    const auto tupleType = pb.NewTupleType({ui64Type, boolType, stringType, utf8Type});

    TRuntimeNode::TList items;
    const size_t bigStrSize = 1024 * 1024 + 100;
    const size_t smallStrSize = 256 * 1024;
    for (size_t i = 0; i < 20; ++i) {

        if (i % 2 == 0) {
            std::string big(bigStrSize, '0' + i);
            std::string small(smallStrSize, 'A' + i);

            items.push_back(pb.NewTuple(tupleType, { pb.NewDataLiteral<ui64>(i), pb.NewDataLiteral<bool>(true),
                                                     pb.NewDataLiteral<NUdf::EDataSlot::String>(big),
                                                     pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(small),
                                                     }));
        } else {
            items.push_back(pb.NewTuple(tupleType, { pb.NewDataLiteral<ui64>(i), pb.NewDataLiteral<bool>(false),
                                                     pb.NewDataLiteral<NUdf::EDataSlot::String>(""),
                                                     pb.NewDataLiteral<NUdf::EDataSlot::Utf8>(""),
                                                     }));

        }
    }

    const auto list = pb.NewList(tupleType, std::move(items));

    auto node = pb.ToFlow(list);
    node = pb.ExpandMap(node, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U)};
    });
    node = pb.WideToBlocks(node);
    if (withBlockExpand) {
        node = pb.BlockExpandChunked(node);
        // WideTakeBlocks won't work on chunked blocks
        node = pb.WideTakeBlocks(node, pb.NewDataLiteral<ui64>(19));
        node = pb.WideFromBlocks(node);
    } else {
        // WideFromBlocks should support chunked blocks
        node = pb.WideFromBlocks(node);
        node = pb.Take(node, pb.NewDataLiteral<ui64>(19));
    }
    node = pb.NarrowMap(node, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewTuple(tupleType, {items[0], items[1], items[2], items[3]});
    });

    const auto pgmReturn = pb.ForwardList(node);
    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    for (size_t i = 0; i < 19; ++i) {
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        ui64 num = item.GetElement(0).Get<ui64>();
        bool bl = item.GetElement(1).Get<bool>();
        auto strVal = item.GetElement(2);
        auto utf8Val = item.GetElement(3);
        std::string_view str = strVal.AsStringRef();
        std::string_view utf8 = utf8Val.AsStringRef();

        UNIT_ASSERT_VALUES_EQUAL(num, i);
        UNIT_ASSERT_VALUES_EQUAL(bl, i % 2 == 0);
        if (i % 2 == 0) {
            std::string big(bigStrSize, '0' + i);
            std::string small(smallStrSize, 'A' + i);
            UNIT_ASSERT_VALUES_EQUAL(str, big);
            UNIT_ASSERT_VALUES_EQUAL(utf8, small);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(str.size(), 0);
            UNIT_ASSERT_VALUES_EQUAL(utf8.size(), 0);
        }
    }

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));

}

} // namespace

Y_UNIT_TEST_LLVM(TestBlockExpandChunked) {
    TestChunked<LLVM>(true);
}

Y_UNIT_TEST_LLVM(TestWideFromBlocksForChunked) {
    TestChunked<LLVM>(false);
}

Y_UNIT_TEST(TestScalar) {
    const ui64 testValue = 42;

    TSetup<false> setup;
    auto& pb = *setup.PgmBuilder;

    auto dataLiteral = pb.NewDataLiteral<ui64>(testValue);
    const auto dataAfterBlocks = pb.AsScalar(dataLiteral);

    const auto graph = setup.BuildGraph(dataAfterBlocks);
    const auto value = graph->GetValue();
    UNIT_ASSERT(value.HasValue() && value.IsBoxed());
    UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(value).GetDatum().scalar_as<arrow::UInt64Scalar>().value, testValue);
}

Y_UNIT_TEST_LLVM(TestReplicateScalar) {
    const ui64 count = 1000;
    const ui32 value = 42;

    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto valueType = pb.NewDataType(NUdf::TDataType<ui32>::Id);

    const auto scalarValue = pb.AsScalar(pb.NewDataLiteral<ui32>(value));
    const auto scalarCount = pb.AsScalar(pb.NewDataLiteral<ui64>(count));

    const auto replicated = pb.ReplicateScalar(scalarValue, scalarCount);

    const auto replicatedType = pb.NewBlockType(valueType, TBlockType::EShape::Many);

    const auto listOfReplicated = pb.NewList(replicatedType, { replicated });

    const auto flowOfReplicated = pb.ToFlow(listOfReplicated);

    const auto flowAfterBlocks = pb.FromBlocks(flowOfReplicated);
    const auto pgmReturn = pb.ForwardList(flowAfterBlocks);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    for (size_t i = 0; i < count; ++i) {
        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), value);
    }

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestBlockFunc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type, ui64Type});
    const auto ui64BlockType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);

    const auto data1 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(1), pb.NewDataLiteral<ui64>(10)});
    const auto data2 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(2), pb.NewDataLiteral<ui64>(20)});
    const auto data3 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(3), pb.NewDataLiteral<ui64>(30)});

    const auto list = pb.NewList(tupleType, {data1, data2, data3});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });
    const auto wideBlocksFlow = pb.WideToBlocks(wideFlow);
    const auto sumWideFlow = pb.WideMap(wideBlocksFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
        return {pb.BlockFunc("Add", ui64BlockType, {items[0], items[1]})};
    });
    const auto sumNarrowFlow = pb.NarrowMap(sumWideFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return items[0];
    });
    const auto pgmReturn = pb.Collect(pb.FromBlocks(sumNarrowFlow));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 11);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 22);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 33);
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestBlockFuncWithNullables) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalUi64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id, true);
    const auto tupleType = pb.NewTupleType({optionalUi64Type, optionalUi64Type});
    const auto emptyOptionalUi64 = pb.NewEmptyOptional(optionalUi64Type);
    const auto ui64OptBlockType = pb.NewBlockType(optionalUi64Type, TBlockType::EShape::Many);

    const auto data1 = pb.NewTuple(tupleType, {
        pb.NewOptional(pb.NewDataLiteral<ui64>(1)),
        emptyOptionalUi64
    });
    const auto data2 = pb.NewTuple(tupleType, {
        emptyOptionalUi64,
        pb.NewOptional(pb.NewDataLiteral<ui64>(20))
    });
    const auto data3 = pb.NewTuple(tupleType, {
        emptyOptionalUi64,
        emptyOptionalUi64
    });
    const auto data4 = pb.NewTuple(tupleType, {
        pb.NewOptional(pb.NewDataLiteral<ui64>(10)),
        pb.NewOptional(pb.NewDataLiteral<ui64>(20))
    });

    const auto list = pb.NewList(tupleType, {data1, data2, data3, data4});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });
    const auto wideBlocksFlow = pb.WideToBlocks(wideFlow);
    const auto sumWideFlow = pb.WideMap(wideBlocksFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
        return {pb.BlockFunc("Add", ui64OptBlockType, {items[0], items[1]})};
    });
    const auto sumNarrowFlow = pb.NarrowMap(sumWideFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return items[0];
    });
    const auto pgmReturn = pb.Collect(pb.FromBlocks(sumNarrowFlow));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 30);

    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestBlockFuncWithNullableScalar) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalUi64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id, true);
    const auto ui64OptBlockType = pb.NewBlockType(optionalUi64Type, TBlockType::EShape::Many);
    const auto emptyOptionalUi64 = pb.NewEmptyOptional(optionalUi64Type);

    const auto list = pb.NewList(optionalUi64Type, {
        pb.NewOptional(pb.NewDataLiteral<ui64>(10)),
        pb.NewOptional(pb.NewDataLiteral<ui64>(20)),
        pb.NewOptional(pb.NewDataLiteral<ui64>(30))
    });
    const auto flow = pb.ToFlow(list);
    const auto blocksFlow = pb.ToBlocks(flow);

    THolder<IComputationGraph> graph;
    auto map = [&](const TProgramBuilder::TUnaryLambda& func) {
        const auto pgmReturn = pb.Collect(pb.FromBlocks(pb.Map(blocksFlow, func)));
        graph = setup.BuildGraph(pgmReturn);
        return graph->GetValue().GetListIterator();
    };

    {
        const auto scalar = pb.AsScalar(emptyOptionalUi64);
        auto iterator = map([&](TRuntimeNode item) -> TRuntimeNode {
            return {pb.BlockFunc("Add", ui64OptBlockType, {scalar, item})};
        });

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    {
        const auto scalar = pb.AsScalar(emptyOptionalUi64);
        auto iterator = map([&](TRuntimeNode item) -> TRuntimeNode {
            return {pb.BlockFunc("Add", ui64OptBlockType, {item, scalar})};
        });

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT(!item);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }

    {
        const auto scalar = pb.AsScalar(pb.NewDataLiteral<ui64>(100));
        auto iterator = map([&](TRuntimeNode item) -> TRuntimeNode {
            return {pb.BlockFunc("Add", ui64OptBlockType, {item, scalar})};
        });

        NUdf::TUnboxedValue item;
        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 110);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 120);

        UNIT_ASSERT(iterator.Next(item));
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 130);

        UNIT_ASSERT(!iterator.Next(item));
        UNIT_ASSERT(!iterator.Next(item));
    }
}

Y_UNIT_TEST_LLVM(TestBlockFuncWithScalar) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto ui64BlockType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);

    const auto data1 = pb.NewDataLiteral<ui64>(10);
    const auto data2 = pb.NewDataLiteral<ui64>(20);
    const auto data3 = pb.NewDataLiteral<ui64>(30);
    const auto rightScalar = pb.AsScalar(pb.NewDataLiteral<ui64>(100));
    const auto leftScalar = pb.AsScalar(pb.NewDataLiteral<ui64>(1000));

    const auto list = pb.NewList(ui64Type, {data1, data2, data3});
    const auto flow = pb.ToFlow(list);
    const auto blocksFlow = pb.ToBlocks(flow);
    const auto sumBlocksFlow = pb.Map(blocksFlow, [&](TRuntimeNode item) -> TRuntimeNode {
        return {pb.BlockFunc("Add", ui64BlockType, { leftScalar, {pb.BlockFunc("Add", ui64BlockType, { item, rightScalar } )}})};
    });
    const auto pgmReturn = pb.Collect(pb.FromBlocks(sumBlocksFlow));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 1110);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 1120);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 1130);

    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestWideFromBlocks) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);

    const auto data1 = pb.NewDataLiteral<ui64>(10);
    const auto data2 = pb.NewDataLiteral<ui64>(20);
    const auto data3 = pb.NewDataLiteral<ui64>(30);

    const auto list = pb.NewList(ui64Type, {data1, data2, data3});
    const auto flow = pb.ToFlow(list);

    const auto blocksFlow = pb.ToBlocks(flow);
    const auto wideFlow = pb.ExpandMap(blocksFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item, pb.AsScalar(pb.NewDataLiteral<ui64>(3ULL))}; });
    const auto wideFlow2 = pb.WideFromBlocks(wideFlow);
    const auto narrowFlow = pb.NarrowMap(wideFlow2, [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); });

    const auto pgmReturn = pb.Collect(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 10);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 20);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 30);

    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestWideToAndFromBlocks) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto tupleType = pb.NewTupleType({ui64Type, ui64Type});

    const auto data1 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(1), pb.NewDataLiteral<ui64>(10)});
    const auto data2 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(2), pb.NewDataLiteral<ui64>(20)});
    const auto data3 = pb.NewTuple(tupleType, {pb.NewDataLiteral<ui64>(3), pb.NewDataLiteral<ui64>(30)});

    const auto list = pb.NewList(tupleType, {data1, data2, data3});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });
    const auto wideBlocksFlow = pb.WideToBlocks(wideFlow);
    const auto wideFlow2 = pb.WideFromBlocks(wideBlocksFlow);
    const auto narrowFlow = pb.NarrowMap(wideFlow2, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return items[1];
    });

    const auto pgmReturn = pb.ForwardList(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 10);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 20);

    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.Get<ui64>(), 30);

    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}
}

Y_UNIT_TEST_SUITE(TMiniKQLDirectKernelTest) {
Y_UNIT_TEST(Simple) {
    TSetup<false> setup;
    auto& pb = *setup.PgmBuilder;

    const auto boolType = pb.NewDataType(NUdf::TDataType<bool>::Id);
    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto boolBlocksType = pb.NewBlockType(boolType, TBlockType::EShape::Many);
    const auto ui64BlocksType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);
    const auto arg1 = pb.Arg(boolBlocksType);
    const auto arg2 = pb.Arg(ui64BlocksType);
    const auto arg3 = pb.Arg(ui64BlocksType);
    const auto ifNode = pb.BlockIf(arg1, arg2, arg3);
    const auto eqNode = pb.BlockFunc("Equals", boolBlocksType, { ifNode, arg2 });

    const auto graph = setup.BuildGraph(eqNode, {arg1.GetNode(), arg2.GetNode(), arg3.GetNode()});
    const auto topology = graph->GetKernelsTopology();
    UNIT_ASSERT(topology);
    UNIT_ASSERT_VALUES_EQUAL(topology->InputArgsCount, 3);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[0].Node->GetKernelName(), "If");
    const std::vector<ui32> expectedInputs1{{0, 1, 2}};
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[0].Inputs, expectedInputs1);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[1].Node->GetKernelName(), "Equals");
    const std::vector<ui32> expectedInputs2{{3, 1}};
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[1].Inputs, expectedInputs2);

    arrow::compute::ExecContext execContext;
    const size_t blockSize = 100000;
    std::vector<arrow::Datum> datums(topology->InputArgsCount + topology->Items.size());
    {
        arrow::UInt8Builder builder1(execContext.memory_pool());
        arrow::UInt64Builder builder2(execContext.memory_pool()), builder3(execContext.memory_pool());
        ARROW_OK(builder1.Reserve(blockSize));
        ARROW_OK(builder2.Reserve(blockSize));
        ARROW_OK(builder3.Reserve(blockSize));
        for (size_t i = 0; i < blockSize; ++i) {
            builder1.UnsafeAppend(i & 1);
            builder2.UnsafeAppend(i);
            builder3.UnsafeAppend(3 * i);
        }

        std::shared_ptr<arrow::ArrayData> data1;
        ARROW_OK(builder1.FinishInternal(&data1));
        std::shared_ptr<arrow::ArrayData> data2;
        ARROW_OK(builder2.FinishInternal(&data2));
        std::shared_ptr<arrow::ArrayData> data3;
        ARROW_OK(builder3.FinishInternal(&data3));
        datums[0] = data1;
        datums[1] = data2;
        datums[2] = data3;
    }

    ExecuteAllKernels(datums, topology, execContext);

    auto res = datums.back().array()->GetValues<ui8>(1);
    for (size_t i = 0; i < blockSize; ++i) {
        auto expected = (((i & 1) ? i : i * 3) == i) ? 1 : 0;
        UNIT_ASSERT_VALUES_EQUAL(res[i], expected);
    }
}

Y_UNIT_TEST(WithScalars) {
    TSetup<false> setup;
    auto& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto ui64BlocksType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);
    const auto scalar = pb.AsScalar(pb.NewDataLiteral(false));
    const auto arg1 = pb.Arg(ui64BlocksType);
    const auto arg2 = pb.Arg(ui64BlocksType);
    const auto ifNode = pb.BlockIf(scalar, arg1, arg2);

    const auto graph = setup.BuildGraph(ifNode, {arg1.GetNode(), arg2.GetNode()});
    const auto topology = graph->GetKernelsTopology();
    UNIT_ASSERT(topology);
    UNIT_ASSERT_VALUES_EQUAL(topology->InputArgsCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items.size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[0].Node->GetKernelName(), "AsScalar");
    const std::vector<ui32> expectedInputs1;
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[0].Inputs, expectedInputs1);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[1].Node->GetKernelName(), "If");
    const std::vector<ui32> expectedInputs2{{2, 0, 1}};
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[1].Inputs, expectedInputs2);

    arrow::compute::ExecContext execContext;
    const size_t blockSize = 100000;
    std::vector<arrow::Datum> datums(topology->InputArgsCount + topology->Items.size());
    {
        arrow::UInt64Builder builder1(execContext.memory_pool()), builder2(execContext.memory_pool());
        ARROW_OK(builder1.Reserve(blockSize));
        ARROW_OK(builder2.Reserve(blockSize));
        for (size_t i = 0; i < blockSize; ++i) {
            builder1.UnsafeAppend(i);
            builder2.UnsafeAppend(3 * i);
        }

        std::shared_ptr<arrow::ArrayData> data1;
        ARROW_OK(builder1.FinishInternal(&data1));
        std::shared_ptr<arrow::ArrayData> data2;
        ARROW_OK(builder2.FinishInternal(&data2));
        datums[0] = data1;
        datums[1] = data2;
    }

    ExecuteAllKernels(datums, topology, execContext);

    auto res = datums.back().array()->GetValues<ui64>(1);
    for (size_t i = 0; i < blockSize; ++i) {
        auto expected = 3 * i;
        UNIT_ASSERT_VALUES_EQUAL(res[i], expected);
    }
}

Y_UNIT_TEST(Udf) {
    TVector<TUdfModuleInfo> modules;
    modules.emplace_back(TUdfModuleInfo{"", "BlockUT", new TBlockUTModule()});
    TSetup<false> setup(GetTestFactory(), std::move(modules));

    auto& pb = *setup.PgmBuilder;

    const auto i32Type = pb.NewDataType(NUdf::TDataType<i32>::Id);
    const auto i32BlocksType = pb.NewBlockType(i32Type, TBlockType::EShape::Many);
    const auto arg1 = pb.Arg(i32BlocksType);
    const auto userType = pb.NewTupleType({
        pb.NewTupleType({i32BlocksType}),
        pb.NewEmptyStructType(),
        pb.NewEmptyTupleType()});
    const auto udf = pb.Udf("BlockUT.Inc_BlocksImpl", pb.NewVoid(), userType);
    const auto apply = pb.Apply(udf, {arg1});

    const auto graph = setup.BuildGraph(apply, {arg1.GetNode() });
    const auto topology = graph->GetKernelsTopology();
    UNIT_ASSERT(topology);
    UNIT_ASSERT_VALUES_EQUAL(topology->InputArgsCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[0].Node->GetKernelName(), "Apply");
    const std::vector<ui32> expectedInputs1{0};
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[0].Inputs, expectedInputs1);

    arrow::compute::ExecContext execContext;
    const size_t blockSize = 10000;
    std::vector<arrow::Datum> datums(topology->InputArgsCount + topology->Items.size());
    {
        arrow::Int32Builder builder1(execContext.memory_pool());
        ARROW_OK(builder1.Reserve(blockSize));
        for (size_t i = 0; i < blockSize; ++i) {
            builder1.UnsafeAppend(i);
        }

        std::shared_ptr<arrow::ArrayData> data1;
        ARROW_OK(builder1.FinishInternal(&data1));
        datums[0] = data1;
    }

    ExecuteAllKernels(datums, topology, execContext);

    auto res = datums.back().array()->GetValues<i32>(1);
    for (size_t i = 0; i < blockSize; ++i) {
        auto expected = i + 1;
        UNIT_ASSERT_VALUES_EQUAL(res[i], expected);
    }
}

Y_UNIT_TEST(ScalarApply) {
    TSetup<false> setup;
    auto& pb = *setup.PgmBuilder;

    const auto ui64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id);
    const auto ui64BlocksType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);
    const auto arg1 = pb.Arg(ui64BlocksType);
    const auto arg2 = pb.Arg(ui64BlocksType);
    const auto scalarApply = pb.ScalarApply({arg1,arg2}, [&](auto args){
        return pb.Add(args[0], args[1]);
    });

    const auto graph = setup.BuildGraph(scalarApply, {arg1.GetNode(), arg2.GetNode()});
    const auto topology = graph->GetKernelsTopology();
    UNIT_ASSERT(topology);
    UNIT_ASSERT_VALUES_EQUAL(topology->InputArgsCount, 2);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[0].Node->GetKernelName(), "ScalarApply");
    const std::vector<ui32> expectedInputs1{{0, 1}};
    UNIT_ASSERT_VALUES_EQUAL(topology->Items[0].Inputs, expectedInputs1);

    arrow::compute::ExecContext execContext;
    const size_t blockSize = 100000;
    std::vector<arrow::Datum> datums(topology->InputArgsCount + topology->Items.size());
    {
        arrow::UInt64Builder builder1(execContext.memory_pool()), builder2(execContext.memory_pool());
        ARROW_OK(builder1.Reserve(blockSize));
        ARROW_OK(builder2.Reserve(blockSize));
        for (size_t i = 0; i < blockSize; ++i) {
            builder1.UnsafeAppend(i);
            builder2.UnsafeAppend(2 * i);
        }

        std::shared_ptr<arrow::ArrayData> data1;
        ARROW_OK(builder1.FinishInternal(&data1));
        std::shared_ptr<arrow::ArrayData> data2;
        ARROW_OK(builder2.FinishInternal(&data2));
        datums[0] = data1;
        datums[1] = data2;
    }

    ExecuteAllKernels(datums, topology, execContext);

    auto res = datums.back().array()->GetValues<ui64>(1);
    for (size_t i = 0; i < blockSize; ++i) {
        auto expected = 3 * i;
        UNIT_ASSERT_VALUES_EQUAL(res[i], expected);
    }
}

}

}
}
