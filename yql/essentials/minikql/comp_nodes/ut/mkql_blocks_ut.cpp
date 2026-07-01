#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

#include <arrow/compute/exec_internal.h>
#include <arrow/array/builder_primitive.h>

#include <yql/essentials/core/sql_types/block.h>
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/public/udf/arrow/udf_arrow_helpers.h>

BEGIN_SIMPLE_ARROW_UDF(TInc, i32(i32)) {
    Y_UNUSED(valueBuilder);
    const i32 arg = args[0].Get<i32>();
    return NYql::NUdf::TUnboxedValuePod(arg + 1);
}

struct TIncKernelExec: public NYql::NUdf::TUnaryKernelExec<TIncKernelExec> {
    template <typename TSink>
    static void Process(const NYql::NUdf::IValueBuilder* valueBuilder, NYql::NUdf::TBlockItem arg, const TSink& sink) {
        Y_UNUSED(valueBuilder);
        sink(NYql::NUdf::TBlockItem(arg.As<i32>() + 1));
    }
};

END_SIMPLE_ARROW_UDF(TInc, TIncKernelExec::Do);

SIMPLE_MODULE(TBlockUTModule,
              TInc)

namespace NKikimr {
namespace NMiniKQL {

namespace {
arrow::Datum ExecuteOneKernel(const IArrowKernelComputationNode* kernelNode,
                              const std::vector<arrow::Datum>& argDatums, arrow::compute::ExecContext& execContext) {
    const auto& kernel = kernelNode->GetArrowKernel();
    arrow::compute::KernelContext kernelContext(&execContext);
    std::unique_ptr<arrow::compute::KernelState> state;
    if (kernel.init) {
        state = ARROW_RESULT(kernel.init(&kernelContext, {&kernel, kernelNode->GetArgsDesc(), nullptr}));
        kernelContext.SetState(state.get());
    }

    auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
    ARROW_OK(executor->Init(&kernelContext, {&kernel, kernelNode->GetArgsDesc(), nullptr}));
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

// Hand-made variant using WideFromBlocks (in order to test ListToBlocks by well-tested nodes rather than actual ListFromBlocks)
TRuntimeNode ListFromBlocks(TProgramBuilder& pb, TRuntimeNode blockList) {
    const auto wideBlocksStream = pb.FromFlow(pb.ExpandMap(pb.ToFlow(blockList), [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {
            pb.Member(item, "key"),
            pb.Member(item, "value"),
            pb.Member(item, NYql::BlockLengthColumnName)};
    }));

    return pb.ForwardList(pb.NarrowMap(pb.ToFlow(pb.WideFromBlocks(wideBlocksStream)), [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewStruct({{"key", items[0]},
                             {"value", items[1]}});
    }));
}

// Hand-made variant using WideToBlocks (in order to test ListFromBlocks by well-tested nodes rather than actual ListToBlocks)
TRuntimeNode ListToBlocks(TProgramBuilder& pb, TRuntimeNode list) {
    const auto wideBlocksStream = pb.WideToBlocks(pb.FromFlow(pb.ExpandMap(pb.ToFlow(list), [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {
            pb.Member(item, "key"),
            pb.Member(item, "value")};
    })));

    return pb.Collect(pb.NarrowMap(pb.ToFlow(wideBlocksStream), [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewStruct({{"key", items[0]},
                             {"value", items[1]},
                             {NYql::BlockLengthColumnName, items[2]}});
    }));
}

using TListTransformer = std::function<TRuntimeNode(TProgramBuilder&, TRuntimeNode)>;

void DoTestListToAndFromBlocks(TSetup<false>& setup, TListTransformer listToBlocksImpl, TListTransformer listFromBlocksImpl) {
    constexpr size_t TEST_SIZE = 1 << 16;
    const TString hugeString(128, '1');

    TProgramBuilder& pb = *setup.PgmBuilder;

    using TKeyValueStruct = NTest::TStructType<
        NTest::TStructMember<"key", ui64>,
        NTest::TStructMember<"value", TStringBuf>>;

    TVector<TString> strings;
    strings.reserve(TEST_SIZE);
    TVector<TKeyValueStruct> listItems;
    listItems.reserve(TEST_SIZE);
    for (size_t i = 0; i < TEST_SIZE; i++) {
        strings.push_back(hugeString + ToString(i));
        // Huge string is used to make less rows fit into one block (in order to test output slicing)
        listItems.push_back(TKeyValueStruct{std::make_tuple(
            NTest::TStructMember<"key", ui64>{ui64(i)},
            NTest::TStructMember<"value", TStringBuf>{TStringBuf(strings.back())})});
    }

    const auto list = NTest::ConvertValueToLiteralNode(pb, listItems);
    const auto blockList = listToBlocksImpl(pb, list);

    const auto graph = setup.BuildGraph(listFromBlocksImpl(pb, blockList));

    TVector<TKeyValueStruct> expected;
    expected.reserve(TEST_SIZE);
    for (size_t i = 0; i < TEST_SIZE; i++) {
        expected.push_back(TKeyValueStruct{std::make_tuple(
            NTest::TStructMember<"key", ui64>{ui64(i)},
            NTest::TStructMember<"value", TStringBuf>{TStringBuf(strings[i])})});
    }

    AssertUnboxedValueElementEqual(graph->GetValue(), expected);
}
} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLBlocksTest) {
Y_UNIT_TEST_LLVM(TestEmpty) {
    TSetup<LLVM> setup;
    auto& pb = *setup.PgmBuilder;

    const auto type = NTest::ConvertToMinikqlType<ui64>(pb);
    const auto list = pb.NewEmptyList(type);
    const auto sourceFlow = pb.ToFlow(list);
    const auto flowAfterBlocks = pb.FromBlocks(pb.ToBlocks(sourceFlow));
    const auto pgmReturn = pb.ForwardList(flowAfterBlocks);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>{});
}

Y_UNIT_TEST_LLVM(TestSimple) {
    static const size_t dataCount = 1000;
    TSetup<LLVM> setup;
    auto& pb = *setup.PgmBuilder;

    TVector<ui64> data(dataCount);
    std::iota(data.begin(), data.end(), 0ULL);
    const auto list = NTest::ConvertValueToLiteralNode(pb, data);
    const auto sourceFlow = pb.ToFlow(list);
    const auto flowAfterBlocks = pb.FromBlocks(pb.ToBlocks(sourceFlow));
    const auto pgmReturn = pb.ForwardList(flowAfterBlocks);

    const auto graph = setup.BuildGraph(pgmReturn);

    TVector<ui64> expected(dataCount);
    std::iota(expected.begin(), expected.end(), 0ULL);
    AssertUnboxedValueElementEqual(graph->GetValue(), expected);
}

Y_UNIT_TEST_LLVM(TestWideToBlocks) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, ui64>>{{1, 10}, {2, 20}, {3, 30}});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });
    const auto wideBlocksFlow = pb.ToFlow(pb.WideToBlocks(pb.FromFlow(wideFlow)));
    const auto narrowBlocksFlow = pb.NarrowMap(wideBlocksFlow, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return items[1];
    });
    const auto narrowFlow = pb.FromBlocks(narrowBlocksFlow);
    const auto pgmReturn = pb.ForwardList(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>{10, 20, 30});
}

Y_UNIT_TEST(TestListToBlocks) {
    TSetup<false> setup;

    DoTestListToAndFromBlocks(
        setup,
        [](TProgramBuilder& pb, TRuntimeNode list) { return pb.ListToBlocks(list); },
        ListFromBlocks);
}

Y_UNIT_TEST(TestListToBlocksMultiUsage) {
    constexpr size_t TEST_SIZE = 1 << 10;

    TSetup<false> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TKeyValueStruct = NTest::TStructType<
        NTest::TStructMember<"key", ui64>,
        NTest::TStructMember<"value", TStringBuf>>;

    TVector<TString> strings;
    strings.reserve(TEST_SIZE);
    TVector<TKeyValueStruct> listItems;
    listItems.reserve(TEST_SIZE);
    for (size_t i = 0; i < TEST_SIZE; i++) {
        strings.push_back(ToString(i));
        listItems.push_back(TKeyValueStruct{std::make_tuple(
            NTest::TStructMember<"key", ui64>{ui64(i)},
            NTest::TStructMember<"value", TStringBuf>{TStringBuf(strings.back())})});
    }

    const auto list = NTest::ConvertValueToLiteralNode(pb, listItems);
    const auto blockList1 = pb.ListToBlocks(list);
    const auto blockList2 = pb.ListToBlocks(list);

    const auto result = pb.Zip({ListFromBlocks(pb, blockList1), ListFromBlocks(pb, blockList2)});

    const auto graph = setup.BuildGraph(result);

    using TPairStruct = std::tuple<TKeyValueStruct, TKeyValueStruct>;
    TVector<TPairStruct> expected;
    expected.reserve(TEST_SIZE);
    for (size_t i = 0; i < TEST_SIZE; i++) {
        TKeyValueStruct kv{std::make_tuple(
            NTest::TStructMember<"key", ui64>{ui64(i)},
            NTest::TStructMember<"value", TStringBuf>{TStringBuf(strings[i])})};
        expected.push_back(TPairStruct{kv, kv});
    }

    AssertUnboxedValueElementEqual(graph->GetValue(), expected);
}

namespace {
template <bool LLVM>
void TestChunked(bool withBlockExpand) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<ui64, bool, TStringBuf, NTest::TUtf8>>(pb);

    TRuntimeNode::TList items;
    const size_t bigStrSize = 1024 * 1024 + 100;
    const size_t smallStrSize = 256 * 1024;
    for (size_t i = 0; i < 20; ++i) {
        if (i % 2 == 0) {
            std::string big(bigStrSize, '0' + i);
            std::string small(smallStrSize, 'A' + i);

            items.push_back(NTest::ConvertValueToLiteralNode(pb, std::tuple<ui64, bool, TStringBuf, NTest::TUtf8>{
                                                                     ui64(i), true, TStringBuf(big), NTest::TUtf8{TStringBuf(small)}}));
        } else {
            items.push_back(NTest::ConvertValueToLiteralNode(pb, std::tuple<ui64, bool, TStringBuf, NTest::TUtf8>{
                                                                     ui64(i), false, TStringBuf(""), NTest::TUtf8{TStringBuf("")}}));
        }
    }

    const auto list = pb.NewList(tupleType, std::move(items));

    auto node = pb.ToFlow(list);
    node = pb.ExpandMap(node, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U)};
    });
    node = pb.WideToBlocks(pb.FromFlow(node));
    if (withBlockExpand) {
        node = pb.BlockExpandChunked(node);
        // WideTakeBlocks won't work on chunked blocks
        node = pb.WideTakeBlocks(node, NTest::ConvertValueToLiteralNode(pb, ui64(19)));
        node = pb.ToFlow(pb.WideFromBlocks(node));
    } else {
        // WideFromBlocks should support chunked blocks
        node = pb.ToFlow(pb.WideFromBlocks(node));
        node = pb.Take(node, NTest::ConvertValueToLiteralNode(pb, ui64(19)));
    }
    node = pb.NarrowMap(node, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return pb.NewTuple(tupleType, {items[0], items[1], items[2], items[3]});
    });

    const auto pgmReturn = pb.ForwardList(node);
    const auto graph = setup.BuildGraph(pgmReturn);

    using TRow = std::tuple<ui64, bool, TStringBuf, NTest::TUtf8>;
    TVector<TString> storedBig, storedSmall;
    storedBig.reserve(10);
    storedSmall.reserve(10);
    TVector<TRow> expected;
    expected.reserve(19);
    for (size_t i = 0; i < 19; ++i) {
        if (i % 2 == 0) {
            storedBig.push_back(TString(bigStrSize, '0' + i));
            storedSmall.push_back(TString(smallStrSize, 'A' + i));
            expected.push_back(TRow{ui64(i), true, TStringBuf(storedBig.back()), NTest::TUtf8{TStringBuf(storedSmall.back())}});
        } else {
            expected.push_back(TRow{ui64(i), false, TStringBuf(""), NTest::TUtf8{TStringBuf("")}});
        }
    }

    AssertUnboxedValueElementEqual(graph->GetValue(), expected);
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

    const auto dataAfterBlocks = pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, ui64(testValue)));

    const auto graph = setup.BuildGraph(dataAfterBlocks);
    const auto value = graph->GetValue();
    UNIT_ASSERT(value.HasValue() && value.IsBoxed());
    UNIT_ASSERT_VALUES_EQUAL(TArrowBlock::From(value).GetDatum().scalar_as<arrow::UInt64Scalar>().value, testValue);
}

template <auto Type, typename ArrowType>
void TestContainerForStringType() {
    TSetup<false> setup;
    auto dataLiteral = setup.PgmBuilder->NewDataLiteral<Type>("\"Just a string\"");
    const auto dataAfterBlocks = setup.PgmBuilder->AsScalar(dataLiteral);
    const auto graph = setup.BuildGraph(dataAfterBlocks);
    const auto value = graph->GetValue();

    UNIT_ASSERT(typeid(*TArrowBlock::From(value).GetDatum().scalar()) == typeid(ArrowType));
}

Y_UNIT_TEST(TestStringTypesHasAppropriateContainer) {
    TestContainerForStringType<NUdf::EDataSlot::Utf8, arrow::StringScalar>();
    TestContainerForStringType<NUdf::EDataSlot::Json, arrow::StringScalar>();
    TestContainerForStringType<NUdf::EDataSlot::Yson, arrow::BinaryScalar>();
    TestContainerForStringType<NUdf::EDataSlot::String, arrow::BinaryScalar>();
}

Y_UNIT_TEST_LLVM(TestReplicateScalar) {
    const ui64 count = 1000;
    const ui32 value = 42;

    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto valueType = NTest::ConvertToMinikqlType<ui32>(pb);

    const auto scalarValue = pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, ui32(value)));
    const auto scalarCount = pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, ui64(count)));

    const auto replicated = pb.ReplicateScalar(scalarValue, scalarCount);

    const auto replicatedType = pb.NewBlockType(valueType, TBlockType::EShape::Many);

    const auto listOfReplicated = pb.NewList(replicatedType, {replicated});

    const auto flowOfReplicated = pb.ToFlow(listOfReplicated);

    const auto flowAfterBlocks = pb.FromBlocks(flowOfReplicated);
    const auto pgmReturn = pb.ForwardList(flowAfterBlocks);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>(count, ui32(value)));
}

Y_UNIT_TEST_LLVM(TestBlockFunc) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = NTest::ConvertToMinikqlType<ui64>(pb);
    const auto ui64BlockType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, ui64>>{{1, 10}, {2, 20}, {3, 30}});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });
    const auto wideBlocksStream = pb.WideToBlocks(pb.FromFlow(wideFlow));
    const auto sumWideStream = pb.WideMap(wideBlocksStream, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
        return {pb.BlockFunc("Add", ui64BlockType, {items[0], items[1]})};
    });
    const auto sumNarrowFlow = pb.NarrowMap(pb.ToFlow(sumWideStream), [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return items[0];
    });
    const auto pgmReturn = pb.Collect(pb.FromBlocks(sumNarrowFlow));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>{11, 22, 33});
}

Y_UNIT_TEST_LLVM(TestBlockFuncWithNullables) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalUi64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id, true);
    const auto ui64OptBlockType = pb.NewBlockType(optionalUi64Type, TBlockType::EShape::Many);

    using TOptPair = std::tuple<TMaybe<ui64>, TMaybe<ui64>>;
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TOptPair>{
                                                               {TMaybe<ui64>(1), TMaybe<ui64>{}},
                                                               {TMaybe<ui64>{}, TMaybe<ui64>(20)},
                                                               {TMaybe<ui64>{}, TMaybe<ui64>{}},
                                                               {TMaybe<ui64>(10), TMaybe<ui64>(20)},
                                                           });
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });
    const auto wideBlocksStream = pb.WideToBlocks(pb.FromFlow(wideFlow));
    const auto sumWideStream = pb.WideMap(wideBlocksStream, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
        return {pb.BlockFunc("Add", ui64OptBlockType, {items[0], items[1]})};
    });
    const auto sumNarrowFlow = pb.NarrowMap(pb.ToFlow(sumWideStream), [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return items[0];
    });
    const auto pgmReturn = pb.Collect(pb.FromBlocks(sumNarrowFlow));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<ui64>>{TMaybe<ui64>{}, TMaybe<ui64>{}, TMaybe<ui64>{}, TMaybe<ui64>(30)});
}

Y_UNIT_TEST_LLVM(TestBlockFuncWithNullableScalar) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalUi64Type = pb.NewDataType(NUdf::TDataType<ui64>::Id, true);
    const auto ui64OptBlockType = pb.NewBlockType(optionalUi64Type, TBlockType::EShape::Many);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<ui64>>{TMaybe<ui64>(10), TMaybe<ui64>(20), TMaybe<ui64>(30)});
    const auto flow = pb.ToFlow(list);
    const auto blocksFlow = pb.ToBlocks(flow);

    THolder<IComputationGraph> graph;
    auto buildAndCheck = [&](const TProgramBuilder::TUnaryLambda& func, const TVector<TMaybe<ui64>>& expected) {
        const auto pgmReturn = pb.Collect(pb.FromBlocks(pb.Map(blocksFlow, func)));
        graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), expected);
    };

    {
        const auto scalar = pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>{}));
        buildAndCheck([&](TRuntimeNode item) -> TRuntimeNode {
            return {pb.BlockFunc("Add", ui64OptBlockType, {scalar, item})};
        },
                      {TMaybe<ui64>{}, TMaybe<ui64>{}, TMaybe<ui64>{}});
    }

    {
        const auto scalar = pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>{}));
        buildAndCheck([&](TRuntimeNode item) -> TRuntimeNode {
            return {pb.BlockFunc("Add", ui64OptBlockType, {item, scalar})};
        },
                      {TMaybe<ui64>{}, TMaybe<ui64>{}, TMaybe<ui64>{}});
    }

    {
        const auto scalar = pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, ui64(100)));
        buildAndCheck([&](TRuntimeNode item) -> TRuntimeNode {
            return {pb.BlockFunc("Add", ui64OptBlockType, {item, scalar})};
        },
                      {TMaybe<ui64>(110), TMaybe<ui64>(120), TMaybe<ui64>(130)});
    }
}

Y_UNIT_TEST_LLVM(TestBlockFuncWithScalar) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto ui64Type = NTest::ConvertToMinikqlType<ui64>(pb);
    const auto ui64BlockType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);

    const auto rightScalar = pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, ui64(100)));
    const auto leftScalar = pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, ui64(1000)));

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui64>{10, 20, 30});
    const auto flow = pb.ToFlow(list);
    const auto blocksFlow = pb.ToBlocks(flow);
    const auto sumBlocksFlow = pb.Map(blocksFlow, [&](TRuntimeNode item) -> TRuntimeNode {
        return {pb.BlockFunc("Add", ui64BlockType, {leftScalar, {pb.BlockFunc("Add", ui64BlockType, {item, rightScalar})}})};
    });
    const auto pgmReturn = pb.Collect(pb.FromBlocks(sumBlocksFlow));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>{1110, 1120, 1130});
}

Y_UNIT_TEST_LLVM(TestWideFromBlocks) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui64>{10, 20, 30});
    const auto flow = pb.ToFlow(list);

    const auto blocksFlow = pb.ToBlocks(flow);
    const auto wideFlow = pb.ExpandMap(blocksFlow, [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item, pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, ui64(3ULL)))}; });
    const auto wideFlow2 = pb.ToFlow(pb.WideFromBlocks(pb.FromFlow(wideFlow)));
    const auto narrowFlow = pb.NarrowMap(wideFlow2, [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); });

    const auto pgmReturn = pb.Collect(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>{10, 20, 30});
}

Y_UNIT_TEST(TestListFromBlocks) {
    TSetup<false> setup;

    DoTestListToAndFromBlocks(
        setup,
        ListToBlocks,
        [](TProgramBuilder& pb, TRuntimeNode list) { return pb.ListFromBlocks(list); });
}

Y_UNIT_TEST_LLVM(TestWideToAndFromBlocks) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui64, ui64>>{{1, 10}, {2, 20}, {3, 30}});
    const auto flow = pb.ToFlow(list);

    const auto wideFlow = pb.ExpandMap(flow, [&](TRuntimeNode item) -> TRuntimeNode::TList {
        return {pb.Nth(item, 0U), pb.Nth(item, 1U)};
    });
    const auto wideBlocksFlow = pb.ToFlow(pb.WideToBlocks(pb.FromFlow(wideFlow)));
    const auto wideFlow2 = pb.ToFlow(pb.WideFromBlocks(pb.FromFlow(wideBlocksFlow)));
    const auto narrowFlow = pb.NarrowMap(wideFlow2, [&](TRuntimeNode::TList items) -> TRuntimeNode {
        return items[1];
    });

    const auto pgmReturn = pb.ForwardList(narrowFlow);

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>{10, 20, 30});
}

Y_UNIT_TEST(TestListToAndFromBlocks) {
    TSetup<false> setup;

    DoTestListToAndFromBlocks(
        setup,
        [](TProgramBuilder& pb, TRuntimeNode list) { return pb.ListToBlocks(list); },
        [](TProgramBuilder& pb, TRuntimeNode list) { return pb.ListFromBlocks(list); });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLBlocksTest)

Y_UNIT_TEST_SUITE(TMiniKQLDirectKernelTest) {
Y_UNIT_TEST(Simple) {
    TSetup<false> setup;
    auto& pb = *setup.PgmBuilder;

    const auto boolType = NTest::ConvertToMinikqlType<bool>(pb);
    const auto ui64Type = NTest::ConvertToMinikqlType<ui64>(pb);
    const auto boolBlocksType = pb.NewBlockType(boolType, TBlockType::EShape::Many);
    const auto ui64BlocksType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);
    const auto arg1 = pb.Arg(boolBlocksType);
    const auto arg2 = pb.Arg(ui64BlocksType);
    const auto arg3 = pb.Arg(ui64BlocksType);
    const auto ifNode = pb.BlockIf(arg1, arg2, arg3);
    const auto eqNode = pb.BlockFunc("Equals", boolBlocksType, {ifNode, arg2});

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

    const auto ui64Type = NTest::ConvertToMinikqlType<ui64>(pb);
    const auto ui64BlocksType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);
    const auto scalar = pb.AsScalar(NTest::ConvertValueToLiteralNode(pb, false));
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

    const auto i32Type = NTest::ConvertToMinikqlType<i32>(pb);
    const auto i32BlocksType = pb.NewBlockType(i32Type, TBlockType::EShape::Many);
    const auto arg1 = pb.Arg(i32BlocksType);
    const auto userType = pb.NewTupleType({pb.NewTupleType({i32BlocksType}),
                                           pb.NewEmptyStructType(),
                                           pb.NewEmptyTupleType()});
    const auto udf = pb.Udf("BlockUT.Inc_BlocksImpl", NTest::ConvertValueToLiteralNode(pb, NTest::TSingularVoid{}), userType);
    const auto apply = pb.Apply(udf, {arg1});

    const auto graph = setup.BuildGraph(apply, {arg1.GetNode()});
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

    const auto ui64Type = NTest::ConvertToMinikqlType<ui64>(pb);
    const auto ui64BlocksType = pb.NewBlockType(ui64Type, TBlockType::EShape::Many);
    const auto arg1 = pb.Arg(ui64BlocksType);
    const auto arg2 = pb.Arg(ui64BlocksType);
    const auto scalarApply = pb.ScalarApply({arg1, arg2}, [&](auto args) {
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

} // Y_UNIT_TEST_SUITE(TMiniKQLDirectKernelTest)

} // namespace NMiniKQL
} // namespace NKikimr
