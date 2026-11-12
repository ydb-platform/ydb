#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>
#include <yql/essentials/public/udf/arrow/defs.h>

#include <arrow/array/builder_binary.h>
#include <arrow/compute/kernel.h>

namespace NKikimr::NMiniKQL {

namespace {

THolder<IComputationGraph> BuildToStringBlockGraph(TSetup<false>& setup) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto optionalUtf8Type = pb.NewDataType(NUdf::TDataType<NUdf::TUtf8>::Id, /*optional=*/true);
    const auto optionalStringType = pb.NewDataType(NUdf::TDataType<char*>::Id, /*optional=*/true);
    const auto utf8BlockType = pb.NewBlockType(optionalUtf8Type, TBlockType::EShape::Many);
    const auto stringBlockType = pb.NewBlockType(optionalStringType, TBlockType::EShape::Many);
    const auto arg = pb.Arg(utf8BlockType);
    return setup.BuildGraph(pb.BlockFunc("ToString", stringBlockType, {arg}), {arg.GetNode()});
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLConvertTest) {
Y_UNIT_TEST_LLVM(ConvertUI8ToBool) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui8>{0, 1, 2, 3, 4});

    const auto pgmReturn = pb.Map(list, [&pb](const TRuntimeNode item) {
        return pb.Convert(item, NTest::ConvertToMinikqlType<bool>(pb));
    });
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui8>{0, 1, 1, 1, 1});
}

Y_UNIT_TEST_LLVM(ToString) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalStringType = pb.NewDataType(NUdf::TDataType<char*>::Id, /*optional=*/true);
    const auto stringBlockType = pb.NewBlockType(optionalStringType, TBlockType::EShape::Many);
    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TMaybe<NTest::TUtf8>>{
                                                               NTest::TUtf8{"first"},
                                                               TMaybe<NTest::TUtf8>{},
                                                               NTest::TUtf8{"third"},
                                                           });
    const auto blocksFlow = pb.ToBlocks(pb.ToFlow(list, {}));
    const auto convertedBlocksFlow = pb.Map(blocksFlow, [&](TRuntimeNode item) -> TRuntimeNode {
        return pb.BlockFunc("ToString", stringBlockType, {item});
    });
    const auto pgmReturn = pb.Collect(pb.FromBlocks(convertedBlocksFlow));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TMaybe<TStringBuf>>{
                                                          TMaybe<TStringBuf>{"first"},
                                                          TMaybe<TStringBuf>{},
                                                          TMaybe<TStringBuf>{"third"},
                                                      });
}

Y_UNIT_TEST(ToStringArrayReusesBuffers) {
    TSetup<false> setup;
    const auto graph = BuildToStringBlockGraph(setup);
    const auto topology = graph->GetKernelsTopology();
    UNIT_ASSERT(topology);
    UNIT_ASSERT_VALUES_EQUAL(topology->InputArgsCount, 1);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items.size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items.front().Node->GetKernelName(), "ToString");

    arrow::compute::ExecContext execContext;
    arrow::StringBuilder builder(execContext.memory_pool());
    ARROW_OK(builder.Append("first"));
    ARROW_OK(builder.AppendNull());
    ARROW_OK(builder.Append("third"));

    std::shared_ptr<arrow::ArrayData> inputArray;
    ARROW_OK(builder.FinishInternal(&inputArray));
    const auto* kernelNode = topology->Items.front().Node.get();
    arrow::compute::KernelContext kernelContext(&execContext);
    const arrow::compute::ExecBatch arrayBatch({arrow::Datum(inputArray)}, inputArray->length);
    arrow::Datum arrayOutput;
    ARROW_OK(kernelNode->GetArrowKernel().exec(&kernelContext, arrayBatch, &arrayOutput));
    UNIT_ASSERT(arrayOutput.is_array());
    UNIT_ASSERT(arrayOutput.array()->type->id() == arrow::Type::BINARY);
    UNIT_ASSERT_VALUES_EQUAL(arrayOutput.array()->buffers.size(), inputArray->buffers.size());
    for (size_t i = 0; i < inputArray->buffers.size(); ++i) {
        UNIT_ASSERT_EQUAL(arrayOutput.array()->buffers[i], inputArray->buffers[i]);
    }
}

Y_UNIT_TEST(ToStringScalar) {
    TSetup<false> setup;
    const auto graph = BuildToStringBlockGraph(setup);
    const auto topology = graph->GetKernelsTopology();
    UNIT_ASSERT(topology);
    UNIT_ASSERT_VALUES_EQUAL(topology->Items.size(), 1);

    const auto scalarInput = std::make_shared<arrow::StringScalar>("scalar");
    arrow::compute::ExecContext execContext;
    const auto* kernelNode = topology->Items.front().Node.get();
    arrow::compute::KernelContext kernelContext(&execContext);
    const arrow::compute::ExecBatch scalarBatch({arrow::Datum(scalarInput)}, 1);
    arrow::Datum scalarOutput;
    ARROW_OK(kernelNode->GetArrowKernel().exec(&kernelContext, scalarBatch, &scalarOutput));
    UNIT_ASSERT(scalarOutput.is_scalar());
    UNIT_ASSERT(scalarOutput.scalar()->type->id() == arrow::Type::BINARY);
    const auto& scalar = scalarOutput.scalar_as<arrow::BinaryScalar>();
    UNIT_ASSERT(scalar.is_valid);
    UNIT_ASSERT_VALUES_EQUAL(scalar.value->ToString(), "scalar");
    UNIT_ASSERT_EQUAL(scalar.value, scalarInput->value);

    // null scalar
    const arrow::compute::ExecBatch nullBatch({arrow::Datum(arrow::MakeNullScalar(arrow::utf8()))}, 1);
    arrow::Datum nullOutput;
    ARROW_OK(kernelNode->GetArrowKernel().exec(&kernelContext, nullBatch, &nullOutput));
    UNIT_ASSERT(nullOutput.is_scalar());
    UNIT_ASSERT(nullOutput.scalar()->type->id() == arrow::Type::BINARY);
    UNIT_ASSERT(!nullOutput.scalar()->is_valid);
}
} // Y_UNIT_TEST_SUITE(TMiniKQLConvertTest)

} // namespace NKikimr::NMiniKQL
