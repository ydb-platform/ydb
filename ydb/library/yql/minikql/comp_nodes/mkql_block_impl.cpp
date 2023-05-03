#include "mkql_block_impl.h"
#include "mkql_block_builder.h"
#include "mkql_block_reader.h"

#include <ydb/library/yql/minikql/arrow/mkql_functions.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/public/udf/arrow/args_dechunker.h>

#include <arrow/compute/exec_internal.h>

namespace NKikimr::NMiniKQL {

namespace {

std::vector<arrow::ValueDescr> ToValueDescr(const TVector<TType*>& types) {
    std::vector<arrow::ValueDescr> res;
    res.reserve(types.size());
    for (const auto& type : types) {
        res.emplace_back(ToValueDescr(type));
    }

    return res;
}

} // namespace

arrow::Datum MakeArrayFromScalar(const arrow::Scalar& scalar, size_t len, TType* type, arrow::MemoryPool& pool) {
    MKQL_ENSURE(len > 0, "Invalid block size");
    auto reader = MakeBlockReader(TTypeInfoHelper(), type);
    auto builder = MakeArrayBuilder(TTypeInfoHelper(), type, pool, len, nullptr);

    auto scalarItem = reader->GetScalarItem(scalar);
    for (size_t i = 0; i < len; ++i) {
        builder->Add(scalarItem);
    }

    return builder->Build(true);
}

arrow::ValueDescr ToValueDescr(TType* type) {
    arrow::ValueDescr ret;
    MKQL_ENSURE(ConvertInputArrowType(type, ret), "can't get arrow type");
    return ret;
}

std::vector<arrow::compute::InputType> ConvertToInputTypes(const TVector<TType*>& argTypes) {
    std::vector<arrow::compute::InputType> result;
    result.reserve(argTypes.size());
    for (auto& type : argTypes) {
        result.emplace_back(ToValueDescr(type));
    }
    return result;
}

arrow::compute::OutputType ConvertToOutputType(TType* output) {
    return arrow::compute::OutputType(ToValueDescr(output));
}

TBlockFuncNode::TBlockFuncNode(TComputationMutables& mutables, TVector<IComputationNode*>&& argsNodes,
    const TVector<TType*>& argsTypes, const arrow::compute::ScalarKernel& kernel,
    std::shared_ptr<arrow::compute::ScalarKernel> kernelHolder,
    const arrow::compute::FunctionOptions* functionOptions)
    : TMutableComputationNode(mutables)
    , StateIndex(mutables.CurValueIndex++)
    , ArgsNodes(std::move(argsNodes))
    , ArgsValuesDescr(ToValueDescr(argsTypes))
    , Kernel(kernel)
    , KernelHolder(std::move(kernelHolder))
    , Options(functionOptions)
    , ScalarOutput(GetResultShape(argsTypes) == TBlockType::EShape::Scalar)
{
}

NUdf::TUnboxedValuePod TBlockFuncNode::DoCalculate(TComputationContext& ctx) const {
    auto& state = GetState(ctx);

    std::vector<arrow::Datum> argDatums;
    for (ui32 i = 0; i < ArgsNodes.size(); ++i) {
        argDatums.emplace_back(TArrowBlock::From(ArgsNodes[i]->GetValue(ctx)).GetDatum());
        Y_VERIFY_DEBUG(ArgsValuesDescr[i] == argDatums.back().descr());
    }

    auto executor = arrow::compute::detail::KernelExecutor::MakeScalar();
    ARROW_OK(executor->Init(&state.KernelContext, { &Kernel, ArgsValuesDescr, Options }));

    if (ScalarOutput) {
        auto listener = std::make_shared<arrow::compute::detail::DatumAccumulator>();
        ARROW_OK(executor->Execute(argDatums, listener.get()));
        auto output = executor->WrapResults(argDatums, listener->values());
        return ctx.HolderFactory.CreateArrowBlock(std::move(output));
    }

    NYql::NUdf::TArgsDechunker dechunker(std::move(argDatums));
    std::vector<arrow::Datum> chunk;
    TVector<std::shared_ptr<arrow::ArrayData>> arrays;

    while (dechunker.Next(chunk)) {
        arrow::compute::detail::DatumAccumulator listener;
        ARROW_OK(executor->Execute(chunk, &listener));
        auto output = executor->WrapResults(chunk, listener.values());

        ForEachArrayData(output, [&](const auto& arr) { arrays.push_back(arr); });
    }

    return ctx.HolderFactory.CreateArrowBlock(MakeArray(arrays));
}


void TBlockFuncNode::RegisterDependencies() const {
    for (const auto& arg : ArgsNodes) {
        DependsOn(arg);
    }
}

TBlockFuncNode::TState& TBlockFuncNode::GetState(TComputationContext& ctx) const {
    auto& result = ctx.MutableValues[StateIndex];
    if (!result.HasValue()) {
        result = ctx.HolderFactory.Create<TState>(Options, Kernel, ArgsValuesDescr, ctx);
    }

    return *static_cast<TState*>(result.AsBoxed().Get());
}

}
