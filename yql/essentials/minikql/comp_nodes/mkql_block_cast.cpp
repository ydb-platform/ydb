#include "mkql_block_cast.h"

#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>

#include <arrow/compute/cast.h>

namespace NKikimr::NMiniKQL {

namespace {

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockCastKernel(const TVector<TType*>& argTypes, TType* resultType, bool safe) {
    std::shared_ptr<arrow::DataType> targetArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), targetArrowType), "Unsupported Arrow type");

    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(
        ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [targetArrowType, safe](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
            arrow::compute::ExecContext execContext(ctx->memory_pool());
            auto result = arrow::compute::Cast(batch.values[0], targetArrowType, arrow::compute::CastOptions(safe), &execContext);
            if (!result.ok()) {
                return result.status();
            }
            *res = std::move(result).ValueUnsafe();
            return arrow::Status::OK();
        });
    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel->mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    return kernel;
}

}

IComputationNode* WrapBlockCast(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arguments");

    const auto dataType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    const auto resultType = AS_TYPE(TBlockType, callable.GetType()->GetReturnType());
    MKQL_ENSURE(dataType->GetShape() == resultType->GetShape(), "Expected matching block shapes");
    const auto safeNode = callable.GetInput(1);
    MKQL_ENSURE(AS_TYPE(TDataType, safeNode.GetStaticType())->GetSchemeType() == NUdf::TDataType<bool>::Id,
        "Expected bool as second argument");
    const auto safe = AS_VALUE(TDataLiteral, safeNode)->AsValue().Get<bool>();

    TComputationNodePtrVector argsNodes = {LocateNode(ctx.NodeLocator, callable, 0)};
    TVector<TType*> argTypes = {dataType};
    auto kernel = MakeBlockCastKernel(argTypes, resultType, safe);
    return new TBlockFuncNode(ctx.Mutables, ctx.RuntimeSettings->DatumValidation.Get(), callable.GetType()->GetName(),
        std::move(argsNodes), argTypes, resultType, *kernel, kernel);
}

} // namespace NKikimr::NMiniKQL
