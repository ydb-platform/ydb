#include "mkql_block_guess.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_block_reader.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsOptional>
class TGuessBlockExec {
public:
    class TGuessKernelState: public arrow::compute::KernelState {
    public:
        explicit TGuessKernelState(TType* inputItemType)
            : Reader_(MakeBlockReader(TTypeInfoHelper(), inputItemType))
        {
        }

        IBlockReader& GetReader() {
            return *Reader_;
        }

    private:
        std::unique_ptr<IBlockReader> Reader_;
    };

    explicit TGuessBlockExec(TType* inputItemType, ui32 alternativeIndex, TType* resultItemType)
        : InputItemType_(inputItemType)
        , AlternativeIndex_(alternativeIndex)
        , ResultItemType_(resultItemType)
    {
    }

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        auto& reader = static_cast<TGuessKernelState&>(*ctx->state()).GetReader();
        const arrow::Datum& variantDatum = batch.values[0];

        if (variantDatum.is_scalar()) {
            *res = ConvertScalar(ResultItemType_,
                                 ComputeOutputItem(reader.GetScalarItem(*variantDatum.scalar())),
                                 *ctx->memory_pool());
            return arrow::Status::OK();
        }

        MKQL_ENSURE(variantDatum.is_array(), "Expected array datum");
        const auto& variantArrayData = variantDatum.array();
        const size_t length = static_cast<size_t>(variantArrayData->length);
        auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), ResultItemType_, *ctx->memory_pool(), length, /*pgBuilder=*/nullptr);
        for (size_t i = 0; i < length; ++i) {
            builder->Add(ComputeOutputItem(reader.GetItem(*variantArrayData, i)));
        }
        *res = builder->Build(/*finish=*/true);
        return arrow::Status::OK();
    }

private:
    TBlockItem ComputeOutputItem(TBlockItem blockItem) const {
        if constexpr (IsOptional) {
            if (!blockItem) {
                return TBlockItem{};
            }
        }
        return blockItem.GetVariantIndex() == AlternativeIndex_
                   ? blockItem.GetVariantItem().MakeOptional()
                   : TBlockItem{};
    }

    TType* const InputItemType_;
    const ui32 AlternativeIndex_;
    TType* const ResultItemType_;
};

template <bool IsOptional>
std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockGuessKernel(const TVector<TType*>& argTypes,
                                                                   TType* resultType,
                                                                   TType* inputItemType,
                                                                   ui32 alternativeIndex) {
    using TExec = TGuessBlockExec<IsOptional>;
    auto exec = std::make_shared<TExec>(
        inputItemType,
        alternativeIndex,
        AS_TYPE(TBlockType, resultType)->GetItemType());
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(
        ConvertToInputTypes(argTypes),
        ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
            return exec->Exec(ctx, batch, res);
        });
    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel->mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    kernel->init = [inputItemType](arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs&) {
        return arrow::Result(std::make_unique<typename TExec::TGuessKernelState>(inputItemType));
    };
    return kernel;
}

} // namespace

IComputationNode* WrapBlockGuess(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arguments");

    auto blockType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    auto inputItemType = blockType->GetItemType();

    bool isOptional;
    auto variantItemType = UnpackOptional(inputItemType, isOptional);
    auto variantType = AS_TYPE(TVariantType, variantItemType);

    const ui32 alternativeIndex = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    MKQL_ENSURE(alternativeIndex < variantType->GetAlternativesCount(), "Bad alternative index");

    auto variantCompute = LocateNode(ctx.NodeLocator, callable, 0);
    TComputationNodePtrVector argsNodes = {variantCompute};
    TVector<TType*> argsTypes = {blockType};

    auto resultType = callable.GetType()->GetReturnType();

    auto kernel = isOptional
                      ? MakeBlockGuessKernel<true>(argsTypes, resultType, inputItemType, alternativeIndex)
                      : MakeBlockGuessKernel<false>(argsTypes, resultType, inputItemType, alternativeIndex);

    return new TBlockFuncNode(ctx.Mutables, ctx.RuntimeSettings->DatumValidation.Get(),
                              callable.GetType()->GetName(), std::move(argsNodes), argsTypes, resultType, *kernel, kernel);
}

} // namespace NMiniKQL
} // namespace NKikimr
