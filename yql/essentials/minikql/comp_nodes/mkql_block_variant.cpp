#include "mkql_block_variant.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/arrow/dense_union_scalar.h>
#include <yql/essentials/public/udf/arrow/util.h>

#include <numeric>

namespace NKikimr::NMiniKQL {

namespace {

class TVariantBlockExec {
public:
    TVariantBlockExec(ui32 alternativeIndex, std::shared_ptr<arrow::DataType> resultArrowType,
                      TVector<std::shared_ptr<arrow::DataType>> alternativeArrowTypes)
        : AlternativeIndex_(alternativeIndex)
        , ResultArrowType_(std::move(resultArrowType))
        , AlternativeArrowTypes_(std::move(alternativeArrowTypes))
    {
    }

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        const arrow::Datum& payloadDatum = batch.values[0];

        if (payloadDatum.is_scalar()) {
            *res = arrow::Datum(std::make_shared<NYql::NUdf::TDenseUnionScalar>(payloadDatum.scalar(), AlternativeIndex_, ResultArrowType_));
            return arrow::Status::OK();
        }

        MKQL_ENSURE(payloadDatum.is_array(), "Expected array datum");
        *res = AttachAsVariantChild(payloadDatum.array(), ctx->memory_pool());
        return arrow::Status::OK();
    }

private:
    std::shared_ptr<arrow::ArrayData> AttachAsVariantChild(const std::shared_ptr<arrow::ArrayData>& payload, arrow::MemoryPool* pool) const {
        const i64 length = payload->length;

        auto typeCodes = ARROW_RESULT(arrow::AllocateBuffer(length, pool));
        std::fill_n(reinterpret_cast<i8*>(typeCodes->mutable_data()), length, static_cast<i8>(AlternativeIndex_));

        auto valueOffsets = ARROW_RESULT(arrow::AllocateBuffer(length * static_cast<i64>(sizeof(i32)), pool));
        auto* offsets = reinterpret_cast<i32*>(valueOffsets->mutable_data());
        std::iota(offsets, offsets + length, 0);

        std::vector<std::shared_ptr<arrow::ArrayData>> children(AlternativeArrowTypes_.size());
        for (size_t i = 0; i < children.size(); ++i) {
            children[i] = (i == AlternativeIndex_) ? payload : NYql::NUdf::MakeEmptyArray(AlternativeArrowTypes_[i], pool);
        }

        return arrow::ArrayData::Make(ResultArrowType_, length,
                                      {nullptr, std::move(typeCodes), std::move(valueOffsets)},
                                      std::move(children), /*null_count=*/0, /*offset=*/0);
    }

    const ui32 AlternativeIndex_;
    const std::shared_ptr<arrow::DataType> ResultArrowType_;
    const TVector<std::shared_ptr<arrow::DataType>> AlternativeArrowTypes_;
};

TVector<std::shared_ptr<arrow::DataType>> ConvertAlternativeArrowTypes(const TVariantType& variantType) {
    TVector<std::shared_ptr<arrow::DataType>> alternativeArrowTypes(variantType.GetAlternativesCount());
    for (ui32 i = 0; i < alternativeArrowTypes.size(); ++i) {
        MKQL_ENSURE(ConvertArrowType(variantType.GetAlternativeType(i), alternativeArrowTypes[i]), "Unsupported arrow type");
    }
    return alternativeArrowTypes;
}

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockVariantKernel(const TVector<TType*>& argTypes,
                                                                     TType* resultType,
                                                                     ui32 alternativeIndex,
                                                                     const TVariantType& variantType) {
    TType* resultItemType = AS_TYPE(TBlockType, resultType)->GetItemType();
    std::shared_ptr<arrow::DataType> resultArrowType;
    MKQL_ENSURE(ConvertArrowType(resultItemType, resultArrowType), "Unsupported arrow type");

    auto exec = std::make_shared<TVariantBlockExec>(
        alternativeIndex, std::move(resultArrowType), ConvertAlternativeArrowTypes(variantType));
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(
        ConvertToInputTypes(argTypes),
        ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
            return exec->Exec(ctx, batch, res);
        });
    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel->mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockVariant(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arguments");

    auto payloadBlockType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());

    const ui32 alternativeIndex = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();

    auto resultType = callable.GetType()->GetReturnType();
    auto variantItemType = AS_TYPE(TBlockType, resultType)->GetItemType();
    auto variantType = AS_TYPE(TVariantType, variantItemType);
    MKQL_ENSURE(alternativeIndex < variantType->GetAlternativesCount(), "Bad alternative index");

    auto payloadCompute = LocateNode(ctx.NodeLocator, callable, 0);
    TComputationNodePtrVector argsNodes = {payloadCompute};
    TVector<TType*> argsTypes = {payloadBlockType};

    auto kernel = MakeBlockVariantKernel(argsTypes, resultType, alternativeIndex, *variantType);

    return new TBlockFuncNode(ctx.Mutables, ctx.RuntimeSettings->DatumValidation.Get(),
                              callable.GetType()->GetName(), std::move(argsNodes), argsTypes, resultType, *kernel, kernel);
}

} // namespace NKikimr::NMiniKQL
