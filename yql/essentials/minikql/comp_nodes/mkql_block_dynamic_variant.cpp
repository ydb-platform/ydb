#include "mkql_block_dynamic_variant.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/utils/runtime_dispatch.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/dense_union.h>
#include <yql/essentials/public/udf/arrow/dense_union_scalar.h>

#include <util/generic/maybe.h>

#include <ranges>
#include <type_traits>

namespace NKikimr::NMiniKQL {

namespace {

THashMap<TStringBuf, ui32> MakeAlternativeFields(const TVariantType& variantType) {
    THashMap<TStringBuf, ui32> fields;
    auto structType = AS_TYPE(TStructType, variantType.GetUnderlyingType());
    for (ui32 i = 0; i < structType->GetMembersCount(); ++i) {
        fields[structType->GetMemberName(i)] = i;
    }
    return fields;
}

template <bool IsTuple, bool IsIndexOptional>
class TDynamicVariantBlockExec {
    using TIndexStringReader = NYql::NUdf::TStringBlockReader<arrow::StringType, IsIndexOptional>;

    using TIndexToAlternative = std::conditional_t<
        IsTuple,
        decltype(std::views::iota(ui32{0}, ui32{0})),
        THashMap<TStringBuf, ui32>>;

public:
    class TKernelState: public arrow::compute::KernelState {
    public:
        explicit TKernelState(TType* payloadItemType)
            : PayloadReader_(NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), payloadItemType))
        {
        }

        NYql::NUdf::IBlockReader& GetPayloadReader() {
            return *PayloadReader_;
        }

        TIndexStringReader& GetIndexReader() {
            return IndexReader_;
        }

    private:
        std::unique_ptr<NYql::NUdf::IBlockReader> PayloadReader_;
        TIndexStringReader IndexReader_;
    };

    TDynamicVariantBlockExec(TType* resultItemType, std::shared_ptr<arrow::DataType> resultArrowType,
                             TIndexToAlternative indexToAlternative)
        : ResultItemType_(resultItemType)
        , ResultArrowType_(std::move(resultArrowType))
        , InnerUnionArrowType_(ResultArrowType_->field(0)->type())
        , IndexToAlternative_(std::move(indexToAlternative))
    {
    }

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        auto& state = static_cast<TKernelState&>(*ctx->state());
        const arrow::Datum& payloadDatum = batch.values[0];
        const arrow::Datum& indexDatum = batch.values[1];

        if (payloadDatum.is_scalar() && indexDatum.is_scalar()) {
            *res = arrow::Datum(ExecScalarScalar(state, payloadDatum.scalar(), *indexDatum.scalar()));
            return arrow::Status::OK();
        }
        *res = ExecToArray(ctx, state, payloadDatum, indexDatum);
        return arrow::Status::OK();
    }

private:
    arrow::Datum ExecToArray(arrow::compute::KernelContext* ctx, TKernelState& state, const arrow::Datum& payloadDatum,
                             const arrow::Datum& indexDatum) const {
        auto& payloadReader = state.GetPayloadReader();
        const bool payloadIsScalar = payloadDatum.is_scalar();
        const bool indexIsScalar = indexDatum.is_scalar();
        const i64 length = payloadIsScalar ? indexDatum.array()->length : payloadDatum.array()->length;

        // A reader item stays valid until that same reader's next GetItem/GetScalarItem call. The payload
        // and index readers are distinct, and on the scalar-payload path the payload reader is never called
        // again, so the item is read once and reused for every row.
        NYql::NUdf::TBlockItem scalarPayloadItem;
        if (payloadIsScalar) {
            scalarPayloadItem = payloadReader.GetScalarItem(*payloadDatum.scalar());
        }
        TMaybe<ui32> scalarAlternative;
        if (indexIsScalar) {
            scalarAlternative = ResolveScalarAlternative(state, *indexDatum.scalar());
        }

        auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), ResultItemType_, *ctx->memory_pool(),
                                                    static_cast<size_t>(length), /*pgBuilder=*/nullptr);
        for (i64 row = 0; row < length; ++row) {
            const TMaybe<ui32> alternative = indexIsScalar ? scalarAlternative : ResolveArrayAlternative(state, *indexDatum.array(), row);
            if (!alternative) {
                builder->Add(NYql::NUdf::TBlockItem{});
                continue;
            }
            // An array payload item is consumed by Add before the payload reader's next GetItem call.
            NYql::NUdf::TBlockItem payloadItem = payloadIsScalar
                                                     ? scalarPayloadItem
                                                     : payloadReader.GetItem(*payloadDatum.array(), static_cast<size_t>(row));
            builder->Add(NYql::NUdf::TBlockItem(*alternative, &payloadItem));
        }
        return builder->Build(/*finish=*/true);
    }

    TMaybe<ui32> ResolveArrayAlternative(TKernelState& state, const arrow::ArrayData& indexArray, i64 row) const {
        if constexpr (IsTuple) {
            if constexpr (IsIndexOptional) {
                if (NYql::NUdf::IsNull(indexArray, row)) {
                    return Nothing();
                }
            }
            const ui32 index = indexArray.GetValues<ui32>(1)[row];
            return index < IndexToAlternative_.size() ? TMaybe<ui32>(IndexToAlternative_[index]) : Nothing();
        } else {
            return ResolveMemberString(state.GetIndexReader().GetItem(indexArray, row));
        }
    }

    TMaybe<ui32> ResolveScalarAlternative(TKernelState& state, const arrow::Scalar& indexScalar) const {
        if constexpr (IsTuple) {
            if constexpr (IsIndexOptional) {
                if (!indexScalar.is_valid) {
                    return Nothing();
                }
            }
            const ui32 index = arrow::internal::checked_cast<const arrow::UInt32Scalar&>(indexScalar).value;
            return index < IndexToAlternative_.size() ? TMaybe<ui32>(IndexToAlternative_[index]) : Nothing();
        } else {
            return ResolveMemberString(state.GetIndexReader().GetScalarItem(indexScalar));
        }
    }

    TMaybe<ui32> ResolveMemberString(const NYql::NUdf::TBlockItem& indexItem) const
        requires(!IsTuple)
    {
        if constexpr (IsIndexOptional) {
            if (!indexItem) {
                return Nothing();
            }
        }
        auto* memberIndex = IndexToAlternative_.FindPtr(indexItem.AsStringRef());
        return memberIndex ? TMaybe<ui32>(*memberIndex) : Nothing();
    }

    std::shared_ptr<arrow::Scalar> ExecScalarScalar(TKernelState& state, const std::shared_ptr<arrow::Scalar>& payloadScalar,
                                                    const arrow::Scalar& indexScalar) const {
        const TMaybe<ui32> alternative = ResolveScalarAlternative(state, indexScalar);
        if (!alternative) {
            return arrow::MakeNullScalar(ResultArrowType_);
        }
        auto denseUnionScalar = std::make_shared<NYql::NUdf::TDenseUnionScalar>(payloadScalar, *alternative, InnerUnionArrowType_);
        return NYql::NUdf::CreateOptionalUnionScalar(std::move(denseUnionScalar), ResultArrowType_);
    }

    TType* const ResultItemType_;
    const std::shared_ptr<arrow::DataType> ResultArrowType_;
    const std::shared_ptr<arrow::DataType> InnerUnionArrowType_;
    const TIndexToAlternative IndexToAlternative_;
};

template <bool IsTuple, bool IsIndexOptional>
std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockDynamicVariantKernel(const TVector<TType*>& argTypes, TType* resultType,
                                                                            const TVariantType& variantType) {
    TType* resultItemType = AS_TYPE(TBlockType, resultType)->GetItemType();
    std::shared_ptr<arrow::DataType> resultArrowType;
    MKQL_ENSURE(ConvertArrowType(resultItemType, resultArrowType), "Unsupported arrow type");

    TType* payloadItemType = AS_TYPE(TBlockType, argTypes[0])->GetItemType();

    using TExec = TDynamicVariantBlockExec<IsTuple, IsIndexOptional>;
    auto makeIndexToAlternative = [&]() {
        if constexpr (IsTuple) {
            return std::views::iota(ui32{0}, variantType.GetAlternativesCount());
        } else {
            return MakeAlternativeFields(variantType);
        }
    };
    auto exec = std::make_shared<TExec>(resultItemType, std::move(resultArrowType), makeIndexToAlternative());

    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(
        ConvertToInputTypes(argTypes),
        ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
            return exec->Exec(ctx, batch, res);
        });
    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    kernel->mem_allocation = arrow::compute::MemAllocation::NO_PREALLOCATE;
    kernel->init = [payloadItemType](arrow::compute::KernelContext*, const arrow::compute::KernelInitArgs&) {
        return arrow::Result(std::make_unique<typename TExec::TKernelState>(payloadItemType));
    };
    return kernel;
}

} // namespace

IComputationNode* WrapBlockDynamicVariant(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 arguments");

    auto payloadBlockType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    auto indexBlockType = AS_TYPE(TBlockType, callable.GetInput(1).GetStaticType());

    auto variantTypeNode = callable.GetInput(2);
    MKQL_ENSURE(variantTypeNode.IsImmediate() && variantTypeNode.GetStaticType()->IsType(), "Expected immediate type");
    auto variantType = AS_TYPE(TVariantType, static_cast<TType*>(variantTypeNode.GetNode()));

    auto resultType = callable.GetType()->GetReturnType();

    auto payloadCompute = LocateNode(ctx.NodeLocator, callable, 0);
    auto indexCompute = LocateNode(ctx.NodeLocator, callable, 1);
    TComputationNodePtrVector argsNodes = {payloadCompute, indexCompute};
    TVector<TType*> argsTypes = {payloadBlockType, indexBlockType};

    const bool isTuple = variantType->GetUnderlyingType()->IsTuple();
    const bool isIndexOptional = indexBlockType->GetItemType()->IsOptional();
    auto kernel = YQL_RUNTIME_DISPATCH(MakeBlockDynamicVariantKernel, 2, isTuple, isIndexOptional, argsTypes, resultType, *variantType);

    return new TBlockFuncNode(ctx.Mutables, ctx.RuntimeSettings->DatumValidation.Get(),
                              callable.GetType()->GetName(), std::move(argsNodes), argsTypes, resultType, *kernel, kernel);
}

} // namespace NKikimr::NMiniKQL
