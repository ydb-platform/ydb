#include "mkql_block_coalesce.h"

#include <yql/essentials/minikql/arrow/arrow_defs.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_type_helper.h>
#include <yql/essentials/public/udf/arrow/block_builder.h>
#include <yql/essentials/public/udf/arrow/block_reader.h>
#include <yql/essentials/public/udf/arrow/util.h>
#include <yql/essentials/minikql/comp_nodes/mkql_block_coalesce_blending_helper.h>
#include <yql/essentials/minikql/defs.h>

#include <arrow/util/bitmap_ops.h>

namespace NKikimr::NMiniKQL {

namespace {

template <typename TType>
void DispatchCoalesceImpl(const arrow::Datum& left, const arrow::Datum& right, arrow::Datum& out, arrow::MemoryPool& pool) {
    bool outHasBitmask = (right.is_array() && right.null_count() > 0) || (right.is_scalar() && !right.scalar()->is_valid);
    auto bitmap = outHasBitmask ? ARROW_RESULT(arrow::AllocateBitmap((left.array()->length + left.array()->offset % 8) * sizeof(ui8), &pool)) : nullptr;
    if (bitmap && bitmap->size() > 0) {
        // Fill first byte with zero to prevent further uninitialized memory access.
        bitmap->mutable_data()[0] = 0;
    }
    out = arrow::ArrayData::Make(right.type(), left.array()->length,
                                 {std::move(bitmap),
                                  ARROW_RESULT(arrow::AllocateBuffer((left.array()->length + left.array()->offset % 8) * sizeof(TType), &pool))},
                                 arrow::kUnknownNullCount, left.array()->offset % 8);
    if (outHasBitmask) {
        if (right.is_scalar()) {
            out = left;
        } else {
            MKQL_ENSURE(TDatumStorageView<TType>(right).bitMask(), "Right array must have a null mask");
            BlendCoalesce<TType, /*isScalar=*/false, /*rightHasBitmask=*/true>(
                TDatumStorageView<TType>(left),
                TDatumStorageView<TType>(right),
                TDatumStorageView<TType>(out),
                left.array()->length);
        }
    } else {
        if (right.is_scalar()) {
            BlendCoalesce<TType, /*isScalar=*/true, /*rightHasBitmask=*/false>(
                TDatumStorageView<TType>(left),
                TDatumStorageView<TType>(right),
                TDatumStorageView<TType>(out),
                left.array()->length);
        } else {
            BlendCoalesce<TType, /*isScalar=*/false, /*rightHasBitmask=*/false>(
                TDatumStorageView<TType>(left),
                TDatumStorageView<TType>(right),
                TDatumStorageView<TType>(out),
                left.array()->length);
        }
    }
}

bool DispatchBlendingCoalesce(const arrow::Datum& left, const arrow::Datum& right, arrow::Datum& out, TType* rightType, bool needUnwrapFirst, arrow::MemoryPool& pool) {
    TTypeInfoHelper typeInfoHelper;
    if (!needUnwrapFirst) {
        bool rightTypeIsOptional;
        rightType = UnpackOptional(rightType, rightTypeIsOptional);
        MKQL_ENSURE(rightTypeIsOptional || rightType->IsPg(), "Right type must be optional or pg.");
    }
    NYql::NUdf::TDataTypeInspector typeData(typeInfoHelper, rightType);
    if (!typeData) {
        return false;
    }
    auto typeId = typeData.GetTypeId();

    switch (NYql::NUdf::GetDataSlot(typeId)) {
        case NYql::NUdf::EDataSlot::Bool:
        case NYql::NUdf::EDataSlot::Int8:
        case NYql::NUdf::EDataSlot::Uint8:
            DispatchCoalesceImpl<ui8>(left, right, out, pool);
            return true;
        case NYql::NUdf::EDataSlot::Int16:
        case NYql::NUdf::EDataSlot::Uint16:
        case NYql::NUdf::EDataSlot::Date:
            DispatchCoalesceImpl<ui16>(left, right, out, pool);
            return true;
        case NYql::NUdf::EDataSlot::Int32:
        case NYql::NUdf::EDataSlot::Uint32:
        case NYql::NUdf::EDataSlot::Date32:
        case NYql::NUdf::EDataSlot::Datetime:
            DispatchCoalesceImpl<ui32>(left, right, out, pool);
            return true;
        case NYql::NUdf::EDataSlot::Int64:
        case NYql::NUdf::EDataSlot::Uint64:
        case NYql::NUdf::EDataSlot::Datetime64:
        case NYql::NUdf::EDataSlot::Timestamp64:
        case NYql::NUdf::EDataSlot::Interval64:
        case NYql::NUdf::EDataSlot::Interval:
        case NYql::NUdf::EDataSlot::Timestamp:
            DispatchCoalesceImpl<ui64>(left, right, out, pool);
            return true;
        case NYql::NUdf::EDataSlot::Double:
            static_assert(sizeof(NUdf::TDataType<double>::TLayout) == sizeof(NUdf::TDataType<ui64>::TLayout));
            DispatchCoalesceImpl<ui64>(left, right, out, pool);
            return true;
        case NYql::NUdf::EDataSlot::Float:
            static_assert(sizeof(NUdf::TDataType<float>::TLayout) == sizeof(NUdf::TDataType<ui32>::TLayout));
            DispatchCoalesceImpl<ui32>(left, right, out, pool);
            return true;
        default:
            // Fallback to general builder/reader pipeline.
            return false;
    }
}

std::shared_ptr<arrow::Scalar> UnwrapScalar(std::shared_ptr<arrow::Scalar> scalar, bool firstScalarIsExternalOptional) {
    if (firstScalarIsExternalOptional) {
        return dynamic_cast<arrow::StructScalar&>(*scalar).value.at(0);
    }
    return scalar;
}

class TCoalesceBlockExec {
public:
    TCoalesceBlockExec(const std::shared_ptr<arrow::DataType>& returnArrowType, TType* firstItemType, TType* secondItemType, bool needUnwrapFirst)
        : ReturnArrowType_(returnArrowType)
        , FirstItemType_(firstItemType)
        , SecondItemType_(secondItemType)
        , NeedUnwrapFirst_(needUnwrapFirst)
        , FirstScalarIsExternalOptional_(NeedWrapWithExternalOptional(FirstItemType_))
    {
    }

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        const auto& first = batch.values[0];
        const auto& second = batch.values[1];

        if (first.is_scalar() && second.is_scalar()) {
            if (first.scalar()->is_valid) {
                *res = NeedUnwrapFirst_ ? UnwrapScalar(first.scalar(), FirstScalarIsExternalOptional_) : first.scalar();
            } else {
                *res = second.scalar();
            }
            return arrow::Status::OK();
        }

        MKQL_ENSURE(!first.is_scalar() || !second.is_scalar(), "Expected at least one array");
        size_t length = Max(first.length(), second.length());
        auto firstReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), FirstItemType_);
        auto secondReader = NYql::NUdf::MakeBlockReader(TTypeInfoHelper(), SecondItemType_);
        if (first.is_scalar()) {
            auto firstValue = firstReader->GetScalarItem(*first.scalar());
            if (firstValue) {
                auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondItemType_, *ctx->memory_pool(), length, /*pgBuilder=*/nullptr);
                builder->Add(NeedUnwrapFirst_ ? firstValue.GetOptionalValue() : firstValue, length);
                *res = builder->Build(true);
            } else {
                *res = second;
            }
        } else if (second.is_scalar()) {
            const auto& firstArray = *first.array();
            if (firstArray.GetNullCount() == 0) {
                *res = NeedUnwrapFirst_ ? Unwrap(firstArray, FirstItemType_) : first;
            } else if ((size_t)firstArray.GetNullCount() == length) {
                auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondItemType_, *ctx->memory_pool(), length, nullptr);
                auto secondValue = secondReader->GetScalarItem(*second.scalar());
                builder->Add(secondValue, length);
                *res = builder->Build(true);
            } else {
                if (DispatchBlendingCoalesce(first, second, *res, SecondItemType_, NeedUnwrapFirst_, *ctx->memory_pool())) {
                    return arrow::Status::OK();
                }

                auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondItemType_, *ctx->memory_pool(), length, nullptr);
                auto secondValue = secondReader->GetScalarItem(*second.scalar());
                for (size_t i = 0; i < length; ++i) {
                    auto firstItem = firstReader->GetItem(firstArray, i);
                    if (firstItem) {
                        builder->Add(NeedUnwrapFirst_ ? firstItem.GetOptionalValue() : firstItem);
                    } else {
                        builder->Add(secondValue);
                    }
                }

                *res = builder->Build(true);
            }
        } else {
            const auto& firstArray = *first.array();
            const auto& secondArray = *second.array();
            if (firstArray.GetNullCount() == 0) {
                *res = NeedUnwrapFirst_ ? Unwrap(firstArray, FirstItemType_) : first;
            } else if ((size_t)firstArray.GetNullCount() == length) {
                *res = second;
            } else {
                if (DispatchBlendingCoalesce(first, second, *res, SecondItemType_, NeedUnwrapFirst_, *ctx->memory_pool())) {
                    return arrow::Status::OK();
                }

                auto builder = NYql::NUdf::MakeArrayBuilder(TTypeInfoHelper(), SecondItemType_, *ctx->memory_pool(), length, nullptr);
                for (size_t i = 0; i < length; ++i) {
                    auto firstItem = firstReader->GetItem(firstArray, i);
                    if (firstItem) {
                        builder->Add(NeedUnwrapFirst_ ? firstItem.GetOptionalValue() : firstItem);
                    } else {
                        auto secondItem = secondReader->GetItem(secondArray, i);
                        builder->Add(secondItem);
                    }
                }

                *res = builder->Build(true);
            }
        }

        return arrow::Status::OK();
    }

private:
    const std::shared_ptr<arrow::DataType> ReturnArrowType_;
    TType* const FirstItemType_;
    TType* const SecondItemType_;
    const bool NeedUnwrapFirst_;
    const bool FirstScalarIsExternalOptional_;
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockCoalesceKernel(const TVector<TType*>& argTypes, TType* resultType, bool needUnwrapFirst) {
    using TExec = TCoalesceBlockExec;

    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TExec>(
        returnArrowType,
        AS_TYPE(TBlockType, argTypes[0])->GetItemType(),
        AS_TYPE(TBlockType, argTypes[1])->GetItemType(),
        needUnwrapFirst);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
                                                                 [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
                                                                     return exec->Exec(ctx, batch, res);
                                                                 });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockCoalesce(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    auto first = callable.GetInput(0);
    auto second = callable.GetInput(1);

    auto firstType = AS_TYPE(TBlockType, first.GetStaticType());
    auto secondType = AS_TYPE(TBlockType, second.GetStaticType());

    auto firstItemType = firstType->GetItemType();
    auto secondItemType = secondType->GetItemType();
    MKQL_ENSURE(firstItemType->IsOptional() || firstItemType->IsPg(), TStringBuilder() << "Expecting Optional or Pg type as first argument, but got: " << *firstItemType);

    bool needUnwrapFirst = false;
    if (!firstItemType->IsSameType(*secondItemType)) {
        // Here the left operand and right operand are of types T? and T respectively.
        // The first operand must be unwrapped to obtain the resulting type.
        needUnwrapFirst = true;
        bool firstOptional;
        firstItemType = UnpackOptional(firstItemType, firstOptional);
        MKQL_ENSURE(firstItemType->IsSameType(*secondItemType), "Uncompatible arguemnt types");
    }

    auto firstCompute = LocateNode(ctx.NodeLocator, callable, 0);
    auto secondCompute = LocateNode(ctx.NodeLocator, callable, 1);
    TComputationNodePtrVector argsNodes = {firstCompute, secondCompute};
    TVector<TType*> argsTypes = {firstType, secondType};

    auto kernel = MakeBlockCoalesceKernel(argsTypes, secondType, needUnwrapFirst);
    return new TBlockFuncNode(ctx.Mutables, ToDatumValidateMode(ctx.ValidateMode), "Coalesce", std::move(argsNodes), argsTypes, callable.GetType()->GetReturnType(), *kernel, kernel);
}

} // namespace NKikimr::NMiniKQL
