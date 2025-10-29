#include "mkql_block_getelem.h"

#include <yql/essentials/minikql/computation/mkql_block_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_type_helper.h>
#include <yql/essentials/public/udf/udf_type_printer.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

inline bool IsOptionalOrNull(const TType* type) {
    return type->IsOptional() || type->IsNull() || type->IsPg();
}

enum class EOptionalityHandlerStrategy {
    // Just return child as is.
    ReturnChildAsIs,
    // Return child and add optionality to it.
    AddOptionalToChild,
    // Return child but set mask to (tuple & child) intersection.
    IntersectOptionals,
    // Return null.
    ReturnNull,
};

// The strategy is based on tuple and its child optionality.
// Tuple<X> -> return child as is (ReturnChildAsIs).
// Tuple<X?> -> return child as is (ReturnChildAsIs).
// Tuple<X>? -> return child and add extra optional level (AddOptionalToChild).
// Tuple<X?>? -> return child as is BUT set mask to (tuple & child) intersection (IntersectOptionals).
// Tuple<Null>? -> return Null (ReturnNull).
EOptionalityHandlerStrategy GetStrategyBasedOnTupleType(TType* tupleType, TType* elementType) {
    if (!tupleType->IsOptional()) {
        return EOptionalityHandlerStrategy::ReturnChildAsIs;
    } else if (elementType->IsNull()) {
        return EOptionalityHandlerStrategy::ReturnNull;
    } else if (IsOptionalOrNull(elementType)) {
        return EOptionalityHandlerStrategy::IntersectOptionals;
    } else {
        return EOptionalityHandlerStrategy::AddOptionalToChild;
    }
}

std::shared_ptr<arrow::Buffer> CreateBitmapIntersection(arrow::compute::KernelContext* ctx,
                                                        std::shared_ptr<arrow::ArrayData> tuple,
                                                        size_t index,
                                                        bool preserveOffset) {
    auto child = tuple->child_data[index];
    auto resultBitmapOffset = preserveOffset ? child->offset : 0;
    if (!tuple->buffers[0]) {
        return child->buffers[0];
    } else if (!child->buffers[0]) {
        auto buffer = AllocateBitmapWithReserve(child->length + resultBitmapOffset, ctx->memory_pool());
        arrow::internal::CopyBitmap(tuple->GetValues<uint8_t>(0, 0), tuple->offset,
                                    child->length, buffer->mutable_data(), resultBitmapOffset);
        return buffer;
    } else {
        auto buffer = AllocateBitmapWithReserve(child->length + resultBitmapOffset, ctx->memory_pool());
        arrow::internal::BitmapAnd(child->GetValues<uint8_t>(0, 0), child->offset,
                                   tuple->GetValues<uint8_t>(0, 0), tuple->offset,
                                   child->length,
                                   resultBitmapOffset, buffer->mutable_data());
        return buffer;
    }
}

class TAddOptionalLevelHelper {
public:
    TAddOptionalLevelHelper(TType* returnType, std::shared_ptr<arrow::DataType> returnArrowType)
        : IsReturnExternalOptional_(NeedWrapWithExternalOptional(returnType))
        , ReturnArrowType_(returnArrowType)
    {
    }

    std::shared_ptr<arrow::ArrayData> AddOptionalToChild(std::shared_ptr<arrow::ArrayData> tuple, size_t index, arrow::compute::KernelContext* ctx) const {
        if (IsReturnExternalOptional_) {
            return arrow::ArrayData::Make(ReturnArrowType_, tuple->length, {tuple->buffers[0]}, {tuple->child_data[index]}, arrow::kUnknownNullCount, tuple->offset);
        } else {
            auto child = tuple->child_data[index];
            auto bitmask = MakeDenseBitmapCopyIfOffsetDiffers(tuple->buffers[0], child->length, tuple->offset, child->offset, ctx->memory_pool());
            return SetOptional(*child, bitmask, child->offset);
        }
    }

    std::shared_ptr<arrow::Scalar> AddOptionalToChildOfValidTuple(const arrow::StructScalar& tuple, size_t index) const {
        if (IsReturnExternalOptional_) {
            return std::make_shared<arrow::StructScalar>(std::vector<std::shared_ptr<arrow::Scalar>>{tuple.value[index]}, ReturnArrowType_);
        } else {
            return tuple.value[index];
        }
    }

    std::shared_ptr<arrow::Scalar> IntersectOptionalsOfValidTuple(const arrow::StructScalar& tuple, size_t index) const {
        auto child = tuple.value[index];
        return child;
    }

    std::shared_ptr<arrow::ArrayData> IntersectOptionals(std::shared_ptr<arrow::ArrayData> tuple, size_t index, arrow::compute::KernelContext* ctx) const {
        auto child = tuple->child_data[index];
        if (IsReturnExternalOptional_) {
            auto intersection = CreateBitmapIntersection(ctx, tuple, index, /*preserveOffset=*/false);
            return SetOptional(*child, intersection, 0);
        } else {
            auto intersection = CreateBitmapIntersection(ctx, tuple, index, /*preserveOffset=*/true);
            return SetOptional(*child, intersection, tuple->child_data[index]->offset);
        }
    }

    std::shared_ptr<arrow::ArrayData> SetOptional(const arrow::ArrayData& input, std::shared_ptr<arrow::Buffer> bitmask, size_t offset) const {
        auto result = input.Copy();
        result->buffers[0] = bitmask;
        result->offset = offset;
        result->SetNullCount(arrow::kUnknownNullCount);
        return result;
    }

    bool NeedAddExtraOptionalLevel() const {
        return IsReturnExternalOptional_;
    }

private:
    const bool IsReturnExternalOptional_;
    const std::shared_ptr<arrow::DataType> ReturnArrowType_;
};

class TBlockGetElementExec {
public:
    TBlockGetElementExec(const std::shared_ptr<arrow::DataType>& returnArrowType, ui32 index, EOptionalityHandlerStrategy resultStrategy, TAddOptionalLevelHelper addOptionalHelper)
        : ReturnArrowType(returnArrowType)
        , Index(index)
        , ResultStrategy(resultStrategy)
        , AddOptionalHelper(std::move(addOptionalHelper))
    {
    }

    arrow::Status ExecArray(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum inputDatum = batch.values[0];
        MKQL_ENSURE(inputDatum.is_array(), "Array expected.");

        const auto& tuple = inputDatum.array();
        auto child = tuple->child_data[Index];
        switch (ResultStrategy) {
            case EOptionalityHandlerStrategy::ReturnChildAsIs: {
                *res = arrow::Datum(child);
                return arrow::Status::OK();
            }
            case EOptionalityHandlerStrategy::AddOptionalToChild: {
                *res = arrow::Datum(AddOptionalHelper.AddOptionalToChild(tuple, Index, ctx));
                return arrow::Status::OK();
            }
            case EOptionalityHandlerStrategy::IntersectOptionals: {
                *res = arrow::Datum(AddOptionalHelper.IntersectOptionals(tuple, Index, ctx));
                return arrow::Status::OK();
            }
            case EOptionalityHandlerStrategy::ReturnNull:
                *res = NYql::NUdf::MakeSingularArray(/*isNull=*/true, tuple->length);
                return arrow::Status::OK();
        }

        return arrow::Status::OK();
    }

    arrow::Status ExecScalar(const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum inputDatum = batch.values[0];
        MKQL_ENSURE(inputDatum.is_scalar(), "Scalar expected.");

        if (!inputDatum.scalar()->is_valid) {
            *res = arrow::Datum(arrow::MakeNullScalar(ReturnArrowType));
            return arrow::Status::OK();
        }
        const auto& tuple = arrow::internal::checked_cast<const arrow::StructScalar&>(*inputDatum.scalar());

        switch (ResultStrategy) {
            case EOptionalityHandlerStrategy::ReturnChildAsIs: {
                *res = arrow::Datum(tuple.value[Index]);
                return arrow::Status::OK();
            }
            case EOptionalityHandlerStrategy::AddOptionalToChild: {
                *res = arrow::Datum(AddOptionalHelper.AddOptionalToChildOfValidTuple(tuple, Index));
                return arrow::Status::OK();
            }
            case EOptionalityHandlerStrategy::IntersectOptionals: {
                *res = arrow::Datum(AddOptionalHelper.IntersectOptionalsOfValidTuple(tuple, Index));
                return arrow::Status::OK();
            }
            case EOptionalityHandlerStrategy::ReturnNull:
                *res = NYql::NUdf::MakeSingularScalar(/*IsNull=*/true);
                return arrow::Status::OK();
        }

        return arrow::Status::OK();
    }

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        if (batch.values[0].is_array()) {
            return ExecArray(ctx, batch, res);
        } else {
            return ExecScalar(batch, res);
        }
    }

private:
    const std::shared_ptr<arrow::DataType> ReturnArrowType;
    const ui32 Index;
    EOptionalityHandlerStrategy ResultStrategy;
    TAddOptionalLevelHelper AddOptionalHelper;
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockGetElementKernel(const TVector<TType*>& argTypes, TType* resultType,
                                                                        ui32 index, EOptionalityHandlerStrategy resultStrategy, TAddOptionalLevelHelper addOptionalHelper) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TBlockGetElementExec>(returnArrowType, index, resultStrategy, std::move(addOptionalHelper));
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
                                                                 [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
                                                                     return exec->Exec(ctx, batch, res);
                                                                 });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

TType* GetElementType(const TStructType* structType, ui32 index) {
    MKQL_ENSURE(index < structType->GetMembersCount(), "Bad member index");
    return structType->GetMemberType(index);
}

TType* GetElementType(const TTupleType* tupleType, ui32 index) {
    MKQL_ENSURE(index < tupleType->GetElementsCount(), "Bad tuple index");
    return tupleType->GetElementType(index);
}

template <typename ObjectType>
IComputationNode* WrapBlockGetElement(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected two args.");
    auto inputObject = callable.GetInput(0);
    auto blockTupleType = AS_TYPE(TBlockType, inputObject.GetStaticType());
    bool isOptional;
    auto objectType = AS_TYPE(ObjectType, UnpackOptional(blockTupleType->GetItemType(), isOptional));
    auto index = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    auto tupleElementType = GetElementType(objectType, index);
    EOptionalityHandlerStrategy strategy = GetStrategyBasedOnTupleType(blockTupleType->GetItemType(), tupleElementType);

    auto* returnType = AS_TYPE(TBlockType, callable.GetType()->GetReturnType());
    std::shared_ptr<arrow::DataType> returnArrowType;

    MKQL_ENSURE(ConvertArrowType(returnType->GetItemType(), returnArrowType), "Unsupported arrow type");
    TAddOptionalLevelHelper addOptionalHelper(returnType->GetItemType(), returnArrowType);

    auto objectNode = LocateNode(ctx.NodeLocator, callable, 0);

    TComputationNodePtrVector argsNodes = {objectNode};
    TVector<TType*> argsTypes = {blockTupleType};
    auto kernel = MakeBlockGetElementKernel(argsTypes, returnType, index, strategy, std::move(addOptionalHelper));
    return new TBlockFuncNode(ctx.Mutables, ToDatumValidateMode(ctx.ValidateMode), callable.GetType()->GetName(), std::move(argsNodes), argsTypes, callable.GetType()->GetReturnType(), *kernel, kernel);
}

} // namespace

IComputationNode* WrapBlockMember(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockGetElement<TStructType>(callable, ctx);
}

IComputationNode* WrapBlockNth(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    return WrapBlockGetElement<TTupleType>(callable, ctx);
}

} // namespace NMiniKQL
} // namespace NKikimr
