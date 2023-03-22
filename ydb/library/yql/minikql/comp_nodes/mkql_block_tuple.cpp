#include "mkql_block_tuple.h"
#include "mkql_block_impl.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

#include <arrow/util/bitmap_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockAsTupleExec {
public:
    TBlockAsTupleExec(const TVector<TType*>& argTypes, const std::shared_ptr<arrow::DataType>& returnArrowType)
        : ArgTypes(argTypes)
        , ReturnArrowType(returnArrowType)
    {}

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        bool allScalars = true;
        size_t length = 0;
        for (const auto& x : batch.values) {
            if (!x.is_scalar()) {
                allScalars = false;
                length = x.array()->length;
                break;
            }
        }

        if (allScalars) {
            // return scalar too
            std::vector<std::shared_ptr<arrow::Scalar>> arrowValue;
            for (const auto& x : batch.values) {
                arrowValue.emplace_back(x.scalar());
            }

            *res = arrow::Datum(std::make_shared<arrow::StructScalar>(arrowValue, ReturnArrowType));
            return arrow::Status::OK();
        }

        auto newArrayData = arrow::ArrayData::Make(ReturnArrowType, length, { nullptr }, 0, 0);
        MKQL_ENSURE(ArgTypes.size() == batch.values.size(), "Mismatch batch columns");
        for (ui32 i = 0; i < batch.values.size(); ++i) {
            const auto& datum = batch.values[i];
            if (datum.is_scalar()) {
                // expand scalar to array
                auto expandedArray = MakeArrayFromScalar(*datum.scalar(), length, AS_TYPE(TBlockType, ArgTypes[i])->GetItemType(), *ctx->memory_pool());
                newArrayData->child_data.push_back(expandedArray.array());
            } else {
                newArrayData->child_data.push_back(datum.array());
            }
        }

        *res = arrow::Datum(newArrayData);
        return arrow::Status::OK();
    }

private:
    const TVector<TType*> ArgTypes;
    const std::shared_ptr<arrow::DataType> ReturnArrowType;
};

class TBlockNthExec {
public:
    TBlockNthExec(const std::shared_ptr<arrow::DataType>& returnArrowType, ui32 index, bool isOptional, bool needExternalOptional)
        : ReturnArrowType(returnArrowType)
        , Index(index)
        , IsOptional(isOptional)
        , NeedExternalOptional(needExternalOptional)
    {}

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum inputDatum = batch.values[0];
        if (inputDatum.is_scalar()) {
            if (inputDatum.scalar()->is_valid) {
                const auto& structScalar = arrow::internal::checked_cast<const arrow::StructScalar&>(*inputDatum.scalar());
                *res = arrow::Datum(structScalar.value[Index]);
            } else {
                *res = arrow::Datum(arrow::MakeNullScalar(ReturnArrowType));
            }
        } else {
            const auto& array = inputDatum.array();
            auto child = array->child_data[Index];
            if (NeedExternalOptional) {
                auto newArrayData = arrow::ArrayData::Make(ReturnArrowType, array->length, { array->buffers[0] });
                newArrayData->child_data.push_back(child);
                *res = arrow::Datum(newArrayData);
            } else if (!IsOptional || !array->buffers[0]) {
                *res = arrow::Datum(child);
            } else {
                auto newArrayData = child->Copy();
                if (!newArrayData->buffers[0]) {
                    newArrayData->buffers[0] = array->buffers[0];
                } else {
                    auto buffer = AllocateBitmapWithReserve(array->length + array->offset, ctx->memory_pool());
                    arrow::internal::BitmapAnd(child->GetValues<uint8_t>(0, 0), child->offset, array->GetValues<uint8_t>(0, 0), array->offset, array->length, array->offset, buffer->mutable_data());
                    newArrayData->buffers[0] = buffer;
                }

                newArrayData->SetNullCount(arrow::kUnknownNullCount);
                *res = arrow::Datum(newArrayData);
            }
        }

        return arrow::Status::OK();
    }

private:
    const std::shared_ptr<arrow::DataType> ReturnArrowType;
    const ui32 Index;
    const bool IsOptional;
    const bool NeedExternalOptional;
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockAsTupleKernel(const TVector<TType*>& argTypes, TType* resultType) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TBlockAsTupleExec>(argTypes, returnArrowType);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockNthKernel(const TVector<TType*>& argTypes, TType* resultType, ui32 index,
    bool isOptional, bool needExternalOptional) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TBlockNthExec>(returnArrowType, index, isOptional, needExternalOptional);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockAsTuple(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    TVector<IComputationNode*> argsNodes;
    TVector<TType*> argsTypes;
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        argsNodes.push_back(LocateNode(ctx.NodeLocator, callable, i));
        argsTypes.push_back(callable.GetInput(i).GetStaticType());
    }

    auto kernel = MakeBlockAsTupleKernel(argsTypes, callable.GetType()->GetReturnType());
    return new TBlockFuncNode(ctx.Mutables, std::move(argsNodes), argsTypes, *kernel, kernel);
}

IComputationNode* WrapBlockNth(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2U, "Expected two args.");
    auto input = callable.GetInput(0U);
    auto blockType = AS_TYPE(TBlockType, input.GetStaticType());
    bool isOptional;
    auto tupleType = AS_TYPE(TTupleType, UnpackOptional(blockType->GetItemType(), isOptional));
    auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1U));
    auto index = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(index < tupleType->GetElementsCount(), "Bad tuple index");
    auto childType = tupleType->GetElementType(index);
    bool needExternalOptional = isOptional && childType->IsVariant();

    auto tuple = LocateNode(ctx.NodeLocator, callable, 0);

    TVector<IComputationNode*> argsNodes = { tuple };
    TVector<TType*> argsTypes = { blockType };
    auto kernel = MakeBlockNthKernel(argsTypes, callable.GetType()->GetReturnType(), index, isOptional, needExternalOptional);
    return new TBlockFuncNode(ctx.Mutables, std::move(argsNodes), argsTypes, *kernel, kernel);
}

}
}
