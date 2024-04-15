#include "mkql_block_getelem.h"

#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockGetElementExec {
public:
    TBlockGetElementExec(const std::shared_ptr<arrow::DataType>& returnArrowType, ui32 index, bool isOptional, bool needExternalOptional)
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

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockGetElementKernel(const TVector<TType*>& argTypes, TType* resultType,
    ui32 index, bool isOptional, bool needExternalOptional) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TBlockGetElementExec>(returnArrowType, index, isOptional, needExternalOptional);
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

template<typename ObjectType>
IComputationNode* WrapBlockGetElement(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected two args.");
    auto inputObject = callable.GetInput(0);
    auto blockType = AS_TYPE(TBlockType, inputObject.GetStaticType());
    bool isOptional;
    auto objectType = AS_TYPE(ObjectType, UnpackOptional(blockType->GetItemType(), isOptional));
    auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1));
    auto index = indexData->AsValue().Get<ui32>();
    auto childType = GetElementType(objectType, index);
    bool needExternalOptional = isOptional && childType->IsVariant();

    auto objectNode = LocateNode(ctx.NodeLocator, callable, 0);

    TComputationNodePtrVector argsNodes = { objectNode };
    TVector<TType*> argsTypes = { blockType };
    auto kernel = MakeBlockGetElementKernel(argsTypes, callable.GetType()->GetReturnType(), index, isOptional, needExternalOptional);
    return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), std::move(argsNodes), argsTypes, *kernel, kernel);
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
