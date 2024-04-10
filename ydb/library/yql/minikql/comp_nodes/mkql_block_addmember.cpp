#include "mkql_block_addmember.h"

#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockAddMemberExec {
public:
    TBlockAddMemberExec(const std::shared_ptr<arrow::DataType>& returnArrowType, ui32 index,
        const TType* structType, const TType* memberType)
        : ReturnArrowType(returnArrowType)
        , StructType(structType)
        , MemberType(memberType)
        , Index(index)
    {}

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum structDatum = batch.values[0];
        arrow::Datum valueDatum = batch.values[1];
        if (structDatum.is_scalar() && valueDatum.is_scalar()) {
            const auto& obj = arrow::internal::checked_cast<const arrow::StructScalar&>(*structDatum.scalar());

            std::vector<std::shared_ptr<arrow::Scalar>> arrowValue;
            for (ui32 i = 0; i < Index; i++) {
                arrowValue.push_back(obj.value[i]);
            }

            arrowValue.push_back(valueDatum.scalar());

            for (ui32 i = Index; i < obj.value.size(); i++) {
                arrowValue.push_back(obj.value[i]);
            }

            *res = arrow::Datum(std::make_shared<arrow::StructScalar>(arrowValue, ReturnArrowType));
        } else if (structDatum.is_scalar()) {
            const auto& obj = arrow::internal::checked_cast<const arrow::StructScalar&>(*structDatum.scalar());
            const size_t blockSize = valueDatum.array()->length;
            auto resArrayData = arrow::ArrayData::Make(ReturnArrowType, blockSize, { std::shared_ptr<arrow::Buffer>{} });

            for (ui32 i = 0; i < Index; i++) {
                const TType* memberType = AS_TYPE(TStructType, StructType)->GetMemberType(i);
                const auto value = MakeArrayFromScalar(*obj.value[i], blockSize, memberType, *ctx->memory_pool());
                resArrayData->child_data.push_back(value.array());
            }

            resArrayData->child_data.push_back(valueDatum.array());

            for (ui32 i = Index; i < obj.value.size(); i++) {
                const TType* memberType = AS_TYPE(TStructType, StructType)->GetMemberType(i);
                const auto value = MakeArrayFromScalar(*obj.value[i], blockSize, memberType, *ctx->memory_pool());
                resArrayData->child_data.push_back(value.array());
            }

            *res = arrow::Datum(resArrayData);
        } else {
            const auto& arr = *structDatum.array();
            auto resArrayData = arrow::ArrayData::Make(ReturnArrowType, arr.length, { std::shared_ptr<arrow::Buffer>{} });

            for (ui32 i = 0; i < Index; i++) {
                resArrayData->child_data.push_back(arr.child_data[i]);
            }

            if (valueDatum.is_scalar()) {
                const auto& vobj = *valueDatum.scalar();
                const auto value = MakeArrayFromScalar(vobj, arr.length, MemberType, *ctx->memory_pool());
                resArrayData->child_data.push_back(value.array());
            } else {
                const auto& varr = *valueDatum.array();
                MKQL_ENSURE(varr.length == arr.length, "Datum arrays must be the same size");
                resArrayData->child_data.push_back(varr.child_data[0]);
            }

            for (ui32 i = Index; i < arr.child_data.size(); i++) {
                resArrayData->child_data.push_back(arr.child_data[i]);
            }

            *res = arrow::Datum(resArrayData);
        }
        return arrow::Status::OK();
    }

private:
    const std::shared_ptr<arrow::DataType> ReturnArrowType;
    const TType* StructType;
    const TType* MemberType;
    const ui32 Index;
};


std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockAddMemberKernel(const TVector<TType*>& argTypes, TType* resultType, ui32 index) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto structType = AS_TYPE(TBlockType, argTypes[0])->GetItemType();
    auto memberType = AS_TYPE(TBlockType, argTypes[1])->GetItemType();
    auto exec = std::make_shared<TBlockAddMemberExec>(returnArrowType, index, structType, memberType);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockAddMember(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected three args.");
    auto structType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    auto valueType = AS_TYPE(TBlockType, callable.GetInput(1).GetStaticType());
    auto memberIndex = AS_VALUE(TDataLiteral, callable.GetInput(2));
    auto index = memberIndex->AsValue().Get<ui32>();

    auto structNode = LocateNode(ctx.NodeLocator, callable, 0);
    auto valueNode = LocateNode(ctx.NodeLocator, callable, 1);

    TComputationNodePtrVector argsNodes = { structNode, valueNode };
    TVector<TType*> argsTypes = { structType, valueType };
    auto kernel = MakeBlockAddMemberKernel(argsTypes, callable.GetType()->GetReturnType(), index);
    return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), std::move(argsNodes), argsTypes, *kernel, kernel);
}

} // namespace NMiniKQL
} // namespace NKikimr
