#include "mkql_block_removemember.h"

#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockRemoveMemberExec {
public:
    TBlockRemoveMemberExec(const std::shared_ptr<arrow::DataType>& returnArrowType, const ui32 index)
        : ReturnArrowType(returnArrowType)
        , Index(index)
    {}

    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        Y_UNUSED(ctx);
        arrow::Datum structDatum = batch.values[0];
        if (structDatum.is_scalar()) {
            const auto& obj = arrow::internal::checked_cast<const arrow::StructScalar&>(*structDatum.scalar());
            std::vector<std::shared_ptr<arrow::Scalar>> arrowValue;
            for (ui32 i = 0; i < Index; i++) {
                arrowValue.push_back(obj.value[i]);
            }
            for (ui32 i = Index + 1; i < obj.value.size(); i++) {
                arrowValue.push_back(obj.value[i]);
            }
            *res = arrow::Datum(std::make_shared<arrow::StructScalar>(arrowValue, ReturnArrowType));
        } else {
            const auto& arr = *structDatum.array();
            auto resArrayData = arrow::ArrayData::Make(ReturnArrowType, arr.length, { arr.buffers[0] });
            for (ui32 i = 0; i < Index; i++) {
                resArrayData->child_data.push_back(arr.child_data[i]);
            }
            for (ui32 i = Index + 1; i < arr.child_data.size(); i++) {
                resArrayData->child_data.push_back(arr.child_data[i]);
            }
            *res = arrow::Datum(resArrayData);
        }
        return arrow::Status::OK();
    }

private:
    const std::shared_ptr<arrow::DataType> ReturnArrowType;
    const ui32 Index;
};


std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockRemoveMemberKernel(const TVector<TType*>& argTypes, TType* resultType, const ui32 index) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TBlockRemoveMemberExec>(returnArrowType, index);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockRemoveMember(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected two args.");
    auto structType = AS_TYPE(TBlockType, callable.GetInput(0).GetStaticType());
    auto memberName = AS_VALUE(TDataLiteral, callable.GetInput(1));
    const ui32 index = memberName->AsValue().Get<ui32>();

    auto structNode = LocateNode(ctx.NodeLocator, callable, 0);

    TComputationNodePtrVector argsNodes = { structNode };
    TVector<TType*> argsTypes = { structType };
    auto kernel = MakeBlockRemoveMemberKernel(argsTypes, callable.GetType()->GetReturnType(), index);
    return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), std::move(argsNodes), argsTypes, *kernel, kernel);
}

} // namespace NMiniKQL
} // namespace NKikimr
