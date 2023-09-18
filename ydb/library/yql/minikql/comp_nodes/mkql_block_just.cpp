#include "mkql_block_just.h"

#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool Trivial>
class TJustBlockExec {
public:
    TJustBlockExec(const std::shared_ptr<arrow::DataType>& returnArrowType)
        : ReturnArrowType(returnArrowType)
    {}

    arrow::Status Exec(arrow::compute::KernelContext*, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        arrow::Datum inputDatum = batch.values[0];
        if (Trivial) {
            *res = inputDatum;
            return arrow::Status::OK();
        }

        if (inputDatum.is_scalar()) {
            std::vector<std::shared_ptr<arrow::Scalar>> arrowValue;
            arrowValue.emplace_back(inputDatum.scalar());
            *res = arrow::Datum(std::make_shared<arrow::StructScalar>(arrowValue, ReturnArrowType));
        } else {
            auto array = inputDatum.array();
            auto newArrayData = arrow::ArrayData::Make(ReturnArrowType, array->length, { nullptr }, 0, 0);
            newArrayData->child_data.push_back(array);
            *res = arrow::Datum(newArrayData);
        }

        return arrow::Status::OK();
    }

private:
    const std::shared_ptr<arrow::DataType> ReturnArrowType;
};

template<bool Trivial>
std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockJustKernel(const TVector<TType*>& argTypes, TType* resultType) {
    using TExec = TJustBlockExec<Trivial>;

    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TExec>(returnArrowType);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockJust(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args");

    auto data = callable.GetInput(0);

    auto dataType = AS_TYPE(TBlockType, data.GetStaticType());
    auto itemType = dataType->GetItemType();

    auto dataCompute = LocateNode(ctx.NodeLocator, callable, 0);

    TComputationNodePtrVector argsNodes = { dataCompute };
    TVector<TType*> argsTypes = { dataType };

    std::shared_ptr<arrow::compute::ScalarKernel> kernel;
    if (itemType->IsOptional() || itemType->IsVariant()) {
        kernel = MakeBlockJustKernel<false>(argsTypes, callable.GetType()->GetReturnType());
    } else {
        kernel = MakeBlockJustKernel<true>(argsTypes, callable.GetType()->GetReturnType());
    }

    return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), std::move(argsNodes), argsTypes, *kernel, kernel);
}

}
}
