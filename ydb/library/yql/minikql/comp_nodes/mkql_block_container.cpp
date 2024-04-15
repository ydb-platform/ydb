#include "mkql_block_container.h"

#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

#include <arrow/util/bitmap_ops.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockAsContainerExec {
public:
    TBlockAsContainerExec(const TVector<TType*>& argTypes, const std::shared_ptr<arrow::DataType>& returnArrowType)
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

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockAsContainerKernel(const TVector<TType*>& argTypes, TType* resultType) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    auto exec = std::make_shared<TBlockAsContainerExec>(argTypes, returnArrowType);
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });

    kernel->null_handling = arrow::compute::NullHandling::COMPUTED_NO_PREALLOCATE;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockAsContainer(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    TComputationNodePtrVector argsNodes;
    TVector<TType*> argsTypes;
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        argsNodes.push_back(LocateNode(ctx.NodeLocator, callable, i));
        argsTypes.push_back(callable.GetInput(i).GetStaticType());
    }

    auto kernel = MakeBlockAsContainerKernel(argsTypes, callable.GetType()->GetReturnType());
    return new TBlockFuncNode(ctx.Mutables, callable.GetType()->GetName(), std::move(argsNodes), argsTypes, *kernel, kernel);
}

} // namespace NMiniKQL
} // namespace NKikimr
