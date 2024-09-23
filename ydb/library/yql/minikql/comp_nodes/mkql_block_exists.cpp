#include "mkql_exists.h"
#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/minikql/computation/mkql_block_impl.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockExistsExec {
public:
    arrow::Status Exec(arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) const {
        const auto& input = batch.values[0];
        MKQL_ENSURE(input.is_array(), "Expected array");
        const auto& arr = *input.array();

        auto nullCount = arr.GetNullCount();
        if (nullCount == arr.length) {
            *res = MakeFalseArray(ctx->memory_pool(), arr.length);
        } else if (nullCount == 0) {
            *res = MakeTrueArray(ctx->memory_pool(), arr.length);
        } else {
            *res = MakeBitmapArray(ctx->memory_pool(), arr.length, arr.offset,
                arr.buffers[0]->data());
        }

        return arrow::Status::OK();
    }
};

std::shared_ptr<arrow::compute::ScalarKernel> MakeBlockExistsKernel(const TVector<TType*>& argTypes, TType* resultType) {
    std::shared_ptr<arrow::DataType> returnArrowType;
    MKQL_ENSURE(ConvertArrowType(AS_TYPE(TBlockType, resultType)->GetItemType(), returnArrowType), "Unsupported arrow type");
    // Ensure the result Arrow type (i.e. boolean) is Arrow UInt8Type.
    Y_DEBUG_ABORT_UNLESS(returnArrowType == arrow::uint8());
    auto exec = std::make_shared<TBlockExistsExec>();
    auto kernel = std::make_shared<arrow::compute::ScalarKernel>(ConvertToInputTypes(argTypes), ConvertToOutputType(resultType),
        [exec](arrow::compute::KernelContext* ctx, const arrow::compute::ExecBatch& batch, arrow::Datum* res) {
        return exec->Exec(ctx, batch, res);
    });
    kernel->null_handling = arrow::compute::NullHandling::OUTPUT_NOT_NULL;
    return kernel;
}

} // namespace

IComputationNode* WrapBlockExists(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    auto compute = LocateNode(ctx.NodeLocator, callable, 0);
    TComputationNodePtrVector argsNodes = { compute };
    TVector<TType*> argsTypes = { callable.GetInput(0).GetStaticType() };
    auto kernel = MakeBlockExistsKernel(argsTypes, callable.GetType()->GetReturnType());
    return new TBlockFuncNode(ctx.Mutables, "Exists", std::move(argsNodes), argsTypes, *kernel, kernel);
}

} // namespace NMiniKQL
} // namespace NKikimr
