#include "mkql_block_add.h"

#include <ydb/library/yql/minikql/arrow/arrow_defs.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/array/builder_primitive.h>
#include <arrow/compute/exec_internal.h>
#include <arrow/compute/function.h>
#include <arrow/compute/kernel.h>
#include <arrow/compute/registry.h>
#include <arrow/util/bit_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TBlockAddWrapper : public TMutableComputationNode<TBlockAddWrapper> {
public:
    TBlockAddWrapper(TComputationMutables& mutables,
        IComputationNode* leftArg,
        IComputationNode* rightArg,
        TType* leftArgType,
        TType* rightArgType,
        TType* outputType)
        : TMutableComputationNode(mutables)
        , LeftArg(leftArg)
        , RightArg(rightArg)
        , LeftValueDesc(ToValueDescr(leftArgType))
        , RightValueDesc(ToValueDescr(rightArgType))
        , OutputValueDescr(ToValueDescr(outputType))
        , Kernel(ResolveKernel(LeftValueDesc, RightValueDesc))
        , OutputTypeBitWidth(static_cast<const arrow::FixedWidthType&>(*OutputValueDescr.type).bit_width())
        , FunctionRegistry(*arrow::compute::GetFunctionRegistry())
    {
        {
            auto execContext = arrow::compute::ExecContext();
            auto kernelContext = arrow::compute::KernelContext(&execContext);
            const auto kernelOutputValueDesc = ARROW_RESULT(Kernel.signature->out_type().Resolve(&kernelContext, {
                LeftValueDesc,
                RightValueDesc
            }));
            Y_VERIFY_DEBUG(kernelOutputValueDesc == OutputValueDescr);
        }

        Y_VERIFY_DEBUG(
            LeftValueDesc.shape == arrow::ValueDescr::ARRAY && RightValueDesc.shape == arrow::ValueDescr::ARRAY ||
            LeftValueDesc.shape == arrow::ValueDescr::SCALAR && RightValueDesc.shape == arrow::ValueDescr::ARRAY ||
            LeftValueDesc.shape == arrow::ValueDescr::ARRAY && RightValueDesc.shape == arrow::ValueDescr::SCALAR);
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto leftValue = LeftArg->GetValue(ctx);
        const auto rightValue = RightArg->GetValue(ctx);
        auto& leftDatum = TArrowBlock::From(leftValue).GetDatum();
        auto& rightDatum = TArrowBlock::From(rightValue).GetDatum();
        Y_VERIFY_DEBUG(leftDatum.descr() == LeftValueDesc);
        Y_VERIFY_DEBUG(rightDatum.descr() == RightValueDesc);
        const auto leftKind = leftDatum.kind();
        const auto rightKind = rightDatum.kind();
        MKQL_ENSURE(leftKind != arrow::Datum::ARRAY || rightKind != arrow::Datum::ARRAY ||
            leftDatum.array()->length == rightDatum.array()->length,
            "block size mismatch: "
                << static_cast<ui64>(leftDatum.array()->length)
                << " != "
                << static_cast<ui64>(rightDatum.array()->length));
        const auto blockLength = leftKind == arrow::Datum::ARRAY
            ? leftDatum.array()->length
            : rightDatum.array()->length;

        auto execContext = arrow::compute::ExecContext(&ctx.ArrowMemoryPool, nullptr, &FunctionRegistry);
        auto kernelContext = arrow::compute::KernelContext(&execContext);

        arrow::Datum output = arrow::ArrayData::Make(
            OutputValueDescr.type,
            blockLength,
            std::vector<std::shared_ptr<arrow::Buffer>> {
                ARROW_RESULT(kernelContext.AllocateBitmap(blockLength)),
                ARROW_RESULT(kernelContext.Allocate(arrow::BitUtil::BytesForBits(OutputTypeBitWidth * blockLength)))
            });
        const auto inputBatch = arrow::compute::ExecBatch({leftDatum, rightDatum}, blockLength);
        ARROW_OK(arrow::compute::detail::PropagateNulls(&kernelContext, inputBatch, output.array().get()));
        ARROW_OK(Kernel.exec(&kernelContext, inputBatch, &output));
        return ctx.HolderFactory.CreateArrowBlock(std::move(output));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(LeftArg);
        this->DependsOn(RightArg);
    }

    static const arrow::compute::ScalarKernel& ResolveKernel(const arrow::ValueDescr& leftArg,
        const arrow::ValueDescr& rightArg)
    {
        auto* functionRegistry = arrow::compute::GetFunctionRegistry();
        Y_VERIFY_DEBUG(functionRegistry != nullptr);
        auto function = ARROW_RESULT(functionRegistry->GetFunction("add"));
        Y_VERIFY_DEBUG(function != nullptr);
        Y_VERIFY_DEBUG(function->kind() == arrow::compute::Function::SCALAR);

        const auto* kernel = ARROW_RESULT(function->DispatchExact({leftArg, rightArg}));
        return *static_cast<const arrow::compute::ScalarKernel*>(kernel);
    }

    static std::shared_ptr<arrow::DataType> ConvertType(TType* type) {
        bool isOptional;
        const auto dataType = UnpackOptionalData(type, isOptional);
        switch (*dataType->GetDataSlot()) {
            case NUdf::EDataSlot::Uint64:
                return arrow::uint64();
            default:
                Y_FAIL("unexpected type %s", TString(dataType->GetKindAsStr()).c_str());
        }
    }

    static arrow::ValueDescr ToValueDescr(TType* type) {
        auto* blockType = AS_TYPE(TBlockType, type);
        const auto shape = blockType->GetShape() == TBlockType::EShape::Single
                           ? arrow::ValueDescr::SCALAR
                           : arrow::ValueDescr::ARRAY;
        return arrow::ValueDescr(ConvertType(blockType->GetItemType()), shape);
    }

private:
    IComputationNode* LeftArg;
    IComputationNode* RightArg;
    const arrow::ValueDescr LeftValueDesc;
    const arrow::ValueDescr RightValueDesc;
    const arrow::ValueDescr OutputValueDescr;
    const arrow::compute::ScalarKernel& Kernel;
    const int OutputTypeBitWidth;
    arrow::compute::FunctionRegistry& FunctionRegistry;
};

}

IComputationNode* WrapBlockAdd(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto* callableType = callable.GetType();
    return new TBlockAddWrapper(ctx.Mutables,
        LocateNode(ctx.NodeLocator, callable, 0),
        LocateNode(ctx.NodeLocator, callable, 1),
        callableType->GetArgumentType(0),
        callableType->GetArgumentType(1),
        callableType->GetReturnType());
}

}
}
