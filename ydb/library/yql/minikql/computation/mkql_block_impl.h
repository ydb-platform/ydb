#pragma once

#include "mkql_computation_node_impl.h"
#include "mkql_computation_node_holders.h"

#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/public/udf/arrow/block_item.h>

#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/datum.h>
#include <arrow/compute/kernel.h>

extern "C" uint64_t GetBlockCount(const NYql::NUdf::TUnboxedValuePod data);
extern "C" uint64_t GetBitmapPopCountCount(const NYql::NUdf::TUnboxedValuePod data);
extern "C" uint8_t GetBitmapScalarValue(const NYql::NUdf::TUnboxedValuePod data);

namespace NKikimr::NMiniKQL {

arrow::Datum ConvertScalar(TType* type, const NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool);
arrow::Datum ConvertScalar(TType* type, const NUdf::TBlockItem& value, arrow::MemoryPool& pool);
arrow::Datum MakeArrayFromScalar(const arrow::Scalar& scalar, size_t len, TType* type, arrow::MemoryPool& pool);

arrow::ValueDescr ToValueDescr(TType* type);
std::vector<arrow::ValueDescr> ToValueDescr(const TVector<TType*>& types);

std::vector<arrow::compute::InputType> ConvertToInputTypes(const TVector<TType*>& argTypes);
arrow::compute::OutputType ConvertToOutputType(TType* output);

NUdf::TUnboxedValuePod MakeBlockCount(const THolderFactory& holderFactory, const uint64_t count);

class TBlockFuncNode : public TMutableComputationNode<TBlockFuncNode> {

public:
    TBlockFuncNode(TComputationMutables& mutables, TStringBuf name, TComputationNodePtrVector&& argsNodes,
        const TVector<TType*>& argsTypes, const arrow::compute::ScalarKernel& kernel,
        std::shared_ptr<arrow::compute::ScalarKernel> kernelHolder = {},
        const arrow::compute::FunctionOptions* functionOptions = nullptr);

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const;
private:
    class TArrowNode : public IArrowKernelComputationNode {
    public:
        TArrowNode(const TBlockFuncNode* parent);
        TStringBuf GetKernelName() const final;
        const arrow::compute::ScalarKernel& GetArrowKernel() const final;
        const std::vector<arrow::ValueDescr>& GetArgsDesc() const final;
        const IComputationNode* GetArgument(ui32 index) const final;

    private:
        const TBlockFuncNode* Parent_;
    };
    friend class TArrowNode;

    struct TState : public TComputationValue<TState> {
        using TComputationValue::TComputationValue;

        TState(TMemoryUsageInfo* memInfo, const arrow::compute::FunctionOptions* options,
               const arrow::compute::ScalarKernel& kernel, const std::vector<arrow::ValueDescr>& argsValuesDescr,
               TComputationContext& ctx)
               : TComputationValue(memInfo)
               , ExecContext(&ctx.ArrowMemoryPool, nullptr, nullptr)
               , KernelContext(&ExecContext)
        {
            if (kernel.init) {
                State = ARROW_RESULT(kernel.init(&KernelContext, { &kernel, argsValuesDescr, options }));
                KernelContext.SetState(State.get());
            }
        }

        arrow::compute::ExecContext ExecContext;
        arrow::compute::KernelContext KernelContext;
        std::unique_ptr<arrow::compute::KernelState> State;
    };

    void RegisterDependencies() const final;
    TState& GetState(TComputationContext& ctx) const;

    std::unique_ptr<IArrowKernelComputationNode> PrepareArrowKernelComputationNode(TComputationContext& ctx) const final;

private:
    const ui32 StateIndex;
    const TComputationNodePtrVector ArgsNodes;
    const std::vector<arrow::ValueDescr> ArgsValuesDescr;
    const arrow::compute::ScalarKernel& Kernel;
    const std::shared_ptr<arrow::compute::ScalarKernel> KernelHolder;
    const arrow::compute::FunctionOptions* const Options;
    const bool ScalarOutput;
    const TString Name;
};

struct TBlockState : public TComputationValue<TBlockState> {
    using TBase = TComputationValue<TBlockState>;

    ui64 Count = 0;
    NUdf::TUnboxedValue* Pointer_ = nullptr;

    TUnboxedValueVector Values;
    std::vector<std::deque<std::shared_ptr<arrow::ArrayData>>> Deques;
    std::vector<std::shared_ptr<arrow::ArrayData>> Arrays;

    TBlockState(TMemoryUsageInfo* memInfo, size_t width);

    void ClearValues();

    void FillArrays();

    ui64 Slice();

    NUdf::TUnboxedValuePod Get(const ui64 sliceSize, const THolderFactory& holderFactory, const size_t idx) const;
};
} //namespace NKikimr::NMiniKQL
