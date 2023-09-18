#pragma once

#include "mkql_computation_node_impl.h"
#include "mkql_computation_node_holders.h"

#include <ydb/library/yql/minikql/arrow/arrow_util.h>
#include <ydb/library/yql/public/udf/arrow/block_item.h>

#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/datum.h>
#include <arrow/compute/kernel.h>

namespace NKikimr::NMiniKQL {

arrow::Datum ConvertScalar(TType* type, const NUdf::TUnboxedValuePod& value, arrow::MemoryPool& pool);
arrow::Datum ConvertScalar(TType* type, const NUdf::TBlockItem& value, arrow::MemoryPool& pool);
arrow::Datum MakeArrayFromScalar(const arrow::Scalar& scalar, size_t len, TType* type, arrow::MemoryPool& pool);

arrow::ValueDescr ToValueDescr(TType* type);
std::vector<arrow::ValueDescr> ToValueDescr(const TVector<TType*>& types);

std::vector<arrow::compute::InputType> ConvertToInputTypes(const TVector<TType*>& argTypes);
arrow::compute::OutputType ConvertToOutputType(TType* output);

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
    TUnboxedValueVector Values;
    std::vector<std::deque<std::shared_ptr<arrow::ArrayData>>> Arrays;

    TBlockState(TMemoryUsageInfo* memInfo, size_t width)
        : TBase(memInfo), Values(width), Arrays(width - 1ULL)
    {}

    void FillArrays(NUdf::TUnboxedValue*const* output) {
        auto& counterDatum = TArrowBlock::From(Values.back()).GetDatum();
        MKQL_ENSURE(counterDatum.is_scalar(), "Unexpected block length type (expecting scalar)");
        Count = counterDatum.scalar_as<arrow::UInt64Scalar>().value;
        if (!Count) {
            return;
        }
        for (size_t i = 0U; i < Arrays.size(); ++i) {
            Arrays[i].clear();
            if (!output[i]) {
                return;
            }
            auto& datum = TArrowBlock::From(Values[i]).GetDatum();
            if (datum.is_scalar()) {
                return;
            }
            MKQL_ENSURE(datum.is_arraylike(), "Unexpected block type (expecting array or chunked array)");
            if (datum.is_array()) {
                Arrays[i].push_back(datum.array());
            } else {
                for (auto& chunk : datum.chunks()) {
                    Arrays[i].push_back(chunk->data());
                }
            }
        }
    }

    void FillOutputs(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) {
        auto sliceSize = Count;
        for (size_t i = 0; i < Arrays.size(); ++i) {
            const auto& arr = Arrays[i];
            if (arr.empty()) {
                continue;
            }

            MKQL_ENSURE(ui64(arr.front()->length) <= Count, "Unexpected array length at column #" << i);
            sliceSize = std::min<ui64>(sliceSize, arr.front()->length);
        }

        for (size_t i = 0; i < Arrays.size(); ++i) {
            if (const auto out = output[i]) {
                if (Arrays[i].empty()) {
                    *out = Values[i];
                    continue;
                }

                if (auto& array = Arrays[i].front(); ui64(array->length) == sliceSize) {
                    *out = ctx.HolderFactory.CreateArrowBlock(std::move(array));
                    Arrays[i].pop_front();
                } else {
                    *out = ctx.HolderFactory.CreateArrowBlock(Chop(array, sliceSize));
                }
            }
        }

        if (const auto out = output[Values.size() - 1U]) {
            *out = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(sliceSize)));
        }
        Count -= sliceSize;
    }
};

template <typename TDerived>
class TStatefulWideFlowBlockComputationNode: public TWideFlowBaseComputationNode<TDerived>
{
protected:
    TStatefulWideFlowBlockComputationNode(TComputationMutables& mutables, const IComputationNode* source, ui32 width)
        : TWideFlowBaseComputationNode<TDerived>(source)
        , StateIndex(mutables.CurValueIndex++)
        , StateKind(EValueRepresentation::Boxed)
        , Width(width)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(width))
    {
        MKQL_ENSURE(width > 0, "Wide flow blocks should have at least one column (block length)");
    }
private:
    struct TState : public TBlockState {
        NUdf::TUnboxedValue ChildState;

        TState(TMemoryUsageInfo* memInfo, size_t width, ui32 wideFieldsIndex, NUdf::TUnboxedValue*const* values, TComputationContext& ctx)
            : TBlockState(memInfo, width)
        {
            auto**const fields = ctx.WideFields.data() + wideFieldsIndex;
            for (size_t i = 0; i < width - 1; ++i) {
                fields[i] = values[i] ? &Values[i] : nullptr;
            }
            fields[width - 1] = &Values.back();
        }
    };

    TState& GetState(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& state = ctx.MutableValues[GetIndex()];
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Width, WideFieldsIndex, output, ctx);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const final {
        auto& s = GetState(ctx, output);
        const auto fields = ctx.WideFields.data() + WideFieldsIndex;
        while (s.Count == 0) {
            if (const auto result = static_cast<const TDerived*>(this)->DoCalculate(s.ChildState, ctx, fields); result != EFetchResult::One) {
                return result;
            }
            s.FillArrays(output);
        }

        s.FillOutputs(ctx, output);
        return EFetchResult::One;
    }

    ui32 GetIndex() const final {
        return StateIndex;
    }

    void CollectDependentIndexes(const IComputationNode* owner, IComputationNode::TIndexesMap& dependencies) const final {
        if (this == owner)
            return;

        const auto ins = dependencies.emplace(StateIndex, StateKind);
        if (ins.second && this->Dependence) {
            this->Dependence->CollectDependentIndexes(owner, dependencies);
        }
    }
protected:
    const ui32 StateIndex;
    const EValueRepresentation StateKind;
    const ui32 Width;
    const ui32 WideFieldsIndex;
};

} //namespace NKikimr::NMiniKQL
