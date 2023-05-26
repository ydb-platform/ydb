#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <ydb/library/yql/minikql/arrow/arrow_util.h>

#include <arrow/array.h>
#include <arrow/scalar.h>
#include <arrow/datum.h>
#include <arrow/compute/kernel.h>

namespace NKikimr::NMiniKQL {

arrow::Datum MakeArrayFromScalar(const arrow::Scalar& scalar, size_t len, TType* type, arrow::MemoryPool& pool);

arrow::ValueDescr ToValueDescr(TType* type);
std::vector<arrow::ValueDescr> ToValueDescr(const TVector<TType*>& types);

std::vector<arrow::compute::InputType> ConvertToInputTypes(const TVector<TType*>& argTypes);
arrow::compute::OutputType ConvertToOutputType(TType* output);

class TBlockFuncNode : public TMutableComputationNode<TBlockFuncNode> {
friend class TArrowNode;
public:
    TBlockFuncNode(TComputationMutables& mutables, TStringBuf name, TVector<IComputationNode*>&& argsNodes,
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
    const TVector<IComputationNode*> ArgsNodes;
    const std::vector<arrow::ValueDescr> ArgsValuesDescr;
    const arrow::compute::ScalarKernel& Kernel;
    const std::shared_ptr<arrow::compute::ScalarKernel> KernelHolder;
    const arrow::compute::FunctionOptions* const Options;
    const bool ScalarOutput;
    const TString Name;
};

template <typename TDerived>
class TStatefulWideFlowBlockComputationNode: public TWideFlowBaseComputationNode<TDerived>
{
protected:
    TStatefulWideFlowBlockComputationNode(TComputationMutables& mutables, const IComputationNode* source, ui32 width)
        : TWideFlowBaseComputationNode<TDerived>(source)
        , StateIndex(mutables.CurValueIndex++)
        , StateKind(EValueRepresentation::Any)
        , Width(width)
    {
        MKQL_ENSURE(width > 0, "Wide flow blocks should have at least one column (block length)");
    }

private:
    struct TState : public TComputationValue<TState> {
        using TBase = TComputationValue<TState>;

        TVector<NUdf::TUnboxedValue> Values;
        TVector<NUdf::TUnboxedValue*> ValuePointers;
        NUdf::TUnboxedValue ChildState;
        TVector<TDeque<std::shared_ptr<arrow::ArrayData>>> Arrays;
        ui64 Count = 0;

        TState(TMemoryUsageInfo* memInfo, size_t width, NUdf::TUnboxedValue*const* values, TComputationContext& ctx)
            : TBase(memInfo)
            , Values(width)
            , ValuePointers(width)
            , Arrays(width - 1)
        {
            for (size_t i = 0; i < width - 1; ++i) {
                ValuePointers[i] = values[i] ? &Values[i] : nullptr;
            }
            ValuePointers.back() = &Values.back();
        }
    };

    TState& GetState(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto& state = ctx.MutableValues[GetIndex()];
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Width, output, ctx);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    EFetchResult FetchValues(TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const final {
        TState& s = GetState(ctx, output);
        const TDerived& child = static_cast<const TDerived&>(*this);
        while (s.Count == 0) {
            auto result = static_cast<const TDerived*>(this)->DoCalculate(s.ChildState, ctx, s.ValuePointers.data());
            if (result != EFetchResult::One) {
                return result;
            }

            auto& counterDatum = TArrowBlock::From(s.Values.back()).GetDatum();
            MKQL_ENSURE(counterDatum.is_scalar(), "Unexpected block length type (expecting scalar)");
            s.Count = counterDatum.template scalar_as<arrow::UInt64Scalar>().value;
            if (!s.Count) {
                continue;
            }
            for (size_t i = 0; i < Width - 1; ++i) {
                s.Arrays[i].clear();
                if (!output[i]) {
                    continue;
                }
                auto& datum = TArrowBlock::From(s.Values[i]).GetDatum();
                if (datum.is_scalar()) {
                    continue;
                }
                MKQL_ENSURE(datum.is_arraylike(), "Unexpected block type (expecting array or chunked array)");
                if (datum.is_array()) {
                    s.Arrays[i].push_back(datum.array());
                } else {
                    for (auto& chunk : datum.chunks()) {
                        s.Arrays[i].push_back(chunk->data());
                    }
                }
            }
        }
        ui64 sliceSize = s.Count;
        for (size_t i = 0; i < s.Arrays.size(); ++i) {
            const auto& arr = s.Arrays[i];
            if (arr.empty()) {
                continue;
            }

            MKQL_ENSURE(arr.front()->length <= s.Count, "Unexpected array length at column #" << i);
            sliceSize = std::min<ui64>(sliceSize, arr.front()->length);
        }

        for (size_t i = 0; i < s.Arrays.size(); ++i) {
            if (!output[i]) {
                continue;
            }
            if (s.Arrays[i].empty()) {
                *(output[i]) = s.Values[i];
                continue;
            }

            auto& array = s.Arrays[i].front();
            if (array->length == sliceSize) {
                *(output[i]) = ctx.HolderFactory.CreateArrowBlock(std::move(array));
                s.Arrays[i].pop_front();
            } else {
                *(output[i]) = ctx.HolderFactory.CreateArrowBlock(Chop(array, sliceSize));
            }
        }

        if (output[Width - 1]) {
            *(output[Width - 1]) = ctx.HolderFactory.CreateArrowBlock(arrow::Datum(std::make_shared<arrow::UInt64Scalar>(sliceSize)));
        }
        s.Count -= sliceSize;
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

    const ui32 StateIndex;
    const EValueRepresentation StateKind;
    const ui32 Width;
};

} //namespace NKikimr::NMiniKQL
