#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <ydb/library/yql/minikql/arrow/arrow_util.h>

#include <arrow/array.h>

namespace NKikimr::NMiniKQL {

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
