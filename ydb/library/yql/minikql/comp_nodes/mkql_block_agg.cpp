#include "mkql_block_agg.h"
#include "mkql_block_agg_factory.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <arrow/array/array_primitive.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TAggParams {
    TStringBuf Name;
    TTupleType* TupleType;
    std::vector<ui32> ArgColumns;
};

class TBlockCombineAllWrapper : public TStatefulWideFlowComputationNode<TBlockCombineAllWrapper> {
public:
    TBlockCombineAllWrapper(TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        ui32 countColumn,
        std::optional<ui32> filterColumn,
        size_t width,
        TVector<TAggParams>&& aggsParams)
        : TStatefulWideFlowComputationNode(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , CountColumn_(countColumn)
        , FilterColumn_(filterColumn)
        , Width_(width)
        , AggsParams_(std::move(aggsParams))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
        TComputationContext& ctx,
        NUdf::TUnboxedValue*const* output) const
    {
        auto& s = GetState(state, ctx);
        if (s.IsFinished_) {
            return EFetchResult::Finish;
        }

        for (;;) {
            auto result = Flow_->FetchValues(ctx, s.ValuePointers_.data());
            if (result == EFetchResult::Yield) {
                return result;
            } else if (result == EFetchResult::One) {
                ui64 batchLength = GetBatchLength(s.Values_.data());
                if (!batchLength) {
                    continue;
                }

                std::optional<ui64> filtered;
                if (FilterColumn_) {
                    arrow::BooleanArray arr(TArrowBlock::From(s.Values_[*FilterColumn_]).GetDatum().array());
                    ui64 popCount = (ui64)arr.true_count();
                    if (popCount == 0) {
                        continue;
                    }

                    filtered = popCount;
                }

                s.HasValues_ = true;
                for (size_t i = 0; i < s.Aggs_.size(); ++i) {
                    if (output[i]) {
                        s.Aggs_[i]->AddMany(s.Values_.data(), batchLength, filtered);
                    }
                }
            } else {
                s.IsFinished_ = true;
                if (!s.HasValues_) {
                    return EFetchResult::Finish;
                }

                for (size_t i = 0; i < s.Aggs_.size(); ++i) {
                    if (auto* out = output[i]; out != nullptr) {
                        *out = s.Aggs_[i]->Finish();
                    }
                }

                return EFetchResult::One;
            }
        }

        return EFetchResult::Finish;
    }

private:
    struct TState : public TComputationValue<TState> {
        TVector<NUdf::TUnboxedValue> Values_;
        TVector<NUdf::TUnboxedValue*> ValuePointers_;
        TVector<std::unique_ptr<IBlockAggregator>> Aggs_;
        bool IsFinished_ = false;
        bool HasValues_ = false;

        TState(TMemoryUsageInfo* memInfo, size_t width, std::optional<ui32> filterColumn, const TVector<TAggParams>& params, const THolderFactory& holderFactory)
            : TComputationValue(memInfo)
            , Values_(width)
            , ValuePointers_(width)
        {
            for (size_t i = 0; i < width; ++i) {
                ValuePointers_[i] = &Values_[i];
            }

            for (const auto& p : params) {
                Aggs_.emplace_back(MakeBlockAggregator(p.Name, p.TupleType, filterColumn, p.ArgColumns, holderFactory));
            }
        }
    };

private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    TState& GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (!state.HasValue()) {
            state = ctx.HolderFactory.Create<TState>(Width_, FilterColumn_, AggsParams_, ctx.HolderFactory);
        }
        return *static_cast<TState*>(state.AsBoxed().Get());
    }

    ui64 GetBatchLength(const NUdf::TUnboxedValue* columns) const {
        return TArrowBlock::From(columns[CountColumn_]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
    }

private:
    IComputationWideFlowNode* Flow_;
    const ui32 CountColumn_;
    std::optional<ui32> FilterColumn_;
    const size_t Width_;
    const TVector<TAggParams> AggsParams_;
};

}

IComputationNode* WrapBlockCombineAll(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");
    const auto flowType = AS_TYPE(TFlowType, callable.GetInput(0).GetStaticType());
    const auto tupleType = AS_TYPE(TTupleType, flowType->GetItemType());

    auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    MKQL_ENSURE(wideFlow != nullptr, "Expected wide flow node");

    ui32 countColumn = AS_VALUE(TDataLiteral, callable.GetInput(1))->AsValue().Get<ui32>();
    auto filterColumnVal = AS_VALUE(TOptionalLiteral, callable.GetInput(2));
    std::optional<ui32> filterColumn;
    if (filterColumnVal->HasItem()) {
        filterColumn = AS_VALUE(TDataLiteral, filterColumnVal->GetItem())->AsValue().Get<ui32>();
    }

    auto aggsVal = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    TVector<TAggParams> aggsParams;
    for (ui32 i = 0; i < aggsVal->GetValuesCount(); ++i) {
        auto aggVal = AS_VALUE(TTupleLiteral, aggsVal->GetValue(i));
        auto name = AS_VALUE(TDataLiteral, aggVal->GetValue(0))->AsValue().AsStringRef();

        std::vector<ui32> argColumns;
        for (ui32 j = 1; j < aggVal->GetValuesCount(); ++j) {
            argColumns.push_back(AS_VALUE(TDataLiteral, aggVal->GetValue(j))->AsValue().Get<ui32>());
        }

        aggsParams.emplace_back(TAggParams{ TStringBuf(name), tupleType, argColumns });
    }

    return new TBlockCombineAllWrapper(ctx.Mutables, wideFlow, countColumn, filterColumn, tupleType->GetElementsCount(), std::move(aggsParams));
}

}
}
