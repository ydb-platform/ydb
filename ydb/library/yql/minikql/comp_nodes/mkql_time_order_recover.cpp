#include "mkql_time_order_recover.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <queue>

namespace NKikimr::NMiniKQL {

namespace {

class TState: public TComputationValue<TState> {
public:
    using TTimestamp = i64; //use signed integers to simplify arithmetics
    using TTimeinterval = i64;

    TState(TMemoryUsageInfo* memInfo, TTimeinterval delay, TTimeinterval ahead, ui32 rowLimit)
        : TComputationValue<TState>(memInfo)
        , Heap(Greater)
        , Delay(delay)
        , Ahead(ahead)
        , RowLimit(rowLimit + 1)
        , Latest(0)
        , Terminating(false)
    {}
    NUdf::TUnboxedValue GetOutputIfReady() {
        if (Terminating && Heap.empty()) {
            return NUdf::TUnboxedValue::MakeFinish();
        }
        if (Heap.empty()) {
            return NUdf::TUnboxedValue{};
        }
        TTimestamp oldest = Heap.top().first;
        if (oldest < Latest + Delay || Heap.size() == RowLimit || Terminating) {
            auto result = std::move(Heap.top().second);
            Heap.pop();
            return result;
        }
        return NUdf::TUnboxedValue{};
    }
    ///return input row in case it cannot process it correctly
    NUdf::TUnboxedValue ProcessRow(TTimestamp t, NUdf::TUnboxedValue&& row) {
        MKQL_ENSURE(!row.IsSpecial(), "Internal logic error");
        MKQL_ENSURE(Heap.size() < RowLimit, "Internal logic error");
        if (Heap.empty()) {
            Latest = t;
        }
        if (Latest + Delay < t && t < Latest + Ahead) {
            Heap.emplace(t, std::move(row));
        } else {
            return row;
        }
        Latest = std::max(Latest, t);
        return NUdf::TUnboxedValue{};
    }
    void Finish() {
        Terminating = true;
    }
private:
    using TEntry = std::pair<TTimestamp, NUdf::TUnboxedValue>;
    static constexpr auto Greater = [](const TEntry& lhs, const TEntry& rhs) {
        return lhs.first > rhs.first;
    };
    using THeap = std::priority_queue<
        TEntry,
        std::vector<TEntry, TMKQLAllocator<TEntry>>,
        decltype(Greater)>;
    THeap Heap;
    const TTimeinterval Delay;
    const TTimeinterval Ahead;
    const ui32 RowLimit;
    TTimestamp Latest;
    bool Terminating; //not applicable for streams, but useful for debug and testing
};

class TTimeOrderRecover : public TStatefulFlowComputationNode<TTimeOrderRecover> {
    using TBaseComputation = TStatefulFlowComputationNode<TTimeOrderRecover>;
public:
    TTimeOrderRecover(
        TComputationMutables& mutables,
        EValueRepresentation kind,
        IComputationNode* inputFlow,
        IComputationExternalNode* inputRowArg,
        IComputationNode* rowTime,
        ui32 inputRowColumnCount,
        ui32 outOfOrderColumnIndex,
        IComputationNode* delay,
        IComputationNode* ahead,
        IComputationNode* rowLimit
    )
        : TBaseComputation(mutables, inputFlow, kind)
        , InputFlow(inputFlow)
        , InputRowArg(inputRowArg)
        , RowTime(rowTime)
        , InputRowColumnCount(inputRowColumnCount)
        , OutOfOrderColumnIndex(outOfOrderColumnIndex)
        , Delay(delay)
        , Ahead(ahead)
        , RowLimit(rowLimit)
        , Cache(mutables)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (stateValue.IsInvalid()) {
            stateValue = ctx.HolderFactory.Create<TState>(
                Delay->GetValue(ctx).Get<i64>(),
                Ahead->GetValue(ctx).Get<i64>(),
                RowLimit->GetValue(ctx).Get<ui32>()
            );
        }
        auto& state = *static_cast<TState *>(stateValue.AsBoxed().Get());
        while (true) {
            if (auto out = state.GetOutputIfReady()) {
                return AddColumn(std::move(out), false, ctx);
            }
            auto item = InputFlow->GetValue(ctx);
            if (item.IsSpecial()) {
                if (item.IsFinish()) {
                    state.Finish();
                } else {
                    return item;
                }
            } else {
                InputRowArg->SetValue(ctx, NUdf::TUnboxedValue{item});
                const auto t = RowTime->GetValue(ctx).Get<ui64>();
                if (auto row = state.ProcessRow(static_cast<TState::TTimestamp>(t), std::move(item))) {
                    return AddColumn(std::move(row), true, ctx);
                }
            }
        }
    }
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(InputFlow)) {
            Own(flow, InputRowArg);
            DependsOn(flow, RowTime);
        }
    }

    NUdf::TUnboxedValue AddColumn(NUdf::TUnboxedValue&& row, bool outOfOrder, TComputationContext& ctx) const {
        if (row.IsSpecial()) {
            return row;
        }
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto result = Cache.NewArray(ctx, InputRowColumnCount + 1, itemsPtr);
        ui32 inputColumnIndex = 0;
        for (ui32 i = 0; i != InputRowColumnCount + 1; ++i) {
            if (OutOfOrderColumnIndex == i) {
                *itemsPtr++ = NUdf::TUnboxedValuePod{outOfOrder};
            } else {
                *itemsPtr++ = std::move(row.GetElements()[inputColumnIndex++]);
            }
        }
        return result;
    }

    IComputationNode* const InputFlow;
    IComputationExternalNode* const InputRowArg;
    IComputationNode* const RowTime;
    const ui32 InputRowColumnCount;
    const ui32 OutOfOrderColumnIndex;
    const IComputationNode* Delay;
    const IComputationNode* Ahead;
    const IComputationNode* RowLimit;
    const TContainerCacheOnContext Cache;
};

} //namespace

IComputationNode* TimeOrderRecover(const TComputationNodeFactoryContext& ctx,
    TRuntimeNode inputFlow,
    TRuntimeNode inputRowArg,
    TRuntimeNode rowTime,
    TRuntimeNode inputRowColumnCount,
    TRuntimeNode outOfOrderColumnIndex,
    TRuntimeNode delay,
    TRuntimeNode ahead,
    TRuntimeNode rowLimit)
{
    return new TTimeOrderRecover(ctx.Mutables
        , GetValueRepresentation(inputFlow.GetStaticType())
        , LocateNode(ctx.NodeLocator, *inputFlow.GetNode())
        , static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputRowArg.GetNode()))
        , LocateNode(ctx.NodeLocator, *rowTime.GetNode())
        , AS_VALUE(TDataLiteral, inputRowColumnCount)->AsValue().Get<ui32>()
        , AS_VALUE(TDataLiteral, outOfOrderColumnIndex)->AsValue().Get<ui32>()
        , LocateNode(ctx.NodeLocator, *delay.GetNode())
        , LocateNode(ctx.NodeLocator, *ahead.GetNode())
        , LocateNode(ctx.NodeLocator, *rowLimit.GetNode())
    );
}

}//namespace NKikimr::NMiniKQL
