#include "mkql_time_order_recover.h"
#include "mkql_saveload.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <queue>

namespace NKikimr::NMiniKQL {

namespace {

constexpr ui32 StateVersion = 1;

class TTimeOrderRecover : public TStatefulFlowComputationNode<TTimeOrderRecover, true> {
    using TBaseComputation = TStatefulFlowComputationNode<TTimeOrderRecover, true>;
public:
    class TState: public TComputationValue<TState> {
    public:
        using TTimestamp = i64; //use signed integers to simplify arithmetics
        using TTimeinterval = i64;
        using TSelf = TTimeOrderRecover;

        TState(
            TMemoryUsageInfo* memInfo,
            const TSelf* self,
            TTimeinterval delay,
            TTimeinterval ahead,
            ui32 rowLimit,
            TComputationContext& ctx)
            : TComputationValue<TState>(memInfo)
            , Self(self)
            , Heap(Greater)
            , Delay(delay)
            , Ahead(ahead)
            , RowLimit(rowLimit + 1)
            , Latest(0)
            , Terminating(false)
            , MonotonicCounter(0)
            , Ctx(ctx)
        {}

    private:
        using THeapKey = std::pair<TTimestamp, ui64>;
        using TEntry = std::pair<THeapKey, NUdf::TUnboxedValue>;
        static constexpr auto Greater = [](const TEntry& lhs, const TEntry& rhs) {
            return lhs.first > rhs.first;
        };
        using TStdHeap = std::priority_queue<
            TEntry,
            std::vector<TEntry, TMKQLAllocator<TEntry>>,
            decltype(Greater)>;


        struct THeap: public TStdHeap {
            template<typename...TArgs>
            THeap(TArgs... args) : TStdHeap(args...) {}
            
            auto begin() const { return c.begin(); }
            auto end() const { return c.end(); }
            auto clear() { return c.clear(); }
        };

    public:

        NUdf::TUnboxedValue GetOutputIfReady() {
            if (Terminating && Heap.empty()) {
                return NUdf::TUnboxedValue::MakeFinish();
            }
            if (Heap.empty()) {
                return NUdf::TUnboxedValue{};
            }
            THeapKey oldestKey = Heap.top().first;
            TTimestamp oldest = oldestKey.first;
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
                Heap.emplace(THeapKey(t, ++MonotonicCounter), std::move(row));
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

        bool HasListItems() const override {
            return false;
        }

        bool Load2(const NUdf::TUnboxedValue& state) override {
            TInputSerializer in(state, EMkqlStateType::SIMPLE_BLOB);

            const auto loadStateVersion = in.GetStateVersion();
            if (loadStateVersion != StateVersion) {
                THROW yexception() << "Invalid state version " << loadStateVersion;
            }
            const auto heapSize = in.Read<ui32>();
            ClearState();
            for (auto i = 0U; i < heapSize; ++i) {
                TTimestamp t = in.Read<ui64>();
                in(MonotonicCounter);
                NUdf::TUnboxedValue row = in.ReadUnboxedValue(Self->Packer.RefMutableObject(Ctx, false, Self->StateType), Ctx);
                Heap.emplace(THeapKey(t, MonotonicCounter), std::move(row));
            }
            in(Latest, Terminating);
            return true;
        }

        NUdf::TUnboxedValue Save() const override {
            TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx);
            out.Write<ui32>(Heap.size());

            for (const TEntry& entry : Heap) {
                THeapKey key = entry.first;
                out(key);
                out.WriteUnboxedValue(Self->Packer.RefMutableObject(Ctx, false, Self->StateType), entry.second);
            }
            out(Latest, Terminating);
            return out.MakeState();
        }

        void ClearState() {
            Heap.clear();
            Latest = 0;
            Terminating = false;
        }

    private:
        const TSelf *const Self;
        THeap Heap;
        const TTimeinterval Delay;
        const TTimeinterval Ahead;
        const ui32 RowLimit;
        TTimestamp Latest;
        bool Terminating; //not applicable for streams, but useful for debug and testing
        ui64 MonotonicCounter;
        TComputationContext& Ctx;
    };

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
        IComputationNode* rowLimit,
        TType* stateType)
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
        , StateType(stateType)
        , Packer(mutables)
    { }

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (stateValue.IsInvalid()) {
            stateValue = ctx.HolderFactory.Create<TState>(
                this,
                Delay->GetValue(ctx).Get<i64>(),
                Ahead->GetValue(ctx).Get<i64>(),
                RowLimit->GetValue(ctx).Get<ui32>(),
                ctx);
        } else if (stateValue.HasValue()) {
            MKQL_ENSURE(stateValue.IsBoxed(), "Expected boxed value");
            bool isStateToLoad = stateValue.HasListItems();
            if (isStateToLoad) {
                // Load from saved state.
                NUdf::TUnboxedValue state = ctx.HolderFactory.Create<TState>(
                    this,
                    Delay->GetValue(ctx).Get<i64>(),
                    Ahead->GetValue(ctx).Get<i64>(),
                    RowLimit->GetValue(ctx).Get<ui32>(),
                    ctx);
                state.Load2(stateValue);
                stateValue = state;
            }
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
    TType* const StateType;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> Packer;
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
    auto* rowType = AS_TYPE(TStructType, AS_TYPE(TFlowType, inputFlow.GetStaticType())->GetItemType());

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
        , rowType
    );
}

}//namespace NKikimr::NMiniKQL
