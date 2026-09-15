#include "mkql_time_order_recover.h"
#include "mkql_saveload.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <queue>

namespace NKikimr::NMiniKQL {

namespace {

constexpr ui32 StateVersion = 1;

class TTimeOrderRecover: public TStatefulFlowComputationNode<TTimeOrderRecover, true> {
    using TBaseComputation = TStatefulFlowComputationNode<TTimeOrderRecover, true>;

public:
    class TState: public TComputationValue<TState> {
    public:
        using TTimestamp = i64; // use signed integers to simplify arithmetics
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
            , Self_(self)
            , Heap_(Greater)
            , Delay_(delay)
            , Ahead_(ahead)
            , RowLimit_(rowLimit + 1)
            , Latest_(0)
            , Terminating_(false)
            , MonotonicCounter_(0)
            , Ctx_(ctx)
        {
        }

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
            template <typename... TArgs>
            explicit THeap(TArgs... args)
                : TStdHeap(args...)
            {
            }

            auto begin() const {
                return c.begin();
            }
            auto end() const {
                return c.end();
            }
            void clear() {
                c.clear();
            }
        };

    public:
        NUdf::TUnboxedValue GetOutputIfReady() {
            if (Terminating_ && Heap_.empty()) {
                return NUdf::TUnboxedValue::MakeFinish();
            }
            if (Heap_.empty()) {
                return NUdf::TUnboxedValue{};
            }
            THeapKey oldestKey = Heap_.top().first;
            TTimestamp oldest = oldestKey.first;
            if (oldest < Latest_ + Delay_ || Heap_.size() == RowLimit_ || Terminating_) {
                auto result = Heap_.top().second;
                Heap_.pop();
                return result;
            }
            return NUdf::TUnboxedValue{};
        }
        /// return input row in case it cannot process it correctly
        NUdf::TUnboxedValue ProcessRow(TTimestamp t, NUdf::TUnboxedValue&& row) {
            MKQL_ENSURE(!row.IsSpecial(), "Internal logic error");
            MKQL_ENSURE(Heap_.size() < RowLimit_, "Internal logic error");
            if (Heap_.empty()) {
                Latest_ = t;
            }
            if (Latest_ + Delay_ < t && t < Latest_ + Ahead_) {
                Heap_.emplace(THeapKey(t, ++MonotonicCounter_), std::move(row));
            } else {
                return row;
            }
            Latest_ = std::max(Latest_, t);
            return NUdf::TUnboxedValue{};
        }
        void Finish() {
            Terminating_ = true;
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
                in(MonotonicCounter_);
                NUdf::TUnboxedValue row = in.ReadUnboxedValue(Self_->Packer_.RefMutableObject(Ctx_, false, Self_->StateType_), Ctx_);
                Heap_.emplace(THeapKey(t, MonotonicCounter_), std::move(row));
            }
            in(Latest_, Terminating_);
            return true;
        }

        NUdf::TUnboxedValue Save() const override {
            TOutputSerializer out(EMkqlStateType::SIMPLE_BLOB, StateVersion, Ctx_);
            out.Write<ui32>(Heap_.size());

            for (const TEntry& entry : Heap_) {
                THeapKey key = entry.first;
                out(key);
                out.WriteUnboxedValue(Self_->Packer_.RefMutableObject(Ctx_, false, Self_->StateType_), entry.second);
            }
            out(Latest_, Terminating_);
            return out.MakeState();
        }

        void ClearState() {
            Heap_.clear();
            Latest_ = 0;
            Terminating_ = false;
        }

        const TSelf* const Self_;
        THeap Heap_;
        const TTimeinterval Delay_;
        const TTimeinterval Ahead_;
        const ui32 RowLimit_;
        TTimestamp Latest_;
        bool Terminating_; // not applicable for streams, but useful for debug and testing
        ui64 MonotonicCounter_;
        TComputationContext& Ctx_;
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
        , InputFlow_(inputFlow)
        , InputRowArg_(inputRowArg)
        , RowTime_(rowTime)
        , InputRowColumnCount_(inputRowColumnCount)
        , OutOfOrderColumnIndex_(outOfOrderColumnIndex)
        , Delay_(delay)
        , Ahead_(ahead)
        , RowLimit_(rowLimit)
        , Cache_(mutables)
        , StateType_(stateType)
        , Packer_(mutables)
    {
    }

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (stateValue.IsInvalid()) {
            stateValue = ctx.HolderFactory.Create<TState>(
                this,
                Delay_->GetValue(ctx).Get<i64>(),
                Ahead_->GetValue(ctx).Get<i64>(),
                RowLimit_->GetValue(ctx).Get<ui32>(),
                ctx);
        } else if (stateValue.HasValue()) {
            MKQL_ENSURE(stateValue.IsBoxed(), "Expected boxed value");
            bool isStateToLoad = stateValue.HasListItems();
            if (isStateToLoad) {
                // Load from saved state.
                NUdf::TUnboxedValue state = ctx.HolderFactory.Create<TState>(
                    this,
                    Delay_->GetValue(ctx).Get<i64>(),
                    Ahead_->GetValue(ctx).Get<i64>(),
                    RowLimit_->GetValue(ctx).Get<ui32>(),
                    ctx);
                state.Load2(stateValue);
                stateValue = state;
            }
        }
        auto& state = *static_cast<TState*>(stateValue.AsBoxed().Get());
        while (true) {
            if (auto out = state.GetOutputIfReady()) {
                return AddColumn(std::move(out), /*outOfOrder=*/false, ctx);
            }
            auto item = InputFlow_->GetValue(ctx);
            if (item.IsSpecial()) {
                if (item.IsFinish()) {
                    state.Finish();
                } else {
                    return item;
                }
            } else {
                InputRowArg_->SetValue(ctx, NUdf::TUnboxedValue{item});
                const auto t = RowTime_->GetValue(ctx).Get<ui64>();
                if (auto row = state.ProcessRow(static_cast<TState::TTimestamp>(t), std::move(item))) {
                    return AddColumn(std::move(row), /*outOfOrder=*/true, ctx);
                }
            }
        }
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(InputFlow_)) {
            Own(flow, InputRowArg_);
            DependsOn(flow, RowTime_);
        }
    }

    NUdf::TUnboxedValue AddColumn(NUdf::TUnboxedValue&& row, bool outOfOrder, TComputationContext& ctx) const {
        if (row.IsSpecial()) {
            return row;
        }
        NUdf::TUnboxedValue* itemsPtr = nullptr;
        auto result = Cache_.NewArray(ctx, InputRowColumnCount_ + 1, itemsPtr);
        ui32 inputColumnIndex = 0;
        for (ui32 i = 0; i != InputRowColumnCount_ + 1; ++i) {
            if (OutOfOrderColumnIndex_ == i) {
                *itemsPtr++ = NUdf::TUnboxedValuePod{outOfOrder};
            } else {
                *itemsPtr++ = row.GetElements()[inputColumnIndex++];
            }
        }
        return result;
    }

    IComputationNode* const InputFlow_;
    IComputationExternalNode* const InputRowArg_;
    IComputationNode* const RowTime_;
    const ui32 InputRowColumnCount_;
    const ui32 OutOfOrderColumnIndex_;
    const IComputationNode* Delay_;
    const IComputationNode* Ahead_;
    const IComputationNode* RowLimit_;
    const TContainerCacheOnContext Cache_;
    TType* const StateType_;
    TMutableObjectOverBoxedValue<TValuePackerBoxed> Packer_;
};

} // namespace

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

    return new TTimeOrderRecover(ctx.Mutables, GetValueRepresentation(inputFlow.GetStaticType()), LocateNode(ctx.NodeLocator, *inputFlow.GetNode()), static_cast<IComputationExternalNode*>(LocateNode(ctx.NodeLocator, *inputRowArg.GetNode())), LocateNode(ctx.NodeLocator, *rowTime.GetNode()), AS_VALUE(TDataLiteral, inputRowColumnCount)->AsValue().Get<ui32>(), AS_VALUE(TDataLiteral, outOfOrderColumnIndex)->AsValue().Get<ui32>(), LocateNode(ctx.NodeLocator, *delay.GetNode()), LocateNode(ctx.NodeLocator, *ahead.GetNode()), LocateNode(ctx.NodeLocator, *rowLimit.GetNode()), rowType);
}

} // namespace NKikimr::NMiniKQL
