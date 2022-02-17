#include "mkql_mapnext.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

struct TState : public TComputationValue<TState> {
    using TComputationValue::TComputationValue;

    std::optional<NUdf::TUnboxedValue> Prev;
    bool Finish = false;
};

class TFlowMapNextWrapper : public TStatefulFlowComputationNode<TFlowMapNextWrapper> {
    typedef TStatefulFlowComputationNode<TFlowMapNextWrapper> TBaseComputation;
public:
    TFlowMapNextWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow,
                        IComputationExternalNode* item, IComputationExternalNode* nextItem, IComputationNode* newItem)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Any)
        , Flow(flow)
        , Item(item)
        , NextItem(nextItem)
        , NewItem(newItem)
    {}

    NUdf::TUnboxedValue DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (!stateValue.HasValue()) {
            stateValue = ctx.HolderFactory.Create<TState>();
        }
        TState& state = *static_cast<TState*>(stateValue.AsBoxed().Get());

        NUdf::TUnboxedValue result;
        for (;;) {
            if (state.Finish) {
                if (!state.Prev) {
                    return NUdf::TUnboxedValuePod::MakeFinish();
                }
                Item->SetValue(ctx, std::move(*state.Prev));
                state.Prev.reset();
                NextItem->SetValue(ctx, NUdf::TUnboxedValuePod());
                return NewItem->GetValue(ctx);
            }

            auto item = Flow->GetValue(ctx);
            if (item.IsYield()) {
                return item;
            }

            if (item.IsFinish()) {
                state.Finish = true;
                continue;
            }

            if (!state.Prev) {
                state.Prev = std::move(item);
                continue;
            }

            Item->SetValue(ctx, std::move(*state.Prev));
            state.Prev = item;
            NextItem->SetValue(ctx, std::move(item));
            result = NewItem->GetValue(ctx);
            break;
        }

        return result;
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            Own(flow, Item);
            Own(flow, NextItem);
            DependsOn(flow, NewItem);
        }
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    IComputationExternalNode* const NextItem;
    IComputationNode* const NewItem;
};

class TStreamMapNextWrapper : public TMutableComputationNode<TStreamMapNextWrapper> {
    typedef TMutableComputationNode<TStreamMapNextWrapper> TBaseComputation;
public:
    TStreamMapNextWrapper(TComputationMutables& mutables, IComputationNode* stream,
                          IComputationExternalNode* item, IComputationExternalNode* nextItem, IComputationNode* newItem)
        : TBaseComputation(mutables)
        , Stream(stream)
        , Item(item)
        , NextItem(nextItem)
        , NewItem(newItem)
        , StateIndex(mutables.CurValueIndex++)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Stream->GetValue(ctx), Item, NextItem, NewItem, StateIndex);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Stream);
        Own(Item);
        Own(NextItem);
        DependsOn(NewItem);
    }

    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& stream,
                     IComputationExternalNode* item, IComputationExternalNode* nextItem, IComputationNode* newItem, ui32 stateIndex)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , Stream(std::move(stream))
            , Item(item)
            , NextItem(nextItem)
            , NewItem(newItem)
            , StateIndex(stateIndex)
        {
        }

    private:
        ui32 GetTraverseCount() const final {
            return 1U;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32) const final {
            return Stream;
        }

        NUdf::TUnboxedValue Save() const final {
            return NUdf::TUnboxedValuePod::Zero();
        }

        void Load(const NUdf::TStringRef&) final {}

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            auto& state = GetState();
            for (;;) {
                if (state.Finish) {
                    if (!state.Prev) {
                        return NUdf::EFetchStatus::Finish;
                    }
                    Item->SetValue(CompCtx, std::move(*state.Prev));
                    state.Prev.reset();
                    NextItem->SetValue(CompCtx, NUdf::TUnboxedValuePod());

                    result = NewItem->GetValue(CompCtx);
                    return NUdf::EFetchStatus::Ok;
                }

                NUdf::TUnboxedValue item;
                const auto status = Stream.Fetch(item);
                if (status == NUdf::EFetchStatus::Yield) {
                    return status;
                }

                if (status == NUdf::EFetchStatus::Finish) {
                    state.Finish = true;
                    continue;
                }

                if (!state.Prev) {
                    state.Prev = std::move(item);
                    continue;
                }

                Item->SetValue(CompCtx, std::move(*state.Prev));
                state.Prev = item;
                NextItem->SetValue(CompCtx, std::move(item));
                result = NewItem->GetValue(CompCtx);
                break;
            }
            return NUdf::EFetchStatus::Ok;
        }

        TState& GetState() const {
            auto& result = CompCtx.MutableValues[StateIndex];
            if (!result.HasValue()) {
                result = CompCtx.HolderFactory.Create<TState>();
            }
            return *static_cast<TState*>(result.AsBoxed().Get());
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue Stream;
        IComputationExternalNode* const Item;
        IComputationExternalNode* const NextItem;
        IComputationNode* const NewItem;
        const ui32 StateIndex;
    };

    IComputationNode* const Stream;
    IComputationExternalNode* const Item;
    IComputationExternalNode* const NextItem;
    IComputationNode* const NewItem;
    const ui32 StateIndex;
};

}

IComputationNode* WrapMapNext(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args, got " << callable.GetInputsCount());
    const auto type = callable.GetType()->GetReturnType();

    const auto input = LocateNode(ctx.NodeLocator, callable, 0);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto nextItemArg = LocateExternalNode(ctx.NodeLocator, callable, 2);
    const auto newItem = LocateNode(ctx.NodeLocator, callable, 3);

    if (type->IsFlow()) {
        return new TFlowMapNextWrapper(ctx.Mutables, GetValueRepresentation(type), input, itemArg, nextItemArg, newItem);
    } else if (type->IsStream()) {
        return new TStreamMapNextWrapper(ctx.Mutables, input, itemArg, nextItemArg, newItem);
    }

    THROW yexception() << "Expected flow or stream.";
}

}
}
