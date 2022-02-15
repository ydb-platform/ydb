#include "mkql_mapnext.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TFlowMapNextWrapper : public TStatelessFlowComputationNode<TFlowMapNextWrapper> {
    typedef TStatelessFlowComputationNode<TFlowMapNextWrapper> TBaseComputation;
public:
    TFlowMapNextWrapper(EValueRepresentation kind, IComputationNode* flow,
                        IComputationExternalNode* item, IComputationExternalNode* nextItem, IComputationNode* newItem)
        : TBaseComputation(flow, kind)
        , Flow(flow)
        , Item(item)
        , NextItem(nextItem)
        , NewItem(newItem)
    {}

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        NUdf::TUnboxedValue result;
        for (;;) {
            if (Finish) {
                if (!Prev) {
                    return NUdf::TUnboxedValuePod::MakeFinish();
                }
                Item->SetValue(ctx, std::move(*Prev));
                Prev.reset();
                NextItem->SetValue(ctx, NUdf::TUnboxedValuePod());
                return NewItem->GetValue(ctx);
            }

            auto item = Flow->GetValue(ctx);
            if (item.IsYield()) {
                return item;
            }

            if (item.IsFinish()) {
                Finish = true;
                continue;
            }

            if (!Prev) {
                Prev = std::move(item);
                continue;
            }

            Item->SetValue(ctx, std::move(*Prev));
            Prev = item;
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
    mutable std::optional<NUdf::TUnboxedValue> Prev;
    mutable bool Finish = false;
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
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Stream->GetValue(ctx), Item, NextItem, NewItem);
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
                     IComputationExternalNode* item, IComputationExternalNode* nextItem, IComputationNode* newItem)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , Stream(std::move(stream))
            , Item(item)
            , NextItem(nextItem)
            , NewItem(newItem)
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
            for (;;) {
                if (Finish) {
                    if (!Prev) {
                        return NUdf::EFetchStatus::Finish;
                    }
                    Item->SetValue(CompCtx, std::move(*Prev));
                    Prev.reset();
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
                    Finish = true;
                    continue;
                }

                if (!Prev) {
                    Prev = std::move(item);
                    continue;
                }

                Item->SetValue(CompCtx, std::move(*Prev));
                Prev = item;
                NextItem->SetValue(CompCtx, std::move(item));
                result = NewItem->GetValue(CompCtx);
                break;
            }
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue Stream;
        IComputationExternalNode* const Item;
        IComputationExternalNode* const NextItem;
        IComputationNode* const NewItem;
        std::optional<NUdf::TUnboxedValue> Prev;
        bool Finish = false;
    };

    IComputationNode* const Stream;
    IComputationExternalNode* const Item;
    IComputationExternalNode* const NextItem;
    IComputationNode* const NewItem;
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
        return new TFlowMapNextWrapper(GetValueRepresentation(type), input, itemArg, nextItemArg, newItem);
    } else if (type->IsStream()) {
        return new TStreamMapNextWrapper(ctx.Mutables, input, itemArg, nextItemArg, newItem);
    }

    THROW yexception() << "Expected flow or stream.";
}

}
}
