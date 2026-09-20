#include "mkql_mapnext.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>

namespace NKikimr::NMiniKQL {

namespace {

struct TState: public TComputationValue<TState> {
    using TComputationValue::TComputationValue;

    std::optional<NUdf::TUnboxedValue> Prev;
    bool Finish = false;
};

class TFlowMapNextWrapper: public TStatefulFlowComputationNode<TFlowMapNextWrapper> {
    using TBaseComputation = TStatefulFlowComputationNode<TFlowMapNextWrapper>;

public:
    TFlowMapNextWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow,
                        IComputationExternalNode* item, IComputationExternalNode* nextItem, IComputationNode* newItem)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Any)
        , Flow_(flow)
        , Item_(item)
        , NextItem_(nextItem)
        , NewItem_(newItem)
    {
    }

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
                Item_->SetValue(ctx, std::move(*state.Prev));
                state.Prev.reset();
                NextItem_->SetValue(ctx, NUdf::TUnboxedValuePod());
                return NewItem_->GetValue(ctx);
            }

            auto item = Flow_->GetValue(ctx);
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

            Item_->SetValue(ctx, std::move(*state.Prev));
            state.Prev = item;
            NextItem_->SetValue(ctx, std::move(item));
            result = NewItem_->GetValue(ctx);
            break;
        }

        return result;
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow_)) {
            Own(flow, Item_);
            Own(flow, NextItem_);
            DependsOn(flow, NewItem_);
        }
    }

    IComputationNode* const Flow_;
    IComputationExternalNode* const Item_;
    IComputationExternalNode* const NextItem_;
    IComputationNode* const NewItem_;
};

class TStreamMapNextWrapper: public TMutableComputationNode<TStreamMapNextWrapper> {
    using TBaseComputation = TMutableComputationNode<TStreamMapNextWrapper>;

public:
    TStreamMapNextWrapper(TComputationMutables& mutables, IComputationNode* stream,
                          IComputationExternalNode* item, IComputationExternalNode* nextItem, IComputationNode* newItem)
        : TBaseComputation(mutables)
        , Stream_(stream)
        , Item_(item)
        , NextItem_(nextItem)
        , NewItem_(newItem)
        , StateIndex_(mutables.CurValueIndex++)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Stream_->GetValue(ctx), Item_, NextItem_, NewItem_, StateIndex_);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Stream_);
        Own(Item_);
        Own(NextItem_);
        DependsOn(NewItem_);
    }

    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& stream,
                     IComputationExternalNode* item, IComputationExternalNode* nextItem, IComputationNode* newItem, ui32 stateIndex)
            : TBase(memInfo)
            , CompCtx_(compCtx)
            , Stream_(std::move(stream))
            , Item_(item)
            , NextItem_(nextItem)
            , NewItem_(newItem)
            , StateIndex_(stateIndex)
        {
        }

    private:
        ui32 GetTraverseCount() const final {
            return 1U;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32) const final {
            return Stream_;
        }

        NUdf::TUnboxedValue Save() const final {
            return NUdf::TUnboxedValuePod::Zero();
        }

        void Load(const NUdf::TStringRef&) final {
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            auto& state = GetState();
            for (;;) {
                if (state.Finish) {
                    if (!state.Prev) {
                        return NUdf::EFetchStatus::Finish;
                    }
                    Item_->SetValue(CompCtx_, std::move(*state.Prev));
                    state.Prev.reset();
                    NextItem_->SetValue(CompCtx_, NUdf::TUnboxedValuePod());

                    result = NewItem_->GetValue(CompCtx_);
                    return NUdf::EFetchStatus::Ok;
                }

                NUdf::TUnboxedValue item;
                const auto status = Stream_.Fetch(item);
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

                Item_->SetValue(CompCtx_, std::move(*state.Prev));
                state.Prev = item;
                NextItem_->SetValue(CompCtx_, std::move(item));
                result = NewItem_->GetValue(CompCtx_);
                break;
            }
            return NUdf::EFetchStatus::Ok;
        }

        TState& GetState() const {
            auto& result = CompCtx_.MutableValues[StateIndex_];
            if (!result.HasValue()) {
                result = CompCtx_.HolderFactory.Create<TState>();
            }
            return *static_cast<TState*>(result.AsBoxed().Get());
        }

        TComputationContext& CompCtx_;
        const NUdf::TUnboxedValue Stream_;
        IComputationExternalNode* const Item_;
        IComputationExternalNode* const NextItem_;
        IComputationNode* const NewItem_;
        const ui32 StateIndex_;
    };

    IComputationNode* const Stream_;
    IComputationExternalNode* const Item_;
    IComputationExternalNode* const NextItem_;
    IComputationNode* const NewItem_;
    const ui32 StateIndex_;
};

} // namespace

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

} // namespace NKikimr::NMiniKQL
