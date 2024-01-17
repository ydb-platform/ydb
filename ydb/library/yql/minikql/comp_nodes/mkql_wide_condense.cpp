#include "mkql_wide_condense.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/utils/cast.h>

namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

template <bool Interruptable, bool UseCtx>
class TWideCondense1Wrapper : public TStatefulWideFlowCodegeneratorNode<TWideCondense1Wrapper<Interruptable, UseCtx>> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideCondense1Wrapper<Interruptable, UseCtx>>;
public:
     TWideCondense1Wrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow,
        TComputationExternalNodePtrVector&& items, TComputationNodePtrVector&& initState,
        TComputationExternalNodePtrVector&& state, IComputationNode* outSwitch, TComputationNodePtrVector&& updateState)
            : TBaseComputation(mutables, flow, EValueRepresentation::Embedded), Flow(flow)
            , Items(std::move(items))
            , InitState(std::move(initState))
            , State(std::move(state))
            , Switch(outSwitch)
            , UpdateState(std::move(updateState))
            , SwitchItem(IsPasstrought(Switch, Items))
            , ItemsOnInit(GetPasstroughtMap(Items, InitState))
            , ItemsOnUpdate(GetPasstroughtMap(Items, UpdateState))
            , UpdateOnItems(GetPasstroughtMap(UpdateState, Items))
            , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
            , TempStateIndex(std::exchange(mutables.CurValueIndex, mutables.CurValueIndex + State.size()))
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsFinish()) {
            return EFetchResult::Finish;
        } else if (state.HasValue() && state.Get<bool>()) {
            if constexpr (UseCtx) {
                CleanupCurrentContext();
            }

            state = NUdf::TUnboxedValuePod(false);
            for (ui32 i = 0U; i < State.size(); ++i)
                State[i]->SetValue(ctx, InitState[i]->GetValue(ctx));
        }

        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        while (true) {
            for (auto i = 0U; i < Items.size(); ++i)
                if (Items[i]->GetDependencesCount() > 0U || ItemsOnInit[i] || ItemsOnUpdate[i] || SwitchItem && i == *SwitchItem)
                    fields[i] = &Items[i]->RefValue(ctx);

            switch (Flow->FetchValues(ctx, fields)) {
                case EFetchResult::Yield:
                    return EFetchResult::Yield;
                case EFetchResult::Finish:
                    break;
                case EFetchResult::One:
                    if (state.IsInvalid()) {
                        state = NUdf::TUnboxedValuePod(false);
                        for (ui32 i = 0U; i < State.size(); ++i)
                            State[i]->SetValue(ctx, InitState[i]->GetValue(ctx));
                    } else {
                        const auto& reset = Switch->GetValue(ctx);
                        if (Interruptable && !reset) {
                            break;
                        }

                        if (reset.template Get<bool>()) {
                            for (const auto state : State) {
                                if (const auto out = *output++) {
                                    *out = state->GetValue(ctx);
                                }
                            }

                            state = NUdf::TUnboxedValuePod(true);
                            return EFetchResult::One;
                        }

                        for (ui32 i = 0U; i < State.size(); ++i)
                            ctx.MutableValues[TempStateIndex + i] = UpdateState[i]->GetValue(ctx);
                        for (ui32 i = 0U; i < State.size(); ++i)
                            State[i]->SetValue(ctx, std::move(ctx.MutableValues[TempStateIndex + i]));
                    }
                    continue;
            }
            break;
        }

        const bool empty = state.IsInvalid();
        state = NUdf::TUnboxedValuePod::MakeFinish();
        if (empty)
            return EFetchResult::Finish;

        for (const auto state : State) {
            if (const auto out = *output++) {
                *out = state->GetValue(ctx);
            }
        }

        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 4U, "result", exit);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Finish)), block);

        const auto empty = PHINode::Create(Type::getInt1Ty(context), 3U, "empty", work);
        const auto bit = CastInst::Create(Instruction::Trunc, state, Type::getInt1Ty(context), "bit", block);
        empty->addIncoming(bit, block);

        const auto choise = SwitchInst::Create(state, work, 2U, block);
        choise->addCase(GetFinish(context), exit);
        choise->addCase(GetTrue(context), init);

        block = init;

        if constexpr (UseCtx) {
            const auto cleanup = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&CleanupCurrentContext));
            const auto cleanupType = FunctionType::get(Type::getVoidTy(context), {}, false);
            const auto cleanupPtr = CastInst::Create(Instruction::IntToPtr, cleanup, PointerType::getUnqual(cleanupType), "cleanup_ctx", block);
            CallInst::Create(cleanupType, cleanupPtr, {}, "", block);
        }

        new StoreInst(GetFalse(context), statePtr, block);

        for (ui32 i = 0U; i < State.size(); ++i) {
            EnsureDynamicCast<ICodegeneratorExternalNode*>(State[i])->CreateSetValue(ctx, block, GetNodeValue(InitState[i], ctx, block));
        }

        empty->addIncoming(ConstantInt::getFalse(context), block);
        BranchInst::Create(work, block);

        block = work;
        const auto getres = GetNodeValues(Flow, ctx, block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Yield)), block);

        const auto action = SwitchInst::Create(getres.first, good, 2U, block);
        action->addCase(ConstantInt::get(resultType, i32(EFetchResult::Finish)), stop);
        action->addCase(ConstantInt::get(resultType, i32(EFetchResult::Yield)), exit);

        block = good;

        std::vector<Value*> items(Items.size(), nullptr);
        for (ui32 i = 0U; i < items.size(); ++i) {
            if (Items[i]->GetDependencesCount() > 0U || ItemsOnInit[i])
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, items[i] = getres.second[i](ctx, block));
            else if (ItemsOnUpdate[i] || SwitchItem && i == *SwitchItem)
                items[i] = getres.second[i](ctx, block);
        }

        BranchInst::Create(init, next, empty, block);

        block = next;

        const auto swap = BasicBlock::Create(context, "swap", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

        const auto reset = SwitchItem ? items[*SwitchItem] : GetNodeValue(Switch, ctx, block);

        if constexpr (Interruptable) {
            const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
            BranchInst::Create(stop, next, IsEmpty(reset, block), block);
            block = pass;
        }

        const auto cast = CastInst::Create(Instruction::Trunc, reset, Type::getInt1Ty(context), "bool", block);
        BranchInst::Create(swap, skip, cast, block);

        block = swap;

        new StoreInst(GetTrue(context), statePtr, block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::One)), block);
        BranchInst::Create(exit, block);

        block = skip;

        std::vector<Value*> updates(State.size(), nullptr);
        for (ui32 i = 0U; i < State.size(); ++i) {
            if (const auto map = UpdateOnItems[i])
                updates[i] = items[*map];
            else if (State[i] != UpdateState[i])
                updates[i] = GetNodeValue(UpdateState[i], ctx, block);
        }

        for (ui32 i = 0U; i < updates.size(); ++i) {
            if (const auto s = updates[i])
                EnsureDynamicCast<ICodegeneratorExternalNode*>(State[i])->CreateSetValue(ctx, block, s);
        }

        empty->addIncoming(ConstantInt::getFalse(context), block);
        BranchInst::Create(work, block);

        block = stop;
        new StoreInst(GetFinish(context), statePtr, block);
        const auto select = SelectInst::Create(empty, ConstantInt::get(resultType, i32(EFetchResult::Finish)), ConstantInt::get(resultType, i32(EFetchResult::One)), "output", block);
        result->addIncoming(select, block);
        BranchInst::Create(exit, block);

        block = exit;

        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(State.size());
        std::transform(State.cbegin(), State.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block){ return GetNodeValue(node, ctx, block); };
        });
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TWideCondense1Wrapper::Own, flow, std::placeholders::_1));
            std::for_each(InitState.cbegin(), InitState.cend(), std::bind(&TWideCondense1Wrapper::DependsOn, flow, std::placeholders::_1));
            std::for_each(State.cbegin(), State.cend(), std::bind(&TWideCondense1Wrapper::Own, flow, std::placeholders::_1));
            TWideCondense1Wrapper::DependsOn(flow, Switch);
            std::for_each(UpdateState.cbegin(), UpdateState.cend(), std::bind(&TWideCondense1Wrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow;
    const TComputationExternalNodePtrVector Items;
    const TComputationNodePtrVector InitState;
    const TComputationExternalNodePtrVector State;
    IComputationNode* const Switch;
    const TComputationNodePtrVector UpdateState;

    const std::optional<size_t> SwitchItem;

    const TPasstroughtMap ItemsOnInit, ItemsOnUpdate, UpdateOnItems;

    ui32 WideFieldsIndex;
    ui32 TempStateIndex;
};

}

IComputationNode* WrapWideCondense1(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 2U, "Expected at least two args.");

    const auto inputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    const auto outputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);

    TComputationNodePtrVector initState, updateState;
    initState.reserve(outputWidth);
    updateState.reserve(outputWidth);

    ui32 index = inputWidth;

    std::generate_n(std::back_inserter(initState), outputWidth, [&](){ return LocateNode(ctx.NodeLocator, callable, ++index); } );

    index += outputWidth;

    const auto outSwitch = LocateNode(ctx.NodeLocator, callable, ++index);

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(index), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool.");

    std::generate_n(std::back_inserter(updateState), outputWidth, [&](){ return LocateNode(ctx.NodeLocator, callable, ++index); } );

    TComputationExternalNodePtrVector items, state;
    items.reserve(inputWidth);
    state.reserve(outputWidth);

    index = 0U;

    std::generate_n(std::back_inserter(items), inputWidth, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); } );

    index += outputWidth;

    std::generate_n(std::back_inserter(state), outputWidth, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); } );

    index = 2 + inputWidth + 3 * outputWidth;
    bool useCtx = false;
    if (index < callable.GetInputsCount()) {
        useCtx = AS_VALUE(TDataLiteral, callable.GetInput(index))->AsValue().Get<bool>();
        ++index;
    }

    if (useCtx) {
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
            if (isOptional) {
                return new TWideCondense1Wrapper<true, true>(ctx.Mutables, wide, std::move(items), std::move(initState), std::move(state), outSwitch, std::move(updateState));
            } else {
                return new TWideCondense1Wrapper<false, true>(ctx.Mutables, wide, std::move(items), std::move(initState), std::move(state), outSwitch, std::move(updateState));
            }
        }
    } else {
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
            if (isOptional) {
                return new TWideCondense1Wrapper<true, false>(ctx.Mutables, wide, std::move(items), std::move(initState), std::move(state), outSwitch, std::move(updateState));
            } else {
                return new TWideCondense1Wrapper<false, false>(ctx.Mutables, wide, std::move(items), std::move(initState), std::move(state), outSwitch, std::move(updateState));
            }
        }
    }

    THROW yexception() << "Expected wide flow.";
}

}
}
