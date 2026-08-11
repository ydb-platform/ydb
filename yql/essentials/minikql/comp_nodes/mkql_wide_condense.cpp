#include "mkql_wide_condense.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/utils/cast.h>

namespace NKikimr::NMiniKQL {

#ifndef MKQL_DISABLE_CODEGEN
using NYql::EnsureDynamicCast;
#endif

namespace {

template <bool Interruptable, bool UseCtx>
class TWideCondense1Wrapper: public TStatefulWideFlowCodegeneratorNode<TWideCondense1Wrapper<Interruptable, UseCtx>> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideCondense1Wrapper<Interruptable, UseCtx>>;

public:
    TWideCondense1Wrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow,
                          TComputationExternalNodePtrVector&& items, TComputationNodePtrVector&& initState,
                          TComputationExternalNodePtrVector&& state, IComputationNode* outSwitch, TComputationNodePtrVector&& updateState)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded)
        , Flow_(flow)
        , Items_(std::move(items))
        , InitState_(std::move(initState))
        , State_(std::move(state))
        , Switch_(outSwitch)
        , UpdateState_(std::move(updateState))
        , SwitchItem_(IsPasstrought(Switch_, Items_))
        , ItemsOnInit_(GetPasstroughtMap(Items_, InitState_))
        , ItemsOnUpdate_(GetPasstroughtMap(Items_, UpdateState_))
        , UpdateOnItems_(GetPasstroughtMap(UpdateState_, Items_))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(Items_.size()))
        , TempStateIndex_(std::exchange(mutables.CurValueIndex, mutables.CurValueIndex + State_.size()))
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (state.IsFinish()) {
            return EFetchResult::Finish;
        } else if (state.HasValue() && state.Get<bool>()) {
            if constexpr (UseCtx) {
                CleanupCurrentContext();
            }

            state = NUdf::TUnboxedValuePod(false);
            for (ui32 i = 0U; i < State_.size(); ++i) {
                State_[i]->SetValue(ctx, InitState_[i]->GetValue(ctx));
            }
        }

        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

        while (true) {
            for (auto i = 0U; i < Items_.size(); ++i) {
                if (Items_[i]->GetDependentsCount() > 0U || ItemsOnInit_[i] || ItemsOnUpdate_[i] || SwitchItem_ && i == *SwitchItem_) {
                    fields[i] = &Items_[i]->RefValue(ctx);
                }
            }

            switch (Flow_->FetchValues(ctx, fields)) {
                case EFetchResult::Yield:
                    return EFetchResult::Yield;
                case EFetchResult::Finish:
                    break;
                case EFetchResult::One:
                    if (state.IsInvalid()) {
                        state = NUdf::TUnboxedValuePod(false);
                        for (ui32 i = 0U; i < State_.size(); ++i) {
                            State_[i]->SetValue(ctx, InitState_[i]->GetValue(ctx));
                        }
                    } else {
                        const auto& reset = Switch_->GetValue(ctx);
                        if (Interruptable && !reset) {
                            break;
                        }

                        if (reset.template Get<bool>()) {
                            for (const auto state : State_) {
                                if (const auto out = *output++) {
                                    *out = state->GetValue(ctx);
                                }
                            }

                            state = NUdf::TUnboxedValuePod(true);
                            return EFetchResult::One;
                        }

                        for (ui32 i = 0U; i < State_.size(); ++i) {
                            ctx.MutableValues[TempStateIndex_ + i] = UpdateState_[i]->GetValue(ctx);
                        }
                        for (ui32 i = 0U; i < State_.size(); ++i) {
                            State_[i]->SetValue(ctx, std::move(ctx.MutableValues[TempStateIndex_ + i]));
                        }
                    }
                    continue;
            }
            break;
        }

        const bool empty = state.IsInvalid();
        state = NUdf::TUnboxedValuePod::MakeFinish();
        if (empty) {
            return EFetchResult::Finish;
        }

        for (const auto state : State_) {
            if (const auto out = *output++) {
                *out = state->GetValue(ctx);
            }
        }

        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
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
            EmitFunctionCall<&CleanupCurrentContext>(Type::getVoidTy(context), {}, ctx, block);
        }

        new StoreInst(GetFalse(context), statePtr, block);

        for (ui32 i = 0U; i < State_.size(); ++i) {
            EnsureDynamicCast<ICodegeneratorExternalNode*>(State_[i])->CreateSetValue(ctx, block, GetNodeValue(InitState_[i], ctx, block));
        }

        empty->addIncoming(ConstantInt::getFalse(context), block);
        BranchInst::Create(work, block);

        block = work;
        const auto getres = GetNodeValues(Flow_, ctx, block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Yield)), block);

        const auto action = SwitchInst::Create(getres.first, good, 2U, block);
        action->addCase(ConstantInt::get(resultType, i32(EFetchResult::Finish)), stop);
        action->addCase(ConstantInt::get(resultType, i32(EFetchResult::Yield)), exit);

        block = good;

        std::vector<Value*> items(Items_.size(), nullptr);
        for (ui32 i = 0U; i < items.size(); ++i) {
            if (Items_[i]->GetDependentsCount() > 0U || ItemsOnInit_[i]) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Items_[i])->CreateSetValue(ctx, block, items[i] = getres.second[i](ctx, block));
            } else if (ItemsOnUpdate_[i] || SwitchItem_ && i == *SwitchItem_) {
                items[i] = getres.second[i](ctx, block);
            }
        }

        BranchInst::Create(init, next, empty, block);

        block = next;

        const auto swap = BasicBlock::Create(context, "swap", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

        const auto reset = SwitchItem_ ? items[*SwitchItem_] : GetNodeValue(Switch_, ctx, block);

        if constexpr (Interruptable) {
            const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
            BranchInst::Create(stop, next, IsEmpty(reset, block, context), block);
            block = pass;
        }

        const auto cast = CastInst::Create(Instruction::Trunc, reset, Type::getInt1Ty(context), "bool", block);
        BranchInst::Create(swap, skip, cast, block);

        block = swap;

        new StoreInst(GetTrue(context), statePtr, block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::One)), block);
        BranchInst::Create(exit, block);

        block = skip;

        std::vector<Value*> updates(State_.size(), nullptr);
        for (ui32 i = 0U; i < State_.size(); ++i) {
            if (const auto map = UpdateOnItems_[i]) {
                updates[i] = items[*map];
            } else if (State_[i] != UpdateState_[i]) {
                updates[i] = GetNodeValue(UpdateState_[i], ctx, block);
            }
        }

        for (ui32 i = 0U; i < updates.size(); ++i) {
            if (const auto s = updates[i]) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(State_[i])->CreateSetValue(ctx, block, s);
            }
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
        getters.reserve(State_.size());
        std::transform(State_.cbegin(), State_.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block) { return GetNodeValue(node, ctx, block); };
        });
        return {result, std::move(getters)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            std::for_each(Items_.cbegin(), Items_.cend(), std::bind(&TWideCondense1Wrapper::Own, flow, std::placeholders::_1));
            std::for_each(InitState_.cbegin(), InitState_.cend(), std::bind(&TWideCondense1Wrapper::DependsOn, flow, std::placeholders::_1));
            std::for_each(State_.cbegin(), State_.cend(), std::bind(&TWideCondense1Wrapper::Own, flow, std::placeholders::_1));
            TWideCondense1Wrapper::DependsOn(flow, Switch_);
            std::for_each(UpdateState_.cbegin(), UpdateState_.cend(), std::bind(&TWideCondense1Wrapper::DependsOn, flow, std::placeholders::_1));
        }
    }

    IComputationWideFlowNode* const Flow_;
    const TComputationExternalNodePtrVector Items_;
    const TComputationNodePtrVector InitState_;
    const TComputationExternalNodePtrVector State_;
    IComputationNode* const Switch_;
    const TComputationNodePtrVector UpdateState_;

    const std::optional<size_t> SwitchItem_;

    const TPasstroughtMap ItemsOnInit_, ItemsOnUpdate_, UpdateOnItems_;

    ui32 WideFieldsIndex_;
    ui32 TempStateIndex_;
};

} // namespace

IComputationNode* WrapWideCondense1(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 2U, "Expected at least two args.");

    const auto inputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    const auto outputWidth = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetType()->GetReturnType()));

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);

    TComputationNodePtrVector initState;
    TComputationNodePtrVector updateState;
    initState.reserve(outputWidth);
    updateState.reserve(outputWidth);

    ui32 index = inputWidth;

    std::generate_n(std::back_inserter(initState), outputWidth, [&]() { return LocateNode(ctx.NodeLocator, callable, ++index); });

    index += outputWidth;

    const auto outSwitch = LocateNode(ctx.NodeLocator, callable, ++index);

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(index), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool.");

    std::generate_n(std::back_inserter(updateState), outputWidth, [&]() { return LocateNode(ctx.NodeLocator, callable, ++index); });

    TComputationExternalNodePtrVector items;
    TComputationExternalNodePtrVector state;
    items.reserve(inputWidth);
    state.reserve(outputWidth);

    index = 0U;

    std::generate_n(std::back_inserter(items), inputWidth, [&]() { return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

    index += outputWidth;

    std::generate_n(std::back_inserter(state), outputWidth, [&]() { return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

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

} // namespace NKikimr::NMiniKQL
