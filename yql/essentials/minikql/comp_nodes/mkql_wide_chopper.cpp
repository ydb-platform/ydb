#include "mkql_chopper.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/utils/cast.h>

namespace NKikimr::NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

using namespace std::placeholders;

class TWideChopperWrapper: public TStatefulWideFlowCodegeneratorNode<TWideChopperWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideChopperWrapper>;

public:
    enum class EState: ui64 {
        Work,
        Chop,
        Next,
        Skip
    };

    TWideChopperWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& itemArgs, TComputationNodePtrVector&& keys, TComputationExternalNodePtrVector&& keyArgs, IComputationNode* chop, IComputationWideFlowProxyNode* input, IComputationWideFlowNode* output)
        : TBaseComputation(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , ItemArgs_(std::move(itemArgs))
        , Keys_(std::move(keys))
        , KeyArgs_(std::move(keyArgs))
        , Chop_(chop)
        , Input_(input)
        , Output_(output)
        , ItemsOnKeys_(GetPasstroughtMap(ItemArgs_, Keys_))
        , KeysOnItems_(GetPasstroughtMap(Keys_, ItemArgs_))
        , SwitchItem_(IsPasstrought(Chop_, ItemArgs_))
        , WideFieldsIndex_(mutables.IncrementWideFieldsIndex(ItemArgs_.size()))
    {
        Input_->SetFetcher(std::bind(&TWideChopperWrapper::DoCalculateInput, this, std::bind(&TWideChopperWrapper::RefState, this, _1), _1, _2));
#ifndef MKQL_DISABLE_CODEGEN
        EnsureDynamicCast<IWideFlowProxyCodegeneratorNode*>(Input_)->SetGenerator(std::bind(&TWideChopperWrapper::DoGenGetValuesInput, this, _1, _2));
#endif
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

        if (state.IsInvalid()) {
            for (auto i = 0U; i < ItemArgs_.size(); ++i) {
                fields[i] = &ItemArgs_[i]->RefValue(ctx);
            }
            if (const auto result = Flow_->FetchValues(ctx, fields); EFetchResult::One != result) {
                return result;
            }

            for (ui32 i = 0U; i < Keys_.size(); ++i) {
                if (KeyArgs_[i]->GetDependentsCount() > 0U) {
                    KeyArgs_[i]->SetValue(ctx, Keys_[i]->GetValue(ctx));
                }
            }
            state = NUdf::TUnboxedValuePod(ui64(EState::Next));
        } else if (EState::Skip == EState(state.Get<ui64>())) {
            do {
                for (auto i = 0U; i < ItemArgs_.size(); ++i) {
                    fields[i] = &ItemArgs_[i]->RefValue(ctx);
                }
                if (const auto result = Flow_->FetchValues(ctx, fields); EFetchResult::One != result) {
                    return result;
                }

            } while (!Chop_->GetValue(ctx).Get<bool>());

            for (ui32 i = 0U; i < Keys_.size(); ++i) {
                if (KeyArgs_[i]->GetDependentsCount() > 0U) {
                    KeyArgs_[i]->SetValue(ctx, Keys_[i]->GetValue(ctx));
                }
            }
            state = NUdf::TUnboxedValuePod(ui64(EState::Next));
        }

        while (true) {
            if (const auto result = Output_->FetchValues(ctx, output); EFetchResult::Finish == result) {
                Input_->InvalidateValue(ctx);
                switch (EState(state.Get<ui64>())) {
                    case EState::Work:
                    case EState::Next:
                        do {
                            for (auto i = 0U; i < ItemArgs_.size(); ++i) {
                                fields[i] = &ItemArgs_[i]->RefValue(ctx);
                            }
                            switch (const auto next = Flow_->FetchValues(ctx, fields)) {
                                case EFetchResult::Yield:
                                    state = NUdf::TUnboxedValuePod(ui64(EState::Skip));
                                case EFetchResult::Finish:
                                    return next;
                                case EFetchResult::One:
                                    break;
                            }
                        } while (!Chop_->GetValue(ctx).Get<bool>());
                    case EState::Chop:
                        for (ui32 i = 0U; i < Keys_.size(); ++i) {
                            if (KeyArgs_[i]->GetDependentsCount() > 0U) {
                                KeyArgs_[i]->SetValue(ctx, Keys_[i]->GetValue(ctx));
                            }
                        }
                        state = NUdf::TUnboxedValuePod(ui64(EState::Next));
                    default:
                        continue;
                }
            } else {
                return result;
            }
        }
    }

private:
    EFetchResult DoCalculateInput(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (EState::Next == EState(state.Get<ui64>())) {
            state = NUdf::TUnboxedValuePod(ui64(EState::Work));
            for (auto i = 0U; i < ItemArgs_.size(); ++i) {
                if (const auto out = output[i]) {
                    *out = ItemArgs_[i]->GetValue(ctx);
                }
            }
            return EFetchResult::One;
        }

        auto** fields = ctx.WideFields.data() + WideFieldsIndex_;

        for (auto i = 0U; i < ItemArgs_.size(); ++i) {
            fields[i] = &ItemArgs_[i]->RefValue(ctx);
        }

        if (const auto result = Flow_->FetchValues(ctx, fields); EFetchResult::One != result) {
            return result;
        }

        for (auto i = 0U; i < ItemArgs_.size(); ++i) {
            if (const auto out = output[i]) {
                *out = *fields[i];
            }
        }

        if (Chop_->GetValue(ctx).Get<bool>()) {
            state = NUdf::TUnboxedValuePod(ui64(EState::Chop));
            return EFetchResult::Finish;
        }

        return EFetchResult::One;
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValuesInput(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto load = BasicBlock::Create(context, "load", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 4U, "result", done);

        const auto valueType = Type::getInt128Ty(context);
        const auto statePtr = GetElementPtrInst::CreateInBounds(valueType, ctx.GetMutables(), {ConstantInt::get(Type::getInt32Ty(context), static_cast<const IComputationNode*>(this)->GetIndex())}, "state_ptr", block);
        const auto entry = new LoadInst(valueType, statePtr, "entry", block);
        const auto next = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, entry, GetConstant(ui64(EState::Next), context), "next", block);

        BranchInst::Create(load, work, next, block);

        block = load;

        new StoreInst(GetConstant(ui64(EState::Work), context), statePtr, block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::One)), block);

        BranchInst::Create(done, block);

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto step = BasicBlock::Create(context, "step", ctx.Func);

        block = work;

        auto getres = GetNodeValues(Flow_, ctx, block);
        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), 0), "special", block);
        result->addIncoming(getres.first, block);
        BranchInst::Create(done, good, special, block);

        block = good;

        std::vector<Value*> items(ItemArgs_.size(), nullptr);
        for (ui32 i = 0U; i < items.size(); ++i) {
            EnsureDynamicCast<ICodegeneratorExternalNode*>(ItemArgs_[i])->CreateSetValue(ctx, block, items[i] = getres.second[i](ctx, block));
        }

        const auto chop = SwitchItem_ ? items[*SwitchItem_] : GetNodeValue(Chop_, ctx, block);
        const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::One)), block);
        BranchInst::Create(step, done, cast, block);

        block = step;

        new StoreInst(GetConstant(ui64(EState::Chop), context), statePtr, block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Finish)), block);
        BranchInst::Create(done, block);

        block = done;

        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(ItemArgs_.size());
        std::transform(ItemArgs_.cbegin(), ItemArgs_.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block) { return GetNodeValue(node, ctx, block); };
        });
        return {result, std::move(getters)};
    }

public:
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);

        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 5U, "result", exit);

        const auto valueType = Type::getInt128Ty(context);
        const auto first = new LoadInst(valueType, statePtr, "first", block);
        const auto enter = SwitchInst::Create(first, loop, 2U, block);
        enter->addCase(GetInvalid(context), init);
        enter->addCase(GetConstant(ui64(EState::Skip), context), pass);

        {
            const auto next = BasicBlock::Create(context, "next", ctx.Func);

            block = init;

            const auto getfirst = GetNodeValues(Flow_, ctx, block);
            const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getfirst.first, ConstantInt::get(getfirst.first->getType(), 0), "special", block);
            result->addIncoming(getfirst.first, block);
            BranchInst::Create(exit, next, special, block);

            block = next;

            new StoreInst(GetConstant(ui64(EState::Next), context), statePtr, block);

            std::vector<Value*> items(ItemArgs_.size(), nullptr);
            for (ui32 i = 0U; i < items.size(); ++i) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(ItemArgs_[i])->CreateSetValue(ctx, block, items[i] = getfirst.second[i](ctx, block));
            }

            for (ui32 i = 0U; i < Keys_.size(); ++i) {
                if (KeyArgs_[i]->GetDependentsCount() > 0U) {
                    const auto map = KeysOnItems_[i];
                    const auto key = map ? items[*map] : GetNodeValue(Keys_[i], ctx, block);
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(KeyArgs_[i])->CreateSetValue(ctx, block, key);
                }
            }

            BranchInst::Create(loop, block);
        }

        const auto part = BasicBlock::Create(context, "part", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto step = BasicBlock::Create(context, "step", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

        block = loop;

        auto getres = GetNodeValues(Output_, ctx, block);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, getres.first, ConstantInt::get(getres.first->getType(), 0), "finish", block);
        result->addIncoming(getres.first, block);
        BranchInst::Create(part, exit, finish, block);

        block = part;

        EnsureDynamicCast<IWideFlowProxyCodegeneratorNode*>(Input_)->CreateInvalidate(ctx, block);

        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Finish)), block);

        const auto choise = SwitchInst::Create(state, exit, 3U, block);
        choise->addCase(GetConstant(ui64(EState::Next), context), pass);
        choise->addCase(GetConstant(ui64(EState::Work), context), pass);
        choise->addCase(GetConstant(ui64(EState::Chop), context), step);

        block = pass;

        const auto getnext = GetNodeValues(Flow_, ctx, block);

        result->addIncoming(getnext.first, block);

        const auto way = SwitchInst::Create(getnext.first, good, 2U, block);
        way->addCase(ConstantInt::get(resultType, i32(EFetchResult::Finish)), exit);
        way->addCase(ConstantInt::get(resultType, i32(EFetchResult::Yield)), skip);

        block = good;

        std::vector<Value*> items(ItemArgs_.size(), nullptr);
        for (ui32 i = 0U; i < items.size(); ++i) {
            EnsureDynamicCast<ICodegeneratorExternalNode*>(ItemArgs_[i])->CreateSetValue(ctx, block, items[i] = getnext.second[i](ctx, block));
        }

        const auto chop = SwitchItem_ ? items[*SwitchItem_] : GetNodeValue(Chop_, ctx, block);
        const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);

        BranchInst::Create(step, pass, cast, block);

        block = step;

        new StoreInst(GetConstant(ui64(EState::Next), context), statePtr, block);

        for (ui32 i = 0U; i < Keys_.size(); ++i) {
            if (KeyArgs_[i]->GetDependentsCount() > 0U) {
                const auto key = GetNodeValue(Keys_[i], ctx, block);
                EnsureDynamicCast<ICodegeneratorExternalNode*>(KeyArgs_[i])->CreateSetValue(ctx, block, key);
            }
        }

        BranchInst::Create(loop, block);

        block = skip;
        new StoreInst(GetConstant(ui64(EState::Skip), context), statePtr, block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Yield)), block);
        BranchInst::Create(exit, block);

        block = exit;
        return {result, std::move(getres.second)};
    }
#endif
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow_)) {
            std::for_each(ItemArgs_.cbegin(), ItemArgs_.cend(), std::bind(&TWideChopperWrapper::Own, flow, std::placeholders::_1));
            std::for_each(Keys_.cbegin(), Keys_.cend(), std::bind(&TWideChopperWrapper::DependsOn, flow, std::placeholders::_1));
            std::for_each(KeyArgs_.cbegin(), KeyArgs_.cend(), std::bind(&TWideChopperWrapper::Own, flow, std::placeholders::_1));
            OwnProxy(flow, Input_);
            DependsOn(flow, Output_);
        }
    }

    // NOLINTNEXTLINE(readability-redundant-access-specifiers)
private:
    IComputationWideFlowNode* const Flow_;

    const TComputationExternalNodePtrVector ItemArgs_;
    const TComputationNodePtrVector Keys_;
    const TComputationExternalNodePtrVector KeyArgs_;

    IComputationNode* const Chop_;

    IComputationWideFlowProxyNode* const Input_;
    IComputationWideFlowNode* const Output_;

    const TPasstroughtMap ItemsOnKeys_, KeysOnItems_;

    const std::optional<size_t> SwitchItem_;

    const ui32 WideFieldsIndex_;
};

} // namespace

IComputationNode* WrapWideChopper(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 4U, "Expected at least four args.");

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    const ui32 width = wideComponents.size();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    const auto keysSize = (callable.GetInputsCount() - width - 4U) >> 1U;

    TComputationNodePtrVector keys;
    keys.reserve(keysSize);
    auto index = width;
    std::generate_n(std::back_inserter(keys), keysSize, [&]() { return LocateNode(ctx.NodeLocator, callable, ++index); });

    index += keysSize;

    const auto switchResult = LocateNode(ctx.NodeLocator, callable, ++index);
    const auto input = LocateNode(ctx.NodeLocator, callable, ++index, /*pop=*/true);
    const auto output = LocateNode(ctx.NodeLocator, callable, ++index, /*pop=*/true);

    TComputationExternalNodePtrVector itemArgs;
    TComputationExternalNodePtrVector keyArgs;
    itemArgs.reserve(width);
    index = 0U;
    std::generate_n(std::back_inserter(itemArgs), width, [&]() { return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

    index += keysSize;
    keyArgs.reserve(keysSize);
    std::generate_n(std::back_inserter(keyArgs), keysSize, [&]() { return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        return new TWideChopperWrapper(ctx.Mutables, wide, std::move(itemArgs), std::move(keys), std::move(keyArgs), switchResult,
                                       EnsureDynamicCast<IComputationWideFlowProxyNode*>(input),
                                       EnsureDynamicCast<IComputationWideFlowNode*>(output));
    }

    THROW yexception() << "Expected wide flow.";
}

} // namespace NKikimr::NMiniKQL
