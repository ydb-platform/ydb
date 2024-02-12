#include "mkql_chopper.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/utils/cast.h>

namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

using namespace std::placeholders;

class TWideChopperWrapper : public TStatefulWideFlowCodegeneratorNode<TWideChopperWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWideChopperWrapper>;
public:
    enum class EState : ui64 {
        Work,
        Chop,
        Next,
        Skip
    };

    TWideChopperWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow,  TComputationExternalNodePtrVector&& itemArgs, TComputationNodePtrVector&& keys, TComputationExternalNodePtrVector&& keyArgs, IComputationNode* chop, IComputationWideFlowProxyNode* input, IComputationWideFlowNode* output)
        : TBaseComputation(mutables, flow, EValueRepresentation::Any)
        , Flow(flow)
        , ItemArgs(std::move(itemArgs))
        , Keys(std::move(keys))
        , KeyArgs(std::move(keyArgs))
        , Chop(chop)
        , Input(input)
        , Output(output)
        , ItemsOnKeys(GetPasstroughtMap(ItemArgs, Keys))
        , KeysOnItems(GetPasstroughtMap(Keys, ItemArgs))
        , SwitchItem(IsPasstrought(Chop, ItemArgs))
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(ItemArgs.size()))
    {
        Input->SetFetcher(std::bind(&TWideChopperWrapper::DoCalculateInput, this, std::bind(&TWideChopperWrapper::RefState, this, _1), _1, _2));
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        if (state.IsInvalid()) {
            for (auto i = 0U; i < ItemArgs.size(); ++i)
                fields[i] = &ItemArgs[i]->RefValue(ctx);
            if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
                return result;

            for (ui32 i = 0U; i < Keys.size(); ++i)
                if (KeyArgs[i]->GetDependencesCount() > 0U)
                    KeyArgs[i]->SetValue(ctx, Keys[i]->GetValue(ctx));
            state = NUdf::TUnboxedValuePod(ui64(EState::Next));
        } else if (EState::Skip == EState(state.Get<ui64>())) {
            do {
                for (auto i = 0U; i < ItemArgs.size(); ++i)
                    fields[i] = &ItemArgs[i]->RefValue(ctx);
                if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
                    return result;

            } while (!Chop->GetValue(ctx).Get<bool>());

            for (ui32 i = 0U; i < Keys.size(); ++i)
                if (KeyArgs[i]->GetDependencesCount() > 0U)
                    KeyArgs[i]->SetValue(ctx, Keys[i]->GetValue(ctx));
            state = NUdf::TUnboxedValuePod(ui64(EState::Next));
        }

        while (true) {
            if (const auto result = Output->FetchValues(ctx, output); EFetchResult::Finish == result) {
                Input->InvalidateValue(ctx);
                switch (EState(state.Get<ui64>())) {
                    case EState::Work:
                    case EState::Next:
                        do {
                            for (auto i = 0U; i < ItemArgs.size(); ++i)
                                fields[i] = &ItemArgs[i]->RefValue(ctx);
                            switch (const auto next = Flow->FetchValues(ctx, fields)) {
                                case EFetchResult::Yield:
                                    state = NUdf::TUnboxedValuePod(ui64(EState::Skip));
                                case EFetchResult::Finish:
                                    return next;
                                case EFetchResult::One:
                                    break;
                            }
                        } while (!Chop->GetValue(ctx).Get<bool>());
                    case EState::Chop:
                        for (ui32 i = 0U; i < Keys.size(); ++i)
                            if (KeyArgs[i]->GetDependencesCount() > 0U)
                                KeyArgs[i]->SetValue(ctx, Keys[i]->GetValue(ctx));
                        state = NUdf::TUnboxedValuePod(ui64(EState::Next));
                    default:
                        continue;
                }
            } else
                return result;
        }
    }
private:
    EFetchResult DoCalculateInput(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (EState::Next == EState(state.Get<ui64>())) {
            state = NUdf::TUnboxedValuePod(ui64(EState::Work));
            for (auto i = 0U; i < ItemArgs.size(); ++i)
                if (const auto out = output[i])
                    *out = ItemArgs[i]->GetValue(ctx);
            return EFetchResult::One;
        }

        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        for (auto i = 0U; i < ItemArgs.size(); ++i)
            fields[i] = &ItemArgs[i]->RefValue(ctx);

        if (const auto result = Flow->FetchValues(ctx, fields); EFetchResult::One != result)
            return result;

        for (auto i = 0U; i < ItemArgs.size(); ++i)
            if (const auto out = output[i])
                *out = *fields[i];

        if (Chop->GetValue(ctx).Get<bool>()) {
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

        auto getres = GetNodeValues(Flow, ctx, block);
        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), 0), "special", block);
        result->addIncoming(getres.first, block);
        BranchInst::Create(done, good, special, block);

        block = good;

        std::vector<Value*> items(ItemArgs.size(), nullptr);
        for (ui32 i = 0U; i < items.size(); ++i) {
            EnsureDynamicCast<ICodegeneratorExternalNode*>(ItemArgs[i])->CreateSetValue(ctx, block, items[i] = getres.second[i](ctx, block));
        }

        const auto chop = SwitchItem ? items[*SwitchItem] : GetNodeValue(Chop, ctx, block);
        const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::One)), block);
        BranchInst::Create(step, done, cast, block);

        block = step;

        new StoreInst(GetConstant(ui64(EState::Chop), context), statePtr, block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Finish)), block);
        BranchInst::Create(done, block);

        block = done;

        ICodegeneratorInlineWideNode::TGettersList getters;
        getters.reserve(ItemArgs.size());
        std::transform(ItemArgs.cbegin(), ItemArgs.cend(), std::back_inserter(getters), [&](IComputationNode* node) {
            return [node](const TCodegenContext& ctx, BasicBlock*& block){ return GetNodeValue(node, ctx, block); };
        });
        return {result, std::move(getters)};
    }
public:
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        EnsureDynamicCast<IWideFlowProxyCodegeneratorNode*>(Input)->SetGenerator(std::bind(&TWideChopperWrapper::DoGenGetValuesInput, this, _1, _2));

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

            const auto getfirst = GetNodeValues(Flow, ctx, block);
            const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getfirst.first, ConstantInt::get(getfirst.first->getType(), 0), "special", block);
            result->addIncoming(getfirst.first, block);
            BranchInst::Create(exit, next, special, block);

            block = next;

            new StoreInst(GetConstant(ui64(EState::Next), context), statePtr, block);

            std::vector<Value*> items(ItemArgs.size(), nullptr);
            for (ui32 i = 0U; i < items.size(); ++i) {
                EnsureDynamicCast<ICodegeneratorExternalNode*>(ItemArgs[i])->CreateSetValue(ctx, block, items[i] = getfirst.second[i](ctx, block));
            }

            for (ui32 i = 0U; i < Keys.size(); ++i) {
                if (KeyArgs[i]->GetDependencesCount() > 0U) {
                    const auto map = KeysOnItems[i];
                    const auto key = map ? items[*map] : GetNodeValue(Keys[i], ctx, block);
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(KeyArgs[i])->CreateSetValue(ctx, block, key);
                }
            }

            BranchInst::Create(loop, block);
        }

        const auto part = BasicBlock::Create(context, "part", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto step = BasicBlock::Create(context, "step", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

        block = loop;

        auto getres = GetNodeValues(Output, ctx, block);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, getres.first, ConstantInt::get(getres.first->getType(), 0), "finish", block);
        result->addIncoming(getres.first, block);
        BranchInst::Create(part, exit, finish, block);

        block = part;

        EnsureDynamicCast<IWideFlowProxyCodegeneratorNode*>(Input)->CreateInvalidate(ctx, block);

        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Finish)), block);

        const auto choise = SwitchInst::Create(state, exit, 3U, block);
        choise->addCase(GetConstant(ui64(EState::Next), context), pass);
        choise->addCase(GetConstant(ui64(EState::Work), context), pass);
        choise->addCase(GetConstant(ui64(EState::Chop), context), step);

        block = pass;

        const auto getnext = GetNodeValues(Flow, ctx, block);

        result->addIncoming(getnext.first, block);

        const auto way = SwitchInst::Create(getnext.first, good, 2U, block);
        way->addCase(ConstantInt::get(resultType, i32(EFetchResult::Finish)), exit);
        way->addCase(ConstantInt::get(resultType, i32(EFetchResult::Yield)), skip);

        block = good;

        std::vector<Value*> items(ItemArgs.size(), nullptr);
        for (ui32 i = 0U; i < items.size(); ++i) {
            EnsureDynamicCast<ICodegeneratorExternalNode*>(ItemArgs[i])->CreateSetValue(ctx, block, items[i] = getnext.second[i](ctx, block));
        }

        const auto chop = SwitchItem ? items[*SwitchItem] : GetNodeValue(Chop, ctx, block);
        const auto cast = CastInst::Create(Instruction::Trunc, chop, Type::getInt1Ty(context), "bool", block);

        BranchInst::Create(step, pass, cast, block);

        block = step;

        new StoreInst(GetConstant(ui64(EState::Next), context), statePtr, block);

        for (ui32 i = 0U; i < Keys.size(); ++i) {
            if (KeyArgs[i]->GetDependencesCount() > 0U) {
                const auto key = GetNodeValue(Keys[i], ctx, block);
                EnsureDynamicCast<ICodegeneratorExternalNode*>(KeyArgs[i])->CreateSetValue(ctx, block, key);
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
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            std::for_each(ItemArgs.cbegin(), ItemArgs.cend(), std::bind(&TWideChopperWrapper::Own, flow, std::placeholders::_1));
            std::for_each(Keys.cbegin(), Keys.cend(), std::bind(&TWideChopperWrapper::DependsOn, flow, std::placeholders::_1));
            std::for_each(KeyArgs.cbegin(), KeyArgs.cend(), std::bind(&TWideChopperWrapper::Own, flow, std::placeholders::_1));
            OwnProxy(flow, Input);
            DependsOn(flow, Output);
        }
    }

    IComputationWideFlowNode *const Flow;

    const TComputationExternalNodePtrVector ItemArgs;
    const TComputationNodePtrVector Keys;
    const TComputationExternalNodePtrVector KeyArgs;

    IComputationNode *const Chop;

    IComputationWideFlowProxyNode *const Input;
    IComputationWideFlowNode *const Output;

    const TPasstroughtMap ItemsOnKeys, KeysOnItems;

    const std::optional<size_t> SwitchItem;

    const ui32 WideFieldsIndex;
};

}

IComputationNode* WrapWideChopper(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 4U, "Expected at least four args.");

    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    const ui32 width = wideComponents.size();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    const auto keysSize = (callable.GetInputsCount() - width - 4U) >> 1U;

    TComputationNodePtrVector keys;
    keys.reserve(keysSize);
    auto index = width;
    std::generate_n(std::back_inserter(keys), keysSize, [&](){ return LocateNode(ctx.NodeLocator, callable, ++index); } );

    index += keysSize;

    const auto switchResult = LocateNode(ctx.NodeLocator, callable, ++index);
    const auto input = LocateNode(ctx.NodeLocator, callable, ++index, true);
    const auto output = LocateNode(ctx.NodeLocator, callable, ++index, true);

    TComputationExternalNodePtrVector itemArgs, keyArgs;
    itemArgs.reserve(width);
    index = 0U;
    std::generate_n(std::back_inserter(itemArgs), width, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); } );

    index += keysSize;
    keyArgs.reserve(keysSize);
    std::generate_n(std::back_inserter(keyArgs), keysSize, [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); } );

    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        return new TWideChopperWrapper(ctx.Mutables, wide, std::move(itemArgs), std::move(keys), std::move(keyArgs), switchResult,
                                       EnsureDynamicCast<IComputationWideFlowProxyNode*>(input),
                                       EnsureDynamicCast<IComputationWideFlowNode*>(output));
    }

    THROW yexception() << "Expected wide flow.";
}

}
}
