#include "mkql_flatmap.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/utils/cast.h>

namespace NKikimr {
namespace NMiniKQL {

using NYql::EnsureDynamicCast;

namespace {

class TFlowFlatMapFlowWrapper : public TStatefulFlowCodegeneratorNode<TFlowFlatMapFlowWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TFlowFlatMapFlowWrapper>;
public:
     TFlowFlatMapFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationExternalNode* input, IComputationNode* output)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded), Flow(flow), Input(input), Output(output)
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
                return item.Release();
            } else {
                state = NUdf::TUnboxedValuePod();
                Input->SetValue(ctx, std::move(item));
            }
        }

        while (true) {
            if (auto output = Output->GetValue(ctx); output.IsFinish()) {
                if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
                    state = NUdf::TUnboxedValuePod::Invalid();
                    return item.Release();
                } else {
                    state = NUdf::TUnboxedValuePod();
                    Input->SetValue(ctx, std::move(item));
                }
            } else {
                return output.Release();
            }
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input);
        MKQL_ENSURE(codegenInput, "Input must be codegenerator node.");

        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto step = BasicBlock::Create(context, "step", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto result = PHINode::Create(valueType, 2U, "result", exit);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto reset = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, state, GetInvalid(context), "reset", block);

        BranchInst::Create(init, work, reset, block);

        block = init;

        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(item, block);
        BranchInst::Create(exit, next, IsSpecial(item, block), block);

        block = next;

        new StoreInst(GetEmpty(context), statePtr, block);
        codegenInput->CreateSetValue(ctx, block, item);

        BranchInst::Create(work, block);

        block = work;

        const auto output = GetNodeValue(Output, ctx, block);

        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, output, GetFinish(context), "finish", block);
        result->addIncoming(output, block);
        BranchInst::Create(step, exit, finish, block);

        block = step;

        new StoreInst(GetInvalid(context), statePtr, block);
        BranchInst::Create(init, block);

        block = exit;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            Own(flow, Input);
            DependsOn(flow, Output);
        }
        Input->AddDependence(Output->GetSource());
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Input;
    IComputationNode* const Output;
};

class TFlowFlatMapWideWrapper : public TStatefulWideFlowCodegeneratorNode<TFlowFlatMapWideWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TFlowFlatMapWideWrapper>;
public:
     TFlowFlatMapWideWrapper(TComputationMutables& mutables, IComputationNode* flow, IComputationExternalNode* input, IComputationWideFlowNode* output)
        : TBaseComputation(mutables, flow, EValueRepresentation::Embedded), Flow(flow), Input(input), Output(output)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
                return item.IsFinish() ? EFetchResult::Finish : EFetchResult::Yield;
            } else {
                state = NUdf::TUnboxedValuePod();
                Input->SetValue(ctx, std::move(item));
            }
        }

        while (true) {
            if (const auto result = Output->FetchValues(ctx, output); EFetchResult::Finish != result)
                return result;
            else if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
                state = NUdf::TUnboxedValuePod::Invalid();
                return item.IsFinish() ? EFetchResult::Finish : EFetchResult::Yield;
            } else {
                state = NUdf::TUnboxedValuePod();
                Input->SetValue(ctx, std::move(item));
            }
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input);
        MKQL_ENSURE(codegenInput, "Input must be codegenerator node.");

        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto step = BasicBlock::Create(context, "step", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 2U, "result", exit);

        const auto state = new LoadInst(Type::getInt128Ty(context), statePtr, "state", block);
        const auto reset = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, state, GetInvalid(context), "reset", block);

        BranchInst::Create(init, work, reset, block);

        block = init;

        const auto item = GetNodeValue(Flow, ctx, block);
        const auto outres = SelectInst::Create(IsFinish(item, block), ConstantInt::get(resultType, i32(EFetchResult::Finish)), ConstantInt::get(resultType, i32(EFetchResult::Yield)), "outres", block);
        result->addIncoming(outres, block);
        BranchInst::Create(exit, next, IsSpecial(item, block), block);

        block = next;

        new StoreInst(GetEmpty(context), statePtr, block);
        codegenInput->CreateSetValue(ctx, block, item);

        BranchInst::Create(work, block);

        block = work;

        auto output = GetNodeValues(Output, ctx, block);

        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, output.first, ConstantInt::get(resultType, 0), "finish", block);
        result->addIncoming(output.first, block);
        BranchInst::Create(step, exit, finish, block);

        block = step;

        new StoreInst(GetInvalid(context), statePtr, block);
        BranchInst::Create(init, block);

        block = exit;
        return {result, std::move(output.second)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            Own(flow, Input);
            DependsOn(flow, Output);
        }
        Input->AddDependence(Output->GetSource());
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Input;
    IComputationWideFlowNode* const Output;
};

class TListFlatMapFlowWrapper : public TStatefulFlowCodegeneratorNode<TListFlatMapFlowWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TListFlatMapFlowWrapper>;
public:
     TListFlatMapFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* list, IComputationExternalNode* input, IComputationNode* output)
        : TBaseComputation(mutables, output, kind, EValueRepresentation::Boxed), List(list), Input(input), Output(output)
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            state = List->GetValue(ctx).GetListIterator();

            if (!state.Next(Input->RefValue(ctx))) {
                state = NUdf::TUnboxedValuePod::MakeFinish();
            }
        }

        if (state.IsFinish()) {
            return NUdf::TUnboxedValuePod::MakeFinish();
        }

        while (true) {
            if (auto output = Output->GetValue(ctx); output.IsFinish()) {
                if (state.Next(Input->RefValue(ctx))) {
                    continue;
                }

                return state = NUdf::TUnboxedValuePod::MakeFinish();
            } else {
                return output.Release();
            }
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input);
        MKQL_ENSURE(codegenInput, "Input must be codegenerator node.");

        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto result = PHINode::Create(valueType, 3U, "result", exit);
        result->addIncoming(GetFinish(context), block);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto choise = SwitchInst::Create(state, work, 2U, block);
        choise->addCase(GetInvalid(context), init);
        choise->addCase(GetFinish(context), exit);

        block = init;

        const auto list = GetNodeValue(List, ctx, block);
        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(statePtr, list, ctx.Codegen, block);
        if (List->IsTemporaryValue()) {
            CleanupBoxed(list, ctx, block);
        }

        BranchInst::Create(next, block);

        block = next;

        const auto iterator = new LoadInst(valueType, statePtr, "iterator", block);
        const auto itemPtr = codegenInput->CreateRefValue(ctx, block);
        const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), iterator, ctx.Codegen, block, itemPtr);
        BranchInst::Create(work, done, status,  block);

        block = work;

        const auto output = GetNodeValue(Output, ctx, block);
        result->addIncoming(output, block);
        BranchInst::Create(next, exit, IsFinish(output, block), block);

        block = done;

        UnRefBoxed(iterator, ctx, block);
        new StoreInst(GetFinish(context), statePtr, block);
        result->addIncoming(GetFinish(context), block);
        BranchInst::Create(exit, block);

        block = exit;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(List)) {
            Own(flow, Input);
            DependsOn(flow, Output);
        }
        Input->AddDependence(Output->GetSource());
    }

    IComputationNode* const List;
    IComputationExternalNode* const Input;
    IComputationNode* const Output;
};

class TListFlatMapWideWrapper : public TStatefulWideFlowCodegeneratorNode<TListFlatMapWideWrapper> {
using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TListFlatMapWideWrapper>;
public:
     TListFlatMapWideWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* input, IComputationWideFlowNode* output)
        : TBaseComputation(mutables, output, EValueRepresentation::Boxed), List(list), Input(input), Output(output)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (state.IsInvalid()) {
            state = List->GetValue(ctx).GetListIterator();

            if (!state.Next(Input->RefValue(ctx))) {
                state = NUdf::TUnboxedValuePod::MakeFinish();
            }
        }

        if (state.IsFinish()) {
            return EFetchResult::Finish;
        }

        while (true) {
            if (const auto result = Output->FetchValues(ctx, output); EFetchResult::Finish != result)
                return result;
            else if (state.Next(Input->RefValue(ctx)))
                continue;

            state = NUdf::TUnboxedValuePod::MakeFinish();
            return EFetchResult::Finish;
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        const auto codegenInput = dynamic_cast<ICodegeneratorExternalNode*>(Input);
        MKQL_ENSURE(codegenInput, "Input must be codegenerator node.");

        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto resultType = Type::getInt32Ty(context);
        const auto result = PHINode::Create(resultType, 3U, "result", exit);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Finish)), block);

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto choise = SwitchInst::Create(state, work, 2U, block);
        choise->addCase(GetInvalid(context), init);
        choise->addCase(GetFinish(context), exit);

        block = init;

        const auto list = GetNodeValue(List, ctx, block);
        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(statePtr, list, ctx.Codegen, block);
        if (List->IsTemporaryValue()) {
            CleanupBoxed(list, ctx, block);
        }

        BranchInst::Create(next, block);

        block = next;

        const auto iterator = new LoadInst(valueType, statePtr, "iterator", block);
        const auto itemPtr = codegenInput->CreateRefValue(ctx, block);
        const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), iterator, ctx.Codegen, block, itemPtr);
        BranchInst::Create(work, done, status,  block);

        block = work;

        auto output = GetNodeValues(Output, ctx, block);
        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLT, output.first, ConstantInt::get(resultType, 0), "finish", block);
        result->addIncoming(output.first, block);
        BranchInst::Create(next, exit, finish, block);

        block = done;

        UnRefBoxed(iterator, ctx, block);
        new StoreInst(GetFinish(context), statePtr, block);
        result->addIncoming(ConstantInt::get(resultType, i32(EFetchResult::Finish)), block);
        BranchInst::Create(exit, block);

        block = exit;
        return {result, std::move(output.second)};
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(List)) {
            Own(flow, Input);
            DependsOn(flow, Output);
        }
        Input->AddDependence(Output->GetSource());
    }

    IComputationNode* const List;
    IComputationExternalNode* const Input;
    IComputationWideFlowNode* const Output;
};

class TNarrowFlatMapFlowWrapper : public TStatefulFlowCodegeneratorNode<TNarrowFlatMapFlowWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TNarrowFlatMapFlowWrapper>;
public:
     TNarrowFlatMapFlowWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationWideFlowNode* flow, TComputationExternalNodePtrVector&& items, IComputationNode* output)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded)
        , Flow(flow)
        , Items(std::move(items))
        , Output(output)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        if (state.IsInvalid()) {
            for (auto i = 0U; i < Items.size(); ++i)
                if (Items[i]->GetDependencesCount() > 0U)
                    fields[i] = &Items[i]->RefValue(ctx);

            switch (Flow->FetchValues(ctx, fields)) {
                case EFetchResult::Finish:
                    return NUdf::TUnboxedValuePod::MakeFinish();
                case EFetchResult::Yield:
                    return NUdf::TUnboxedValuePod::MakeYield();
                default:
                    state = NUdf::TUnboxedValuePod();
            }
        }

        while (true) {
            if (auto output = Output->GetValue(ctx); output.IsFinish()) {
                for (auto i = 0U; i < Items.size(); ++i)
                    if (Items[i]->GetDependencesCount() > 0U)
                        fields[i] = &Items[i]->RefValue(ctx);

                switch (Flow->FetchValues(ctx, fields)) {
                    case EFetchResult::Finish:
                        return NUdf::TUnboxedValuePod::MakeFinish();
                    case EFetchResult::Yield:
                        return NUdf::TUnboxedValuePod::MakeYield();
                    default:
                        state = NUdf::TUnboxedValuePod();
                }
            } else {
                return output.Release();
            }
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto step = BasicBlock::Create(context, "step", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto result = PHINode::Create(valueType, 2U, "result", exit);

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto reset = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, state, GetInvalid(context), "reset", block);

        BranchInst::Create(init, work, reset, block);

        block = init;

        const auto getres = GetNodeValues(Flow, ctx, block);

        const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, getres.first, ConstantInt::get(getres.first->getType(), 0), "yield", block);
        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, getres.first, ConstantInt::get(getres.first->getType(), 0), "good", block);

        const auto outres = SelectInst::Create(yield, GetYield(context), GetFinish(context), "outres", block);

        result->addIncoming(outres, block);
        BranchInst::Create(next, exit, good, block);

        block = next;

        new StoreInst(GetEmpty(context), statePtr, block);

        for (auto i = 0U; i < Items.size(); ++i)
            if (Items[i]->GetDependencesCount() > 0U)
                EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));

        BranchInst::Create(work, block);

        block = work;

        const auto output = GetNodeValue(Output, ctx, block);

        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, output, GetFinish(context), "finish", block);
        result->addIncoming(output, block);
        BranchInst::Create(step, exit, finish, block);

        block = step;

        new StoreInst(GetInvalid(context), statePtr, block);
        BranchInst::Create(init, block);

        block = exit;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = FlowDependsOn(Flow)) {
            std::for_each(Items.cbegin(), Items.cend(), std::bind(&TNarrowFlatMapFlowWrapper::Own, flow, std::placeholders::_1));
            DependsOn(flow, Output);
        }
        std::for_each(Items.cbegin(), Items.cend(), std::bind(&IComputationNode::AddDependence, std::placeholders::_1, Output->GetSource()));
    }

    IComputationWideFlowNode* const Flow;
    const TComputationExternalNodePtrVector Items;
    IComputationNode* const Output;

    const ui32 WideFieldsIndex;
};

template <bool IsMultiRowPerItem, bool ResultContainerOpt>
class TFlowFlatMapWrapper : public std::conditional_t<IsMultiRowPerItem,
    TStatefulFlowCodegeneratorNode<TFlowFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>,
    TStatelessFlowCodegeneratorNode<TFlowFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>> {
using TBaseComputation = std::conditional_t<IsMultiRowPerItem,
    TStatefulFlowCodegeneratorNode<TFlowFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>,
    TStatelessFlowCodegeneratorNode<TFlowFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>>;
public:
     TFlowFlatMapWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* flow, IComputationExternalNode* item, IComputationNode* newItem)
        : TBaseComputation(mutables, flow, kind), Flow(flow), Item(item), NewItem(newItem)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        while (true) {
            if (auto item = Flow->GetValue(ctx); item.IsSpecial()) {
                return item.Release();
            } else {
                Item->SetValue(ctx, std::move(item));
            }

            if (auto newItem = NewItem->GetValue(ctx)) {
                return newItem.Release().GetOptionalValueIf<!IsMultiRowPerItem && ResultContainerOpt>();
            }
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        while (!state.IsFinish()) {
            if (state.HasValue()) {
                if constexpr (ResultContainerOpt) {
                    switch (NUdf::TUnboxedValue result; state.Fetch(result)) {
                        case NUdf::EFetchStatus::Finish: break;
                        case NUdf::EFetchStatus::Yield: return NUdf::TUnboxedValuePod::MakeYield();
                        case NUdf::EFetchStatus::Ok: return result.Release();
                    }
                } else if (NUdf::TUnboxedValue result; state.Next(result)) {
                    return result.Release();
                }
                state.Clear();
            }

            NUdf::TUnboxedValue item = DoCalculate(ctx);
            if (item.IsSpecial()) {
                return item.Release();
            } else {
                state = ResultContainerOpt ? std::move(item) : item.GetListIterator();
            }
        }

        return state;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);

        BranchInst::Create(loop, block);
        block = loop;

        const auto item = GetNodeValue(Flow, ctx, block);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
        const auto result = PHINode::Create(item->getType(), 2, "result", exit);

        result->addIncoming(item, block);
        BranchInst::Create(exit, work, IsSpecial(item, block), block);

        block = work;
        codegenItem->CreateSetValue(ctx, block, item);
        const auto value = GetNodeValue(NewItem, ctx, block);
        result->addIncoming(!IsMultiRowPerItem && ResultContainerOpt ? GetOptionalValue(context, value, block) : value, block);
        BranchInst::Create(loop, exit, IsEmpty(value, block), block);

        block = exit;
        return result;
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* currentPtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto statusType = Type::getInt32Ty(context);
        const auto valueType = Type::getInt128Ty(context);
        const auto valuePtr = new AllocaInst(valueType, 0U, "value_ptr", &ctx.Func->getEntryBlock().back());
        new StoreInst(ConstantInt::get(valueType, 0), valuePtr, block);

        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        const auto result = PHINode::Create(valueType, ResultContainerOpt ? 3U : 2U, "result", over);

        BranchInst::Create(more, block);

        block = more;

        const auto current = new LoadInst(valueType, currentPtr, "current", block);
        BranchInst::Create(pull, skip, HasValue(current, block), block);

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            block = pull;

            if constexpr (ResultContainerOpt) {
                const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, current, ctx.Codegen, block, valuePtr);

                result->addIncoming(GetYield(context), block);
                const auto choise = SwitchInst::Create(status, good, 2U, block);
                choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), over);
                choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), next);
            } else {
                const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), current, ctx.Codegen, block, valuePtr);
                BranchInst::Create(good, next, status, block);
            }

            block = good;
            const auto value = new LoadInst(valueType, valuePtr, "value", block);
            ValueRelease(static_cast<const IComputationNode*>(this)->GetRepresentation(), value, ctx, block);
            result->addIncoming(value, block);
            BranchInst::Create(over, block);

            block = next;
            UnRefBoxed(current, ctx, block);
            new StoreInst(ConstantInt::get(current->getType(), 0), currentPtr, block);
            BranchInst::Create(skip, block);
        }

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            block = skip;

            const auto list = DoGenerateGetValue(ctx, block);
            result->addIncoming(list, block);
            BranchInst::Create(over, good, IsSpecial(list, block),  block);

            block = good;
            if constexpr (ResultContainerOpt) {
                new StoreInst(list, currentPtr, block);
                AddRefBoxed(list, ctx, block);
            } else {
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(currentPtr, list, ctx.Codegen, block);
                if (NewItem->IsTemporaryValue()) {
                    CleanupBoxed(list, ctx, block);
                }
            }
            BranchInst::Create(more, block);
        }

        block = over;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(this->Flow)) {
            this->Own(flow, this->Item);
            this->DependsOn(flow, this->NewItem);
        }
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    IComputationNode* const NewItem;
};

template <bool IsMultiRowPerItem, bool ResultContainerOpt>
class TNarrowFlatMapWrapper : public std::conditional_t<IsMultiRowPerItem,
    TStatefulFlowCodegeneratorNode<TNarrowFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>,
    TStatelessFlowCodegeneratorNode<TNarrowFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>> {
using TBaseComputation = std::conditional_t<IsMultiRowPerItem,
    TStatefulFlowCodegeneratorNode<TNarrowFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>,
    TStatelessFlowCodegeneratorNode<TNarrowFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>>;
public:
     TNarrowFlatMapWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationWideFlowNode* flow, const TComputationExternalNodePtrVector&& items, IComputationNode* newItem)
        : TBaseComputation(mutables, flow, kind)
        , Flow(flow)
        , Items(std::move(items))
        , NewItem(newItem)
        , PasstroughItem(GetPasstroughtMap(TComputationNodePtrVector{NewItem}, Items).front())
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Items.size()))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto** fields = ctx.WideFields.data() + WideFieldsIndex;

        while (true) {
            for (auto i = 0U; i < Items.size(); ++i)
                if (NewItem == Items[i] || Items[i]->GetDependencesCount() > 0U)
                    fields[i] = &Items[i]->RefValue(ctx);

            switch (const auto result = Flow->FetchValues(ctx, fields)) {
                case EFetchResult::Finish:
                    return NUdf::TUnboxedValuePod::MakeFinish();
                case EFetchResult::Yield:
                    return NUdf::TUnboxedValuePod::MakeYield();
                case EFetchResult::One:
                    break;
            }

            if (auto newItem = NewItem->GetValue(ctx)) {
                return newItem.Release().GetOptionalValueIf<!IsMultiRowPerItem && ResultContainerOpt>();
            }
        }
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        while (!state.IsFinish()) {
            if (state.HasValue()) {
                if constexpr (ResultContainerOpt) {
                    switch (NUdf::TUnboxedValue result; state.Fetch(result)) {
                        case NUdf::EFetchStatus::Finish: break;
                        case NUdf::EFetchStatus::Yield: return NUdf::TUnboxedValuePod::MakeYield();
                        case NUdf::EFetchStatus::Ok: return result.Release();
                    }
                } else if (NUdf::TUnboxedValue result; state.Next(result)) {
                    return result.Release();
                }
                state.Clear();
            }

            NUdf::TUnboxedValue item = DoCalculate(ctx);
            if (item.IsSpecial()) {
                return item.Release();
            } else {
                state = ResultContainerOpt ? std::move(item) : item.GetListIterator();
            }
        }

        return state;
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
        const auto result = PHINode::Create(Type::getInt128Ty(context), 2, "result", exit);

        BranchInst::Create(loop, block);
        block = loop;

        const auto getres = GetNodeValues(Flow, ctx, block);

        const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, getres.first, ConstantInt::get(getres.first->getType(), 0), "yield", block);
        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, getres.first, ConstantInt::get(getres.first->getType(), 0), "good", block);

        const auto outres = SelectInst::Create(yield, GetYield(context), GetFinish(context), "outres", block);


        result->addIncoming(outres, block);
        BranchInst::Create(work, exit, good, block);

        block = work;

        Value* value = nullptr;
        if (const auto passtrough = PasstroughItem) {
            value = getres.second[*passtrough](ctx, block);
        } else {
            for (auto i = 0U; i < Items.size(); ++i)
                if (Items[i]->GetDependencesCount() > 0U)
                    EnsureDynamicCast<ICodegeneratorExternalNode*>(Items[i])->CreateSetValue(ctx, block, getres.second[i](ctx, block));

            value = GetNodeValue(NewItem, ctx, block);
        }

        result->addIncoming(!IsMultiRowPerItem && ResultContainerOpt ? GetOptionalValue(context, value, block) : value, block);
        BranchInst::Create(loop, exit, IsEmpty(value, block), block);

        block = exit;
        return result;
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* currentPtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto statusType = Type::getInt32Ty(context);
        const auto valueType = Type::getInt128Ty(context);
        const auto valuePtr = new AllocaInst(valueType, 0U, "value_ptr", &ctx.Func->getEntryBlock().back());
        new StoreInst(ConstantInt::get(valueType, 0), valuePtr, block);

        const auto more = BasicBlock::Create(context, "more", ctx.Func);
        const auto pull = BasicBlock::Create(context, "pull", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
        const auto over = BasicBlock::Create(context, "over", ctx.Func);

        const auto result = PHINode::Create(valueType, ResultContainerOpt ? 3U : 2U, "result", over);

        BranchInst::Create(more, block);

        block = more;

        const auto current = new LoadInst(valueType, currentPtr, "current", block);
        BranchInst::Create(pull, skip, HasValue(current, block), block);

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            block = pull;

            if constexpr (ResultContainerOpt) {
                const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, current, ctx.Codegen, block, valuePtr);

                result->addIncoming(GetYield(context), block);
                const auto choise = SwitchInst::Create(status, good, 2U, block);
                choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Yield)), over);
                choise->addCase(ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), next);
            } else {
                const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(Type::getInt1Ty(context), current, ctx.Codegen, block, valuePtr);
                BranchInst::Create(good, next, status, block);
            }

            block = good;
            const auto value = new LoadInst(valueType, valuePtr, "value", block);
            ValueRelease(static_cast<const IComputationNode*>(this)->GetRepresentation(), value, ctx, block);
            result->addIncoming(value, block);
            BranchInst::Create(over, block);

            block = next;
            UnRefBoxed(current, ctx, block);
            new StoreInst(ConstantInt::get(current->getType(), 0), currentPtr, block);
            BranchInst::Create(skip, block);
        }

        {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            block = skip;

            const auto list = DoGenerateGetValue(ctx, block);
            result->addIncoming(list, block);
            BranchInst::Create(over, good, IsSpecial(list, block),  block);

            block = good;
            if constexpr (ResultContainerOpt) {
                new StoreInst(list, currentPtr, block);
                AddRefBoxed(list, ctx, block);
            } else {
                CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(currentPtr, list, ctx.Codegen, block);
                if (NewItem->IsTemporaryValue()) {
                    CleanupBoxed(list, ctx, block);
                }
            }
            BranchInst::Create(more, block);
        }

        block = over;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            for (const auto& item : this->Items)
                this->Own(flow, item);
            this->DependsOn(flow, this->NewItem);
        }
    }

    IComputationWideFlowNode* const Flow;
    const TComputationExternalNodePtrVector Items;
    IComputationNode* const NewItem;

    const std::optional<size_t> PasstroughItem;

    const ui32 WideFieldsIndex;
};

template <bool MultiOptional>
class TSimpleListValue : public TCustomListValue {
public:
    class TIterator : public TComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& iter, IComputationExternalNode* item, IComputationNode* newItem)
            : TComputationValue<TIterator>(memInfo)
            , CompCtx(compCtx)
            , Iter(std::move(iter))
            , Item(item)
            , NewItem(newItem)
        {}

    private:
        bool Next(NUdf::TUnboxedValue& value) final {
            for (;;) {
                if (!Iter.Next(Item->RefValue(CompCtx))) {
                    return false;
                }

                if (auto newItem = NewItem->GetValue(CompCtx)) {
                    value = newItem.Release().template GetOptionalValueIf<MultiOptional>();
                    return true;
                }
            }
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue Iter;

        IComputationExternalNode* const Item;
        IComputationNode* const NewItem;
    };

    TSimpleListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, IComputationExternalNode* item, IComputationNode* newItem)
        : TCustomListValue(memInfo)
        , CompCtx(compCtx)
        , List(std::move(list))
        , Item(item)
        , NewItem(newItem)
    {
    }

private:
    NUdf::TUnboxedValue GetListIterator() const final {
        return CompCtx.HolderFactory.Create<TIterator>(CompCtx, List.GetListIterator(), Item, NewItem);
    }

    TComputationContext& CompCtx;
    const NUdf::TUnboxedValue List;
    IComputationExternalNode* const Item;
    IComputationNode* const NewItem;
};

template <bool MultiOptional>
class TSimpleStreamValue : public TComputationValue<TSimpleStreamValue<MultiOptional>> {
public:
    using TBase = TComputationValue<TSimpleStreamValue>;

    TSimpleStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& stream, IComputationExternalNode* item, IComputationNode* newItem)
        : TBase(memInfo)
        , CompCtx(compCtx)
        , Stream(std::move(stream))
        , Item(item)
        , NewItem(newItem)
    {}

private:
    ui32 GetTraverseCount() const override {
        return 1;
    }

    NUdf::TUnboxedValue GetTraverseItem(ui32 index) const override {
        Y_UNUSED(index);
        return Stream;
    }

    NUdf::TUnboxedValue Save() const override {
        return NUdf::TUnboxedValue::Zero();
    }

    void Load(const NUdf::TStringRef& state) override {
        Y_UNUSED(state);
    }

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
        for (;;) {
            const auto status = Stream.Fetch(Item->RefValue(CompCtx));
            if (NUdf::EFetchStatus::Ok != status) {
                return status;
            }

            if (auto newItem = NewItem->GetValue(CompCtx)) {
                result = newItem.Release().template GetOptionalValueIf<MultiOptional>();
                return NUdf::EFetchStatus::Ok;
            }
        }
    }

private:
    TComputationContext& CompCtx;
    const NUdf::TUnboxedValue Stream;
    IComputationExternalNode* const Item;
    IComputationNode* const NewItem;
};

template <bool IsNewStream>
class TListValue : public TCustomListValue {
public:
    class TIterator : public TComputationValue<TIterator> {
    public:
        TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& iter, IComputationExternalNode* item, IComputationNode* newItem)
            : TComputationValue<TIterator>(memInfo)
            , CompCtx(compCtx)
            , Iter(std::move(iter))
            , Item(item)
            , NewItem(newItem)
        {}

    private:
        bool Next(NUdf::TUnboxedValue& value) final {
            for (NUdf::TUnboxedValue current = std::move(Current);; current.Clear()) {
                if (!current) {
                    if (Iter.Next(Item->RefValue(CompCtx))) {
                        current = IsNewStream ? NewItem->GetValue(CompCtx) : NewItem->GetValue(CompCtx).GetListIterator();
                    } else {
                        return false;
                    }
                }

                if constexpr (IsNewStream) {
                    const auto status = current.Fetch(value);
                    MKQL_ENSURE(status != NUdf::EFetchStatus::Yield, "Unexpected stream status");
                    if (NUdf::EFetchStatus::Finish == status) {
                        continue;
                    }
                } else {
                    if (!current.Next(value)) {
                        continue;
                    }
                }

                Current = std::move(current);
                return true;
            }
        }

        TComputationContext& CompCtx;
        const NUdf::TUnboxedValue Iter;

        IComputationExternalNode* const Item;
        IComputationNode* const NewItem;

        NUdf::TUnboxedValue Current;
    };

    TListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& list, IComputationExternalNode* item, IComputationNode* newItem)
        : TCustomListValue(memInfo)
        , CompCtx(compCtx)
        , List(std::move(list))
        , Item(item)
        , NewItem(newItem)
    {}

private:
    NUdf::TUnboxedValue GetListIterator() const final {
        return CompCtx.HolderFactory.Create<TIterator>(CompCtx, List.GetListIterator(), Item, NewItem);
    }

    TComputationContext& CompCtx;
    const NUdf::TUnboxedValue List;
    IComputationExternalNode* const Item;
    IComputationNode* const NewItem;
};

template <bool IsNewStream>
class TStreamValue : public TComputationValue<TStreamValue<IsNewStream>> {
public:
    using TBase = TComputationValue<TStreamValue<IsNewStream>>;

    TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, NUdf::TUnboxedValue&& stream, IComputationExternalNode* item, IComputationNode* newItem)
        : TBase(memInfo)
        , CompCtx(compCtx)
        , Stream(std::move(stream))
        , Item(item)
        , NewItem(newItem)
    {}

private:
    ui32 GetTraverseCount() const override {
        return 1;
    }

    NUdf::TUnboxedValue GetTraverseItem(ui32 index) const override {
        Y_UNUSED(index);
        return Stream;
    }

    NUdf::TUnboxedValue Save() const override {
        return NUdf::TUnboxedValue::Zero();
    }

    void Load(const NUdf::TStringRef& state) override {
        Y_UNUSED(state);
    }

    NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
        for (NUdf::TUnboxedValue current = std::move(Current);; current.Clear()) {
            if (!current) {
                const auto status = Stream.Fetch(Item->RefValue(CompCtx));
                if (NUdf::EFetchStatus::Ok != status) {
                    return status;
                }
                current = IsNewStream ? NewItem->GetValue(CompCtx) : NewItem->GetValue(CompCtx).GetListIterator();
            }

            auto status = NUdf::EFetchStatus::Ok;
            if constexpr (IsNewStream) {
                status = current.Fetch(result);
                if (NUdf::EFetchStatus::Finish == status) {
                    continue;
                }
            } else {
                if (!current.Next(result)) {
                    continue;
                }
            }

            Current = std::move(current);
            return status;
        }
    }

private:
    TComputationContext& CompCtx;
    const NUdf::TUnboxedValue Stream;
    IComputationExternalNode* const Item;
    IComputationNode* const NewItem;

    NUdf::TUnboxedValue Current;
};

template <bool IsInputStream, bool IsMultiRowPerItem, bool ResultContainerOpt>
class TBaseFlatMapWrapper {
protected:
     TBaseFlatMapWrapper(IComputationNode* list, IComputationExternalNode* item, IComputationNode* newItem)
        : List(list), Item(item), NewItem(newItem)
    {}

#ifndef MKQL_DISABLE_CODEGEN
    using TCodegenValue = std::conditional_t<IsInputStream,
        typename std::conditional_t<IsMultiRowPerItem, TStreamCodegenStatefulValue, TStreamCodegenValueStateless>,
        typename std::conditional_t<IsMultiRowPerItem, TCustomListCodegenStatefulValue, TCustomListCodegenValue>>;

    Function* GenerateSimpleMapper(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);

        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = IsInputStream ? Type::getInt32Ty(context) : Type::getInt1Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        ctx.Annotator = &annotator;

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(loop, block);
        block = loop;

        const auto itemPtr = codegenItem->CreateRefValue(ctx, block);
        const auto status = IsInputStream ?
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, itemPtr):
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(statusType, container, codegen, block, itemPtr);

        const auto icmp = IsInputStream ?
            CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block):
            status;

        BranchInst::Create(good, done, icmp, block);
        block = good;

        const auto resItem = GetNodeValue(NewItem, ctx, block);

        BranchInst::Create(loop, pass, IsEmpty(resItem, block), block);

        block = pass;

        SafeUnRefUnboxed(valuePtr, ctx, block);
        const auto getOpt = GetOptionalValue(context, resItem, block);
        new StoreInst(getOpt, valuePtr, block);
        ValueAddRef(NewItem->GetRepresentation(), valuePtr, ctx, block);

        BranchInst::Create(done, block);

        block = done;
        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    Function* GenerateMapper(NYql::NCodegen::ICodegen& codegen, const TString& name) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);

        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = IsInputStream ? Type::getInt32Ty(context) : Type::getInt1Ty(context);
        const auto stateType = ResultContainerOpt ? Type::getInt32Ty(context) : Type::getInt1Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType), PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        ctx.Annotator = &annotator;

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto currentArg = &*++args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        const auto zero = ConstantInt::get(valueType, 0);

        const auto init = new LoadInst(valueType, currentArg, "init", block);

        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
        const auto cont = BasicBlock::Create(context, "cont", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto current = PHINode::Create(valueType, 2, "result", pass);
        current->addIncoming(init, block);

        const auto step = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, init, zero, "step", block);
        BranchInst::Create(next, pass, step, block);

        block = next;

        const auto itemPtr = codegenItem->CreateRefValue(ctx, block);
        const auto status = IsInputStream ?
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, itemPtr):
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(statusType, container, codegen, block, itemPtr);

        const auto icmp = IsInputStream ?
            CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block):
            status;

        BranchInst::Create(good, done, icmp, block);
        block = good;

        if constexpr (ResultContainerOpt) {
            GetNodeValue(currentArg, NewItem, ctx, block);
        } else {
            const auto list = GetNodeValue(NewItem, ctx, block);
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListIterator>(currentArg, list, codegen, block);
            if (NewItem->IsTemporaryValue())
                CleanupBoxed(list, ctx, block);
        }

        const auto iter = new LoadInst(valueType, currentArg, "iter", block);
        current->addIncoming(iter, block);

        BranchInst::Create(pass, block);
        block = pass;

        const auto state = ResultContainerOpt ?
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(stateType, current, codegen, block, valuePtr):
            CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Next>(stateType, current, codegen, block, valuePtr);

        const auto scmp = ResultContainerOpt ?
            CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, state, ConstantInt::get(stateType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), "scmp", block):
            state;

        BranchInst::Create(exit, cont, scmp, block);

        block = cont;
        UnRefBoxed(current, ctx, block);
        BranchInst::Create(next, block);

        block = exit;
        ReturnInst::Create(context, IsInputStream ? (ResultContainerOpt ? state : ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok))) : ConstantInt::getTrue(context), block);

        block = done;
        new StoreInst(zero, currentArg, block);
        ReturnInst::Create(context, status, block);

        return ctx.Func;
    }

    using TFlatMapPtr = std::conditional_t<IsInputStream,
        typename std::conditional_t<IsMultiRowPerItem, TStreamCodegenStatefulValue, TStreamCodegenValueStateless>::TFetchPtr,
        typename std::conditional_t<IsMultiRowPerItem, TCustomListCodegenStatefulValue, TCustomListCodegenValue>::TNextPtr
    >;

    Function* FlatMapFunc = nullptr;

    TFlatMapPtr FlatMap = nullptr;
#endif

    IComputationNode* const List;
    IComputationExternalNode* const Item;
    IComputationNode* const NewItem;
};

template <bool IsMultiRowPerItem, bool ResultContainerOpt>
class TStreamFlatMapWrapper : public TCustomValueCodegeneratorNode<TStreamFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>,
    private TBaseFlatMapWrapper<true, IsMultiRowPerItem, ResultContainerOpt> {
    typedef TBaseFlatMapWrapper<true, IsMultiRowPerItem, ResultContainerOpt> TBaseWrapper;
    typedef TCustomValueCodegeneratorNode<TStreamFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>> TBaseComputation;
public:
    TStreamFlatMapWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, IComputationNode* newItem)
        : TBaseComputation(mutables), TBaseWrapper(list, item, newItem)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && this->FlatMap)
            return ctx.HolderFactory.Create<typename TBaseWrapper::TCodegenValue>(this->FlatMap, &ctx, this->List->GetValue(ctx));
#endif
        return ctx.HolderFactory.Create<std::conditional_t<IsMultiRowPerItem, TStreamValue<ResultContainerOpt>, TSimpleStreamValue<ResultContainerOpt>>>(ctx, this->List->GetValue(ctx), this->Item, this->NewItem);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(this->List);
        this->Own(this->Item);
        this->DependsOn(this->NewItem);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        this->FlatMapFunc = IsMultiRowPerItem ?
            this->GenerateMapper(codegen, TBaseComputation::MakeName("Fetch")):
            this->GenerateSimpleMapper(codegen, TBaseComputation::MakeName("Fetch"));
        codegen.ExportSymbol(this->FlatMapFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (this->FlatMapFunc)
            this->FlatMap = reinterpret_cast<typename TBaseWrapper::TFlatMapPtr>(codegen.GetPointerToFunction(this->FlatMapFunc));
    }
#endif
};

#ifndef MKQL_DISABLE_CODEGEN
NUdf::TUnboxedValuePod* MyArrayAlloc(const ui64 size) {
    return TMKQLAllocator<NUdf::TUnboxedValuePod>::allocate(size);
}

void MyArrayFree(const NUdf::TUnboxedValuePod *const ptr, const ui64 size) noexcept {
    TMKQLAllocator<NUdf::TUnboxedValuePod>::deallocate(ptr, size);
}
#endif
template <bool IsMultiRowPerItem, bool ResultContainerOpt>
class TListFlatMapWrapper : public TBothWaysCodegeneratorNode<TListFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>>,
    private TBaseFlatMapWrapper<false, IsMultiRowPerItem, ResultContainerOpt> {
    typedef TBaseFlatMapWrapper<false, IsMultiRowPerItem, ResultContainerOpt> TBaseWrapper;
    typedef TBothWaysCodegeneratorNode<TListFlatMapWrapper<IsMultiRowPerItem, ResultContainerOpt>> TBaseComputation;
    static constexpr size_t UseOnStack = 1ULL << 8ULL;
public:
    TListFlatMapWrapper(TComputationMutables& mutables, IComputationNode* list, IComputationExternalNode* item, IComputationNode* newItem)
        : TBaseComputation(mutables), TBaseWrapper(list, item, newItem)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = this->List->GetValue(ctx);
        if (const auto elements = list.GetElements()) {
            const auto size = list.GetListLength();
            TUnboxedValueVector values(size);

            auto it = values.begin();
            std::for_each(elements, elements + size, [&] (NUdf::TUnboxedValue item) {
                this->Item->SetValue(ctx, std::move(item));
                *it = this->NewItem->GetValue(ctx);
                if (IsMultiRowPerItem || *it) {
                    auto value = it->GetOptionalValueIf<!IsMultiRowPerItem && ResultContainerOpt>();
                    *it++ = value;
                }
            });

            if constexpr (IsMultiRowPerItem) {
                return ctx.HolderFactory.ExtendList<ResultContainerOpt>(values.data(), values.size());
            }

            NUdf::TUnboxedValue* items = nullptr;
            const auto result = ctx.HolderFactory.CreateDirectArrayHolder(std::distance(values.begin(), it), items);
            std::move(values.begin(), it, items);
            return result;
        }


        return ctx.HolderFactory.Create<std::conditional_t<IsMultiRowPerItem, TListValue<ResultContainerOpt>, TSimpleListValue<ResultContainerOpt>>>(ctx, std::move(list), this->Item, this->NewItem);
    }

#ifndef MKQL_DISABLE_CODEGEN
    NUdf::TUnboxedValuePod MakeLazyList(TComputationContext& ctx, const NUdf::TUnboxedValuePod value) const {
        return ctx.HolderFactory.Create<typename TBaseWrapper::TCodegenValue>(this->FlatMap, &ctx, value);
    }

    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(this->Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");

        const auto list = GetNodeValue(this->List, ctx, block);

        const auto lazy = BasicBlock::Create(context, "lazy", ctx.Func);
        const auto hard = BasicBlock::Create(context, "hard", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto map = PHINode::Create(list->getType(), 3U, "map", done);

        const auto elementsType = PointerType::getUnqual(list->getType());
        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(elementsType, list, ctx.Codegen, block);
        const auto fill = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, elements, ConstantPointerNull::get(elementsType), "fill", block);

        BranchInst::Create(hard, lazy, fill, block);

        {
            block = hard;

            const auto smsk = BasicBlock::Create(context, "smsk", ctx.Func);
            const auto hmsk = BasicBlock::Create(context, "hmsk", ctx.Func);
            const auto main = BasicBlock::Create(context, "main", ctx.Func);
            const auto free = BasicBlock::Create(context, "free", ctx.Func);

            const auto vector = PHINode::Create(PointerType::getUnqual(list->getType()), 2U, "vector", main);

            const auto size = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetListLength>(Type::getInt64Ty(context), list, ctx.Codegen, block);

            const auto zeroSize = ConstantInt::get(size->getType(), 0);
            const auto plusSize = ConstantInt::get(size->getType(), 1);

            const auto heap = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, ConstantInt::get(size->getType(), UseOnStack), "heap", block);
            BranchInst::Create(hmsk, smsk, heap, block);

            {
                block = smsk;

                const auto arrayType = ArrayType::get(list->getType(), UseOnStack);
                const auto array = *this->Stateless || ctx.AlwaysInline ?
                    new AllocaInst(arrayType, 0U, "array", &ctx.Func->getEntryBlock().back()):
                    new AllocaInst(arrayType, 0U, "array", block);
                const auto ptr = GetElementPtrInst::CreateInBounds(arrayType, array, {zeroSize, zeroSize}, "ptr", block);

                vector->addIncoming(ptr, block);
                BranchInst::Create(main, block);
            }

            {
                block = hmsk;

                const auto fnType = FunctionType::get(vector->getType(), {size->getType()}, false);
                const auto name = "MyArrayAlloc";
                ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&MyArrayAlloc));
                const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);
                const auto ptr = CallInst::Create(func, {size}, "ptr", block);
                vector->addIncoming(ptr, block);
                BranchInst::Create(main, block);
            }

            block = main;

            const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
            const auto next = BasicBlock::Create(context, "next", ctx.Func);
            const auto stop = BasicBlock::Create(context, "stop", ctx.Func);

            const auto index = PHINode::Create(size->getType(), 2U, "index", loop);
            index->addIncoming(zeroSize, block);

            const auto idx = IsMultiRowPerItem ? index : PHINode::Create(size->getType(), 2U, "idx", loop);
            if constexpr (!IsMultiRowPerItem) {
                idx->addIncoming(zeroSize, block);
            }

            BranchInst::Create(loop, block);

            block = loop;

            const auto more = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGT, size, index, "more", block);

            BranchInst::Create(next, stop, more, block);

            block = next;
            const auto src = GetElementPtrInst::CreateInBounds(list->getType(), elements, {index}, "src", block);
            const auto item = new LoadInst(list->getType(), src, "item", block);
            codegenItem->CreateSetValue(ctx, block, item);
            const auto dst = GetElementPtrInst::CreateInBounds(list->getType(), vector, {idx}, "dst", block);
            GetNodeValue(dst, this->NewItem, ctx, block);

            const auto inc = BinaryOperator::CreateAdd(index, plusSize, "inc", block);
            index->addIncoming(inc, block);

            if constexpr (!IsMultiRowPerItem) {
                const auto plus = BinaryOperator::CreateAdd(idx, plusSize, "plus", block);
                const auto load = new LoadInst(list->getType(), dst, "load", block);
                new StoreInst(GetOptionalValue(context, load, block), dst, block);
                const auto move = SelectInst::Create(IsExists(load, block), plus, idx, "move", block);
                idx->addIncoming(move, block);
            }

            BranchInst::Create(loop, block);

            block = stop;

            if (this->List->IsTemporaryValue()) {
                CleanupBoxed(list, ctx, block);
            }

            Value* res;
            if constexpr (!IsMultiRowPerItem) {
                const auto newType = PointerType::getUnqual(list->getType());
                const auto newPtr = *this->Stateless || ctx.AlwaysInline ?
                    new AllocaInst(newType, 0U, "new_ptr", &ctx.Func->getEntryBlock().back()):
                    new AllocaInst(newType, 0U, "new_ptr", block);
                res = GenNewArray(ctx, idx, newPtr, block);
                const auto target = new LoadInst(newType, newPtr, "target", block);

                const auto pType = PointerType::getUnqual(Type::getInt8Ty(context));
                const auto pdst = CastInst::Create(Instruction::BitCast, target, pType, "pdst", block);
                const auto psrc = CastInst::Create(Instruction::BitCast, vector, pType, "psrc", block);
                const auto bytes = BinaryOperator::CreateShl(idx, ConstantInt::get(idx->getType(), 4), "bytes", block);

                const auto fnType = FunctionType::get(Type::getVoidTy(context), {pType, pType, bytes->getType(), Type::getInt1Ty(context)}, false);
                const auto func = ctx.Codegen.GetModule().getOrInsertFunction("llvm.memcpy.p0i8.p0i8.i64", fnType);
                CallInst::Create(func, {pdst, psrc, bytes, ConstantInt::getFalse(context)}, "", block);
            } else {
                const auto factory = ctx.GetFactory();

                const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::ExtendList<ResultContainerOpt>));

                if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                    const auto funType = FunctionType::get(list->getType(), {factory->getType(), vector->getType(), index->getType()}, false);
                    const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                    res = CallInst::Create(funType, funcPtr, {factory, vector, index}, "res", block);
                } else {
                    const auto retPtr = new AllocaInst(list->getType(), 0U, "ret_ptr", block);
                    const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType(), vector->getType(), index->getType()}, false);
                    const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                    CallInst::Create(funType, funcPtr, {factory, retPtr, vector, index}, "", block);
                    res = new LoadInst(list->getType(), retPtr, "res", block);
                }
            }
            map->addIncoming(res, block);
            BranchInst::Create(free, done, heap, block);

            {
                block = free;

                const auto fnType = FunctionType::get(Type::getVoidTy(context), {vector->getType(), size->getType()}, false);
                const auto name = "MyArrayFree";
                ctx.Codegen.AddGlobalMapping(name, reinterpret_cast<const void*>(&MyArrayFree));
                const auto func = ctx.Codegen.GetModule().getOrInsertFunction(name, fnType);
                CallInst::Create(func, {vector, size}, "", block);

                map->addIncoming(res, block);
                BranchInst::Create(done, block);
            }
        }

        {
            block = lazy;

            const auto doFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TListFlatMapWrapper::MakeLazyList));
            const auto ptrType = PointerType::getUnqual(StructType::get(context));
            const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(list->getType() , {self->getType(), ctx.Ctx->getType(), list->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                const auto value = CallInst::Create(funType, doFuncPtr, {self, ctx.Ctx, list}, "value", block);
                map->addIncoming(value, block);
            } else {
                const auto resultPtr = new AllocaInst(list->getType(), 0U, "return", block);
                new StoreInst(list, resultPtr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {self->getType(), resultPtr->getType(), ctx.Ctx->getType(), resultPtr->getType()}, false);
                const auto doFuncPtr = CastInst::Create(Instruction::IntToPtr, doFunc, PointerType::getUnqual(funType), "function", block);
                CallInst::Create(funType, doFuncPtr, {self, resultPtr, ctx.Ctx, resultPtr}, "", block);
                const auto value = new LoadInst(list->getType(), resultPtr, "value", block);
                map->addIncoming(value, block);
            }
            BranchInst::Create(done, block);
        }

        block = done;
        return map;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(this->List);
        this->Own(this->Item);
        this->DependsOn(this->NewItem);
    }
#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListFlatMapWrapper>::GenerateFunctions(codegen);
        this->FlatMapFunc = IsMultiRowPerItem ?
            this->GenerateMapper(codegen, TBaseComputation::MakeName("Next")):
            this->GenerateSimpleMapper(codegen, TBaseComputation::MakeName("Next"));
        codegen.ExportSymbol(this->FlatMapFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        TMutableCodegeneratorRootNode<TListFlatMapWrapper>::FinalizeFunctions(codegen);
        if (this->FlatMapFunc)
            this->FlatMap = reinterpret_cast<typename TBaseWrapper::TFlatMapPtr>(codegen.GetPointerToFunction(this->FlatMapFunc));
    }
#endif
};

}

IComputationNode* WrapFlatMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 args");

    const auto listType = callable.GetInput(0).GetStaticType();;
    const auto newListType = callable.GetInput(2).GetStaticType();

    const auto type = callable.GetType()->GetReturnType();
    const auto list = LocateNode(ctx.NodeLocator, callable, 0);
    const auto newItem = LocateNode(ctx.NodeLocator, callable, 2);
    const auto itemArg = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto kind = GetValueRepresentation(type);
    if (listType->IsFlow()) {
        if (newListType->IsFlow()) {
            if (const auto wideOut = dynamic_cast<IComputationWideFlowNode*>(newItem))
                return new TFlowFlatMapWideWrapper(ctx.Mutables, list, itemArg, wideOut);
            else
                return new TFlowFlatMapFlowWrapper(ctx.Mutables, kind, list, itemArg, newItem);
        } else if (newListType->IsList()) {
            return new TFlowFlatMapWrapper<true, false>(ctx.Mutables, kind, list, itemArg, newItem);
        } else if (newListType->IsStream()) {
            return new TFlowFlatMapWrapper<true, true>(ctx.Mutables, kind, list, itemArg, newItem);
        } else if (newListType->IsOptional()) {
            if (AS_TYPE(TOptionalType, newListType)->GetItemType()->IsOptional()) {
                return new TFlowFlatMapWrapper<false, true>(ctx.Mutables, kind, list, itemArg, newItem);
            } else {
                return new TFlowFlatMapWrapper<false, false>(ctx.Mutables, kind, list, itemArg, newItem);
            }
        }
    } else if (listType->IsStream()) {
        if (newListType->IsList()) {
            return new TStreamFlatMapWrapper<true, false>(ctx.Mutables, list, itemArg, newItem);
        } else if (newListType->IsStream()) {
            return new TStreamFlatMapWrapper<true, true>(ctx.Mutables, list, itemArg, newItem);
        } else if (newListType->IsOptional()) {
            if (AS_TYPE(TOptionalType, newListType)->GetItemType()->IsOptional()) {
                return new TStreamFlatMapWrapper<false, true>(ctx.Mutables, list, itemArg, newItem);
            } else {
                return new TStreamFlatMapWrapper<false, false>(ctx.Mutables, list, itemArg, newItem);
            }
        }
    } else if (listType->IsList()) {
        if (newListType->IsFlow()) {
            if (const auto wideOut = dynamic_cast<IComputationWideFlowNode*>(newItem))
                return new TListFlatMapWideWrapper(ctx.Mutables, list, itemArg, wideOut);
            else
                return new TListFlatMapFlowWrapper(ctx.Mutables, kind, list, itemArg, newItem);
        } else if (newListType->IsList()) {
            return new TListFlatMapWrapper<true, false>(ctx.Mutables, list, itemArg, newItem);
        } else if (newListType->IsStream()) {
            return new TListFlatMapWrapper<true, true>(ctx.Mutables, list, itemArg, newItem);
        } else if (newListType->IsOptional()) {
            if (AS_TYPE(TOptionalType, newListType)->GetItemType()->IsOptional()) {
                return new TListFlatMapWrapper<false, true>(ctx.Mutables, list, itemArg, newItem);
            } else {
                return new TListFlatMapWrapper<false, false>(ctx.Mutables, list, itemArg, newItem);
            }
        }
    }

    THROW yexception() << "Expected flow, list or stream of lists, streams or optionals.";
}

IComputationNode* WrapNarrowFlatMap(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 1U, "Expected at least two args.");
    const auto width = GetWideComponentsCount(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    MKQL_ENSURE(callable.GetInputsCount() == width + 2U, "Wrong signature.");

    const auto last = callable.GetInputsCount() - 1U;
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0U);
    const auto newItem = LocateNode(ctx.NodeLocator, callable, last);

    TComputationExternalNodePtrVector args(width, nullptr);
    ui32 index = 0U;
    std::generate(args.begin(), args.end(), [&](){ return LocateExternalNode(ctx.NodeLocator, callable, ++index); });

    const auto newListType = callable.GetInput(last).GetStaticType();
    const auto kind = GetValueRepresentation(callable.GetType()->GetReturnType());
    if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
        if (newListType->IsFlow()) {
            return new TNarrowFlatMapFlowWrapper(ctx.Mutables, kind, wide, std::move(args), newItem);
        } else if (newListType->IsList()) {
            return new TNarrowFlatMapWrapper<true, false>(ctx.Mutables, kind, wide, std::move(args), newItem);
        } else if (newListType->IsStream()) {
            return new TNarrowFlatMapWrapper<true, true>(ctx.Mutables, kind, wide, std::move(args), newItem);
        } else if (newListType->IsOptional()) {
            if (AS_TYPE(TOptionalType, newListType)->GetItemType()->IsOptional()) {
                return new TNarrowFlatMapWrapper<false, true>(ctx.Mutables, kind, wide, std::move(args), newItem);
            } else {
                return new TNarrowFlatMapWrapper<false, false>(ctx.Mutables, kind, wide, std::move(args), newItem);
            }
        }
    }

    THROW yexception() << "Expected wide flow.";
}

}
}
