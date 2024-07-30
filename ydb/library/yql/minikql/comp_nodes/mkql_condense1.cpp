#include "mkql_condense1.h"
#include "mkql_squeeze_state.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool Interruptable, bool UseCtx>
class TCondense1FlowWrapper : public TStatefulFlowCodegeneratorNode<TCondense1FlowWrapper<Interruptable, UseCtx>> {
    typedef TStatefulFlowCodegeneratorNode<TCondense1FlowWrapper<Interruptable, UseCtx>> TBaseComputation;
public:
     TCondense1FlowWrapper(
        TComputationMutables& mutables,
        EValueRepresentation kind,
        IComputationNode* flow,
        IComputationExternalNode* item,
        IComputationExternalNode* state,
        IComputationNode* outSwitch,
        IComputationNode* initState,
        IComputationNode* updateState)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded),
            Flow(flow), Item(item), State(state), Switch(outSwitch), InitState(initState), UpdateState(updateState)
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return static_cast<const NUdf::TUnboxedValuePod&>(state);
        } else if (state.HasValue()) {
            if constexpr (UseCtx) {
                CleanupCurrentContext();
            }
            state = NUdf::TUnboxedValuePod();
            State->SetValue(ctx, InitState->GetValue(ctx));
        }

        while (true) {
            auto item = Flow->GetValue(ctx);
            if (item.IsYield()) {
                return item.Release();
            }

            if (item.IsFinish()) {
                break;
            }

            Item->SetValue(ctx, std::move(item));

            if (state.IsInvalid()) {
                state = NUdf::TUnboxedValuePod();
                State->SetValue(ctx, InitState->GetValue(ctx));
            } else {
                if (Switch) {
                    const auto& reset = Switch->GetValue(ctx);
                    if (Interruptable && !reset) {
                        break;
                    }

                    if (reset.template Get<bool>()) {
                        state = NUdf::TUnboxedValuePod::Zero();
                        return State->GetValue(ctx).Release();
                    }
                }

                State->SetValue(ctx, UpdateState->GetValue(ctx));
            }
        }

        const bool empty = state.IsInvalid();
        state = NUdf::TUnboxedValuePod::MakeFinish();
        return empty ? NUdf::TUnboxedValuePod::MakeFinish() : State->GetValue(ctx).Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");
        const auto codegenState = dynamic_cast<ICodegeneratorExternalNode*>(State);
        MKQL_ENSURE(codegenState, "State must be codegenerator node.");

        const auto step = BasicBlock::Create(context, "step", ctx.Func);
        const auto frst = BasicBlock::Create(context, "frst", ctx.Func);
        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto result = PHINode::Create(valueType, Switch ? 4U : 3U, "result", exit);
        result->addIncoming(state, block);

        const auto way = SwitchInst::Create(state, frst, 2U, block);
        way->addCase(GetFalse(context), step);
        way->addCase(GetFinish(context), exit);

        block = step;

        if constexpr (UseCtx) {
            const auto cleanup = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&CleanupCurrentContext));
            const auto cleanupType = FunctionType::get(Type::getVoidTy(context), {}, false);
            const auto cleanupPtr = CastInst::Create(Instruction::IntToPtr, cleanup, PointerType::getUnqual(cleanupType), "cleanup_ctx", block);
            CallInst::Create(cleanupType, cleanupPtr, {}, "", block);
        }

        new StoreInst(GetEmpty(context), statePtr, block);
        codegenState->CreateSetValue(ctx, block, GetNodeValue(InitState, ctx, block));
        BranchInst::Create(frst, block);

        block = frst;

        const auto invalid = IsInvalid(state, block);
        const auto empty = PHINode::Create(invalid->getType(), 3U, "empty", work);
        empty->addIncoming(invalid, block);
        BranchInst::Create(work, block);

        block = work;
        const auto item = GetNodeValue(Flow, ctx, block);
        result->addIncoming(item, block);

        const auto action = SwitchInst::Create(item, good, 2U, block);
        action->addCase(GetFinish(context), stop);
        action->addCase(GetYield(context), exit);

        block = good;

        codegenItem->CreateSetValue(ctx, block, item);
        BranchInst::Create(init, next, empty, block);

        block = init;

        new StoreInst(GetEmpty(context), statePtr, block);
        codegenState->CreateSetValue(ctx, block, GetNodeValue(InitState, ctx, block));
        empty->addIncoming(ConstantInt::getFalse(context), block);
        BranchInst::Create(work, block);

        block = next;

        if (Switch) {
            const auto swap = BasicBlock::Create(context, "swap", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            const auto reset = GetNodeValue(Switch, ctx, block);
            if constexpr (Interruptable) {
                const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
                BranchInst::Create(stop, pass, IsEmpty(reset, block), block);
                block = pass;
            }

            const auto cast = CastInst::Create(Instruction::Trunc, reset, Type::getInt1Ty(context), "bool", block);
            BranchInst::Create(swap, skip, cast, block);

            block = swap;

            new StoreInst(GetFalse(context), statePtr, block);
            const auto output = codegenState->CreateGetValue(ctx, block);
            result->addIncoming(output, block);
            BranchInst::Create(exit, block);

            block = skip;
        }

        codegenState->CreateSetValue(ctx, block, GetNodeValue(UpdateState, ctx, block));
        empty->addIncoming(ConstantInt::getFalse(context), block);
        BranchInst::Create(work, block);

        block = stop;
        new StoreInst(GetFinish(context), statePtr, block);
        const auto output = codegenState->CreateGetValue(ctx, block);
        const auto select = SelectInst::Create(empty, GetFinish(context), output, "output", block);
        result->addIncoming(select, block);
        BranchInst::Create(exit, block);

        block = exit;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            this->Own(flow, Item);
            this->Own(flow, State);
            this->DependsOn(flow, Switch);
            this->DependsOn(flow, InitState);
            this->DependsOn(flow, UpdateState);
        }
    }

    IComputationNode* const Flow;
    IComputationExternalNode* const Item;
    IComputationExternalNode* const State;
    IComputationNode* const Switch;
    IComputationNode* const InitState;
    IComputationNode* const UpdateState;
};

template <bool Interruptable, bool UseCtx>
class TCondense1Wrapper : public TCustomValueCodegeneratorNode<TCondense1Wrapper<Interruptable, UseCtx>> {
    typedef TCustomValueCodegeneratorNode<TCondense1Wrapper<Interruptable, UseCtx>> TBaseComputation;
public:
    class TValue : public TComputationValue<TValue> {
    public:
        using TBase = TComputationValue<TValue>;

        TValue(
            TMemoryUsageInfo* memInfo,
            NUdf::TUnboxedValue&& stream,
            const TSqueezeState& state,
            TComputationContext& ctx)
            : TBase(memInfo)
            , Stream(std::move(stream))
            , Ctx(ctx)
            , State(state)
        {}

    private:
        ui32 GetTraverseCount() const final {
            return 1;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32 index) const final {
            Y_UNUSED(index);
            return Stream;
        }

        NUdf::TUnboxedValue Save() const final {
            return State.Save(Ctx);
        }

        void Load(const NUdf::TStringRef& state) final {
            State.Load(Ctx, state);
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) final {
            switch (State.Stage) {
                case ESqueezeState::Finished:
                    return NUdf::EFetchStatus::Finish;
                case ESqueezeState::NeedInit:
                    if constexpr (UseCtx) {
                        CleanupCurrentContext();
                    }
                    State.Stage = ESqueezeState::Work;
                    State.State->SetValue(Ctx, State.InitState->GetValue(Ctx));
                    break;
                default:
                    break;
            }

            while (true) {
                const auto status = Stream.Fetch(State.Item->RefValue(Ctx));
                if (status == NUdf::EFetchStatus::Yield) {
                    return status;
                }

                if (status == NUdf::EFetchStatus::Finish) {
                    break;
                }

                if (ESqueezeState::Idle == State.Stage) {
                    State.Stage = ESqueezeState::Work;
                    State.State->SetValue(Ctx, State.InitState->GetValue(Ctx));
                } else {
                    if (State.Switch) {
                        const auto& reset = State.Switch->GetValue(Ctx);
                        if (Interruptable && !reset) {
                            break;
                        }

                        if (reset.template Get<bool>()) {
                            State.Stage = ESqueezeState::NeedInit;
                            result = State.State->GetValue(Ctx);
                            return NUdf::EFetchStatus::Ok;
                        }
                    }

                    State.State->SetValue(Ctx, State.UpdateState->GetValue(Ctx));
                }
            }

            if (ESqueezeState::Idle == State.Stage) {
                State.Stage = ESqueezeState::Finished;
                return NUdf::EFetchStatus::Finish;
            }

            result = State.State->GetValue(Ctx);
            State.Stage = ESqueezeState::Finished;
            return NUdf::EFetchStatus::Ok;
        }

        const NUdf::TUnboxedValue Stream;
        TComputationContext& Ctx;
        TSqueezeState State;
    };

    TCondense1Wrapper(
        TComputationMutables& mutables,
        IComputationNode* stream,
        IComputationExternalNode* item,
        IComputationExternalNode* state,
        IComputationNode* outSwitch,
        IComputationNode* initState,
        IComputationNode* updateState,
        IComputationExternalNode* inSave = nullptr,
        IComputationNode* outSave = nullptr,
        IComputationExternalNode* inLoad = nullptr,
        IComputationNode* outLoad = nullptr,
        TType* stateType = nullptr)
        : TBaseComputation(mutables)
        , Stream(stream)
        , State(item, state, outSwitch, initState, updateState, inSave, outSave, inLoad, outLoad, stateType)
    {
        this->Stateless = false;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Fetch)
            return ctx.HolderFactory.Create<TSqueezeCodegenValue>(State, Fetch, ctx, Stream->GetValue(ctx));
#endif
        return ctx.HolderFactory.Create<TValue>(Stream->GetValue(ctx), State, ctx);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream);
        this->Own(State.Item);
        this->Own(State.State);
        this->DependsOn(State.Switch);
        this->DependsOn(State.InitState);
        this->DependsOn(State.UpdateState);

        this->Own(State.InSave);
        this->DependsOn(State.OutSave);
        this->Own(State.InLoad);
        this->DependsOn(State.OutLoad);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        FetchFunc = GenerateFetch(codegen);
        codegen.ExportSymbol(FetchFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (FetchFunc) {
            Fetch = reinterpret_cast<TFetchPtr>(codegen.GetPointerToFunction(FetchFunc));
        }
    }

    Function* GenerateFetch(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(State.Item);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(State.State);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        const auto& name = TBaseComputation::MakeName("Fetch");
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto stateType = Type::getInt8Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType), PointerType::getUnqual(stateType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        ctx.Annotator = &annotator;

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto valuePtr = &*++args;
        const auto statePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        const auto step = BasicBlock::Create(context, "step", ctx.Func);
        const auto none = BasicBlock::Create(context, "none", ctx.Func);
        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);

        const auto state0 = new LoadInst(stateType, statePtr, "state0", block);

        const auto select = SwitchInst::Create(state0, loop, 2U, block);
        select->addCase(ConstantInt::get(stateType, static_cast<ui8>(ESqueezeState::Finished)), none);
        select->addCase(ConstantInt::get(stateType, static_cast<ui8>(ESqueezeState::NeedInit)), step);

        block = none;
        ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), block);

        block = step;

        if constexpr (UseCtx) {
            const auto cleanup = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&CleanupCurrentContext));
            const auto cleanupType = FunctionType::get(Type::getVoidTy(context), {}, false);
            const auto cleanupPtr = CastInst::Create(Instruction::IntToPtr, cleanup, PointerType::getUnqual(cleanupType), "cleanup_ctx", block);
            CallInst::Create(cleanupType, cleanupPtr, {}, "", block);
        }

        new StoreInst(ConstantInt::get(state0->getType(), static_cast<ui8>(ESqueezeState::Work)), statePtr, block);
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(State.InitState, ctx, block));
        BranchInst::Create(loop, block);

        block = loop;

        const auto itemPtr = codegenItemArg->CreateRefValue(ctx, block);
        const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, itemPtr);

        const auto ychk = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Yield)), "ychk", block);

        const auto yies = BasicBlock::Create(context, "yies", ctx.Func);
        const auto nope = BasicBlock::Create(context, "nope", ctx.Func);
        BranchInst::Create(yies, nope, ychk, block);

        block = yies;
        ReturnInst::Create(context, status, block);

        block = nope;
        const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, status, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Finish)), "cond", block);

        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);

        BranchInst::Create(stop, good, icmp, block);
        block = good;

        const auto state1 = new LoadInst(stateType, statePtr, "state1", block);
        const auto one = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, state1, ConstantInt::get(state1->getType(), static_cast<ui8>(ESqueezeState::Idle)), "one", block);

        const auto wait = BasicBlock::Create(context, "wait", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);

        const auto phi = PHINode::Create(valueType, 2, "phi", next);

        BranchInst::Create(wait, work, one, block);
        block = wait;

        new StoreInst(ConstantInt::get(state1->getType(), static_cast<ui8>(ESqueezeState::Work)), statePtr, block);
        const auto reset = GetNodeValue(State.InitState, ctx, block);
        phi->addIncoming(reset, block);
        BranchInst::Create(next, block);

        block = work;

        if (State.Switch) {
            const auto swap = BasicBlock::Create(context, "swap", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            const auto reset = GetNodeValue(State.Switch, ctx, block);
            if constexpr (Interruptable) {
                const auto next = BasicBlock::Create(context, "next", ctx.Func);
                const auto done = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, reset, ConstantInt::get(reset->getType(), 0), "done", block);
                BranchInst::Create(stop, next, done, block);
                block = next;
            }

            const auto cast = CastInst::Create(Instruction::Trunc, reset, Type::getInt1Ty(context), "bool", block);
            BranchInst::Create(swap, skip, cast, block);

            block = swap;
            new StoreInst(ConstantInt::get(state1->getType(), static_cast<ui8>(ESqueezeState::NeedInit)), statePtr, block);
            SafeUnRefUnboxed(valuePtr, ctx, block);
            const auto state = codegenStateArg->CreateGetValue(ctx, block);
            new StoreInst(state, valuePtr, block);
            ValueAddRef(State.State->GetRepresentation(), valuePtr, ctx, block);
            ReturnInst::Create(context, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);

            block = skip;
        }

        const auto state = GetNodeValue(State.UpdateState, ctx, block);
        phi->addIncoming(state, block);
        BranchInst::Create(next, block);

        block = next;

        codegenStateArg->CreateSetValue(ctx, block, phi);
        BranchInst::Create(loop, block);

        block = stop;
        const auto state2 = new LoadInst(stateType, statePtr, "state2", block);

        const auto full = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, state2, ConstantInt::get(state2->getType(), static_cast<ui8>(ESqueezeState::Work)), "full", block);
        new StoreInst(ConstantInt::get(state2->getType(), static_cast<ui8>(ESqueezeState::Finished)), statePtr, block);

        const auto fill = BasicBlock::Create(context, "fill", ctx.Func);
        BranchInst::Create(fill, yies, full, block);

        block = fill;

        SafeUnRefUnboxed(valuePtr, ctx, block);
        const auto result = codegenStateArg->CreateGetValue(ctx, block);
        new StoreInst(result, valuePtr, block);
        ValueAddRef(State.State->GetRepresentation(), valuePtr, ctx, block);
        ReturnInst::Create(context, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);

        return ctx.Func;
    }

    using TFetchPtr = TSqueezeCodegenValue::TFetchPtr;

    Function* FetchFunc = nullptr;

    TFetchPtr Fetch = nullptr;
#endif
    IComputationNode* const Stream;
    TSqueezeState State;
};

}

template <bool UseCtx>
IComputationNode* WrapCondense1Impl(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto initState = LocateNode(ctx.NodeLocator, callable, 2);
    const auto outSwitch = LocateNode(ctx.NodeLocator, callable, 4);
    const auto updateState = LocateNode(ctx.NodeLocator, callable, 5);
    const auto item = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto state = LocateExternalNode(ctx.NodeLocator, callable, 3);

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(4), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool.");

    const auto type = callable.GetType()->GetReturnType();
    if (type->IsFlow()) {
        const auto kind = GetValueRepresentation(AS_TYPE(TFlowType, type)->GetItemType());
        if (isOptional) {
            return new TCondense1FlowWrapper<true, UseCtx>(ctx.Mutables, kind, stream, item, state, outSwitch, initState, updateState);
        } else {
            return new TCondense1FlowWrapper<false, UseCtx>(ctx.Mutables, kind, stream, item, state, outSwitch, initState, updateState);
        }
    } else {
        if (isOptional) {
            return new TCondense1Wrapper<true, UseCtx>(ctx.Mutables, stream, item, state, outSwitch, initState, updateState);
        } else {
            return new TCondense1Wrapper<false, UseCtx>(ctx.Mutables, stream, item, state, outSwitch, initState, updateState);
        }
    }
}

IComputationNode* WrapCondense1(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 6 || callable.GetInputsCount() == 7, "Expected 6 or 7 args");
    bool useCtx = false;
    if (callable.GetInputsCount() >= 7) {
        useCtx = AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<bool>();
    }

    if (useCtx) {
        return WrapCondense1Impl<true>(callable, ctx);
    } else {
        return WrapCondense1Impl<false>(callable, ctx);
    }
}

IComputationNode* WrapSqueeze1(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 9, "Expected 9 args");

    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto initState = LocateNode(ctx.NodeLocator, callable, 2);
    const auto updateState = LocateNode(ctx.NodeLocator, callable, 4);
    const auto item = LocateExternalNode(ctx.NodeLocator, callable, 1);
    const auto state = LocateExternalNode(ctx.NodeLocator, callable, 3);

    IComputationExternalNode* inSave = nullptr;
    IComputationNode* outSave = nullptr;
    IComputationExternalNode* inLoad = nullptr;
    IComputationNode* outLoad = nullptr;

    const auto hasSaveLoad = !callable.GetInput(6).GetStaticType()->IsVoid();
    if (hasSaveLoad) {
        outSave = LocateNode(ctx.NodeLocator, callable, 6);
        outLoad = LocateNode(ctx.NodeLocator, callable, 8);
        inSave = LocateExternalNode(ctx.NodeLocator, callable, 5);
        inLoad = LocateExternalNode(ctx.NodeLocator, callable, 7);
    }
    const auto stateType = hasSaveLoad ? callable.GetInput(6).GetStaticType() : nullptr;

    return new TCondense1Wrapper<false, false>(ctx.Mutables, stream, item, state, nullptr, initState, updateState, inSave, outSave, inLoad, outLoad, stateType);
}

}
}
