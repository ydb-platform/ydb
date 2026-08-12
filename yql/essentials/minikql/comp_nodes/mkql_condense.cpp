#include "mkql_condense.h"
#include "mkql_squeeze_state.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE

namespace NKikimr::NMiniKQL {

namespace {

template <bool Interruptable, bool UseCtx>
class TCondenseFlowWrapper: public TStatefulFlowCodegeneratorNode<TCondenseFlowWrapper<Interruptable, UseCtx>> {
    using TBaseComputation = TStatefulFlowCodegeneratorNode<TCondenseFlowWrapper<Interruptable, UseCtx>>;

public:
    TCondenseFlowWrapper(
        TComputationMutables& mutables,
        EValueRepresentation kind,
        IComputationNode* flow,
        IComputationExternalNode* item,
        IComputationExternalNode* state,
        IComputationNode* outSwitch,
        IComputationNode* initState,
        IComputationNode* updateState)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Embedded)
        ,
        Flow_(flow)
        , Item_(item)
        , State_(state)
        , Switch_(outSwitch)
        , InitState_(initState)
        , UpdateState_(updateState)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsFinish()) {
            return state;
        }

        if (state.IsInvalid()) {
            state = NUdf::TUnboxedValuePod();
            State_->SetValue(ctx, InitState_->GetValue(ctx));
        } else if (state.HasValue()) {
            if constexpr (UseCtx) {
                CleanupCurrentContext();
            }
            state = NUdf::TUnboxedValuePod();
            State_->SetValue(ctx, InitState_->GetValue(ctx));
            State_->SetValue(ctx, UpdateState_->GetValue(ctx));
        }

        while (true) {
            auto item = Flow_->GetValue(ctx);
            if (item.IsYield()) {
                return item.Release();
            }

            if (item.IsFinish()) {
                break;
            }

            Item_->SetValue(ctx, std::move(item));

            if (Switch_) {
                const auto& reset = Switch_->GetValue(ctx);
                if (Interruptable && !reset) {
                    break;
                }

                if (reset.template Get<bool>()) {
                    state = NUdf::TUnboxedValuePod::Zero();
                    return State_->GetValue(ctx).Release();
                }
            }

            State_->SetValue(ctx, UpdateState_->GetValue(ctx));
        }

        state = NUdf::TUnboxedValue::MakeFinish();
        return State_->GetValue(ctx).Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto codegenItem = dynamic_cast<ICodegeneratorExternalNode*>(Item_);
        MKQL_ENSURE(codegenItem, "Item must be codegenerator node.");
        const auto codegenState = dynamic_cast<ICodegeneratorExternalNode*>(State_);
        MKQL_ENSURE(codegenState, "State must be codegenerator node.");

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto stop = BasicBlock::Create(context, "stop", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        const auto valueType = Type::getInt128Ty(context);
        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto result = PHINode::Create(valueType, Switch_ ? 4U : 3U, "result", exit);
        result->addIncoming(state, block);

        const auto select = SwitchInst::Create(state, work, 3U, block);
        select->addCase(GetFinish(context), exit);
        select->addCase(GetInvalid(context), init);
        select->addCase(GetFalse(context), next);

        block = init;
        new StoreInst(GetEmpty(context), statePtr, block);
        codegenState->CreateSetValue(ctx, block, GetNodeValue(InitState_, ctx, block));
        BranchInst::Create(work, block);

        block = next;

        if constexpr (UseCtx) {
            EmitFunctionCall<&CleanupCurrentContext>(Type::getVoidTy(context), {}, ctx, block);
        }

        new StoreInst(GetEmpty(context), statePtr, block);
        codegenState->CreateSetValue(ctx, block, GetNodeValue(InitState_, ctx, block));
        codegenState->CreateSetValue(ctx, block, GetNodeValue(UpdateState_, ctx, block));
        BranchInst::Create(work, block);

        block = work;
        const auto item = GetNodeValue(Flow_, ctx, block);
        result->addIncoming(item, block);

        const auto action = SwitchInst::Create(item, good, 2U, block);
        action->addCase(GetFinish(context), stop);
        action->addCase(GetYield(context), exit);

        block = good;

        codegenItem->CreateSetValue(ctx, block, item);

        if (Switch_) {
            const auto swap = BasicBlock::Create(context, "swap", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            const auto reset = GetNodeValue(Switch_, ctx, block);
            if constexpr (Interruptable) {
                const auto next = BasicBlock::Create(context, "next", ctx.Func);
                BranchInst::Create(stop, next, IsEmpty(reset, block, context), block);
                block = next;
            }

            const auto cast = CastInst::Create(Instruction::Trunc, reset, Type::getInt1Ty(context), "bool", block);
            BranchInst::Create(swap, skip, cast, block);

            block = swap;

            new StoreInst(GetFalse(context), statePtr, block);
            result->addIncoming(GetNodeValue(State_, ctx, block), block);
            BranchInst::Create(exit, block);

            block = skip;
        }

        codegenState->CreateSetValue(ctx, block, GetNodeValue(UpdateState_, ctx, block));
        BranchInst::Create(work, block);

        block = stop;
        new StoreInst(GetFinish(context), statePtr, block);
        const auto output = codegenState->CreateGetValue(ctx, block);
        result->addIncoming(output, block);
        BranchInst::Create(exit, block);

        block = exit;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow_)) {
            this->Own(flow, Item_);
            this->Own(flow, State_);
            this->DependsOn(flow, InitState_);
            this->DependsOn(flow, Switch_);
            this->DependsOn(flow, UpdateState_);
        }
    }

    IComputationNode* const Flow_;
    IComputationExternalNode* const Item_;
    IComputationExternalNode* const State_;
    IComputationNode* const Switch_;
    IComputationNode* const InitState_;
    IComputationNode* const UpdateState_;
};

template <bool Interruptable, bool UseCtx>
class TCondenseWrapper: public TCustomValueCodegeneratorNode<TCondenseWrapper<Interruptable, UseCtx>> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TCondenseWrapper<Interruptable, UseCtx>>;

public:
    class TValue: public TComputationValue<TValue> {
    public:
        using TBase = TComputationValue<TValue>;

        TValue(
            TMemoryUsageInfo* memInfo,
            NUdf::TUnboxedValue&& stream,
            const TSqueezeState& state,
            TComputationContext& ctx)
            : TBase(memInfo)
            , Stream_(std::move(stream))
            , Ctx_(ctx)
            , State_(state)
        {
        }

    private:
        ui32 GetTraverseCount() const final {
            return 1;
        }

        NUdf::TUnboxedValue GetTraverseItem(ui32 index) const final {
            Y_UNUSED(index);
            return Stream_;
        }

        NUdf::TUnboxedValue Save() const final {
            return State_.Save(Ctx_);
        }

        void Load(const NUdf::TStringRef& state) final {
            State_.Load(Ctx_, state);
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            switch (State_.Stage) {
                case ESqueezeState::Finished:
                    return NUdf::EFetchStatus::Finish;
                case ESqueezeState::Idle:
                    State_.Stage = ESqueezeState::Work;
                    State_.State->SetValue(Ctx_, State_.InitState->GetValue(Ctx_));
                    break;
                case ESqueezeState::NeedInit:
                    if constexpr (UseCtx) {
                        CleanupCurrentContext();
                    }
                    State_.Stage = ESqueezeState::Work;
                    State_.State->SetValue(Ctx_, State_.InitState->GetValue(Ctx_));
                    State_.State->SetValue(Ctx_, State_.UpdateState->GetValue(Ctx_));
                    break;
                default:
                    break;
            }

            while (true) {
                NYql::NUdf::TUnboxedValue fetchResult;
                const auto status = Stream_.Fetch(fetchResult);
                if (status == NUdf::EFetchStatus::Yield) {
                    return status;
                }

                if (status == NUdf::EFetchStatus::Finish) {
                    break;
                }

                State_.Item->SetValue(Ctx_, std::move(fetchResult));

                if (State_.Switch) {
                    const auto& reset = State_.Switch->GetValue(Ctx_);
                    if (Interruptable && !reset) {
                        break;
                    }

                    if (reset.template Get<bool>()) {
                        State_.Stage = ESqueezeState::NeedInit;
                        result = State_.State->GetValue(Ctx_);
                        return NUdf::EFetchStatus::Ok;
                    }
                }

                State_.State->SetValue(Ctx_, State_.UpdateState->GetValue(Ctx_));
            }

            State_.Stage = ESqueezeState::Finished;
            result = State_.State->GetValue(Ctx_);
            return NUdf::EFetchStatus::Ok;
        }

        const NUdf::TUnboxedValue Stream_;
        TComputationContext& Ctx_;
        TSqueezeState State_;
    };

    TCondenseWrapper(
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
        , Stream_(stream)
        , State_(item, state, outSwitch, initState, updateState, inSave, outSave, inLoad, outLoad, stateType)
    {
        this->Stateless_ = false;
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Fetch_) {
            return ctx.HolderFactory.Create<TSqueezeCodegenValue>(State_, Fetch_, ctx, Stream_->GetValue(ctx));
        }
#endif
        return ctx.HolderFactory.Create<TValue>(Stream_->GetValue(ctx), State_, ctx);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Stream_);
        this->Own(State_.Item);
        this->Own(State_.State);
        this->DependsOn(State_.Switch);
        this->DependsOn(State_.InitState);
        this->DependsOn(State_.UpdateState);

        this->Own(State_.InSave);
        this->DependsOn(State_.OutSave);
        this->Own(State_.InLoad);
        this->DependsOn(State_.OutLoad);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        FetchFunc_ = GenerateFetch(codegen);
        codegen.ExportSymbol(FetchFunc_);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (FetchFunc_) {
            Fetch_ = reinterpret_cast<TFetchPtr>(codegen.GetPointerToFunction(FetchFunc_));
        }
    }

    Function* GenerateFetch(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto codegenItemArg = dynamic_cast<ICodegeneratorExternalNode*>(State_.Item);
        const auto codegenStateArg = dynamic_cast<ICodegeneratorExternalNode*>(State_.State);

        MKQL_ENSURE(codegenItemArg, "Item arg must be codegenerator node.");
        MKQL_ENSURE(codegenStateArg, "State arg must be codegenerator node.");

        const auto& name = TBaseComputation::MakeName("Fetch");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto stateType = Type::getInt8Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType), PointerType::getUnqual(stateType)}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;
        const auto valuePtr = &*++args;
        const auto statePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = static_cast<Value*>(containerArg);

        const auto state = new LoadInst(stateType, statePtr, "state", block);

        const auto init = BasicBlock::Create(context, "init", ctx.Func);
        const auto next = BasicBlock::Create(context, "next", ctx.Func);
        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto none = BasicBlock::Create(context, "none", ctx.Func);

        const auto select = SwitchInst::Create(state, work, 3U, block);
        select->addCase(ConstantInt::get(stateType, static_cast<ui8>(ESqueezeState::Finished)), none);
        select->addCase(ConstantInt::get(stateType, static_cast<ui8>(ESqueezeState::Idle)), init);
        select->addCase(ConstantInt::get(stateType, static_cast<ui8>(ESqueezeState::NeedInit)), next);

        block = none;
        ReturnInst::Create(context, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Finish)), block);

        block = init;

        new StoreInst(ConstantInt::get(state->getType(), static_cast<ui8>(ESqueezeState::Work)), statePtr, block);
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(State_.InitState, ctx, block));
        BranchInst::Create(work, block);

        block = next;

        if constexpr (UseCtx) {
            EmitFunctionCall<&CleanupCurrentContext>(Type::getVoidTy(context), {}, ctx, block);
        }

        new StoreInst(ConstantInt::get(state->getType(), static_cast<ui8>(ESqueezeState::Work)), statePtr, block);
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(State_.InitState, ctx, block));
        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(State_.UpdateState, ctx, block));

        BranchInst::Create(work, block);

        block = work;

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);

        BranchInst::Create(loop, block);
        block = loop;

        const auto [status, itemPtr] = RefValueWithCallResult(codegenItemArg, ctx, block, [&](Value* itemPtr) {
            return CallBoxedValueFetch(container, ctx, block, itemPtr);
        });

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

        if (State_.Switch) {
            const auto swap = BasicBlock::Create(context, "swap", ctx.Func);
            const auto skip = BasicBlock::Create(context, "skip", ctx.Func);

            const auto reset = GetNodeValue(State_.Switch, ctx, block);
            if constexpr (Interruptable) {
                const auto pass = BasicBlock::Create(context, "pass", ctx.Func);
                const auto done = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, reset, ConstantInt::get(reset->getType(), 0), "done", block);
                BranchInst::Create(stop, pass, done, block);
                block = pass;
            }

            const auto cast = CastInst::Create(Instruction::Trunc, reset, Type::getInt1Ty(context), "bool", block);

            BranchInst::Create(swap, skip, cast, block);

            block = swap;

            new StoreInst(ConstantInt::get(state->getType(), static_cast<ui8>(ESqueezeState::NeedInit)), statePtr, block);
            SafeUnRefUnboxedOne(valuePtr, ctx, block);
            const auto state = codegenStateArg->CreateGetValue(ctx, block);
            new StoreInst(state, valuePtr, block);
            ValueAddRef(State_.State->GetRepresentation(), valuePtr, ctx, block);
            ReturnInst::Create(context, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);

            block = skip;
        }

        codegenStateArg->CreateSetValue(ctx, block, GetNodeValue(State_.UpdateState, ctx, block));
        BranchInst::Create(loop, block);

        block = stop;
        new StoreInst(ConstantInt::get(state->getType(), static_cast<ui8>(ESqueezeState::Finished)), statePtr, block);
        SafeUnRefUnboxedOne(valuePtr, ctx, block);
        const auto result = codegenStateArg->CreateGetValue(ctx, block);
        new StoreInst(result, valuePtr, block);
        ValueAddRef(State_.State->GetRepresentation(), valuePtr, ctx, block);
        ReturnInst::Create(context, ConstantInt::get(status->getType(), static_cast<ui32>(NUdf::EFetchStatus::Ok)), block);

        return ctx.Func;
    }

    using TFetchPtr = TSqueezeCodegenValue::TFetchPtr;

    Function* FetchFunc_ = nullptr;

    TFetchPtr Fetch_ = nullptr;
#endif
    IComputationNode* const Stream_;
    TSqueezeState State_;
};

} // namespace

template <bool UseCtx>
IComputationNode* WrapCondenseImpl(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto initState = LocateNode(ctx.NodeLocator, callable, 1);
    const auto outSwitch = LocateNode(ctx.NodeLocator, callable, 4);
    const auto updateState = LocateNode(ctx.NodeLocator, callable, 5);
    const auto item = LocateExternalNode(ctx.NodeLocator, callable, 2);
    const auto state = LocateExternalNode(ctx.NodeLocator, callable, 3);

    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(4), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<bool>::Id, "Expected bool.");

    const auto type = callable.GetType()->GetReturnType();
    if (type->IsFlow()) {
        const auto kind = GetValueRepresentation(AS_TYPE(TFlowType, type)->GetItemType());
        if (isOptional) {
            return new TCondenseFlowWrapper<true, UseCtx>(ctx.Mutables, kind, stream, item, state, outSwitch, initState, updateState);
        } else {
            return new TCondenseFlowWrapper<false, UseCtx>(ctx.Mutables, kind, stream, item, state, outSwitch, initState, updateState);
        }
    } else {
        if (isOptional) {
            return new TCondenseWrapper<true, UseCtx>(ctx.Mutables, stream, item, state, outSwitch, initState, updateState);
        } else {
            return new TCondenseWrapper<false, UseCtx>(ctx.Mutables, stream, item, state, outSwitch, initState, updateState);
        }
    }
}

IComputationNode* WrapCondense(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 6 || callable.GetInputsCount() == 7, "Expected 6 or 7 args");

    bool useCtx = false;
    if (callable.GetInputsCount() >= 7) {
        useCtx = AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<bool>();
    }

    if (useCtx) {
        return WrapCondenseImpl<true>(callable, ctx);
    } else {
        return WrapCondenseImpl<false>(callable, ctx);
    }
}

IComputationNode* WrapSqueeze(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 9, "Expected 9 args");

    const auto stream = LocateNode(ctx.NodeLocator, callable, 0);
    const auto initState = LocateNode(ctx.NodeLocator, callable, 1);
    const auto updateState = LocateNode(ctx.NodeLocator, callable, 4);
    const auto item = LocateExternalNode(ctx.NodeLocator, callable, 2);
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

    return new TCondenseWrapper<false, false>(ctx.Mutables, stream, item, state, /*outSwitch=*/nullptr, initState, updateState, inSave, outSave, inLoad, outLoad, stateType);
}

} // namespace NKikimr::NMiniKQL
