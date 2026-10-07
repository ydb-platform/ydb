#include "mkql_withcontext.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/parser/pg_wrapper/interface/context.h>

#include <util/generic/scope.h>

namespace NKikimr::NMiniKQL {

namespace {

class TWithContextWrapper: public TMutableComputationNode<TWithContextWrapper> {
    using TBaseComputation = TMutableComputationNode<TWithContextWrapper>;

public:
    TWithContextWrapper(TComputationMutables& mutables, const std::string_view& contextType, IComputationNode* arg)
        : TBaseComputation(mutables)
        , Arg_(arg)
        , ContextType_(contextType)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto prev = TlsAllocState->CurrentContext;
        TlsAllocState->CurrentContext = PgInitializeContext(ContextType_);
        Y_DEFER {
            PgDestroyContext(ContextType_, TlsAllocState->CurrentContext);
            TlsAllocState->CurrentContext = prev;
        };

        TPAllocScope scope;
        return Arg_->GetValue(compCtx).Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg_);
    }

    IComputationNode* const Arg_;
    const std::string_view ContextType_;
};

struct TState: public TComputationValue<TState> {
    TState(TMemoryUsageInfo* memInfo, const std::string_view& contextType)
        : TComputationValue(memInfo)
        , ContextType_(contextType)
        , Ctx_(PgInitializeContext(ContextType_))
    {
        Scope_.Detach();
    }

    void Attach() {
        Scope_.Attach();
        PrevContext_ = TlsAllocState->CurrentContext;
        TlsAllocState->CurrentContext = Ctx_;
    }

    void Detach(const bool cleanup) {
        if (cleanup) {
            Cleanup();
        }

        Scope_.Detach();
        TlsAllocState->CurrentContext = PrevContext_;
    }

    ~TState() override {
        Cleanup();
    }

private:
    void Cleanup() {
        if (Ctx_) {
            PgDestroyContext(ContextType_, Ctx_);
            Ctx_ = nullptr;
            Scope_.Cleanup();
        }
    }

    const std::string_view ContextType_;
    void* Ctx_;
    TPAllocScope Scope_;
    void* PrevContext_ = nullptr;
};

class TWithContextFlowWrapper: public TStatefulFlowCodegeneratorNode<TWithContextFlowWrapper> {
    using TBaseComputation = TStatefulFlowCodegeneratorNode<TWithContextFlowWrapper>;

public:
    TWithContextFlowWrapper(TComputationMutables& mutables, const std::string_view& contextType,
                            EValueRepresentation kind, IComputationNode* flow)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Any)
        , Flow_(flow)
        , ContextType_(contextType)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (!stateValue.HasValue()) {
            MakeState(ctx, stateValue);
        }

        auto& state = *static_cast<TState*>(stateValue.AsBoxed().Get());
        state.Attach();

        auto item = Flow_->GetValue(ctx);
        state.Detach(item.IsFinish());
        return item.Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto statePtrType = PointerType::getUnqual(structPtrType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TWithContextFlowWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        EmitFunctionCall<&TState::Attach>(Type::getVoidTy(context), {stateArg}, ctx, block);

        const auto value = GetNodeValue(Flow_, ctx, block);
        const auto finish = IsFinish(value, block, context);

        EmitFunctionCall<&TState::Detach>(Type::getVoidTy(context), {stateArg, finish}, ctx, block);

        return value;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ContextType_);
    }

    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow_);
    }

    IComputationNode* const Flow_;
    const std::string_view ContextType_;
};

class TWithContextWideFlowWrapper: public TStatefulWideFlowCodegeneratorNode<TWithContextWideFlowWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TWithContextWideFlowWrapper>;

public:
    TWithContextWideFlowWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow,
                                const std::string_view& contextType)
        : TBaseComputation(mutables, flow, EValueRepresentation::Any)
        , Flow_(flow)
        , ContextType_(contextType)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
        if (!stateValue.HasValue()) {
            MakeState(ctx, stateValue);
        }

        auto& state = *static_cast<TState*>(stateValue.AsBoxed().Get());
        state.Attach();

        const auto status = Flow_->FetchValues(ctx, output);
        state.Detach(status == EFetchResult::Finish);
        return status;
    }
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto statePtrType = PointerType::getUnqual(structPtrType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block, context), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        EmitFunctionCall<&TWithContextWideFlowWrapper::MakeState>(Type::getVoidTy(context), {self, ctx.Ctx, statePtr}, ctx, block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(statePtrType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        EmitFunctionCall<&TState::Attach>(Type::getVoidTy(context), {stateArg}, ctx, block);

        auto getres = GetNodeValues(Flow_, ctx, block);

        const auto special = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SLE, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Yield)), "special", block);

        BranchInst::Create(exit, good, special, block);

        block = good;

        const auto arrayType = ArrayType::get(valueType, getres.second.size());
        const auto arrayPtr = new AllocaInst(arrayType, 0U, "array_ptr", &ctx.Func->getEntryBlock().back());
        Value* array = UndefValue::get(arrayType);
        for (auto idx = 0U; idx < getres.second.size(); ++idx) {
            const auto item = getres.second[idx](ctx, block);
            array = InsertValueInst::Create(array, item, {idx}, (TString("value_") += ToString(idx)).c_str(), block);
        }
        new StoreInst(array, arrayPtr, block);

        BranchInst::Create(exit, block);

        block = exit;

        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Finish)), "finish", block);

        EmitFunctionCall<&TState::Detach>(Type::getVoidTy(context), {stateArg, finish}, ctx, block);

        for (auto idx = 0U; idx < getres.second.size(); ++idx) {
            getres.second[idx] = [idx, arrayPtr, arrayType, indexType, valueType](const TCodegenContext& ctx, BasicBlock*& block) {
                Y_UNUSED(ctx);
                const auto itemPtr = GetElementPtrInst::CreateInBounds(arrayType, arrayPtr, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx)}, (TString("ptr_") += ToString(idx)).c_str(), block);
                return new LoadInst(valueType, itemPtr, (TString("item_") += ToString(idx)).c_str(), block);
            };
        }

        return getres;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ContextType_);
    }

    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow_);
    }

    IComputationWideFlowNode* const Flow_;
    const std::string_view ContextType_;
};

} // namespace

IComputationNode* WrapWithContext(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto contextTypeData = AS_VALUE(TDataLiteral, callable.GetInput(0));
    const auto contextType = contextTypeData->AsValue().AsStringRef();
    const auto arg = LocateNode(ctx.NodeLocator, callable, 1);
    if (const auto type = callable.GetType()->GetReturnType(); type->IsFlow()) {
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(arg)) {
            return new TWithContextWideFlowWrapper(ctx.Mutables, wide, contextType);
        } else {
            return new TWithContextFlowWrapper(ctx.Mutables, contextType, GetValueRepresentation(type), arg);
        }
    } else {
        MKQL_ENSURE(!callable.GetInput(1).GetStaticType()->IsStream(), "Stream is not expected here");
        return new TWithContextWrapper(ctx.Mutables, contextType, arg);
    }
}

} // namespace NKikimr::NMiniKQL
