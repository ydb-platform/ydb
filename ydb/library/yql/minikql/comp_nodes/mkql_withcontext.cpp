#include "mkql_withcontext.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/context.h>

#include <util/generic/scope.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TWithContextWrapper : public TMutableComputationNode<TWithContextWrapper> {
    typedef TMutableComputationNode<TWithContextWrapper> TBaseComputation;
public:
    TWithContextWrapper(TComputationMutables& mutables, const std::string_view& contextType, IComputationNode* arg)
        : TBaseComputation(mutables)
        , Arg(arg)
        , ContextType(contextType)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto prev = TlsAllocState->CurrentContext;
        TlsAllocState->CurrentContext = PgInitializeContext(ContextType);
        Y_DEFER {
            PgDestroyContext(ContextType, TlsAllocState->CurrentContext);
            TlsAllocState->CurrentContext = prev;
        };

        TPAllocScope scope;
        return Arg->GetValue(compCtx).Release();
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Arg);
    }

    IComputationNode* const Arg;
    const std::string_view ContextType;
};

struct TState : public TComputationValue<TState> {
    TState(TMemoryUsageInfo* memInfo, const std::string_view& contextType)
        : TComputationValue(memInfo)
        , ContextType(contextType)
        , Ctx(PgInitializeContext(ContextType))
    {
        Scope.Detach();
    }

    void Attach() {
        Scope.Attach();
        PrevContext = TlsAllocState->CurrentContext;
        TlsAllocState->CurrentContext = Ctx;
    }

    void Detach(const bool cleanup) {
        if (cleanup)
            Cleanup();

        Scope.Detach();
        TlsAllocState->CurrentContext = PrevContext;
    }

    ~TState() {
        Cleanup();
    }
private:
    void Cleanup() {
        if (Ctx) {
            PgDestroyContext(ContextType, Ctx);
            Ctx = nullptr;
            Scope.Cleanup();
        }
    }

    const std::string_view ContextType;
    void* Ctx;
    TPAllocScope Scope;
    void* PrevContext = nullptr;
};

class TWithContextFlowWrapper : public TStatefulFlowCodegeneratorNode<TWithContextFlowWrapper> {
using TBaseComputation = TStatefulFlowCodegeneratorNode<TWithContextFlowWrapper>;
public:
    TWithContextFlowWrapper(TComputationMutables& mutables, const std::string_view& contextType,
        EValueRepresentation kind, IComputationNode* flow)
        : TBaseComputation(mutables, flow, kind, EValueRepresentation::Any)
        , Flow(flow)
        , ContextType(contextType)
    {}

    NUdf::TUnboxedValuePod DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx) const {
        if (!stateValue.HasValue()) {
            MakeState(ctx, stateValue);
        }

        auto& state = *static_cast<TState*>(stateValue.AsBoxed().Get());
        state.Attach();

        auto item = Flow->GetValue(ctx);
        state.Detach(item.IsFinish());
        return item.Release();
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto statePtrType = PointerType::getUnqual(structPtrType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWithContextFlowWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeType, makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(valueType, statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto attachFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Attach));
        const auto attachFuncType = FunctionType::get(Type::getVoidTy(context), { statePtrType }, false);
        const auto attachFuncPtr = CastInst::Create(Instruction::IntToPtr, attachFunc, PointerType::getUnqual(attachFuncType), "attach", block);
        CallInst::Create(attachFuncType, attachFuncPtr, { stateArg }, "", block);

        const auto value = GetNodeValue(Flow, ctx, block);
        const auto finish = IsFinish(value, block);

        const auto detachFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Detach));
        const auto detachFuncType = FunctionType::get(Type::getVoidTy(context), { statePtrType, finish->getType() }, false);
        const auto detachFuncPtr = CastInst::Create(Instruction::IntToPtr, detachFunc, PointerType::getUnqual(detachFuncType), "detach", block);
        CallInst::Create(detachFuncType, detachFuncPtr, { stateArg, finish }, "", block);

        return value;
    }
#endif
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ContextType);
    }

    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow);
    }

    IComputationNode* const Flow;
    const std::string_view ContextType;
};

class TWithContextWideFlowWrapper : public TStatefulWideFlowComputationNode<TWithContextWideFlowWrapper> {
using TBaseComputation = TStatefulWideFlowComputationNode<TWithContextWideFlowWrapper>;
public:
    TWithContextWideFlowWrapper(TComputationMutables& mutables, IComputationWideFlowNode* flow,
        const std::string_view& contextType)
        : TBaseComputation(mutables, flow, EValueRepresentation::Any)
        , Flow(flow)
        , ContextType(contextType)
    {}

    EFetchResult DoCalculate(NUdf::TUnboxedValue& stateValue, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        if (!stateValue.HasValue()) {
            MakeState(ctx, stateValue);
        }

        auto& state = *static_cast<TState*>(stateValue.AsBoxed().Get());
        state.Attach();

        const auto status = Flow->FetchValues(ctx, output);
        state.Detach(status == EFetchResult::Finish);
        return status;
    }
/*
#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        Cerr << Flow->DebugString() << Endl;
        Y_FAIL("bad");
        auto& context = ctx.Codegen.GetContext();

        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);
        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto statePtrType = PointerType::getUnqual(structPtrType);

        const auto make = BasicBlock::Create(context, "make", ctx.Func);
        const auto main = BasicBlock::Create(context, "main", ctx.Func);

        BranchInst::Create(main, make, HasValue(statePtr, block), block);
        block = make;

        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto makeFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TWithContextWideFlowWrapper::MakeState));
        const auto makeType = FunctionType::get(Type::getVoidTy(context), {self->getType(), ctx.Ctx->getType(), statePtr->getType()}, false);
        const auto makeFuncPtr = CastInst::Create(Instruction::IntToPtr, makeFunc, PointerType::getUnqual(makeType), "function", block);
        CallInst::Create(makeFuncPtr, {self, ctx.Ctx, statePtr}, "", block);
        BranchInst::Create(main, block);

        block = main;

        const auto state = new LoadInst(statePtr, "state", block);
        const auto half = CastInst::Create(Instruction::Trunc, state, Type::getInt64Ty(context), "half", block);
        const auto stateArg = CastInst::Create(Instruction::IntToPtr, half, statePtrType, "state_arg", block);

        const auto attachFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Attach));
        const auto attachFuncType = FunctionType::get(Type::getVoidTy(context), { statePtrType }, false);
        const auto attachFuncPtr = CastInst::Create(Instruction::IntToPtr, attachFunc, PointerType::getUnqual(attachFuncType), "attach", block);
        CallInst::Create(attachFuncPtr, { stateArg }, "", block);

        auto getres = GetNodeValues(Flow, ctx, block);
        const auto array = new AllocaInst(ArrayType::get(valueType, getres.second.size()), 0U, "array", &ctx.Func->getEntryBlock().back());
        auto i = 0;
        for (auto& getter : getres.second) {
            const auto itemPtr = GetElementPtrInst::CreateInBounds(array, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, i)}, "item_ptr", &ctx.Func->getEntryBlock().back());
            const auto item = getter(ctx, block);
            ValueAddRef(EValueRepresentation::Any, item, ctx, block);
            new StoreInst(item, itemPtr, block);
            getter = [itemPtr] (const TCodegenContext& ctx, BasicBlock*& block) {
                const auto item = new LoadInst(itemPtr, "item", block);
                ValueRelease(EValueRepresentation::Any, item, ctx, block);
                return item;
            };
            ++i;
        }

        const auto finish = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, getres.first, ConstantInt::get(getres.first->getType(), static_cast<i32>(EFetchResult::Finish)), "finish", block);

        const auto detachFunc = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TState::Detach));
        const auto detachFuncType = FunctionType::get(Type::getVoidTy(context), { statePtrType, finish->getType() }, false);
        const auto detachFuncPtr = CastInst::Create(Instruction::IntToPtr, detachFunc, PointerType::getUnqual(detachFuncType), "detach", block);
        CallInst::Create(detachFuncPtr, { stateArg, finish }, "", block);

        return getres;
    }
#endif
*/
private:
    void MakeState(TComputationContext& ctx, NUdf::TUnboxedValue& state) const {
        state = ctx.HolderFactory.Create<TState>(ContextType);
    }

    void RegisterDependencies() const final {
        this->FlowDependsOn(Flow);
    }

    IComputationWideFlowNode* const Flow;
    const std::string_view ContextType;
};

}

IComputationNode* WrapWithContext(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto contextTypeData = AS_VALUE(TDataLiteral, callable.GetInput(0));
    const auto contextType = contextTypeData->AsValue().AsStringRef();
    const auto arg = LocateNode(ctx.NodeLocator, callable, 1);
    if (callable.GetInput(1).GetStaticType()->IsFlow()) {
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(arg)) {
            return new TWithContextWideFlowWrapper(ctx.Mutables, wide, contextType);
        } else {
            const auto type = callable.GetType()->GetReturnType();
            return new TWithContextFlowWrapper(ctx.Mutables, contextType, GetValueRepresentation(type), arg);
        }
    } else {
        MKQL_ENSURE(!callable.GetInput(1).GetStaticType()->IsStream(), "Stream is not expected here");
        return new TWithContextWrapper(ctx.Mutables, contextType, arg);
    }
}

}
}
