#include "mkql_iterator.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TIteratorWrapper : public TMutableCodegeneratorNode<TIteratorWrapper> {
    typedef TMutableCodegeneratorNode<TIteratorWrapper> TBaseComputation;
public:
    TIteratorWrapper(TComputationMutables& mutables, IComputationNode* list, TComputationNodePtrVector&& dependentNodes)
        : TBaseComputation(mutables, EValueRepresentation::Boxed), List(list), DependentNodes(std::move(dependentNodes))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.CreateIteratorOverList(List->GetValue(ctx).Release());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto value = GetNodeValue(List, ctx, block);

        const auto factory = ctx.GetFactory();
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::CreateIteratorOverList));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto signature = FunctionType::get(value->getType(), {factory->getType(), value->getType()}, false);
            const auto creator = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(signature), "creator", block);
            const auto output = CallInst::Create(signature, creator, {factory, value}, "output", block);
            return output;
        } else {
            const auto place = new AllocaInst(value->getType(), 0U, "place", block);
            new StoreInst(value, place, block);
            const auto signature = FunctionType::get(Type::getVoidTy(context), {factory->getType(), place->getType(), place->getType()}, false);
            const auto creator = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(signature), "creator", block);
            CallInst::Create(signature, creator, {factory, place, place}, "", block);
            const auto output = new LoadInst(value->getType(), place, "output", block);
            return output;
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(List);
        std::for_each(DependentNodes.cbegin(), DependentNodes.cend(),std::bind(&TIteratorWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode *const List;
    const TComputationNodePtrVector DependentNodes;
};

class TForwardListWrapper : public TMutableCodegeneratorNode<TForwardListWrapper> {
    typedef TMutableCodegeneratorNode<TForwardListWrapper> TBaseComputation;
public:
    TForwardListWrapper(TComputationMutables& mutables, IComputationNode* stream)
        : TBaseComputation(mutables, EValueRepresentation::Boxed), Stream(stream)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.CreateForwardList(Stream->GetValue(ctx).Release());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto value = GetNodeValue(Stream, ctx, block);

        const auto factory = ctx.GetFactory();
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::CreateForwardList));

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto signature = FunctionType::get(value->getType(), {factory->getType(), value->getType()}, false);
            const auto creator = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(signature), "creator", block);
            const auto output = CallInst::Create(signature, creator, {factory, value}, "output", block);
            return output;
        } else {
            const auto place = new AllocaInst(value->getType(), 0U, "place", block);
            new StoreInst(value, place, block);
            const auto signature = FunctionType::get(Type::getVoidTy(context), {factory->getType(), place->getType(), place->getType()}, false);
            const auto creator = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(signature), "creator", block);
            CallInst::Create(signature, creator, {factory, place, place}, "", block);
            const auto output = new LoadInst(value->getType(), place, "output", block);
            return output;
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(Stream);
    }

    IComputationNode *const Stream;
};

class TFlowForwardListWrapper : public TCustomValueCodegeneratorNode<TFlowForwardListWrapper> {
    typedef TCustomValueCodegeneratorNode<TFlowForwardListWrapper> TBaseComputation;
public:
    class TIterator : public TComputationValue<TIterator> {
    public:
        using TPtr = IComputationNode*;

        TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, TPtr flow)
            : TComputationValue<TIterator>(memInfo), CompCtx(compCtx), Flow(flow)
        {}

    private:
        bool Next(NUdf::TUnboxedValue& value) final {
            value = Flow->GetValue(CompCtx);
            if (value.IsYield()) {
                Throw();
            }
            return !value.IsFinish();
        }

        TComputationContext& CompCtx;
        const TPtr Flow;
    };

    class TCodegenIterator : public TComputationValue<TCodegenIterator> {
    public:
        using TPtr = bool (*)(TComputationContext*, NUdf::TUnboxedValuePod&);

        TCodegenIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, TPtr func)
            : TComputationValue<TCodegenIterator>(memInfo), CompCtx(compCtx), Func(func)
        {}

    private:
        bool Next(NUdf::TUnboxedValue& value) final {
            return Func(&CompCtx, value);
        }

        TComputationContext& CompCtx;
        const TPtr Func;
    };

    template <class TIterator>
    class TForwardListValue : public TCustomListValue {
    public:
        TForwardListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, typename TIterator::TPtr ptr)
            : TCustomListValue(memInfo), CompCtx(compCtx), Ptr(ptr)
        {}

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            if (const auto ptr = Ptr) {
                Ptr = nullptr;
                return CompCtx.HolderFactory.Create<TIterator>(CompCtx, ptr);
            }

            THROW yexception() << "Second pass on forward list.";
        }

        TComputationContext& CompCtx;
        mutable typename TIterator::TPtr Ptr;
    };

    TFlowForwardListWrapper(TComputationMutables& mutables, IComputationNode* flow)
        : TBaseComputation(mutables), Flow(flow)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Next)
            return ctx.HolderFactory.Create<TForwardListValue<TCodegenIterator>>(ctx, Next);
#endif
        return ctx.HolderFactory.Create<TForwardListValue<TIterator>>(ctx, Flow);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Flow);
    }

    [[noreturn]] static void Throw() {
        UdfTerminate("Unexpected flow status.");
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        NextFunc = GenerateNext(codegen);
        codegen.ExportSymbol(NextFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (NextFunc)
            Next = reinterpret_cast<TCodegenIterator::TPtr>(codegen.GetPointerToFunction(NextFunc));
    }

    Function* GenerateNext(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("Next");
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto contextType = GetCompContextType(context);
        const auto funcType = FunctionType::get(Type::getInt1Ty(context), {PointerType::getUnqual(contextType), PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        SafeUnRefUnboxed(valuePtr, ctx, block);
        GetNodeValue(valuePtr, Flow, ctx, block);

        const auto value = new LoadInst(valueType, valuePtr, "value", block);

        const auto kill = BasicBlock::Create(context, "kill", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(kill, good, IsYield(value, block), block);

        block = kill;
        const auto doThrow = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TFlowForwardListWrapper::Throw));
        const auto doThrowType = FunctionType::get(Type::getVoidTy(context), {}, false);
        const auto doThrowPtr = CastInst::Create(Instruction::IntToPtr, doThrow, PointerType::getUnqual(doThrowType), "thrower", block);
        CallInst::Create(doThrowType, doThrowPtr, {}, "", block)->setTailCall();
        new UnreachableInst(context, block);

        block = good;
        const auto result = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, value, GetFinish(context), "result", block);
        ReturnInst::Create(context, result, block);
        return ctx.Func;
    }

    Function* NextFunc = nullptr;

    TCodegenIterator::TPtr Next = nullptr;
#endif
    IComputationNode* const Flow;
};

}

IComputationNode* WrapEmptyIterator(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 0, "Expected 0 arg");
    const auto type = callable.GetType()->GetReturnType();
    if (type->IsFlow()) {
        return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValuePod::MakeFinish());
    } else if (type->IsStream()) {
        return ctx.NodeFactory.CreateEmptyNode();
    }
    THROW yexception() << "Expected flow or stream.";
}

IComputationNode* WrapIterator(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() >= 1, "Expected at least 1 arg");
    const auto type = callable.GetInput(0).GetStaticType();
    MKQL_ENSURE(type->IsList(), "Requires list");

    TComputationNodePtrVector dependentNodes(callable.GetInputsCount() - 1);
    for (ui32 i = 1; i < callable.GetInputsCount(); ++i) {
        dependentNodes[i - 1] = LocateNode(ctx.NodeLocator, callable, i);
    }

    return new TIteratorWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), std::move(dependentNodes));
}

IComputationNode* WrapForwardList(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    const auto type = callable.GetInput(0).GetStaticType();
    if (type->IsFlow()) {
        return new TFlowForwardListWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
    } else if (type->IsStream()) {
        return new TForwardListWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
    }
    THROW yexception() << "Expected flow or stream.";
}

}
}
