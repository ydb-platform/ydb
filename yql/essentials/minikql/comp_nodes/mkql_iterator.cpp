#include "mkql_iterator.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

class TIteratorWrapper: public TMutableCodegeneratorNode<TIteratorWrapper> {
    using TBaseComputation = TMutableCodegeneratorNode<TIteratorWrapper>;

public:
    TIteratorWrapper(TComputationMutables& mutables, IComputationNode* list, TComputationNodePtrVector&& dependentNodes)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , List_(list)
        , DependentNodes_(std::move(dependentNodes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.CreateIteratorOverList(List_->GetValue(ctx).Release());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        const auto value = GetNodeValue(List_, ctx, block);

        const auto factory = ctx.GetFactory();

        return EmitFunctionCall<&THolderFactory::CreateIteratorOverList>(value->getType(), {factory, value}, ctx, block);
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(List_);
        std::for_each(DependentNodes_.cbegin(), DependentNodes_.cend(), std::bind(&TIteratorWrapper::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode* const List_;
    const TComputationNodePtrVector DependentNodes_;
};

class TForwardListWrapper: public TMutableCodegeneratorNode<TForwardListWrapper> {
    using TBaseComputation = TMutableCodegeneratorNode<TForwardListWrapper>;

public:
    TForwardListWrapper(TComputationMutables& mutables, IComputationNode* stream)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Stream_(stream)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.CreateForwardList(Stream_->GetValue(ctx).Release());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        const auto value = GetNodeValue(Stream_, ctx, block);

        const auto factory = ctx.GetFactory();

        return EmitFunctionCall<&THolderFactory::CreateForwardList>(value->getType(), {factory, value}, ctx, block);
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(Stream_);
    }

    IComputationNode* const Stream_;
};

class TFlowForwardListWrapper: public TCustomValueCodegeneratorNode<TFlowForwardListWrapper> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TFlowForwardListWrapper>;

public:
    class TIterator: public TComputationValue<TIterator> {
    public:
        using TPtr = IComputationNode*;

        TIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, TPtr flow)
            : TComputationValue<TIterator>(memInfo)
            , CompCtx_(compCtx)
            , Flow_(flow)
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& value) final {
            value = Flow_->GetValue(CompCtx_);
            if (value.IsYield()) {
                Throw();
            }
            return !value.IsFinish();
        }

        TComputationContext& CompCtx_;
        const TPtr Flow_;
    };

    class TCodegenIterator: public TComputationValue<TCodegenIterator> {
    public:
        using TPtr = bool (*)(TComputationContext*, NUdf::TUnboxedValuePod&);

        TCodegenIterator(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, TPtr func)
            : TComputationValue<TCodegenIterator>(memInfo)
            , CompCtx_(compCtx)
            , Func_(func)
        {
        }

    private:
        bool Next(NUdf::TUnboxedValue& value) final {
            return Func_(&CompCtx_, value);
        }

        TComputationContext& CompCtx_;
        const TPtr Func_;
    };

    template <class TIterator>
    class TForwardListValue: public TCustomListValue {
    public:
        TForwardListValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, typename TIterator::TPtr ptr)
            : TCustomListValue(memInfo)
            , CompCtx_(compCtx)
            , Ptr_(ptr)
        {
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const final {
            if (const auto ptr = Ptr_) {
                Ptr_ = nullptr;
                return CompCtx_.HolderFactory.Create<TIterator>(CompCtx_, ptr);
            }

            THROW yexception() << "Second pass on forward list.";
        }

        TComputationContext& CompCtx_;
        mutable typename TIterator::TPtr Ptr_;
    };

    TFlowForwardListWrapper(TComputationMutables& mutables, IComputationNode* flow)
        : TBaseComputation(mutables)
        , Flow_(flow)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Next_) {
            return ctx.HolderFactory.Create<TForwardListValue<TCodegenIterator>>(ctx, Next_);
        }
#endif
        return ctx.HolderFactory.Create<TForwardListValue<TIterator>>(ctx, Flow_);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Flow_);
    }

    [[noreturn]] static void Throw() {
        UdfTerminate("Unexpected flow status.");
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        NextFunc_ = GenerateNext(codegen);
        codegen.ExportSymbol(NextFunc_);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (NextFunc_) {
            Next_ = reinterpret_cast<TCodegenIterator::TPtr>(codegen.GetPointerToFunction(NextFunc_));
        }
    }

    Function* GenerateNext(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("Next");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto contextType = GetCompContextType(context);
        const auto funcType = FunctionType::get(Type::getInt1Ty(context), {PointerType::getUnqual(contextType), PointerType::getUnqual(valueType)}, /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto valuePtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        SafeUnRefUnboxedOne(valuePtr, ctx, block);
        GetNodeValue(valuePtr, Flow_, ctx, block);

        const auto value = new LoadInst(valueType, valuePtr, "value", block);

        const auto kill = BasicBlock::Create(context, "kill", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);

        BranchInst::Create(kill, good, IsYield(value, block, context), block);

        block = kill;
        EmitFunctionCall<&TFlowForwardListWrapper::Throw>(Type::getVoidTy(context), {}, ctx, block);
        new UnreachableInst(context, block);

        block = good;
        const auto result = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, value, GetFinish(context), "result", block);
        ReturnInst::Create(context, result, block);
        return ctx.Func;
    }

    Function* NextFunc_ = nullptr;

    TCodegenIterator::TPtr Next_ = nullptr;
#endif
    IComputationNode* const Flow_;
};

} // namespace

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

} // namespace NKikimr::NMiniKQL
