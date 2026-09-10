#include "mkql_callable.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

class TCallableWrapper: public TCustomValueCodegeneratorNode<TCallableWrapper> {
    using TBaseComputation = TCustomValueCodegeneratorNode<TCallableWrapper>;

private:
    class TValue: public TComputationValue<TValue> {
    public:
        TValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, IComputationNode* resultNode,
               const TComputationExternalNodePtrVector& argNodes)
            : TComputationValue(memInfo)
            , CompCtx_(compCtx)
            , ResultNode_(resultNode)
            , ArgNodes_(argNodes)
            , Upvalues_(compCtx, resultNode, argNodes)
        {
        }

    private:
        NUdf::TUnboxedValue Run(const NUdf::IValueBuilder*, const NUdf::TUnboxedValuePod* args) const override {
            for (const auto node : ArgNodes_) {
                node->SetValue(CompCtx_, NUdf::TUnboxedValuePod(*args++));
            }

            if (!Upvalues_) {
                return ResultNode_->GetValue(CompCtx_);
            }

            Upvalues_.SetUpvalues(CompCtx_);

            const auto result = ResultNode_->GetValue(CompCtx_);

            Upvalues_.RestoreUpvalues(CompCtx_);

            return result;
        }

        TComputationContext& CompCtx_;
        IComputationNode* const ResultNode_;
        const TComputationExternalNodePtrVector ArgNodes_;
        const TComputationUpvalues Upvalues_;
    };

    class TCodegenValue: public TComputationValue<TCodegenValue> {
    public:
        using TBase = TComputationValue<TCodegenValue>;

        using TRunPtr = NUdf::TUnboxedValuePod (*)(const TComputationUpvalues*, TComputationContext*, const NUdf::TUnboxedValuePod*);

        TCodegenValue(TMemoryUsageInfo* memInfo, TRunPtr run, TComputationContext* ctx, IComputationNode* resultNode, const TComputationExternalNodePtrVector& argNodes)
            : TBase(memInfo)
            , RunFunc_(run)
            , Ctx_(ctx)
            , Upvalues_(*ctx, resultNode, argNodes)
        {
        }

    private:
        NUdf::TUnboxedValue Run(const NUdf::IValueBuilder*, const NUdf::TUnboxedValuePod* args) const override {
            return RunFunc_(&Upvalues_, Ctx_, args);
        }

        const TRunPtr RunFunc_;
        TComputationContext* const Ctx_;
        const TComputationUpvalues Upvalues_;
    };

public:
    TCallableWrapper(TComputationMutables& mutables, IComputationNode* resultNode, TComputationExternalNodePtrVector&& argNodes)
        : TBaseComputation(mutables)
        , ResultNode_(resultNode)
        , ArgNodes_(std::move(argNodes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Run_) {
            return ctx.HolderFactory.Create<TCodegenValue>(Run_, &ctx, ResultNode_, ArgNodes_);
        }
#endif
        return ctx.HolderFactory.Create<TValue>(ctx, ResultNode_, ArgNodes_);
    }

private:
    void RegisterDependencies() const final {
        for (const auto& arg : ArgNodes_) {
            Own(arg);
        }

        DependsOn(ResultNode_);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        RunFunc_ = GenerateRun(codegen);
        codegen.ExportSymbol(RunFunc_);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (RunFunc_) {
            Run_ = reinterpret_cast<TRunPtr>(codegen.GetPointerToFunction(RunFunc_));
        }
    }

    Function* GenerateRun(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("Run");
        if (const auto f = module.getFunction(name.c_str())) {
            return f;
        }

        const auto valueType = Type::getInt128Ty(context);
        const auto argsType = ArrayType::get(valueType, ArgNodes_.size());
        const auto contextType = GetCompContextType(context);
        const auto upvaluesType = StructType::get(context);

        const auto funcType = FunctionType::get(
            valueType,
            {PointerType::getUnqual(upvaluesType), PointerType::getUnqual(contextType), PointerType::getUnqual(argsType)},
            /*isVarArg=*/false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);

        auto args = ctx.Func->arg_begin();

        const auto upvalues = &*args;
        ctx.Ctx = &*++args;
        const auto argsPtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto arguments = new LoadInst(argsType, argsPtr, "arguments", block);
        const auto emitCall = [&](BasicBlock*& resultBlock) {
            unsigned i = 0U;
            for (const auto node : ArgNodes_) {
                const auto arg = ExtractValueInst::Create(arguments, {i++}, "arg", resultBlock);
                const auto codegenArgNode = dynamic_cast<ICodegeneratorExternalNode*>(node);
                MKQL_ENSURE(codegenArgNode, "Argument must be codegenerator node.");
                codegenArgNode->CreateSetValue(ctx, resultBlock, arg);
            }
            return GetNodeValue(ResultNode_, ctx, resultBlock);
        };

        const auto hasUpvalues = EmitFunctionCall < &TComputationUpvalues::operator bool>(Type::getInt1Ty(context), {upvalues}, ctx, block);
        const auto withoutUpvalues = BasicBlock::Create(context, "without_upvalues", ctx.Func);
        const auto withUpvalues = BasicBlock::Create(context, "with_upvalues", ctx.Func);
        BranchInst::Create(withUpvalues, withoutUpvalues, hasUpvalues, block);

        block = withoutUpvalues;
        ReturnInst::Create(context, emitCall(block), block);

        block = withUpvalues;
        EmitFunctionCall<&TComputationUpvalues::SetUpvalues>(Type::getVoidTy(context), {upvalues, ctx.Ctx}, ctx, block);

        // XXX: Preserve the calculated result in the local storage, so further
        // ResultNode_ invalidation via RestoreUpvalues call doesn't spoil the
        // target (e.g. particular slot in Mutables), where the result is stored.
        const auto resultStorage = new AllocaInst(valueType, 0U, "result", block);
        new StoreInst(emitCall(block), resultStorage, block);
        ValueAddRef(ResultNode_->GetRepresentation(), resultStorage, ctx, block);

        EmitFunctionCall<&TComputationUpvalues::RestoreUpvalues>(Type::getVoidTy(context), {upvalues, ctx.Ctx}, ctx, block);

        const auto result = new LoadInst(valueType, resultStorage, "result", block);
        ValueRelease(ResultNode_->GetRepresentation(), resultStorage, ctx, block);
        ReturnInst::Create(context, result, block);

        return ctx.Func;
    }

    using TRunPtr = TCodegenValue::TRunPtr;

    Function* RunFunc_ = nullptr;

    TRunPtr Run_ = nullptr;
#endif

    IComputationNode* const ResultNode_;
    const TComputationExternalNodePtrVector ArgNodes_;
};

} // namespace

IComputationNode* WrapCallable(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() > 0U, "Expected at least one argument");

    const auto argsCount = callable.GetInputsCount() - 1U;
    const auto resultNode = LocateNode(ctx.NodeLocator, callable, argsCount);

    TComputationExternalNodePtrVector argNodes(argsCount);
    for (ui32 i = 0U; i < argsCount; ++i) {
        const auto listItem = AS_CALLABLE("Arg", callable.GetInput(i));
        MKQL_ENSURE(listItem->GetType()->GetName() == "Arg", "Wrong Callable arguments");
        MKQL_ENSURE(listItem->GetInputsCount() == 0, "Wrong Callable arguments");
        MKQL_ENSURE(listItem->GetType()->IsMergeDisabled(), "Merge mode is not disabled");

        argNodes[i] = LocateExternalNode(ctx.NodeLocator, callable, i);
    }
    return new TCallableWrapper(ctx.Mutables, resultNode, std::move(argNodes));
}

} // namespace NKikimr::NMiniKQL
