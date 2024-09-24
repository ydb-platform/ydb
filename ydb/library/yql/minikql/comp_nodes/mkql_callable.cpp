#include "mkql_callable.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TCallableWrapper : public TCustomValueCodegeneratorNode<TCallableWrapper> {
    typedef TCustomValueCodegeneratorNode<TCallableWrapper> TBaseComputation;
private:
    class TValue : public TComputationValue<TValue> {
    public:
        TValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, IComputationNode* resultNode,
            const TComputationExternalNodePtrVector& argNodes)
            : TComputationValue(memInfo)
            , CompCtx(compCtx)
            , ResultNode(resultNode)
            , ArgNodes(argNodes)
        {}

    private:
        NUdf::TUnboxedValue Run(const NUdf::IValueBuilder*, const NUdf::TUnboxedValuePod* args) const override
        {
            for (const auto node : ArgNodes) {
                node->SetValue(CompCtx, NUdf::TUnboxedValuePod(*args++));
            }

            return ResultNode->GetValue(CompCtx);
        }

        TComputationContext& CompCtx;
        IComputationNode *const ResultNode;
        const TComputationExternalNodePtrVector ArgNodes;
    };

    class TCodegenValue : public TComputationValue<TCodegenValue> {
    public:
        using TBase = TComputationValue<TCodegenValue>;

        using TRunPtr = NUdf::TUnboxedValuePod (*)(TComputationContext*, const NUdf::TUnboxedValuePod*);

        TCodegenValue(TMemoryUsageInfo* memInfo, TRunPtr run, TComputationContext* ctx)
            : TBase(memInfo)
            , RunFunc(run)
            , Ctx(ctx)
        {}

    private:
        NUdf::TUnboxedValue Run(const NUdf::IValueBuilder*, const NUdf::TUnboxedValuePod* args) const override {
            return RunFunc(Ctx, args);
        }

        const TRunPtr RunFunc;
        TComputationContext* const Ctx;
    };
public:
    TCallableWrapper(TComputationMutables& mutables, IComputationNode* resultNode, TComputationExternalNodePtrVector&& argNodes)
        : TBaseComputation(mutables)
        , ResultNode(resultNode)
        , ArgNodes(std::move(argNodes))
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Run)
            return ctx.HolderFactory.Create<TCodegenValue>(Run, &ctx);
#endif
        return ctx.HolderFactory.Create<TValue>(ctx, ResultNode, ArgNodes);
    }

private:
    void RegisterDependencies() const final {
        for (const auto& arg : ArgNodes) {
            Own(arg);
        }

        DependsOn(ResultNode);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        RunFunc = GenerateRun(codegen);
        codegen.ExportSymbol(RunFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (RunFunc)
            Run = reinterpret_cast<TRunPtr>(codegen.GetPointerToFunction(RunFunc));
    }

    Function* GenerateRun(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("Run");
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto argsType = ArrayType::get(valueType, ArgNodes.size());
        const auto contextType = GetCompContextType(context);

        const auto funcType = codegen.GetEffectiveTarget() != NYql::NCodegen::ETarget::Windows ?
            FunctionType::get(valueType, {PointerType::getUnqual(contextType), PointerType::getUnqual(argsType)}, false):
            FunctionType::get(Type::getVoidTy(context), {PointerType::getUnqual(valueType), PointerType::getUnqual(contextType), PointerType::getUnqual(argsType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        

        auto args = ctx.Func->arg_begin();

        const auto resultArg = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? &*args++ : nullptr;
        if (resultArg) {
            resultArg->addAttr(Attribute::StructRet);
            resultArg->addAttr(Attribute::NoAlias);
        }

        ctx.Ctx = &*args;
        const auto argsPtr = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto arguments = new LoadInst(argsType, argsPtr, "arguments", block);

        unsigned i = 0U;
        for (const auto node : ArgNodes) {
            const auto arg = ExtractValueInst::Create(arguments, {i++}, "arg", block);
            const auto codegenArgNode = dynamic_cast<ICodegeneratorExternalNode*>(node);
            MKQL_ENSURE(codegenArgNode, "Argument must be codegenerator node.");
            codegenArgNode->CreateSetValue(ctx, block, arg);
        }

        const auto result = GetNodeValue(ResultNode, ctx, block);

        if (resultArg) {
            new StoreInst(result, resultArg, block);
            ReturnInst::Create(context, block);
        } else {
            ReturnInst::Create(context, result, block);
        }
        return ctx.Func;
    }

    using TRunPtr = TCodegenValue::TRunPtr;

    Function* RunFunc = nullptr;

    TRunPtr Run = nullptr;
#endif

    IComputationNode *const ResultNode;
    const TComputationExternalNodePtrVector ArgNodes;
};

}

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

}
}
