#include "mkql_discard.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TDiscardFlowWrapper : public TStatelessFlowCodegeneratorRootNode<TDiscardFlowWrapper> {
    typedef TStatelessFlowCodegeneratorRootNode<TDiscardFlowWrapper> TBaseComputation;
public:
    TDiscardFlowWrapper(IComputationNode* flow)
        : TBaseComputation(flow, EValueRepresentation::Embedded), Flow(flow)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        while (true) {
            if (auto item = Flow->GetValue(ctx); item.IsSpecial())
                return item.Release();
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto skip = BasicBlock::Create(context, "skip", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        BranchInst::Create(loop, block);

        block = loop;
        const auto item = GetNodeValue(Flow, ctx, block);
        BranchInst::Create(exit, skip, IsSpecial(item, block), block);

        block = skip;
        ValueCleanup(Flow->GetRepresentation(), item, ctx, block);
        BranchInst::Create(loop, block);

        block = exit;
        return item;
    }
#endif
private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow);
    }

    IComputationNode* const Flow;
};

class TDiscardWideFlowWrapper : public TStatelessFlowCodegeneratorRootNode<TDiscardWideFlowWrapper> {
using TBaseComputation = TStatelessFlowCodegeneratorRootNode<TDiscardWideFlowWrapper>;
public:
    TDiscardWideFlowWrapper(IComputationWideFlowNode* flow, ui32 size)
        : TBaseComputation(flow, EValueRepresentation::Embedded), Flow(flow), Stub(size, nullptr)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        while (true) {
            switch (Flow->FetchValues(ctx, Stub.data())) {
                case EFetchResult::Finish:
                    return NUdf::TUnboxedValuePod::MakeFinish();
                case EFetchResult::Yield:
                    return NUdf::TUnboxedValuePod::MakeYield();
                default:
                    continue;
            }
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);
        const auto exit = BasicBlock::Create(context, "exit", ctx.Func);

        BranchInst::Create(loop, block);

        block = loop;

        const auto result = GetNodeValues(Flow, ctx, block).first;
        const auto good = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_SGT, result, ConstantInt::get(result->getType(), 0), "good", block);
        BranchInst::Create(loop, exit, good, block);

        block = exit;

        const auto yield = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, result, ConstantInt::get(result->getType(), 0), "yield", block);
        const auto outres = SelectInst::Create(yield, GetYield(context), GetFinish(context), "outres", block);
        return outres;
    }
#endif
private:
    void RegisterDependencies() const final {
        FlowDependsOn(Flow);
    }

    IComputationWideFlowNode* const Flow;
    mutable std::vector<NUdf::TUnboxedValue*> Stub;
};

class TDiscardWrapper : public TCustomValueCodegeneratorNode<TDiscardWrapper> {
    typedef TCustomValueCodegeneratorNode<TDiscardWrapper> TBaseComputation;
public:
    class TValue : public TComputationValue<TValue> {
    public:
        TValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& stream)
            : TComputationValue(memInfo)
            , Stream(std::move(stream))
        {
        }

        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue&) override {
            for (NUdf::TUnboxedValue item;;) {
                const auto status = Stream.Fetch(item);
                if (status != NUdf::EFetchStatus::Ok) {
                    return status;
                }
            }
        }

    private:
        const NUdf::TUnboxedValue Stream;
    };

    TDiscardWrapper(TComputationMutables& mutables, IComputationNode* stream)
        : TBaseComputation(mutables)
        , Stream(stream)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
#ifndef MKQL_DISABLE_CODEGEN
        if (ctx.ExecuteLLVM && Fetch)
            return ctx.HolderFactory.Create<TStreamCodegenValueStateless>(Fetch, &ctx, Stream->GetValue(ctx));
#endif
        return ctx.HolderFactory.Create<TValue>(Stream->GetValue(ctx));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Stream);
    }

#ifndef MKQL_DISABLE_CODEGEN
    void GenerateFunctions(NYql::NCodegen::ICodegen& codegen) final {
        FetchFunc = GenerateFetch(codegen);
        codegen.ExportSymbol(FetchFunc);
    }

    void FinalizeFunctions(NYql::NCodegen::ICodegen& codegen) final {
        if (FetchFunc)
            Fetch = reinterpret_cast<TFetchPtr>(codegen.GetPointerToFunction(FetchFunc));
    }

    Function* GenerateFetch(NYql::NCodegen::ICodegen& codegen) const {
        auto& module = codegen.GetModule();
        auto& context = codegen.GetContext();

        const auto& name = TBaseComputation::MakeName("Fetch");
        if (const auto f = module.getFunction(name.c_str()))
            return f;

        const auto valueType = Type::getInt128Ty(context);
        const auto containerType = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ? static_cast<Type*>(PointerType::getUnqual(valueType)) : static_cast<Type*>(valueType);
        const auto contextType = GetCompContextType(context);
        const auto statusType = Type::getInt32Ty(context);
        const auto funcType = FunctionType::get(statusType, {PointerType::getUnqual(contextType), containerType, PointerType::getUnqual(valueType)}, false);

        TCodegenContext ctx(codegen);
        ctx.Func = cast<Function>(module.getOrInsertFunction(name.c_str(), funcType).getCallee());

        DISubprogramAnnotator annotator(ctx, ctx.Func);
        

        auto args = ctx.Func->arg_begin();

        ctx.Ctx = &*args;
        const auto containerArg = &*++args;

        const auto main = BasicBlock::Create(context, "main", ctx.Func);
        auto block = main;

        const auto container = codegen.GetEffectiveTarget() == NYql::NCodegen::ETarget::Windows ?
            new LoadInst(valueType, containerArg, "load_container", false, block) : static_cast<Value*>(containerArg);

        const auto loop = BasicBlock::Create(context, "loop", ctx.Func);

        const auto stub = new AllocaInst(valueType, 0U, "stub", block);
        new StoreInst(ConstantInt::get(valueType, 0), stub, block);

        BranchInst::Create(loop, block);
        block = loop;

        const auto status = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::Fetch>(statusType, container, codegen, block, stub);

        const auto icmp = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, status, ConstantInt::get(statusType, static_cast<ui32>(NUdf::EFetchStatus::Ok)), "cond", block);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        BranchInst::Create(done, loop, icmp, block);

        block = done;
        ReturnInst::Create(context, status, block);
        return ctx.Func;
    }

    using TFetchPtr = TStreamCodegenValueStateless::TFetchPtr;

    Function* FetchFunc = nullptr;

    TFetchPtr Fetch = nullptr;
#endif

    IComputationNode* const Stream;
};

}

IComputationNode* WrapDiscard(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    const auto type = callable.GetType()->GetReturnType();
    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    if (type->IsFlow()) {
        if (const auto wide = dynamic_cast<IComputationWideFlowNode*>(flow)) {
            auto flowType = AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType());
            if (RuntimeVersion > 35 && flowType->GetItemType()->IsMulti() || flowType->GetItemType()->IsTuple()) {
                return new TDiscardWideFlowWrapper(wide, GetWideComponentsCount(flowType));
            }
            return new TDiscardWideFlowWrapper(wide, 0U);
        } else {
            return new TDiscardFlowWrapper(flow);
        }
    } else if (type->IsStream()) {
        return new TDiscardWrapper(ctx.Mutables, flow);
    }

    THROW yexception() << "Expected flow or stream.";
}

}
}
