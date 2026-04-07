#include "mkql_collect.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TCollectFlowWrapper: public TMutableCodegeneratorRootNode<TCollectFlowWrapper> {
    using TBaseComputation = TMutableCodegeneratorRootNode<TCollectFlowWrapper>;

public:
    TCollectFlowWrapper(TComputationMutables& mutables, IComputationNode* flow)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Flow(flow)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        for (NUdf::TUnboxedValue list = ctx.HolderFactory.GetEmptyContainerLazy();;) {
            auto item = Flow->GetValue(ctx);
            if (item.IsFinish()) {
                return list.Release();
            }
            MKQL_ENSURE(!item.IsYield(), "Unexpected flow status!");
            list = ctx.HolderFactory.Append(list.Release(), item.Release());
        }
    }
#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto factory = ctx.GetFactory();

        const auto valueType = Type::getInt128Ty(context);

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto burn = BasicBlock::Create(context, "burn", ctx.Func);

        const auto list = PHINode::Create(valueType, 2U, "list", work);

        const auto first = EmitFunctionCall<&THolderFactory::GetEmptyContainerLazy>(valueType, {factory}, ctx, block);
        list->addIncoming(first, block);

        BranchInst::Create(work, block);

        block = work;

        const auto item = GetNodeValue(Flow, ctx, block);

        const auto select = SwitchInst::Create(item, good, 2U, block);
        select->addCase(GetFinish(context), done);
        select->addCase(GetYield(context), burn);

        {
            block = good;

            const auto next = EmitFunctionCall<&THolderFactory::Append>(valueType, {factory, list, item}, ctx, block);
            list->addIncoming(next, block);
            BranchInst::Create(work, block);
        }

        {
            block = burn;
            EmitFunctionCall<&TCollectFlowWrapper::Throw>(Type::getVoidTy(context), {}, ctx, block);
            new UnreachableInst(context, block);
        }

        block = done;
        return list;
    }
#endif
private:
    [[noreturn]] static void Throw() {
        UdfTerminate("Unexpected flow status!");
    }

    void RegisterDependencies() const final {
        this->DependsOn(Flow);
    }

    IComputationNode* const Flow;
};

template <bool IsList>
class TCollectWrapper: public TMutableCodegeneratorNode<TCollectWrapper<IsList>> {
    typedef TMutableCodegeneratorNode<TCollectWrapper<IsList>> TBaseComputation;

public:
    TCollectWrapper(TComputationMutables& mutables, IComputationNode* seq)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , Seq(seq)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto seq = Seq->GetValue(ctx);
        if (IsList && seq.GetElements()) {
            return seq.Release();
        }

        return ctx.HolderFactory.Collect<!IsList>(seq.Release());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto factory = ctx.GetFactory();

        const auto seq = GetNodeValue(Seq, ctx, block);

        if constexpr (IsList) {
            const auto work = BasicBlock::Create(context, "work", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);

            const auto valueType = Type::getInt128Ty(context);
            const auto ptrType = PointerType::getUnqual(valueType);

            const auto result = PHINode::Create(valueType, 2U, "result", done);
            const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(ptrType, seq, ctx.Codegen, block);
            const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, elements, ConstantPointerNull::get(ptrType), "null", block);
            result->addIncoming(seq, block);
            BranchInst::Create(work, done, null, block);

            block = work;
            const auto res = EmitFunctionCall<&THolderFactory::Collect<!IsList>>(seq->getType(), {factory, seq}, ctx, block);
            result->addIncoming(res, block);
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            return EmitFunctionCall<&THolderFactory::Collect<!IsList>>(seq->getType(), {factory, seq}, ctx, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Seq);
    }

    IComputationNode* const Seq;
};

} // namespace

IComputationNode* WrapCollect(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    const auto type = callable.GetInput(0).GetStaticType();
    const auto list = LocateNode(ctx.NodeLocator, callable, 0);

    if (type->IsFlow()) {
        return new TCollectFlowWrapper(ctx.Mutables, list);
    } else if (type->IsList()) {
        return new TCollectWrapper<true>(ctx.Mutables, list);
    } else if (type->IsStream()) {
        return new TCollectWrapper<false>(ctx.Mutables, list);
    }

    THROW yexception() << "Expected flow, list or stream.";
}

} // namespace NMiniKQL
} // namespace NKikimr
