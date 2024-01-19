#include "mkql_lazy_list.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsOptional>
class TLazyListWrapper : public TMutableCodegeneratorNode<TLazyListWrapper<IsOptional>> {
    typedef TMutableCodegeneratorNode<TLazyListWrapper<IsOptional>> TBaseComputation;
public:

    TLazyListWrapper(TComputationMutables& mutables, IComputationNode* list)
        : TBaseComputation(mutables, EValueRepresentation::Boxed), List(list)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto list = List->GetValue(ctx);

        if (IsOptional && !list) {
            return NUdf::TUnboxedValuePod();
        }

        if (list.GetElements()) {
            return ctx.HolderFactory.LazyList(list.Release());
        }

        return list.Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto factory = ctx.GetFactory();
        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::LazyList));

        const auto list = GetNodeValue(List, ctx, block);

        const auto wrap = BasicBlock::Create(context, "wrap", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto lazy = PHINode::Create(list->getType(), IsOptional ? 3U : 2U, "lazy", done);
        lazy->addIncoming(list, block);

        if constexpr (IsOptional) {
            const auto test = BasicBlock::Create(context, "test", ctx.Func);
            BranchInst::Create(done, test, IsEmpty(list, block), block);

            block = test;
            lazy->addIncoming(list, block);
        }

        const auto ptrType = PointerType::getUnqual(list->getType());
        const auto elements = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetElements>(ptrType, list, ctx.Codegen, block);
        const auto null = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, elements, ConstantPointerNull::get(ptrType), "null", block);

        BranchInst::Create(done, wrap, null, block);

        block = wrap;

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(list->getType(), {factory->getType(), list->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            const auto res = CallInst::Create(funType, funcPtr, {factory, list}, "res", block);
            lazy->addIncoming(res, block);
        } else {
            const auto retPtr = new AllocaInst(list->getType(), 0U, "ret_ptr", block);
            new StoreInst(list, retPtr, block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType(), retPtr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, funcPtr, {factory, retPtr, retPtr}, "", block);
            const auto res = new LoadInst(list->getType(), retPtr, "res", block);
            lazy->addIncoming(res, block);
        }

        BranchInst::Create(done, block);

        block = done;
        return lazy;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(List);
    }

    IComputationNode* const List;
};

}

IComputationNode* WrapLazyList(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1U, "Expected single arg, got " << callable.GetInputsCount());
    const auto list = LocateNode(ctx.NodeLocator, callable, 0);

    if (callable.GetInput(0).GetStaticType()->IsOptional()) {
        return new TLazyListWrapper<true>(ctx.Mutables, list);
    } else {
        return new TLazyListWrapper<false>(ctx.Mutables, list);
    }
}

}
}
