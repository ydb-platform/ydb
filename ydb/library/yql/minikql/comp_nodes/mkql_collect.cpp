#include "mkql_collect.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TCollectFlowWrapper : public TMutableCodegeneratorRootNode<TCollectFlowWrapper> {
using TBaseComputation = TMutableCodegeneratorRootNode<TCollectFlowWrapper>;
public:
    TCollectFlowWrapper(TComputationMutables& mutables, IComputationNode* flow)
        : TBaseComputation(mutables, EValueRepresentation::Boxed), Flow(flow)
    {}

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

        const auto empty = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::GetEmptyContainerLazy));
        const auto append = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::Append));

        const auto work = BasicBlock::Create(context, "work", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);
        const auto burn = BasicBlock::Create(context, "burn", ctx.Func);

        const auto list = PHINode::Create(valueType, 2U, "list", work);

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(valueType, {factory->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, empty, PointerType::getUnqual(funType), "empty", block);
            const auto first = CallInst::Create(funType, funcPtr, {factory}, "init", block);
            list->addIncoming(first, block);
        } else {
            const auto ptr = new AllocaInst(valueType, 0U, "ptr", block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), ptr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, empty, PointerType::getUnqual(funType), "empty", block);
            CallInst::Create(funType, funcPtr, {factory, ptr}, "", block);
            const auto first = new LoadInst(valueType, ptr, "init", block);
            list->addIncoming(first, block);
        }

        BranchInst::Create(work, block);

        block = work;

        const auto item = GetNodeValue(Flow, ctx, block);

        const auto select = SwitchInst::Create(item, good, 2U, block);
        select->addCase(GetFinish(context), done);
        select->addCase(GetYield(context), burn);

        {
            block = good;

            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(valueType, {factory->getType(), list->getType(), item->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, append, PointerType::getUnqual(funType), "append", block);
                const auto next = CallInst::Create(funType, funcPtr, {factory, list, item}, "next", block);
                list->addIncoming(next, block);
            } else {
                const auto retPtr = new AllocaInst(list->getType(), 0U, "ret_ptr", block);
                const auto itemPtr = new AllocaInst(item->getType(), 0U, "item_ptr", block);
                new StoreInst(list, retPtr, block);
                new StoreInst(item, itemPtr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType(), retPtr->getType(), itemPtr->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, append, PointerType::getUnqual(funType), "append", block);
                CallInst::Create(funType, funcPtr, {factory, retPtr, retPtr, itemPtr}, "", block);
                const auto next = new LoadInst(list->getType(), retPtr, "next", block);
                list->addIncoming(next, block);
            }
            BranchInst::Create(work, block);
        }

        {
            block = burn;
            const auto thrower = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&TCollectFlowWrapper::Throw));
            const auto throwerType = FunctionType::get(Type::getVoidTy(context), {}, false);
            const auto throwerPtr = CastInst::Create(Instruction::IntToPtr, thrower, PointerType::getUnqual(throwerType), "thrower", block);
            CallInst::Create(throwerType, throwerPtr, {}, "", block);
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
class TCollectWrapper : public TMutableCodegeneratorNode<TCollectWrapper<IsList>> {
    typedef TMutableCodegeneratorNode<TCollectWrapper<IsList>> TBaseComputation;
public:
    TCollectWrapper(TComputationMutables& mutables, IComputationNode* seq)
        : TBaseComputation(mutables, EValueRepresentation::Boxed), Seq(seq)
    {}

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

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::Collect<!IsList>));

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
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(seq->getType(), {factory->getType(), seq->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                const auto res = CallInst::Create(funType, funcPtr, {factory, seq}, "res", block);
                result->addIncoming(res, block);
            } else {
                const auto ptr = new AllocaInst(seq->getType(), 0U, "ptr", block);
                new StoreInst(seq, ptr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), ptr->getType(), ptr->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                CallInst::Create(funType, funcPtr, {factory, ptr, ptr}, "", block);
                const auto res = new LoadInst(seq->getType(), ptr, "res", block);
                result->addIncoming(res, block);
            }
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(seq->getType(), {factory->getType(), seq->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                const auto res = CallInst::Create(funType, funcPtr, {factory, seq}, "res", block);
                return res;
            } else {
                const auto ptr = new AllocaInst(seq->getType(), 0U, "ptr", block);
                new StoreInst(seq, ptr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), ptr->getType(), ptr->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                CallInst::Create(funType, funcPtr, {factory, ptr, ptr}, "", block);
                const auto res = new LoadInst(seq->getType(), ptr, "res", block);
                return res;
            }
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Seq);
    }

    IComputationNode* const Seq;
};

}

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

}
}
