#include "mkql_guess.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsOptional>
class TGuessWrapper: public TMutableCodegeneratorPtrNode<TGuessWrapper<IsOptional>> {
    typedef TMutableCodegeneratorPtrNode<TGuessWrapper<IsOptional>> TBaseComputation;
public:
    TGuessWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* varNode, ui32 index)
        : TBaseComputation(mutables, kind)
        , VarNode(varNode)
        , Index(index)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto var = VarNode->GetValue(compCtx);

        if (IsOptional && !var) {
            return NUdf::TUnboxedValuePod();
        }

        const auto currentIndex = var.GetVariantIndex();
        if (Index == currentIndex) {
            return var.Release().GetVariantItem().MakeOptional();
        } else {
            return NUdf::TUnboxedValuePod();
        }
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);

        const auto var = GetNodeValue(VarNode, ctx, block);

        const auto ind = ConstantInt::get(indexType, Index);
        const auto zero = ConstantInt::get(valueType, 0ULL);

        const auto none = BasicBlock::Create(context, "none", ctx.Func);

        if constexpr (IsOptional) {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);

            BranchInst::Create(none, good, IsEmpty(var, block), block);

            block = good;
        }

        const auto lshr = BinaryOperator::CreateLShr(var, ConstantInt::get(valueType, 122), "lshr",  block);
        const auto trunc = CastInst::Create(Instruction::Trunc, lshr, indexType, "trunc", block);

        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, trunc, ConstantInt::get(indexType , 0), "check", block);

        const auto boxed = BasicBlock::Create(context, "boxed", ctx.Func);
        const auto embed = BasicBlock::Create(context, "embed", ctx.Func);
        const auto step = BasicBlock::Create(context, "step", ctx.Func);

        const auto index = PHINode::Create(indexType, 2, "index", step);

        BranchInst::Create(embed, boxed, check, block);

        block = embed;

        const auto dec = BinaryOperator::CreateSub(trunc, ConstantInt::get(indexType, 1), "dec",  block);
        index->addIncoming(dec, block);
        BranchInst::Create(step, block);

        block = boxed;

        const auto idx = CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetVariantIndex>(indexType, var, ctx.Codegen, block);
        index->addIncoming(idx, block);
        BranchInst::Create(step, block);

        block = step;

        const auto equal = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, index, ind, "equal", block);

        const auto same = BasicBlock::Create(context, "same", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        BranchInst::Create(same, none, equal, block);

        block = none;
        new StoreInst(zero, pointer, block);
        BranchInst::Create(done, block);

        block = same;

        const auto box = BasicBlock::Create(context, "box", ctx.Func);
        const auto emb = BasicBlock::Create(context, "emb", ctx.Func);
        BranchInst::Create(emb, box, check, block);

        block = emb;

        const uint64_t init[] = {0xFFFFFFFFFFFFFFFFULL, 0x3FFFFFFFFFFFFFFULL};
        const auto mask = ConstantInt::get(valueType, APInt(128, 2, init));
        const auto clean = BinaryOperator::CreateAnd(var, mask, "clean",  block);
        new StoreInst(MakeOptional(context, clean, block), pointer, block);
        ValueAddRef(this->RepresentationKind, pointer, ctx, block);

        BranchInst::Create(done, block);

        block = box;
        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetVariantItem>(pointer, var, ctx.Codegen, block);

        const auto load = new LoadInst(valueType, pointer, "load", block);
        new StoreInst(MakeOptional(context, load, block), pointer, block);

        BranchInst::Create(done, block);

        block = done;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(VarNode);
    }

    IComputationNode *const VarNode;
    const ui32 Index;
};

}

IComputationNode* WrapGuess(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arguments");
    bool isOptional;
    const auto unpacked = UnpackOptional(callable.GetInput(0), isOptional);
    const auto varType = AS_TYPE(TVariantType, unpacked);

    const auto indexData = AS_VALUE(TDataLiteral, callable.GetInput(1));
    const ui32 index = indexData->AsValue().Get<ui32>();
    MKQL_ENSURE(index < varType->GetAlternativesCount(), "Bad alternative index");

    const auto variant = LocateNode(ctx.NodeLocator, callable, 0);
    if (isOptional) {
        return new TGuessWrapper<true>(ctx.Mutables, GetValueRepresentation(varType->GetAlternativeType(index)), variant, index);
    } else {
        return new TGuessWrapper<false>(ctx.Mutables, GetValueRepresentation(varType->GetAlternativeType(index)), variant, index);
    }
}

}
}
