#include "mkql_varitem.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsOptional>
class TVariantItemWrapper: public TMutableCodegeneratorPtrNode<TVariantItemWrapper<IsOptional>> {
    typedef TMutableCodegeneratorPtrNode<TVariantItemWrapper<IsOptional>> TBaseComputation;
public:
    TVariantItemWrapper(TComputationMutables& mutables, EValueRepresentation kind, IComputationNode* varNode)
        : TBaseComputation(mutables, kind)
        , VarNode(varNode)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        auto var = VarNode->GetValue(compCtx);

        if (IsOptional && !var) {
            return NUdf::TUnboxedValuePod();
        }

        return var.Release().GetVariantItem().Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    void DoGenerateGetValue(const TCodegenContext& ctx, Value* pointer, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);

        const auto var = GetNodeValue(VarNode, ctx, block);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        if (IsOptional) {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto none = BasicBlock::Create(context, "none", ctx.Func);

            BranchInst::Create(none, good, IsEmpty(var, block), block);

            block = none;
            new StoreInst(var, pointer, block);
            BranchInst::Create(done, block);

            block = good;
        }

        const auto lshr = BinaryOperator::CreateLShr(var, ConstantInt::get(valueType, 122), "lshr",  block);
        const auto trunc = CastInst::Create(Instruction::Trunc, lshr, indexType, "trunc", block);
        const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_NE, trunc, ConstantInt::get(indexType , 0), "check", block);

        const auto box = BasicBlock::Create(context, "box", ctx.Func);
        const auto emb = BasicBlock::Create(context, "emb", ctx.Func);
        BranchInst::Create(emb, box, check, block);

        block = emb;

        const uint64_t init[] = {0xFFFFFFFFFFFFFFFFULL, 0x3FFFFFFFFFFFFFFULL};
        const auto mask = ConstantInt::get(valueType, APInt(128, 2, init));
        const auto clean = BinaryOperator::CreateAnd(var, mask, "clean",  block);
        new StoreInst(clean, pointer, block);
        ValueAddRef(this->RepresentationKind, pointer, ctx, block);
        BranchInst::Create(done, block);

        block = box;
        CallBoxedValueVirtualMethod<NUdf::TBoxedValueAccessor::EMethod::GetVariantItem>(pointer, var, ctx.Codegen, block);
        BranchInst::Create(done, block);

        block = done;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(VarNode);
    }

    IComputationNode *const VarNode;
};

}

IComputationNode* WrapVariantItem(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 argument");
    bool isOptional;
    const auto unpacked = UnpackOptional(callable.GetInput(0), isOptional);
    const auto varType = AS_TYPE(TVariantType, unpacked);

    const auto variant = LocateNode(ctx.NodeLocator, callable, 0);
    if (isOptional) {
        return new TVariantItemWrapper<true>(ctx.Mutables, GetValueRepresentation(varType->GetAlternativeType(0)), variant);
    } else {
        return new TVariantItemWrapper<false>(ctx.Mutables, GetValueRepresentation(varType->GetAlternativeType(0)), variant);
    }
}

}
}
