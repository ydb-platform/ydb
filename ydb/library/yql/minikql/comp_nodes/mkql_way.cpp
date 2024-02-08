#include "mkql_way.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <util/string/cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsOptional>
class TWayWrapper: public TMutableCodegeneratorNode<TWayWrapper<IsOptional>> {
    typedef TMutableCodegeneratorNode<TWayWrapper<IsOptional>> TBaseComputation;
public:
    TWayWrapper(TComputationMutables& mutables, IComputationNode* varNode, EValueRepresentation kind, TComputationNodePtrVector&& literals)
        : TBaseComputation(mutables, kind)
        , VarNode(varNode)
        , Literals(std::move(literals))
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        const auto& var = VarNode->GetValue(ctx);
        if (IsOptional && !var) {
            return NUdf::TUnboxedValuePod();
        }

        const ui32 index = var.GetVariantIndex();
        return Literals[index]->GetValue(ctx).Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto valueType = Type::getInt128Ty(context);
        const auto indexType = Type::getInt32Ty(context);


        const auto var = GetNodeValue(VarNode, ctx, block);

        const auto zero = ConstantInt::get(valueType, 0ULL);

        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(valueType, Literals.size() + IsOptional ? 2U : 1U, "result", done);

        if (IsOptional) {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            BranchInst::Create(done, good, IsEmpty(var, block), block);
            result->addIncoming(zero, block);

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

        const auto choise = SwitchInst::Create(index, done, Literals.size(), block);
        result->addIncoming(zero, block);

        for (ui32 i = 0; i < Literals.size(); ++i) {
            const auto var = BasicBlock::Create(context, (TString("case_") += ToString(i)).c_str(), ctx.Func);
            choise->addCase(ConstantInt::get(Type::getInt32Ty(context), i), var);
            block = var;

            const auto way = GetNodeValue(Literals[i], ctx, block);

            result->addIncoming(way, block);
            BranchInst::Create(done, block);
        }

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(VarNode);
        std::for_each(Literals.cbegin(), Literals.cend(),std::bind(&TWayWrapper<IsOptional>::DependsOn, this, std::placeholders::_1));
    }

    IComputationNode *const VarNode;
    const TComputationNodePtrVector Literals;
};

}

IComputationNode* WrapWay(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 argument");
    bool isOptional;
    const auto unpacked = UnpackOptional(callable.GetInput(0), isOptional);
    const auto varType = AS_TYPE(TVariantType, unpacked);
    const auto structType = varType->GetUnderlyingType()->IsTuple() ? nullptr : AS_TYPE(TStructType, varType->GetUnderlyingType());
    const auto size = varType->GetAlternativesCount();

    TComputationNodePtrVector literals(size);
    EValueRepresentation kind = EValueRepresentation::Embedded;

    for (ui32 idx = 0U; idx < size; ++idx) {
        const auto node = literals[idx] = ctx.NodeFactory.CreateImmutableNode(structType ? MakeString(structType->GetMemberName(idx)) : NUdf::TUnboxedValuePod(idx));
        ctx.NodePushBack(node);
        if (node->GetRepresentation() != EValueRepresentation::Embedded) {
            kind = EValueRepresentation::Any;
        }
    }

    const auto variant = LocateNode(ctx.NodeLocator, callable, 0);
    if (isOptional) {
        return new TWayWrapper<true>(ctx.Mutables, variant, kind, std::move(literals));
    } else {
        return new TWayWrapper<false>(ctx.Mutables, variant, kind, std::move(literals));
    }
}

}
}
