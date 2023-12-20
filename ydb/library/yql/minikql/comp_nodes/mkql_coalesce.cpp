#include "mkql_coalesce.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool Unpack>
class TCoalesceWrapper : public TBinaryCodegeneratorNode<TCoalesceWrapper<Unpack>> {
    typedef TBinaryCodegeneratorNode<TCoalesceWrapper<Unpack>> TBaseComputation;
public:
    TCoalesceWrapper(IComputationNode* left, IComputationNode* right, EValueRepresentation kind)
        : TBaseComputation(left, right, kind)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        if (auto left = this->Left->GetValue(compCtx)) {
            return left.Release().template GetOptionalValueIf<Unpack>();
        }

        return this->Right->GetValue(compCtx).Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto left = GetNodeValue(this->Left, ctx, block);

        const auto null = BasicBlock::Create(context, "null", ctx.Func);
        const auto good = BasicBlock::Create(context, "good", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto result = PHINode::Create(left->getType(), 2, "result", done);

        BranchInst::Create(good, null, IsExists(left, block), block);

        block = null;
        const auto right = GetNodeValue(this->Right, ctx, block);

        result->addIncoming(right, block);
        BranchInst::Create(done, block);

        block = good;

        const auto unpack = Unpack ? GetOptionalValue(context, left, block) : left;
        result->addIncoming(unpack, block);

        BranchInst::Create(done, block);
        block = done;
        return result;
    }
#endif
};

}

IComputationNode* WrapCoalesce(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    bool isLeftOptional = false;
    const auto& leftType = UnpackOptional(callable.GetInput(0), isLeftOptional);
    MKQL_ENSURE(isLeftOptional || leftType->IsPg(), "Expected optional or pg");

    bool isRightOptional = false;
    if (!leftType->IsSameType(*callable.GetInput(1).GetStaticType())) {
        const auto& rightType = UnpackOptional(callable.GetInput(1), isRightOptional);
        MKQL_ENSURE(leftType->IsSameType(*rightType), "Mismatch types");
    }

    const auto kind = GetValueRepresentation(callable.GetType()->GetReturnType());

    if (isRightOptional)
        return new TCoalesceWrapper<false>(LocateNode(ctx.NodeLocator, callable, 0), LocateNode(ctx.NodeLocator, callable, 1), kind);
    else
        return new TCoalesceWrapper<true>(LocateNode(ctx.NodeLocator, callable, 0), LocateNode(ctx.NodeLocator, callable, 1), kind);
}

}
}
