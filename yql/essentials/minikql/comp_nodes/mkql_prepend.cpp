#include "mkql_prepend.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <array>

namespace NKikimr::NMiniKQL {

namespace {

template <bool IsVoid>
class TPrependWrapper: public TMutableCodegeneratorNode<TPrependWrapper<IsVoid>> {
    using TBaseComputation = TMutableCodegeneratorNode<TPrependWrapper<IsVoid>>;

public:
    TPrependWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right)
        : TBaseComputation(mutables, right->GetRepresentation())
        , Left_(left)
        , Right_(right)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto left = Left_->GetValue(ctx);
        auto right = Right_->GetValue(ctx);

        if (IsVoid && !left.IsBoxed()) {
            return right.Release();
        }

        return ctx.HolderFactory.Prepend(left.Release(), right.Release());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto factory = ctx.GetFactory();

        const auto left = GetNodeValue(Left_, ctx, block);
        const auto right = GetNodeValue(Right_, ctx, block);

        if constexpr (IsVoid) {
            const auto work = BasicBlock::Create(context, "work", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto result = PHINode::Create(right->getType(), 2, "result", done);
            result->addIncoming(right, block);

            const std::array<uint64_t, 2> init = {0x0ULL, 0x300000000000000ULL};
            const auto mask = ConstantInt::get(left->getType(), APInt(128, init));
            const auto boxed = BinaryOperator::CreateAnd(left, mask, "boxed", block);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, boxed, mask, "check", block);
            BranchInst::Create(work, done, check, block);
            block = work;

            const auto res = EmitFunctionCall<&THolderFactory::Prepend>(right->getType(), {factory, left, right}, ctx, block);
            result->addIncoming(res, block);

            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            return EmitFunctionCall<&THolderFactory::Prepend>(right->getType(), {factory, left, right}, ctx, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Left_);
        this->DependsOn(Right_);
    }

    IComputationNode* const Left_;
    IComputationNode* const Right_;
};

} // namespace

IComputationNode* WrapPrepend(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto leftType = callable.GetInput(0).GetStaticType();
    const auto rightType = AS_TYPE(TListType, callable.GetInput(1));

    MKQL_ENSURE(rightType->GetItemType()->IsSameType(*leftType), "Mismatch item type");

    const auto left = LocateNode(ctx.NodeLocator, callable, 0);
    const auto right = LocateNode(ctx.NodeLocator, callable, 1);
    if (leftType->IsVoid()) {
        return new TPrependWrapper<true>(ctx.Mutables, left, right);
    } else {
        return new TPrependWrapper<false>(ctx.Mutables, left, right);
    }
}

} // namespace NKikimr::NMiniKQL
