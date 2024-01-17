#include "mkql_append.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template<bool IsVoid>
class TAppendWrapper : public TMutableCodegeneratorNode<TAppendWrapper<IsVoid>> {
    typedef TMutableCodegeneratorNode<TAppendWrapper<IsVoid>> TBaseComputation;
public:
    TAppendWrapper(TComputationMutables& mutables, IComputationNode* left, IComputationNode* right)
        : TBaseComputation(mutables, left->GetRepresentation())
        , Left(left)
        , Right(right)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto left = Left->GetValue(ctx);
        auto right = Right->GetValue(ctx);

        if (IsVoid && !right.IsBoxed())
            return left.Release();

        return ctx.HolderFactory.Append(left.Release(), right.Release());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto factory = ctx.GetFactory();

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::Append));

        const auto left = GetNodeValue(Left, ctx, block);
        const auto right = GetNodeValue(Right, ctx, block);

        if constexpr (IsVoid) {
            const auto work = BasicBlock::Create(context, "work", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto result = PHINode::Create(left->getType(), 2, "result", done);
            result->addIncoming(left, block);

            const uint64_t init[] = {0x0ULL, 0x300000000000000ULL};
            const auto mask = ConstantInt::get(right->getType(), APInt(128, 2, init));
            const auto boxed = BinaryOperator::CreateAnd(right, mask, "boxed",  block);
            const auto check = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, boxed, mask, "check", block);
            BranchInst::Create(work, done, check, block);
            block = work;

            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(left->getType(), {factory->getType(), left->getType(), right->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                const auto res = CallInst::Create(funType, funcPtr, {factory, left, right}, "res", block);
                result->addIncoming(res, block);
            } else {
                const auto retPtr = new AllocaInst(left->getType(), 0U, "ret_ptr", block);
                const auto itemPtr = new AllocaInst(right->getType(), 0U, "item_ptr", block);
                new StoreInst(left, retPtr, block);
                new StoreInst(right, itemPtr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType(), retPtr->getType(), itemPtr->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                CallInst::Create(funType, funcPtr, {factory, retPtr, retPtr, itemPtr}, "", block);
                const auto res = new LoadInst(left->getType(), retPtr, "res", block);
                result->addIncoming(res, block);
            }

            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
                const auto funType = FunctionType::get(left->getType(), {factory->getType(), left->getType(), right->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                const auto res = CallInst::Create(funType, funcPtr, {factory, left, right}, "res", block);
                return res;
            } else {
                const auto retPtr = new AllocaInst(left->getType(), 0U, "ret_ptr", block);
                const auto itemPtr = new AllocaInst(right->getType(), 0U, "item_ptr", block);
                new StoreInst(left, retPtr, block);
                new StoreInst(right, itemPtr, block);
                const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType(), retPtr->getType(), itemPtr->getType()}, false);
                const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
                CallInst::Create(funType, funcPtr, {factory, retPtr, retPtr, itemPtr}, "", block);
                const auto res = new LoadInst(left->getType(), retPtr, "res", block);
                return res;
            }
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Left);
        this->DependsOn(Right);
    }

    IComputationNode* const Left;
    IComputationNode* const Right;
};

}

IComputationNode* WrapAppend(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto leftType = AS_TYPE(TListType, callable.GetInput(0));
    const auto rightType = callable.GetInput(1).GetStaticType();

    MKQL_ENSURE(leftType->GetItemType()->IsSameType(*rightType), "Mismatch item type");

    const auto left = LocateNode(ctx.NodeLocator, callable, 0);
    const auto right = LocateNode(ctx.NodeLocator, callable, 1);
    if (rightType->IsVoid())
        return new TAppendWrapper<true>(ctx.Mutables, left, right);
    else
        return new TAppendWrapper<false>(ctx.Mutables, left, right);
}

}
}
