#include "mkql_reverse.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr::NMiniKQL {

namespace {

class TReverseWrapper: public TMutableCodegeneratorNode<TReverseWrapper> {
    using TBaseComputation = TMutableCodegeneratorNode<TReverseWrapper>;

public:
    TReverseWrapper(TComputationMutables& mutables, IComputationNode* list)
        : TBaseComputation(mutables, list->GetRepresentation())
        , List_(list)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.ReverseList(ctx.Builder, List_->GetValue(ctx).Release());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto indexType = Type::getInt32Ty(context);

        const auto first = GetElementPtrInst::CreateInBounds(GetCompContextType(context), ctx.Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 0)}, "first", block);
        const auto fourth = GetElementPtrInst::CreateInBounds(GetCompContextType(context), ctx.Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 3)}, "fourth", block);

        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto factory = new LoadInst(structPtrType, first, "factory", block);
        const auto builder = new LoadInst(structPtrType, fourth, "builder", block);

        const auto list = GetNodeValue(List_, ctx, block);

        return EmitFunctionCall<&THolderFactory::ReverseList>(list->getType(), {factory, builder, list}, ctx, block);
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(List_);
    }

    IComputationNode* const List_;
};

} // namespace

IComputationNode* WrapReverse(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

    return new TReverseWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
}

} // namespace NKikimr::NMiniKQL
