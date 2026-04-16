#include "mkql_toindexdict.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TToIndexDictWrapper: public TMutableCodegeneratorNode<TToIndexDictWrapper> {
    typedef TMutableCodegeneratorNode<TToIndexDictWrapper> TBaseComputation;

public:
    TToIndexDictWrapper(TComputationMutables& mutables, IComputationNode* list)
        : TBaseComputation(mutables, list->GetRepresentation())
        , List(list)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.ToIndexDict(ctx.Builder, List->GetValue(ctx).Release());
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();

        const auto indexType = Type::getInt32Ty(context);

        const auto first = GetElementPtrInst::CreateInBounds(GetCompContextType(context), ctx.Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 0)}, "first", block);
        const auto fourth = GetElementPtrInst::CreateInBounds(GetCompContextType(context), ctx.Ctx, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, 3)}, "fourth", block);

        const auto structPtrType = PointerType::getUnqual(StructType::get(context));
        const auto factory = new LoadInst(structPtrType, first, "factory", block);
        const auto builder = new LoadInst(structPtrType, fourth, "builder", block);

        const auto list = GetNodeValue(List, ctx, block);

        return EmitFunctionCall<&THolderFactory::ToIndexDict>(list->getType(), {factory, builder, list}, ctx, block);
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(List);
    }

    IComputationNode* const List;
};

} // namespace

IComputationNode* WrapToIndexDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args");
    return new TToIndexDictWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
}

} // namespace NMiniKQL
} // namespace NKikimr
