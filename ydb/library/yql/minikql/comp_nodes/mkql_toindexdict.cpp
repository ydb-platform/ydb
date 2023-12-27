#include "mkql_toindexdict.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TToIndexDictWrapper : public TMutableCodegeneratorNode<TToIndexDictWrapper> {
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

        const auto func = ConstantInt::get(Type::getInt64Ty(context), GetMethodPtr(&THolderFactory::ToIndexDict));

        const auto list = GetNodeValue(List, ctx, block);

        if (NYql::NCodegen::ETarget::Windows != ctx.Codegen.GetEffectiveTarget()) {
            const auto funType = FunctionType::get(list->getType(), {factory->getType(), builder->getType(), list->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            const auto result = CallInst::Create(funType, funcPtr, {factory, builder, list}, "result", block);
            return result;
        } else {
            const auto retPtr = new AllocaInst(list->getType(), 0U, "ret_ptr", block);
            new StoreInst(list, retPtr, block);
            const auto funType = FunctionType::get(Type::getVoidTy(context), {factory->getType(), retPtr->getType(), builder->getType(), retPtr->getType()}, false);
            const auto funcPtr = CastInst::Create(Instruction::IntToPtr, func, PointerType::getUnqual(funType), "function", block);
            CallInst::Create(funType, funcPtr, {factory, retPtr, builder, retPtr}, "", block);
            const auto result = new LoadInst(list->getType(), retPtr, "result", block);
            return result;
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        DependsOn(List);
    }

    IComputationNode* const List;
};

}

IComputationNode* WrapToIndexDict(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args");
    return new TToIndexDictWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
}

}
}
