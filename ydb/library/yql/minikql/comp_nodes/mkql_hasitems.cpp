#include "mkql_hasitems.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

template <bool IsDict, bool IsOptional>
class THasItemsWrapper : public TMutableCodegeneratorNode<THasItemsWrapper<IsDict, IsOptional>> {
    typedef TMutableCodegeneratorNode<THasItemsWrapper<IsDict, IsOptional>> TBaseComputation;
public:
    THasItemsWrapper(TComputationMutables& mutables, IComputationNode* collection)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Collection(collection)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const auto& collection = Collection->GetValue(compCtx);
        if (IsOptional && !collection) {
            return NUdf::TUnboxedValuePod();
        }

        const bool hasItems = IsDict ? collection.HasDictItems() : collection.HasListItems();
        return NUdf::TUnboxedValuePod(hasItems);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto collection = GetNodeValue(Collection, ctx, block);

        if constexpr (IsOptional) {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto result = PHINode::Create(collection->getType(), 2U, "result", done);

            result->addIncoming(collection, block);
            BranchInst::Create(done, good, IsEmpty(collection, block), block);

            block = good;

            const auto has = CallBoxedValueVirtualMethod<IsDict ? NUdf::TBoxedValueAccessor::EMethod::HasDictItems : NUdf::TBoxedValueAccessor::EMethod::HasListItems>(Type::getInt1Ty(context), collection, ctx.Codegen, block);
            if (Collection->IsTemporaryValue())
                CleanupBoxed(collection, ctx, block);
            result->addIncoming(MakeBoolean(has, context, block), block);
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            const auto has = CallBoxedValueVirtualMethod<IsDict ? NUdf::TBoxedValueAccessor::EMethod::HasDictItems : NUdf::TBoxedValueAccessor::EMethod::HasListItems>(Type::getInt1Ty(context), collection, ctx.Codegen, block);
            if (Collection->IsTemporaryValue())
                CleanupBoxed(collection, ctx, block);
            return MakeBoolean(has, context, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Collection);
    }

    IComputationNode* const Collection;
};

}

IComputationNode* WrapHasItems(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    bool isOptional;
    const auto type = UnpackOptional(callable.GetInput(0).GetStaticType(), isOptional);
    if (type->IsDict()) {
        if (isOptional)
            return new THasItemsWrapper<true, true>(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
        else
            return new THasItemsWrapper<true, false>(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
    } else {
        if (isOptional)
            return new THasItemsWrapper<false, true>(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
        else
            return new THasItemsWrapper<false, false>(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
    }

    THROW yexception() << "Expected list or dict.";
}

}
}
