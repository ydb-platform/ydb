#include "mkql_hasitems.h"
#include <yql/essentials/utils/runtime_dispatch.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_node_builder.h>

namespace NKikimr::NMiniKQL {

namespace {

template <bool IsDict, bool IsOptional>
class THasItemsWrapper: public TMutableCodegeneratorNode<THasItemsWrapper<IsDict, IsOptional>> {
    using TBaseComputation = TMutableCodegeneratorNode<THasItemsWrapper<IsDict, IsOptional>>;

public:
    THasItemsWrapper(TComputationMutables& mutables, IComputationNode* collection)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Collection_(collection)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        const auto& collection = Collection_->GetValue(compCtx);
        if (IsOptional && !collection) {
            return NUdf::TUnboxedValuePod();
        }

        const bool hasItems = IsDict ? collection.HasDictItems() : collection.HasListItems();
        return NUdf::TUnboxedValuePod(hasItems);
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();
        const auto collection = GetNodeValue(Collection_, ctx, block);

        if constexpr (IsOptional) {
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            const auto done = BasicBlock::Create(context, "done", ctx.Func);
            const auto result = PHINode::Create(collection->getType(), 2U, "result", done);

            result->addIncoming(collection, block);
            BranchInst::Create(done, good, IsEmpty(collection, block, context), block);

            block = good;

            const auto has = CallBoxedValueVirtualMethod < IsDict ? NUdf::TBoxedValueAccessor::EMethod::HasDictItems : NUdf::TBoxedValueAccessor::EMethod::HasListItems > (Type::getInt1Ty(context), collection, ctx.Codegen, block);
            if (Collection_->IsTemporaryValue()) {
                CleanupBoxed(collection, ctx, block);
            }
            result->addIncoming(MakeBoolean(has, context, block), block);
            BranchInst::Create(done, block);

            block = done;
            return result;
        } else {
            const auto has = CallBoxedValueVirtualMethod < IsDict ? NUdf::TBoxedValueAccessor::EMethod::HasDictItems : NUdf::TBoxedValueAccessor::EMethod::HasListItems > (Type::getInt1Ty(context), collection, ctx.Codegen, block);
            if (Collection_->IsTemporaryValue()) {
                CleanupBoxed(collection, ctx, block);
            }
            return MakeBoolean(has, context, block);
        }
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Collection_);
    }

    IComputationNode* const Collection_;
};

} // namespace

IComputationNode* WrapHasItems(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    bool isOptional;
    const auto type = UnpackOptional(callable.GetInput(0).GetStaticType(), isOptional);
    return YQL_RUNTIME_DISPATCH_NEW(IComputationNode*, THasItemsWrapper, 2, type->IsDict(), isOptional, ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
}

} // namespace NKikimr::NMiniKQL
