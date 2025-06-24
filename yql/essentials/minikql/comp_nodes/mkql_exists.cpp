#include "mkql_exists.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h>  // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TExistsWrapper : public TDecoratorCodegeneratorNode<TExistsWrapper> {
    typedef TDecoratorCodegeneratorNode<TExistsWrapper> TBaseComputation;
public:
    TExistsWrapper(IComputationNode* optional)
        : TBaseComputation(optional)
    {}

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext&, const NUdf::TUnboxedValuePod& value) const {
        return NUdf::TUnboxedValuePod(bool(value));
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, Value* value, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        const auto check = IsExists(value, block, context);
        if (Node_->IsTemporaryValue())
            ValueCleanup(Node_->GetRepresentation(), value, ctx, block);
        return MakeBoolean(check, context, block);
    }
#endif
};

}

IComputationNode* WrapExists(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    return new TExistsWrapper(LocateNode(ctx.NodeLocator, callable, 0));
}

}
}
