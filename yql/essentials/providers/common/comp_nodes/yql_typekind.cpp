#include "yql_typekind.h"
#include "yql_type_resource.h"
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL {

class TTypeKindWrapper: public TMutableComputationNode<TTypeKindWrapper> {
    typedef TMutableComputationNode<TTypeKindWrapper> TBaseComputation;

public:
    TTypeKindWrapper(TComputationMutables& mutables, IComputationNode* handle)
        : TBaseComputation(mutables)
        , Handle_(handle)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto handle = Handle_->GetValue(ctx);
        auto type = GetYqlType(handle);
        return MakeString(ToString(type->GetKind()));
    }

    void RegisterDependencies() const override {
        DependsOn(Handle_);
    }

private:
    IComputationNode* Handle_;
};

IComputationNode* WrapTypeKind(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    Y_UNUSED(exprCtxMutableIndex);
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    auto handle = LocateNode(ctx.NodeLocator, callable, 0);
    return new TTypeKindWrapper(ctx.Mutables, handle);
}

} // namespace NKikimr::NMiniKQL
