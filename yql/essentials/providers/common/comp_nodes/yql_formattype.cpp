#include "yql_formattype.h"
#include "yql_type_resource.h"
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL {

class TFormatTypeWrapper: public TMutableComputationNode<TFormatTypeWrapper> {
    typedef TMutableComputationNode<TFormatTypeWrapper> TBaseComputation;

public:
    TFormatTypeWrapper(TComputationMutables& mutables, IComputationNode* handle)
        : TBaseComputation(mutables)
        , Handle_(handle)
    {
    }

    NUdf::TUnboxedValue DoCalculate(TComputationContext& ctx) const {
        auto handle = Handle_->GetValue(ctx);
        auto type = GetYqlType(handle);
        return MakeString(NYql::FormatType(type));
    }

    void RegisterDependencies() const override {
        DependsOn(Handle_);
    }

private:
    IComputationNode* Handle_;
};

IComputationNode* WrapFormatType(TCallable& callable, const TComputationNodeFactoryContext& ctx, ui32 exprCtxMutableIndex) {
    Y_UNUSED(exprCtxMutableIndex);
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    auto handle = LocateNode(ctx.NodeLocator, callable, 0);
    return new TFormatTypeWrapper(ctx.Mutables, handle);
}

} // namespace NKikimr::NMiniKQL
