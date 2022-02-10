#include "mkql_now.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TNowWrapper : public TMutableComputationNode<TNowWrapper> {
    typedef TMutableComputationNode<TNowWrapper> TBaseComputation;
public:
    TNowWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& dependentNodes)
        : TBaseComputation(mutables)
        , DependentNodes(dependentNodes)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return NUdf::TUnboxedValuePod(ctx.TimeProvider.Now().MicroSeconds());
    }

private:
    void RegisterDependencies() const final {
        std::for_each(DependentNodes.cbegin(), DependentNodes.cend(), std::bind(&TNowWrapper::DependsOn, this, std::placeholders::_1));
    }

    const TComputationNodePtrVector DependentNodes;
};

}

IComputationNode* WrapNow(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    TComputationNodePtrVector dependentNodes(callable.GetInputsCount());
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        dependentNodes[i] = LocateNode(ctx.NodeLocator, callable, i);
    }

    return new TNowWrapper(ctx.Mutables, std::move(dependentNodes));
}

}
}
