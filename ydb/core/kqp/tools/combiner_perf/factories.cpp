#include "factories.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetPerfTestFactory(TComputationNodeFactory customFactory) {
    return [customFactory](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestList") {
            return new TExternalComputationNode(ctx.Mutables);
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}

}
}
