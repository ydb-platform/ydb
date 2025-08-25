#include "factories.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_aggregate.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetPerfTestFactory(TComputationNodeFactory customFactory) {
    return [customFactory](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestList"sv) {
            return new TExternalComputationNode(ctx.Mutables);
        }
        else if (callable.GetType()->GetName() == "DqHashCombine"sv) {
            return WrapDqHashCombine(callable, ctx);
        }
        else if (callable.GetType()->GetName() == "DqHashAggregate"sv) {
            return WrapDqHashAggregate(callable, ctx);
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}

}
}
