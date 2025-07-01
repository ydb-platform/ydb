#include "dq_factories.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/dq/comp_nodes/dq_block_hash_join.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine.h>

namespace NKikimr::NMiniKQL {

TComputationNodeFactory GetDqNodeFactory(TComputationNodeFactory customFactory) {
    return [customFactory](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "ExternalNode"sv) {
            return new TExternalComputationNode(ctx.Mutables);
        }
        else if (callable.GetType()->GetName() == "DqBlockHashJoin"sv) {
            return WrapDqBlockHashJoin(callable, ctx);
        }
        else if (callable.GetType()->GetName() == "DqHashCombine"sv) {
            return WrapDqHashCombine(callable, ctx);
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}

} // namespace NKikimr::NMiniKQL
