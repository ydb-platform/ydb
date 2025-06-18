#include "dq_factories.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/dq/comp_nodes/dq_block_hash_join.h>

namespace NKikimr::NMiniKQL {

TComputationNodeFactory GetDqNodeFactory(TComputationNodeFactory customFactory) {
    return [customFactory](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "DqBlockHashJoin"sv) {
            return WrapDqBlockHashJoin(callable, ctx);
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}

} // namespace NKikimr::NMiniKQL
