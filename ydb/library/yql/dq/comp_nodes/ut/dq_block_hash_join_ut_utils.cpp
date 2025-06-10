#include "dq_block_hash_join_ut_utils.h"

#include <ydb/library/yql/dq/comp_nodes/dq_block_hash_join.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetNodeFactory() {
    auto factory = GetBuiltinFactory();
    return [factory](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "DqBlockHashJoin") {
            return WrapBlockHashJoin(callable, ctx);
        }
        return factory(callable, ctx);
    };
}

} // namespace NMiniKQL
} // namespace NKikimr 