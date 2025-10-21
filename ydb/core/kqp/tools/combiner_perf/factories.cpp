#include "factories.h"

#include <ydb/library/yql/dq/comp_nodes/dq_block_hash_join.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_aggregate.h>
#include <ydb/library/yql/dq/comp_nodes/dq_hash_combine.h>
#include <ydb/library/yql/dq/comp_nodes/dq_scalar_hash_join.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetPerfTestFactory(TComputationNodeFactory customFactory) {
    return [customFactory](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        TStringBuf callable_name = callable.GetType()->GetName();
        if (callable_name == "TestList"sv) {
            return new TExternalComputationNode(ctx.Mutables);
        }

        if (callable_name == "DqBlockHashJoin"sv) {
            return WrapDqBlockHashJoin(callable, ctx);
        }

        else if (callable_name == "DqHashCombine"sv) {
            return WrapDqHashCombine(callable, ctx);
        }

        else if (callable_name == "DqHashAggregate"sv) {
            return WrapDqHashAggregate(callable, ctx);
        }

        else if (callable_name == "DqBlockHashJoin") {
            return WrapDqBlockHashJoin(callable, ctx);
        }

        else if (callable_name == "DqScalarHashJoin") {
            return WrapDqScalarHashJoin(callable, ctx);
        }

        return GetBuiltinFactory()(callable, ctx);
    };
}

} // namespace NMiniKQL
} // namespace NKikimr
