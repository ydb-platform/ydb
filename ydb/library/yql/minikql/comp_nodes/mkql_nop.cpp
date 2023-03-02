#include "mkql_nop.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapNop(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    return LocateNode(ctx.NodeLocator, callable, 0);
}

}
}
