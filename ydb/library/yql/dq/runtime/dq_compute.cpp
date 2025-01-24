#include "dq_compute.h"

#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/parser/pg_wrapper/interface/pack.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>
#include <yql/essentials/providers/common/comp_nodes/yql_factory.h>
#include "yql/essentials/utils/yql_panic.h"

namespace NYql::NDq {

using namespace NKikimr;
using namespace NMiniKQL;

TComputationNodeFactory GetDqBaseComputeFactory(const TDqComputeContextBase* computeCtx) {
    YQL_ENSURE(computeCtx);
    auto builtinFactory = GetCompositeWithBuiltinFactory({
        NYql::GetPgFactory(),
        NKikimr::NMiniKQL::GetYqlFactory()
    });

    return [builtinFactory]
        (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            if (auto builtin = builtinFactory(callable, ctx)) {
                return builtin;
            }

            return nullptr;
        };
}

}
