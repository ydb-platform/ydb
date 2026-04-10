#include "datashard_kqp_compute.h"

#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/engine/mkql_engine_flat_host.h>
#include <ydb/core/kqp/runtime/kqp_runtime_impl.h>

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_cast.h>

#include <util/generic/cast.h>

namespace NKikimr {
namespace NMiniKQL {

IComputationNode* WrapKqpDeleteRows(TCallable& , const TComputationNodeFactoryContext&,
    TKqpDatashardComputeContext& , const TString& )
{
    MKQL_ENSURE_S(false,"Not implemented");
}

} // namespace NMiniKQL
} // namespace NKikimr
