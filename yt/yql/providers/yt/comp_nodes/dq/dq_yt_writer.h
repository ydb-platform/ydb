#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>

namespace NYql::NDqs {

NKikimr::NMiniKQL::IComputationNode* WrapYtDqRowsWideWrite(NKikimr::NMiniKQL::TCallable& callable, NKikimr::NMiniKQL::IStatsRegistry* jobStats, const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx);

} // NYql
