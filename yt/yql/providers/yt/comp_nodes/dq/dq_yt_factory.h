#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_stats_registry.h>

namespace NYql {

NKikimr::NMiniKQL::TComputationNodeFactory GetDqYtFactory(NKikimr::NMiniKQL::IStatsRegistry* jobStats = nullptr);

}
