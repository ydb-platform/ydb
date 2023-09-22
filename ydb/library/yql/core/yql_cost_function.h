#pragma once

#include "yql_statistics.h"

/**
 * The cost function for cost based optimizer currently consists of methods for computing
 * both the cost and cardinalities of individual plan operators
*/
namespace NYql {

enum EJoinImplType {
    DictJoin,
    MapJoin,
    GraceJoin
};

TOptimizerStatistics ComputeJoinStats(TOptimizerStatistics leftStats, TOptimizerStatistics rightStats, EJoinImplType joinType);

}