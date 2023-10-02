#include "yql_cost_function.h"

using namespace NYql;

/**
 * Compute the cost and output cardinality of a join
 * 
 * Currently a very basic computation targeted at GraceJoin
 * 
 * The build is on the right side, so we make the build side a bit more expensive than the probe
*/
TOptimizerStatistics NYql::ComputeJoinStats(TOptimizerStatistics leftStats, TOptimizerStatistics rightStats, EJoinImplType joinImpl) {
    Y_UNUSED(joinImpl);

    double newCard = 0.2 * leftStats.Nrows * rightStats.Nrows;
    int newNCols = leftStats.Ncols + rightStats.Ncols;
    double cost = leftStats.Nrows + 2.0 * rightStats.Nrows 
            + newCard 
            + leftStats.Cost + rightStats.Cost;

    return TOptimizerStatistics(newCard, newNCols, cost);
}
