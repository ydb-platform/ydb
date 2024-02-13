#include "yql_cost_function.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

using namespace NYql;

namespace {

bool IsPKJoin(const TOptimizerStatistics& stats, const TVector<TString>& joinKeys) {
    if (stats.KeyColumns.size()==0) {
        return false;
    }

    for(size_t i=0; i<stats.KeyColumns.size(); i++){
        if (std::find(joinKeys.begin(), joinKeys.end(), stats.KeyColumns[i]) == joinKeys.end()) {
            return false;
        }
    }
    return true;
}

}

bool NDq::operator < (const NDq::TJoinColumn& c1, const NDq::TJoinColumn& c2) {
    if (c1.RelName < c2.RelName){
        return true;
    } else if (c1.RelName == c2.RelName) {
        return c1.AttributeName < c2.AttributeName;
    }
    return false;
}

/**
 * Compute the cost and output cardinality of a join
 * 
 * Currently a very basic computation targeted at GraceJoin
 * 
 * The build is on the right side, so we make the build side a bit more expensive than the probe
*/

TOptimizerStatistics NYql::ComputeJoinStats(const TOptimizerStatistics& leftStats, const TOptimizerStatistics& rightStats, 
    const TVector<TString>& leftJoinKeys, const TVector<TString>& rightJoinKeys, EJoinAlgoType joinAlgo, const IProviderContext& ctx) {

    double newCard;
    EStatisticsType outputType;
    bool leftKeyColumns = false;
    bool rightKeyColumns = false;


    if (IsPKJoin(rightStats,rightJoinKeys)) {
        newCard = leftStats.Nrows;
        leftKeyColumns = true;
        if (leftStats.Type == EStatisticsType::BaseTable){
            outputType = EStatisticsType::FilteredFactTable;
        } else {
            outputType = leftStats.Type;
        }
    }
    else if (IsPKJoin(leftStats,leftJoinKeys)) {
        newCard = rightStats.Nrows;
        rightKeyColumns = true;
        if (rightStats.Type == EStatisticsType::BaseTable){
            outputType = EStatisticsType::FilteredFactTable;
        } else {
            outputType = rightStats.Type;
        }
    }
    else {
        newCard = 0.2 * leftStats.Nrows * rightStats.Nrows;
        outputType = EStatisticsType::ManyManyJoin;
    }

    int newNCols = leftStats.Ncols + rightStats.Ncols;

    double cost = ctx.ComputeJoinCost(leftStats, rightStats, joinAlgo)
        + newCard 
        + leftStats.Cost + rightStats.Cost;

    return TOptimizerStatistics(outputType, newCard, newNCols, cost, 
        leftKeyColumns ? leftStats.KeyColumns : ( rightKeyColumns ? rightStats.KeyColumns : TOptimizerStatistics::EmptyColumns));
}


TOptimizerStatistics NYql::ComputeJoinStats(const TOptimizerStatistics& leftStats, const TOptimizerStatistics& rightStats, 
    const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& joinConditions, EJoinAlgoType joinAlgo, const IProviderContext& ctx) {

    TVector<TString> leftJoinKeys;
    TVector<TString> rightJoinKeys;

    for (auto c : joinConditions) {
        leftJoinKeys.emplace_back(c.first.AttributeName);
        rightJoinKeys.emplace_back(c.second.AttributeName);
    }

    return ComputeJoinStats(leftStats, rightStats, leftJoinKeys, rightJoinKeys, joinAlgo, ctx);
}

