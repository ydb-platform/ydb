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
    double selectivity = 1.0;


    if (IsPKJoin(rightStats,rightJoinKeys)) {
        newCard = leftStats.Nrows * rightStats.Selectivity;
        selectivity = leftStats.Selectivity * rightStats.Selectivity;
        leftKeyColumns = true;
        if (leftStats.Type == EStatisticsType::BaseTable){
            outputType = EStatisticsType::FilteredFactTable;
        } else {
            outputType = leftStats.Type;
        }
    }
    else if (IsPKJoin(leftStats,leftJoinKeys)) {
        newCard = rightStats.Nrows;
        newCard = rightStats.Nrows * leftStats.Selectivity;
        selectivity = leftStats.Selectivity * rightStats.Selectivity;

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
    double newByteSize = leftStats.Nrows ? (leftStats.ByteSize / leftStats.Nrows) * newCard : 0 +
            rightStats.Nrows ? (rightStats.ByteSize / rightStats.Nrows) * newCard : 0;

    double cost = ctx.ComputeJoinCost(leftStats, rightStats, newCard, newByteSize, joinAlgo)
        + leftStats.Cost + rightStats.Cost;

    auto result = TOptimizerStatistics(outputType, newCard, newNCols, newByteSize, cost, 
        leftKeyColumns ? leftStats.KeyColumns : ( rightKeyColumns ? rightStats.KeyColumns : TOptimizerStatistics::EmptyColumns));
    result.Selectivity = selectivity;
    return result;
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

