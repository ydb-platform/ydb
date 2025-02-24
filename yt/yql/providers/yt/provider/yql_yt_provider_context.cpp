
#include <yt/yql/providers/yt/provider/yql_yt_provider_context.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/generic/bitmap.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>

namespace NYql {

TMaybe<double> TYtProviderContext::ColumnNumUniqueValues(const TDynBitMap& relMap,  const NDq::TJoinColumn& joinColumn) const {
    auto entry = ColumnIndex_.find(joinColumn.AttributeName);
    if (entry == ColumnIndex_.end()) {
        return Nothing();
    }
    for (const auto& relColEntry : entry->second) {
        int relIndex = relColEntry.first;
        int colIndex = relColEntry.second;
        if (relMap[relIndex] && RelInfo_[relIndex].Label == joinColumn.RelName &&
            RelInfo_[relIndex].ColumnInfo[colIndex].EstimatedUniqueCount) {
            return RelInfo_[relIndex].ColumnInfo[colIndex].EstimatedUniqueCount;
        }
    }
    return Nothing();
}

TString TYtProviderContext::DebugStatistic(const TVector<NDq::TJoinColumn>& columns, const TOptimizerStatistics& stat) const {
    std::stringstream ss;
    const auto* specific = static_cast<TYtProviderStatistic*>(stat.Specific.get());
    ss << "Rels: [";
    int i = 0;
    for (size_t pos = specific->RelBitmap.FirstNonZeroBit(); pos != specific->RelBitmap.Size(); pos = specific->RelBitmap.NextNonZeroBit(pos)) {
        if (i > 0) {
            ss << ", ";
        }
        ss << RelInfo_[pos].Label;
        ++i;
    }
    ss << "]\n";
    ss << "Columns = [";
    i = 0;
    for (const auto& joinColumn : columns) {
        if (i > 0) {
            ss << ", ";
        }
        ss << joinColumn.AttributeName;
        auto numUniques = ColumnNumUniqueValues(specific->RelBitmap, joinColumn);
        if (numUniques) {
            ss << " (" << *numUniques << " uniques)";
        } else {
            ss << " (? uniques)";
        }
        ++i;
    }
    ss << "]\n";
    ss << "SortColumns = [";
    for (int i = 0; i < std::ssize(specific->SortColumns); ++i) {
        if (i > 0) {
            ss << ", ";
        }
        ss << specific->SortColumns[i];
    }
    ss << "]\n";
    ss << "Ncols = " << stat.Ncols << ", ByteSize = " << stat.ByteSize << ", Nrows = "
        << stat.Nrows << ", Cost = " << stat.Cost << "\n";
    return ss.str();
}

TMaybe<double> TYtProviderContext::FindMaxUniqueVals(const TYtProviderStatistic& specific, const TVector<NDq::TJoinColumn>& columns) const {
    TMaybe<double> result;
    for (const auto& joinColumn : columns) {
        auto val = ColumnNumUniqueValues(specific.RelBitmap, joinColumn);
        if (val && (!result || *val > *result)) {
            result = val;
        }
    }
    return result;
}

double ComputeCardinality(TMaybe<double> maxUniques, double leftCardinality, double rightCardinality) {
    if (!maxUniques) {
        double result = 0.2 * leftCardinality * rightCardinality;
        return result;
    }
    if (*maxUniques == 0.0) {
        return 0.0;
    }
    double result = leftCardinality / *maxUniques * rightCardinality;
    return result;
}

TYtProviderContext::TYtProviderContext(TJoinAlgoLimits limits, TVector<TYtProviderRelInfo> relInfo)
    : Limits_(limits)
    , RelInfo_(std::move(relInfo)) {
    for (int relIndex = 0; relIndex < std::ssize(RelInfo_); ++relIndex) {
        const auto& rel = RelInfo_[relIndex];
        for (int colIndex = 0; colIndex < std::ssize(rel.ColumnInfo); ++colIndex) {
            const auto& column = rel.ColumnInfo[colIndex].ColumnName;
            ColumnIndex_[column].insert(std::make_pair(relIndex, colIndex));
        }
    }
}

bool TYtProviderContext::IsMapJoinApplicable(const TOptimizerStatistics& stat) const {
    const TYtProviderStatistic* specific = static_cast<const TYtProviderStatistic*>(stat.Specific.get());
    return stat.Type == EStatisticsType::BaseTable && specific->SizeInfo.MapJoinMemSize &&
        *specific->SizeInfo.MapJoinMemSize < Limits_.MapJoinMemLimit;
}

bool TYtProviderContext::IsLookupJoinApplicable(const TOptimizerStatistics& table, const TOptimizerStatistics& lookupTable, const TVector<NDq::TJoinColumn>& tableJoinKeys) const {
    const TYtProviderStatistic* tableSpecific = static_cast<const TYtProviderStatistic*>(table.Specific.get());
    const TYtProviderStatistic* lookupTableSpecific = static_cast<const TYtProviderStatistic*>(lookupTable.Specific.get());
    if (table.Type != EStatisticsType::BaseTable || tableSpecific->SortColumns.empty()) {
        return false;
    }
    const auto& relName = RelInfo_[tableSpecific->RelBitmap.FirstNonZeroBit()].Label;
    NDq::TJoinColumn sortColumn(relName, tableSpecific->SortColumns[0]);
    if (std::find(tableJoinKeys.begin(), tableJoinKeys.end(), sortColumn) == tableJoinKeys.end()) {
        return false;
    }
    if (lookupTable.Nrows > Limits_.LookupJoinMaxRows ||
        !lookupTableSpecific->SizeInfo.LookupJoinMemSize || *lookupTableSpecific->SizeInfo.LookupJoinMemSize >= Limits_.LookupJoinMemLimit) {
        return false;
    }

    return true;
}

bool TYtProviderContext::IsJoinApplicable(
    const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    const TVector<NDq::TJoinColumn>& leftJoinKeys,
    const TVector<NDq::TJoinColumn>& /*rightJoinKeys*/,
    EJoinAlgoType joinAlgo,
    EJoinKind /*joinKind*/) {
    if (joinAlgo == EJoinAlgoType::LookupJoin) {
        return !leftJoinKeys.empty() && IsLookupJoinApplicable(left->Stats, right->Stats, leftJoinKeys);
    }
    if (joinAlgo == EJoinAlgoType::MapJoin) {
        return IsMapJoinApplicable(right->Stats);
    }
    return joinAlgo == EJoinAlgoType::MergeJoin;
}

TOptimizerStatistics TYtProviderContext::ComputeJoinStatsV1(
    const TOptimizerStatistics& leftStats,
    const TOptimizerStatistics& rightStats,
    const TVector<NDq::TJoinColumn>& leftJoinKeys,
    const TVector<NDq::TJoinColumn>& rightJoinKeys,
    EJoinAlgoType joinAlgo,
    EJoinKind /*joinKind*/,
    TCardinalityHints::TCardinalityHint* /*maybeHint*/,
    bool shuffleLeftSide,
    bool shuffleRightSide) const {

    Y_UNUSED(shuffleLeftSide, shuffleRightSide);

    const TYtProviderStatistic* leftSpecific = static_cast<const TYtProviderStatistic*>(leftStats.Specific.get());
    const TYtProviderStatistic* rightSpecific = static_cast<const TYtProviderStatistic*>(rightStats.Specific.get());

    TMaybe<double> maxUniques;
    auto leftMaxUniques = FindMaxUniqueVals(*leftSpecific, leftJoinKeys);
    if (leftMaxUniques && (!maxUniques || *maxUniques < *leftMaxUniques)) {
        maxUniques = *leftMaxUniques;
    }
    auto rightMaxUniques = FindMaxUniqueVals(*rightSpecific, rightJoinKeys);
    if (rightMaxUniques && (!maxUniques || *maxUniques < *rightMaxUniques)) {
        maxUniques = *rightMaxUniques;
    }

    auto resultSpecific = std::make_unique<TYtProviderStatistic>();

    resultSpecific->RelBitmap = leftSpecific->RelBitmap | rightSpecific->RelBitmap;
    resultSpecific->JoinAlgo = joinAlgo;

    double outputCardinality = ComputeCardinality(maxUniques, leftStats.Nrows, rightStats.Nrows);
    double leftCardinalityFactor = leftStats.Nrows != 0.0 ? outputCardinality / leftStats.Nrows : 0.0;
    double rightCardinalityFactor = rightStats.Nrows != 0.0 ? outputCardinality / rightStats.Nrows : 0.0;

    double outputByteSize = leftCardinalityFactor * leftStats.ByteSize + rightCardinalityFactor * rightStats.ByteSize;

    auto leftReadBytes = leftStats.ByteSize;
    double rightReadBytes = rightStats.ByteSize;

    if (joinAlgo == EJoinAlgoType::LookupJoin) {
        auto leftJoinUniques = FindMaxUniqueVals(*leftSpecific, TVector<NDq::TJoinColumn>{leftJoinKeys[0]});
        auto rightJoinUniques = FindMaxUniqueVals(*rightSpecific, TVector<NDq::TJoinColumn>{rightJoinKeys[0]});
        if (leftJoinUniques && rightJoinUniques && *rightJoinUniques < *leftJoinUniques) {
            leftReadBytes *= *rightJoinUniques / *leftJoinUniques;
        }
    }

    if (joinAlgo == EJoinAlgoType::MapJoin &&
        (leftSpecific->JoinAlgo == EJoinAlgoType::MapJoin || leftSpecific->JoinAlgo == EJoinAlgoType::LookupJoin) &&
        rightStats.Type == EStatisticsType::BaseTable) {
        // Optimistically assume that consecutive MapJoins are fused together.
        leftReadBytes = 0;
    }

    double outputCost =
        leftStats.Cost + rightStats.Cost +
        leftReadBytes + rightReadBytes +
        outputByteSize;

    TOptimizerStatistics result(
        EStatisticsType::ManyManyJoin,
        outputCardinality,
        leftStats.Ncols + rightStats.Ncols,
        outputByteSize,
        outputCost,
        {}, {}, EStorageType::NA, std::move(resultSpecific));

    return result;
}

}  // namespace NYql
