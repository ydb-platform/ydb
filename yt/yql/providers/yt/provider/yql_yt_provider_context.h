#pragma once

#include <util/generic/bitmap.h>
#include <util/generic/maybe.h>

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>

namespace NYql {

struct TYtColumnStatistic {
    TString ColumnName;
    TMaybe<int64_t> EstimatedUniqueCount;
    TMaybe<int64_t> DataWeight;
};

struct TRelSizeInfo {
    TMaybe<ui64> MapJoinMemSize;
    TMaybe<ui64> LookupJoinMemSize;
};

struct TYtProviderStatistic : public IProviderStatistics {
    TDynBitMap RelBitmap;
    TVector<TString> SortColumns;
    TMaybe<EJoinAlgoType> JoinAlgo;
    TRelSizeInfo SizeInfo;
};

struct TYtProviderRelInfo {
    TString Label;
    TVector<TYtColumnStatistic> ColumnInfo;
    TVector<TString> SortColumns;
};

class TYtProviderContext : public TBaseProviderContext {
public:
    struct TJoinAlgoLimits {
        ui64 MapJoinMemLimit;
        ui64 LookupJoinMemLimit;
        ui64 LookupJoinMaxRows;
    };

    TYtProviderContext(TJoinAlgoLimits limits, TVector<TYtProviderRelInfo> relInfo);

    virtual TOptimizerStatistics ComputeJoinStats(
        const TOptimizerStatistics& leftStats,
        const TOptimizerStatistics& rightStats,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind,
        TCardinalityHints::TCardinalityHint* maybeHint = nullptr,
        bool shuffleLeftSide = false,
        bool shuffleRightSide = false) const override;

    bool IsJoinApplicable(
        const std::shared_ptr<IBaseOptimizerNode>& leftStats,
        const std::shared_ptr<IBaseOptimizerNode>& rightStats,
        const TVector<NDq::TJoinColumn>& leftJoinKeys,
        const TVector<NDq::TJoinColumn>& rightJoinKeys,
        EJoinAlgoType joinAlgo,
        EJoinKind joinKind) override;

private:
    bool IsLookupJoinApplicable(const TOptimizerStatistics& table, const TOptimizerStatistics& lookupTable, const TVector<NDq::TJoinColumn>& tableJoinKeys) const;

    bool IsMapJoinApplicable(const TOptimizerStatistics& table) const;

    TDynBitMap ExtractColumnsBitmap(const TDynBitMap& columnBitmap, const TVector<TString>& columns) const;

    TVector<TYtColumnStatistic> MergeColumnStatistics(const TYtProviderStatistic& leftSpecific, const TYtProviderStatistic& rightSpecific, const TDynBitMap& outputBitmap) const;

    TMaybe<double> FindMaxUniqueVals(const TYtProviderStatistic& specific, const TVector<NDq::TJoinColumn>& columns) const;

    TMaybe<double> ColumnNumUniqueValues(const TDynBitMap& relMap,  const NDq::TJoinColumn& columnName) const;

    TString DebugStatistic(const TVector<NDq::TJoinColumn>& columns, const TOptimizerStatistics& stat) const;

    const TJoinAlgoLimits Limits_;
    TVector<TYtProviderRelInfo> RelInfo_;
    THashMap<TString, THashSet<std::pair<int, int>>> ColumnIndex_;
};

}  // namespace NYql
