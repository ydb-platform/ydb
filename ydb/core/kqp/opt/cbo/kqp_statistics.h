#pragma once

#include "cbo_interesting_orderings.h"

#include <yql/essentials/core/minsketch/count_min_sketch.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/generic/string.h>

#include <memory>
#include <optional>
#include <iostream>

// Forward-declare TExprNode used by TKqpStatsStore.
namespace NYql { class TExprNode; }

namespace NKikimr::NKqp {

enum EStatisticsType : ui32 {
    BaseTable,
    FilteredFactTable,
    ManyManyJoin
};

enum EStorageType : ui32 {
    NA,
    RowStorage,
    ColumnStorage
};

class IProviderStatistics {
public:
    virtual ~IProviderStatistics() {}
};

struct TColumnStatistics {
    std::optional<double> NumUniqueVals;
    std::optional<double> HyperLogLog;
    std::shared_ptr<NKikimr::TCountMinSketch> CountMinSketch;
    std::shared_ptr<NKikimr::TEqWidthHistogramEstimator> EqWidthHistogramEstimator;
    TString Type;

    TColumnStatistics() {}
};

class TShufflingOrderingsByJoinLabels {
public:
    void Add(TVector<TString> joinLabels, TOrderingsStateMachine::TLogicalOrderings shufflings) {
        std::sort(joinLabels.begin(), joinLabels.end());
        Entries_.emplace_back(std::move(joinLabels), std::move(shufflings));
    }

    TMaybe<TOrderingsStateMachine::TLogicalOrderings> GetShufflingOrderigsByJoinLabels(
        TVector<TString> searchingLabels
    ) {
        std::sort(searchingLabels.begin(), searchingLabels.end());
        for (const auto& [joinLabels, shufflings] : Entries_) {
            if (searchingLabels == joinLabels) {
                return shufflings;
            }
        }
        return Nothing();
    }

    TString ToString() const;

private:
    TVector<std::pair<TVector<TString>, TOrderingsStateMachine::TLogicalOrderings>> Entries_;
};

struct TOptimizerStatistics {
    struct TKeyColumns : public TSimpleRefCount<TKeyColumns> {
        TVector<TString> Data;
        explicit TKeyColumns(TVector<TString> data) : Data(std::move(data)) {}

        TVector<TJoinColumn> ToJoinColumns(const TString& alias) {
            TVector<TJoinColumn> columns;
            columns.reserve(Data.size());
            for (const auto& column : Data) {
                columns.push_back(TJoinColumn(alias, column));
            }
            return columns;
        }
    };

    struct TSortColumns : public TSimpleRefCount<TSortColumns> {
        TVector<TString> Columns;
        TVector<TString> Aliases;

        TSortColumns(const TVector<TString>& cols, const TVector<TString>& aliases)
            : Columns(cols)
            , Aliases(aliases)
        {}
    };

    struct TColumnStatMap : public TSimpleRefCount<TColumnStatMap> {
        THashMap<TString, TColumnStatistics> Data;
        TColumnStatMap() {}
        explicit TColumnStatMap(THashMap<TString, TColumnStatistics> data) : Data(std::move(data)) {}
    };

    struct TShuffledByColumns : public TSimpleRefCount<TShuffledByColumns> {
        TVector<TJoinColumn> Data;
        explicit TShuffledByColumns(TVector<TJoinColumn> data) : Data(std::move(data)) {}
        TString ToString() {
            TString result;
            for (const auto& column : Data) {
                result.append(column.RelName).append(".").append(column.AttributeName).append(", ");
            }
            if (!result.empty()) {
                result.pop_back();
                result.pop_back();
            }
            return result;
        }
    };

    EStatisticsType Type = BaseTable;
    double Nrows = 0;
    int Ncols = 0;
    double ByteSize = 0;
    double Cost = 0;
    double Selectivity = 1.0;
    ui32 JoinDepth = 1;
    TIntrusivePtr<TKeyColumns> KeyColumns;
    TIntrusivePtr<TColumnStatMap> ColumnStatistics;

    TIntrusivePtr<TShuffledByColumns> ShuffledByColumns;

    TIntrusivePtr<TSortColumns> SortColumns;
    EStorageType StorageType = EStorageType::NA;
    std::shared_ptr<IProviderStatistics> Specific;
    std::shared_ptr<TVector<TString>> Labels = {};

    TString SourceTableName;
    TSimpleSharedPtr<THashSet<TString>> Aliases;
    TIntrusivePtr<TTableAliasMap> TableAliases;

    TOrderingsStateMachine::TLogicalOrderings LogicalOrderings;
    TOrderingsStateMachine::TLogicalOrderings SortingOrderings;
    TOrderingsStateMachine::TLogicalOrderings ReversedSortingOrderings;

    std::optional<std::size_t> ShuffleOrderingIdx;
    std::int64_t SortingOrderingIdx = -1;
    std::int64_t ShufflingOrderingIdx = -1;

    bool CBOFired = false;

    TOptimizerStatistics(TOptimizerStatistics&&) = default;
    TOptimizerStatistics& operator=(TOptimizerStatistics&&) = default;
    TOptimizerStatistics(const TOptimizerStatistics&) = default;
    TOptimizerStatistics& operator=(const TOptimizerStatistics&) = default;
    TOptimizerStatistics() = default;

    explicit TOptimizerStatistics(
        EStatisticsType type,
        double nrows = 0.0,
        int ncols = 0,
        double byteSize = 0.0,
        double cost = 0.0,
        TIntrusivePtr<TKeyColumns> keyColumns = {},
        TIntrusivePtr<TColumnStatMap> columnMap = {},
        EStorageType storageType = EStorageType::NA,
        std::shared_ptr<IProviderStatistics> specific = nullptr
    );

    TOptimizerStatistics& operator+=(const TOptimizerStatistics& other);
    bool Empty() const;

    friend std::ostream& operator<<(std::ostream& os, const TOptimizerStatistics& s);

    TString ToString() const;
};

std::shared_ptr<TOptimizerStatistics> OverrideStatistics(
    const TOptimizerStatistics& s,
    const TStringBuf& tablePath,
    const std::shared_ptr<NJson::TJsonValue>& stats);

class TKqpStatsStore {
    THashMap<ui64, std::shared_ptr<TOptimizerStatistics>> Map_;
public:
    bool ContainsStats(const NYql::TExprNode* input) const;
    std::shared_ptr<TOptimizerStatistics> GetStats(const NYql::TExprNode* input) const;
    void SetStats(const NYql::TExprNode* input, std::shared_ptr<TOptimizerStatistics> stats);

    // Parallel to TypeCtx FSMs; NKqp types avoid conversion at the KQP/YQL stats boundary.
    TSimpleSharedPtr<TOrderingsStateMachine> ShufflingsFSM;
    TSimpleSharedPtr<TOrderingsStateMachine> SortingsFSM;
};

} // namespace NKikimr::NKqp
