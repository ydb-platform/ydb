#pragma once

#include "yql_cost_function.h"
#include <yql/essentials/core/minsketch/count_min_sketch.h>
#include <yql/essentials/core/histogram/eq_width_histogram.h>

#include <library/cpp/json/json_reader.h>

#include <util/generic/vector.h>
#include <util/generic/hash.h>

#include <util/generic/string.h>
#include <optional>
#include <iostream>

namespace NYql {

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

// Providers may subclass this struct to associate specific statistics, useful to
// derive stats for higher-level operators in the plan.
struct IProviderStatistics {
    virtual ~IProviderStatistics() {}
};

struct TColumnStatistics {
    std::optional<double> NumUniqueVals;
    std::optional<double> HyperLogLog;
    std::shared_ptr<NKikimr::TCountMinSketch> CountMinSketch;
    std::shared_ptr<NKikimr::NOptimizerHistograms::TEqWidthHistogramEstimator> EqWidthHistogramEstimator;
    TString Type;

    TColumnStatistics() {}
};

/**
 * Optimizer Statistics struct records per-table and per-column statistics
 * for the current operator in the plan. Currently, only Nrows and Ncols are
 * recorded.
 * Cost is also included in statistics, as its updated concurrently with statistics
 * all of the time.
*/
struct TOptimizerStatistics {
    struct TKeyColumns : public TSimpleRefCount<TKeyColumns> {
        TVector<TString> Data;
        TKeyColumns(TVector<TString> data) : Data(std::move(data)) {}
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
        THashMap<TString,TColumnStatistics> Data;
        TColumnStatMap() {}
        TColumnStatMap(THashMap<TString,TColumnStatistics> data) : Data(std::move(data)) {}
    };

    struct TShuffledByColumns : public TSimpleRefCount<TShuffledByColumns> {
        TVector<NDq::TJoinColumn> Data;
        TShuffledByColumns(TVector<NDq::TJoinColumn> data) : Data(std::move(data)) {}
    };

    EStatisticsType Type = BaseTable;
    double Nrows = 0;
    int Ncols = 0;
    double ByteSize = 0;
    double Cost = 0;
    double Selectivity = 1.0;
    TIntrusivePtr<TKeyColumns> KeyColumns;
    TIntrusivePtr<TColumnStatMap> ColumnStatistics;
    TIntrusivePtr<TShuffledByColumns> ShuffledByColumns;
    EStorageType StorageType = EStorageType::NA;
    TIntrusivePtr<TSortColumns> SortColumns;
    std::shared_ptr<IProviderStatistics> Specific;
    std::shared_ptr<TVector<TString>> Labels = {};

    TOptimizerStatistics(TOptimizerStatistics&&) = default;
    TOptimizerStatistics& operator=(TOptimizerStatistics&&) = default;
    TOptimizerStatistics(const TOptimizerStatistics&) = default;
    TOptimizerStatistics& operator=(const TOptimizerStatistics&) = default;
    TOptimizerStatistics() = default;

    TOptimizerStatistics(
        EStatisticsType type,
        double nrows = 0.0,
        int ncols = 0,
        double byteSize = 0.0,
        double cost = 0.0,
        TIntrusivePtr<TKeyColumns> keyColumns = {},
        TIntrusivePtr<TColumnStatMap> columnMap = {},
        EStorageType storageType = EStorageType::NA,
        std::shared_ptr<IProviderStatistics> specific = nullptr);

    TOptimizerStatistics& operator+=(const TOptimizerStatistics& other);
    bool Empty() const;

    friend std::ostream& operator<<(std::ostream& os, const TOptimizerStatistics& s);

    TString ToString() const;
};

std::shared_ptr<TOptimizerStatistics> OverrideStatistics(const TOptimizerStatistics& s, const TStringBuf& tablePath, const std::shared_ptr<NJson::TJsonValue>& stats);

}
