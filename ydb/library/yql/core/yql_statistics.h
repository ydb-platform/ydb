#pragma once

#include <ydb/library/minsketch/count_min_sketch.h>

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

// Providers may subclass this struct to associate specific statistics, useful to
// derive stats for higher-level operators in the plan.
struct IProviderStatistics {
    virtual ~IProviderStatistics() {}
};

struct TColumnStatistics {
    std::optional<double> NumUniqueVals;
    std::optional<double> HyperLogLog;
    std::shared_ptr<NKikimr::TCountMinSketch> CountMinSketch;
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
        TKeyColumns(const TVector<TString>& vec) : Data(vec) {}
    };

    struct TColumnStatMap : public TSimpleRefCount<TColumnStatMap> {
        THashMap<TString,TColumnStatistics> Data;
        TColumnStatMap() {}
        TColumnStatMap(const THashMap<TString,TColumnStatistics>& map) : Data(map) {}
    };

    EStatisticsType Type = BaseTable;
    double Nrows = 0;
    int Ncols = 0;
    double ByteSize = 0;
    double Cost = 0;
    double Selectivity = 1.0;
    TIntrusivePtr<TKeyColumns> KeyColumns;
    TIntrusivePtr<TColumnStatMap> ColumnStatistics;
    std::unique_ptr<const IProviderStatistics> Specific;
    std::shared_ptr<TVector<TString>> Labels = {};

    TOptimizerStatistics(TOptimizerStatistics&&) = default;
    TOptimizerStatistics() {}

    TOptimizerStatistics(
        EStatisticsType type,
        double nrows = 0.0,
        int ncols = 0,
        double byteSize = 0.0,
        double cost = 0.0,
        TIntrusivePtr<TKeyColumns> keyColumns = {},
        TIntrusivePtr<TColumnStatMap> columnMap = {},
        std::unique_ptr<IProviderStatistics> specific = nullptr);

    TOptimizerStatistics& operator+=(const TOptimizerStatistics& other);
    bool Empty() const;

    friend std::ostream& operator<<(std::ostream& os, const TOptimizerStatistics& s);

    TString ToString() const;
};

std::shared_ptr<TOptimizerStatistics> OverrideStatistics(const TOptimizerStatistics& s, const TStringBuf& tablePath, const std::shared_ptr<NJson::TJsonValue>& stats);

}
