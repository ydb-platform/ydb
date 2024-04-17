#pragma once

#include <util/generic/vector.h>
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

/**
 * Optimizer Statistics struct records per-table and per-column statistics
 * for the current operator in the plan. Currently, only Nrows and Ncols are
 * recorded.
 * Cost is also included in statistics, as its updated concurrently with statistics
 * all of the time.
*/
struct TOptimizerStatistics {
    EStatisticsType Type = BaseTable;
    double Nrows = 0;
    int Ncols = 0;
    double ByteSize = 0;
    double Cost = 0;
    double Selectivity = 1.0;
    const TVector<TString>& KeyColumns;
    std::unique_ptr<const IProviderStatistics> Specific;

    TOptimizerStatistics(TOptimizerStatistics&&) = default;
    TOptimizerStatistics() : KeyColumns(EmptyColumns) {}

    TOptimizerStatistics(
        EStatisticsType type,
        double nrows = 0.0,
        int ncols = 0,
        double byteSize = 0.0,
        double cost = 0.0,
        const TVector<TString>& keyColumns = EmptyColumns,
        std::unique_ptr<IProviderStatistics> specific = nullptr);

    TOptimizerStatistics& operator+=(const TOptimizerStatistics& other);
    bool Empty() const;

    friend std::ostream& operator<<(std::ostream& os, const TOptimizerStatistics& s);

    static const TVector<TString>& EmptyColumns;
};
}
