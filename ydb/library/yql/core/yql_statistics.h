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
    double Cost = 0;
    double Selectivity = 1.0;
    const TVector<TString>& KeyColumns;

    TOptimizerStatistics() : KeyColumns(EmptyColumns) {}
    TOptimizerStatistics(double nrows, int ncols): Nrows(nrows), Ncols(ncols), KeyColumns(EmptyColumns) {}
    TOptimizerStatistics(double nrows, int ncols, double cost): Nrows(nrows), Ncols(ncols), Cost(cost), KeyColumns(EmptyColumns) {}
    TOptimizerStatistics(EStatisticsType type, double nrows, int ncols, double cost): Type(type), Nrows(nrows), Ncols(ncols), Cost(cost), KeyColumns(EmptyColumns) {}
    TOptimizerStatistics(EStatisticsType type, double nrows, int ncols, double cost, const TVector<TString>& keyColumns): Type(type), Nrows(nrows), Ncols(ncols), Cost(cost), KeyColumns(keyColumns) {}

    TOptimizerStatistics& operator+=(const TOptimizerStatistics& other);
    bool Empty() const;

    friend std::ostream& operator<<(std::ostream& os, const TOptimizerStatistics& s);

    static const TVector<TString>& EmptyColumns;
};
}
