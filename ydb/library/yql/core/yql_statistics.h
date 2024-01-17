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
    double Cost;
    TVector<TString> KeyColumns;

    TString Descr;

    TOptimizerStatistics() {}
    TOptimizerStatistics(double nrows, int ncols): Nrows(nrows), Ncols(ncols) {}
    TOptimizerStatistics(double nrows, int ncols, double cost): Nrows(nrows), Ncols(ncols), Cost(cost) {}
    TOptimizerStatistics(EStatisticsType type, double nrows, int ncols, double cost): Type(type), Nrows(nrows), Ncols(ncols), Cost(cost) {}
    TOptimizerStatistics(EStatisticsType type, double nrows, int ncols, double cost, TVector<TString> keyColumns): Type(type), Nrows(nrows), Ncols(ncols), Cost(cost), KeyColumns(keyColumns) {}
    TOptimizerStatistics(double nrows,int ncols, double cost, TString descr): Nrows(nrows), Ncols(ncols), Cost(cost), Descr(descr) {}


    TOptimizerStatistics& operator+=(const TOptimizerStatistics& other);
    bool Empty() const;

    friend std::ostream& operator<<(std::ostream& os, const TOptimizerStatistics& s);
};
}
