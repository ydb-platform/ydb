#pragma once

#include <optional>
#include <iostream>

namespace NYql {

/**
 * Optimizer Statistics struct records per-table and per-column statistics
 * for the current operator in the plan. Currently, only Nrows and Ncols are
 * recorded.
 * Cost is also included in statistics, as its updated concurrently with statistics
 * all of the time. Cost is optional, so it could be missing.
*/
struct TOptimizerStatistics {
    double Nrows = 0;
    int Ncols = 0;
    std::optional<double> Cost;

    TOptimizerStatistics() : Cost(std::nullopt) {}
    TOptimizerStatistics(double nrows,int ncols): Nrows(nrows), Ncols(ncols), Cost(std::nullopt) {}
    TOptimizerStatistics(double nrows,int ncols, double cost): Nrows(nrows), Ncols(ncols), Cost(cost) {}

    TOptimizerStatistics& operator+=(const TOptimizerStatistics& other);
    bool Empty() const;

    friend std::ostream& operator<<(std::ostream& os, const TOptimizerStatistics& s);
};
}
