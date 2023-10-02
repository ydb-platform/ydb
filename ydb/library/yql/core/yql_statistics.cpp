#include "yql_statistics.h"

using namespace NYql;

std::ostream& NYql::operator<<(std::ostream& os, const TOptimizerStatistics& s) {
    os << "Nrows: " << s.Nrows << ", Ncols: " << s.Ncols << ", Cost: " << s.Cost ;
    return os;
}

bool TOptimizerStatistics::Empty() const {
    return ! (Nrows || Ncols || Cost);
}

TOptimizerStatistics& TOptimizerStatistics::operator+=(const TOptimizerStatistics& other) {
    Nrows += other.Nrows;
    Ncols += other.Ncols;
    Cost += other.Cost;
    return *this;
}
