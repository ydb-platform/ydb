#include "yql_statistics.h"

using namespace NYql;

std::ostream& NYql::operator<<(std::ostream& os, const TOptimizerStatistics& s) {
    os << "Nrows: " << s.Nrows << ", Ncols: " << s.Ncols;
    os << ", Cost: ";
    if (s.Cost.has_value()){
        os << s.Cost.value();
    } else {
        os << "none";
    }
    return os;
}