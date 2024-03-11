#include "yql_statistics.h"

using namespace NYql;

std::ostream& NYql::operator<<(std::ostream& os, const TOptimizerStatistics& s) {
    os << "Type: " << s.Type << ", Nrows: " << s.Nrows << ", Ncols: " << s.Ncols << ", ByteSize: " << s.ByteSize << ", Cost: " << s.Cost ;
    return os;
}

bool TOptimizerStatistics::Empty() const {
    return ! (Nrows || Ncols || Cost);
}

TOptimizerStatistics& TOptimizerStatistics::operator+=(const TOptimizerStatistics& other) {
    Nrows += other.Nrows;
    Ncols += other.Ncols;
    ByteSize += other.ByteSize;
    Cost += other.Cost;
    return *this;
}

const TVector<TString>& TOptimizerStatistics::EmptyColumns = TVector<TString>();
