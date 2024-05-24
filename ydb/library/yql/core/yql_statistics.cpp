#include <util/generic/yexception.h>
#include "yql_statistics.h"

using namespace NYql;

static TString ConvertToStatisticsTypeString(EStatisticsType type) {
    switch (type) {
        case EStatisticsType::BaseTable:
            return "BaseTable";
        case EStatisticsType::FilteredFactTable:
            return "FilteredFactTable";
        case EStatisticsType::ManyManyJoin:
            return "ManyManyJoin";
        default:
            Y_ENSURE(false,"Unknown EStatisticsType");
    }
    return "";
}

std::ostream& NYql::operator<<(std::ostream& os, const TOptimizerStatistics& s) {
    os << "Type: " << ConvertToStatisticsTypeString(s.Type) << ", Nrows: " << s.Nrows
        << ", Ncols: " << s.Ncols << ", ByteSize: " << s.ByteSize << ", Cost: " << s.Cost;
    return os;
}

bool TOptimizerStatistics::Empty() const {
    return ! (Nrows || Ncols || Cost);
}

TOptimizerStatistics::TOptimizerStatistics(
    EStatisticsType type,
    double nrows,
    int ncols,
    double byteSize,
    double cost,
    const TVector<TString>& keyColumns,
    std::unique_ptr<IProviderStatistics> specific)
    : Type(type)
    , Nrows(nrows)
    , Ncols(ncols)
    , ByteSize(byteSize)
    , Cost(cost)
    , KeyColumns(keyColumns)
    , Specific(std::move(specific))
{
}

TOptimizerStatistics& TOptimizerStatistics::operator+=(const TOptimizerStatistics& other) {
    Nrows += other.Nrows;
    Ncols += other.Ncols;
    ByteSize += other.ByteSize;
    Cost += other.Cost;
    return *this;
}

const TVector<TString>& TOptimizerStatistics::EmptyColumns = TVector<TString>();
