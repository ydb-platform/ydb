#pragma once

#include <yql/essentials/core/yql_statistics.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

struct TInfoUnit;

class TRBOMetadata {
public:
    EStatisticsType Type = EStatisticsType::BaseTable;
    EStorageType StorageType = EStorageType::NA;

    THashSet<TString> Aliases;
    TVector<TInfoUnit> KeyColumns;
    int ColumnsCount = 0;
    TVector<TInfoUnit> ShuffledByColumns;
    TVector<std::pair<TInfoUnit,bool>> SortColumns;

    std::optional<std::int64_t> SortingOrderingIdx;
    std::optional<std::int64_t> ShufflingOrderingIdx;

    TString ToString(ui32 printOptions);
};

class TRBOStatistics {
public:
    double RecordsCount = 0;
    double DataSize = 0;
    double Selectivity = 1.0;

    TString ToString(ui32 printOptions);
};

}
}