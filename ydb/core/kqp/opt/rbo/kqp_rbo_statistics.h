#pragma once

#include <yql/essentials/core/yql_statistics.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

struct TInfoUnit;
struct TPhysicalOpProps;

struct TColumnLineage {
    TColumnLineage(TString alias, TString tableName, TString columnName) : SourceAlias(alias), 
        TableName(tableName),
        ColumnName(columnName) {}

    TString GetCannonicalAlias() {
        if (SourceAlias != "") {
            return SourceAlias;
        }
        else {
            return TableName;
        }
    }

    TString SourceAlias;
    TString TableName;
    TString ColumnName;
};

class TRBOMetadata {
public:
    EStatisticsType Type = EStatisticsType::BaseTable;
    EStorageType StorageType = EStorageType::NA;

    THashMap<TString, TColumnLineage> ColumnLineage;
    TVector<TInfoUnit> KeyColumns;
    int ColumnsCount = 0;
    TVector<TInfoUnit> ShuffledByColumns;
    TVector<std::pair<TInfoUnit,bool>> SortColumns;

    std::optional<std::int64_t> SortingOrderingIdx;
    std::optional<std::int64_t> ShufflingOrderingIdx;

    TInfoUnit MapColumn(TInfoUnit col);
    TString ToString(ui32 printOptions);
};

class TRBOStatistics {
public:
    double RecordsCount = 0;
    double DataSize = 0;
    double Selectivity = 1.0;

    TString ToString(ui32 printOptions);
};

TOptimizerStatistics BuildOptimizerStatistics(TPhysicalOpProps & props, bool withStatsAndCosts);
TOptimizerStatistics BuildOptimizerStatistics(TPhysicalOpProps & props, bool withStatsAndCosts, TVector<TInfoUnit> keyColumns);

}
}