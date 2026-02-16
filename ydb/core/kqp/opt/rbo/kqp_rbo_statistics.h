#pragma once

#include "kqp_info_unit.h"
#include <yql/essentials/core/yql_statistics.h>

namespace NKikimr {
namespace NKqp {

using namespace NYql;

struct TPhysicalOpProps;

struct TColumnLineageEntry {
    TColumnLineageEntry(TString alias, TString tableName, TString columnName) : SourceAlias(alias), 
        TableName(tableName),
        ColumnName(columnName) {}

    TColumnLineageEntry(TString alias, TString tableName, TString columnName, int duplicateNo) : SourceAlias(alias), 
        TableName(tableName),
        ColumnName(columnName),
        DuplicateNo(duplicateNo) {}

    TString GetCannonicalAlias() const {
        TStringBuilder res;

        if (SourceAlias != "") {
            res << SourceAlias;
        }
        else {
            res << TableName;
        }

        if (DuplicateNo != 0) {
            res << "_#" << DuplicateNo;
        }

        return res;
    }

    TString GetSourceAlias() const {
        if (SourceAlias != "") {
            return GetCannonicalAlias();
        }
        else {
            return "";
        }
    }

    TString GetRawAlias() const {
        if (SourceAlias != "") {
            return SourceAlias;
        } else {
            return TableName;
        }
    }

    TInfoUnit GetInfoUnit() const {
        return TInfoUnit(GetCannonicalAlias(), ColumnName);
    }

    TString SourceAlias;
    TString TableName;
    TString ColumnName;
    int DuplicateNo{0};
};

struct TColumnLineage {
    void AddMapping(const TInfoUnit& unit, const TColumnLineageEntry& entry);
    int AddAlias(const TString& alias, const TString& tableName);
    void Merge(const TColumnLineage& other);

    THashMap<TInfoUnit, TColumnLineageEntry, TInfoUnit::THashFunction> Mapping;
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> ReverseMapping;
    THashMap<TString, int, TInfoUnit::THashFunction> MaxDuplicateId;
};

class TRBOMetadata {
public:
    EStatisticsType Type = EStatisticsType::BaseTable;
    EStorageType StorageType = EStorageType::NA;

    TColumnLineage ColumnLineage;
    TVector<TInfoUnit> KeyColumns;
    ui32 ColumnsCount = 0;
    TVector<TInfoUnit> ShuffledByColumns;
    TVector<std::pair<TInfoUnit,bool>> SortColumns;

    std::optional<std::int64_t> SortingOrderingIdx;
    std::optional<std::int64_t> ShufflingOrderingIdx;

    TInfoUnit MapColumn(const TInfoUnit& col);
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
TOptimizerStatistics BuildOptimizerStatistics(TPhysicalOpProps & props, bool withStatsAndCosts, const TVector<TInfoUnit>& keyColumns);

}
}