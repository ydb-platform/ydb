#include "kqp_rbo_statistics.h"
#include "kqp_operator.h"
#include <ydb/core/kqp/common/kqp_yql.h>

namespace NKikimr {
namespace NKqp {


void TColumnLineage::AddMapping(const TInfoUnit& unit, const TColumnLineageEntry& entry) {
    const auto rawAlias = entry.GetRawAlias();

    if (entry.DuplicateNo != 0) {
        int duplicateId = entry.DuplicateNo;
        if (MaxDuplicateId.contains(rawAlias)) {
            int maxId = MaxDuplicateId.at(rawAlias);
            if (maxId > duplicateId) {
                duplicateId = maxId;
            }
        }
        Mapping.insert({unit, entry});
        MaxDuplicateId.insert({rawAlias, duplicateId});
    } else {
        Mapping.insert({unit, entry});
        MaxDuplicateId.insert({rawAlias, 0});
    }
    ReverseMapping.insert({TInfoUnit(entry.GetCannonicalAlias(), entry.ColumnName), unit});
}

int TColumnLineage::AddAlias(const TString& alias, const TString& tableName) {
    const auto rawAlias = alias != "" ? alias : tableName;
    int duplicateId = 0;
    if (MaxDuplicateId.contains(rawAlias)) {
        duplicateId = MaxDuplicateId.at(rawAlias) + 1;
    }
    MaxDuplicateId.insert({rawAlias, duplicateId});
    return duplicateId;
}

void TColumnLineage::Merge(const TColumnLineage& other) {
    // We'll be adding mappings one by one, so MaxDuplicateId will be
    // changing. Thus we save here before we start merging to detect
    // conficts correctly
    auto currDuplicates = MaxDuplicateId;

    for (auto [iu, entry] : other.Mapping) {
        auto rawAlias = entry.GetRawAlias();
        if (currDuplicates.contains(rawAlias)) {
            entry.DuplicateNo = currDuplicates.at(rawAlias) + 1;
        }
        AddMapping(iu, entry);
    }
}

TInfoUnit TRBOMetadata::MapColumn(const TInfoUnit& key) {
    if (ColumnLineage.Mapping.contains(key)) {
        return ColumnLineage.Mapping.at(key).GetInfoUnit();
    } else {
        return key;
    }
}

TOptimizerStatistics BuildOptimizerStatistics(TPhysicalOpProps& props, bool withStatsAndCosts, const NYql::TTypeAnnotationContext& typeCtx) {
    TVector<TInfoUnit> mappedKeyColumns;
    TVector<TString> keyColumnNames;

    for (const auto& col : props.Metadata->KeyColumns) {
        mappedKeyColumns.push_back(props.Metadata->MapColumn(col));
    }

    for (const auto& iu: mappedKeyColumns) {
        keyColumnNames.push_back(iu.GetColumnName());
    }

    const double cost = props.Cost.has_value() ? *props.Cost : 0.0;

    // Build column statistics for the set of IUs
    // Use lineage table to obtain table and column names, look up table names in the
    // type annotation context and place them in the local map. If there are multiple tables - 
    // then its a result of join or set operation, don't create column statistics
    TString table;
    THashSet<TString> attributes;

    for (const auto& [iu, lineageEntry]: props.Metadata->ColumnLineage.Mapping) {
        if (table != "" && table != lineageEntry.TableName) {
            attributes.clear();
            break;
        }
        table = lineageEntry.TableName;
        attributes.insert(lineageEntry.ColumnName);
    }

    TIntrusivePtr<TOptimizerStatistics::TColumnStatMap> ColumnStatistics;

    THashMap<TString, TColumnStatistics> columnStatsMap;

    if (attributes.size() && typeCtx.ColumnStatisticsByTableName.contains(table)) {
        const auto& globalMap = typeCtx.ColumnStatisticsByTableName.at(table)->Data;

        for (const auto& columnName : attributes) {
            if (globalMap.contains(columnName)) {
                columnStatsMap.insert({columnName, globalMap.at(columnName)});
            }    
        }

        if (columnStatsMap.size()) {
            ColumnStatistics = MakeIntrusive<TOptimizerStatistics::TColumnStatMap>(
                TOptimizerStatistics::TColumnStatMap(columnStatsMap));
        }
    }


    return TOptimizerStatistics(props.Metadata->Type, 
        withStatsAndCosts ? props.Statistics->EBytes : 0.0,
        props.Metadata->ColumnsCount,
        withStatsAndCosts ? props.Statistics->EBytes : 0.0,
        withStatsAndCosts ? cost : 0.0,
        TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(
            new TOptimizerStatistics::TKeyColumns(keyColumnNames)),
        ColumnStatistics
        );
}

TString TRBOMetadata::ToString(ui32 printOptions) {
    TStringBuilder builder;

    if (printOptions & (EPrintPlanOptions::PrintBasicMetadata | EPrintPlanOptions::PrintFullMetadata)) {
        TString metadataType;

        switch (Type) {
            case EStatisticsType::BaseTable:
                metadataType = "BaseTable";
                break;
            case EStatisticsType::FilteredFactTable:
                metadataType = "FilteredFactTable";
                break;
            case EStatisticsType::ManyManyJoin:
                metadataType = "ManyManyJoin";
                break;
        default:
            Y_ENSURE(false,"Unknown EStatisticsType");
        }

        builder << "Type: " << metadataType;

        TString storageType;

        switch(StorageType) {
            case EStorageType::RowStorage:
                storageType = "Row";
                break;
            case EStorageType::ColumnStorage:
                storageType = "Column";
                break;
            case EStorageType::NA:
                storageType = "N/A";
                break;
        }

        builder << ", ColumnsCount: " << ColumnsCount << ", Storage: " << storageType << ", KeyCols: [";

        for (size_t i = 0; i < KeyColumns.size(); i++) {
            builder << KeyColumns[i].GetFullName();
            if (i != KeyColumns.size() - 1) {
                builder << ", ";
            }
        }

        builder << "]";
    }

    if (printOptions & EPrintPlanOptions::PrintFullMetadata) {
        builder << ", Lineage: {";
        for (const auto &[k, v] : ColumnLineage.Mapping) {
            builder << k.GetFullName() << ": <ColName: " << v.ColumnName 
                << ", Alias: " << v.SourceAlias 
                << ", Table: " << v.TableName
                << ", DuplicateNo: " << v.DuplicateNo
                << ">, ";
        }
        builder << "}";
    }

    return builder;
}

TString TRBOStatistics::ToString(ui32 printOptions) {
    Y_UNUSED(printOptions);
    return TStringBuilder() << "N records: " << ERows << ", Size: " << EBytes << ", Selectivity: " << Selectivity;
}

}
}