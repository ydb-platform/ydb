#include "kqp_rbo_statistics.h"
#include "kqp_operator.h"

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

TOptimizerStatistics BuildOptimizerStatistics(TPhysicalOpProps& props, bool withStatsAndCosts) {
    TVector<TInfoUnit> keyColumns;
    return BuildOptimizerStatistics(props, withStatsAndCosts, keyColumns);
}

TOptimizerStatistics BuildOptimizerStatistics(TPhysicalOpProps& props, bool withStatsAndCosts, const TVector<TInfoUnit>& keyColumns) {
    TVector<TString> keyColumnNames;
    for (const auto& iu: (keyColumns.empty() ? props.Metadata->KeyColumns : keyColumns)) {
        keyColumnNames.push_back(iu.GetColumnName());
    }

    const double cost = props.Cost.has_value() ? *props.Cost : 0.0;

    return TOptimizerStatistics(props.Metadata->Type, 
        withStatsAndCosts ? props.Statistics->DataSize : 0.0,
        props.Metadata->ColumnsCount,
        withStatsAndCosts ? props.Statistics->DataSize : 0.0,
        withStatsAndCosts ? cost : 0.0,
        TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(
            new TOptimizerStatistics::TKeyColumns(keyColumnNames)));
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
            builder << KeyColumns[i].GetAlias() << "." << KeyColumns[i].GetColumnName();
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
    return TStringBuilder() << "N records: " << RecordsCount << ", Size: " << DataSize << ", Selectivity: " << Selectivity; 
}

}
}