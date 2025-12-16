#include "kqp_rbo_statistics.h"
#include "kqp_operator.h"

namespace NKikimr {
namespace NKqp {


TInfoUnit TRBOMetadata::MapColumn(TInfoUnit key) {
    auto fullName = key.GetFullName();
    if (ColumnLineage.contains(fullName)) {
        return TInfoUnit(ColumnLineage.at(fullName).GetCannonicalAlias(), ColumnLineage.at(fullName).ColumnName);
    } else {
        return key;
    }
}
TOptimizerStatistics BuildOptimizerStatistics(TPhysicalOpProps & props, bool withStatsAndCosts ){
    return BuildOptimizerStatistics(props, withStatsAndCosts, {});
}

TOptimizerStatistics BuildOptimizerStatistics(TPhysicalOpProps & props, bool withStatsAndCosts, TVector<TInfoUnit> keyColumns) {
    TVector<TString> keyColumnNames;
    for (auto iu: (keyColumns.empty() ? props.Metadata->KeyColumns : keyColumns)) {
        keyColumnNames.push_back(iu.GetColumnName());
    }

    double cost = props.Cost.has_value() ? *props.Cost : 0.0;

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
        for (auto &[k, v] : ColumnLineage) {
            builder << k << ": <ColName: " << v.ColumnName 
                << ", Alias: " << v.SourceAlias 
                << ", Table: " << v.TableName
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