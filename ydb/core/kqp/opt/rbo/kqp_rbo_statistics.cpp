#include "kqp_rbo_statistics.h"
#include "kqp_operator.h"

namespace NKikimr {
namespace NKqp {

TString TRBOMetadata::ToString(ui32 printOptions) {
    TStringBuilder builder;

    if (printOptions & EPrintPlanOptions::PrintBasicMetadata) {
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

        builder << ", Storage: " << storageType << ", KeyCols: [";

        for (size_t i=0; i<KeyColumns.size(); i++) {
            builder << KeyColumns[i].Alias << "." << KeyColumns[i].ColumnName;
            if (i!=KeyColumns.size()-1) {
                builder << ", ";
            }
        }

        builder << "]";
    }

    return builder;
}

TString TRBOStatistics::ToString(ui32 printOptions) {
    Y_UNUSED(printOptions);
    return TStringBuilder() << "N records: " << RecordsCount << ", Size: " << DataSize << ", Selectivity: " << Selectivity; 
}

}
}