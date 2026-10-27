#include "kqp_rbo_statistics.h"
#include "kqp_operator.h"
#include <ydb/core/kqp/common/kqp_yql.h>

namespace NKikimr {
namespace NKqp {

const TColumnLineageEntry* FindSourceStatistics(const IOperator& op, TInfoUnitId id, const TColumnLineage& lineage) {
    return op.Props.Metadata && op.Props.Metadata->SourceStatsColumns.Contains(id) ? lineage.Find(id) : nullptr;
}

TOptimizerStatistics BuildOptimizerStatistics(IOperator& op, const TColumnLineage& lineage, bool withStatsAndCosts, const NYql::TTypeAnnotationContext& typeCtx) {
    auto& props = op.Props;
    const auto& outputIUs = op.GetOutputIUs();
    TVector<TString> keyColumnNames;

    for (const auto id : props.Metadata->KeyColumns.Items()) {
        keyColumnNames.push_back(ToString(id));
    }

    const double cost = props.Cost.has_value() ? *props.Cost : 0.0;

    // Build column statistics for the set of IUs
    // Use lineage table to obtain table and column names, look up table names in the
    // type annotation context and place them in the local map. If there are multiple tables - 
    // then its a result of join or set operation, don't create column statistics
    TString table;
    THashSet<TString> attributes;

    for (const auto id : outputIUs) {
        const auto* lineageEntry = lineage.Find(id);
        if (!lineageEntry) {
            continue;
        }
        // Columns without source statistics take an empty table name, as
        // computed columns (e.g. aggregate results) have no source table.
        const TString sourceTable = props.Metadata->SourceStatsColumns.Contains(id) ? lineageEntry->TableName : TString{};
        if (table != "" && table != sourceTable) {
            attributes.clear();
            break;
        }
        table = sourceTable;
        attributes.insert(lineageEntry->ColumnName);
    }

    TIntrusivePtr<TOptimizerStatistics::TColumnStatMap> ColumnStatistics;

    THashMap<TString, TColumnStatistics> columnStatsMap;
    THashMap<TString, TMultiColumnStatistics> multiColumnStatsMap;

    if (attributes.size() && typeCtx.ColumnStatisticsByTableName.contains(table)) {
        const auto& globalStats = *typeCtx.ColumnStatisticsByTableName.at(table);
        const auto& globalMap = globalStats.Data;

        // The statistics consumer sees the same decimal IDs as expression ASTs.
        // Multiple bindings of one storage field each receive its statistics.
        THashMap<TString, TString> idByColumnName;
        for (const auto id : outputIUs) {
            const auto* source = FindSourceStatistics(op, id, lineage);
            if (!source) {
                continue;
            }
            idByColumnName[source->ColumnName] = ToString(id);
            if (const auto it = globalMap.find(source->ColumnName); it != globalMap.end()) {
                columnStatsMap.emplace(ToString(id), it->second);
            }
        }

        for (const auto& [_, multiColumnStats] : globalStats.MultiData) {
            TVector<TString> translatedColumns;
            for (const auto& column : multiColumnStats.Columns) {
                const auto it = idByColumnName.find(column);
                if (it == idByColumnName.end()) {
                    translatedColumns.clear();
                    break;
                }
                translatedColumns.push_back(it->second);
            }

            if (translatedColumns.empty()) {
                continue;
            }

            TMultiColumnStatistics translated(multiColumnStats);
            translated.Columns = translatedColumns;
            multiColumnStatsMap[MakeMultiColumnKey(translatedColumns)] = std::move(translated);
        }

        if (columnStatsMap.size() || multiColumnStatsMap.size()) {
            ColumnStatistics = MakeIntrusive<TOptimizerStatistics::TColumnStatMap>(
                std::move(columnStatsMap), std::move(multiColumnStatsMap));
        }
    }

    TOptimizerStatistics stats(props.Metadata->Type,
        withStatsAndCosts ? props.Statistics->ERows : 0.0,
        props.Metadata->ColumnsCount,
        withStatsAndCosts ? props.Statistics->EBytes : 0.0,
        withStatsAndCosts ? cost : 0.0,
        TIntrusivePtr<TOptimizerStatistics::TKeyColumns>(
            new TOptimizerStatistics::TKeyColumns(keyColumnNames)),
        ColumnStatistics
        );

    if (withStatsAndCosts && props.Statistics.has_value()) {
        stats.Selectivity = props.Statistics->Selectivity;
    }

    return stats;
}

TString TRBOMetadata::ToString(ui32 printOptions, const TInfoUnitRegistry& registry) {
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
            case EStatisticsType::Constant:
                metadataType = "Constant";
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

        for (size_t i = 0; i < KeyColumns.Items().size(); i++) {
            builder << registry.GetDebugName(KeyColumns.Items()[i]);
            if (i != KeyColumns.Items().size() - 1) {
                builder << ", ";
            }
        }

        builder << "]";
    }

    return builder;
}

TString TRBOStatistics::ToString(ui32 printOptions) {
    Y_UNUSED(printOptions);
    return TStringBuilder() << "N records: " << ERows << ", Size: " << EBytes << ", Selectivity: " << Selectivity;
}

}
}
