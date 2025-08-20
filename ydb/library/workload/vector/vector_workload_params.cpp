#include "vector_enums.h"
#include "vector_workload_params.h"
#include "vector_workload_generator.h"

#include <util/datetime/base.h>
#include <util/generic/serialized_enum.h>

#include <format>
#include <string>

#include <algorithm>

namespace NYdbWorkload {

void TVectorWorkloadParams::ConfigureOpts(NLastGetopt::TOpts& opts, const ECommandType commandType, int workloadType) {
    auto addCommonParam = [&]() {
        opts.AddLongOption( "table", "Table name.")
            .DefaultValue("vector_index_workload").StoreResult(&TableName);
        opts.AddLongOption( "index", "Index name.")
            .DefaultValue("index").StoreResult(&IndexName);
    };

    auto addInitParam = [&]() {
        opts.AddLongOption( "rows", "Number of vectors to init the table.")
            .Required().StoreResult(&VectorInitCount);
        opts.AddLongOption( "distance", "Distance/similarity function")
            .Required().StoreResult(&Distance);
        opts.AddLongOption( "vector-type", "Type of vectors")
            .Required().StoreResult(&VectorType);
        opts.AddLongOption( "vector-dimension", "Vector dimension.")
            .Required().StoreResult(&VectorDimension);
        opts.AddLongOption( "kmeans-tree-levels", "Number of levels in the kmeans tree")
            .Required().StoreResult(&KmeansTreeLevels);
        opts.AddLongOption( "kmeans-tree-clusters", "Number of cluster in kmeans")
            .Required().StoreResult(&KmeansTreeClusters);

    };

    auto addUpsertParam = [&]() {
    };

    auto addSelectParam = [&]() {
        opts.AddLongOption( "query-table", "Name of the table with predefined search vectors.")
            .DefaultValue("").StoreResult(&QueryTableName);
        opts.AddLongOption( "targets", "Number of vectors to search as targets.")
            .DefaultValue(100).StoreResult(&Targets);
        opts.AddLongOption( "limit", "Maximum number of vectors to return.")
            .DefaultValue(5).StoreResult(&Limit);
        opts.AddLongOption( "kmeans-tree-clusters", "Maximum number of clusters to use during search.")
            .DefaultValue(1).StoreResult(&KmeansTreeSearchClusters);
        opts.AddLongOption( "recall-threads", "Number of threads for concurrent queries during recall measurement.")
            .DefaultValue(10).StoreResult(&RecallThreads);
        opts.AddLongOption( "recall", "Measure recall metrics. It trains on 'targets' vector by bruce-force search.")
            .StoreTrue(&Recall);
        opts.AddLongOption( "non-indexed", "Take vector settings from the index, but search without the index.")
            .StoreTrue(&NonIndexedSearch);
        opts.AddLongOption("stale-ro", "Read with StaleRO mode")
            .StoreTrue(&StaleRO);            
    };

    switch (commandType) {
    case TWorkloadParams::ECommandType::Init:
        addCommonParam();
        addInitParam();
        break;
    case TWorkloadParams::ECommandType::Run:
        addCommonParam();
        switch (static_cast<EWorkloadRunType>(workloadType)) {
        case EWorkloadRunType::Upsert:
            addUpsertParam();
            break;
        case EWorkloadRunType::Select:
            addSelectParam();
            break;
        }
        break;
    default:
        break;
    }
}

void TVectorWorkloadParams::Init() {
    const TString tablePath = GetFullTableName(TableName.c_str());

    auto session = TableClient->GetSession().ExtractValueSync().GetSession();
    auto describeTableResult = session.DescribeTable(tablePath,
        NYdb::NTable::TDescribeTableSettings().WithTableStatistics(true)).GetValueSync();
    Y_ABORT_UNLESS(describeTableResult.IsSuccess(), "DescribeTable failed: %s", describeTableResult.GetIssues().ToString().c_str());

    // Get the table description
    const auto& tableDescription = describeTableResult.GetTableDescription();

    // Find the specified index
    bool indexFound = false;

    Y_ABORT_UNLESS(tableDescription.GetPrimaryKeyColumns().size() == 1,
        "Only single key is supported. But table %s has %d key columns", TableName.c_str(), tableDescription.GetPrimaryKeyColumns().size());
    KeyColumn = tableDescription.GetPrimaryKeyColumns().at(0);

    for (const auto& index : tableDescription.GetIndexDescriptions()) {
        if (index.GetIndexName() == IndexName) {
            indexFound = true;

            // Check if we have more than one column (indicating a prefixed index)
            const auto& keyColumns = index.GetIndexColumns();
            if (keyColumns.size() > 1) {
                // The first column is the prefix column, the last column is the embedding
                PrefixColumn = keyColumns[0];
            }
            EmbeddingColumn = keyColumns.back();

            // Extract the distance metric from index settings
            const auto& indexSettings = std::get<NYdb::NTable::TKMeansTreeSettings>(index.GetIndexSettings());
            Metric = indexSettings.Settings.Metric;
            VectorDimension = indexSettings.Settings.VectorDimension;

            break;
        }
    }

    // Verify that key and prefix columns have integer types
    for (const auto& column : tableDescription.GetColumns()) {
        if (PrefixColumn.has_value() && column.Name == PrefixColumn.value()) {
            auto str = column.Type.ToString();
            if (str[str.size()-1] == '?')
                str.resize(str.size()-1);
            PrefixType = str;
        }
        if (column.Name == KeyColumn) {
            KeyIsInt = (column.Type.ToString().contains("int") || column.Type.ToString().contains("Int"));
        }
    }

    if (!TableRowCount) {
        TableRowCount = tableDescription.GetTableRows();
    }
    Y_ABORT_UNLESS(TableRowCount > 0, "Table %s is empty or statistics is not calculated yet", TableName.c_str());

    // If we have fewer vectors than requested targets, adjust Params.Targets
    Y_ABORT_UNLESS(TableRowCount >= Targets, "Requested more targets than row number in the dataset.");

    Y_ABORT_UNLESS(indexFound, "Index %s not found in table %s", IndexName.c_str(), TableName.c_str());

    if (QueryTableName) {
        const TString tablePath = GetFullTableName(QueryTableName.c_str());
        auto describeTableResult = session.DescribeTable(tablePath).GetValueSync();
        Y_ABORT_UNLESS(describeTableResult.IsSuccess(), "DescribeTable failed: %s", describeTableResult.GetIssues().ToString().c_str());

        const auto& tableDescription = describeTableResult.GetTableDescription();
        Y_ABORT_UNLESS(tableDescription.GetPrimaryKeyColumns().size() == 1,
            "Only single key is supported. But table %s has %d key columns", QueryTableName.c_str(), tableDescription.GetPrimaryKeyColumns().size());
        QueryTableKeyColumn = tableDescription.GetPrimaryKeyColumns().at(0);
    }

    if (NonIndexedSearch) {
        IndexName = "";
    }
}

void TVectorWorkloadParams::Validate(const ECommandType commandType, int workloadType) {
    switch (commandType) {
        case TWorkloadParams::ECommandType::Init:
            break;
        case TWorkloadParams::ECommandType::Run:
            switch (static_cast<EWorkloadRunType>(workloadType)) {
                case EWorkloadRunType::Upsert:
                    break;
                case EWorkloadRunType::Select:
                    break;
            }
            break;
        case TWorkloadParams::ECommandType::Clean:
            break;
        case TWorkloadParams::ECommandType::Root:
            break;
        case TWorkloadParams::ECommandType::Import:
            break;
    }
}

THolder<IWorkloadQueryGenerator> TVectorWorkloadParams::CreateGenerator() const {
    return MakeHolder<TVectorWorkloadGenerator>(this);
}

TString TVectorWorkloadParams::GetWorkloadName() const {
    return "vector";
}

} // namespace NYdbWorkload
