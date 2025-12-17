#include "vector_data_generator.h"
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
    auto addUpsertParam = [&]() {
    };

    auto addSelectParam = [&]() {
        opts.AddLongOption( "query-table", "Name of the table with predefined search vectors")
            .DefaultValue("").StoreResult(&QueryTableName);
        opts.AddLongOption( "targets", "Number of vectors to search as targets")
            .DefaultValue(100).StoreResult(&Targets);
        opts.AddLongOption( "limit", "Maximum number of vectors to return")
            .DefaultValue(5).StoreResult(&Limit);
        opts.AddLongOption( "kmeans-tree-clusters", "Maximum number of clusters to use during search")
            .DefaultValue(1).StoreResult(&KmeansTreeSearchClusters);
        opts.AddLongOption( "recall-threads", "Number of threads for concurrent queries during recall measurement")
            .DefaultValue(10).StoreResult(&RecallThreads);
        opts.AddLongOption( "recall", "Measure recall metrics. It trains on 'targets' vector by bruce-force search")
            .StoreTrue(&Recall);
        opts.AddLongOption( "non-indexed", "Take vector settings from the index, but search without the index")
            .StoreTrue(&NonIndexedSearch);
        opts.AddLongOption("stale-ro", "Read with StaleRO mode")
            .StoreTrue(&StaleRO);
    };

    switch (commandType) {
    case TWorkloadParams::ECommandType::Init:
        NVector::ConfigureTableOpts(opts, &TableOpts);
        break;
    case TWorkloadParams::ECommandType::Import:
        ConfigureCommonOpts(opts);
        break;
    case TWorkloadParams::ECommandType::Run:
        ConfigureCommonOpts(opts);
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

void TVectorWorkloadParams::ConfigureCommonOpts(NLastGetopt::TOpts& opts) {
    opts.AddLongOption( "table", "Table name")
        .DefaultValue(TableOpts.Name).StoreResult(&TableOpts.Name);
    opts.AddLongOption( "index", "Index name")
        .DefaultValue(IndexName).StoreResult(&IndexName);
}

void TVectorWorkloadParams::ConfigureIndexOpts(NLastGetopt::TOpts& opts) {
    NVector::ConfigureVectorOpts(opts, &VectorOpts);

    opts.AddLongOption( "distance", "Distance/similarity function")
        .Required().StoreResult(&Distance);
    opts.AddLongOption( "kmeans-tree-levels", "Number of levels in the kmeans tree. Reference: https://ydb.tech/docs/dev/vector-indexes#kmeans-tree-type")
        .Required().StoreResult(&KmeansTreeLevels);
    opts.AddLongOption( "kmeans-tree-clusters", "Number of clusters in kmeans. Reference: https://ydb.tech/docs/dev/vector-indexes#kmeans-tree-type")
        .Required().StoreResult(&KmeansTreeClusters);
}

TVector<TString> TVectorWorkloadParams::GetColumns() const {
    TVector<TString> result(KeyColumns.begin(), KeyColumns.end());
    result.emplace_back(EmbeddingColumn);
    if (PrefixColumn.has_value()) {
        result.emplace_back(PrefixColumn.value());
    }
    return result;
}

void TVectorWorkloadParams::Init() {
    const TString tablePath = GetFullTableName(TableOpts.Name.c_str());

    auto session = TableClient->GetSession().ExtractValueSync().GetSession();
    auto describeTableResult = session.DescribeTable(tablePath,
        NYdb::NTable::TDescribeTableSettings().WithTableStatistics(true)).GetValueSync();
    Y_ABORT_UNLESS(describeTableResult.IsSuccess(), "DescribeTable failed: %s", describeTableResult.GetIssues().ToString().c_str());

    // Get the table description
    const auto& tableDescription = describeTableResult.GetTableDescription();

    // Find the specified index
    bool indexFound = false;

    KeyColumns = tableDescription.GetPrimaryKeyColumns();

    for (const auto& index : tableDescription.GetIndexDescriptions()) {
        if (index.GetIndexName() == IndexName) {
            indexFound = true;

            // Check if we have more than one column (indicating a prefixed index)
            const auto& keyColumns = index.GetIndexColumns();
            if (keyColumns.size() > 1) {
                // The first column is the prefix column, the last column is the embedding
                Y_ABORT_UNLESS(keyColumns.size() == 2, "Only single prefix column is supported");
                PrefixColumn = keyColumns[0];
            }
            EmbeddingColumn = keyColumns.back();

            // Extract the distance metric from index settings
            const auto& indexSettings = std::get<NYdb::NTable::TKMeansTreeSettings>(index.GetIndexSettings());
            Metric = indexSettings.Settings.Metric;
            VectorOpts.VectorDimension = indexSettings.Settings.VectorDimension;

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
        if (KeyColumns.size() == 1 && column.Name == KeyColumns[0]) {
            KeyIsInt = (column.Type.ToString().contains("int") || column.Type.ToString().contains("Int"));
        }
    }

    if (!TableRowCount) {
        TableRowCount = tableDescription.GetTableRows();
    }
    Y_ABORT_UNLESS(TableRowCount > 0, "Table %s is empty or statistics is not calculated yet", TableOpts.Name.c_str());

    // If we have fewer vectors than requested targets, adjust Params.Targets
    Y_ABORT_UNLESS(TableRowCount >= Targets, "Requested more targets than row number in the dataset.");

    Y_ABORT_UNLESS(indexFound, "Index %s not found in table %s", IndexName.c_str(), TableOpts.Name.c_str());

    if (QueryTableName) {
        const TString tablePath = GetFullTableName(QueryTableName.c_str());
        auto describeTableResult = session.DescribeTable(tablePath).GetValueSync();
        Y_ABORT_UNLESS(describeTableResult.IsSuccess(), "DescribeTable failed: %s", describeTableResult.GetIssues().ToString().c_str());

        const auto& tableDescription = describeTableResult.GetTableDescription();
        QueryTableKeyColumns = tableDescription.GetPrimaryKeyColumns();
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

TWorkloadDataInitializer::TList TVectorWorkloadParams::CreateDataInitializers() const {
    return {
        std::make_shared<TWorkloadVectorFilesDataInitializer>(*this),
        std::make_shared<TWorkloadVectorGenerateDataInitializer>(*this),
    };
}

TString TVectorWorkloadParams::GetWorkloadName() const {
    return "vector";
}

} // namespace NYdbWorkload
