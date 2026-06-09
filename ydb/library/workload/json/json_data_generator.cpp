#include "json_data_generator.h"

#include <ydb/library/json_index/json_corpus.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/string/builder.h>

namespace NYdbWorkload {

using NKikimr::NJsonIndex::TCorpusOptions;
using NKikimr::NJsonIndex::TJsonCorpus;

namespace {

TString BuildIndexDDL(const TJsonWorkloadParams& params) {
    TStringBuilder ddl;
    ddl << "--!syntax_v1\n";
    ddl << "ALTER TABLE `" << params.GetFullTableName(params.TableName.c_str()) << "`\n";
    ddl << "ADD INDEX `" << params.IndexName << "`\n";
    ddl << "GLOBAL USING json ON (`Text`);";
    return ddl;
}

class TJsonCorpusBulkGenerator final: public IBulkDataGenerator {
private:
    static constexpr size_t PORTION_SIZE = 1000;

    const TJsonWorkloadParams& Params;
    const ui64 RowCount;
    const TJsonCorpus Corpus;
    TAdaptiveLock Lock;
    ui64 DoneRows = 0;

public:
    TJsonCorpusBulkGenerator(const TJsonWorkloadParams& params, ui64 rowCount, TJsonCorpus corpus)
        : IBulkDataGenerator(params.TableName, rowCount)
        , Params(params)
        , RowCount(rowCount)
        , Corpus(std::move(corpus))
    {}

    TDataPortions GenerateDataPortion() override {
        ui64 batchStart = 0;
        size_t currentBatchSize = 0;
        with_lock(Lock) {
            if (DoneRows >= RowCount) {
                return {};
            }
            batchStart = DoneRows;
            currentBatchSize = Min<ui64>(PORTION_SIZE, RowCount - DoneRows);
            DoneRows += currentBatchSize;
        }

        NYdb::TValueBuilder valueBuilder;
        valueBuilder.BeginList();
        for (size_t i = 0; i < currentBatchSize; ++i) {
            const auto& row = Corpus.Rows()[batchStart + i];
            auto& listItem = valueBuilder.AddListItem();
            listItem.BeginStruct();
            listItem.AddMember("Key").Uint64(row.Key);
            const std::optional<std::string> text = row.JsonText
                ? std::optional<std::string>(row.JsonText->c_str())
                : std::nullopt;
            if (Params.Binary) {
                listItem.AddMember("Text").OptionalJsonDocument(text);
            } else {
                listItem.AddMember("Text").OptionalJson(text);
            }
            listItem.EndStruct();
        }
        valueBuilder.EndList();

        return {MakeIntrusive<TDataPortion>(
            Params.GetFullTableName(Params.TableName.c_str()),
            valueBuilder.Build(),
            currentBatchSize
        )};
    }
};

} // namespace

TJsonDataInitializer::TJsonDataInitializer(const TJsonWorkloadParams& params)
    : TWorkloadDataInitializerBase("generator", "Generate JSON corpus and build JSON index", params)
    , JsonParams(params)
    , RowCount(params.RowCount)
    , Seed(params.Seed)
{}

void TJsonDataInitializer::ConfigureOpts(NLastGetopt::TOpts& opts) {
    opts.AddLongOption("rows", "Number of rows to generate.")
        .DefaultValue(RowCount)
        .StoreResult(&RowCount);
    opts.AddLongOption("seed", "Random seed for corpus generation.")
        .DefaultValue(Seed)
        .StoreResult(&Seed);
}

TBulkDataGeneratorList TJsonDataInitializer::DoGetBulkInitialData() {
    Cout << "Generating " << RowCount << " JSON corpus rows..." << Endl;
    TJsonCorpus corpus(TCorpusOptions{.RowCount = RowCount, .Seed = Seed});
    return {std::make_shared<TJsonCorpusBulkGenerator>(JsonParams, RowCount, std::move(corpus))};
}

int TJsonDataInitializer::PostImport() {
    const TString ddlQuery = BuildIndexDDL(JsonParams);

    Cout << "Building JSON index ..." << Endl;
    const auto result = JsonParams.QueryClient->RetryQuerySync([&ddlQuery](NYdb::NQuery::TSession session) {
        return session.ExecuteQuery(ddlQuery, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
    });
    NYdb::NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    Cout << "Building JSON index ...Ok" << Endl;

    return EXIT_SUCCESS;
}

} // namespace NYdbWorkload
