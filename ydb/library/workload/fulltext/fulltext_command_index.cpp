#include "fulltext_command_index.h"

#include <format>

namespace NYdbWorkload {

TFulltextWorkloadCommandIndexBase::TFulltextWorkloadCommandIndexBase(NYdbWorkload::TFulltextWorkloadParams& params, const TString& name, const TString& description)
    : TYdbCommand(name, {}, description)
    , Params(params)
{}

void TFulltextWorkloadCommandIndexBase::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->SetFreeArgsNum(0);
    config.Opts->AddLongOption("dry-run", "Dry run")
        .Optional().StoreTrue(&DryRun);

    DoConfig(config);
}

int TFulltextWorkloadCommandIndexBase::Run(TConfig& config) {
    Params.DbPath = config.Database;

    Driver = MakeHolder<NYdb::TDriver>(CreateDriver(config));
    QueryClient = MakeHolder<NYdb::NQuery::TQueryClient>(*Driver);
    Params.SetClients(QueryClient.get(), nullptr, nullptr, nullptr);

    return DoRun();
}

void TFulltextWorkloadCommandIndexBase::HandleQuery(const TString& query) {
    if (DryRun) {
        Cout << query << Endl;
    } else {
        auto result = QueryClient->RetryQuerySync([query](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        });
        NYdb::NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    }
}

TFulltextWorkloadCommandBuildIndex::TFulltextWorkloadCommandBuildIndex(NYdbWorkload::TFulltextWorkloadParams& params)
    : TFulltextWorkloadCommandIndexBase(params, "build-index", "Create and initialize a fulltext index for the workload")
{}

void TFulltextWorkloadCommandBuildIndex::DoConfig(TConfig& config) {
    config.Opts->AddLongOption("table", "Table name")
        .DefaultValue(Params.TableName).StoreResult(&Params.TableName);
    config.Opts->AddLongOption("index-name", "Fulltext index name")
        .DefaultValue(Params.IndexName).StoreResult(&Params.IndexName);
    config.Opts->AddLongOption("index-type", "Fulltext index type (fulltext_plain, fulltext_relevance)")
        .DefaultValue(Params.IndexType).StoreResult(&Params.IndexType);
}

int TFulltextWorkloadCommandBuildIndex::DoRun() {
    const TString ddlQuery = std::format(R"sql(
            ALTER TABLE `{0}/{1}`
            ADD INDEX `{2}`
            GLOBAL SYNC USING {3}
            ON (text) WITH (
                use_filter_lowercase=true,
                tokenizer=standard
            );
        )sql",
        Params.DbPath.c_str(),
        Params.TableName.c_str(),
        Params.IndexName.c_str(),
        Params.IndexType.c_str()
    );

    if (!ddlQuery.empty()) {
        Cout << "Build fulltext index ..."  << Endl;
        HandleQuery(ddlQuery);
        Cout << "Build fulltext index ...Ok"  << Endl;
    }

    return EXIT_SUCCESS;
}

TFulltextWorkloadCommandDropIndex::TFulltextWorkloadCommandDropIndex(NYdbWorkload::TFulltextWorkloadParams& params)
    : TFulltextWorkloadCommandIndexBase(params, "drop-index", "Drop the fulltext index created for the workload")
{}

void TFulltextWorkloadCommandDropIndex::DoConfig(TConfig& config) {
    config.Opts->AddLongOption("table", "Table name")
        .DefaultValue(Params.TableName).StoreResult(&Params.TableName);
    config.Opts->AddLongOption("index-name", "Fulltext index name")
        .DefaultValue(Params.IndexName).StoreResult(&Params.IndexName);
}

int TFulltextWorkloadCommandDropIndex::DoRun() {
    const TString ddlQuery = std::format(R"_(
            ALTER TABLE `{0}/{1}`
            DROP INDEX `{2}`;
        )_",
        Params.DbPath.c_str(),
        Params.TableName.c_str(),
        Params.IndexName.c_str()
    );

    if (!ddlQuery.empty()) {
        Cout << "Drop fulltext index ..."  << Endl;
        HandleQuery(ddlQuery);
        Cout << "Drop fulltext index ...Ok"  << Endl;
    }

    return EXIT_SUCCESS;
}

} // namespace NYdbWorkload
