#include "vector_command_index.h"

namespace NYdbWorkload {

TWorkloadCommandIndexBase::TWorkloadCommandIndexBase(NYdbWorkload::TVectorWorkloadParams& params, const TString& name, const TString& description)
    : TYdbCommand(name, {}, description)
    , Params(params)
{}

void TWorkloadCommandIndexBase::Config(TConfig& config) {
    TYdbCommand::Config(config);

    config.Opts->SetFreeArgsNum(0);
    config.Opts->AddLongOption("dry-run", "Dry run")
        .Optional().StoreTrue(&DryRun);

    DoConfig(config);
}

int TWorkloadCommandIndexBase::Run(TConfig& config) {
    Params.DbPath = config.Database;

    Driver = MakeHolder<NYdb::TDriver>(CreateDriver(config));
    QueryClient = MakeHolder<NYdb::NQuery::TQueryClient>(*Driver);
    Params.SetClients(QueryClient.get(), nullptr, nullptr, nullptr);

    return DoRun();
}

void TWorkloadCommandIndexBase::HandleQuery(const TString& query) {
    if (DryRun) {
        Cout << query << Endl;
    } else {
        auto result = QueryClient->RetryQuerySync([query](NYdb::NQuery::TSession session) {
            return session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).GetValueSync();
        });
        NYdb::NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
    }
}

TWorkloadCommandBuildIndex::TWorkloadCommandBuildIndex(NYdbWorkload::TVectorWorkloadParams& params)
    : TWorkloadCommandIndexBase(params, "build-index", "Create and initialize an index table for the workload")
{}

void TWorkloadCommandBuildIndex::DoConfig(TConfig& config) {
    Params.ConfigureCommonOpts(config.Opts->GetOpts());
    Params.ConfigureIndexOpts(config.Opts->GetOpts());
}

int TWorkloadCommandBuildIndex::DoRun() {
    TStringBuilder ddlQuery;
    ddlQuery << "ALTER TABLE `" << Params.DbPath << "/" << Params.TableOpts.Name << "`\n";
    ddlQuery << "ADD INDEX `" << Params.IndexName << "`\n";
    ddlQuery << "GLOBAL USING vector_kmeans_tree\n";
    ddlQuery << "ON (embedding)\n";
    ddlQuery << "WITH (\n";
    ddlQuery << "    " << Params.GetDistanceDDL() << ",\n";
    ddlQuery << "    vector_type=" << Params.VectorOpts.VectorType << ",\n";
    ddlQuery << "    vector_dimension=" << Params.VectorOpts.VectorDimension;
    if (Params.KmeansTreeLevels) {
        ddlQuery << ",\n    levels=" << Params.KmeansTreeLevels;
    }
    if (Params.KmeansTreeClusters) {
        ddlQuery << ",\n    clusters=" << Params.KmeansTreeClusters;
    }
    ddlQuery << "\n);";

    if (!ddlQuery.empty()) {
        Cout << "Build vector index ..."  << Endl;
        HandleQuery(ddlQuery);
        Cout << "Build vector index ...Ok"  << Endl;
    }

    return EXIT_SUCCESS;
}

TWorkloadCommandDropIndex::TWorkloadCommandDropIndex(NYdbWorkload::TVectorWorkloadParams& params)
    : TWorkloadCommandIndexBase(params, "drop-index", "Drop the index table created for the workload")
{}

void TWorkloadCommandDropIndex::DoConfig(TConfig& config) {
    Params.ConfigureCommonOpts(config.Opts->GetOpts());
}

int TWorkloadCommandDropIndex::DoRun() {
    const TString ddlQuery = std::format(R"_(
            ALTER TABLE `{0}/{1}`
            DROP INDEX `{2}`;
        )_",
        Params.DbPath.c_str(),
        Params.TableOpts.Name.c_str(),
        Params.IndexName.c_str()
    );

    if (!ddlQuery.empty()) {
        Cout << "Drop vector index ..."  << Endl;
        HandleQuery(ddlQuery);
        Cout << "Drop vector index ...Ok"  << Endl;
    }

    return EXIT_SUCCESS;
}

} // namespace NYdbWorkload
