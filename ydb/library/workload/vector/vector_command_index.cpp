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
    const TString ddlQuery = std::format(R"_(
            ALTER TABLE `{0}/{1}`
            ADD INDEX `{2}`
            GLOBAL USING vector_kmeans_tree
            ON (embedding)
            WITH (
                distance={3},
                vector_type={4},
                vector_dimension={5},
                levels={6},
                clusters={7}
            );
        )_",
        Params.DbPath.c_str(),
        Params.TableName.c_str(),
        Params.IndexName.c_str(),
        Params.Distance.c_str(),
        Params.VectorType.c_str(),
        Params.VectorDimension,
        Params.KmeansTreeLevels,
        Params.KmeansTreeClusters
    );

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
        Params.TableName.c_str(),
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
