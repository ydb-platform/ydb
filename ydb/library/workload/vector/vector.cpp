#include "vector.h"

#include "ydb/public/lib/ydb_cli/commands/ydb_workload.h"
#include "ydb/public/lib/ydb_cli/commands/ydb_workload_import.h"

namespace NYdb::NConsoleClient {

    class TWorkloadCommandBuildIndex final : public TYdbCommand {
    private:
        NYdbWorkload::TVectorWorkloadParams& Params;
        bool DryRun = false;

        THolder<TDriver> Driver;
        THolder<NTable::TTableClient> TableClient;

    public:
        TWorkloadCommandBuildIndex(NYdbWorkload::TVectorWorkloadParams& params)
            : TYdbCommand("build-index", {}, "Create and initialize an index table for the workload")
            , Params(params)
        {}

        virtual void Config(TConfig& config) override {
            TYdbCommand::Config(config);

            config.Opts->SetFreeArgsNum(0);
            config.Opts->AddLongOption("dry-run", "Dry run")
                .Optional().StoreTrue(&DryRun);

            Params.ConfigureCommonOpts(config.Opts->GetOpts());
            Params.ConfigureIndexOpts(config.Opts->GetOpts());
        }

        virtual int Run(TConfig& config) override {
            Params.DbPath = config.Database;

            Driver = MakeHolder<NYdb::TDriver>(CreateDriver(config));
            TableClient = MakeHolder<NTable::TTableClient>(*Driver);
            Params.SetClients(nullptr, nullptr, TableClient.Get(), nullptr);

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
                Cout << "Init vector index ..."  << Endl;
                if (DryRun) {
                    Cout << ddlQuery << Endl;
                } else {
                    auto result = TableClient->RetryOperationSync([ddlQuery](NTable::TSession session) {
                        return session.ExecuteSchemeQuery(ddlQuery.c_str()).GetValueSync();
                    });
                    NStatusHelpers::ThrowOnErrorOrPrintIssues(result);
                }
                Cout << "Init vector index ...Ok"  << Endl;
            }

            return EXIT_SUCCESS;
        }
    };

    TCommandVector::TCommandVector()
        : TClientCommandTree("vector", {}, "YDB vector workload")
        , Params(std::make_unique<NYdbWorkload::TVectorWorkloadParams>())
    {
        if (const auto desc = Params->GetDescription(NYdbWorkload::TWorkloadParams::ECommandType::Root, 0)) {
            Description = desc;
        }
        AddCommand(std::make_unique<TWorkloadCommandInit>(*Params));
        if (auto import = TWorkloadCommandImport::Create(*Params)) {
            AddCommand(std::move(import));
        }

        AddCommand(std::make_unique<TWorkloadCommandBuildIndex>(*Params));

        auto supportedWorkloads = Params->CreateGenerator()->GetSupportedWorkloadTypes();
        switch (supportedWorkloads.size()) {
        case 0:
            break;
        case 1:
            supportedWorkloads.back().CommandName = "run";
            AddCommand(std::make_unique<TWorkloadCommandRun>(*Params, supportedWorkloads.back()));
            break;
        default: {
            auto run = std::make_unique<TClientCommandTree>("run", std::initializer_list<TString>(), "Run YDB vector workload");
            for (const auto& type: supportedWorkloads) {
                run->AddCommand(std::make_unique<TWorkloadCommandRun>(*Params, type));
            }
            AddCommand(std::move(run));
            break;
        }
        }
        AddCommand(std::make_unique<TWorkloadCommandClean>(*Params));
    }

    void TCommandVector::Config(TConfig& config) {
        TClientCommandTree::Config(config);
        Params->ConfigureOpts(config.Opts->GetOpts(), NYdbWorkload::TWorkloadParams::ECommandType::Root, 0);
    }

} // namespace NYdb::NConsoleClient
