#include <library/cpp/colorizer/colors.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/datetime/base.h>

#include <ydb/library/yql/providers/pq/gateway/dummy/yql_pq_dummy_gateway.h>
#include <ydb/tests/tools/fqrun/src/fq_runner.h>
#include <ydb/tests/tools/kqprun/runlib/application.h>
#include <ydb/tests/tools/kqprun/runlib/utils.h>

#ifdef PROFILE_MEMORY_ALLOCATIONS
#include <library/cpp/lfalloc/alloc_profiler/profiler.h>
#endif

using namespace NKikimrRun;

namespace NFqRun {

namespace {

struct TExecutionOptions {
    TString Query;
    std::vector<FederatedQuery::ConnectionContent> Connections;
    std::vector<FederatedQuery::BindingContent> Bindings;

    bool HasResults() const {
        return !Query.empty();
    }

    TRequestOptions GetQueryOptions() const {
        return {
            .Query = Query
        };
    }

    void Validate(const TRunnerOptions& runnerOptions) const {
        if (!Query && !runnerOptions.FqSettings.MonitoringEnabled && !runnerOptions.FqSettings.GrpcEnabled) {
            ythrow yexception() << "Nothing to execute and is not running as daemon";
        }
    }
};

void RunArgumentQueries(const TExecutionOptions& executionOptions, TFqRunner& runner) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    if (!executionOptions.Connections.empty()) {
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Creating connections..." << colors.Default() << Endl;
        if (!runner.CreateConnections(executionOptions.Connections)) {
            ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Failed to create connections";
        }
    }

    if (!executionOptions.Bindings.empty()) {
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Creating bindings..." << colors.Default() << Endl;
        if (!runner.CreateBindings(executionOptions.Bindings)) {
            ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Failed to create bindings";
        }
    }

    if (executionOptions.Query) {
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Executing query..." << colors.Default() << Endl;
        if (!runner.ExecuteStreamQuery(executionOptions.GetQueryOptions())) {
            ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Query execution failed";
        }
        Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Fetching query results..." << colors.Default() << Endl;
        if (!runner.FetchQueryResults()) {
            ythrow yexception() << TInstant::Now().ToIsoStringLocal() << " Fetch query results failed";
        }
    }

    if (executionOptions.HasResults()) {
        try {
            runner.PrintQueryResults();
        } catch (...) {
            ythrow yexception() << "Failed to print script results, reason:\n" <<  CurrentExceptionMessage();
        }
    }
}

void RunAsDaemon() {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Initialization finished" << colors.Default() << Endl;
    while (true) {
        Sleep(TDuration::Seconds(1));
    }
}

void RunScript(const TExecutionOptions& executionOptions, const TRunnerOptions& runnerOptions) {
    NColorizer::TColors colors = NColorizer::AutoColors(Cout);

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Initialization of fq runner..." << colors.Default() << Endl;
    TFqRunner runner(runnerOptions);

    try {
        RunArgumentQueries(executionOptions, runner);
    } catch (const yexception& exception) {
        if (runnerOptions.FqSettings.MonitoringEnabled) {
            Cerr << colors.Red() <<  CurrentExceptionMessage() << colors.Default() << Endl;
        } else {
            throw exception;
        }
    }

    if (runnerOptions.FqSettings.MonitoringEnabled || runnerOptions.FqSettings.GrpcEnabled) {
        RunAsDaemon();
    }

    Cout << colors.Yellow() << TInstant::Now().ToIsoStringLocal() << " Finalization of fq runner..." << colors.Default() << Endl;
}

class TMain : public TMainBase {
    using EVerbose = TFqSetupSettings::EVerbose;

protected:
    void RegisterOptions(NLastGetopt::TOpts& options) override {
        options.SetTitle("FqRun -- tool to execute stream queries through FQ proxy");
        options.AddHelpOption('h');
        options.SetFreeArgsNum(0);

        // Inputs

        options.AddLongOption('p', "query", "Query to execute")
            .RequiredArgument("file")
            .StoreMappedResult(&ExecutionOptions.Query, &LoadFile);

        options.AddLongOption('s', "sql", "Query SQL text to execute")
            .RequiredArgument("str")
            .StoreResult(&ExecutionOptions.Query);
        options.MutuallyExclusive("query", "sql");

        options.AddLongOption('c', "connection", "External datasource connection protobuf FederatedQuery::ConnectionContent")
            .RequiredArgument("file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                auto& connection = ExecutionOptions.Connections.emplace_back();
                const TString file(TString(option->CurValOrDef()));
                if (!google::protobuf::TextFormat::ParseFromString(LoadFile(file), &connection)) {
                    ythrow yexception() << "Bad format of FQ connection in file '" << file << "'";
                }
                SetupAcl(connection.mutable_acl());
            });

        options.AddLongOption('b', "binding", "External datasource binding protobuf FederatedQuery::BindingContent")
            .RequiredArgument("file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                auto& binding = ExecutionOptions.Bindings.emplace_back();
                const TString file(TString(option->CurValOrDef()));
                if (!google::protobuf::TextFormat::ParseFromString(LoadFile(file), &binding)) {
                    ythrow yexception() << "Bad format of FQ binding in file '" << file << "'";
                }
                SetupAcl(binding.mutable_acl());
            });

        options.AddLongOption("fq-cfg", "File with FQ config (NFq::NConfig::TConfig for FQ proxy)")
            .RequiredArgument("file")
            .DefaultValue("./configuration/fq_config.conf")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                if (!google::protobuf::TextFormat::ParseFromString(LoadFile(TString(option->CurValOrDef())), &RunnerOptions.FqSettings.FqConfig)) {
                    ythrow yexception() << "Bad format of FQ configuration";
                }
            });

        options.AddLongOption("as-cfg", "File with actor system config (TActorSystemConfig), use '-' for default")
            .RequiredArgument("file")
            .DefaultValue("./configuration/as_config.conf")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                const TString file(option->CurValOrDef());
                if (file == "-") {
                    return;
                }

                RunnerOptions.FqSettings.ActorSystemConfig = NKikimrConfig::TActorSystemConfig();
                if (!google::protobuf::TextFormat::ParseFromString(LoadFile(file), &(*RunnerOptions.FqSettings.ActorSystemConfig))) {
                    ythrow yexception() << "Bad format of actor system configuration";
                }
            });

        options.AddLongOption("emulate-s3", "Enable readings by s3 provider from files, `bucket` value in connection - path to folder with files")
            .NoArgument()
            .SetFlag(&RunnerOptions.FqSettings.EmulateS3);

        options.AddLongOption("emulate-pq", "Emulate YDS with local file, accepts list of tables to emulate with following format: topic@file (can be used in query from cluster `pq`)")
            .RequiredArgument("topic@file")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                TStringBuf topicName, others;
                TStringBuf(option->CurVal()).Split('@', topicName, others);
                TStringBuf path, partitionCountStr;
                TStringBuf(others).Split(':', path, partitionCountStr);
                size_t partitionCount = !partitionCountStr.empty() ? FromString<size_t>(partitionCountStr) : 1;
                if (topicName.empty() || path.empty()) {
                    ythrow yexception() << "Incorrect PQ file mapping, expected form topic@path[:partitions_count]" << Endl;
                }
                if (!PqFilesMapping.emplace(topicName, NYql::TDummyTopic("pq", TString(topicName), TString(path), partitionCount)).second) {
                    ythrow yexception() << "Got duplicated topic name: " << topicName;
                }
            });

        // Outputs

        options.AddLongOption("result-file", "File with query results (use '-' to write in stdout)")
            .RequiredArgument("file")
            .DefaultValue("-")
            .StoreMappedResultT<TString>(&RunnerOptions.ResultOutput, &GetDefaultOutput);

        TChoices<EResultOutputFormat> resultFormat({
            {"rows", EResultOutputFormat::RowsJson},
            {"full-json", EResultOutputFormat::FullJson},
            {"full-proto", EResultOutputFormat::FullProto}
        });
        options.AddLongOption('R', "result-format", "Query result format")
            .RequiredArgument("result-format")
            .DefaultValue("rows")
            .Choices(resultFormat.GetChoices())
            .StoreMappedResultT<TString>(&RunnerOptions.ResultOutputFormat, resultFormat);

        // Pipeline settings

        options.AddLongOption("verbose", TStringBuilder() << "Common verbose level (max level " << static_cast<ui32>(EVerbose::Max) - 1 << ")")
            .RequiredArgument("uint")
            .DefaultValue(static_cast<ui8>(EVerbose::Info))
            .StoreMappedResultT<ui8>(&RunnerOptions.FqSettings.VerboseLevel, [](ui8 value) {
                return static_cast<EVerbose>(std::min(value, static_cast<ui8>(EVerbose::Max)));
            });

        RegisterKikimrOptions(options, RunnerOptions.FqSettings);
    }

    int DoRun(NLastGetopt::TOptsParseResult&&) override {
        ExecutionOptions.Validate(RunnerOptions);

        RunnerOptions.FqSettings.YqlToken = GetEnv(YQL_TOKEN_VARIABLE);
        RunnerOptions.FqSettings.FunctionRegistry = CreateFunctionRegistry().Get();

        auto& gatewayConfig = *RunnerOptions.FqSettings.FqConfig.mutable_gateways();
        FillTokens(gatewayConfig.mutable_pq());
        FillTokens(gatewayConfig.mutable_s3());
        FillTokens(gatewayConfig.mutable_generic());
        FillTokens(gatewayConfig.mutable_ydb());
        FillTokens(gatewayConfig.mutable_solomon());

        auto& logConfig = RunnerOptions.FqSettings.LogConfig;
        logConfig.SetDefaultLevel(NActors::NLog::EPriority::PRI_CRIT);
        FillLogConfig(logConfig);

        if (!PqFilesMapping.empty()) {
            auto fileGateway = MakeIntrusive<NYql::TDummyPqGateway>();
            for (const auto& [_, topic] : PqFilesMapping) {
                fileGateway->AddDummyTopic(topic);
            }
            RunnerOptions.FqSettings.PqGateway = std::move(fileGateway);
        }

#ifdef PROFILE_MEMORY_ALLOCATIONS
        if (RunnerOptions.FqSettings.VerboseLevel >= 1) {
            Cout << CoutColors.Cyan() << "Starting profile memory allocations" << CoutColors.Default() << Endl;
        }
        NAllocProfiler::StartAllocationSampling(true);
#else
        if (ProfileAllocationsOutput) {
            ythrow yexception() << "Profile memory allocations disabled, please rebuild fqrun with flag `-D PROFILE_MEMORY_ALLOCATIONS`";
        }
#endif

        RunScript(ExecutionOptions, RunnerOptions);

#ifdef PROFILE_MEMORY_ALLOCATIONS
        if (RunnerOptions.FqSettings.VerboseLevel >= 1) {
            Cout << CoutColors.Cyan() << "Finishing profile memory allocations" << CoutColors.Default() << Endl;
        }
        FinishProfileMemoryAllocations();
#endif

        return 0;
    }

private:
    template <typename TGatewayConfig>
    void FillTokens(TGatewayConfig* gateway) const {
        for (auto& cluster : *gateway->mutable_clustermapping()) {
            if (!cluster.GetToken()) {
                cluster.SetToken(RunnerOptions.FqSettings.YqlToken);
            }
        }
    }

private:
    inline static NColorizer::TColors CoutColors = NColorizer::AutoColors(Cout);

    TExecutionOptions ExecutionOptions;
    TRunnerOptions RunnerOptions;
    std::unordered_map<TString, NYql::TDummyTopic> PqFilesMapping;
};

}  // anonymous namespace

}  // namespace NFqRun

int main(int argc, const char* argv[]) {
    SetupSignalActions();

    try {
        NFqRun::TMain().Run(argc, argv);
    } catch (...) {
        NColorizer::TColors colors = NColorizer::AutoColors(Cerr);

        Cerr << colors.Red() <<  CurrentExceptionMessage() << colors.Default() << Endl;
        return 1;
    }
}
