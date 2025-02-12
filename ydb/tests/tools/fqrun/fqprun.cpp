#include <library/cpp/colorizer/colors.h>
#include <library/cpp/getopt/last_getopt.h>

#include <util/datetime/base.h>

#include <ydb/tests/tools/fqrun/src/fq_runner.h>
#include <ydb/tests/tools/kqprun/runlib/application.h>
#include <ydb/tests/tools/kqprun/runlib/utils.h>

using namespace NKikimrRun;

namespace NFqRun {

namespace {

struct TExecutionOptions {
    TString Query;

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
protected:
    void RegisterOptions(NLastGetopt::TOpts& options) override {
        options.SetTitle("FqRun -- tool to execute stream queries through FQ proxy");
        options.AddHelpOption('h');
        options.SetFreeArgsNum(0);

        // Inputs

        options.AddLongOption('p', "query", "Query to execute")
            .RequiredArgument("file")
            .StoreMappedResult(&ExecutionOptions.Query, &LoadFile);

        options.AddLongOption("fq-cfg", "File with FQ config (NFq::NConfig::TConfig for FQ proxy)")
            .RequiredArgument("file")
            .DefaultValue("./configuration/fq_config.conf")
            .Handler1([this](const NLastGetopt::TOptsParser* option) {
                if (!google::protobuf::TextFormat::ParseFromString(LoadFile(TString(option->CurValOrDef())), &RunnerOptions.FqSettings.FqConfig)) {
                    ythrow yexception() << "Bad format of FQ configuration";
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

        RegisterKikimrOptions(options, RunnerOptions.FqSettings);
    }

    int DoRun(NLastGetopt::TOptsParseResult&&) override {
        ExecutionOptions.Validate(RunnerOptions);

        auto& logConfig = RunnerOptions.FqSettings.LogConfig;
        logConfig.SetDefaultLevel(NActors::NLog::EPriority::PRI_CRIT);
        FillLogConfig(logConfig);

        RunScript(ExecutionOptions, RunnerOptions);

        return 0;
    }

private:
    TExecutionOptions ExecutionOptions;
    TRunnerOptions RunnerOptions;
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
