#include "scheme_workload.h"
#include "client.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_command.h>
#include <ydb/public/lib/ydb_cli/common/interruptable.h>

#include <library/cpp/time_provider/monotonic.h>

#include <util/generic/scope.h>
#include <util/stream/file.h>

#include <algorithm>

namespace NYdb::NConsoleClient {
namespace {

class TCommandSchemeLoad final : public TYdbCommand, private TInterruptableCommand {
public:
    explicit TCommandSchemeLoad(bool init)
        : TYdbCommand(init ? "init" : "run", {}, init ? "Create initial_count tables and topics" : "Run configured schema operations on tables and topics")
        , Init_(init)
    {}

    void Config(TConfig& config) override {
        TYdbCommand::Config(config);
        config.SetFreeArgsNum(0);
        config.Opts->AddLongOption("config", "Optional workload YAML configuration file.").RequiredArgument("PATH").StoreResult(&ConfigPath_);
        config.Opts->AddLongOption("path", "Workload directory relative to the database; overrides YAML.").RequiredArgument("PATH").StoreResult(&Path_);
        config.Opts->AddLongOption("tables", "Target table count; overrides YAML.").RequiredArgument("N").StoreResult(&Tables_);
        config.Opts->AddLongOption("topics", "Target topic count; overrides YAML.").RequiredArgument("N").StoreResult(&Topics_);
        config.Opts->AddLongOption("partitions", "Initial partitions for all configured object groups; overrides YAML.").RequiredArgument("N").StoreResult(&Partitions_);
        config.Opts->AddLongOption("operations", "Replace run operations and in-flight limits: create=N,alter=N,describe=N,list=N. Omitted operations are disabled.")
            .RequiredArgument("NAME=N,...").StoreResult(&Operations_);
        config.Opts->AddLongOption('t', "threads", "Number of workload threads for preparing and starting operations.")
            .DefaultValue(NYdbWorkload::NScheme::DEFAULT_WORKLOAD_THREADS).StoreResult(&Threads_);
        if (!Init_) {
            config.Opts->AddLongOption('s', "seconds", "Duration of CREATE/ALTER traffic after schema validation; waits for outstanding requests afterwards.")
                .DefaultValue(60).StoreResult(&Seconds_);
        }
    }

    void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        NYdbWorkload::NScheme::TConfigOverrides overrides;
        if (config.ParseResult->Has("path")) {
            overrides.Path = Path_;
        }
        if (config.ParseResult->Has("tables")) {
            overrides.Tables = Tables_;
        }
        if (config.ParseResult->Has("topics")) {
            overrides.Topics = Topics_;
        }
        if (config.ParseResult->Has("partitions")) {
            overrides.Partitions = Partitions_;
        }
        if (config.ParseResult->Has("operations")) {
            overrides.Operations = Operations_;
        }
        const TString yaml = config.ParseResult->Has("config") ? TFileInput(ConfigPath_).ReadAll() : TString("{}");
        WorkloadConfig_ = NYdbWorkload::NScheme::ParseConfig(yaml, overrides);
        Y_ENSURE(Threads_ > 0 && Threads_ <= NYdbWorkload::NScheme::MAX_WORKLOAD_THREADS,
            "--threads must be in [1, " << NYdbWorkload::NScheme::MAX_WORKLOAD_THREADS << "]");
        if (!Init_) {
            Y_ENSURE(Seconds_ > 0, "--seconds must be positive");
        }
    }

    int Run(TConfig& config) override {
        SetInterruptHandlers();
        Y_DEFER { ResetInterrupted(); };
        auto driver = CreateDriver(config);
        const ui64 concurrency = Init_ ? std::max(1u, WorkloadConfig_.CreateConcurrency) : WorkloadConfig_.TotalConcurrency();
        auto client = CreateSchemeWorkloadClient(driver, config.Database, std::max<ui64>(1, std::min<ui64>(concurrency, WorkloadConfig_.ObjectCount())));
        NYdbWorkload::NScheme::TRunner runner(WorkloadConfig_, *client, [] { return IsInterrupted(); }, Threads_);
        const auto started = TMonotonic::Now();
        const auto print = [&] {
            const auto counters = runner.Statistics();
            Cout << "Operation\tSucceeded" << Endl;
            for (size_t i = 0; i < counters.size(); ++i) {
                Cout << NYdbWorkload::NScheme::OperationName(static_cast<NYdbWorkload::NScheme::EOperation>(i)) << '\t' << counters[i] << Endl;
            }
            Cout << "ElapsedSeconds\t" << (TMonotonic::Now() - started).SecondsFloat() << Endl;
        };
        try {
            if (Init_) {
                runner.Init();
            } else {
                runner.Run(TDuration::Seconds(Seconds_));
            }
        } catch (...) {
            print();
            throw;
        }
        print();
        return EXIT_SUCCESS;
    }

private:
    const bool Init_;
    TString ConfigPath_;
    TString Path_;
    TString Tables_;
    TString Topics_;
    TString Partitions_;
    TString Operations_;
    ui32 Seconds_ = 60;
    ui32 Threads_ = NYdbWorkload::NScheme::DEFAULT_WORKLOAD_THREADS;
    NYdbWorkload::NScheme::TConfig WorkloadConfig_;
};

} // namespace

TCommandWorkloadScheme::TCommandWorkloadScheme()
    : TClientCommandTree("scheme", {}, "Stress schema creation, alteration, description and directory listing")
{
    AddCommand(std::make_unique<TCommandSchemeLoad>(true));
    AddCommand(std::make_unique<TCommandSchemeLoad>(false));
}

} // namespace NYdb::NConsoleClient
