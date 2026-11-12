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
        : TYdbCommand(init ? "init" : "run", {}, init ? "Create initial_count tables and topics" : "Create missing objects and repeatedly alter tables and topics")
        , Init(init)
    {}

    void Config(TConfig& config) override {
        TYdbCommand::Config(config);
        config.SetFreeArgsNum(0);
        config.Opts->AddLongOption("config", "Workload YAML configuration file.").Required().RequiredArgument("PATH").StoreResult(&ConfigPath);
        if (!Init) {
            config.Opts->AddLongOption('s', "seconds", "Duration of CREATE/ALTER traffic after schema validation; waits for outstanding requests afterwards.")
                .DefaultValue(60).StoreResult(&Seconds);
        }
    }

    void Parse(TConfig& config) override {
        TClientCommand::Parse(config);
        WorkloadConfig = NYdbWorkload::NScheme::ParseConfig(TFileInput(ConfigPath).ReadAll());
        Y_ENSURE(Seconds > 0, "--seconds must be positive");
    }

    int Run(TConfig& config) override {
        SetInterruptHandlers();
        Y_DEFER { ResetInterrupted(); };
        auto driver = CreateDriver(config);
        const ui64 concurrency = Init ? WorkloadConfig.CreateConcurrency
            : ui64(WorkloadConfig.CreateConcurrency) + WorkloadConfig.AlterConcurrency;
        auto client = CreateSchemeWorkloadClient(driver, config.Database, std::min<ui64>(concurrency, WorkloadConfig.ObjectCount()));
        NYdbWorkload::NScheme::TRunner runner(WorkloadConfig, *client, [] { return IsInterrupted(); });
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
            if (Init) {
                runner.Init();
            } else {
                runner.Run(TDuration::Seconds(Seconds));
            }
        } catch (...) {
            print();
            throw;
        }
        print();
        return EXIT_SUCCESS;
    }

private:
    const bool Init;
    TString ConfigPath;
    ui32 Seconds = 60;
    NYdbWorkload::NScheme::TConfig WorkloadConfig;
};

} // namespace

TCommandWorkloadScheme::TCommandWorkloadScheme()
    : TClientCommandTree("scheme", {}, "Create and alter tables and topics to stress schema operations")
{
    AddCommand(std::make_unique<TCommandSchemeLoad>(true));
    AddCommand(std::make_unique<TCommandSchemeLoad>(false));
}

} // namespace NYdb::NConsoleClient
